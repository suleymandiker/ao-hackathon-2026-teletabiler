# -*- coding: utf-8 -*-
import hashlib
import json
import re

from ai_engine import call_ai_agent, load_prompt


class ParserPolicyDiscovery:
    """Control-plane discovery for genuinely unknown/custom text formats."""

    MAX_SAMPLES = 150
    MAX_AI_SAMPLES = 40
    POLICY_SCHEMA_VERSION = 3

    @staticmethod
    def signature(samples):
        # Structural fingerprint: bounded sample only, never per event.
        normalized = []
        for line in samples[:ParserPolicyDiscovery.MAX_SAMPLES]:
            head = line.splitlines()[0][:500]
            head = re.sub(r"\d+", "#", head)
            head = re.sub(r"[0-9a-fA-F]{8,}", "<HEX>", head)
            normalized.append(head)
        blob = "\n".join(normalized).encode("utf-8", "replace")
        return hashlib.sha256(blob).hexdigest()[:24]

    @staticmethod
    def enabled():
        # AI configuration/credentials are owned centrally by ai_engine.py.
        # ParserPipeline.ai_enabled remains the feature-level on/off switch.
        return True

    @staticmethod
    def _shape(text):
        """Cheap structural shape used only inside the bounded discovery sample."""
        head = str(text).splitlines()[0][:500]
        head = re.sub(
            r"\b\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}(?:[.,]\d+)?(?:Z|[+-]\d{2}:?\d{2})?",
            "<TS>",
            head,
        )
        head = re.sub(r"\b\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}(?:\.\d+)?", "<TS>", head)
        head = re.sub(r"\b(?:\d{1,3}\.){3}\d{1,3}\b", "<IP>", head)
        head = re.sub(r"\b0x[0-9a-fA-F]+\b", "<HEX>", head)
        head = re.sub(r"\b[0-9a-fA-F]{8,}\b", "<HEX>", head)
        head = re.sub(r"\b\d+\b", "<N>", head)
        head = re.sub(r"\s+", " ", head).strip()
        return head[:220]

    @classmethod
    def representative_samples(cls, samples, limit=None):
        """Select bounded, structurally diverse examples without random state.

        First keep one example per observed structural shape, then fill remaining
        slots evenly across the bounded input. Memory remains O(MAX_SAMPLES).
        """
        bounded = [x for x in samples[:cls.MAX_SAMPLES] if x and x.strip()]
        if not bounded:
            return []

        limit = min(int(limit or cls.MAX_AI_SAMPLES), len(bounded))
        selected = []
        selected_indexes = set()
        seen_shapes = set()

        for idx, event in enumerate(bounded):
            shape = cls._shape(event)
            if shape in seen_shapes:
                continue
            seen_shapes.add(shape)
            selected.append(event)
            selected_indexes.add(idx)
            if len(selected) >= limit:
                return selected

        if len(selected) < limit:
            # Deterministic even coverage across the remaining bounded window.
            if limit == 1:
                candidate_indexes = [0]
            else:
                candidate_indexes = [
                    round(i * (len(bounded) - 1) / (limit - 1))
                    for i in range(limit)
                ]
            for idx in candidate_indexes:
                if idx not in selected_indexes:
                    selected.append(bounded[idx])
                    selected_indexes.add(idx)
                    if len(selected) >= limit:
                        break

        if len(selected) < limit:
            for idx, event in enumerate(bounded):
                if idx not in selected_indexes:
                    selected.append(event)
                    if len(selected) >= limit:
                        break

        return selected

    @staticmethod
    def _repair_extra_numeric_header_group(policy, samples):
        """Repair one common AI structural error without hard-coding a log family.

        If a candidate regex matches none of the bounded samples and starts with a
        timestamp capture followed by two consecutive numeric captures, try removing
        either numeric capture. This handles cases where the model hallucinated an
        extra pid/tid-like column. The repair is accepted only when it materially
        improves bounded regex coverage; unreferenced attribute mappings are removed.
        """
        if not policy or not samples:
            return policy

        regex = policy.get("regex")
        if not isinstance(regex, str):
            return policy

        try:
            original = re.compile(regex)
        except re.error:
            return policy

        bounded = [x.splitlines()[0] for x in samples[:ParserPolicyDiscovery.MAX_SAMPLES] if x]
        if not bounded:
            return policy

        original_hits = sum(1 for line in bounded if original.match(line))
        if original_hits:
            return policy

        # Match adjacent simple named numeric groups separated by whitespace.
        numeric_group = (
            r"(?P<prefix>\(\?P<(?P<first>[A-Za-z_]\w*)>"
            r"(?:\\d\+|\[0-9\]\+)\)\s+)"
            r"(?P<second_group>\(\?P<(?P<second>[A-Za-z_]\w*)>"
            r"(?:\\d\+|\[0-9\]\+)\)\s+)"
        )
        pair = re.search(numeric_group, regex)
        if not pair:
            return policy

        candidates = []
        # Remove first or second numeric capture; let bounded coverage decide.
        for group_name, span_name in (
            (pair.group("first"), "prefix"),
            (pair.group("second"), "second_group"),
        ):
            start, end = pair.span(span_name)
            candidate_regex = regex[:start] + regex[end:]
            try:
                candidate_pattern = re.compile(candidate_regex)
            except re.error:
                continue
            hits = sum(1 for line in bounded if candidate_pattern.match(line))
            candidates.append((hits, group_name, candidate_regex, candidate_pattern))

        if not candidates:
            return policy

        hits, removed_group, repaired_regex, repaired_pattern = max(
            candidates, key=lambda item: item[0]
        )
        coverage = hits / len(bounded)
        if coverage < 0.90:
            return policy

        repaired = dict(policy)
        repaired["regex"] = repaired_regex

        # Remove references to the deleted capture. Canonical claimed groups are
        # nulled; event attributes using it are dropped. All other mappings remain.
        for key in (
            "timestamp_group",
            "severity_group",
            "message_group",
            "host_group",
            "service_group",
            "component_group",
            "trace_id_group",
            "span_id_group",
        ):
            if repaired.get(key) == removed_group:
                repaired[key] = None

        attrs = dict(repaired.get("attribute_groups") or {})

        # A structural repair proves only that one numeric column exists; it does
        # not prove the semantic label the AI assigned to that column. If the
        # repair had to choose between adjacent numeric captures (for example
        # pid/tid-like guesses), drop mappings for BOTH captures from attributes.
        # The raw value remains preserved in CanonicalEvent.raw and can be named
        # later by an enrichment/schema layer with independent evidence.
        ambiguous_numeric_groups = {
            pair.group("first"),
            pair.group("second"),
        }
        attrs = {
            name: ref
            for name, ref in attrs.items()
            if ref not in ambiguous_numeric_groups
        }
        repaired["attribute_groups"] = attrs

        # Defensive group-reference validation after the rewrite.
        valid_names = set(repaired_pattern.groupindex)
        for key in (
            "timestamp_group",
            "severity_group",
            "message_group",
            "host_group",
            "service_group",
            "component_group",
            "trace_id_group",
            "span_id_group",
        ):
            ref = repaired.get(key)
            if isinstance(ref, str) and not ref.isdigit() and ref not in valid_names:
                repaired[key] = None
        repaired["attribute_groups"] = {
            name: ref
            for name, ref in repaired["attribute_groups"].items()
            if not (isinstance(ref, str) and not ref.isdigit() and ref not in valid_names)
        }

        print(
            "[ParserPolicyDiscovery] deterministic repair | "
            f"removed_group={removed_group} | coverage={coverage:.2%}"
        )
        return repaired

    @staticmethod
    def _recover_leading_tokens(policy, samples, max_tokens=3):
        """Recover omitted transport/source prefix tokens without another AI call.

        The learned regex and all capture groups remain unchanged; only a bounded
        non-capturing prefix is added. A repair is accepted only with >=90% bounded
        coverage and only when the original candidate has zero matches.
        """
        if not policy or not samples:
            return policy
        regex = policy.get("regex")
        if not isinstance(regex, str) or not regex.startswith("^"):
            return policy
        try:
            original = re.compile(regex)
        except re.error:
            return policy
        bounded = [x.splitlines()[0] for x in samples[:ParserPolicyDiscovery.MAX_SAMPLES] if x]
        if not bounded or any(original.match(line) for line in bounded):
            return policy

        body = regex[1:]
        best = None
        for leading_tokens in range(1, max_tokens + 1):
            candidate_regex = rf"^(?:\S+\s+){{{leading_tokens}}}" + body
            try:
                candidate = re.compile(candidate_regex)
            except re.error:
                continue
            hits = sum(1 for line in bounded if candidate.match(line))
            coverage = hits / len(bounded)
            if best is None or coverage > best[0]:
                best = (coverage, leading_tokens, candidate_regex)

        if not best or best[0] < 0.90:
            return policy

        repaired = dict(policy)
        repaired["regex"] = best[2]
        print(
            "[ParserPolicyDiscovery] prefix recovery | "
            f"leading_tokens={best[1]} | coverage={best[0]:.2%}"
        )
        return repaired

    def discover(self, samples):
        if not samples:
            return None

        try:
            prompt = load_prompt(
                "parser_discovery.md",
                samples="\n---\n".join(
                    self.representative_samples(samples, self.MAX_AI_SAMPLES)
                ),
            )
            reply, _duration = call_ai_agent(
                "Parser_Discovery",
                (
                    "You learn deterministic parser policies for unknown log formats. "
                    "Return only the requested JSON object. Never invent fields."
                ),
                prompt,
                temperature=0.0,
                max_tokens=1200,
                response_format={"type": "json_object"},
            )
        except Exception:
            return None

        if not isinstance(reply, str):
            return None

        content = reply.strip()
        if content.startswith("Hata:") or content.startswith("API Hatası") or content.startswith("Bağlantı Hatası"):
            return None

        if content.startswith("```"):
            content = re.sub(r"^```(?:json)?\s*|\s*```$", "", content, flags=re.I)

        try:
            policy = json.loads(content)
        except (TypeError, json.JSONDecodeError):
            return None

        if not isinstance(policy, dict):
            return None

        regex = policy.get("regex")
        if not isinstance(regex, str) or not regex.startswith("^") or len(regex) > 2000:
            return None

        # Python's re module requires (?P<name>...) for named groups.
        # Some LLMs occasionally emit the PCRE/.NET-style (?<name>...).
        # Normalize only that named-capture syntax; do not rewrite any other
        # part of the learned regex.
        regex = re.sub(r"\(\?<([A-Za-z_]\w*)>", r"(?P<\1>", regex)
        policy["regex"] = regex

        try:
            pattern = re.compile(regex)
        except re.error as exc:
            print(f"[ParserPolicyDiscovery] rejected invalid Python regex: {exc}")
            return None

        # Normalize optional policy keys so the runtime contract is stable.
        for key in (
            "timestamp_group",
            "severity_group",
            "message_group",
            "host_group",
            "service_group",
            "component_group",
            "trace_id_group",
            "span_id_group",
        ):
            policy.setdefault(key, None)

        attribute_groups = policy.get("attribute_groups")
        if attribute_groups is None:
            policy["attribute_groups"] = {}
        elif not isinstance(attribute_groups, dict):
            return None

        # Reject policies that reference groups the regex does not expose.
        group_names = set(pattern.groupindex)
        group_count = pattern.groups

        def valid_group(ref):
            if ref is None:
                return True
            if isinstance(ref, int):
                return 1 <= ref <= group_count
            if isinstance(ref, str):
                if ref in group_names:
                    return True
                if ref.isdigit():
                    return 1 <= int(ref) <= group_count
            return False

        for key in (
            "timestamp_group",
            "severity_group",
            "message_group",
            "host_group",
            "service_group",
            "component_group",
            "trace_id_group",
            "span_id_group",
        ):
            if not valid_group(policy.get(key)):
                return None

        for attr_name, group_ref in policy["attribute_groups"].items():
            if not isinstance(attr_name, str) or not attr_name.strip():
                return None
            if not valid_group(group_ref):
                return None

        policy["_policy_schema_version"] = self.POLICY_SCHEMA_VERSION
        policy = self._repair_extra_numeric_header_group(policy, samples)
        policy = self._recover_leading_tokens(policy, samples)
        return policy

    @staticmethod
    def _timestamp_claim_is_plausible(raw_timestamp, parsed_timestamp):
        """Reject structurally matching timestamp claims with implausible semantics.

        Numeric captures are especially risky because sequence/event IDs can be
        interpreted as Unix epoch seconds.  We fail closed only when the parser
        actually produced an absolute datetime outside a deliberately broad
        operational range. Non-absolute/source-local timestamps remain allowed.
        """
        if raw_timestamp in (None, ""):
            return False

        if parsed_timestamp is None:
            # A source-local clock without enough timezone/year evidence is valid
            # structural evidence; TimestampNormalizer intentionally leaves it None.
            return True

        year = getattr(parsed_timestamp, "year", None)
        if year is None:
            return True

        # Broad enough for historical operational datasets while rejecting classic
        # small sequence IDs accidentally interpreted as Unix epoch timestamps.
        return 1980 <= int(year) <= 2100

    @classmethod
    def validate(cls, policy, samples, min_success=0.90):
        """Validate coverage, extraction, and the learned message boundary.

        A policy that claims structural header fields must also expose a real
        message_group. This prevents timestamp/severity/component/process-like
        header material from silently becoming CanonicalEvent.message.
        """
        if not policy or not samples:
            return False, 0.0

        if policy.get("_policy_schema_version") != cls.POLICY_SCHEMA_VERSION:
            return False, 0.0

        structural_keys = (
            "timestamp_group",
            "severity_group",
            "host_group",
            "service_group",
            "component_group",
            "trace_id_group",
            "span_id_group",
        )
        has_structural_claim = any(policy.get(key) for key in structural_keys)
        has_structural_claim = has_structural_claim or bool(policy.get("attribute_groups"))

        # If the policy understands a header, it must explicitly identify the body.
        # Falling back to the whole raw event would re-introduce that header.
        if has_structural_claim and not policy.get("message_group"):
            return False, 0.0

        try:
            from parser_layer.parsers.policy_parser import PolicyParser
            parser = PolicyParser(policy)
        except Exception:
            return False, 0.0

        success = 0
        total = len(samples)

        for event in samples:
            try:
                first_line = event.splitlines()[0] if event else ""
                match = parser.pattern.match(first_line)
                parsed = parser.parse(event)
            except Exception:
                parsed = None
                match = None

            if not parsed or not match:
                continue

            message = parsed.get("message")
            if not isinstance(message, str) or not message.strip():
                continue

            # A learned message_group must really be a body boundary. For policies
            # that claim header structure, the message may not start at column 0.
            message_group = policy.get("message_group")
            if message_group:
                try:
                    message_span = parser._group_span(match, message_group)
                except AttributeError:
                    try:
                        ref = message_group
                        if isinstance(ref, str) and ref.isdigit():
                            ref = int(ref)
                        message_span = match.span(ref)
                    except Exception:
                        message_span = None
                except Exception:
                    message_span = None

                if not message_span or message_span[0] < 0:
                    continue
                if has_structural_claim and message_span[0] == 0:
                    continue

                # The first-line capture must equal the first line of the parsed
                # message. Multiline continuation is appended by PolicyParser.
                try:
                    ref = message_group
                    if isinstance(ref, str) and ref.isdigit():
                        ref = int(ref)
                    captured_message = match.group(ref)
                except Exception:
                    captured_message = None
                if not captured_message or message.splitlines()[0] != captured_message:
                    continue

            # A timestamp group proves structural extraction, not necessarily an
            # absolute datetime. Validate the raw source capture.
            if policy.get("timestamp_group"):
                try:
                    raw_timestamp = parser._value(match, "timestamp_group")
                except Exception:
                    raw_timestamp = None
                if raw_timestamp in (None, ""):
                    continue
                if not cls._timestamp_claim_is_plausible(
                    raw_timestamp,
                    parsed.get("timestamp"),
                ):
                    continue

            claimed_pairs = (
                ("severity_group", "severity"),
                ("host_group", "host"),
                ("service_group", "service"),
                ("component_group", "component"),
                ("trace_id_group", "trace_id"),
                ("span_id_group", "span_id"),
            )
            if any(
                policy.get(policy_key) and parsed.get(output_key) in (None, "")
                for policy_key, output_key in claimed_pairs
            ):
                continue

            attrs = parsed.get("attributes")
            if not isinstance(attrs, dict):
                continue
            if any(
                name not in attrs or attrs[name] in (None, "")
                for name in (policy.get("attribute_groups") or {})
            ):
                continue

            success += 1

        ratio = success / total if total else 0.0
        return ratio >= min_success, ratio
