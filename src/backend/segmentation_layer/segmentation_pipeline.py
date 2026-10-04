# -*- coding: utf-8 -*-
"""Simple, streaming, AI-assisted multiline segmentation.

Architecture:
    small bounded sample -> structural signature -> verified policy registry
    -> on miss: AI discovery -> validate -> one-shot rediscovery/repair -> registry store
    -> streaming header classification -> multiline assembly -> logical events

The full log is never sent to the LLM and is never loaded into RAM.
"""
from __future__ import annotations

import hashlib
import os
import re
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

from ai_engine import call_ai_agent, load_prompt
from segmentation_layer.header_discovery import AIResponseError, HeaderDiscovery
from segmentation_layer.multiline_assembler import MultilineAssembler
from segmentation_layer.policy_registry import SegmentationPolicyRegistry
from segmentation_layer.regex_validator import RegexValidator


SEGMENTATION_SCHEMA_VERSION = "v44-header-signature-v5-stable-prefix"

DEFAULT_FALLBACK_REGEX = (
    r'^(?:<\d{1,3}>'
    r'|\S+\s+\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}(?:[.,]\d{3,6})?'
    r'|\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}'
    r'|\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2}'
    r'|\d{1,3}(?:\.\d{1,3}){3}\s+\S+\s+\S+\s+\['
    r'|(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2}\s+\d{2}:\d{2}:\d{2}'
    r'|[IWEF]\d{4}\s+\d{2}:\d{2}:\d{2}'
    r'|(?:INFO|ERROR|WARN|WARNING|DEBUG|TRACE|FATAL|CRITICAL)\s+\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}'
    r'|(?:INFO|ERROR|WARN|WARNING|DEBUG|TRACE|FATAL|CRITICAL)\s*[:|\-]'
    r')'
)


class StratifiedSampler:
    """Read a small representative sample from across a potentially huge file.

    Small files are sampled completely and exactly once. Large files use
    bounded contiguous windows distributed by byte position. Physical lines
    are deduplicated by byte offset.
    """

    def __init__(
        self,
        num_chunks: int = 10,
        chunk_size: int = 15,
        sample_budget: int = 150,
    ):
        self.num_chunks = max(1, int(num_chunks))
        self.chunk_size = max(1, int(chunk_size))
        self.sample_budget = max(20, int(sample_budget))

    def sample(self, file_path: str) -> Tuple[List[str], int]:
        file_size = os.path.getsize(file_path)
        if file_size <= 0:
            return [], 0

        # Small-file path: inspect only budget + 1 lines. If EOF is reached
        # within the budget, the complete file is the best possible sample.
        small_sample: List[str] = []
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            for _ in range(self.sample_budget + 1):
                raw = f.readline()
                if not raw:
                    break
                small_sample.append(raw.rstrip("\r\n"))

        if len(small_sample) <= self.sample_budget:
            return small_sample, file_size

        # Large-file path: preserve local multiline context while sampling
        # across the whole byte range. Deduplicate overlapping windows.
        samples: List[str] = []
        seen_offsets = set()
        step = max(file_size // self.num_chunks, 1)

        with open(file_path, "rb") as f:
            for i in range(self.num_chunks):
                byte_pos = min(i * step, max(file_size - 1, 0))
                f.seek(byte_pos)

                if byte_pos > 0:
                    f.readline()  # discard partial physical line

                for _ in range(self.chunk_size):
                    line_offset = f.tell()
                    raw = f.readline()
                    if not raw:
                        break
                    if line_offset in seen_offsets:
                        continue

                    seen_offsets.add(line_offset)
                    samples.append(
                        raw.decode("utf-8", errors="ignore").rstrip("\r\n")
                    )

                    if len(samples) >= self.sample_budget:
                        return samples, file_size

        return samples, file_size


class SegmentationPipeline:
    def __init__(
        self,
        discovery_sample_lines: int = 150,
        enable_ai: bool = True,
        registry_path: Optional[str] = None,
    ):
        self.enable_ai = bool(enable_ai)
        self.discovery_sample_lines = max(20, int(discovery_sample_lines))
        self.sampler = StratifiedSampler(
            num_chunks=10,
            chunk_size=max(1, self.discovery_sample_lines // 10),
            sample_budget=self.discovery_sample_lines,
        )
        self.discovery = HeaderDiscovery()
        self.validator = RegexValidator()
        self.assembler = MultilineAssembler()
        self.policy_registry = SegmentationPolicyRegistry(db_path=registry_path)
        self.last_result: Dict[str, Any] = {}

    def prepare(self, file_path: str, force_rediscovery: bool = False) -> Dict[str, Any]:
        # Keep discovery sampling and registry identity separate.
        # Stratified byte positions move when a file grows; using that sample
        # as the registry key made appended events cause needless new formats.
        samples, file_size = self.sampler.sample(file_path)
        signature_samples = self._signature_sample(file_path)
        signature = self._format_signature(signature_samples)

        # One registry lookup per source/session. A verified registry hit is
        # immediately reusable; do not rescan a 100 GB file just to revalidate
        # a policy that was already validated before it was stored.
        if not force_rediscovery:
            policy, level = self.policy_registry.get(signature)
            if policy is not None:
                result = self._result_from_registry(policy, signature, level, file_size)
                self.last_result = result
                print(
                    f"[SEGMENTATION] POLICY FOUND | registry={level} | "
                    f"signature={signature} | AI=0"
                )
                print(f"prepare_result={policy} | AI=0")
                return result
            

        ai_calls = 0
        token_usage: Dict[str, Any] = {}
        candidate_regex: Optional[str] = None
        confidence = 0.0
        source = "fallback"
        reason = ""

        if self.enable_ai:
            print(
                f"[SEGMENTATION] NEW FORMAT | signature={signature} | "
                f"AI discovery on {len(samples)} sampled lines"
            )
            try:
                ai_calls = 1
                discovered = self.discovery.discover(samples)
                candidate_regex = discovered["event_header_regex"]
                confidence = float(discovered.get("confidence", 0.0) or 0.0)
                token_usage = dict(discovered.get("_token_usage") or {})
                source = "ai-discovery"
                reason = "AI candidate"
                # Diagnostic only: expose the exact candidate passed to the
                # deterministic validator. repr() makes escaping visible so
                # malformed/double-escaped regexes can be diagnosed precisely.
                print(f"[SEGMENTATION] AI REGEX | {candidate_regex!r}")
            except AIResponseError as exc:
                token_usage = dict(exc.usage or {})
                reason = f"AI discovery failed: {exc}"
                print(f"[SEGMENTATION] AI discovery failed -> deterministic fallback | {exc}")
        else:
            reason = "AI disabled"

        # AI output is only a candidate. Deterministic validation is the gate.
        if candidate_regex:
            validation = self._admit_candidate(
                file_path=file_path,
                samples=samples,
                candidate_regex=candidate_regex,
                label="AI",
            )

            if validation.get("accepted"):
                result = self._build_result(
                    signature=signature,
                    file_size=file_size,
                    regex=candidate_regex,
                    confidence=confidence,
                    source=source,
                    reason=reason,
                    validation=validation,
                    ai_calls=ai_calls,
                    token_usage=token_usage,
                )
                self.policy_registry.put(signature, result)
                self.last_result = result
                print(f"[SEGMENTATION] VERIFIED POLICY STORED | signature={signature}")
                return result

            reason = "AI candidate rejected by deterministic validation"

            # If the AI candidate matched zero real headers, first try one cheap,
            # deterministic recovery for a common structural mistake: the model
            # may have learned a valid suffix of the header but skipped one or two
            # leading source columns. Candidate selection is scored only on the
            # already-bounded discovery sample; only the best recovered candidate
            # is sent through the normal full deterministic validator.
            #
            # This is format-agnostic: no BGL/Android/Apache tokens are known here.
            if int(validation.get("header_matches", 0) or 0) == 0:
                recovered = self._recover_leading_prefix(
                    samples=samples,
                    candidate_regex=candidate_regex,
                    max_leading_tokens=2,
                )
                if recovered is not None:
                    recovered_regex, recovery_meta = recovered
                    print(
                        f"[SEGMENTATION] PREFIX RECOVERY | "
                        f"leading_tokens={recovery_meta['leading_tokens']} | "
                        f"sample_matches={recovery_meta['sample_matches']}/"
                        f"{recovery_meta['sample_total']} | "
                        f"regex={recovered_regex!r}"
                    )

                    recovered_validation = self._admit_candidate(
                        file_path=file_path,
                        samples=samples,
                        candidate_regex=recovered_regex,
                        label="PREFIX RECOVERY",
                    )

                    if recovered_validation.get("accepted"):
                        result = self._build_result(
                            signature=signature,
                            file_size=file_size,
                            regex=recovered_regex,
                            confidence=confidence,
                            source="deterministic-prefix-recovery",
                            reason="AI candidate recovered by deterministic leading-prefix correction",
                            validation=recovered_validation,
                            ai_calls=ai_calls,
                            token_usage=token_usage,
                        )
                        self.policy_registry.put(signature, result)
                        self.last_result = result
                        print(
                            f"[SEGMENTATION] VERIFIED PREFIX-RECOVERED POLICY STORED | "
                            f"signature={signature}"
                        )
                        return result

                    # A partial recovery is still valuable evidence. Feed that
                    # candidate into the existing one-shot AI repair path instead
                    # of doing clean rediscovery from a completely zero-match regex.
                    if int(recovered_validation.get("header_matches", 0) or 0) > 0:
                        candidate_regex = recovered_regex
                        validation = recovered_validation
                        reason = (
                            "Deterministic prefix recovery produced partial header "
                            "coverage; one bounded AI repair is allowed"
                        )

            # One bounded, failure-directed repair attempt. This is deliberately
            # not a retry loop: discovery gets at most one correction using
            # deterministic validation evidence plus a small representative set
            # of lines that the rejected regex failed to match.
            if self.enable_ai and ai_calls == 1:
                guard_meta = validation.get("partial_miss_guard") or {}
                guard_audit_lines = list(guard_meta.get("audit_lines") or [])
                repair_source = guard_audit_lines if guard_audit_lines else list(samples)
                repair_evidence = self._repair_samples(repair_source, candidate_regex)
                repair_samples = (
                    repair_evidence.get("missed", [])
                    + repair_evidence.get("matched", [])
                )
                if repair_samples:
                    evidence_source = (
                        "bounded-real-source"
                        if guard_audit_lines
                        else "discovery-sample"
                    )
                    print(
                        f"[SEGMENTATION] AI CORRECTION | "
                        f"evidence_lines={len(repair_samples)} | "
                        f"missed={len(repair_evidence.get('missed', []))} | "
                        f"matched={len(repair_evidence.get('matched', []))} | "
                        f"sampling=structural-diverse | "
                        f"source={evidence_source} | max_attempts=1"
                    )
                    try:
                        correction_mode = (
                            "rediscovery"
                            if int(validation.get("header_matches", 0) or 0) == 0
                            else "repair"
                        )
                        print(
                            f"[SEGMENTATION] AI CORRECTION MODE | "
                            f"{correction_mode.upper()}"
                        )
                        repaired = self._repair_candidate(
                            candidate_regex=candidate_regex,
                            validation=validation,
                            samples=repair_samples,
                            mode=correction_mode,
                            evidence=repair_evidence,
                        )
                        ai_calls += 1
                        repaired_regex = str(repaired["event_header_regex"])
                        repaired_confidence = float(
                            repaired.get("confidence", confidence) or 0.0
                        )
                        token_usage = self._merge_token_usage(
                            token_usage,
                            dict(repaired.get("_token_usage") or {}),
                        )
                        print(f"[SEGMENTATION] AI {correction_mode.upper()} REGEX | {repaired_regex!r}")

                        repair_validation = self._admit_candidate(
                            file_path=file_path,
                            samples=samples,
                            candidate_regex=repaired_regex,
                            label=f"AI {correction_mode.upper()}",
                        )

                        # The repair model may correctly generalize the candidate
                        # grammar while accidentally dropping a deterministic
                        # leading-prefix correction discovered earlier. Preserve
                        # the same format-agnostic recovery opportunity after the
                        # single AI correction. This remains bounded: alternatives
                        # are scored only on the discovery sample and exactly one
                        # best candidate is sent through the normal deterministic
                        # admission gate.
                        if (
                            not repair_validation.get("accepted")
                            and int(repair_validation.get("header_matches", 0) or 0) == 0
                        ):
                            repaired_recovery = self._recover_leading_prefix(
                                samples=samples,
                                candidate_regex=repaired_regex,
                                max_leading_tokens=2,
                            )
                            if repaired_recovery is not None:
                                recovered_repair_regex, recovered_repair_meta = repaired_recovery
                                print(
                                    f"[SEGMENTATION] REPAIR PREFIX RECOVERY | "
                                    f"leading_tokens={recovered_repair_meta['leading_tokens']} | "
                                    f"sample_matches={recovered_repair_meta['sample_matches']}/"
                                    f"{recovered_repair_meta['sample_total']} | "
                                    f"regex={recovered_repair_regex!r}"
                                )

                                recovered_repair_validation = self._admit_candidate(
                                    file_path=file_path,
                                    samples=samples,
                                    candidate_regex=recovered_repair_regex,
                                    label="REPAIR PREFIX RECOVERY",
                                )

                                # Continue the existing post-repair flow using
                                # the recovered candidate. If it is only partial,
                                # the existing structural generalizer below gets
                                # its bounded matched/missed real-source evidence.
                                if (
                                    recovered_repair_validation.get("accepted")
                                    or int(
                                        recovered_repair_validation.get(
                                            "header_matches", 0
                                        )
                                        or 0
                                    )
                                    > 0
                                ):
                                    repaired_regex = recovered_repair_regex
                                    repair_validation = recovered_repair_validation

                        if repair_validation.get("accepted"):
                            result = self._build_result(
                                signature=signature,
                                file_size=file_size,
                                regex=repaired_regex,
                                confidence=repaired_confidence,
                                source=f"ai-{correction_mode}",
                                reason=f"AI {correction_mode} accepted after deterministic validation failure",
                                validation=repair_validation,
                                ai_calls=ai_calls,
                                token_usage=token_usage,
                            )
                            self.policy_registry.put(signature, result)
                            self.last_result = result
                            print(
                                f"[SEGMENTATION] VERIFIED REPAIRED POLICY STORED | "
                                f"signature={signature}"
                            )
                            return result

                        # The single AI correction is exhausted. Before falling
                        # back to broad token-prefix generalization, preserve the
                        # learned grammar and relax only fixed-width numeric
                        # quantifiers that bounded matched/missed evidence proves
                        # are too strict (for example zero-padded vs non-padded
                        # timestamp components). Every produced candidate still
                        # goes through the complete deterministic admission gate.
                        repair_guard = repair_validation.get("partial_miss_guard") or {}
                        repair_audit_lines = list(repair_guard.get("audit_lines") or [])

                        # Numeric-width recovery is iterative because the first
                        # bounded guard may expose only one of several independent
                        # zero-padding variations. After each rejected generalized
                        # candidate, feed THAT candidate's new bounded guard
                        # evidence into the next round. Keep the loop strictly
                        # bounded and require every candidate to pass the normal
                        # deterministic admission gate before it can be stored.
                        width_candidate = repaired_regex
                        width_evidence = repair_audit_lines
                        seen_width_candidates = {repaired_regex}

                        for width_round in range(1, 5):
                            width_generalized_regex = self._generalize_numeric_widths(
                                candidate_regex=width_candidate,
                                evidence_lines=width_evidence,
                            )
                            if (
                                not width_generalized_regex
                                or width_generalized_regex == width_candidate
                                or width_generalized_regex in seen_width_candidates
                            ):
                                break

                            seen_width_candidates.add(width_generalized_regex)
                            print(
                                f"[SEGMENTATION] NUMERIC WIDTH GENERALIZATION "
                                f"ROUND {width_round} | "
                                f"regex={width_generalized_regex!r}"
                            )
                            width_generalized_validation = self._admit_candidate(
                                file_path=file_path,
                                samples=samples,
                                candidate_regex=width_generalized_regex,
                                label=(
                                    f"NUMERIC WIDTH GENERALIZATION ROUND {width_round}"
                                ),
                            )
                            if width_generalized_validation.get("accepted"):
                                result = self._build_result(
                                    signature=signature,
                                    file_size=file_size,
                                    regex=width_generalized_regex,
                                    confidence=repaired_confidence,
                                    source="deterministic-numeric-width-generalization",
                                    reason=(
                                        "Rejected AI repair generalized only fixed-width "
                                        "numeric quantifiers proven too strict by bounded "
                                        "matched/missed real-source evidence"
                                    ),
                                    validation=width_generalized_validation,
                                    ai_calls=ai_calls,
                                    token_usage=token_usage,
                                )
                                self.policy_registry.put(signature, result)
                                self.last_result = result
                                print(
                                    f"[SEGMENTATION] VERIFIED NUMERIC-WIDTH-GENERALIZED "
                                    f"POLICY STORED | signature={signature}"
                                )
                                return result

                            # Crucial: use the NEW guard evidence from this rejected
                            # candidate, not the stale evidence from the AI repair.
                            next_guard = (
                                width_generalized_validation.get("partial_miss_guard")
                                or {}
                            )
                            next_evidence = list(next_guard.get("audit_lines") or [])
                            if not next_evidence:
                                break

                            width_candidate = width_generalized_regex
                            width_evidence = next_evidence

                        # If grammar-preserving width relaxation is insufficient,
                        # retain the existing broader structural-prefix recovery.
                        generalized_regex = self._generalize_structural_prefix(
                            candidate_regex=repaired_regex,
                            evidence_lines=repair_audit_lines,
                        )
                        if generalized_regex and generalized_regex != repaired_regex:
                            print(
                                f"[SEGMENTATION] STRUCTURAL GENERALIZATION | "
                                f"regex={generalized_regex!r}"
                            )
                            generalized_validation = self._admit_candidate(
                                file_path=file_path,
                                samples=samples,
                                candidate_regex=generalized_regex,
                                label="STRUCTURAL GENERALIZATION",
                            )
                            if generalized_validation.get("accepted"):
                                result = self._build_result(
                                    signature=signature,
                                    file_size=file_size,
                                    regex=generalized_regex,
                                    confidence=repaired_confidence,
                                    source="deterministic-structural-generalization",
                                    reason=(
                                        "Rejected AI repair generalized from bounded "
                                        "matched/missed real-source header evidence"
                                    ),
                                    validation=generalized_validation,
                                    ai_calls=ai_calls,
                                    token_usage=token_usage,
                                )
                                self.policy_registry.put(signature, result)
                                self.last_result = result
                                print(
                                    f"[SEGMENTATION] VERIFIED STRUCTURALLY-GENERALIZED "
                                    f"POLICY STORED | signature={signature}"
                                )
                                return result

                        candidate_regex = repaired_regex
                        confidence = repaired_confidence
                        reason = "AI repair rejected by deterministic validation"
                    except AIResponseError as exc:
                        ai_calls += 1
                        token_usage = self._merge_token_usage(
                            token_usage,
                            dict(exc.usage or {}),
                        )
                        reason = f"AI repair failed: {exc}"
                        print(
                            f"[SEGMENTATION] AI repair failed -> "
                            f"deterministic fallback | {exc}"
                        )

        # Safe deterministic fallback. It is also stored only if validation
        # proves that it works for this format.
        fallback_validation = self._admit_candidate(
            file_path=file_path,
            samples=samples,
            candidate_regex=DEFAULT_FALLBACK_REGEX,
            label="FALLBACK",
        )
        if fallback_validation.get("accepted"):
            result = self._build_result(
                signature=signature,
                file_size=file_size,
                regex=DEFAULT_FALLBACK_REGEX,
                confidence=0.0,
                source="deterministic-fallback",
                reason=reason or "fallback",
                validation=fallback_validation,
                ai_calls=ai_calls,
                token_usage=token_usage,
            )
            self.policy_registry.put(signature, result)
            self.last_result = result
            print(f"[SEGMENTATION] VERIFIED FALLBACK STORED | signature={signature}")
            return result

        result = {
            "verified": False,
            "format_signature": signature,
            "file_size_bytes": file_size,
            "regex": candidate_regex or DEFAULT_FALLBACK_REGEX,
            "source": "unverified",
            "reason": reason or "No candidate passed validation",
            "coverage_score": fallback_validation.get("coverage_score", 0.0),
            "validation": fallback_validation,
            "registry_hit": "miss",
            "ai_calls": ai_calls,
            "ai_call_total": ai_calls,
            "token_usage": token_usage,
        }
        self.last_result = result
        return result

    def iter_events(self, file_path: str, force_rediscovery: bool = False) -> Iterator[str]:
        result = self.prepare(file_path, force_rediscovery=force_rediscovery)
        if not result.get("verified"):
            
            event_header_regex = str(result["regex"])

            print("=== SEGMENTATION DEBUG ===", flush=True)
            print("file_path:", file_path, flush=True)
            print("regex repr:", repr(event_header_regex), flush=True)

            rx = re.compile(event_header_regex)

            physical_lines = 0
            regex_matches = 0
            first_matches = []

            with open(file_path, "r", encoding="utf-8", errors="replace") as f:
                for i, line in enumerate(f, 1):
                    physical_lines += 1

                    if rx.match(line):
                        regex_matches += 1

                        if len(first_matches) < 5:
                            first_matches.append((i, line[:150]))

            print("physical_lines:", physical_lines, flush=True)
            print("regex_matches:", regex_matches, flush=True)
            print("first_matches:", first_matches, flush=True)
            print("=== SEGMENTATION DEBUG COMPLETE ===", flush=True)
            

            raise RuntimeError(
                "Segmentation verification failed; refusing to use an unverified policy. "
                + str(result.get("reason", "unknown reason"))
            )
        yield from self.assembler.iter_events(file_path, str(result["regex"]))

    def process_file(self, file_path: str, force_rediscovery: bool = False) -> Iterator[str]:
        yield from self.iter_events(file_path, force_rediscovery=force_rediscovery)

    def _validate(self, file_path: str, regex: str) -> Dict[str, Any]:
        return self.validator.validate(
            file_path,
            regex,
        )

    @staticmethod
    def _partial_miss_guard(
        *,
        samples: Iterable[str],
        candidate_regex: str,
        min_matched_headers: int = 3,
        suspicious_similarity: float = 0.75,
    ) -> Dict[str, Any]:
        """Detect likely missed headers among candidate-regex non-matches.

        This is an admission guard, not a format parser. It stays generic by
        comparing the leading token structure of matched and unmatched raw lines.

        A candidate is suspicious when:
        - it matches enough bounded-sample lines to establish a header-family
          structural reference, and
        - one or more unmatched lines have a leading token structure very similar
          to known matched headers.

        This catches over-specific AI regexes (for example, a numeric field that
        was learned as unsigned even though rare signed values exist) without
        hard-coding any log format or field value.
        """
        raw = [str(line).rstrip("\r\n") for line in samples if str(line).strip()]
        if not raw:
            return {
                "suspicious": False,
                "matched": 0,
                "unmatched": 0,
                "similar_unmatched": 0,
                "match_ratio": 0.0,
            }

        try:
            compiled = re.compile(candidate_regex)
        except re.error:
            return {
                "suspicious": False,
                "matched": 0,
                "unmatched": len(raw),
                "similar_unmatched": 0,
                "match_ratio": 0.0,
            }

        matched = [line for line in raw if compiled.match(line)]
        unmatched = [line for line in raw if not compiled.match(line)]
        match_ratio = len(matched) / len(raw)

        # Do not require a high overall match ratio here. A badly over-specific
        # AI regex can still describe one legitimate header family very well
        # while excluding other legitimate families (HPC is a concrete example).
        # The guard's job is precisely to detect that case. We only need a small
        # stable set of matched headers as a structural reference.
        if len(matched) < min_matched_headers or not unmatched:
            return {
                "suspicious": False,
                "matched": len(matched),
                "unmatched": len(unmatched),
                "similar_unmatched": 0,
                "match_ratio": match_ratio,
            }

        def token_shape(token: str) -> str:
            # Preserve sign/punctuation shape while abstracting literal values.
            out: List[str] = []
            previous = None
            for ch in token[:48]:
                if ch.isdigit():
                    kind = "D"
                elif ch.isalpha():
                    kind = "A"
                else:
                    kind = ch
                if kind != previous:
                    out.append(kind)
                    previous = kind
            return "".join(out) or "<EMPTY>"

        def shape(line: str) -> List[str]:
            # Event boundary grammar is dominated by leading fields.
            return [token_shape(tok) for tok in line.lstrip().split()[:8]]

        matched_shapes = [shape(line) for line in matched]

        def similarity(a: List[str], b: List[str]) -> float:
            width = max(len(a), len(b), 1)
            same = sum(
                1
                for i in range(min(len(a), len(b)))
                if a[i] == b[i]
            )
            return same / width

        similar_unmatched = 0
        examples: List[str] = []
        for line in unmatched:
            u_shape = shape(line)
            best = max(
                (similarity(u_shape, m_shape) for m_shape in matched_shapes),
                default=0.0,
            )
            if best >= suspicious_similarity:
                similar_unmatched += 1
                if len(examples) < 6:
                    examples.append(line)

        return {
            "suspicious": similar_unmatched > 0,
            "matched": len(matched),
            "unmatched": len(unmatched),
            "similar_unmatched": similar_unmatched,
            "match_ratio": match_ratio,
            "examples": examples,
        }

    def _admit_candidate(
        self,
        *,
        file_path: str,
        samples: Iterable[str],
        candidate_regex: str,
        label: str,
    ) -> Dict[str, Any]:
        """Run the complete deterministic admission gate for any candidate.

        Every candidate, regardless of provenance (AI discovery, AI repair,
        rediscovery, prefix recovery, or deterministic fallback), must pass the
        same validation and partial-miss structural guard before it can be stored.
        """
        validation = self._validate(file_path, candidate_regex)
        self._print_validation(label, validation)

        partial_guard = self._partial_miss_guard(
            samples=samples,
            candidate_regex=candidate_regex,
        )

        # The 150-line discovery sample can miss a rare header variant. When the
        # validator itself exposes a non-zero header deficit, audit a bounded set
        # of matched and unmatched lines from the real source before admission.
        # This remains streaming and bounded; the file is never loaded into RAM.
        header_matches = int(validation.get("header_matches", 0) or 0)

        # Always collect bounded real-source contrast evidence for a partial
        # candidate. The discovery sample may already prove the candidate is
        # suspicious, but it is still only a sample and may omit the rare variant
        # that explains the real full-file miss. The same bounded evidence is
        # reused by the one allowed AI correction.
        if header_matches > 0:
            audit_lines = self._bounded_candidate_audit_lines(
                file_path=file_path,
                candidate_regex=candidate_regex,
            )
            if audit_lines:
                full_guard = self._partial_miss_guard(
                    samples=audit_lines,
                    candidate_regex=candidate_regex,
                )

                # Preserve real-source evidence regardless of which guard first
                # detected the problem. This guarantees repair uses source=
                # bounded-real-source whenever real matched/unmatched contrast
                # exists.
                if partial_guard.get("suspicious") or full_guard.get("suspicious"):
                    chosen_guard = (
                        full_guard
                        if full_guard.get("suspicious")
                        else partial_guard
                    )
                    partial_guard = dict(chosen_guard)
                    partial_guard["audit_lines"] = list(audit_lines)

        validation = dict(validation)
        validation["partial_miss_guard"] = partial_guard

        if partial_guard.get("suspicious"):
            print(
                f"[SEGMENTATION] PARTIAL MISS GUARD | "
                f"candidate={label} | "
                f"matched={partial_guard['matched']} | "
                f"unmatched={partial_guard['unmatched']} | "
                f"similar_unmatched={partial_guard['similar_unmatched']} | "
                f"match_ratio={partial_guard['match_ratio']:.2%} | "
                f"accepted=False"
            )
            validation["accepted"] = False
            validation["partial_miss_suspicious"] = True

        return validation

    @staticmethod
    def _bounded_candidate_audit_lines(
        *,
        file_path: str,
        candidate_regex: str,
        max_matched: int = 48,
        max_unmatched: int = 48,
    ) -> List[str]:
        """Collect bounded real-source evidence for the admission guard.

        Discovery sampling is intentionally small, so rare structural variants
        can be absent from it. This helper streams the source and retains only a
        small matched/unmatched contrast set. It contains no format-specific
        knowledge and never sends the full file to AI.
        """
        try:
            compiled = re.compile(candidate_regex)
        except re.error:
            return []

        matched: List[str] = []
        unmatched: List[str] = []

        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            for raw in f:
                line = raw.rstrip("\r\n")
                if not line.strip():
                    continue

                if compiled.match(line):
                    if len(matched) < max_matched:
                        matched.append(line)
                elif len(unmatched) < max_unmatched:
                    unmatched.append(line)

                if len(matched) >= max_matched and len(unmatched) >= max_unmatched:
                    break

        if not matched or not unmatched:
            return []

        return matched + unmatched

    @staticmethod
    def _generalize_numeric_widths(
        *,
        candidate_regex: str,
        evidence_lines: Iterable[str],
        min_matched_headers: int = 3,
        min_improvement: int = 1,
    ) -> Optional[str]:
        """Relax only evidence-proven fixed-width numeric quantifiers.

        This is deliberately narrower than token-prefix structural
        generalization. It keeps the AI-learned anchors, delimiters and field
        order intact and considers only ``\\d{N}`` atoms with N > 1.

        For each fixed-width numeric atom, a candidate ``\\d{1,N}`` relaxation
        is tested against the same bounded matched/missed real-source evidence.
        A relaxation is retained only when it increases matches and never loses
        a line already matched by the current candidate. Changes are therefore
        data-driven, bounded and format-agnostic.

        The returned regex is still only a candidate; the caller must run the
        normal full deterministic admission gate before storing it.
        """
        raw = [
            str(line).rstrip("\r\n")
            for line in evidence_lines
            if str(line).strip()
        ]
        if not raw:
            return None

        current = str(candidate_regex or "").strip()
        if not current:
            return None

        try:
            current_compiled = re.compile(current)
        except re.error:
            return None

        current_matched = {
            idx for idx, line in enumerate(raw) if current_compiled.match(line)
        }
        if len(current_matched) < max(1, int(min_matched_headers)):
            return None

        # Evaluate fixed-width atoms iteratively. A single-field relaxation can
        # show zero immediate gain when missed records vary in several fields at
        # once (e.g. hour + minute + second all lose zero-padding). Therefore,
        # besides individually improving atoms, try the cumulative relaxation of
        # all still-fixed numeric atoms. It is accepted only if bounded evidence
        # proves a strict gain and every previously matched line remains matched.
        changed = False
        fixed_digit = re.compile(r"\\d\{(?P<width>[2-9]\d*)\}")

        while True:
            atoms = list(fixed_digit.finditer(current))
            if not atoms:
                break

            best_trial = None
            best_matches = current_matched
            best_gain = 0

            # First prefer the narrowest possible change: one atom.
            for atom in atoms:
                width = int(atom.group("width"))
                relaxed_atom = rf"\d{{1,{width}}}"
                trial = current[: atom.start()] + relaxed_atom + current[atom.end() :]
                try:
                    compiled = re.compile(trial)
                except re.error:
                    continue
                matched = {idx for idx, line in enumerate(raw) if compiled.match(line)}
                if not current_matched.issubset(matched):
                    continue
                gain = len(matched) - len(current_matched)
                if gain > best_gain:
                    best_trial, best_matches, best_gain = trial, matched, gain

            if best_trial is not None and best_gain >= max(1, int(min_improvement)):
                current = best_trial
                current_matched = best_matches
                changed = True
                continue

            # No single atom helps. Test the cumulative hypothesis because real
            # records may change several adjacent fixed-width numeric fields
            # simultaneously. This still preserves all non-numeric grammar.
            pieces = []
            pos = 0
            for atom in atoms:
                pieces.append(current[pos:atom.start()])
                width = int(atom.group("width"))
                pieces.append(rf"\d{{1,{width}}}")
                pos = atom.end()
            pieces.append(current[pos:])
            trial = "".join(pieces)

            try:
                compiled = re.compile(trial)
            except re.error:
                break

            matched = {idx for idx, line in enumerate(raw) if compiled.match(line)}
            gain = len(matched) - len(current_matched)
            if (
                current_matched.issubset(matched)
                and gain >= max(1, int(min_improvement))
            ):
                current = trial
                current_matched = matched
                changed = True
                continue

            break

        return current if changed else None

    @staticmethod
    def _generalize_structural_prefix(
        *,
        candidate_regex: str,
        evidence_lines: Iterable[str],
        max_tokens: int = 10,
    ) -> Optional[str]:
        """Generalize an over-specific token-prefix regex from bounded evidence.

        The candidate itself determines how many leading whitespace-delimited
        fields constitute its boundary prefix. We then infer only primitive token
        classes from real matched/missed evidence. This deliberately avoids
        enumerating literal hosts, components, categories, or message values.

        It is conservative: without both matched and unmatched evidence, or when
        the candidate's consumed prefix width cannot be inferred consistently,
        no generalized regex is produced. The caller must still run the complete
        deterministic admission gate before storing the result.
        """
        raw = [
            str(line).rstrip("\r\n")
            for line in evidence_lines
            if str(line).strip()
        ]
        if not raw:
            return None

        try:
            compiled = re.compile(candidate_regex)
        except re.error:
            return None

        matched = []
        unmatched = []
        consumed_widths: List[int] = []

        for line in raw:
            match = compiled.match(line)
            if match:
                matched.append(line)
                consumed = match.group(0).strip()
                width = len(consumed.split())
                if 1 <= width <= max_tokens:
                    consumed_widths.append(width)
            else:
                unmatched.append(line)

        if len(matched) < 3 or not unmatched or not consumed_widths:
            return None

        # Require a stable candidate prefix width. This prevents a regex with
        # variable-length alternatives from being generalized ambiguously.
        width_counts: Dict[int, int] = {}
        for width in consumed_widths:
            width_counts[width] = width_counts.get(width, 0) + 1
        prefix_width, prefix_count = max(
            width_counts.items(),
            key=lambda item: item[1],
        )
        if prefix_count / len(consumed_widths) < 0.80:
            return None

        rows: List[List[str]] = []
        for line in raw:
            tokens = line.lstrip().split()
            if len(tokens) >= prefix_width:
                rows.append(tokens[:prefix_width])

        if len(rows) < 4:
            return None

        def token_pattern(values: List[str]) -> str:
            # Infer only broad lexical classes. Signed integer handling is
            # intentionally generic and is learned from evidence, not a format.
            if all(re.fullmatch(r"\d+", value) for value in values):
                return r"\d+"
            if all(re.fullmatch(r"[+-]?\d+", value) for value in values):
                return r"[+-]?\d+"
            if all(
                re.fullmatch(r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)", value)
                for value in values
            ):
                return r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)"
            if all(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_.:-]*", value) for value in values):
                return r"\S+"
            return r"\S+"

        columns = list(zip(*rows))
        parts = [token_pattern(list(values)) for values in columns]
        if not parts:
            return None

        return r"^" + r"\s+".join(parts) + r"\s+"

    @staticmethod
    def _recover_leading_prefix(
        *,
        samples: Iterable[str],
        candidate_regex: str,
        max_leading_tokens: int = 2,
    ) -> Optional[tuple[str, Dict[str, int]]]:
        """Recover a candidate that appears to start after leading source fields.

        The AI sometimes recognizes a stable timestamp/date suffix but omits an
        event-type or source column at the beginning of each physical record.
        Rather than hard-code any format, try generic ``\\S+\\s+`` prefixes.

        To keep this safe for very large files, all alternatives are scored only
        against the small discovery sample. The caller validates exactly one best
        candidate against the real file using the existing deterministic gate.
        """
        raw_samples = [
            str(line).rstrip("\r\n")
            for line in samples
            if str(line).strip()
        ]
        if not raw_samples:
            return None

        body = str(candidate_regex or "").strip()
        if not body:
            return None

        # Preserve candidate semantics while replacing only its start anchor.
        if body.startswith("^"):
            body = body[1:]
        body = body.lstrip()
        if not body:
            return None

        best_regex: Optional[str] = None
        best_matches = 0
        best_tokens = 0

        for leading_tokens in range(1, max(1, int(max_leading_tokens)) + 1):
            prefix = rf"^(?:\S+\s+){{{leading_tokens}}}"
            recovered_regex = prefix + body
            try:
                compiled = re.compile(recovered_regex)
            except re.error:
                continue

            matches = sum(1 for line in raw_samples if compiled.match(line))
            if matches > best_matches:
                best_regex = recovered_regex
                best_matches = matches
                best_tokens = leading_tokens

        # Zero improvement is not recovery; leave clean rediscovery untouched.
        if best_regex is None or best_matches <= 0:
            return None

        return best_regex, {
            "leading_tokens": best_tokens,
            "sample_matches": best_matches,
            "sample_total": len(raw_samples),
        }

    def _repair_candidate(
        self,
        *,
        candidate_regex: str,
        validation: Dict[str, Any],
        samples: Iterable[str],
        mode: str = "repair",
        evidence: Optional[Dict[str, List[str]]] = None,
    ) -> Dict[str, Any]:
        """Correct one rejected candidate exactly once.

        Zero header matches means the previous structural assumption is wrong,
        so the model must rediscover from raw lines instead of anchoring on the
        rejected regex. Partial matches use contrastive repair.
        """
        samples_text = HeaderDiscovery._format_samples(
            samples,
            max_lines=30,
            max_line_length=500,
        )

        evidence = evidence or {}
        missed_lines = list(evidence.get("missed") or [])
        matched_lines = list(evidence.get("matched") or [])
        missed_text = HeaderDiscovery._format_samples(
            missed_lines,
            max_lines=24,
            max_line_length=500,
        )
        matched_text = HeaderDiscovery._format_samples(
            matched_lines,
            max_lines=8,
            max_line_length=500,
        )

        parser_validation = validation.get("parser_validation") or {}
        system_prompt = (
            load_prompt("common_system.md")
            + "\n\n"
            + load_prompt("segmentation_discovery.md")
        )
        mode = "rediscovery" if mode == "rediscovery" else "repair"
        common_rules = (
            "Return the same JSON object schema required by the segmentation "
            "discovery prompt. The regex is for EVENT BOUNDARY detection, not full "
            "log parsing. Prefer the shortest stable structural prefix that reliably "
            "identifies the beginning of an event. Do not require message text or "
            "late fields such as component/source/severity when an earlier stable "
            "prefix is sufficient. Do not invent transport/sample prefixes. Keep "
            "the regex anchored at ^.\n"
        )

        if mode == "rediscovery":
            # Clean-room rediscovery: the rejected regex is intentionally NOT
            # shown to the model. With zero matches it is not useful evidence;
            # including it can anchor the model to the same wrong grammar.
            user_prompt = (
                "CLEAN REDISCOVERY REQUIRED. A previous segmentation candidate "
                "matched zero real source lines, so discard all previous grammar "
                "assumptions and infer the event-header structure only from the "
                "RAW source lines below.\n\n"
                + common_rules
                + "Important: inspect each RAW line from its FIRST CHARACTER. "
                "Do not skip or silently discard an initial token/column even if "
                "it looks like '-', a label, event type, category, or identifier. "
                "Determine which leading columns are actually part of the source "
                "record. Prefer a minimal stable prefix that generalizes across "
                "different first-token values.\n\n"
                + "Representative RAW source lines (UNTRUSTED DATA):\n"
                + samples_text
            )
        else:
            # Partial-match repair is contrastive, so the rejected regex and
            # validation evidence remain useful here.
            user_prompt = (
                "The previous candidate matched some source lines but failed "
                "deterministic validation. REPAIR it by comparing matched and "
                "unmatched RAW lines and generalizing only the missing structural "
                "variation. Do not overfit individual messages.\n\n"
                + common_rules
                + f"Rejected regex (FAILURE EVIDENCE ONLY):\n{candidate_regex}\n\n"
                + "Deterministic validation evidence:\n"
                + f"- header_matches: {validation.get('header_matches', 0)}\n"
                + f"- event_count: {validation.get('event_count', 0)}\n"
                + f"- coverage_score: {validation.get('coverage_score', 0.0)}\n"
                + f"- zero_loss: {validation.get('accounting_zero_loss', False)}\n"
                + f"- parser_success: {parser_validation.get('success_ratio', 0.0)}\n\n"
                + "The evidence below is explicitly CONTRASTIVE. Lines in the "
                "MISSED group are real source lines that the rejected regex did NOT "
                "match. Lines in the MATCHED group are real source lines that it DID "
                "match. Treat both groups as candidate event-header evidence.\n\n"
                + "Infer the SHORTEST COMMON STRUCTURAL PREFIX that generalizes across "
                "both groups. Generalize field syntax (for example signed/unsigned or "
                "different token values) instead of enumerating observed literal "
                "families. Do not build alternations from specific host names, tags, "
                "components, categories, or message values. Do not require late "
                "message fields when earlier columns are sufficient for a boundary. "
                "Do not return the rejected regex unchanged if it excludes any "
                "structurally compatible MISSED line.\n\n"
                + "MISSED HEADER CANDIDATES (UNTRUSTED RAW DATA):\n"
                + (missed_text or "<none>")
                + "\n\nMATCHED HEADER EXAMPLES (UNTRUSTED RAW DATA):\n"
                + (matched_text or "<none>")
            )

        response, duration, usage = call_ai_agent(
            self.discovery.agent_key,
            system_prompt,
            user_prompt,
            temperature=0.0,
            max_tokens=512,
            response_format={"type": "json_object"},
            return_usage=True,
        )
        if usage.get("finish_reason") in {"length", "max_tokens"}:
            raise AIResponseError(
                f"LLM repair response truncated "
                f"(finish_reason={usage.get('finish_reason')})",
                usage=usage,
                duration_sec=duration,
            )

        try:
            result = HeaderDiscovery._parse_result(response)
        except Exception as exc:
            raise AIResponseError(
                f"Invalid AI repair response: {exc}",
                usage=usage,
                duration_sec=duration,
            ) from exc

        result["duration_sec"] = round(duration, 4)
        result["_token_usage"] = usage
        return result

    @staticmethod
    def _repair_samples(
        samples: Iterable[str],
        candidate_regex: str,
        max_unmatched: int = 24,
        max_matched: int = 8,
    ) -> Dict[str, List[str]]:
        """Return labeled bounded correction evidence with structural diversity.

        This stays format-agnostic. It does not know BGL, Android, Apache, etc.
        Instead it groups source lines by the shape of their leading tokens and
        round-robins across those groups so a dominant prefix family cannot hide
        rarer header variants during rediscovery.
        """
        try:
            compiled = re.compile(candidate_regex)
        except re.error:
            compiled = None

        raw_samples = [
            str(line).rstrip("\r\n")
            for line in samples
            if str(line).strip()
        ]

        unmatched: List[str] = []
        matched: List[str] = []
        for line in raw_samples:
            is_match = bool(compiled.match(line)) if compiled is not None else False
            (matched if is_match else unmatched).append(line)

        def token_shape(token: str) -> str:
            if not token:
                return "<EMPTY>"
            result: List[str] = []
            previous = None
            for ch in token[:48]:
                if ch.isdigit():
                    kind = "D"
                elif ch.isalpha():
                    kind = "A"
                else:
                    kind = ch
                if kind != previous:
                    result.append(kind)
                    previous = kind
            return "".join(result)

        def structural_key(line: str) -> str:
            # Boundary grammar is dominated by the beginning of a record.
            # Use shapes, not literal token values, to remain format-agnostic.
            tokens = line.lstrip().split()[:4]
            shaped: List[str] = []
            for token in tokens:
                length_bucket = min(len(token) // 4, 8)
                shaped.append(f"{token_shape(token)}:{length_bucket}")
            return "|".join(shaped)

        def diverse_take(lines: List[str], limit: int) -> List[str]:
            if limit <= 0 or not lines:
                return []

            families: Dict[str, List[str]] = {}
            family_order: List[str] = []
            seen = set()

            for line in lines:
                # Deduplicate near-identical messages without erasing the
                # structural family of their leading fields.
                normalized = re.sub(r"\d+", "<N>", line[:200])
                normalized = re.sub(r"\s+", " ", normalized).strip()
                family = structural_key(line)
                dedupe_key = (family, normalized)
                if dedupe_key in seen:
                    continue
                seen.add(dedupe_key)

                if family not in families:
                    families[family] = []
                    family_order.append(family)
                families[family].append(line)

            selected: List[str] = []
            offsets = {family: 0 for family in family_order}

            # Round-robin across structural families.
            while len(selected) < limit:
                progressed = False
                for family in family_order:
                    idx = offsets[family]
                    bucket = families[family]
                    if idx < len(bucket):
                        selected.append(bucket[idx])
                        offsets[family] = idx + 1
                        progressed = True
                        if len(selected) >= limit:
                            break
                if not progressed:
                    break

            return selected

        # Preserve group identity all the way into the AI repair prompt.
        # Flattening these groups loses the key contrastive signal and encourages
        # the model to enumerate literal families instead of generalizing grammar.
        return {
            "missed": diverse_take(unmatched, max_unmatched),
            "matched": diverse_take(matched, max_matched),
        }

    @staticmethod
    def _merge_token_usage(
        first: Dict[str, Any],
        second: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Keep compact cumulative token accounting across discovery + repair."""
        merged = dict(second or {})
        for key in ("prompt_tokens", "completion_tokens", "total_tokens"):
            merged[key] = int((first or {}).get(key, 0) or 0) + int(
                (second or {}).get(key, 0) or 0
            )
        merged["calls"] = int((first or {}).get("calls", 1 if first else 0)) + int(
            (second or {}).get("calls", 1 if second else 0)
        )
        return merged

    @staticmethod
    def _print_validation(label: str, validation: Dict[str, Any]) -> None:
        print(
            f"[SEGMENTATION] VALIDATE {label} | "
            f"headers={validation.get('header_matches', 0)} | "
            f"events={validation.get('event_count', 0)} | "
            f"coverage={validation.get('coverage_score', 0.0):.2f}% | "
            f"parser={(validation.get('parser_validation') or {}).get('success_ratio', 0.0):.2f}% | "
            f"accepted={validation.get('accepted', False)}"
        )

    @staticmethod
    def _build_result(
        *,
        signature: str,
        file_size: int,
        regex: str,
        confidence: float,
        source: str,
        reason: str,
        validation: Dict[str, Any],
        ai_calls: int,
        token_usage: Dict[str, Any],
    ) -> Dict[str, Any]:
        return {
            "verified": True,
            "format_signature": signature,
            "file_size_bytes": file_size,
            "regex": regex,
            "confidence": round(float(confidence or 0.0), 4),
            "source": source,
            "reason": reason,
            "coverage_score": validation.get("coverage_score", 0.0),
            "validation": validation,
            "registry_hit": "miss",
            "ai_calls": ai_calls,
            "ai_call_total": ai_calls,
            "token_usage": token_usage,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }

    @staticmethod
    def _result_from_registry(
        policy: Dict[str, Any], signature: str, level: str, file_size: int
    ) -> Dict[str, Any]:
        validation = {
            "accepted": True,
            "coverage_score": float(policy.get("coverage_score", 0.0) or 0.0),
            "event_count": int(policy.get("event_count", 0) or 0),
            "accounting_zero_loss": bool(policy.get("zero_loss", True)),
            "parser_validation": {
                "success_ratio": float(policy.get("parser_success", 0.0) or 0.0)
            },
            "source": "stored_verified_policy",
        }
        return {
            "verified": True,
            "format_signature": signature,
            "file_size_bytes": file_size,
            "regex": policy["regex"],
            "confidence": float(policy.get("confidence", 0.0) or 0.0),
            "source": f"policy-registry-{level}",
            "policy_source": policy.get("source"),
            "reason": "verified policy reused",
            "coverage_score": validation["coverage_score"],
            "validation": validation,
            "registry_hit": level,
            "ai_calls": 0,
            "ai_call_total": 0,
            "token_usage": {},
        }

    @staticmethod
    def _signature_sample(
        file_path: str,
        max_lines: int = 150,
    ) -> List[str]:
        """Read a stable bounded prefix used only for registry identity.

        AI discovery/validation still uses StratifiedSampler across the file.
        Registry identity does not, because stratified byte positions change
        when the file is appended even if its source grammar is unchanged.

        This remains bounded for very large files. Mid-stream format drift is
        a separate runtime concern and should not make identity depend on file
        length.
        """
        limit = max(20, int(max_lines))
        out: List[str] = []
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            for _ in range(limit):
                raw = f.readline()
                if not raw:
                    break
                out.append(raw.rstrip("\r\n"))
        return out

    @staticmethod
    def _header_shape(line: str) -> Optional[str]:
        """Return a known segmentation-policy-relevant header family.

        Event bodies, tracebacks, JSON continuations and arbitrary message
        text intentionally return None. Their presence/frequency must not
        create a new format identity.
        """
        text = str(line).rstrip("\r\n")
        stripped = text.strip()
        if not stripped:
            return None

        if re.match(r"^<\d{1,3}>", stripped):
            return "HEADER:SYSLOG_ANGLE"

        if re.match(r"^[IWEF]\d{4}\s+\d{2}:\d{2}:\d{2}", stripped):
            return "HEADER:KLOG"

        if re.match(
            r"^\d{1,3}(?:\.\d{1,3}){3}\s+\S+\s+\S+\s+\[",
            stripped,
        ):
            return "HEADER:NGINX"

        if re.match(
            r"^\S+\s+\d{4}-\d{2}-\d{2}[T\s]"
            r"\d{2}:\d{2}:\d{2}(?:[.,]\d{3,6})?",
            stripped,
            re.IGNORECASE,
        ):
            return "HEADER:PREFIXED_TIMESTAMP"

        if re.match(
            r"^\d{4}-\d{2}-\d{2}T"
            r"\d{2}:\d{2}:\d{2}(?:[.,]\d+)?Z?",
            stripped,
            re.IGNORECASE,
        ):
            return "HEADER:ISO_T_TIMESTAMP"

        if re.match(
            r"^\d{4}-\d{2}-\d{2}\s+"
            r"\d{2}:\d{2}:\d{2}(?:[.,]\d+)?",
            stripped,
            re.IGNORECASE,
        ):
            return "HEADER:ISO_SPACE_TIMESTAMP"

        if re.match(
            r"^\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2}",
            stripped,
        ):
            return "HEADER:SLASH_TIMESTAMP"

        if re.match(
            r"^(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)"
            r"\s+\d{1,2}\s+\d{2}:\d{2}:\d{2}",
            stripped,
            re.IGNORECASE,
        ):
            return "HEADER:SYSLOG_TIMESTAMP"

        if re.match(
            r"^(?:INFO|ERROR|WARN|WARNING|DEBUG|TRACE|FATAL|CRITICAL)"
            r"\s+\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}",
            stripped,
            re.IGNORECASE,
        ):
            return "HEADER:LEVEL_TIME"

        if re.match(
            r"^(?:INFO|ERROR|WARN|WARNING|DEBUG|TRACE|FATAL|CRITICAL)"
            r"\s*[:|\-]",
            stripped,
            re.IGNORECASE,
        ):
            return "HEADER:LEVEL_PREFIX"

        return None

    @staticmethod
    def _looks_like_continuation(line: str) -> bool:
        """Conservatively reject common multiline/body lines from signatures."""
        raw = str(line).rstrip("\r\n")
        stripped = raw.strip()
        if not stripped:
            return True

        # Indentation is a strong continuation signal for stack traces,
        # pretty-printed JSON and wrapped application messages.
        if raw[:1].isspace():
            return True

        if re.match(
            r"^(?:Traceback\b|Caused by:|Suppressed:|During handling of\b|"
            r"File\s+[\"']|at\s+\S+\(|\.\.\.\s+\d+\s+more\b)",
            stripped,
            re.IGNORECASE,
        ):
            return True

        # Standalone structural JSON/body lines are not event headers.
        if stripped in {"{", "}", "[", "]", "},", "],"}:
            return True
        if re.match(r'^[}\]],?$', stripped):
            return True

        return False

    @staticmethod
    def _has_header_anchor(line: str) -> bool:
        """Return True when an unknown top-level line has header-like anchors.

        We deliberately require a time/date or severity signal near the start.
        This prevents arbitrary message bodies from becoming format identity
        while still allowing previously unseen delimiter/order conventions.
        """
        prefix = str(line).strip()[:160]
        if not prefix:
            return False

        level = bool(
            re.search(
                r"(?<![A-Za-z])"
                r"(?:TRACE|DEBUG|INFO|NOTICE|WARN|WARNING|ERROR|ERR|"
                r"CRITICAL|FATAL|SEVERE|ALERT|EMERG|EMERGENCY)"
                r"(?![A-Za-z])",
                prefix,
                re.IGNORECASE,
            )
        )
        time_like = bool(
            re.search(
                r"(?:"
                r"\d{4}[-/.]\d{1,2}[-/.]\d{1,2}"
                r"|\d{1,2}[-/.]\d{1,2}[-/.]\d{2,4}"
                r"|(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)"
                r"\s+\d{1,2}"
                r"|\d{1,2}:\d{2}:\d{2}"
                r")",
                prefix,
                re.IGNORECASE,
            )
        )
        return level or time_like

    @staticmethod
    def _unknown_header_shape(line: str) -> Optional[str]:
        """Build a content-independent structural fingerprint for unknown headers.

        This is only a registry-key fallback. It does not segment events and
        does not become a runtime boundary rule. AI/fallback discovery plus
        deterministic validation still decides which policy may be stored.
        """
        raw = str(line).rstrip("\r\n")
        stripped = raw.strip()
        if not stripped:
            return None
        if SegmentationPipeline._looks_like_continuation(raw):
            return None
        if not SegmentationPipeline._has_header_anchor(stripped):
            return None

        # Work only on the header-like prefix. Long message bodies should not
        # change the identity of a source format.
        s = stripped[:220]

        # Normalize high-variance values before generic words/numbers.
        substitutions = [
            (r"\b\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:[.,]\d+)?(?:Z|[+-]\d{2}:?\d{2})?\b", "<DATETIME>"),
            (r"\b\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}(?:[.,]\d+)?\b", "<DATETIME>"),
            (r"\b\d{1,2}[./-]\d{1,2}[./-]\d{2,4}(?:[@ T]\d{1,2}:\d{2}:\d{2}(?:[.,]\d+)?)?\b", "<DATETIME>"),
            (r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2}(?:\s+\d{2}:\d{2}:\d{2})?\b", "<DATETIME>"),
            (r"\b\d{1,2}:\d{2}:\d{2}(?:[.,]\d+)?\b", "<TIME>"),
            (r"\b(?:TRACE|DEBUG|INFO|NOTICE|WARN|WARNING|ERROR|ERR|CRITICAL|FATAL|SEVERE|ALERT|EMERG|EMERGENCY)\b", "<LEVEL>"),
            (r"\b(?:\d{1,3}\.){3}\d{1,3}\b", "<IP>"),
            (r"\b[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\b", "<UUID>"),
            (r"\b0x[0-9a-f]+\b", "<HEX>"),
            (r"\b\d+\b", "<N>"),
            (r'"[^"]*"', '"<Q>"'),
            (r"'[^']*'", "'<Q>'"),
        ]
        for pattern, repl in substitutions:
            s = re.sub(pattern, repl, s, flags=re.IGNORECASE)

        # Preserve punctuation/delimiter grammar, but collapse variable words.
        # Key names before '=' or ':' are retained because they describe the
        # header grammar; ordinary free-text words become <TXT>.
        tokens = re.split(r"(\s+|[|,:;=@\[\](){}<>/\\\-]+)", s)
        shaped: List[str] = []
        for i, token in enumerate(tokens):
            if not token or token.isspace():
                continue

            if token.startswith("<") and token.endswith(">"):
                shaped.append(token.upper())
                continue

            if re.fullmatch(r"[|,:;=@\[\](){}<>/\\\-]+", token):
                shaped.append(token)
                continue

            # Keep stable field names only when immediately followed by '=' or
            # ':'. Values and arbitrary message words must not affect identity.
            next_nonspace = ""
            for nxt in tokens[i + 1:]:
                if nxt and not nxt.isspace():
                    next_nonspace = nxt
                    break

            if (
                re.fullmatch(r"[A-Za-z_][A-Za-z0-9_.-]{0,40}", token)
                and next_nonspace in {"=", ":"}
            ):
                shaped.append("K:" + token.lower())
            else:
                shaped.append("<TXT>")

        # Collapse repeated generic text tokens while retaining delimiters.
        compact: List[str] = []
        for token in shaped:
            if token == "<TXT>" and compact and compact[-1] == "<TXT>":
                continue
            compact.append(token)

        if not compact:
            return None

        # Bound registry-key material. The hash is generated later.
        return "HEADER:UNKNOWN_STRUCT:" + "".join(compact[:80])

    @classmethod
    def _format_signature(cls, samples: Iterable[str]) -> str:
        """Create a stable registry signature from a stable source prefix.

        Callers pass the bounded prefix returned by _signature_sample(), not
        the stratified discovery sample. Known header families are coarse and
        content-free; unknown conventions use normalized structural shapes.

        Event count, file size, and later appended events therefore do not
        perturb the registry key. Structural fallback remains identity-only
        and never becomes an event-boundary rule.
        """
        known_shapes = set()
        unknown_shapes = set()

        for line in samples:
            known = cls._header_shape(line)
            if known is not None:
                known_shapes.add(known)
                continue

            unknown = cls._unknown_header_shape(line)
            if unknown is not None:
                unknown_shapes.add(unknown)

        parts: List[str] = []
        parts.extend(sorted(known_shapes))

        # Unknown structural shapes matter when they are present because they
        # distinguish new grammars that our built-in header catalog does not
        # yet know. Limit cardinality so a noisy sample cannot create an
        # unbounded signature payload.
        parts.extend(sorted(unknown_shapes)[:24])

        canonical = "\n".join(parts) or "HEADER:UNKNOWN_NO_ANCHOR"

        h = hashlib.sha256()
        h.update(SEGMENTATION_SCHEMA_VERSION.encode("utf-8"))
        h.update(b"\n")
        h.update(canonical.encode("utf-8", errors="ignore"))
        return h.hexdigest()[:24]

