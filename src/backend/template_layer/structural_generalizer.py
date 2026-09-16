# -*- coding: utf-8 -*-
import re
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class GeneralizationEvidence:
    rule: str
    original: str
    replacement: str
    confidence: float


@dataclass(frozen=True, slots=True)
class GeneralizationResult:
    template: str
    evidence: tuple[GeneralizationEvidence, ...]


class StructuralTokenGeneralizer:
    """Conservative structural normalization between MessageMasker and Drain3.

    This layer is intentionally dataset/vendor agnostic.  It only generalizes
    values whose surrounding syntax strongly indicates runtime identity.

    Every mutation produces evidence.  TemplateValidator uses that evidence as
    an allow-list; Drain3 still cannot freely replace semantic literals.
    """

    VARIABLE_TOKENS = ("<ID>", "<UUID>", "<IP>", "<URL>", "<HEX>", "<NUM>", "<*>")

    # UUID-shaped values may be followed by '_' or another non-hex character.
    # MessageMasker's \b UUID rule intentionally remains unchanged; this layer
    # handles UUIDs embedded in a larger structural token.
    _EMBEDDED_UUID = re.compile(
        r"(?<![0-9A-Fa-f])"
        r"[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-"
        r"[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}"
        r"(?![0-9A-Fa-f-])"
    )

    # Runtime object names commonly have an alphabetic structural prefix and a
    # numeric instance index.  Require '_' plus a numeric suffix and preserve
    # the prefix.  A small numeric suffix is accepted here only when the token
    # occurs in an identity-like syntactic position (see _generalize_token).
    _PREFIXED_COUNTER = re.compile(
        r"^(?P<prefix>[A-Za-z][A-Za-z0-9.-]*_)(?P<number>\d+)$"
    )

    # Directory-like runtime shard names: subdir62, shard17, partition42 etc.
    # This is deliberately NOT applied to arbitrary words+number.  It is only
    # enabled for slash-delimited path components.
    # Dot-delimited runtime counters such as core.862 are structurally similar
    # to underscore counters, but dot notation is also common in versions and
    # semantic names. Therefore this shape is generalized only when surrounding
    # syntax explicitly denotes runtime object creation/identity.
    _DOTTED_COUNTER = re.compile(
        r"^(?P<prefix>[A-Za-z][A-Za-z0-9_-]*\.)(?P<number>\d+)$"
    )

    _PATH_COUNTER = re.compile(
        r"^(?P<prefix>[A-Za-z][A-Za-z_-]{2,})(?P<number>\d+)$"
    )

    # A host/domain is considered structural only when it participates in an
    # endpoint (host:port) or proxy-like arrow/path syntax.  Plain domains in
    # prose are left untouched.
    _HOST_PORT = re.compile(
        r"(?P<host>"
        r"(?:[A-Za-z0-9](?:[A-Za-z0-9-]{0,62}[A-Za-z0-9])?\.)+"
        r"[A-Za-z]{2,63}"
        r"|"
        r"[A-Za-z][A-Za-z0-9._-]*"
        r"):(?P<port><NUM>|\d{1,5})"
    )

    # Executable/process token at the beginning of a structured payload,
    # optionally after a bracketed prefix.  This does not globally mask *.exe.
    _LEADING_PROCESS = re.compile(
        r"^(?P<head>(?:\[[^\]]+\]\s+)?)"
        r"(?P<proc>[A-Za-z0-9_.+-]+\.exe)"
        r"(?P<tail>\s+-\s+)"
    )

    # Path components that are already strongly identity-shaped.  This handles
    # forms such as _task_2008_... that survived deterministic masking.
    _PATH_COMPOUND_ID = re.compile(
        r"(?P<prefix>[/\\](?:_)?[A-Za-z][A-Za-z0-9.-]*_)"
        r"(?=[A-Za-z0-9_-]*\d)"
        r"(?=[A-Za-z0-9_-]*[_-])"
        r"[A-Za-z0-9]+(?:[_-][A-Za-z0-9]+)+"
    )

    # Bare hexadecimal machine addresses frequently appear after explicit
    # address-bearing labels (iar/dear/address/addr/pc).  Do not classify every
    # short hex-looking token globally: context is the evidence.
    _LABELED_HEX_ADDRESS = re.compile(
        r"(?i)(?P<label>\b(?:iar|dear|address|addr|pc)\b(?P<sep>\s*[:=]?\s+))"
        r"(?P<value>(?=[0-9A-Fa-f]*[A-Fa-f])[0-9A-Fa-f]{6,16})\b"
    )

    # Program/file path values are runtime parameters only in explicit
    # executable/file-bearing message syntax.  The surrounding literal remains
    # untouched, so this is substantially safer than globally wildcarding paths.
    _PROGRAM_PATH = re.compile(
        r"(?i)(?P<label>\b(?:program|loading)\s+)"
        r"(?P<path>(?:[A-Za-z]:)?[/\\][^\s,:;]+)"
    )

    def _generalize_labeled_hex_addresses(self, value, evidence):
        def repl(match):
            original = match.group(0)
            replacement = f"{match.group('label')}<HEX>"
            evidence.append(
                GeneralizationEvidence(
                    rule="labeled-hex-address",
                    original=original,
                    replacement=replacement,
                    confidence=0.99,
                )
            )
            return replacement
        return self._LABELED_HEX_ADDRESS.sub(repl, value)

    def _generalize_program_paths(self, value, evidence):
        def repl(match):
            original = match.group(0)
            replacement = f"{match.group('label')}<ID>"
            evidence.append(
                GeneralizationEvidence(
                    rule="explicit-program-path",
                    original=original,
                    replacement=replacement,
                    confidence=0.99,
                )
            )
            return replacement
        return self._PROGRAM_PATH.sub(repl, value)

    def generalize(self, deterministic_template):
        value = " ".join(str(deterministic_template or "").split())
        if not value:
            return GeneralizationResult(value, ())

        evidence = []

        value = self._generalize_labeled_hex_addresses(value, evidence)
        value = self._generalize_program_paths(value, evidence)

        value = self._sub_with_evidence(
            value,
            self._EMBEDDED_UUID,
            "<UUID>",
            "embedded-uuid",
            1.0,
            evidence,
        )

        value = self._sub_with_evidence(
            value,
            self._PATH_COMPOUND_ID,
            lambda m: f"{m.group('prefix')}<ID>",
            "path-compound-runtime-id",
            0.99,
            evidence,
        )

        value = self._generalize_path_counters(value, evidence)
        value = self._generalize_identity_counters(value, evidence)
        value = self._generalize_dotted_runtime_counters(value, evidence)
        value = self._generalize_structured_process(value, evidence)
        value = self._generalize_endpoints(value, evidence)

        return GeneralizationResult(value, tuple(evidence))

    def _generalize_path_counters(self, value, evidence):
        # Keep delimiters so Windows and POSIX paths both work.
        parts = re.split(r"([/\\])", value)
        for i, part in enumerate(parts):
            if i == 0 or parts[i - 1] not in {"/", "\\"}:
                continue
            match = self._PATH_COUNTER.fullmatch(part)
            if not match:
                continue
            replacement = f"{match.group('prefix')}<ID>"
            evidence.append(
                GeneralizationEvidence(
                    rule="path-runtime-counter",
                    original=part,
                    replacement=replacement,
                    confidence=0.98,
                )
            )
            parts[i] = replacement
        return "".join(parts)

    def _generalize_identity_counters(self, value, evidence):
        tokens = value.split()
        for i, token in enumerate(tokens):
            # Preserve punctuation around the structural token.
            lead, core, tail = self._unwrap_token(token)
            match = self._PREFIXED_COUNTER.fullmatch(core)
            if not match:
                continue

            # A short suffix such as phase_2 is not enough by itself.  Require
            # evidence from the surrounding syntax that this token is an
            # identity/object reference.
            prev_token = tokens[i - 1].lower().strip("[]():,") if i else ""
            next_token = tokens[i + 1].lower().strip("[]():,") if i + 1 < len(tokens) else ""
            context_words = {
                "block", "partition", "task", "attempt", "container", "session",
                "object", "instance", "for", "from", "to", "of", "stored",
            }
            structural_context = (
                prev_token in context_words
                or next_token in {"stored", "not", "for", "from", "to"}
                or core.lower().startswith(("rdd_", "broadcast_", "task_", "attempt_", "container_"))
            )
            if not structural_context:
                continue

            replacement_core = f"{match.group('prefix')}<ID>"
            replacement = f"{lead}{replacement_core}{tail}"
            evidence.append(
                GeneralizationEvidence(
                    rule="contextual-runtime-counter",
                    original=token,
                    replacement=replacement,
                    confidence=0.97,
                )
            )
            tokens[i] = replacement
        return " ".join(tokens)

    def _generalize_dotted_runtime_counters(self, value, evidence):
        tokens = value.split()
        for i, token in enumerate(tokens):
            lead, core, tail = self._unwrap_token(token)
            match = self._DOTTED_COUNTER.fullmatch(core)
            if not match:
                continue

            # Do not generalize arbitrary dotted numbers such as versions.
            # Require an adjacent literal that explicitly denotes creation or
            # allocation of a runtime object.
            prev_token = tokens[i - 1].lower().strip("[]():,") if i else ""
            runtime_identity_context = {
                "generating", "generated", "creating", "created",
                "allocating", "allocated", "spawning", "spawned",
            }
            if prev_token not in runtime_identity_context:
                continue

            replacement_core = f"{match.group('prefix')}<ID>"
            replacement = f"{lead}{replacement_core}{tail}"
            evidence.append(
                GeneralizationEvidence(
                    rule="contextual-dotted-runtime-counter",
                    original=token,
                    replacement=replacement,
                    confidence=0.99,
                )
            )
            tokens[i] = replacement

        return " ".join(tokens)

    def _generalize_structured_process(self, value, evidence):
        match = self._LEADING_PROCESS.search(value)
        if not match:
            return value
        original = match.group("proc")
        replacement = "<ID>"
        evidence.append(
            GeneralizationEvidence(
                rule="structured-leading-process",
                original=original,
                replacement=replacement,
                confidence=0.96,
            )
        )
        return (
            value[: match.start("proc")]
            + replacement
            + value[match.end("proc") :]
        )

    def _generalize_endpoints(self, value, evidence):
        def repl(match):
            host = match.group("host")
            port = match.group("port")

            # IPs are already deterministic variables; do not create redundant
            # evidence or weaken their more precise placeholder.
            if host in self.VARIABLE_TOKENS:
                return match.group(0)

            replacement = f"<ID>:{port if port in self.VARIABLE_TOKENS else '<NUM>'}"
            evidence.append(
                GeneralizationEvidence(
                    rule="structured-host-port",
                    original=match.group(0),
                    replacement=replacement,
                    confidence=0.96,
                )
            )
            return replacement

        return self._HOST_PORT.sub(repl, value)

    @staticmethod
    def _unwrap_token(token):
        lead_match = re.match(r"^[\[\({'\"]*", token)
        tail_match = re.search(r"[\]\)}:,;'\".]*$", token)
        lead = lead_match.group(0) if lead_match else ""
        tail = tail_match.group(0) if tail_match else ""
        start = len(lead)
        end = len(token) - len(tail) if tail else len(token)
        return lead, token[start:end], tail

    @staticmethod
    def _sub_with_evidence(value, pattern, replacement, rule, confidence, evidence):
        def repl(match):
            original = match.group(0)
            new_value = replacement(match) if callable(replacement) else replacement
            if original != new_value:
                evidence.append(
                    GeneralizationEvidence(
                        rule=rule,
                        original=original,
                        replacement=new_value,
                        confidence=confidence,
                    )
                )
            return new_value

        return pattern.sub(repl, value)
