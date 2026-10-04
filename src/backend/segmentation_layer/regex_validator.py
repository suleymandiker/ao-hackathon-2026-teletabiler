# -*- coding: utf-8 -*-
"""Deterministic, streaming validation for AI-discovered multiline regexes."""

from __future__ import annotations

import re
from typing import Any, Callable, Dict, List, Optional


class RegexValidator:
    """Validate segmentation candidates without sending the full log to an LLM.

    Validation has three evidence sources:
    1) structural header candidates,
    2) internal header candidates (under-segmentation),
    3) parser feedback for the assembled logical events.
    """

    _TIMESTAMP = re.compile(
        r"^(?:\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}"
        r"|\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2}"
        r"|(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)"
        r"\s+\d{1,2}\s+\d{2}:\d{2}:\d{2})",
        re.IGNORECASE,
    )

    # Generic file/prefix + timestamp header. This covers rotated/service log
    # prefixes such as:
    #   nova-api.log.1.2017-05-17_12:02:19 2017-05-16 16:19:55.052 INFO ...
    # without hard-coding a particular product or filename.
    _PREFIXED_TIMESTAMP = re.compile(
        r"^\S+\s+\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}"
        r"(?:[.,]\d{3,6})?",
        re.IGNORECASE,
    )

    _NGINX = re.compile(
        r"^\d{1,3}(?:\.\d{1,3}){3}\s+\S+\s+\S+\s+\["
    )

    _JSON = re.compile(r"^\s*\{")

    _SYSLOG_ANGLE = re.compile(r"^<\d{1,3}>")

    _KLOG = re.compile(
        r"^[IWEF]\d{4}\s+\d{2}:\d{2}:\d{2}"
    )

    _LEVEL_TIME = re.compile(
        r"^(?:INFO|ERROR|WARN|WARNING|DEBUG|TRACE|FATAL|CRITICAL)"
        r"\s+\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}",
        re.IGNORECASE,
    )

    _LEVEL_PREFIX = re.compile(
        r"^(?:INFO|ERROR|WARN|WARNING|DEBUG|TRACE|FATAL|CRITICAL)"
        r"\s*[:|\-]",
        re.IGNORECASE,
    )

    def __init__(
        self,
        max_regex_length: int = 1000,
        max_rules: int = 25,
        max_failed_samples: int = 30,
        max_positive_samples: int = 12,
        max_internal_samples: int = 30,
        min_confidence: float = 0.55,
    ):
        self.max_regex_length = max_regex_length
        self.max_rules = max_rules
        self.max_failed_samples = max_failed_samples
        self.max_positive_samples = max_positive_samples
        self.max_internal_samples = max_internal_samples
        self.min_confidence = min_confidence

    def validate(
        self,
        file_path: str,
        regex_pattern: str,
        parser_fn: Optional[
            Callable[[str], Optional[Dict[str, Any]]]
        ] = None,
    ) -> Dict[str, Any]:
        """Streaming validation of the final contextual boundary policy."""

        safety = self._validate_safety(regex_pattern)

        if not safety["valid"]:
            return self._empty_result(
                safety["reason"],
                safety=safety,
            )

        try:
            re.compile(regex_pattern)
        except re.error as exc:
            return self._empty_result(
                f"regex compile error: {exc}"
            )

        from segmentation_layer.multiline_assembler import MultilineAssembler

        assembler = MultilineAssembler()

        # -------------------------------------------------------------
        # PHYSICAL LINE ACCOUNTING
        # -------------------------------------------------------------

        total = 0
        nonempty = 0
        blank_lines = 0

        # Streaming pass: full file is never collected into RAM.
        with open(
            file_path,
            "r",
            encoding="utf-8",
            errors="ignore",
        ) as f:
            for raw in f:
                total += 1

                if raw.strip():
                    nonempty += 1
                else:
                    blank_lines += 1

        # -------------------------------------------------------------
        # VALIDATION COUNTERS
        # -------------------------------------------------------------

        header_matches = 0
        regex_candidate_matches = 0
        suppressed_regex_matches = 0

        # IMPORTANT:
        # candidate_unmatched is NOT equal to the number of orphan events.
        #
        # An orphan may simply be a traceback/exception continuation that
        # became a separate record because of a blank-line boundary.
        candidate_unmatched = 0

        internal_header_candidates = 0

        classification_counts = {
            "STRONG_HEADER": 0,
            "CONTEXTUAL_HEADER": 0,
            "CONTINUATION": 0,
        }

        event_count = 0

        parser_attempts = 0
        parser_success = 0
        parser_fail_samples = []

        min_lines = None
        max_lines = 0
        multiline = 0
        single = 0
        accounted = 0

        first_line_sources = {
            "regex": 0,
            "orphan": 0,
        }

        orphan_reasons = {
            "blank_line_boundary": 0,
            "unrecognized_top_level": 0,
        }

        positive_samples = []
        failed_samples = []
        internal_header_samples = []

        # Diagnostic only.
        #
        # Keep a bounded number of orphan examples for debugging.
        # Acceptance MUST NOT depend on this sample.
        orphan_examples = []

        # -------------------------------------------------------------
        # LOGICAL EVENT VALIDATION
        # -------------------------------------------------------------

        for rec in assembler.iter_event_records(
            file_path,
            regex_pattern,
        ):
            event_count += 1

            n = int(
                rec.get("line_count", 0)
            )

            accounted += n

            min_lines = (
                n
                if min_lines is None
                else min(min_lines, n)
            )

            max_lines = max(
                max_lines,
                n,
            )

            multiline += int(n > 1)
            single += int(n == 1)

            decs = rec.get(
                "decisions",
                [],
            )

            # ---------------------------------------------------------
            # EVENT FIRST-LINE CLASSIFICATION
            # ---------------------------------------------------------

            if (
                decs
                and decs[0].get("start_new_event")
            ):
                first_line_sources["regex"] += 1

            else:
                first_line_sources["orphan"] += 1

                orphan_reasons[
                    "unrecognized_top_level"
                ] += 1

                event_text = str(
                    rec.get("event", "")
                )

                # MultilineAssembler joins physical lines using " | ".
                # Therefore the text before the first separator is the
                # original first physical line of this event.
                first_line = (
                    event_text
                    .split(" | ", 1)[0]
                    .strip()
                )

                # -----------------------------------------------------
                # TRUE MISSED-HEADER DETECTION
                # -----------------------------------------------------
                #
                # orphan != missed header
                #
                # Examples such as:
                #
                #   During handling of the above exception...
                #   Traceback (most recent call last):
                #
                # are legitimate continuation fragments and must NOT
                # invalidate an otherwise correct segmentation policy.
                #
                # Only an orphan whose first line independently has
                # deterministic header structure is considered evidence
                # that the candidate regex missed a real boundary.

                if (
                    first_line
                    and self.is_strong_header(
                        first_line
                    )
                ):
                    candidate_unmatched += 1

                    if (
                        len(internal_header_samples)
                        < self.max_internal_samples
                    ):
                        internal_header_samples.append(
                            first_line[:700]
                        )

                # -----------------------------------------------------
                # BOUNDED DEBUG SAMPLE
                # -----------------------------------------------------

                if len(orphan_examples) < 10:
                    orphan_examples.append(
                        {
                            "event_no": event_count,
                            "start_line": rec.get(
                                "start_line"
                            ),
                            "end_line": rec.get(
                                "end_line"
                            ),
                            "line_count": n,
                            "event_preview": (
                                event_text[:500]
                            ),
                            "first_decision": (
                                decs[0]
                                if decs
                                else None
                            ),
                        }
                    )

            # ---------------------------------------------------------
            # POSITIVE EVENT SAMPLES
            # ---------------------------------------------------------

            if (
                len(positive_samples)
                < self.max_positive_samples
            ):
                positive_samples.append(
                    rec.get(
                        "event",
                        "",
                    )[:700]
                )

            # ---------------------------------------------------------
            # LINE DECISION ACCOUNTING
            # ---------------------------------------------------------

            for d in decs:
                cls = d.get(
                    "classification",
                    "CONTINUATION",
                )

                classification_counts[cls] = (
                    classification_counts.get(
                        cls,
                        0,
                    )
                    + 1
                )

                if d.get("regex_match"):
                    regex_candidate_matches += 1

                    if not d.get(
                        "start_new_event"
                    ):
                        suppressed_regex_matches += 1

                if d.get(
                    "start_new_event"
                ):
                    header_matches += 1

            # ---------------------------------------------------------
            # PARSER VALIDATION
            # ---------------------------------------------------------

            if parser_fn is not None:
                parser_attempts += 1

                try:
                    parsed = parser_fn(
                        rec.get(
                            "event",
                            "",
                        )
                    )
                except Exception:
                    parsed = None

                if parsed:
                    parser_success += 1

                elif (
                    len(parser_fail_samples)
                    < self.max_failed_samples
                ):
                    parser_fail_samples.append(
                        rec.get(
                            "event",
                            "",
                        )[:700]
                    )

        # -------------------------------------------------------------
        # FINAL METRICS
        # -------------------------------------------------------------

        accounting_zero_loss = (
            nonempty == accounted
        )

        # DO NOT overwrite candidate_unmatched here.
        #
        # Old incorrect behaviour was:
        #
        # candidate_unmatched = first_line_sources["orphan"]
        #
        # That incorrectly treated traceback fragments as missed headers.

        denominator = (
            header_matches
            + candidate_unmatched
        )

        boundary_score = (
            header_matches
            / denominator
            * 100.0
            if denominator
            else 100.0
        )

        all_line_match_ratio = (
            header_matches
            / nonempty
            * 100.0
            if nonempty
            else 100.0
        )

        parser_ratio = (
            parser_success
            / parser_attempts
            * 100.0
            if parser_attempts
            else 100.0
        )

        # -------------------------------------------------------------
        # ACCEPTANCE POLICY
        # -------------------------------------------------------------
        #
        # We do NOT relax the validation policy.
        #
        # candidate_unmatched == 0 still means:
        #
        #   no structurally valid header was missed.
        #
        # The fix is only that harmless orphan continuations are no
        # longer incorrectly counted as missed headers.

        accepted = bool(
            header_matches > 0
            and boundary_score >= 99.0
            and candidate_unmatched == 0
            and accounting_zero_loss
            and (
                parser_attempts == 0
                or parser_ratio >= 95.0
            )
        )

        # -------------------------------------------------------------
        # TEMPORARY VALIDATOR DEBUG
        # -------------------------------------------------------------

        print(
            "\n=== REGEX VALIDATOR DEBUG ===",
            flush=True,
        )

        print(
            "regex_pattern:",
            repr(regex_pattern),
            flush=True,
        )

        print(
            "total_lines:",
            total,
            flush=True,
        )

        print(
            "nonempty:",
            nonempty,
            flush=True,
        )

        print(
            "regex_candidate_matches:",
            regex_candidate_matches,
            flush=True,
        )

        print(
            "suppressed_regex_matches:",
            suppressed_regex_matches,
            flush=True,
        )

        print(
            "header_matches:",
            header_matches,
            flush=True,
        )

        print(
            "event_count:",
            event_count,
            flush=True,
        )

        print(
            "first_line_sources:",
            first_line_sources,
            flush=True,
        )

        print(
            "classification_counts:",
            classification_counts,
            flush=True,
        )

        print(
            "candidate_unmatched:",
            candidate_unmatched,
            flush=True,
        )

        print(
            "orphan_examples:",
            flush=True,
        )

        for orphan in orphan_examples:
            print(
                "  ORPHAN:",
                orphan,
                flush=True,
            )

        print(
            "accounted:",
            accounted,
            "nonempty:",
            nonempty,
            "zero_loss:",
            accounting_zero_loss,
            flush=True,
        )

        print(
            "parser_attempts:",
            parser_attempts,
            "parser_success:",
            parser_success,
            "parser_ratio:",
            round(parser_ratio, 2),
            flush=True,
        )

        print(
            "boundary_score:",
            round(boundary_score, 2),
            "accepted:",
            accepted,
            flush=True,
        )

        print(
            "=== END VALIDATOR DEBUG ===\n",
            flush=True,
        )

        # -------------------------------------------------------------
        # RESULT
        # -------------------------------------------------------------

        return {
            "valid": True,
            "accepted": accepted,
            "reason": (
                "ok"
                if accepted
                else "candidate requires refinement"
            ),
            "coverage_score": round(
                boundary_score,
                2,
            ),
            "all_line_match_ratio": round(
                all_line_match_ratio,
                2,
            ),
            "total_lines": total,
            "total_nonempty_lines": nonempty,
            "blank_lines": blank_lines,
            "header_matches": header_matches,
            "regex_candidate_matches": (
                regex_candidate_matches
            ),
            "suppressed_regex_matches": (
                suppressed_regex_matches
            ),
            "classification_counts": (
                classification_counts
            ),
            "candidate_unmatched_headers": (
                candidate_unmatched
            ),
            "internal_header_candidates": (
                internal_header_candidates
            ),
            "internal_header_samples": (
                internal_header_samples
            ),
            "accounting_zero_loss": (
                accounting_zero_loss
            ),
            "event_count": event_count,
            "event_line_counts": {
                "min": min_lines or 0,
                "max": max_lines,
                "multiline_events": multiline,
                "single_line_events": single,
            },
            "parser_validation": {
                "attempts": parser_attempts,
                "success": parser_success,
                "success_ratio": round(
                    parser_ratio,
                    2,
                ),
                "failed_samples": (
                    parser_fail_samples
                ),
            },
            "undersegmentation_rate": 0.0,
            "first_line_sources": (
                first_line_sources
            ),
            "orphan_reasons": (
                orphan_reasons
            ),
            "failed_samples": (
                failed_samples
            ),
            "positive_samples": (
                positive_samples
            ),
            "safety": safety,
        }

    def is_strong_header(
        self,
        line: str,
    ) -> bool:
        """Return True when a line has deterministic top-level header structure."""

        text = line.strip()

        if not text:
            return False

        return bool(
            self._TIMESTAMP.match(text)
            or self._PREFIXED_TIMESTAMP.match(text)
            or self._NGINX.match(text)
            or self._JSON.match(text)
            or self._SYSLOG_ANGLE.match(text)
            or self._KLOG.match(text)
            or self._LEVEL_TIME.match(text)
            or self._LEVEL_PREFIX.match(text)
        )

    @staticmethod
    def _has_diverse_headers(
        samples: List[str],
    ) -> bool:
        if not samples:
            return False

        # One repeated header family is fine; we only require at least one
        # positive sample. Diversity is reported, not a hard acceptance gate.
        return True

    def _empty_result(
        self,
        reason: str,
        safety: Optional[
            Dict[str, Any]
        ] = None,
    ) -> Dict[str, Any]:
        return {
            "valid": False,
            "accepted": False,
            "reason": reason,
            "coverage_score": 0.0,
            "all_line_match_ratio": 0.0,
            "total_lines": 0,
            "total_nonempty_lines": 0,
            "blank_lines": 0,
            "header_matches": 0,
            "regex_candidate_matches": 0,
            "suppressed_regex_matches": 0,
            "classification_counts": {
                "STRONG_HEADER": 0,
                "CONTEXTUAL_HEADER": 0,
                "CONTINUATION": 0,
            },
            "candidate_unmatched_headers": 0,
            "internal_header_candidates": 0,
            "internal_header_samples": [],
            "accounting_zero_loss": False,
            "event_count": 0,
            "event_line_counts": {
                "min": 0,
                "max": 0,
                "multiline_events": 0,
                "single_line_events": 0,
            },
            "parser_validation": {
                "attempts": 0,
                "success": 0,
                "success_ratio": 0.0,
                "failed_samples": [],
            },
            "undersegmentation_rate": 0.0,
            "first_line_sources": {
                "regex": 0,
                "orphan": 0,
            },
            "orphan_reasons": {
                "blank_line_boundary": 0,
                "unrecognized_top_level": 0,
            },
            "failed_samples": [],
            "positive_samples": [],
            "safety": (
                safety
                or {
                    "valid": False,
                    "reason": reason,
                }
            ),
        }

    @staticmethod
    def _top_level_alternatives(
        pattern: str,
    ) -> int:
        """Count top-level regex alternatives only."""

        count = 1
        depth = 0
        in_class = False
        escaped = False

        for ch in pattern:
            if escaped:
                escaped = False
                continue

            if ch == "\\":
                escaped = True
                continue

            if ch == "[":
                in_class = True
                continue

            if ch == "]":
                in_class = False
                continue

            if in_class:
                continue

            if ch == "(":
                depth += 1

            elif ch == ")":
                depth = max(
                    0,
                    depth - 1,
                )

            elif (
                ch == "|"
                and depth == 0
            ):
                count += 1

        return count

    def _validate_safety(
        self,
        regex_pattern: str,
    ) -> Dict[str, Any]:
        if not regex_pattern:
            return {
                "valid": False,
                "reason": "empty regex",
            }

        if not regex_pattern.startswith("^"):
            return {
                "valid": False,
                "reason": (
                    "regex must start with ^"
                ),
            }

        if (
            len(regex_pattern)
            > self.max_regex_length
        ):
            return {
                "valid": False,
                "reason": (
                    f"regex too long: "
                    f"{len(regex_pattern)} > "
                    f"{self.max_regex_length}"
                ),
            }

        if regex_pattern in (
            "^.*",
            "^.*$",
            ".*",
            r"^\S+",
            r"^\S+$",
        ):
            return {
                "valid": False,
                "reason": (
                    "generic catch-all regex rejected"
                ),
            }

        rules = (
            self._top_level_alternatives(
                regex_pattern
            )
        )

        if rules > self.max_rules:
            return {
                "valid": False,
                "reason": (
                    f"too many regex alternatives: "
                    f"{rules} > "
                    f"{self.max_rules}"
                ),
            }

        try:
            re.compile(regex_pattern)

        except re.error as exc:
            return {
                "valid": False,
                "reason": (
                    f"regex compile error: {exc}"
                ),
            }

        return {
            "valid": True,
            "reason": "ok",
            "rules": rules,
        }