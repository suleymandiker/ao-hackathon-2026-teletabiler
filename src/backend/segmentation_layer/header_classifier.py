# -*- coding: utf-8 -*-
"""Cheap contextual line classifier for streaming segmentation.

Regex is treated as candidate evidence, never as the final boundary decision.
"""
from __future__ import annotations
import re
from dataclasses import dataclass
from typing import Optional

STRONG_HEADER = "STRONG_HEADER"
CONTEXTUAL_HEADER = "CONTEXTUAL_HEADER"
CONTINUATION = "CONTINUATION"

@dataclass(frozen=True)
class LineDecision:
    classification: str
    start_new_event: bool
    regex_match: bool
    reason: str
    confidence: float

class HeaderClassifier:
    _TIMESTAMP = re.compile(r"^(?:\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}|\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2}|(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2}\s+\d{2}:\d{2}:\d{2})", re.I)
    _PREFIXED_TIMESTAMP = re.compile(r"^\S+\s+\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}(?:[.,]\d{3,6})?", re.I)
    _SYSLOG = re.compile(r"^<\d{1,3}>")
    _KLOG = re.compile(r"^[IWEF]\d{4}\s+\d{2}:\d{2}:\d{2}")
    _LEVEL = re.compile(r"^(?:INFO|ERROR|WARN|WARNING|DEBUG|TRACE|FATAL|CRITICAL)(?:\s+\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}|\s*[:|\-])", re.I)
    _NGINX = re.compile(r"^\d{1,3}(?:\.\d{1,3}){3}\s+\S+\s+\S+\s+\[")
    _TRACE_CONT = re.compile(r"^(?:Traceback \(most recent call last\):|Caused by:|Suppressed:|\.\.\. \d+ more\b|at\s+\S+|File\s+\"|[A-Za-z_][\w.]*?(?:Error|Exception):)")
    _STRUCTURED_CONT = re.compile(r"^(?:caused_by:|detail:|dependency chain:|symptom:|recommendation:|client:|server:|request:|upstream:|host:)", re.I)

    def classify(self, line: str, regex: Optional[re.Pattern], *, has_current_event: bool, after_blank: bool = False) -> LineDecision:
        text = line.rstrip("\r\n")
        stripped = text.strip()
        if not stripped:
            return LineDecision(CONTINUATION, False, False, "blank", 1.0)
        regex_match = bool(regex.match(text)) if regex else False

        # Known multiline continuations override a broad AI/fallback regex.
        if text[:1].isspace() or self._TRACE_CONT.match(stripped) or self._STRUCTURED_CONT.match(stripped):
            return LineDecision(CONTINUATION, False, regex_match, "known_multiline_continuation", 0.99)

        strong = bool(self._TIMESTAMP.match(stripped) or self._PREFIXED_TIMESTAMP.match(stripped) or self._SYSLOG.match(stripped) or self._KLOG.match(stripped) or self._LEVEL.match(stripped) or self._NGINX.match(stripped))
        if strong:
            return LineDecision(STRONG_HEADER, True, regex_match, "deterministic_strong_header", 1.0)

        # Opening JSON/object line is contextual: top-level/after blank can begin
        # an event; inside an existing event it is continuation data.
        if stripped == "{" or stripped.startswith("{\""):
            start = (not has_current_event) or after_blank
            return LineDecision(CONTEXTUAL_HEADER if start else CONTINUATION, start, regex_match, "json_context", 0.90 if start else 0.95)

        # A verified/discovered policy regex is allowed to define boundaries for
        # otherwise unknown formats. Continuation safety above is a hard veto, so
        # broad AI regexes still cannot split known traceback/stacktrace lines.
        #
        # This is intentionally format-agnostic: Android logcat, proprietary
        # application logs, or another unseen grammar do not need a new built-in
        # HeaderClassifier rule once their policy regex has been validated.
        if regex_match:
            return LineDecision(
                CONTEXTUAL_HEADER,
                True,
                True,
                "policy_regex_header",
                0.90,
            )

        return LineDecision(CONTINUATION, False, False, "continuation_default", 0.98 if has_current_event else 0.55)
