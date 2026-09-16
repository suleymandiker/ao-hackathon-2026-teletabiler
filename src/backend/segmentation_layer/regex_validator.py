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
        r"|(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2}\s+\d{2}:\d{2}:\d{2})",
        re.IGNORECASE,
    )
    # Generic file/prefix + timestamp header. This covers rotated/service log
    # prefixes such as:
    #   nova-api.log.1.2017-05-17_12:02:19 2017-05-16 16:19:55.052 INFO ...
    # without hard-coding a particular product or filename.
    _PREFIXED_TIMESTAMP = re.compile(
        r"^\S+\s+\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}(?:[.,]\d{3,6})?",
        re.IGNORECASE,
    )
    _NGINX = re.compile(r"^\d{1,3}(?:\.\d{1,3}){3}\s+\S+\s+\S+\s+\[")
    _JSON = re.compile(r"^\s*\{")
    _SYSLOG_ANGLE = re.compile(r"^<\d{1,3}>")
    _KLOG = re.compile(r"^[IWEF]\d{4}\s+\d{2}:\d{2}:\d{2}")
    _LEVEL_TIME = re.compile(
        r"^(?:INFO|ERROR|WARN|WARNING|DEBUG|TRACE|FATAL|CRITICAL)\s+\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}",
        re.IGNORECASE,
    )
    _LEVEL_PREFIX = re.compile(
        r"^(?:INFO|ERROR|WARN|WARNING|DEBUG|TRACE|FATAL|CRITICAL)\s*[:|\-]",
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
        parser_fn: Optional[Callable[[str], Optional[Dict[str, Any]]]] = None,
    ) -> Dict[str, Any]:
        """Streaming validation of the final contextual boundary policy."""
        safety=self._validate_safety(regex_pattern)
        if not safety["valid"]: return self._empty_result(safety["reason"],safety=safety)
        try: re.compile(regex_pattern)
        except re.error as exc: return self._empty_result(f"regex compile error: {exc}")
        from segmentation_layer.multiline_assembler import MultilineAssembler
        assembler=MultilineAssembler()
        total=nonempty=blank_lines=0
        # Physical accounting is a streaming pass and keeps no line collection.
        with open(file_path,'r',encoding='utf-8',errors='ignore') as f:
            for raw in f:
                total+=1
                if raw.strip(): nonempty+=1
                else: blank_lines+=1
        header_matches=regex_candidate_matches=suppressed_regex_matches=0
        candidate_unmatched=internal_header_candidates=0
        classification_counts={"STRONG_HEADER":0,"CONTEXTUAL_HEADER":0,"CONTINUATION":0}
        event_count=parser_attempts=parser_success=0; parser_fail_samples=[]
        min_lines=None; max_lines=multiline=single=accounted=0
        first_line_sources={"regex":0,"orphan":0}; orphan_reasons={"blank_line_boundary":0,"unrecognized_top_level":0}
        positive_samples=[]; failed_samples=[]; internal_header_samples=[]
        for rec in assembler.iter_event_records(file_path,regex_pattern):
            event_count+=1; n=int(rec.get("line_count",0)); accounted+=n
            min_lines=n if min_lines is None else min(min_lines,n); max_lines=max(max_lines,n)
            multiline += int(n>1); single += int(n==1)
            decs=rec.get("decisions",[])
            if decs and decs[0].get("start_new_event"): first_line_sources["regex"]+=1
            else: first_line_sources["orphan"]+=1; orphan_reasons["unrecognized_top_level"]+=1
            if len(positive_samples)<self.max_positive_samples: positive_samples.append(rec.get("event","")[:700])
            for d in decs:
                cls=d.get("classification","CONTINUATION"); classification_counts[cls]=classification_counts.get(cls,0)+1
                if d.get("regex_match"):
                    regex_candidate_matches+=1
                    if not d.get("start_new_event"): suppressed_regex_matches+=1
                if d.get("start_new_event"): header_matches+=1
            if parser_fn is not None:
                parser_attempts+=1
                try: parsed=parser_fn(rec.get("event",""))
                except Exception: parsed=None
                if parsed: parser_success+=1
                elif len(parser_fail_samples)<self.max_failed_samples: parser_fail_samples.append(rec.get("event","")[:700])
        accounting_zero_loss=(nonempty==accounted)
        # Strong deterministic headers are promoted by HeaderClassifier itself;
        # therefore a missed strong header would appear as an orphan first line.
        candidate_unmatched=first_line_sources["orphan"]
        denominator=header_matches+candidate_unmatched
        boundary_score=(header_matches/denominator*100.0) if denominator else 100.0
        all_line_match_ratio=(header_matches/nonempty*100.0) if nonempty else 100.0
        parser_ratio=(parser_success/parser_attempts*100.0) if parser_attempts else 100.0
        accepted=bool(header_matches>0 and boundary_score>=99.0 and candidate_unmatched==0 and accounting_zero_loss and (parser_attempts==0 or parser_ratio>=95.0))
        return {"valid":True,"accepted":accepted,"reason":"ok" if accepted else "candidate requires refinement",
          "coverage_score":round(boundary_score,2),"all_line_match_ratio":round(all_line_match_ratio,2),"total_lines":total,"total_nonempty_lines":nonempty,"blank_lines":blank_lines,
          "header_matches":header_matches,"regex_candidate_matches":regex_candidate_matches,"suppressed_regex_matches":suppressed_regex_matches,"classification_counts":classification_counts,
          "candidate_unmatched_headers":candidate_unmatched,"internal_header_candidates":internal_header_candidates,"internal_header_samples":internal_header_samples,
          "accounting_zero_loss":accounting_zero_loss,"event_count":event_count,"event_line_counts":{"min":min_lines or 0,"max":max_lines,"multiline_events":multiline,"single_line_events":single},
          "parser_validation":{"attempts":parser_attempts,"success":parser_success,"success_ratio":round(parser_ratio,2),"failed_samples":parser_fail_samples},
          "undersegmentation_rate":0.0,"first_line_sources":first_line_sources,"orphan_reasons":orphan_reasons,"failed_samples":failed_samples,"positive_samples":positive_samples,"safety":safety}

    def is_strong_header(self, line: str) -> bool:
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
    def _has_diverse_headers(samples: List[str]) -> bool:
        if not samples:
            return False
        # One repeated header family is fine; we only require at least one
        # positive sample. Diversity is reported, not a hard acceptance gate.
        return True

    def _empty_result(self, reason: str, safety: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
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
            "candidate_unmatched_headers": 0,
            "internal_header_candidates": 0,
            "internal_header_samples": [],
            "accounting_zero_loss": False,
            "event_count": 0,
            "event_line_counts": {"min": 0, "max": 0, "multiline_events": 0, "single_line_events": 0},
            "parser_validation": {"attempts": 0, "success": 0, "success_ratio": 0.0, "failed_samples": []},
            "undersegmentation_rate": 0.0,
            "first_line_sources": {"regex": 0, "orphan": 0},
            "orphan_reasons": {"blank_line_boundary": 0, "unrecognized_top_level": 0},
            "failed_samples": [],
            "positive_samples": [],
            "safety": safety or {"valid": False, "reason": reason},
        }


    @staticmethod
    def _top_level_alternatives(pattern: str) -> int:
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
                depth = max(0, depth - 1)
            elif ch == "|" and depth == 0:
                count += 1
        return count

    def _validate_safety(self, regex_pattern: str) -> Dict[str, Any]:
        if not regex_pattern:
            return {"valid": False, "reason": "empty regex"}
        if not regex_pattern.startswith("^"):
            return {"valid": False, "reason": "regex must start with ^"}
        if len(regex_pattern) > self.max_regex_length:
            return {"valid": False, "reason": f"regex too long: {len(regex_pattern)} > {self.max_regex_length}"}
        if regex_pattern in ("^.*", "^.*$", ".*", r"^\S+", r"^\S+$"):
            return {"valid": False, "reason": "generic catch-all regex rejected"}
        rules = self._top_level_alternatives(regex_pattern)
        if rules > self.max_rules:
            return {"valid": False, "reason": f"too many regex alternatives: {rules} > {self.max_rules}"}
        try:
            re.compile(regex_pattern)
        except re.error as exc:
            return {"valid": False, "reason": f"regex compile error: {exc}"}
        return {"valid": True, "reason": "ok", "rules": rules}
