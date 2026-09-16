# -*- coding: utf-8 -*-
"""Context-aware deterministic streaming multiline event assembler."""
from __future__ import annotations
import re
from typing import Iterator, Dict, Any
from segmentation_layer.header_classifier import HeaderClassifier

class MultilineAssembler:
    def __init__(self, separator: str = " | ", classifier=None):
        self.separator = separator
        self.classifier = classifier or HeaderClassifier()

    def iter_events(self, file_path: str, regex_pattern: str) -> Iterator[str]:
        for record in self.iter_event_records(file_path, regex_pattern):
            yield record["event"]

    def iter_event_records(self, file_path: str, regex_pattern: str) -> Iterator[Dict[str, Any]]:
        compiled = re.compile(regex_pattern) if regex_pattern else None
        current=[]; start_line=0; decisions=[]; after_blank=True
        def make(end_line):
            return {"event": self._join(current), "line_count": len(current), "start_line": start_line, "end_line": end_line,
                    "first_line_is_header": bool(decisions and decisions[0]["start_new_event"]),
                    "source": decisions[0]["classification"].lower() if decisions else "orphan", "decisions": list(decisions)}
        with open(file_path,'r',encoding='utf-8',errors='ignore') as f:
            for line_no, raw in enumerate(f,1):
                line=raw.rstrip('\r\n')
                if not line.strip():
                    if current:
                        yield make(line_no-1); current=[]; decisions=[]; start_line=0
                    after_blank=True; continue
                d=self.classifier.classify(line, compiled, has_current_event=bool(current), after_blank=after_blank)
                if d.start_new_event and current:
                    yield make(line_no-1); current=[]; decisions=[]; start_line=0
                    # Reclassify at top-level so contextual rules see no active event.
                    d=self.classifier.classify(line, compiled, has_current_event=False, after_blank=after_blank)
                if not current: start_line=line_no
                current.append(line.strip())
                decisions.append({"line_no":line_no,"classification":d.classification,"start_new_event":d.start_new_event,"regex_match":d.regex_match,"reason":d.reason,"confidence":d.confidence})
                after_blank=False
        if current: yield make(line_no if 'line_no' in locals() else 0)

    def _join(self, lines):
        return self.separator.join(part for part in lines if part)
