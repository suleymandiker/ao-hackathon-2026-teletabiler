# -*- coding: utf-8 -*-
"""Shared deterministic boundary state and the compatible file adapter."""
from __future__ import annotations

import re
from typing import Iterator, Dict, Any

from segmentation_layer.contracts import AssemblyEvent, AssemblyStep, LineEvidence
from segmentation_layer.header_classifier import CONTINUATION, HeaderClassifier, LineDecision


class MultilineAssemblyState:
    """One stream's analysis-local state; input strings are opaque units.

    feed() never treats iterable/page exhaustion as a boundary. flush() is an
    explicit cut, is idempotent, and permits reuse with fresh boundary context.
    Ordinals count feed calls, including blanks; they are not source positions.
    Only a pending event is retained; its size is not subject to a hard bound.

    Injected classifiers retain their existing classify() hook and are
    responsible for their own purity. The default uses classify_raw().
    """

    def __init__(self, regex_pattern: str, *, separator: str = "\n", classifier=None):
        self._regex = re.compile(regex_pattern) if regex_pattern else None
        self._separator = separator
        self._classifier = classifier or HeaderClassifier()
        self._ordinal = 0
        self._current: list[str] = []
        self._evidence: list[LineEvidence] = []
        self._after_blank = True

    @property
    def has_pending(self) -> bool:
        return bool(self._current)

    def _classify(self, line: str, has_current_event: bool) -> LineDecision:
        # Preserve custom/subclass hooks accepted by the legacy file facade.
        # Default record processing never invokes strip/rstrip classification.
        classify = (self._classifier.classify_raw
                    if type(self._classifier) is HeaderClassifier
                    else self._classifier.classify)
        return classify(line, self._regex, has_current_event=has_current_event,
                        after_blank=self._after_blank)

    def feed(self, line: str) -> AssemblyStep:
        """Consume exactly one input unit and optionally emit its predecessor."""
        if not isinstance(line, str):
            raise TypeError("line must be str")
        self._ordinal += 1
        if not line or line.isspace():
            self._after_blank = True
            evidence = LineEvidence(
                self._ordinal, LineDecision(CONTINUATION, False, False, "blank", 1.0), False,
            )
            if self.has_pending:
                self._evidence.append(evidence)
            return AssemblyStep(evidence)

        decision = self._classify(line, self.has_pending)
        completed = None
        if decision.start_new_event and self.has_pending:
            completed = self._take_event()
            # Keep blank context while reclassifying at top level, as before.
            decision = self._classify(line, False)

        evidence = LineEvidence(self._ordinal, decision, True)
        self._current.append(line)
        self._evidence.append(evidence)
        self._after_blank = False
        return AssemblyStep(evidence, completed)

    def _take_event(self) -> AssemblyEvent:
        event = AssemblyEvent(self._separator.join(self._current), tuple(self._evidence))
        self._current.clear()
        self._evidence.clear()
        return event

    def flush(self) -> AssemblyEvent | None:
        """Release the pending event once; do not reset ordinal identity."""
        event = self._take_event() if self.has_pending else None
        self._after_blank = True
        return event


class MultilineAssembler:
    def __init__(self, separator: str = "\n", classifier=None):
        self.separator = separator
        self.classifier = classifier or HeaderClassifier()

    def new_state(self, regex_pattern: str) -> MultilineAssemblyState:
        """Create independent state; never store pending events on this facade."""
        return MultilineAssemblyState(regex_pattern, separator=self.separator, classifier=self.classifier)

    def iter_events(self, file_path: str, regex_pattern: str) -> Iterator[str]:
        for record in self.iter_event_records(file_path, regex_pattern):
            yield record["event"]

    def iter_event_records(
        self,
        file_path: str,
        regex_pattern: str,
    ) -> Iterator[Dict[str, Any]]:
        """Read a finite file with legacy decoding, blank omission and EOF flush.

        Every physical line is fed once. Only this adapter translates internal
        ordinals into real file line numbers and removes file line terminators.
        Blank lines provide context without flushing or appearing in event text.
        """
        state = self.new_state(regex_pattern)
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            for raw in f:
                step = state.feed(raw.rstrip("\r\n"))
                if step.completed is not None:
                    yield self._file_record(step.completed)
        tail = state.flush()
        if tail is not None:
            yield self._file_record(tail)

    @staticmethod
    def _file_record(event: AssemblyEvent) -> Dict[str, Any]:
        retained = [item for item in event.evidence if item.included]
        decisions = [
            dict(line_no=item.ordinal, classification=item.decision.classification,
                 start_new_event=item.decision.start_new_event, regex_match=item.decision.regex_match,
                 reason=item.decision.reason, confidence=item.decision.confidence)
            for item in retained
        ]
        return {
            "event": event.text,
            "line_count": len(retained),
            "start_line": retained[0].ordinal,
            "end_line": retained[-1].ordinal,
            "first_line_is_header": bool(decisions[0]["start_new_event"]),
            "source": decisions[0]["classification"].lower(),
            "decisions": decisions,
        }

    def _join(self, lines):
        return self.separator.join(part for part in lines if part)
