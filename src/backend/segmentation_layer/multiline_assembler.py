# -*- coding: utf-8 -*-
"""Context-aware deterministic streaming multiline event assembler."""
from __future__ import annotations

import re
from typing import Iterator, Dict, Any

from segmentation_layer.header_classifier import HeaderClassifier


class MultilineAssembler:
    def __init__(self, separator: str = "\n", classifier=None):
        self.separator = separator
        self.classifier = classifier or HeaderClassifier()

    def iter_events(self, file_path: str, regex_pattern: str) -> Iterator[str]:
        for record in self.iter_event_records(file_path, regex_pattern):
            yield record["event"]

    def iter_event_records(
        self,
        file_path: str,
        regex_pattern: str,
    ) -> Iterator[Dict[str, Any]]:
        """
        Stream physical lines and assemble logical events.

        Important blank-line rule:
        - A blank line is context, NOT an event boundary by itself.
        - It sets after_blank=True.
        - The next non-empty line is classified with that context.
        - A real/new header flushes the previous event.
        - A traceback/continuation after a blank remains attached to the
          current event.

        This keeps chained exceptions such as:

            ERROR ...
            Traceback ...
            ValueError ...

            During handling of the above exception ...

            Traceback ...
            ServiceUnavailableError ...

        as one logical event until the next real header arrives.
        """
        compiled = re.compile(regex_pattern) if regex_pattern else None

        current = []
        start_line = 0
        last_content_line = 0
        decisions = []
        after_blank = True

        def make():
            return {
                "event": self._join(current),
                "line_count": len(current),
                "start_line": start_line,
                "end_line": last_content_line,
                "first_line_is_header": bool(
                    decisions and decisions[0]["start_new_event"]
                ),
                "source": (
                    decisions[0]["classification"].lower()
                    if decisions
                    else "orphan"
                ),
                "decisions": list(decisions),
            }

        with open(
            file_path,
            "r",
            encoding="utf-8",
            errors="ignore",
        ) as f:
            for line_no, raw in enumerate(f, 1):
                line = raw.rstrip("\r\n")

                # Blank lines do NOT flush the event.
                # They only provide context for the next non-empty line.
                if not line.strip():
                    after_blank = True
                    continue

                d = self.classifier.classify(
                    line,
                    compiled,
                    has_current_event=bool(current),
                    after_blank=after_blank,
                )

                # A real header starts a new event. Flush the previous one.
                if d.start_new_event and current:
                    yield make()

                    current = []
                    decisions = []
                    start_line = 0
                    last_content_line = 0

                    # Reclassify at top-level so contextual rules see that
                    # there is no active event anymore.
                    d = self.classifier.classify(
                        line,
                        compiled,
                        has_current_event=False,
                        after_blank=after_blank,
                    )

                if not current:
                    start_line = line_no

                current.append(line)
                last_content_line = line_no

                decisions.append(
                    {
                        "line_no": line_no,
                        "classification": d.classification,
                        "start_new_event": d.start_new_event,
                        "regex_match": d.regex_match,
                        "reason": d.reason,
                        "confidence": d.confidence,
                    }
                )

                after_blank = False

        if current:
            yield make()

    def _join(self, lines):
        return self.separator.join(part for part in lines if part)
