# -*- coding: utf-8 -*-
import re

from parser_layer.timestamp.timestamp_normalizer import TimestampNormalizer


class PositionalStructuredParser:
    """
    Precision-first parser for fixed-column records whose structural envelope is:

        marker sequence YYYY.MM.DD location precise_timestamp same_location
        facility component severity message...

    The parser is intentionally generic:
    - no dataset names
    - no hard-coded RAS/KERNEL/APP literals
    - location columns must be identical
    - both date columns must describe the same calendar day
    - severity must occupy the structural ninth token
    - message must be non-empty

    Source-local timestamps are preserved as evidence; TimestampNormalizer may
    legitimately return None when no timezone is present.
    """

    SEVERITY = r"(?:FATAL|CRITICAL|ERROR|SEVERE|WARNING|WARN|NOTICE|INFO|DEBUG|TRACE)"

    RECORD = re.compile(
        rf"^(?P<marker>\S+)\s+"
        rf"(?P<sequence>\d+)\s+"
        rf"(?P<date>\d{{4}}\.\d{{2}}\.\d{{2}})\s+"
        rf"(?P<location>\S+)\s+"
        rf"(?P<raw_timestamp>\d{{4}}-\d{{2}}-\d{{2}}-\d{{2}}\.\d{{2}}\.\d{{2}}\.\d+)\s+"
        rf"(?P=location)\s+"
        rf"(?P<facility>[A-Za-z][A-Za-z0-9_.-]*)\s+"
        rf"(?P<component>[A-Za-z][A-Za-z0-9_.-]*)\s+"
        rf"(?P<severity>{SEVERITY})\s+"
        rf"(?P<message>\S.*)$",
        re.I,
    )

    def __init__(self):
        self.ts = TimestampNormalizer()

    @staticmethod
    def _same_calendar_day(date_dot, raw_timestamp):
        return date_dot.replace(".", "-") == raw_timestamp[:10]

    @staticmethod
    def _normalize_severity(value):
        value = value.upper()
        return "WARNING" if value == "WARN" else value

    def parse(self, log):
        if not log or not log.strip():
            return None

        event = log.strip()
        first_line, *continuation = event.splitlines()
        match = self.RECORD.match(first_line)
        if not match:
            return None

        gd = match.groupdict()
        if not self._same_calendar_day(gd["date"], gd["raw_timestamp"]):
            return None

        first_message = gd["message"].strip()
        message = (
            "\n".join([first_message, *continuation]).strip()
            if continuation
            else first_message
        )
        if not message:
            return None

        # The numeric sequence and marker are structural columns but their exact
        # semantics are not evidenced by the raw shape, so they remain attributes
        # rather than being mislabeled as pid/event_id/etc.
        attributes = {
            "record_marker": gd["marker"],
            "record_sequence": gd["sequence"],
            "record_date": gd["date"],
            "facility": gd["facility"],
        }

        return {
            "timestamp": self.ts.normalize(gd["raw_timestamp"]),
            "raw_timestamp": gd["raw_timestamp"],
            "severity": self._normalize_severity(gd["severity"]),
            "message": message,
            "host": None if gd["location"].upper() == "NULL" else gd["location"],
            "component": gd["component"],
            "attributes": attributes,
        }
