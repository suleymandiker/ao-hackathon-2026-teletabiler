import re
from parser_layer.timestamp.timestamp_normalizer import TimestampNormalizer


class PlainTextParser:
    SEVERITY_PATTERN = re.compile(
        r'(?<![A-Za-z])(?:FATAL|CRITICAL|ERROR|WARNING|WARN|NOTICE|INFO|DEBUG|TRACE)(?![A-Za-z])',
        re.I,
    )

    def __init__(self):
        self.ts = TimestampNormalizer()

    def parse(self, log):
        if not log or not log.strip():
            return None

        event = log.strip()
        first_line = event.splitlines()[0]
        timestamp, raw_timestamp = self.ts.extract(first_line)

        # Conservative: inspect only the first/header line. Do not infer severity
        # from exception prose or continuation stacktrace lines.
        sev_match = self.SEVERITY_PATTERN.search(first_line[:512])
        severity = sev_match.group(0).upper() if sev_match else None
        if severity == "WARN":
            severity = "WARNING"

        return {
            "timestamp": timestamp,
            "raw_timestamp": raw_timestamp,
            "severity": severity,
            "message": event,
            "attributes": {},
        }
