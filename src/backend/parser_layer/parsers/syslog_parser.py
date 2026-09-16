import re

from parser_layer.timestamp.timestamp_normalizer import TimestampNormalizer


class SyslogParser:
    """Deterministic RFC5424 / RFC3164 parser.

    The parser only promotes severity when the raw event provides evidence:
    RFC5424/RFC3164 PRI, or an explicit textual severity token in the message.
    Traditional RFC3164 timestamps without year/timezone are preserved as
    nonabsolute evidence and therefore normalize to None under the parser's
    timestamp contract.
    """

    SEVERITY_MAP = {
        0: "EMERGENCY",
        1: "ALERT",
        2: "CRITICAL",
        3: "ERROR",
        4: "WARNING",
        5: "NOTICE",
        6: "INFO",
        7: "DEBUG",
    }

    TEXT_SEVERITY_RE = re.compile(
        r"(?<![A-Za-z])"
        r"(FATAL|CRITICAL|ERROR|WARNING|WARN|NOTICE|INFO|DEBUG|TRACE)"
        r"(?![A-Za-z])",
        re.I,
    )

    # Same conservative key=value semantics used by the generic KV parser:
    # quoted values may contain spaces; unquoted values stop at whitespace or
    # a common closing wrapper.
    KV_PATTERN = re.compile(
        r'([A-Za-z_][A-Za-z0-9_.-]*)=(".*?"|\'.*?\'|[^\s,)\]}]+)'
    )

    def __init__(self):
        self.ts = TimestampNormalizer()

        # RFC 5424-ish modern syslog. Keep compatibility with the project's
        # existing accepted shape while anchoring at the start of the event.
        self.rfc5424_regex = re.compile(
            r"^\s*<(\d+)>v?\d?\s+([\d\-T:\.Z\+]+)\s+"
            r"(\S+)\s+(\S+)\s+(.*)"
        )

        # Traditional RFC 3164 Linux syslog. PRI is optional.
        self.rfc3164_regex = re.compile(
            r"^\s*(?:<(\d+)>)?\s*"
            r"([A-Z][a-z]{2}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2})\s+"
            r"(\S+)\s+([^:]+):\s+(.*)"
        )

    def parse(self, log):
        if not log:
            return None

        log = log.strip()
        if not log:
            return None

        match = self.rfc5424_regex.search(log)
        if match:
            priority = int(match.group(1))
            timestamp_raw = match.group(2)
            host = match.group(3)
            app_name = match.group(4)
            message_part = match.group(5)
            return self._build_payload(
                timestamp_raw=timestamp_raw,
                severity_code=priority % 8,
                message_part=message_part,
                host=host,
                app_name=app_name,
                priority_val=priority,
            )

        match = self.rfc3164_regex.search(log)
        if match:
            priority_str = match.group(1)
            priority_val = int(priority_str) if priority_str else None
            severity_code = priority_val % 8 if priority_val is not None else None

            return self._build_payload(
                timestamp_raw=match.group(2),
                severity_code=severity_code,
                message_part=match.group(5),
                host=match.group(3),
                app_name=match.group(4),
                priority_val=priority_val,
            )

        return None

    @staticmethod
    def _convert_attribute_value(value):
        text = value

        if (
            len(text) >= 2
            and text[0] == text[-1]
            and text[0] in ('"', "'")
        ):
            text = text[1:-1]

        if text.isdigit():
            # Preserve numeric-looking identifiers/values with leading zeros.
            # "0" is safely numeric, but "02345000" must remain exactly observed.
            if len(text) > 1 and text.startswith("0"):
                return text
            return int(text)

        try:
            return float(text)
        except ValueError:
            return text

    def _extract_attributes(self, message_part):
        attributes = {}
        for key, value in self.KV_PATTERN.findall(message_part or ""):
            # Empty assignments such as "logname=" or "ruser=" are intentionally
            # not captured by KV_PATTERN; absent information is not invented.
            #
            # If the same key occurs more than once in one message, preserve the
            # first observed value. A later occurrence must not silently overwrite
            # earlier evidence.
            if key not in attributes:
                attributes[key] = self._convert_attribute_value(value)
        return attributes

    def _textual_severity(self, message_part):
        match = self.TEXT_SEVERITY_RE.search(message_part or "")
        if not match:
            return None

        severity = match.group(1).upper()
        return "WARNING" if severity == "WARN" else severity

    def _build_payload(
        self,
        timestamp_raw,
        severity_code,
        message_part,
        host,
        app_name,
        priority_val,
    ):
        severity = (
            self._map_severity(severity_code)
            if severity_code is not None
            else self._textual_severity(message_part)
        )

        attributes = {
            "syslog_app_name": app_name.strip() if app_name else None,
            "syslog_priority": priority_val,
        }
        attributes.update(self._extract_attributes(message_part))

        return {
            "timestamp": self.ts.normalize(timestamp_raw) if timestamp_raw else None,
            "severity": severity,
            "message": f"{app_name.strip()}: {message_part.strip()}",
            "host": host.strip() if host else None,
            "attributes": attributes,
        }

    def _map_severity(self, code):
        if code is None:
            return None
        return self.SEVERITY_MAP.get(code)
