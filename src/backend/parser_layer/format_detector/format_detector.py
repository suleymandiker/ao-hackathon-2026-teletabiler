import json
import re


class FormatDetector:
    def __init__(self):
        self.syslog_rfc3164_pattern = re.compile(
            r'^(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d+\s+\d{2}:\d{2}:\d{2}'
        )
        self.nginx_pattern = re.compile(
            r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\s+\S+\s+\S+\s+\['
        )
        self.app_log_iso_pattern = re.compile(
            r'^\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}'
        )
        self.app_log_us_pattern = re.compile(
            r'^\d{2}/\d{2}/\d{4}\s\d{2}:\d{2}:\d{2}'
        )
        self.klog_pattern = re.compile(
            r'^[IWEF]\d{4}\s\d{2}:\d{2}:\d{2}\.\d+'
        )
        self.vllm_pattern = re.compile(
            r'^(?:INFO|ERROR|WARN|WARNING|DEBUG|TRACE|FATAL|CRITICAL)\s+'
            r'\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\s+\['
        )

        # Generic "source-prefix + timestamp" support. The prefix is deliberately
        # bounded to one non-whitespace token so arbitrary prose is not promoted
        # to structured text merely because it contains a date later in the message.
        self.prefixed_iso_pattern = re.compile(
            r'^\S+\s+\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}(?:[.,]\d+)?'
        )

        # Fixed-column positional record. Detection intentionally validates only
        # the strong prefix; PositionalStructuredParser performs the stricter
        # equality/date consistency checks before accepting the event.
        self.positional_structured_pattern = re.compile(
            r'^\S+\s+\d+\s+\d{4}\.\d{2}\.\d{2}\s+\S+\s+'
            r'\d{4}-\d{2}-\d{2}-\d{2}\.\d{2}\.\d{2}\.\d+\s+\S+\s+'
            r'[A-Za-z][A-Za-z0-9_.-]*\s+[A-Za-z][A-Za-z0-9_.-]*\s+'
            r'(?:FATAL|CRITICAL|ERROR|SEVERE|WARNING|WARN|NOTICE|INFO|DEBUG|TRACE)\s+',
            re.I,
        )

    _KV_ASSIGNMENT = re.compile(
        r"(?<![A-Za-z0-9_.-])(?P<key>[A-Za-z_][A-Za-z0-9_.-]*)\s*=\s*"
    )

    def _looks_like_kv(self, log):
        """Recognize a KV envelope, not an arbitrary key=value inside message text."""
        matches = list(self._KV_ASSIGNMENT.finditer(log))
        if not matches:
            return False

        # Multiple assignments are strong evidence of a KV record regardless of
        # their exact values.
        if len(matches) >= 2:
            return True

        first = matches[0]
        prefix = log[: first.start()].strip()

        # A single assignment is accepted only near the record header. This keeps
        # message-body details such as "... unavailable state (HWID=1973)" from
        # hijacking routing while preserving ordinary "level=INFO message..." logs.
        if not prefix:
            return True
        return len(prefix) <= 64 and len(prefix.split()) <= 4

    def detect(self, log):
        log = log.strip()
        if not log:
            return None

        if log.startswith("{") and self._is_json(log):
            return "json"

        if log.startswith("<") or self.syslog_rfc3164_pattern.match(log):
            return "syslog"

        if self.positional_structured_pattern.match(log):
            return "positional_structured"

        if (
            log.startswith("[")
            or "|" in log
            or self.nginx_pattern.match(log)
            or self.app_log_iso_pattern.match(log)
            or self.app_log_us_pattern.match(log)
            or self.klog_pattern.match(log)
            or self.vllm_pattern.match(log)
            or self.prefixed_iso_pattern.match(log)
        ):
            return "structured_text"

        if not log.startswith("{") and self._looks_like_kv(log):
            return "kv"

        return "plain_text"

    def _is_json(self, log):
        try:
            json.loads(log)
            return True
        except ValueError:
            return False
