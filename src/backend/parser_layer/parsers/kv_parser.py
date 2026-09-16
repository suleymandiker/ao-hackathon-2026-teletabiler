import re

from parser_layer.timestamp.timestamp_normalizer import TimestampNormalizer


class KVParser:
    def __init__(self):
        self.ts = TimestampNormalizer()

        # Generic key=value extraction.
        #
        # Unquoted values stop at whitespace or a common closing wrapper so
        # constructs such as:
        #   (HWID=1973)
        #   [status=503]
        #   {node=node-1}
        # produce 1973 / 503 / node-1 rather than leaking ), ] or } into value.
        #
        # Quoted values are kept intact and may contain spaces.
        self.kv_pattern = re.compile(
            r'([a-zA-Z0-9_]+)=(".*?"|\'.*?\'|[^\s,)\]}]+)'
        )

    @staticmethod
    def _convert_value(value):
        """Remove matching quotes and conservatively normalize scalar types."""
        clean_value = value

        if (
            len(clean_value) >= 2
            and clean_value[0] == clean_value[-1]
            and clean_value[0] in ('"', "'")
        ):
            clean_value = clean_value[1:-1]

        if clean_value.isdigit():
            # Preserve observed leading-zero scalar text (except plain "0").
            if len(clean_value) > 1 and clean_value.startswith("0"):
                return clean_value
            return int(clean_value)

        try:
            return float(clean_value)
        except ValueError:
            return clean_value

    def parse(self, log):
        if not log:
            return None

        log = log.strip()
        if not log:
            return None

        fields = {}

        # 1. Deterministic KV extraction.
        for key, value in self.kv_pattern.findall(log):
            # Preserve the first observed value when a key repeats in one event.
            if key not in fields:
                fields[key] = self._convert_value(value)

        # 2. Promote only explicitly named standard fields.
        raw_time = fields.pop("timestamp", None)
        if raw_time is None:
            raw_time = fields.pop("time", None)

        severity = fields.pop("level", None)
        if severity is None:
            severity = fields.pop("severity", None)

        # Do not invent INFO merely because a line contains some key=value pair.
        # If no explicit KV severity exists, accept only a strong bracketed token.
        if severity is None:
            sev_match = re.search(
                r'\[(ERROR|WARN|WARNING|INFO|DEBUG|CRITICAL|FATAL)\]',
                log,
                re.I,
            )
            severity = sev_match.group(1) if sev_match else None

        message = fields.pop("msg", None)
        if message is None:
            message = fields.pop("message", None)

        if message is None:
            message = log.split(" - ")[-1] if " - " in log else log

        normalized_severity = None
        if severity is not None:
            normalized_severity = str(severity).upper().strip('"\' ')
            if normalized_severity == "WARN":
                normalized_severity = "WARNING"

        return {
            "timestamp": self.ts.normalize(str(raw_time)) if raw_time is not None else None,
            "severity": normalized_severity,
            "message": str(message).strip('"\' '),
            "attributes": fields,
        }
