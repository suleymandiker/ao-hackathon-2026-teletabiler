"""Diagnostic evidence only: never fills canonical timestamp or invents context."""
import json
import re
from datetime import datetime

from parser_layer.timestamp.timestamp_normalizer import TimestampNormalizer, FORMATS

_KEYS = ("timestamp", "time", "@timestamp")
_CLOCK = r"\d{1,2}:\d{2}:\d{2}(?:[.,]\d+)?"
_HEADER = re.compile(
    r"^\s*(?:<\d+>(?:\d+\s+)?)?\[?(?P<time>"
    r"\d{4}-\d{2}-\d{2}[T ]" + _CLOCK + r"(?:\s?(?:Z|[+-]\d{2}:?\d{2}))?"
    r"|\d{2}/\d{2}/\d{4}\s+" + _CLOCK +
    r"|\d{1,2}/[A-Za-z]{3}/\d{4}:" + _CLOCK + r"(?:\s+[+-]\d{4})?"
    r"|\d{8}-\d{1,2}:\d{1,2}:\d{1,2}:\d{1,3}"
    r"|[A-Za-z]{3}\s+\d{1,2}\s+" + _CLOCK +
    r"|\d{2}-\d{2}\s+" + _CLOCK +
    r"|" + _CLOCK + r")(?=$|\s|\]|\|)"
)
_KV = re.compile(r'''^(?:timestamp|time|@timestamp)=(?:"([^"]*)"|'([^']*)'|(\S+))''')


def _raw_value(raw, explicit):
    if explicit is not None:
        return explicit
    text = str(raw or "").strip()
    if text.startswith("{"):
        try:
            data = json.loads(text)
            if isinstance(data, dict):
                for key in _KEYS:
                    if key in data:
                        return data[key]
        except (ValueError, TypeError):
            pass
        # Never discover event time inside a JSON message body.
        return None
    header = text.partition("\n")[0]
    match = _KV.match(header)
    if match:
        return next(value for value in match.groups() if value is not None)
    match = _HEADER.match(header)
    return match.group("time") if match else None


def _status(value):
    if value is None or (isinstance(value, str) and not value.strip()):
        return "missing"
    if not isinstance(value, (str, int, float, datetime)) or isinstance(value, bool):
        return "invalid"
    if isinstance(value, datetime) and value.utcoffset() is None:
        return "timezone_missing"
    text = str(value).strip()
    # Recognized but not promoted by the chosen parser: do not call this missing
    # timezone and do not silently override its timestamp extraction contract.
    try:
        if TimestampNormalizer().normalize(value) is not None:
            return "unparsed"
    except (ValueError, OverflowError, OSError, TypeError):
        return "invalid"
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return "timezone_missing"
    except ValueError:
        pass
    for fmt in (*FORMATS, "%Y%m%d-%H:%M:%S:%f", "%d/%b/%Y:%H:%M:%S"):
        try:
            parsed = datetime.strptime(text, fmt)
            return "timezone_missing" if parsed.tzinfo is None else "unparsed"
        except ValueError:
            pass
    for fmt in ("%b %d %H:%M:%S", "%m-%d %H:%M:%S.%f", "%m-%d %H:%M:%S"):
        try:
            # Leap-year sentinel validates the calendar, never supplies event time.
            datetime.strptime("2000 " + text, "%Y " + fmt)
            return "year_missing"
        except ValueError:
            pass
    for fmt in ("%H:%M:%S.%f", "%H:%M:%S"):
        try:
            datetime.strptime(text.replace(",", "."), fmt)
            return "date_missing"
        except ValueError:
            pass
    return "invalid"


def unresolved_timestamp_evidence(raw, explicit=None):
    value = _raw_value(raw, explicit)
    return {"raw_timestamp": value, "timestamp_status": _status(value)}
