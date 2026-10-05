from datetime import datetime, timezone
import re


FORMATS = (
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S,%f",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M:%S %z",
    "%Y-%m-%dT%H:%M:%S.%f%z",
    "%Y-%m-%dT%H:%M:%S%z",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S",
    "%d/%b/%Y:%H:%M:%S %z",
    "%m/%d/%Y %H:%M:%S.%f",
    "%m/%d/%Y %H:%M:%S",
)

HEALTHAPP_NONABSOLUTE_RE = re.compile(r"^\\d{8}-\\d{1,2}:\\d{1,2}:\\d{1,2}:\\d{1,3}(?=\\|)")

# Source-local timestamp evidence. These patterns intentionally participate in
# extract() even when normalize() must return None because timezone/year evidence
# is incomplete. Parser layers may still use the raw span to remove a structural
# timestamp header without inventing an absolute CanonicalEvent timestamp.
APACHE_BRACKET_CLOCK_RE = re.compile(
    r"(?<=\[)(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)\s+"
    r"(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+"
    r"\d{1,2}\s+\d{2}:\d{2}:\d{2}\s+\d{4}(?=\])"
)

MONTH_DAY_CLOCK_RE = re.compile(
    r"(?<=\[)\d{1,2}\.\d{1,2}\s+\d{2}:\d{2}:\d{2}(?=\])"
)


TIMESTAMP_SEARCH_PATTERNS = (
    HEALTHAPP_NONABSOLUTE_RE,
    APACHE_BRACKET_CLOCK_RE,
    MONTH_DAY_CLOCK_RE,
    # Keep the entire explicit timezone for both ISO date/time separators.
    # Dropping Z or an adjacent offset turns a resolvable instant into naive time.
    # A following word such as "ZooKeeper" is not a Z timezone token.
    re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:[.,]\d+)?(?:\s*(?:Z|[+-]\d{2}:?\d{2})(?![\w:+-]))?"),
    re.compile(r"\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}(?:[.,]\d+)?(?:\s*(?:Z|[+-]\d{2}:?\d{2})(?![\w:+-]))?"),
    re.compile(r"\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2}(?:\.\d+)?"),
    re.compile(r"\d{1,2}/[A-Z][a-z]{2}/\d{4}:\d{2}:\d{2}:\d{2}(?:\s+[+-]\d{4})?"),
)


class TimestampNormalizer:
    """Normalize parsed timestamps to timezone-aware UTC ``datetime`` values."""

    def normalize(self, ts):
        if ts is None:
            return None

        if isinstance(ts, datetime):
            if ts.tzinfo is None:
                return ts.replace(tzinfo=timezone.utc)
            return ts.astimezone(timezone.utc)

        if isinstance(ts, (int, float)):
            value = float(ts)
            if value > 10_000_000_000:
                value /= 1000.0
            return datetime.fromtimestamp(value, tz=timezone.utc)

        text = str(ts).strip()
        if not text:
            return None

        # HealthApp source-local clock: YYYYMMDD-H:M:S:fff. It contains a
        # calendar date and clock time but no timezone/offset. Preserve it as
        # raw timestamp evidence only; CanonicalEvent.timestamp must remain None.
        if HEALTHAPP_NONABSOLUTE_RE.fullmatch(text):
            return None

        if re.fullmatch(r"\d+(?:\.\d+)?", text):
            value = float(text)
            if value > 10_000_000_000:
                value /= 1000.0
            return datetime.fromtimestamp(value, tz=timezone.utc)

        iso_text = text[:-1] + "+00:00" if text.endswith("Z") else text
        try:
            dt = datetime.fromisoformat(iso_text)
            # A calendar/clock value without an explicit timezone is source-local
            # evidence, not an absolute instant. Never invent UTC here.
            if dt.tzinfo is None:
                return None
            return dt.astimezone(timezone.utc)
        except ValueError:
            pass

        for fmt in FORMATS:
            try:
                dt = datetime.strptime(text, fmt)
                if dt.tzinfo is None:
                    return None
                return dt.astimezone(timezone.utc)
            except ValueError:
                continue
        return None

    def extract(self, log):
        """Return ``(datetime | None, raw_timestamp | None)`` from header line."""
        if not log:
            return None, None
        first_line = str(log).splitlines()[0]
        for pattern in TIMESTAMP_SEARCH_PATTERNS:
            match = pattern.search(first_line)
            if match:
                raw = match.group(0)
                return self.normalize(raw), raw
        return None, None
