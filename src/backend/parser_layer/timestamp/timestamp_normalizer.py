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

    @staticmethod
    def _absolute(dt, source_timezone):
        if dt.utcoffset() is not None:
            return dt.astimezone(timezone.utc)
        if source_timezone is None:
            return None
        # Require an unambiguous real local instant. Never choose a DST fold or
        # normalize a nonexistent clock silently, and never consult machine time.
        candidates = set()
        for fold in (0, 1):
            candidate = dt.replace(tzinfo=source_timezone, fold=fold).astimezone(timezone.utc)
            if candidate.astimezone(source_timezone).replace(tzinfo=None) == dt:
                candidates.add(candidate)
        return next(iter(candidates)) if len(candidates) == 1 else None

    def normalize(self, ts, *, source_timezone=None):
        if ts is None or isinstance(ts, bool):
            return None

        if isinstance(ts, datetime):
            return self._absolute(ts, source_timezone)

        if isinstance(ts, (int, float)):
            value = float(ts)
            if value > 10_000_000_000:
                value /= 1000.0
            return datetime.fromtimestamp(value, tz=timezone.utc)

        text = str(ts).strip()
        if not text:
            return None

        if re.fullmatch(r"\d+(?:\.\d+)?", text):
            value = float(text)
            if value > 10_000_000_000:
                value /= 1000.0
            return datetime.fromtimestamp(value, tz=timezone.utc)

        iso_text = text[:-1] + "+00:00" if text.endswith("Z") else text
        try:
            dt = datetime.fromisoformat(iso_text)
            # A date alone does not establish an occurrence clock.
            if dt.utcoffset() is None and not re.search(r'[T ]\d{2}:\d{2}:\d{2}', text):
                return None
            return self._absolute(dt, source_timezone)
        except ValueError:
            pass

        for fmt in (*FORMATS, "%Y%m%d-%H:%M:%S:%f", "%Y-%m-%d-%H.%M.%S.%f",
                    "%a %b %d %H:%M:%S %Y"):
            try:
                dt = datetime.strptime(text, fmt)
                return self._absolute(dt, source_timezone)
            except ValueError:
                continue
        return None

    def extract(self, log):
        """Read event time only at the header start, optionally bracketed or
        severity-prefixed. Pattern priority must never select a payload date.
        Other producer-prefix layouts require an explicit parser/policy contract.
        """
        if not log:
            return None, None
        first_line = str(log).splitlines()[0].lstrip()
        prefix = re.match(r'(?:\[(?:INFO|ERROR|WARN|WARNING|DEBUG|TRACE|FATAL|CRITICAL)\]|'
                          r'INFO|ERROR|WARN|WARNING|DEBUG|TRACE|FATAL|CRITICAL)\s*[:|\-]?\s+', first_line)
        start = prefix.end() if prefix else 0
        if first_line[start:start + 1] == '[':
            start += 1
        for pattern in TIMESTAMP_SEARCH_PATTERNS:
            match = pattern.match(first_line, start)
            if match:
                raw = match.group(0)
                return self.normalize(raw), raw
        return None, None

    def legacy_message_span(self, log):
        """Compatibility span for existing message/template extraction ONLY.

        This historical search can match payload dates. Its result must never
        supply occurrence time. Retain it here to avoid changing message/template
        identity as a side effect of the independent timestamp-authority fix.
        """
        if not log:
            return None
        first_line = str(log).splitlines()[0]
        for pattern in TIMESTAMP_SEARCH_PATTERNS:
            match = pattern.search(first_line)
            if match:
                return match.group(0)
        return None
