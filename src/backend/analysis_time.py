"""Source-time coordinates and invocation-local finite analysis diagnostics.

Observation/retrieval clocks never supply event time. Infinity is used only in
sort keys to place unknown times last; it is never serialized as a timestamp.
"""
from dataclasses import dataclass
import math

from parser_layer.timestamp.timestamp_normalizer import TimestampNormalizer


def source_time_ms(value):
    """Use the parser's existing timestamp contract without inventing a timezone."""
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = TimestampNormalizer().normalize(value)
        return int(parsed.timestamp() * 1000) if parsed is not None else None
    except (ValueError, TypeError, OverflowError, OSError):
        return None


def signal_time(row, field='first_seen_ms'):
    value = row.get(field)
    if row.get('timestamp_resolved') is False:
        return None
    return value if type(value) in (int, float) and math.isfinite(value) else None


def time_key(row, field='first_seen_ms'):
    value = signal_time(row, field)
    return value if value is not None else math.inf


def order_key(row):
    """An explicit source ordinal is evidence; container iteration is not."""
    ordinal = row.get('source_order')
    return (ordinal if type(ordinal) is int else math.inf,
            str(row.get('signal_id') or row.get('event_id') or ''))


def signal_order(row):
    return (time_key(row), *order_key(row))


def time_span(rows):
    firsts = [signal_time(row) for row in rows]
    lasts = [signal_time(row, 'last_seen_ms') for row in rows]
    if any(value is None for value in firsts + lasts):
        return None, None
    return min(firsts), max(lasts)


@dataclass(frozen=True)
class AnalysisTimeContext:
    analysis_reference_time_ms: int | None

    @property
    def timestamp_basis(self):
        return 'source' if self.analysis_reference_time_ms is not None else 'unresolved'

    def to_dict(self):
        return dict(analysis_reference_time_ms=self.analysis_reference_time_ms,
                    timestamp_basis=self.timestamp_basis)
