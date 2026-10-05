"""Source-supplied occurrence-time context; no global or machine timezone state."""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from parser_layer.timestamp.timestamp_normalizer import TimestampNormalizer


TimestampBasis = Literal['message_explicit', 'message_source_timezone', 'source_record', 'untimed']
BASES = ('message_explicit', 'message_source_timezone', 'source_record', 'untimed')


@dataclass(frozen=True)
class TimestampSourcePolicy:
    """Immutable policy supplied for one source analysis, never a parser default."""
    source_timezone: str | None = None
    zone: ZoneInfo | None = field(init=False, repr=False, compare=False)

    def __post_init__(self):
        try:
            zone = ZoneInfo(self.source_timezone) if self.source_timezone is not None else None
        except (ZoneInfoNotFoundError, ValueError, TypeError):
            raise ValueError('source_timezone must be a valid IANA timezone or None') from None
        object.__setattr__(self, 'zone', zone)


@dataclass(frozen=True)
class TimestampContext:
    policy: TimestampSourcePolicy = field(default_factory=TimestampSourcePolicy)
    source_record_time: datetime | str | int | float | None = None
    source_record_raw: str | int | float | None = None
    source_record_field: str | None = None


def _normalize(value, zone=None):
    try:
        return TimestampNormalizer().normalize(value, source_timezone=zone)
    except (ValueError, OverflowError, OSError, TypeError):
        return None


def _evidence(value):
    # A timestamp capture is scalar evidence, never duplicate nested payloads.
    if isinstance(value, datetime):
        return value.isoformat()
    return value if type(value) in (str, int, float) else None


@dataclass(frozen=True)
class EventTime:
    timestamp: datetime | None
    basis: TimestampBasis
    message_timestamp_raw: object
    context: TimestampContext
    source_record_time: datetime | None

    def provenance(self):
        return {
            'basis': self.basis,
            'message_timestamp_raw': _evidence(self.message_timestamp_raw),
            'source_timezone': self.context.policy.source_timezone,
            'source_record_time': self.source_record_time.isoformat() if self.source_record_time else None,
            'source_record_timestamp_raw': _evidence(self.context.source_record_raw),
            'source_record_field': self.context.source_record_field,
        }


def resolve_event_time(message_time, context=None):
    """Resolve an already-authoritative parser capture; never search raw payloads.

    Explicit message instant > policy-resolved message clock > source-record
    absolute instant > untimed. Source policy never localizes acquisition clocks.
    """
    context = context or TimestampContext()
    record_time = _normalize(context.source_record_time)
    if record_time is None:
        record_time = _normalize(context.source_record_raw)
    timestamp = _normalize(message_time)
    basis: TimestampBasis = 'message_explicit'
    if timestamp is None:
        timestamp = _normalize(message_time, context.policy.zone) if context.policy.zone else None
        basis = 'message_source_timezone'
    if timestamp is None:
        timestamp, basis = record_time, 'source_record'
    if timestamp is None:
        basis = 'untimed'
    return EventTime(timestamp, basis, message_time, context, record_time)
