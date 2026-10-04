"""Immutable acquisition data, independent of parsing and provider APIs.

These contracts carry explicitly supplied values only. They do not infer stream
identity, normalize text/timestamps, generate event IDs, or perform I/O.
Containers must be tuples with immutable contents; mutable inputs are rejected
rather than silently converted. Provider mappings and persistence are separate.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum


Scalar = str | int | float | bool | None
_SCALAR_TYPES = (str, int, float, bool, type(None))
_OPTIONAL_TEXT_TYPES = (str, type(None))


def _require_type(value, allowed_types, name):
    """Reject unsupported/mutable values without coercing source evidence."""
    if type(value) not in allowed_types:
        expected = " or ".join(kind.__name__ for kind in allowed_types)
        raise TypeError(f"{name} must be {expected}")


@dataclass(frozen=True, slots=True)
class StreamIdentity:
    """Identity coordinates for one independently assemblable stream.

    Workload is descriptive metadata, excluded from equality and hashing.
    Missing coordinates remain None; equality does not establish that an
    incomplete identity is safe for assembly. No ordering is inferred.
    """

    source_scope: str
    namespace: str | None = None
    workload: str | None = field(default=None, compare=False)
    pod: str | None = None
    pod_instance: str | None = None
    container: str | None = None
    container_instance: str | None = None
    channel: str | None = None

    def __post_init__(self):
        _require_type(self.source_scope, (str,), "source_scope")
        for name in (
            "namespace", "workload", "pod", "pod_instance", "container",
            "container_instance", "channel",
        ):
            _require_type(getattr(self, name), _OPTIONAL_TEXT_TYPES, name)


@dataclass(frozen=True, slots=True)
class SourceReference:
    """Provider-supplied retrieval coordinates, not a canonical event ID.

    source_partition names a concrete collection/partition in the source scope.
    The provider must establish generation/version and deduplication semantics;
    equality here only compares the supplied coordinates.
    """

    source_scope: str
    source_partition: str
    record_id: str
    generation: str | None = None
    version: str | int | None = None

    def __post_init__(self):
        for name in ("source_scope", "source_partition", "record_id"):
            _require_type(getattr(self, name), (str,), name)
        _require_type(self.generation, _OPTIONAL_TEXT_TYPES, "generation")
        _require_type(self.version, (str, int, type(None)), "version")


class Framing(str, Enum):
    PHYSICAL_LINE = "PHYSICAL_LINE"
    LOGICAL_EVENT = "LOGICAL_EVENT"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True, slots=True)
class IngestedLogRecord:
    """One retrieved record with exact text and explicit acquisition evidence.

    raw_text must be a string, including when empty. Timestamp values are carried
    without parsing, timezone conversion, or assigning an observation clock.
    retrieval_order is an opaque tuple of built-in scalar values. metadata is
    an ordered tuple of (string key, scalar value) pairs, not a mutable mapping;
    nested metadata is intentionally unsupported in this first version.
    """

    raw_text: str
    source_reference: SourceReference
    source_timestamp_raw: Scalar = None
    source_timestamp: datetime | None = None
    stream_identity: StreamIdentity | None = None
    retrieval_order: tuple[Scalar, ...] | None = None
    first_observed_at: datetime | None = None
    framing: Framing = Framing.UNKNOWN
    metadata: tuple[tuple[str, Scalar], ...] = ()
    mapping_version: str | None = None

    def __post_init__(self):
        _require_type(self.raw_text, (str,), "raw_text")
        _require_type(self.source_reference, (SourceReference,), "source_reference")
        _require_type(self.source_timestamp_raw, _SCALAR_TYPES, "source_timestamp_raw")
        for name in ("source_timestamp", "first_observed_at"):
            _require_type(getattr(self, name), (datetime, type(None)), name)
        _require_type(self.stream_identity, (StreamIdentity, type(None)), "stream_identity")
        _require_type(self.retrieval_order, (tuple, type(None)), "retrieval_order")
        if self.retrieval_order is not None:
            for value in self.retrieval_order:
                _require_type(value, _SCALAR_TYPES, "retrieval_order item")
        _require_type(self.framing, (Framing,), "framing")
        _require_type(self.metadata, (tuple,), "metadata")
        for item in self.metadata:
            _require_type(item, (tuple,), "metadata item")
            if len(item) != 2:
                raise ValueError("metadata items must be (key, value) pairs")
            _require_type(item[0], (str,), "metadata key")
            _require_type(item[1], _SCALAR_TYPES, "metadata value")
        _require_type(self.mapping_version, _OPTIONAL_TEXT_TYPES, "mapping_version")


@dataclass(frozen=True, slots=True)
class SourcePage:
    """One successful finite read, not an iterator or a source failure.

    Consuming records exhausts this page only. interval_exhausted must be stated
    explicitly; neither empty records nor a missing cursor imply it. The limit
    flags report independent stopping conditions and may coincide with interval
    completion. Cursor contents are uninterpreted text/bytes, including empty
    tokens. Temporary failures belong at the acquisition exception boundary.
    """

    records: tuple[IngestedLogRecord, ...]
    interval_exhausted: bool
    next_cursor: str | bytes | None = None
    page_limit_reached: bool = False
    cycle_budget_reached: bool = False

    def __post_init__(self):
        _require_type(self.records, (tuple,), "records")
        for record in self.records:
            _require_type(record, (IngestedLogRecord,), "records item")
        _require_type(self.next_cursor, (str, bytes, type(None)), "next_cursor")
        for name in ("interval_exhausted", "page_limit_reached", "cycle_budget_reached"):
            _require_type(getattr(self, name), (bool,), name)
