"""Immutable segmentation evidence, separate from canonical parser events.

Policy snapshots are externally validated configuration/learning references.
Assembly evidence and outputs belong to one analysis and perform no I/O.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from ingestion_layer import contracts as ingestion
from segmentation_layer.header_classifier import LineDecision


def _nonblank_text(value: str, name: str) -> None:
    if type(value) is not str:
        raise TypeError(f"{name} must be str")
    if not value or value.isspace():
        raise ValueError(f"{name} must contain non-whitespace text")


def _tuple_of(value, item_type, name: str) -> None:
    if type(value) is not tuple or any(not isinstance(item, item_type) for item in value):
        raise TypeError(f"{name} must be a tuple of {item_type.__name__}")


@dataclass(frozen=True, slots=True)
class StreamKey:
    """Exact immutable incarnation coordinates; descriptive names are excluded."""

    source_scope: str
    pod_instance: str
    container_instance: str
    channel: str

    def __post_init__(self):
        for name in ("source_scope", "pod_instance", "container_instance", "channel"):
            _nonblank_text(getattr(self, name), name)

    @classmethod
    def from_identity(cls, identity: ingestion.StreamIdentity | None) -> StreamKey | None:
        """Return no key for incomplete evidence; never infer or normalize it."""
        if identity is None:
            return None
        values = (identity.source_scope, identity.pod_instance,
                  identity.container_instance, identity.channel)
        if any(value is None or not value or value.isspace() for value in values):
            return None
        return cls(*values)


@dataclass(frozen=True, slots=True)
class SegmentationPolicy:
    """Caller-attested prevalidated policy, not a discovery/admission request.

    validation_reference identifies the external validation evidence.
    Construction does not establish applicability to a stream; the deterministic
    provider owns that binding. The session never admits an AI proposal or reads
    a registry. Snapshots remain pinned until their stream state is closed.
    """

    policy_id: str
    regex_pattern: str
    validation_reference: str

    def __post_init__(self):
        for name in ("policy_id", "regex_pattern", "validation_reference"):
            _nonblank_text(getattr(self, name), name)


@dataclass(frozen=True, slots=True)
class LineEvidence:
    """One input unit's decision; ordinal is local to one assembly state.

    An ordinal is NOT a source/file position. File adapters may map it to
    physical lines because they feed every line, including omitted blanks.
    included describes compatibility text inclusion, not provenance retention.
    """

    ordinal: int
    decision: LineDecision
    included: bool

    def __post_init__(self):
        if type(self.ordinal) is not int or self.ordinal < 1:
            raise ValueError("ordinal must be a positive int")
        if not isinstance(self.decision, LineDecision):
            raise TypeError("decision must be LineDecision")
        if type(self.included) is not bool:
            raise TypeError("included must be bool")


@dataclass(frozen=True, slots=True)
class AssemblyEvent:
    """Internal single-stream text snapshot, before acquisition provenance."""

    text: str
    evidence: tuple[LineEvidence, ...]

    def __post_init__(self):
        if type(self.text) is not str:
            raise TypeError("text must be str")
        _tuple_of(self.evidence, LineEvidence, "evidence")


@dataclass(frozen=True, slots=True)
class AssemblyStep:
    """Evidence for the fed unit and, optionally, the preceding closed event."""

    evidence: LineEvidence
    completed: AssemblyEvent | None = None

    def __post_init__(self):
        if not isinstance(self.evidence, LineEvidence):
            raise TypeError("evidence must be LineEvidence")
        if self.completed is not None and not isinstance(self.completed, AssemblyEvent):
            raise TypeError("completed must be AssemblyEvent or None")


EmissionReason = Literal["next_header", "explicit_stream_close", "analysis_end"]


@dataclass(frozen=True)
class AssembledEvent:
    """Analysis output with all contributors, including omitted blank units.

    records and evidence correspond one-for-one in supplied stream order.
    No reference is deduplicated. The closing header belongs to the next event.
    An explicit close records a lifecycle cut, not proof the producer ended.
    """

    text: str
    stream_key: StreamKey
    records: tuple[ingestion.IngestedLogRecord, ...]
    evidence: tuple[LineEvidence, ...]
    policy: SegmentationPolicy
    emission_reason: EmissionReason

    def __post_init__(self):
        if type(self.text) is not str:
            raise TypeError("text must be str")
        if not isinstance(self.stream_key, StreamKey):
            raise TypeError("stream_key must be StreamKey")
        if not isinstance(self.policy, SegmentationPolicy):
            raise TypeError("policy must be SegmentationPolicy")
        _tuple_of(self.records, ingestion.IngestedLogRecord, "records")
        _tuple_of(self.evidence, LineEvidence, "evidence")
        if not self.records or len(self.records) != len(self.evidence):
            raise ValueError("records and evidence must be nonempty and correspond one-for-one")
        if type(self.emission_reason) is not str:
            raise TypeError("emission_reason must be str")
        if self.emission_reason not in ("next_header", "explicit_stream_close", "analysis_end"):
            raise ValueError("Unknown emission_reason")


DispositionReason = Literal[
    "unsupported_framing", "missing_stream_identity", "incomplete_stream_identity",
    "no_policy", "blank_context",
]


@dataclass(frozen=True)
class UnassembledRecord:
    """Unchanged input with an explicit non-assembly disposition.

    blank_context accounts for a blank with no active event. Its boundary
    context is applied, but it is not manufactured into an empty logical event.
    """

    record: ingestion.IngestedLogRecord
    reason: DispositionReason
    stream_key: StreamKey | None = None
    evidence: LineEvidence | None = None
    policy: SegmentationPolicy | None = None

    def __post_init__(self):
        if not isinstance(self.record, ingestion.IngestedLogRecord):
            raise TypeError("record must be IngestedLogRecord")
        if type(self.reason) is not str:
            raise TypeError("reason must be str")
        if self.reason not in ("unsupported_framing", "missing_stream_identity",
                               "incomplete_stream_identity", "no_policy", "blank_context"):
            raise ValueError("Unknown disposition reason")
        for value, kind, name in ((self.stream_key, StreamKey, "stream_key"),
                                  (self.evidence, LineEvidence, "evidence"),
                                  (self.policy, SegmentationPolicy, "policy")):
            if value is not None and not isinstance(value, kind):
                raise TypeError(f"{name} must be {kind.__name__} or None")


SegmentationOutput = AssembledEvent | UnassembledRecord
