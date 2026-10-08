"""Source-neutral incremental segmentation, owned by one finite analysis.

There is no acquisition loop, policy discovery, persistence, cursor handling,
ordering repair, deduplication or implicit timeout/page flush. Pending events
are lifecycle bounded, not byte bounded. Consume outputs instead of retaining
an input history. A session is single-owner and is not a concurrent shared cache.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Iterator

from ingestion_layer import contracts as ingestion
from segmentation_layer.contracts import (
    AssembledEvent, AssemblyEvent, EmissionReason, SegmentationOutput,
    SegmentationPolicy, StreamKey, UnassembledRecord,
)
from segmentation_layer.multiline_assembler import MultilineAssemblyState


PolicyProvider = Callable[[StreamKey, ingestion.IngestedLogRecord], SegmentationPolicy | None]


@dataclass
class _StreamState:
    policy: SegmentationPolicy
    assembly: MultilineAssemblyState
    records: list[ingestion.IngestedLogRecord] = field(default_factory=list)

    def output(self, key: StreamKey, event: AssemblyEvent, reason: EmissionReason) -> AssembledEvent:
        output = AssembledEvent(event.text, key, tuple(self.records), event.evidence, self.policy, reason)
        self.records.clear()
        return output


class SegmentationSession:
    """Multiplex independent immutable stream incarnations within one analysis.

    policy_provider(key, first_record) is trusted application configuration: it
    must deterministically return a prevalidated immutable snapshot or None,
    with no discovery, parser, network, LLM, registry or persistence operations.
    It runs only when no active state exists. New incarnations need no advance
    enumeration; a returned snapshot is pinned until explicit stream closure.
    No-policy records are returned immediately and never buffered for replay.
    """

    def __init__(self, policy_provider: PolicyProvider, *, max_active_streams=None,
                 max_pending_records=None, max_pending_chars=None):
        if not callable(policy_provider):
            raise TypeError("policy_provider must be callable")
        self._policy_provider = policy_provider
        self._streams: dict[StreamKey, _StreamState] = {}
        self._closed = False
        self._pending_records = 0
        self._pending_chars = 0
        self._max_active_streams = max_active_streams
        self._max_pending_records = max_pending_records
        self._max_pending_chars = max_pending_chars

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def active_stream_count(self) -> int:
        return len(self._streams)

    @property
    def pending_record_count(self) -> int:
        return self._pending_records

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("SegmentationSession is closed")

    def feed(self, record: ingestion.IngestedLogRecord) -> tuple[SegmentationOutput, ...]:
        """Consume one opaque record; return zero or one event/disposition.

        Incomplete/unsupported records cannot affect another stream. Blank
        records inside an event remain contributors but are omitted from text.
        Leading blanks receive their own context disposition, not a fake event.
        """
        self._ensure_open()
        if not isinstance(record, ingestion.IngestedLogRecord):
            raise TypeError("record must be IngestedLogRecord")
        if record.framing is not ingestion.Framing.PHYSICAL_LINE:
            return (UnassembledRecord(record, "unsupported_framing"),)

        key = StreamKey.from_identity(record.stream_identity)
        if key is None:
            reason = ("missing_stream_identity" if record.stream_identity is None
                      else "incomplete_stream_identity")
            return (UnassembledRecord(record, reason),)

        state = self._streams.get(key)
        if state is None:
            if self._max_active_streams is not None and len(self._streams) >= self._max_active_streams:
                raise ValueError('Monitoring active-stream memory limit reached')
            policy = self._policy_provider(key, record)
            if policy is None:
                return (UnassembledRecord(record, "no_policy", stream_key=key),)
            if not isinstance(policy, SegmentationPolicy):
                raise TypeError("policy_provider must return SegmentationPolicy or None")
            # Regex compilation can fail before any stream state is installed.
            state = _StreamState(policy, MultilineAssemblyState(policy.regex_pattern))
            self._streams[key] = state

        step = state.assembly.feed(record.raw_text)
        outputs = ()
        if step.completed is not None:
            self._pending_records -= len(state.records)
            self._pending_chars -= sum(len(item.raw_text) for item in state.records)
            outputs = (state.output(key, step.completed, "next_header"),)
        if state.assembly.has_pending:
            # The closing header is appended only AFTER packaging its predecessor.
            state.records.append(record)
            self._pending_records += 1
            self._pending_chars += len(record.raw_text)
            if self._max_pending_records is not None and self._pending_records > self._max_pending_records:
                raise ValueError('Monitoring pending-event memory limit reached')
            if self._max_pending_chars is not None and self._pending_chars > self._max_pending_chars:
                raise ValueError('Monitoring pending-event byte limit reached')
        else:
            return (UnassembledRecord(record, "blank_context", key, step.evidence, state.policy),)
        return outputs

    def feed_page(self, page: ingestion.SourcePage) -> Iterator[SegmentationOutput]:
        """Lazily feed records in supplied order; no page flag can flush state.

        Exhaust this iterator before starting another operation. Partial
        consumption processes only that prefix, without transaction/rollback or
        cursor advancement. The iterator holds its current input page's records;
        the session retains no pages or emitted-event history.
        """
        self._ensure_open()
        if not isinstance(page, ingestion.SourcePage):
            raise TypeError("page must be SourcePage")
        return self._feed_records(page.records)

    def _feed_records(self, records: tuple[ingestion.IngestedLogRecord, ...]) -> Iterator[SegmentationOutput]:
        self._ensure_open()
        for record in records:
            yield from self.feed(record)

    def close_stream(self, stream_key: StreamKey) -> tuple[AssembledEvent, ...]:
        """Flush and remove only this stream; repeated closure emits nothing.

        Later input for this key may start fresh state. This explicit cut does
        not infer producer death, late-arrival handling, or a replay policy.
        """
        if not isinstance(stream_key, StreamKey):
            raise TypeError("stream_key must be StreamKey")
        state = self._streams.pop(stream_key, None)
        if state is None:
            return ()
        self._pending_records -= len(state.records)
        self._pending_chars -= sum(len(item.raw_text) for item in state.records)
        event = state.assembly.flush()
        if event is None:
            return ()
        return (state.output(stream_key, event, "explicit_stream_close"),)

    def close(self) -> tuple[AssembledEvent, ...]:
        """End the analysis eagerly, releasing state in first-seen stream order.

        The returned tuple owns the remaining tails; this session keeps none.
        Repeated close is empty, and subsequent feed/feed_page calls fail.
        """
        if self._closed:
            return ()
        self._closed = True
        outputs = []
        for key, state in self._streams.items():
            self._pending_records -= len(state.records)
            self._pending_chars -= sum(len(item.raw_text) for item in state.records)
            event = state.assembly.flush()
            if event is not None:
                outputs.append(state.output(key, event, "analysis_end"))
        self._streams.clear()
        return tuple(outputs)
