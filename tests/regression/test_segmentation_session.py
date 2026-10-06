"""Offline, analysis-local segmentation of immutable acquisition records."""

import builtins
from dataclasses import FrozenInstanceError, replace
import gc
import importlib
import io
from pathlib import Path
import socket
import sqlite3
from types import SimpleNamespace
import weakref

import pytest


HEADER = "2026-01-01T00:00:00Z started  "
NEXT = "2026-01-01T00:00:01Z next"
REGEX = r"^\d{4}-\d{2}-\d{2}T"


@pytest.fixture(autouse=True)
def boundary(monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[2] / "src" / "backend"))

    def forbidden(*args, **kwargs):
        pytest.fail("Segmentation session must not access external or persistent state")

    for owner, names in (
        (sqlite3, ("connect",)), (sqlite3.dbapi2, ("connect",)),
        (socket.socket, ("connect", "connect_ex")),
        (socket, ("create_connection", "getaddrinfo")),
    ):
        for name in names:
            monkeypatch.setattr(owner, name, forbidden)
    original_import = builtins.__import__
    blocked_roots = {
        "ai_engine", "parser_layer", "template_layer", "aggregation_layer",
        "signal_qualification_layer", "correlation_layer", "incident_candidate_layer",
        "context_enrichment_layer", "rca_layer", "learning_planning_layer",
        "downstream_pipeline", "full_pipeline_v2", "input_package_layer",
        "requests", "httpx", "urllib3", "opensearchpy", "elasticsearch", "drain3", "streamlit",
    }
    blocked_modules = {
        "segmentation_layer.segmentation_pipeline", "segmentation_layer.header_discovery",
        "segmentation_layer.regex_validator", "segmentation_layer.policy_registry",
        "ingestion_layer.opensearch_source", "ingestion_layer.opensearch_client",
        "ingestion_layer.opensearch_config",
    }

    def guarded_import(name, *args, **kwargs):
        if name.split(".")[0] in blocked_roots or name in blocked_modules:
            pytest.fail(f"Segmentation session must not import {name}")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded_import)
    return forbidden


@pytest.fixture
def api():
    return SimpleNamespace(
        ingestion=importlib.import_module("ingestion_layer.contracts"),
        contracts=importlib.import_module("segmentation_layer.contracts"),
        session=importlib.import_module("segmentation_layer.segmentation_session"),
    )


@pytest.fixture
def policy(api):
    return api.contracts.SegmentationPolicy("timestamp-v1", REGEX, "offline-fixture-v1")


def identity(api, **changes):
    values = dict(source_scope="cluster", namespace="namespace", workload="workload",
                  pod="pod-name", pod_instance="pod-uid", container="container-name",
                  container_instance="container-uid", channel="stdout")
    return api.ingestion.StreamIdentity(**(values | changes))


def record(api, text=HEADER, *, identity_changes=None, number=1, **changes):
    values = dict(
        raw_text=text,
        source_reference=api.ingestion.SourceReference(
            "acquisition-profile", "partition", str(number), generation="generation-1", version=2,
        ),
        stream_identity=identity(api, **(identity_changes or {})),
        source_timestamp_raw="2026-01-01T00:00:00.123456789Z",
        retrieval_order=("same-millisecond", number),
        framing=api.ingestion.Framing.PHYSICAL_LINE,
        metadata=(("sequence", number), ("context", "preserve me")), mapping_version="v1",
    )
    return api.ingestion.IngestedLogRecord(**(values | changes))


def session(api, policy):
    return api.session.SegmentationSession(lambda key, first_record: policy)


def page(api, *records, **changes):
    return api.ingestion.SourcePage(tuple(records), **({"interval_exhausted": False} | changes))


@pytest.mark.parametrize('closure,status', [
    ('next_header', 'complete'), ('analysis_end', 'possible_incomplete'),
    ('explicit_stream_close', 'possible_incomplete'),
])
def test_boundary_status_uses_closure_evidence(api, policy, closure, status):
    subject = session(api, policy)
    first = record(api)
    subject.feed(first)
    if closure == 'next_header':
        event, = subject.feed(record(api, NEXT, number=2))
    elif closure == 'explicit_stream_close':
        event, = subject.close_stream(api.contracts.StreamKey.from_identity(first.stream_identity))
    else:
        event, = subject.close()
    assert event.boundary_status == status
    assert event.text == first.raw_text and event.records == (first,)


def test_event_spans_pages_and_next_header_is_not_previous_contributor(api, policy):
    subject = session(api, policy)
    first = record(api)
    continuation = record(api, "\t  detail  ", number=2)
    next_header = record(api, NEXT, number=3)
    assert list(subject.feed_page(page(api, first, next_cursor="opaque"))) == []
    assert subject.pending_record_count == 1
    outputs = list(subject.feed_page(page(api, continuation, next_header, interval_exhausted=True)))
    assert len(outputs) == 1
    event = outputs[0]
    assert isinstance(event, api.contracts.AssembledEvent)
    assert event.text == HEADER + "\n\t  detail  "
    assert event.records == (first, continuation)
    assert event.records[0] is first and event.records[1] is continuation
    assert event.emission_reason == "next_header"
    assert event.boundary_status == "complete"  # earlier page cuts carry no quality penalty
    assert event.policy is policy
    assert all(not hasattr(item, "line_no") for item in event.evidence)
    assert subject.pending_record_count == 1
    tail, = subject.close()
    assert tail.records == (next_header,) and tail.boundary_status == 'possible_incomplete'


@pytest.mark.parametrize("empty", [False, True])
@pytest.mark.parametrize("interval", [False, True])
@pytest.mark.parametrize("page_limit", [False, True])
@pytest.mark.parametrize("cycle_limit", [False, True])
@pytest.mark.parametrize("cursor", [None, "opaque:{not-json}"])
def test_page_flags_and_empty_pages_never_flush(api, policy, empty, interval, page_limit, cycle_limit, cursor):
    subject = session(api, policy)
    subject.feed(record(api))
    rows = () if empty else (record(api, "  continuation", number=2),)
    assert list(subject.feed_page(page(api, *rows, interval_exhausted=interval,
                                      page_limit_reached=page_limit, cycle_budget_reached=cycle_limit,
                                      next_cursor=cursor))) == []
    assert subject.pending_record_count == (1 if empty else 2)
    tail, = subject.close()
    assert tail.boundary_status == 'possible_incomplete'
    assert subject.close() == ()


@pytest.mark.parametrize("changes", [
    {"pod_instance": "pod-2", "pod": "pod-2-name"},
    {"channel": "stderr"},
    {"pod_instance": "pod-restarted-same-name"},
    {"container_instance": "container-restarted-same-name"},
    {"source_scope": "other-cluster"},
])
def test_immutable_incarnations_are_isolated_when_interleaved(api, policy, changes):
    subject = session(api, policy)
    a = record(api)
    b = record(api, HEADER + " stream-b", identity_changes=changes, number=2)
    a_cont = record(api, "  continuation-a", number=3)
    b_cont = record(api, "  continuation-b", identity_changes=changes, number=4)
    for row in (a, b, a_cont, b_cont):
        assert subject.feed(row) == ()
    # Stream B completes first even though A started first.
    b_event = subject.feed(record(api, NEXT, identity_changes=changes, number=5))[0]
    assert b_event.records == (b, b_cont)
    a_event = subject.feed(record(api, NEXT, number=6))[0]
    assert a_event.records == (a, a_cont)
    assert a_event.stream_key != b_event.stream_key
    assert subject.active_stream_count == 2


def test_descriptive_labels_and_reference_scope_do_not_route_streams(api, policy):
    subject = session(api, policy)
    first = record(api)
    renamed = record(api, "  continuation", number=2, identity_changes={
        "namespace": None, "workload": "renamed", "pod": "renamed", "container": None,
    }, source_reference=api.ingestion.SourceReference("another-profile", "new-partition", "2"))
    assert first.stream_identity != renamed.stream_identity
    subject.feed(first)
    subject.feed(renamed)
    event = subject.close()[0]
    assert event.records == (first, renamed)
    assert event.stream_key == api.contracts.StreamKey("cluster", "pod-uid", "container-uid", "stdout")
    assert subject.active_stream_count == 0


@pytest.mark.parametrize("coordinate", ["source_scope", "pod_instance", "container_instance", "channel"])
@pytest.mark.parametrize("missing", ["", " \t", None])
def test_incomplete_coordinates_never_create_or_join_state(api, policy, coordinate, missing):
    if coordinate == "source_scope" and missing is None:
        # This missing value is rejected even earlier by the unchanged contract.
        with pytest.raises(TypeError, match="source_scope"):
            record(api, identity_changes={coordinate: missing})
        return
    subject = session(api, policy)
    complete = record(api)
    subject.feed(complete)
    broken = record(api, "  unresolved", identity_changes={coordinate: missing}, number=2)
    repeated = replace(broken, source_reference=api.ingestion.SourceReference("profile", "partition", "3"))
    for row in (broken, repeated):
        result, = subject.feed(row)
        assert isinstance(result, api.contracts.UnassembledRecord)
        assert result.record is row
        assert result.reason == "incomplete_stream_identity"
        assert result.stream_key is None
        assert row.framing is api.ingestion.Framing.PHYSICAL_LINE
        assert subject.active_stream_count == 1
        assert subject.pending_record_count == 1
    assert subject.close()[0].records == (complete,)


def test_missing_identity_remains_explicit_without_fallback_state(api, policy):
    subject = session(api, policy)
    for number in range(3):
        row = record(api, "  unresolved", number=number, stream_identity=None)
        result, = subject.feed(row)
        assert result.record is row
        assert result.reason == "missing_stream_identity"
    assert subject.active_stream_count == subject.pending_record_count == 0
    assert subject.close() == ()


def test_key_preserves_exact_identity_values(api):
    key_type = api.contracts.StreamKey
    assert key_type.from_identity(identity(api)) == key_type("cluster", "pod-uid", "container-uid", "stdout")
    for changes in ({"source_scope": " cluster "}, {"channel": "STDOUT"}, {"pod_instance": "pod-uid "}):
        assert key_type.from_identity(identity(api, **changes)) != key_type.from_identity(identity(api))
    with pytest.raises(FrozenInstanceError):
        key_type("cluster", "pod", "container", "stdout").channel = "stderr"
    for blank in ("", " \t"):
        with pytest.raises(ValueError):
            key_type("cluster", "pod", "container", blank)


def test_policy_provider_handles_new_incarnations_and_pins_active_policy(api, policy):
    other = api.contracts.SegmentationPolicy("json-v1", r'^\{"', "offline-json-fixture")
    selected = [policy]
    calls = []

    def provide(key, first_record):
        calls.append((key, first_record))
        return selected[0]

    subject = api.session.SegmentationSession(provide)
    first = record(api)
    subject.feed(first)
    selected[0] = other
    continuation = record(api, '{"nested": true}', number=2)
    assert subject.feed(continuation) == ()
    assert len(calls) == 1
    restarted = record(api, '{"first": true}', identity_changes={"pod_instance": "future-pod"}, number=3)
    subject.feed(restarted)
    assert len(calls) == 2
    outputs = subject.close()
    assert outputs[0].records == (first, continuation)
    assert outputs[0].policy is policy
    assert outputs[1].policy is other


def test_no_policy_does_not_buffer_or_reuse_other_stream_policy(api, policy):
    subject = api.session.SegmentationSession(lambda key, row: policy if key.channel == "stdout" else None)
    subject.feed(record(api))
    for number in range(4):
        row = record(api, "  no policy", identity_changes={"channel": "stderr"}, number=number)
        result, = subject.feed(row)
        assert result.reason == "no_policy" and result.record is row
    assert subject.active_stream_count == subject.pending_record_count == 1


@pytest.mark.parametrize("framing", ["UNKNOWN", "LOGICAL_EVENT"])
def test_unsupported_framing_is_explicit_and_cannot_flush(api, policy, framing):
    subject = session(api, policy)
    first = record(api)
    subject.feed(first)
    row = record(api, NEXT, framing=api.ingestion.Framing[framing])
    result, = subject.feed(row)
    assert result.reason == "unsupported_framing" and result.record is row
    assert subject.close()[0].records == (first,)


def test_equal_timestamps_and_repeated_references_survive_in_supplied_order(api, policy):
    subject = session(api, policy)
    first = record(api)
    second = record(api, "  sequence-high", number=2**80 + 7)
    third = replace(second, raw_text="  sequence-low", retrieval_order=("same-millisecond", 3))
    assert first.source_timestamp_raw == second.source_timestamp_raw == third.source_timestamp_raw
    assert second.source_reference is third.source_reference
    for row in (first, second, third, third):
        subject.feed(row)
    event, = subject.close()
    assert event.records == (first, second, third, third)
    assert event.records[1].retrieval_order == ("same-millisecond", 2**80 + 7)
    assert event.records[2].retrieval_order == ("same-millisecond", 3)
    assert event.text == "\n".join(row.raw_text for row in event.records)


def test_blank_provenance_survives_pages_and_context_is_per_stream(api, policy):
    json_policy = api.contracts.SegmentationPolicy("no-json-regex", r"^EVENT ", "offline-context-fixture")
    subject = session(api, json_policy)
    a = record(api, '{"a": 1}')
    b = record(api, '{"b": 1}', identity_changes={"pod_instance": "b"}, number=2)
    blank = record(api, " \t\r\n", number=3)
    assert list(subject.feed_page(page(api, a, b, blank))) == []
    b_cont = record(api, '{"nested": 2}', identity_changes={"pod_instance": "b"}, number=4)
    a_next = record(api, '{"a": 2}', number=5)
    outputs = list(subject.feed_page(page(api, b_cont, a_next)))
    assert len(outputs) == 1
    assert outputs[0].records == (a, blank)
    assert outputs[0].text == a.raw_text
    assert outputs[0].evidence[-1].included is False
    assert outputs[0].evidence[-1].decision.reason == "blank"
    assert subject.close_stream(api.contracts.StreamKey.from_identity(b.stream_identity))[0].records == (b, b_cont)


def test_leading_blank_has_context_disposition_and_no_fake_event(api, policy):
    subject = session(api, policy)
    row = record(api, " \t\r\n")
    result, = subject.feed(row)
    assert result.reason == "blank_context"
    assert result.record is row
    assert result.evidence.included is False
    assert subject.pending_record_count == 0
    assert subject.close() == ()


def test_opaque_record_content_and_complete_source_evidence(api, policy):
    subject = session(api, policy)
    first = record(api, HEADER + "\r\nembedded\r\n")
    second = record(api, "\t\u03a9 literal \\n  \r\n", number=2)
    subject.feed(first)
    subject.feed(second)
    event, = subject.close()
    assert event.text == first.raw_text + "\n" + second.raw_text
    assert event.records[0] is first and event.records[1] is second
    assert event.records[0].source_reference.generation == "generation-1"
    assert event.records[0].metadata == first.metadata
    assert event.records[0].mapping_version == "v1"
    assert len(event.evidence) == 2
    assert len(event.text.splitlines()) > len(event.evidence)


def test_close_stream_and_analysis_close_are_explicit_ordered_and_idempotent(api, policy):
    subject = session(api, policy)
    rows = [record(api, HEADER + suffix, identity_changes={"pod_instance": suffix}, number=n)
            for n, suffix in enumerate(("z", "a", "m"))]
    for row in rows:
        subject.feed(row)
    key = api.contracts.StreamKey.from_identity(rows[1].stream_identity)
    output, = subject.close_stream(key)
    assert output.records == (rows[1],) and output.emission_reason == "explicit_stream_close"
    assert output.boundary_status == 'possible_incomplete'
    assert subject.close_stream(key) == ()
    assert subject.active_stream_count == subject.pending_record_count == 2
    outputs = subject.close()
    assert [item.records for item in outputs] == [(rows[0],), (rows[2],)]
    assert all(item.emission_reason == "analysis_end" for item in outputs)
    assert all(item.boundary_status == 'possible_incomplete' for item in outputs)
    assert subject.active_stream_count == subject.pending_record_count == 0
    assert subject.closed is True
    assert subject.close() == ()
    assert subject.close_stream(key) == ()
    with pytest.raises(RuntimeError, match="closed"):
        subject.feed(rows[0])
    with pytest.raises(RuntimeError, match="closed"):
        subject.feed_page(page(api))


@pytest.mark.parametrize('status,eligible', [
    ('complete', True), ('possible_incomplete', False), ('confirmed_truncated', False),
    (None, False), ('unknown', False),
])
def test_future_baseline_boundary_guard_requires_complete_evidence(api, status, eligible):
    assert api.contracts.boundary_allows_baseline_evidence(status) is eligible


def test_explicitly_closed_stream_can_start_fresh_without_old_context(api, policy):
    subject = session(api, policy)
    first = record(api)
    subject.feed(first)
    key = api.contracts.StreamKey.from_identity(first.stream_identity)
    subject.close_stream(key)
    orphan = record(api, "orphan after explicit cut", number=2)
    subject.feed(orphan)
    event, = subject.close()
    assert event.records == (orphan,)
    assert event.evidence[0].decision.start_new_event is False


def test_separate_sessions_do_not_share_state(api, policy):
    first = session(api, policy)
    second = session(api, policy)
    a = record(api)
    b = record(api, "orphan")
    first.feed(a)
    second.feed(b)
    assert second.close()[0].records == (b,)
    assert first.close()[0].records == (a,)


def test_page_consumption_is_lazy_ordered_and_has_no_prefetch(api, policy):
    subject = session(api, policy)
    rows = [record(api, HEADER + str(n), number=n) for n in range(4)]
    outputs = subject.feed_page(page(api, *rows))
    assert subject.active_stream_count == 0
    assert next(outputs).records == (rows[0],)
    assert subject.pending_record_count == 1
    assert [event.records for event in outputs] == [(rows[1],), (rows[2],)]
    assert subject.close()[0].records == (rows[3],)


def test_emitted_outputs_and_closed_stream_state_are_released(api, policy):
    # Add only weak-reference support to the immutable acquisition record.
    # No SourcePage is involved here (its contract requires exact record types).
    class TrackedRecord(api.ingestion.IngestedLogRecord):
        __slots__ = ("__weakref__",)

    def tracked_record(number):
        return TrackedRecord(
            HEADER + str(number), api.ingestion.SourceReference("profile", "partition", str(number)),
            stream_identity=identity(api), framing=api.ingestion.Framing.PHYSICAL_LINE,
        )

    subject = session(api, policy)
    subject.feed(record(api))
    refs = []
    contributor_refs = []
    for number in range(1, 501):
        contributor = tracked_record(number)
        contributor_refs.append(weakref.ref(contributor))
        output, = subject.feed(contributor)
        del contributor
        refs.append(weakref.ref(output))
        del output
        assert subject.active_stream_count == subject.pending_record_count == 1
    gc.collect()
    assert all(reference() is None for reference in refs)
    assert all(reference() is None for reference in contributor_refs[:-1])
    assert contributor_refs[-1]() is not None  # Only the active tail survives.
    key = api.contracts.StreamKey.from_identity(identity(api))
    output, = subject.close_stream(key)
    reference = weakref.ref(output)
    del output
    gc.collect()
    assert reference() is None
    assert contributor_refs[-1]() is None
    assert subject.active_stream_count == subject.pending_record_count == 0
    subject.feed(record(api))
    output, = subject.close()
    reference = weakref.ref(output)
    del output
    gc.collect()
    assert reference() is None
    assert subject.active_stream_count == subject.pending_record_count == 0


def test_output_and_policy_snapshots_are_immutable(api, policy):
    with pytest.raises(FrozenInstanceError):
        policy.regex_pattern = "^changed"
    subject = session(api, policy)
    subject.feed(record(api))
    event, = subject.close()
    with pytest.raises(FrozenInstanceError):
        event.records = ()
    with pytest.raises(FrozenInstanceError):
        event.evidence[0].included = False
    with pytest.raises(TypeError):
        replace(event, records=list(event.records))
    with pytest.raises(TypeError):
        replace(event, evidence=list(event.evidence))
    with pytest.raises(TypeError):
        replace(event.evidence[0], decision={})


def test_invalid_provider_result_does_not_create_stream_state(api):
    subject = api.session.SegmentationSession(lambda key, row: {"regex": REGEX})
    with pytest.raises(TypeError, match="SegmentationPolicy"):
        subject.feed(record(api))
    assert subject.active_stream_count == subject.pending_record_count == 0


def test_late_policy_binding_does_not_replay_unassembled_records(api, policy):
    binding = [None]
    subject = api.session.SegmentationSession(lambda key, row: binding[0])
    unresolved = record(api)
    assert subject.feed(unresolved)[0].reason == "no_policy"
    binding[0] = policy
    continuation = record(api, "orphan after binding", number=2)
    subject.feed(continuation)
    assert subject.close()[0].records == (continuation,)


def test_page_iterator_created_before_close_cannot_feed_after_close(api, policy):
    subject = session(api, policy)
    pending = subject.feed_page(page(api, record(api)))
    empty = subject.feed_page(page(api))
    subject.close()
    for iterator in (pending, empty):
        with pytest.raises(RuntimeError, match="closed"):
            list(iterator)


def test_import_construction_and_feed_have_no_file_or_processing_access(api, policy, monkeypatch, boundary):
    # Import guard covers cached imports too; reload exercises module-level code.
    with monkeypatch.context() as guard:
        guard.setattr(builtins, "open", boundary)
        guard.setattr(io, "open", boundary)
        importlib.reload(api.session)
        subject = session(api, policy)
        assert subject.feed(record(api)) == ()
        assert subject.close()[0].text == HEADER
