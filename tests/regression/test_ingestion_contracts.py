"""Acquisition contracts preserve evidence without touching processing state."""

import builtins
from dataclasses import FrozenInstanceError, replace
from datetime import datetime, timedelta, timezone
import importlib
import io
from pathlib import Path
import socket
import sqlite3
import sys

import pytest


@pytest.fixture
def contracts(monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[2] / "src" / "backend"))

    def forbidden(*args, **kwargs):
        pytest.fail("Acquisition contracts must not perform external or persistent I/O")

    monkeypatch.setattr(sqlite3, "connect", forbidden)
    monkeypatch.setattr(sqlite3.dbapi2, "connect", forbidden)
    monkeypatch.setattr(socket.socket, "connect", forbidden)
    monkeypatch.setattr(socket.socket, "connect_ex", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(socket, "getaddrinfo", forbidden)
    original_import = builtins.__import__
    blocked = {
        "ai_engine", "parser_layer", "segmentation_layer", "template_layer",
        "aggregation_layer", "signal_qualification_layer", "correlation_layer",
        "incident_candidate_layer", "context_enrichment_layer", "rca_layer",
        "learning_planning_layer", "downstream_pipeline", "full_pipeline_v2",
        "input_package_layer", "requests", "urllib3", "httpx", "opensearchpy",
        "elasticsearch", "drain3",
    }

    def guarded_import(name, *args, **kwargs):
        if name.split(".")[0] in blocked:
            pytest.fail(f"Acquisition contracts must not import {name}")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded_import)
    return importlib.import_module("ingestion_layer.contracts")


@pytest.fixture
def reference(contracts):
    return contracts.SourceReference("test-source", "collection-1", "00017")


@pytest.mark.parametrize("raw", [
    " \tbaşladı Ω 🚀\n\t  continuation\n\nfinished  \t",
    "  header\r\n\tcontinuation\r\n",
    "", " \t\n ", "literal \\n stays literal",
])
def test_raw_text_is_preserved_exactly(contracts, reference, raw):
    record = contracts.IngestedLogRecord(raw, reference)
    assert record.raw_text is raw
    assert record.raw_text.encode("utf-8") == raw.encode("utf-8")
    assert record.framing is contracts.Framing.UNKNOWN


@pytest.mark.parametrize("raw", [None, 17, b"text", [], {}])
def test_non_string_text_is_rejected_without_coercion(contracts, reference, raw):
    with pytest.raises(TypeError, match="raw_text"):
        contracts.IngestedLogRecord(raw, reference)


def test_missing_evidence_stays_explicit(contracts, reference):
    record = contracts.IngestedLogRecord("message", reference)
    assert record.stream_identity is None
    assert record.source_timestamp_raw is None
    assert record.source_timestamp is None
    assert record.first_observed_at is None
    assert record.retrieval_order is None
    assert record.mapping_version is None
    assert record.metadata == ()
    identity = contracts.StreamIdentity("test-source")
    assert all(getattr(identity, name) is None for name in (
        "namespace", "workload", "pod", "pod_instance", "container",
        "container_instance", "channel",
    ))


def test_source_reference_is_not_a_generated_canonical_id(contracts, reference):
    first = contracts.IngestedLogRecord("same text", reference)
    second = contracts.IngestedLogRecord("same text", reference)
    assert first.source_reference is second.source_reference is reference
    assert reference.record_id == "00017"
    assert first == second
    assert not hasattr(first, "event_id")
    assert not hasattr(reference, "event_id")
    assert reference == contracts.SourceReference("test-source", "collection-1", "00017")
    assert len({reference, replace(reference, generation="generation-2"),
                replace(reference, version=2), replace(reference, version="2")}) == 4


def test_stream_identity_is_hashable_and_workload_is_descriptive(contracts):
    identity = contracts.StreamIdentity(
        "test-source", namespace="ns", workload="old-description", pod="pod-1",
        pod_instance="pod-incarnation-1", container="app", container_instance="run-1",
    )
    same_stream = replace(identity, workload="new-description")
    assert same_stream.workload == "new-description"
    assert same_stream == identity
    assert hash(same_stream) == hash(identity)
    assert {identity: "pending event"}[same_stream] == "pending event"


@pytest.mark.parametrize("coordinate", ["pod", "pod_instance", "container", "container_instance"])
def test_producer_incarnations_can_identify_different_streams(contracts, coordinate):
    identity = contracts.StreamIdentity(
        "test-source", pod="pod-1", pod_instance="uid-1",
        container="app", container_instance="run-1",
    )
    assert len({identity, replace(identity, **{coordinate: "different"})}) == 2


def test_contract_fields_are_frozen(contracts, reference):
    record = contracts.IngestedLogRecord("text", reference)
    objects_and_fields = (
        (contracts.StreamIdentity("test-source"), "source_scope"),
        (reference, "record_id"),
        (record, "raw_text"),
        (contracts.SourcePage((record,), False), "records"),
    )
    for value, name in objects_and_fields:
        with pytest.raises(FrozenInstanceError):
            setattr(value, name, "replacement")


def test_retrieval_order_keeps_exact_scalar_values_and_types(contracts, reference):
    order = ("2026-10-04T00:00:00.123456789Z", 2**63 + 1, "00017", 1.25, True, None)
    record = contracts.IngestedLogRecord("text", reference, retrieval_order=order)
    assert record.retrieval_order is order
    assert tuple(type(value) for value in record.retrieval_order) == (
        str, int, str, float, bool, type(None),
    )


def test_explicit_timestamps_and_framing_are_carried_without_normalization(contracts, reference):
    source_time = datetime(2026, 10, 4, 12, 0, tzinfo=timezone(timedelta(hours=3)))
    observed = datetime(2026, 10, 4, 9, 0, 1, tzinfo=timezone.utc)
    for framing in contracts.Framing:
        record = contracts.IngestedLogRecord(
            "header\n  continuation", reference,
            source_timestamp_raw="  original timestamp text  ",
            source_timestamp=source_time, first_observed_at=observed, framing=framing,
        )
        assert record.source_timestamp_raw == "  original timestamp text  "
        assert record.source_timestamp is source_time
        assert record.first_observed_at is observed
        assert record.framing is framing


def test_scalar_metadata_is_immutable(contracts, reference):
    metadata = (("producer", "001"), ("attempt", 2), ("fraction", 0.5),
                ("flag", False), ("missing", None))
    record = contracts.IngestedLogRecord("text", reference, metadata=metadata)
    assert record.metadata is metadata
    with pytest.raises(TypeError):
        record.metadata[0][1] = "changed"


@pytest.mark.parametrize("field,value", [
    ("metadata", {"nested": []}),
    ("metadata", [("key", "value")]),
    ("metadata", (("key", []),)),
    ("metadata", (("key", {}),)),
    ("retrieval_order", [1, "token"]),
    ("retrieval_order", ({"position": 1},)),
    ("source_timestamp_raw", []),
])
def test_mutable_evidence_is_rejected(contracts, reference, field, value):
    with pytest.raises(TypeError, match=field):
        contracts.IngestedLogRecord("text", reference, **{field: value})


@pytest.mark.parametrize("cursor", [None, "", b"", "opaque:{not-json}", b"\x00\xffnext"])
def test_cursor_is_carried_without_interpretation(contracts, cursor):
    page = contracts.SourcePage((), interval_exhausted=False, next_cursor=cursor)
    assert page.next_cursor is cursor
    assert page.interval_exhausted is False


@pytest.mark.parametrize("interval,page_limit,cycle_limit", [
    (False, False, False), (False, True, False), (False, False, True),
    (False, True, True), (True, False, False), (True, True, True),
])
def test_interval_and_page_cycle_limits_are_independent(
    contracts, reference, interval, page_limit, cycle_limit,
):
    records = (contracts.IngestedLogRecord("text", reference),)
    page = contracts.SourcePage(
        records, interval_exhausted=interval, page_limit_reached=page_limit,
        cycle_budget_reached=cycle_limit,
    )
    assert tuple(iter(page.records)) == records
    assert page.interval_exhausted is interval
    assert page.page_limit_reached is page_limit
    assert page.cycle_budget_reached is cycle_limit


def test_interval_completion_must_be_explicit(contracts):
    with pytest.raises(TypeError, match="interval_exhausted"):
        contracts.SourcePage(())
    assert contracts.SourcePage((), False).interval_exhausted is False
    assert contracts.SourcePage((), True).interval_exhausted is True


def test_page_rejects_lazy_records_without_consuming_them(contracts):
    def endless_source():
        pytest.fail("A finite page must not consume a source iterator")
        yield

    with pytest.raises(TypeError, match="records"):
        contracts.SourcePage(endless_source(), False)
    with pytest.raises(TypeError, match="next_cursor"):
        contracts.SourcePage((), False, next_cursor={"position": []})


def test_import_and_construction_do_not_access_files_or_processing_state(contracts, monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("Contract import/construction must not access application files")

    # Network/SQLite and processing imports are already blocked by the fixture.
    # Execute a fresh module to exercise imports even after earlier tests.
    # Reloading the shared module replaces enum/classes still referenced by
    # previously imported pipeline/resolver modules and leaks test-only state.
    original_framing = contracts.Framing
    spec = importlib.util.spec_from_file_location('_isolated_ingestion_contracts', contracts.__file__)
    module = importlib.util.module_from_spec(spec)
    with monkeypatch.context() as guard:
        guard.setitem(sys.modules, spec.name, module)
        guard.setattr(builtins, "open", forbidden)
        guard.setattr(io, "open", forbidden)
        spec.loader.exec_module(module)
        reference = module.SourceReference("test-source", "partition", "source-id")
        record = module.IngestedLogRecord(
            "raw", reference, stream_identity=module.StreamIdentity("test-source"),
        )
        page = module.SourcePage((record,), False, next_cursor=b"opaque")
        assert page.records == (record,)
        assert record.first_observed_at is None
        assert not hasattr(record, "event_id")
    assert contracts.Framing is original_framing
