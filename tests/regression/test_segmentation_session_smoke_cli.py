"""Offline CLI validation using fake acquisition and real segmentation sessions."""

import builtins
from dataclasses import replace
from datetime import datetime, timezone
import importlib.util
import io
import json
import logging
import os
from pathlib import Path
import socket
import sqlite3
from types import SimpleNamespace

import pytest
import requests


SECRET = "synthetic-password-do-not-print"
TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJzZWNyZXQifQ.signature"
PRIVATE = "Authorization: Bearer " + TOKEN
HEADER = "2026-10-04T00:00:00Z " + PRIVATE
NEXT = "2026-10-04T00:00:01Z " + SECRET
CURSOR = "private-cursor-" + TOKEN


@pytest.fixture
def smoke(monkeypatch):
    root = Path(__file__).resolve().parents[2]
    monkeypatch.syspath_prepend(str(root / "src" / "backend"))
    monkeypatch.syspath_prepend(str(root / "tools"))

    def forbidden(*args, **kwargs):
        pytest.fail("Manual CLI tests must not access network, SQLite or processing state")

    for owner, names in (
        (requests.sessions.Session, ("__init__", "request")),
        (socket.socket, ("connect", "connect_ex")),
        (socket, ("create_connection", "getaddrinfo")),
        (sqlite3, ("connect",)), (sqlite3.dbapi2, ("connect",)),
    ):
        for name in names:
            monkeypatch.setattr(owner, name, forbidden)
    original_import = builtins.__import__
    blocked = {
        "ai_engine", "parser_layer", "template_layer", "downstream_pipeline",
        "full_pipeline_v2", "streamlit", "drain3",
        "segmentation_layer.segmentation_pipeline", "segmentation_layer.header_discovery",
        "segmentation_layer.regex_validator", "segmentation_layer.policy_registry",
    }

    def guarded_import(name, globals=None, locals=None, fromlist=(), level=0):
        candidates = {name, name.split(".")[0]} | {name + "." + item for item in fromlist or ()}
        if candidates & blocked:
            pytest.fail("Forbidden processing import in segmentation smoke")
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", guarded_import)
    for name in list(os.environ):
        if name.startswith(("OPENSEARCH_", "SEGMENTATION_SMOKE_")):
            monkeypatch.delenv(name)
    environment = {
        "OPENSEARCH_HOSTS": "https://node-a.invalid, node-b.invalid",
        "OPENSEARCH_USERNAME": "private-user",
        "OPENSEARCH_PASSWORD": SECRET,
        "OPENSEARCH_USE_SSL": "true", "OPENSEARCH_VERIFY_CERTS": "true",
        "OPENSEARCH_CA_BUNDLE": "synthetic-ca.pem",
        "OPENSEARCH_SOURCE_SCOPE": "private-profile", "OPENSEARCH_INDEX": "private-index-*",
        "OPENSEARCH_SMOKE_END": "2026-10-04T12:00:00+03:00",
        "OPENSEARCH_SMOKE_LOOKBACK_MINUTES": "30", "OPENSEARCH_SMOKE_PAGE_SIZE": "2",
        "OPENSEARCH_SMOKE_NAMESPACE": "private-namespace",
        "OPENSEARCH_SMOKE_WORKLOAD": "private-workload", "OPENSEARCH_SMOKE_CONTAINER": "",
        "SEGMENTATION_SMOKE_REGEX": r"^\d{4}-\d{2}-\d{2}T",
        "SEGMENTATION_SMOKE_POLICY_ID": "private-policy-" + TOKEN,
        "SEGMENTATION_SMOKE_VALIDATION_REFERENCE": "private-validation-" + TOKEN,
    }
    for name, value in environment.items():
        monkeypatch.setenv(name, value)
    spec = importlib.util.spec_from_file_location(
        "manual_segmentation_session_smoke", root / "tools" / "segmentation_session_smoke.py",
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.test_forbidden = forbidden
    return module


def record(number, raw=HEADER, **identity_changes):
    from ingestion_layer.contracts import Framing, IngestedLogRecord, SourceReference, StreamIdentity

    values = dict(source_scope="private-cluster", namespace="private-namespace", workload="private-workload",
                  pod="private-pod", pod_instance="private-pod-id", container="private-container",
                  container_instance="private-container-id", channel="stdout")
    return IngestedLogRecord(
        raw, SourceReference("private-profile", "private-index-1", "private-record-" + str(number)),
        stream_identity=StreamIdentity(**(values | identity_changes)),
        source_timestamp_raw="2026-10-04T00:00:00.123456789Z",
        retrieval_order=(1791072000123, 2**80 + number),
        metadata=(("sequence", 2**80 + number), ("private", TOKEN)), framing=Framing.PHYSICAL_LINE,
    )


def pages(*, spanning=True):
    from ingestion_layer.contracts import SourcePage

    first = (record(1), record(2, "  " + PRIVATE)) if spanning else (record(1), record(2, NEXT))
    second = (record(3, "\t" + SECRET), record(4, NEXT)) if spanning else (
        record(3, HEADER, pod_instance="other-pod", container_instance="other-container", channel="stderr"),
        record(4, NEXT, pod_instance="other-pod", container_instance="other-container", channel="stderr"),
    )
    return (SourcePage(first, False, next_cursor=CURSOR, page_limit_reached=True),
            SourcePage(second, True, cycle_budget_reached=True))


def install_source(smoke, monkeypatch, source_pages, before_second=None):
    calls, configs, closed = [], [], []
    iterator = iter(source_pages)

    class FakeClient:
        def __init__(self, config):
            configs.append(config)

        def __enter__(self):
            return self

        def __exit__(self, *args):
            closed.append(True)

    class FakeSource:
        def __init__(self, client):
            pass

        def read_page(self, **kwargs):
            if len(calls) == 1 and before_second is not None:
                before_second()
            calls.append(kwargs)
            return next(iterator)

    monkeypatch.setattr(smoke, "OpenSearchClient", FakeClient)
    monkeypatch.setattr(smoke, "OpenSearchSource", FakeSource)
    return SimpleNamespace(calls=calls, configs=configs, closed=closed)


def safe_rows(capsys):
    captured = capsys.readouterr()
    assert captured.err == ""
    for private in (SECRET, TOKEN, PRIVATE, CURSOR, "private-user", "private-index", "private-record",
                    "private-cluster", "private-pod", "private-policy", "private-validation", "Traceback"):
        assert private not in captured.out
    rows = [json.loads(line) for line in captured.out.splitlines()]
    assert sum("result" in row for row in rows) == 1
    assert "result" in rows[-1]
    return rows


def test_real_sessions_receive_two_fake_pages_and_continuous_replay(smoke, monkeypatch, capsys):
    real_session = smoke.SegmentationSession
    sessions, page_calls, stream_closes = [], [], []

    class SpySession(real_session):
        def __init__(self, provider):
            super().__init__(provider)
            sessions.append(self)

        def feed_page(self, page):
            page_calls.append((self, page))
            return super().feed_page(page)

        def close_stream(self, key):
            stream_closes.append(key)
            pytest.fail("The smoke must not close a stream at a page boundary")

    monkeypatch.setattr(smoke, "SegmentationSession", SpySession)
    acquired = pages()

    def before_second():
        assert len(sessions) == 1
        assert sessions[0].active_stream_count == 1
        assert sessions[0].pending_record_count == 2
        assert sessions[0].closed is False

    source = install_source(smoke, monkeypatch, acquired, before_second)
    assert smoke.main([]) == 0
    rows = safe_rows(capsys)
    result = rows[-1]
    assert result["result"] == "PASS"
    for field in ("page_boundary_equivalent", "cross_page_multiline_observed",
                  "stream_isolation_valid", "close_released_all_state"):
        assert result[field] is True
    assert result["pages_read"] == 2 and result["records_read"] == 4
    assert result["stream_count"] == result["channel_count"] == 1
    assert result["analysis_end_outputs"] == 1
    assert len(sessions) == 2 and sessions[0] is not sessions[1]
    assert page_calls == [(sessions[0], acquired[0]), (sessions[0], acquired[1])]
    assert stream_closes == []
    assert all(s.closed and s.active_stream_count == s.pending_record_count == 0 for s in sessions)
    assert [call["cursor"] for call in source.calls] == [None, CURSOR]
    assert all(call["start"] == datetime(2026, 10, 4, 8, 30, tzinfo=timezone.utc) for call in source.calls)
    assert all(call["end"] == datetime(2026, 10, 4, 9, tzinfo=timezone.utc) for call in source.calls)
    assert source.configs[0].verify_certs is True
    assert source.configs[0].ca_bundle == "synthetic-ca.pem"
    assert source.configs[0].hosts == ("https://node-a.invalid", "node-b.invalid")
    assert source.configs[0].page_size_limit == 2 and source.closed == [True]


def test_no_cross_page_event_still_passes_with_honest_notice(smoke, monkeypatch, capsys):
    install_source(smoke, monkeypatch, pages(spanning=False))
    assert smoke.main([]) == 0
    rows = safe_rows(capsys)
    result = rows[-1]
    assert result["result"] == "PASS" and result["cross_page_multiline_observed"] is False
    assert result["page_boundary_equivalent"] is True
    assert result["stream_count"] == result["channel_count"] == 2
    assert result["pod_instance_count"] == result["container_instance_count"] == 2
    assert any("NOT OBSERVED" in row.get("notice", "") for row in rows)


@pytest.mark.parametrize("name", [
    "OPENSEARCH_HOSTS", "OPENSEARCH_USERNAME", "OPENSEARCH_PASSWORD", "OPENSEARCH_USE_SSL",
    "OPENSEARCH_VERIFY_CERTS", "OPENSEARCH_SOURCE_SCOPE", "OPENSEARCH_INDEX",
    "SEGMENTATION_SMOKE_REGEX", "SEGMENTATION_SMOKE_POLICY_ID", "SEGMENTATION_SMOKE_VALIDATION_REFERENCE",
])
def test_missing_configuration_fails_before_network_without_echo(smoke, monkeypatch, capsys, name):
    monkeypatch.delenv(name)
    assert smoke.main([]) == 1
    assert safe_rows(capsys)[-1] == {"result": "FAIL", "reason": "configuration_error"}


@pytest.mark.parametrize("name,value", [
    ("SEGMENTATION_SMOKE_REGEX", "[" + TOKEN),
    ("OPENSEARCH_VERIFY_CERTS", TOKEN), ("OPENSEARCH_SMOKE_PAGE_SIZE", "0"),
    ("OPENSEARCH_SMOKE_LOOKBACK_MINUTES", "0"), ("OPENSEARCH_SMOKE_END", TOKEN),
])
def test_invalid_configuration_is_redacted_before_acquisition(smoke, monkeypatch, capsys, name, value):
    monkeypatch.setenv(name, value)
    assert smoke.main([]) == 1
    assert safe_rows(capsys)[-1]["reason"] == "configuration_error"


@pytest.mark.parametrize("case", ["empty_first", "no_cursor", "empty_second"])
def test_two_nonempty_pages_are_required(smoke, monkeypatch, capsys, case):
    first, second = pages()
    if case == "empty_first":
        first = replace(first, records=())
    elif case == "no_cursor":
        first = replace(first, next_cursor=None)
    else:
        second = replace(second, records=())
    source = install_source(smoke, monkeypatch, (first, second))
    assert smoke.main([]) == 1
    rows = safe_rows(capsys)
    assert rows[-1]["result"] == "FAIL"
    assert len(source.calls) == (2 if case == "empty_second" else 1)
    assert source.closed == [True]


def test_page_boundary_flush_is_detected(smoke, monkeypatch, capsys):
    class FlushesAtPage(smoke.SegmentationSession):
        def feed_page(self, page):
            yield from super().feed_page(page)
            yield from self.close()

    monkeypatch.setattr(smoke, "SegmentationSession", FlushesAtPage)
    install_source(smoke, monkeypatch, pages())
    assert smoke.main([]) == 1
    assert safe_rows(capsys)[-1]["reason"] == "unexpected_emission_reason"


@pytest.mark.parametrize("coordinate", ["source_scope", "pod_instance", "container_instance", "channel"])
def test_stream_contamination_causes_failure(smoke, monkeypatch, capsys, coordinate):
    from segmentation_layer.contracts import AssembledEvent
    foreign = record(99, "  foreign", **{coordinate: "foreign-value"})

    class Contaminated(smoke.SegmentationSession):
        def feed_page(self, page):
            for output in super().feed_page(page):
                if isinstance(output, AssembledEvent):
                    output = replace(output, records=(foreign,) + output.records[1:])
                yield output

    monkeypatch.setattr(smoke, "SegmentationSession", Contaminated)
    install_source(smoke, monkeypatch, pages())
    assert smoke.main([]) == 1
    assert safe_rows(capsys)[-1]["reason"] == "stream_contamination"


def test_paged_vs_continuous_output_difference_causes_failure(smoke, monkeypatch, capsys):
    from segmentation_layer.contracts import AssembledEvent

    class DifferentEvidence(smoke.SegmentationSession):
        def feed_page(self, page):
            for output in super().feed_page(page):
                if isinstance(output, AssembledEvent):
                    first = output.evidence[0]
                    output = replace(output, evidence=(replace(first, ordinal=first.ordinal + 100),) + output.evidence[1:])
                yield output

    monkeypatch.setattr(smoke, "SegmentationSession", DifferentEvidence)
    install_source(smoke, monkeypatch, pages())
    assert smoke.main([]) == 1
    assert safe_rows(capsys)[-1]["reason"] == "page_boundary_not_equivalent"


def test_close_that_retains_state_fails(smoke, monkeypatch, capsys):
    class RetainsState(smoke.SegmentationSession):
        def close(self):
            return ()

    monkeypatch.setattr(smoke, "SegmentationSession", RetainsState)
    install_source(smoke, monkeypatch, pages())
    assert smoke.main([]) == 1
    assert safe_rows(capsys)[-1]["reason"] == "close_did_not_release_state"


def test_arbitrary_exception_details_and_arguments_are_never_echoed(smoke, monkeypatch, capsys):
    def fail(config):
        raise RuntimeError(PRIVATE + SECRET)

    monkeypatch.setattr(smoke, "OpenSearchClient", fail)
    assert smoke.main([]) == 1
    assert safe_rows(capsys)[-1]["reason"] == "runtime_error"
    assert smoke.main(["--unknown=" + TOKEN]) == 1
    assert safe_rows(capsys)[-1]["reason"] == "invalid_arguments"


def test_no_files_processing_state_or_global_tls_warning_changes(smoke, monkeypatch, capsys):
    import warnings
    from urllib3.exceptions import InsecureRequestWarning

    monkeypatch.setenv("OPENSEARCH_VERIFY_CERTS", "false")
    monkeypatch.delenv("OPENSEARCH_CA_BUNDLE")
    source = install_source(smoke, monkeypatch, pages(),
                            before_second=lambda: warnings.warn(PRIVATE, InsecureRequestWarning))
    initial_filters = list(warnings.filters)
    initial_logging = logging.root.manager.disable
    with monkeypatch.context() as guard:
        guard.setattr(builtins, "open", smoke.test_forbidden)
        guard.setattr(io, "open", smoke.test_forbidden)
        assert smoke.main([]) == 0
    safe_rows(capsys)
    assert warnings.filters == initial_filters
    assert logging.root.manager.disable == initial_logging
    assert source.configs[0].verify_certs is False


def test_duplicate_reference_values_do_not_create_false_cross_page_observation(smoke, monkeypatch, capsys):
    from ingestion_layer.contracts import SourcePage

    first = record(1)
    second = replace(first, raw_text=NEXT)
    acquired = (SourcePage((first,), False, next_cursor=CURSOR), SourcePage((second,), True))
    install_source(smoke, monkeypatch, acquired)
    assert smoke.main([]) == 0
    assert safe_rows(capsys)[-1]["cross_page_multiline_observed"] is False


def test_leading_and_pending_blank_provenance_is_accepted(smoke, monkeypatch, capsys):
    from ingestion_layer.contracts import SourcePage

    acquired = (SourcePage((record(1, " \t"), record(2)), False, next_cursor=CURSOR),
                SourcePage((record(3, "\t\r\n"), record(4, NEXT)), True))
    install_source(smoke, monkeypatch, acquired)
    assert smoke.main([]) == 0
    result = safe_rows(capsys)[-1]
    assert result["cross_page_multiline_observed"] is True
    assert result["blank_context_outputs"] == 1


@pytest.mark.parametrize("coordinate", ["source_scope", "pod_instance", "container_instance", "channel"])
def test_interleaved_streams_with_one_distinct_coordinate_stay_separate(smoke, monkeypatch, capsys, coordinate):
    from ingestion_layer.contracts import SourcePage

    other = {coordinate: "different-incarnation"}
    acquired = (SourcePage((record(1), record(2, **other)), False, next_cursor=CURSOR),
                SourcePage((record(3, "  " + PRIVATE), record(4, "  " + SECRET, **other)), True))
    install_source(smoke, monkeypatch, acquired)
    assert smoke.main([]) == 0
    result = safe_rows(capsys)[-1]
    assert result["stream_count"] == result["analysis_end_outputs"] == 2
    assert result["cross_page_multiline_observed"] is True
    assert result["stream_isolation_valid"] is True


@pytest.mark.parametrize("fault", ["copied_contributor", "reordered_contributors", "changed_text", "lost_tail"])
def test_provenance_audit_rejects_corrupted_outputs(smoke, monkeypatch, capsys, fault):
    class Corrupted(smoke.SegmentationSession):
        def close(self):
            outputs = super().close()
            if not outputs:
                return outputs
            if fault == "lost_tail":
                return ()
            output = outputs[0]
            if fault == "copied_contributor":
                output = replace(output, records=(replace(output.records[0]),) + output.records[1:])
            elif fault == "reordered_contributors":
                output = replace(output, records=tuple(reversed(output.records)))
            else:
                output = replace(output, text=PRIVATE)
            return (output,) + outputs[1:]

    from ingestion_layer.contracts import SourcePage

    acquired = (SourcePage((record(1),), False, next_cursor=CURSOR),
                SourcePage((record(2, "  " + PRIVATE),), True))
    monkeypatch.setattr(smoke, "SegmentationSession", Corrupted)
    install_source(smoke, monkeypatch, acquired)
    assert smoke.main([]) == 1
    assert safe_rows(capsys)[-1]["reason"] == "provenance_mismatch"


@pytest.mark.parametrize("exception_name,reason", [
    ("OpenSearchClientError", "acquisition_error"), ("OpenSearchSourceError", "acquisition_error"),
    ("SmokeFailure", "runtime_error"),
])
def test_named_exception_details_are_not_trusted(smoke, monkeypatch, capsys, exception_name, reason):
    def fail(config):
        logging.critical(PRIVATE)
        raise getattr(smoke, exception_name)(PRIVATE + SECRET)

    initial_logging = logging.root.manager.disable
    monkeypatch.setattr(smoke, "OpenSearchClient", fail)
    assert smoke.main([]) == 1
    assert safe_rows(capsys)[-1] == {"result": "FAIL", "reason": reason}
    assert logging.root.manager.disable == initial_logging


def test_page_flags_are_not_lifecycle_boundaries(smoke, monkeypatch, capsys):
    first, second = pages()
    acquired = (replace(first, interval_exhausted=True, page_limit_reached=False, cycle_budget_reached=True),
                replace(second, interval_exhausted=False, page_limit_reached=True, cycle_budget_reached=False))
    install_source(smoke, monkeypatch, acquired)
    assert smoke.main([]) == 0
    result = safe_rows(capsys)[-1]
    assert result["cross_page_multiline_observed"] is True
    assert result["page_boundary_equivalent"] is True


@pytest.mark.parametrize("framing,identity", [("LOGICAL_EVENT", True), ("PHYSICAL_LINE", False)])
def test_unassemblable_records_fail_explicitly(smoke, monkeypatch, capsys, framing, identity):
    from ingestion_layer.contracts import Framing

    first, second = pages()
    original = first.records[0]
    first = replace(first, records=(replace(original, framing=getattr(Framing, framing),
                                           stream_identity=original.stream_identity if identity else None),))
    install_source(smoke, monkeypatch, (first, second))
    assert smoke.main([]) == 1
    assert safe_rows(capsys)[-1]["reason"] == "record_not_accepted"


def test_non_tls_warnings_cannot_echo_sensitive_data(smoke, monkeypatch, capsys):
    import warnings

    install_source(smoke, monkeypatch, pages(),
                   before_second=lambda: warnings.warn(PRIVATE, RuntimeWarning))
    initial_filters = list(warnings.filters)
    with warnings.catch_warnings(record=True) as emitted:
        warnings.simplefilter("always")
        assert smoke.main([]) == 0
    safe_rows(capsys)
    assert warnings.filters == initial_filters
    assert not emitted
