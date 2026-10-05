"""Additive parser outcomes must not change canonical events or routing."""

from dataclasses import FrozenInstanceError
from datetime import datetime, timezone
import json
from pathlib import Path
import socket
import sqlite3
from types import SimpleNamespace

import pytest


OBSERVED = datetime(2026, 10, 4, 9, 0, tzinfo=timezone.utc)
EVENT_TIME = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
JSON_EVENT = (
    '{"timestamp":"2026-01-02T03:04:05Z","level":50,'
    '"message":"  database unavailable  ","service":"orders-api",'
    '"host":"app-01","trace_id":"abc123","attempt":2}'
)
PLAIN_EVENT = "  operation started\n    continuation detail  "


@pytest.fixture
def pipeline_factory(monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[2] / "src" / "backend"))

    def forbidden(*args, **kwargs):
        pytest.fail("Network, LLM, preparation, and registry access are forbidden")

    monkeypatch.setattr(sqlite3, "connect", forbidden)
    monkeypatch.setattr(sqlite3.dbapi2, "connect", forbidden)
    monkeypatch.setattr(socket.socket, "connect", forbidden)
    monkeypatch.setattr(socket.socket, "connect_ex", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(socket, "getaddrinfo", forbidden)

    import requests
    import ai_engine

    monkeypatch.setattr(requests.sessions.Session, "request", forbidden)
    monkeypatch.setattr(ai_engine, "call_ai_agent", forbidden)

    import parser_layer.parser_pipeline as module
    import parser_layer.canonical_event_builder as builder
    import parser_layer.policy.policy_discovery as discovery

    monkeypatch.setattr(discovery, "call_ai_agent", forbidden)
    # Patch constructors before creating a pipeline: even construction normally
    # initializes SQLite. No production state is opened by these tests.
    monkeypatch.setattr(module, "ParserPolicyRegistry", lambda *args, **kwargs: SimpleNamespace(
        get=forbidden, save=forbidden, invalidate=forbidden,
    ))
    monkeypatch.setattr(module, "ParserPolicyDiscovery", lambda: SimpleNamespace(
        signature=forbidden, discover=forbidden, validate=forbidden, enabled=forbidden,
    ))
    monkeypatch.setattr(module.ParserPipeline, "prepare", forbidden)

    class FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return OBSERVED

    monkeypatch.setattr(builder, "datetime", FixedDatetime)
    return module.ParserPipeline


def expected_event(raw, event_id, *, timestamp=None, severity=None, message=None,
                   service=None, host=None, trace_id=None, attributes=None):
    """Explicit pre-change payload snapshots, including real generated IDs."""
    return {
        "schema_version": "2.0",
        "event_id": event_id,
        "timestamp": timestamp,
        "observed_timestamp": OBSERVED,
        "severity": severity,
        "message": message,
        "resource": {"service": service, "host": host, "component": None},
        "trace": {"trace_id": trace_id, "span_id": None},
        "attributes": attributes if attributes is not None else {
            "raw_timestamp": None, "timestamp_status": "missing",
        },
        "raw": raw,
        "timestamp_provenance": {
            "basis": "message_explicit" if timestamp is not None else "untimed",
            "message_timestamp_raw": "2026-01-02T03:04:05Z" if timestamp is not None else None,
            "source_timezone": None, "source_record_time": None,
            "source_record_timestamp_raw": None, "source_record_field": None,
        },
    }


BUILTIN_CASES = [
    ("json", expected_event(
        JSON_EVENT, "30f93b03eca70", timestamp=EVENT_TIME, severity="ERROR",
        message="database unavailable", service="orders-api", host="app-01",
        trace_id="abc123", attributes={"attempt": 2},
    )),
    ("kv", expected_event(
        'time=2026-01-02T03:04:05Z level=WARN service=orders-api message="retry later" attempt=2',
        "63f9df50ddf6b", timestamp=EVENT_TIME, severity="WARNING",
        message="retry later", service="orders-api", attributes={"attempt": 2},
    )),
    ("structured_text", expected_event(
        "2026-01-02T03:04:05Z INFO operation started\n    ERROR continuation detail",
        "19a9130bc0afe", timestamp=EVENT_TIME, severity="INFO",
        message="operation started\n    ERROR continuation detail", attributes={},
    )),
    ("syslog", expected_event(
        "<34>1 2026-01-02T03:04:05Z host-01 app 123 ID47 - operation failed",
        "1bf79cfc16bfb", timestamp=EVENT_TIME, severity="CRITICAL",
        message="app: 123 ID47 - operation failed", host="host-01",
        attributes={"syslog_app_name": "app", "syslog_priority": 34},
    )),
]


@pytest.mark.parametrize("parser_id,expected", BUILTIN_CASES, ids=[case[0] for case in BUILTIN_CASES])
def test_process_preserves_pre_change_builtin_payload(pipeline_factory, parser_id, expected):
    actual = pipeline_factory().process(expected["raw"])
    assert actual == expected
    # Check serialized content and field ordering as well as Python values.
    assert json.dumps(actual, default=str) == json.dumps(expected, default=str)


@pytest.mark.parametrize("parser_id,expected", BUILTIN_CASES, ids=[case[0] for case in BUILTIN_CASES])
def test_builtin_outcome_preserves_payload(pipeline_factory, parser_id, expected):
    from parser_layer.contracts import Delivery, Recognition

    outcome = pipeline_factory().process_with_outcome(expected["raw"])
    assert outcome.event == expected
    assert outcome.recognition is Recognition.BUILTIN_RECOGNIZED
    assert outcome.delivery is Delivery.PARSED
    assert outcome.parser_id == parser_id
    assert outcome.reason_code is None


def test_plain_text_preserves_raw_and_existing_message_normalization(pipeline_factory):
    from parser_layer.contracts import Delivery, Recognition

    expected = expected_event(
        PLAIN_EVENT, "898a75f581999", message="operation started\n    continuation detail",
    )
    outcome = pipeline_factory().process_with_outcome(PLAIN_EVENT)
    assert outcome.event == expected
    assert outcome.event["raw"] == PLAIN_EVENT
    assert outcome.recognition is Recognition.PLAIN_TEXT
    assert outcome.delivery is Delivery.PARSED
    assert outcome.parser_id == "plain_text"
    assert outcome.reason_code == "plain_text_unclassified"
    assert pipeline_factory().process(PLAIN_EVENT) == expected


@pytest.mark.parametrize("raw", [None, "", " \t\r\n"])
def test_empty_input_is_explicit_and_does_not_allocate_an_event(pipeline_factory, raw):
    from parser_layer.contracts import Delivery, Recognition

    pipeline = pipeline_factory()
    outcome = pipeline.process_with_outcome(raw)
    assert outcome.event is None
    assert outcome.recognition is Recognition.EMPTY_INPUT
    assert outcome.delivery is Delivery.IGNORED
    assert outcome.parser_id is None
    assert outcome.reason_code == "empty_input"
    assert pipeline.process(raw) is None
    assert pipeline.process(JSON_EVENT) == BUILTIN_CASES[0][1]


def test_rejection_does_not_claim_unsupported_or_malformed(pipeline_factory):
    from parser_layer.contracts import Delivery

    raw = "CUSTOM | INFO semantic body"
    expected = expected_event(raw, "778cede0f64a3", severity="INFO", message=raw)
    outcome = pipeline_factory().process_with_outcome(raw)
    assert outcome.event == expected
    assert outcome.recognition is None
    assert outcome.delivery is Delivery.FALLBACK
    assert outcome.parser_id == "plain_text"
    assert outcome.reason_code == "no_fields"
    assert pipeline_factory().process(raw) == expected


def test_plain_text_routing_does_not_claim_malformed_detection(pipeline_factory):
    from parser_layer.contracts import Delivery, Recognition

    raw = '{"message":'
    outcome = pipeline_factory().process_with_outcome(raw)
    assert outcome.event == expected_event(raw, "b1ee69fcc6ba5", message=raw)
    assert outcome.recognition is Recognition.PLAIN_TEXT
    assert outcome.delivery is Delivery.PARSED
    assert outcome.reason_code == "plain_text_unclassified"


@pytest.mark.parametrize("failure_site", ["parser", "detector"])
def test_exceptions_preserve_event_and_are_explicit(pipeline_factory, monkeypatch, caplog, failure_site):
    from parser_layer.contracts import Delivery, Recognition

    def fail(*args):
        raise ValueError("deliberate parse failure")

    def failing_pipeline():
        pipeline = pipeline_factory()
        if failure_site == "parser":
            monkeypatch.setattr(pipeline.parsers["plain_text"], "parse", fail)
        else:
            monkeypatch.setattr(pipeline.detector, "detect", fail)
        return pipeline

    outcome = failing_pipeline().process_with_outcome(PLAIN_EVENT)
    assert outcome.recognition is Recognition.PARSER_ERROR
    assert outcome.delivery is Delivery.FALLBACK
    assert outcome.parser_id == "safe_fallback"
    assert outcome.reason_code == "processing_exception"
    assert outcome.event["message"] == PLAIN_EVENT
    assert outcome.event["raw"] == PLAIN_EVENT
    assert outcome.event["severity"] is None
    assert outcome.event["timestamp"] is None
    assert "deliberate parse failure" in caplog.text
    assert failing_pipeline().process(PLAIN_EVENT) == outcome.event


def test_empty_parser_fields_keep_last_resort_fallback(pipeline_factory, monkeypatch):
    from parser_layer.contracts import Delivery

    def rejecting_pipeline():
        pipeline = pipeline_factory()
        monkeypatch.setattr(pipeline.parsers["plain_text"], "parse", lambda event: None)
        return pipeline

    outcome = rejecting_pipeline().process_with_outcome(PLAIN_EVENT)
    assert outcome.recognition is None
    assert outcome.delivery is Delivery.FALLBACK
    assert outcome.parser_id == "safe_fallback"
    assert outcome.reason_code == "no_fields"
    assert outcome.event["message"] == PLAIN_EVENT
    assert outcome.event["raw"] == PLAIN_EVENT
    assert rejecting_pipeline().process(PLAIN_EVENT) == outcome.event


def test_compatibility_wrapper_returns_the_same_mutable_event(pipeline_factory, monkeypatch):
    from parser_layer.contracts import Delivery, ParseOutcome, Recognition

    event = {"raw": "same object"}
    outcome = ParseOutcome(event, Recognition.PLAIN_TEXT, Delivery.PARSED, "plain_text", None)
    pipeline = pipeline_factory()
    monkeypatch.setattr(pipeline, "process_with_outcome", lambda raw: outcome)
    assert pipeline.process("same object") is event
    with pytest.raises(FrozenInstanceError):
        outcome.delivery = Delivery.FALLBACK
    # The envelope is frozen; the existing canonical dictionary is not frozen.
    event["message"] = "still mutable"
    assert outcome.event["message"] == "still mutable"


@pytest.mark.parametrize("method", ["process", "process_with_outcome"])
def test_existing_uncaught_errors_still_escape(pipeline_factory, monkeypatch, method):
    pipeline = pipeline_factory()
    with pytest.raises(AttributeError):
        getattr(pipeline, method)(123)  # Existing input guard is outside the try block.

    def broken_builder(*args):
        raise RuntimeError("builder unavailable")

    monkeypatch.setattr(pipeline.builder, "build", broken_builder)
    with pytest.raises(RuntimeError, match="builder unavailable"):
        getattr(pipeline, method)(PLAIN_EVENT)


@pytest.mark.parametrize("returns_fields", [True, False])
def test_existing_custom_parser_branch_is_preserved_without_discovery(pipeline_factory, returns_fields):
    from parser_layer.contracts import Delivery

    fields = {"timestamp": None, "severity": "DEBUG", "message": "custom body", "attributes": {}}

    def configured_pipeline():
        pipeline = pipeline_factory()
        pipeline.custom_parser = SimpleNamespace(parse=lambda raw: fields if returns_fields else None)
        return pipeline

    outcome = configured_pipeline().process_with_outcome(PLAIN_EVENT)
    assert outcome.recognition is None  # Phase 1 has no learned-recognition category.
    assert outcome.delivery is (Delivery.PARSED if returns_fields else Delivery.FALLBACK)
    assert outcome.parser_id == ("custom" if returns_fields else "plain_text")
    assert outcome.reason_code == ("custom_parser_result" if returns_fields else "no_fields")
    assert outcome.event["message"] == ("custom body" if returns_fields else PLAIN_EVENT.strip())
    assert outcome.event["raw"] == PLAIN_EVENT
    assert configured_pipeline().process(PLAIN_EVENT) == outcome.event
