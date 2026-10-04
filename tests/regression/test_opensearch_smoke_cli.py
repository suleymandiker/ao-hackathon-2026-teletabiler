"""Small offline checks for the manual tool's environment and output boundary."""

from datetime import datetime, timezone
import importlib.util
import json
import os
from pathlib import Path
import socket

import pytest
import requests


@pytest.fixture
def smoke(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("The manual smoke test must not make network requests in pytest")

    monkeypatch.setattr(requests.sessions.Session, "__init__", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(socket.socket, "connect", forbidden)
    for name in list(os.environ):
        if name.startswith("OPENSEARCH_"):
            monkeypatch.delenv(name)
    for name, value in {
        "HOSTS": "https://node-a.invalid, node-b.invalid", "USERNAME": "synthetic-user",
        "PASSWORD": "synthetic-secret", "USE_SSL": "true", "VERIFY_CERTS": "false",
        "SOURCE_SCOPE": "synthetic-profile", "INDEX": "synthetic-index-*",
        "SMOKE_END": "2026-10-04T12:00:00+03:00", "SMOKE_LOOKBACK_MINUTES": "30",
        "SMOKE_PAGE_SIZE": "1", "SMOKE_NAMESPACE": "synthetic-ns",
        "SMOKE_WORKLOAD": "synthetic-workload", "SMOKE_CONTAINER": "synthetic-container",
    }.items():
        monkeypatch.setenv("OPENSEARCH_" + name, value)
    root = Path(__file__).resolve().parents[2]
    monkeypatch.syspath_prepend(str(root / "src" / "backend"))
    spec = importlib.util.spec_from_file_location("manual_opensearch_smoke", root / "tools" / "opensearch_smoke.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # Import alone must not construct a client.
    return module


def test_environment_and_output_allowlist_with_fake_pages(smoke, monkeypatch, capsys):
    from ingestion_layer.contracts import Framing, IngestedLogRecord, SourcePage, SourceReference, StreamIdentity

    calls, configs = [], []
    raw = 'synthetic private message: {"Authorization": "synthetic-token"}'
    record = IngestedLogRecord(
        raw, SourceReference("synthetic-profile", "synthetic-index-1", "record-1"),
        stream_identity=StreamIdentity(
            "cluster", namespace="synthetic-ns", workload="synthetic-workload", pod="pod",
            pod_instance="uid", container="synthetic-container", container_instance="run", channel="stderr",
        ), retrieval_order=(1791104400000, 2**60 + 1), framing=Framing.PHYSICAL_LINE,
    )
    first = SourcePage((record,), False, next_cursor="synthetic-private-cursor", page_limit_reached=True)
    pages = iter((first, SourcePage((), True), first))

    class FakeClient:
        def __init__(self, config):
            configs.append(config)

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

    class FakeSource:
        def __init__(self, client):
            pass

        def read_page(self, **kwargs):
            calls.append(kwargs)
            return next(pages)

    monkeypatch.setattr(smoke, "OpenSearchClient", FakeClient)
    monkeypatch.setattr(smoke, "OpenSearchSource", FakeSource)
    assert smoke.main([]) == 0
    config = configs[0]
    assert config.hosts == ("https://node-a.invalid", "node-b.invalid")
    assert config.use_ssl is True and config.verify_certs is False
    assert config.index_expression == "synthetic-index-*" and config.page_size_limit == 1
    assert [call["cursor"] for call in calls] == [None, "synthetic-private-cursor", None]
    assert calls[0] == calls[2]
    assert all(call["start"] == datetime(2026, 10, 4, 8, 30, tzinfo=timezone.utc) for call in calls)
    assert all(call["end"] == datetime(2026, 10, 4, 9, tzinfo=timezone.utc) for call in calls)
    output = capsys.readouterr()
    assert output.err == ""
    for secret in (raw, "synthetic-secret", "synthetic-user", "Authorization", "synthetic-private-cursor"):
        assert secret not in output.out
    rows = [json.loads(line) for line in output.out.splitlines()]
    record_row = next(row for row in rows if "record_id" in row)
    assert set(record_row) == {
        "page", "source_partition", "record_id", "version", "namespace", "workload",
        "pod", "pod_instance_present", "container", "container_instance_present",
        "channel", "retrieval_order", "framing", "raw_text_length",
    }
    assert record_row["raw_text_length"] == len(raw)
    assert rows[-1]["result"] == "PASS"


def test_invalid_boolean_fails_without_echoing_environment_value(smoke, monkeypatch, capsys):
    monkeypatch.setenv("OPENSEARCH_VERIFY_CERTS", "synthetic-sensitive-invalid-value")
    assert smoke.main([]) == 1
    output = capsys.readouterr()
    assert "OPENSEARCH_VERIFY_CERTS must be true or false" in output.err
    assert "synthetic-sensitive-invalid-value" not in output.out + output.err


def test_unexpected_exception_details_are_not_printed(smoke, monkeypatch, capsys):
    def failed_client(config):
        raise RuntimeError("synthetic-secret Authorization: synthetic-token private document")

    monkeypatch.setattr(smoke, "OpenSearchClient", failed_client)
    assert smoke.main([]) == 1
    output = capsys.readouterr()
    assert output.err == "FAIL: configuration or unexpected validation error; details suppressed.\n"
    for secret in ("synthetic-secret", "Authorization", "private document", "Traceback"):
        assert secret not in output.out + output.err
