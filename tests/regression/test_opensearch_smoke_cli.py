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
    import application_environment
    # CLI bootstrap must not read real repository credentials during tests.
    monkeypatch.setattr(application_environment, 'load_dotenv', lambda *a, **k: None)
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


def test_cli_loads_shared_environment_before_configuration(smoke, monkeypatch):
    import application_environment
    calls = []
    monkeypatch.setattr(application_environment, 'load_environment', lambda: calls.append('environment'))
    # Patch the imported binding too if the tool uses a direct import.
    monkeypatch.setattr(smoke, 'load_environment', application_environment.load_environment, raising=False)
    monkeypatch.setattr(smoke, 'run_smoke', lambda: calls.append('smoke'))
    assert smoke.main([]) == 0
    assert calls == ['environment', 'smoke']


def test_cli_dotenv_and_daily_resolution_share_production_paths(smoke, monkeypatch, tmp_path, capsys):
    import application_environment
    from dotenv import load_dotenv
    from test_opensearch_source import hit, page
    # dotenv adds keys directly; keep those additions scoped to this test too.
    monkeypatch.setattr(os, 'environ', dict(os.environ))
    env_file = tmp_path / '.env'
    env_file.write_text('\n'.join([
        'OPENSEARCH_HOSTS=https://synthetic.invalid', 'OPENSEARCH_USERNAME=file-reader',
        'OPENSEARCH_PASSWORD=temporary-fixture-secret', 'OPENSEARCH_USE_SSL=true',
        'OPENSEARCH_VERIFY_CERTS=true', 'OPENSEARCH_SOURCE_SCOPE=profile',
        'OPENSEARCH_INDEX=synthetic-cluster*', 'OPENSEARCH_INDEX_STRATEGY=daily_utc',
        'OPENSEARCH_SMOKE_END=2026-10-04T09:00Z', 'OPENSEARCH_SMOKE_LOOKBACK_MINUTES=15',
        'OPENSEARCH_SMOKE_PAGE_SIZE=1', 'OPENSEARCH_SMOKE_NAMESPACE=synthetic-ns',
        'OPENSEARCH_SMOKE_WORKLOAD=synthetic-app', 'OPENSEARCH_SMOKE_CONTAINER=synthetic-container',
    ]) + '\n', encoding='utf-8')
    for name in list(os.environ):
        if name.startswith('OPENSEARCH_'):
            monkeypatch.delenv(name)
    monkeypatch.setenv('OPENSEARCH_USERNAME', 'process-reader')
    bootstrap_paths, calls, configs = [], [], []
    def configured_dotenv(path, *, override):
        bootstrap_paths.append(Path(path))
        assert override is False
        return load_dotenv(env_file, override=override)
    monkeypatch.setattr(application_environment, 'load_dotenv', configured_dotenv)
    class Client:
        def __init__(self, config):
            self.config = config
            configs.append(config)
        def __enter__(self):
            return self
        def __exit__(self, *args):
            pass
        def post_json(self, path, query):
            calls.append((path, query))
            row = hit('two' if 'search_after' in query else 'one', 2 if 'search_after' in query else 1)
            row['_index'] = 'synthetic-cluster-2026.10.04'
            return page(row)
    monkeypatch.setattr(smoke, 'OpenSearchClient', Client)
    assert smoke.main([]) == 0
    assert bootstrap_paths == [smoke.ROOT / '.env']
    assert configs[0].username == 'process-reader'
    assert configs[0].index_expression == 'synthetic-cluster*'
    assert len(calls) == 3
    assert all(path == '/synthetic-cluster-2026.10.04/_search' for path, query in calls)
    output = capsys.readouterr()
    rows = [json.loads(line) for line in output.out.splitlines()]
    assert rows[0]['resolved_index'] == 'synthetic-cluster-2026.10.04'
    assert rows[0]['index_strategy'] == 'daily_utc'
    assert rows[-1] == {'result': 'PASS', 'page1_repeat_stable': True, 'page2_read': True, 'page2_nonempty': True}
    for secret in ('temporary-fixture-secret', 'process-reader', 'file-reader'):
        assert secret not in output.out + output.err
