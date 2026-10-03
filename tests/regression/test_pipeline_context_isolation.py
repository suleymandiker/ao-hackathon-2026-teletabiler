"""Regression coverage for context retained by a reused full pipeline."""

import json
from pathlib import Path
import socket
import sqlite3
import tempfile
from types import SimpleNamespace
from zipfile import ZipFile

import pytest


@pytest.fixture(autouse=True)
def isolated_backend(monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[2] / "src" / "backend"))

    def forbidden(*args, **kwargs):
        pytest.fail("Network, LLM, and SQLite access are forbidden in this regression")

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
    # Also guard an already-imported RCA module's local gateway reference.
    import rca_layer.rca_engine as rca_engine

    monkeypatch.setattr(rca_engine, "call_ai_agent", forbidden)


class FakeSegmenter:
    def iter_events(self, path):
        yield Path(path).read_text(encoding="utf-8").rstrip("\n")


class FakeParser:
    def process(self, raw):
        return {
            "schema_version": "2.0",
            "event_id": "unrelated-raw-event",
            "timestamp": "2026-01-02T00:00:00Z",
            "severity": "ERROR",
            "message": "database connection error",
            "resource": {"host": "db-01", "service": "orders-db"},
            "attributes": {},
            "raw": raw,
        }


class FakeTemplater:
    def __init__(self, state_path=None, candidate_state_path=None):
        self.last_decision = {"source": "test", "validator_reason": "fixed-template"}

    def process(self, event):
        return SimpleNamespace(
            template_id="raw-connection-error",
            template=event["message"],
            reliable=True,
        )

    def save_state(self):
        pass


def test_raw_analysis_does_not_inherit_package_context(monkeypatch, tmp_path):
    import full_pipeline_v2

    monkeypatch.setattr(full_pipeline_v2, "SegmentationPipeline", FakeSegmenter)
    monkeypatch.setattr(full_pipeline_v2, "ParserPipeline", FakeParser)
    monkeypatch.setattr(full_pipeline_v2, "TemplatePipeline", FakeTemplater)
    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))
    pipeline = full_pipeline_v2.FullAIOpsPipelineV2(
        template_state=str(tmp_path / "unused-template.json"),
        drain_state=str(tmp_path / "unused-drain.bin"),
        use_ai_rca=False,
    )

    inventory_row = {
        "host": "db-01",
        "servis": "orders-db",
        "veri_merkezi": "package-only-dc",
        "kabin": "rack-1",
        "ortam": "package-only-env",
        "is_kritikligi": "high",
    }
    package_path = tmp_path / "with-topology.zip"
    with ZipFile(package_path, "w") as package:
        package.writestr("alarms.json", json.dumps([{
            "alarm_id": "package-alarm",
            "timestamp": "2026-01-01T00:00:00Z",
            "host": "db-01",
            "service": "orders-db",
            "severity": 5,
            "alarm_type": "disk_full",
            "message": "data volume full",
        }]))
        package.writestr(
            "host_inventory.csv",
            ",".join(inventory_row) + "\n" + ",".join(inventory_row.values()) + "\n",
        )
        package.writestr(
            "service_dependencies.csv",
            "kaynak_servis,hedef_servis,bagimlilik_tipi,kritiklik\n"
            "orders-api,orders-db,db,high\n",
        )

    package_result = pipeline.process_package(str(package_path))
    assert len(package_result["incidents"]) == 1
    assert package_result["incidents"][0]["context"]["host_inventory"] == [inventory_row]
    assert pipeline.downstream.topology.dependency_path("orders-api", "orders-db") == [
        "orders-api", "orders-db",
    ]

    # Reuse the cached object's lifecycle; matching names belong to a new analysis.
    raw_path = tmp_path / "unrelated.log"
    raw_path.write_text(
        "2026-01-02T00:00:00Z ERROR database connection error\n",
        encoding="utf-8",
    )
    raw_result = pipeline.process_file(str(raw_path))
    assert raw_result["stats"]["templated"] == 1
    assert len(raw_result["incidents"]) == 1
    raw_context = raw_result["incidents"][0]["context"]
    assert raw_context["hosts"] == ["db-01"]
    assert raw_context["services"] == ["orders-db"]
    assert raw_context["host_inventory"] == []
    assert raw_context["dependencies"] == []

    # An empty context or no context is valid; package lookups must not survive.
    for component in (
        pipeline.downstream,
        pipeline.downstream.correlator,
        pipeline.downstream.enricher,
        pipeline.downstream.incidents,
    ):
        topology = component.topology
        if topology is not None:
            assert topology.host_context("db-01") == {}
            assert topology.dependency_path("orders-api", "orders-db") is None
