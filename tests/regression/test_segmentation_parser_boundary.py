"""Segmentation owns boundaries, not parsers or canonical event state."""

from copy import deepcopy
from pathlib import Path
import socket
import sqlite3
from types import SimpleNamespace

import pytest


LINES = [
    "2026-01-01T00:00:00Z operation started",
    "    ERROR detail from continuation",
    "",
    "    detail after blank",
    "2026-01-01T00:00:01Z operation completed",
]
# Current normalization: omit blank lines, preserve indentation and join with LF.
EVENTS = ["\n".join([LINES[0], LINES[1], LINES[3]]), LINES[4]]
HEADER_REGEX = r"^\d{4}-\d{2}-\d{2}T"
# Captured before the ownership change; existing policies must remain reusable.
SIGNATURE = "f1611b2a88a00a6ad5546e9b"


@pytest.fixture(autouse=True)
def isolated_backend(monkeypatch, tmp_path):
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[2] / "src" / "backend"))
    monkeypatch.setenv("AIOPS_POLICY_REGISTRY_PATH", str(tmp_path / "policies.sqlite3"))
    forbidden_calls = []

    def forbid(label):
        def tripwire(*args, **kwargs):
            forbidden_calls.append(label)
            # pytest.fail is not swallowed by production's except Exception.
            pytest.fail(f"Segmentation must not call {label}")
        return tripwire

    real_connect = sqlite3.connect
    allowed_databases = set()

    def guarded_connect(database, *args, **kwargs):
        if Path(database).resolve() not in allowed_databases:
            forbid("SQLite outside the explicitly allowed temporary database")()
        return real_connect(database, *args, **kwargs)

    monkeypatch.setattr(sqlite3, "connect", guarded_connect)
    monkeypatch.setattr(sqlite3.dbapi2, "connect", guarded_connect)
    for owner, names in (
        (socket.socket, ("connect", "connect_ex")),
        (socket, ("create_connection", "getaddrinfo")),
    ):
        for name in names:
            monkeypatch.setattr(owner, name, forbid(f"network.{name}"))

    import requests
    import ai_engine

    monkeypatch.setattr(requests.sessions.Session, "request", forbid("HTTP request"))
    monkeypatch.setattr(ai_engine, "call_ai_agent", forbid("LLM gateway"))

    import parser_layer.parser_pipeline as parser
    import parser_layer.policy.policy_discovery as parser_discovery
    import segmentation_layer.header_discovery as header_discovery
    import segmentation_layer.policy_registry as segmentation_registry
    import segmentation_layer.segmentation_pipeline as segmentation

    for module in (parser_discovery, header_discovery, segmentation):
        monkeypatch.setattr(module, "call_ai_agent", forbid("imported LLM gateway"))
    monkeypatch.setattr(
        header_discovery.HeaderDiscovery, "discover", forbid("header discovery"),
    )
    for owner, names in (
        (parser.ParserPipeline, ("prepare", "process", "process_with_outcome")),
        (parser.ParserPolicyRegistry, ("get", "save", "invalidate")),
        (parser.ParserPolicyDiscovery, ("signature", "discover", "validate", "enabled")),
        (parser.CanonicalEventBuilder, ("build", "_generate_unique_event_id")),
        (parser.FormatDetector, ("detect",)),
    ):
        for name in names:
            monkeypatch.setattr(owner, name, forbid(f"{owner.__name__}.{name}"))
    for parser_class in (
        parser.JsonParser, parser.SyslogParser, parser.KVParser,
        parser.StructuredTextParser, parser.PositionalStructuredParser,
        parser.PlainTextParser, parser.PolicyParser,
    ):
        monkeypatch.setattr(parser_class, "parse", forbid(f"{parser_class.__name__}.parse"))
    # Never reuse or clear the application's process-wide learning cache.
    monkeypatch.setattr(segmentation_registry.SegmentationPolicyRegistry, "_memory", {})
    return SimpleNamespace(
        segmentation=segmentation, parser=parser, forbid=forbid,
        forbidden_calls=forbidden_calls, allowed_databases=allowed_databases,
    )


@pytest.fixture
def no_parser(monkeypatch, isolated_backend):
    env = isolated_backend
    for owner in (
        env.parser.ParserPipeline, env.parser.ParserPolicyRegistry,
        env.parser.CanonicalEventBuilder,
    ):
        monkeypatch.setattr(owner, "__init__", env.forbid(f"{owner.__name__} construction"))
    return env


@pytest.fixture
def fake_registry(monkeypatch, isolated_backend):
    class MemoryRegistry:
        def __init__(self):
            self.policies = {}
            self.get_calls = []
            self.put_calls = []

        def get(self, signature):
            self.get_calls.append(signature)
            policy = self.policies.get(signature)
            return (deepcopy(policy), "memory") if policy else (None, "miss")

        def put(self, signature, policy):
            assert policy["verified"] is True
            self.put_calls.append((signature, deepcopy(policy)))
            validation = policy["validation"]
            self.policies[signature] = {
                "verified": True,
                "regex": policy["regex"],
                "source": policy["source"],
                "confidence": policy["confidence"],
                "coverage_score": validation["coverage_score"],
                "event_count": validation["event_count"],
                "zero_loss": validation["accounting_zero_loss"],
                "parser_success": validation["parser_validation"]["success_ratio"],
            }

    registry = MemoryRegistry()
    monkeypatch.setattr(
        isolated_backend.segmentation, "SegmentationPolicyRegistry",
        lambda **kwargs: registry,
    )
    return registry


def write_log(tmp_path, terminator="\n"):
    path = tmp_path / "multiline.log"
    path.write_bytes((terminator.join(LINES) + terminator).encode("utf-8"))
    return str(path)


def assert_structural_validation(result):
    expected = {
        "valid": True, "accepted": True, "event_count": 2,
        "header_matches": 2, "candidate_unmatched_headers": 0,
        "accounting_zero_loss": True, "coverage_score": 100.0,
        "all_line_match_ratio": 50.0,
        "total_lines": 5, "total_nonempty_lines": 4, "blank_lines": 1,
        "event_line_counts": {
            "min": 1, "max": 3, "multiline_events": 1, "single_line_events": 1,
        },
        "parser_validation": {
            "attempts": 0, "success": 0, "success_ratio": 100.0, "failed_samples": [],
        },
    }
    assert {key: result[key] for key in expected} == expected


@pytest.mark.parametrize("enable_ai", [False, True], ids=["offline", "ai-enabled-construction"])
def test_constructor_does_not_construct_parser_pipeline(no_parser, fake_registry, enable_ai):
    no_parser.segmentation.SegmentationPipeline(enable_ai=enable_ai)
    assert no_parser.forbidden_calls == []
    assert fake_registry.get_calls == fake_registry.put_calls == []


def test_offline_validation_and_iteration_have_no_parser_side_effects(
    isolated_backend, fake_registry, monkeypatch, tmp_path,
):
    env = isolated_backend
    learning_state = {"existing-policy": {"regex": "^OLD ", "version": 1}}
    before = deepcopy(learning_state)
    constructions = []

    def inert_parser_registry(*args, **kwargs):
        constructions.append((args, kwargs))
        return SimpleNamespace(
            _memory=learning_state,
            get=env.forbid("parser registry get"),
            save=env.forbid("parser registry save"),
            invalidate=env.forbid("parser registry invalidate"),
        )

    # Let the current constructor reach validation without opening SQLite.
    # Construction is recorded and forbidden by the final assertion as well.
    monkeypatch.setattr(env.parser, "ParserPolicyRegistry", inert_parser_registry)
    pipeline = env.segmentation.SegmentationPipeline(enable_ai=False)
    path = write_log(tmp_path)
    assert_structural_validation(pipeline._validate(path, HEADER_REGEX))
    assert list(pipeline.iter_events(path)) == EVENTS
    assert pipeline.last_result["ai_calls"] == 0
    assert constructions == []
    assert learning_state == before
    assert env.forbidden_calls == []


def test_validator_without_parser_evidence_preserves_boundary_decisions(no_parser, tmp_path):
    validator = no_parser.segmentation.RegexValidator()
    path = write_log(tmp_path)
    assert_structural_validation(validator.validate(path, HEADER_REGEX))

    catch_all = validator.validate(path, r"^.*$", parser_fn=None)
    assert catch_all["valid"] is False
    assert catch_all["accepted"] is False
    assert catch_all["reason"] == "generic catch-all regex rejected"
    assert catch_all["parser_validation"]["attempts"] == 0

    prose = tmp_path / "prose.log"
    prose.write_text("ordinary prose\n    continuation\n", encoding="utf-8")
    no_headers = validator.validate(str(prose), r"^EVENT ", parser_fn=None)
    assert no_headers["valid"] is True
    assert no_headers["accepted"] is False
    assert no_headers["header_matches"] == 0
    assert no_headers["accounting_zero_loss"] is True
    assert no_headers["parser_validation"]["attempts"] == 0
    assert no_parser.forbidden_calls == []


@pytest.mark.parametrize("terminator", ["\n", "\r\n"], ids=["lf", "crlf"])
def test_offline_miss_then_hit_preserves_logical_events(
    no_parser, fake_registry, tmp_path, terminator,
):
    pipeline = no_parser.segmentation.SegmentationPipeline(enable_ai=False)
    path = write_log(tmp_path, terminator)
    cold_events = list(pipeline.iter_events(path))
    cold = deepcopy(pipeline.last_result)
    warm_events = list(pipeline.iter_events(path))
    warm = pipeline.last_result

    assert cold_events == warm_events == EVENTS
    assert cold["registry_hit"] == "miss"
    assert warm["registry_hit"] == "memory"
    assert fake_registry.get_calls == [SIGNATURE, SIGNATURE]
    assert len(fake_registry.put_calls) == 1
    assert fake_registry.put_calls[0][0] == SIGNATURE
    for result in (cold, warm):
        assert result["format_signature"] == SIGNATURE
        assert result["regex"] == no_parser.segmentation.DEFAULT_FALLBACK_REGEX
        assert result["verified"] is True
        assert result["ai_calls"] == result["ai_call_total"] == 0
        assert result["validation"]["event_count"] == 2
        assert result["validation"]["accounting_zero_loss"] is True
        records = list(pipeline.assembler.iter_event_records(path, result["regex"]))
        assert [record["event"] for record in records] == EVENTS
        assert [record["line_count"] for record in records] == [3, 1]
        assert [(record["start_line"], record["end_line"]) for record in records] == [
            (1, 4), (5, 5),
        ]
    assert no_parser.forbidden_calls == []


def test_existing_segmentation_policy_storage_remains_compatible(
    isolated_backend, monkeypatch, tmp_path,
):
    env = isolated_backend
    database = tmp_path / "policies.sqlite3"
    env.allowed_databases.add(database.resolve())
    path = write_log(tmp_path)
    # Seed the existing on-disk format directly, independent of current put().
    with sqlite3.connect(database) as connection:
        connection.execute("""
            CREATE TABLE segmentation_policies (
                signature TEXT PRIMARY KEY,
                regex TEXT NOT NULL,
                source TEXT NOT NULL,
                confidence REAL NOT NULL DEFAULT 0,
                coverage_score REAL NOT NULL DEFAULT 0,
                parser_success REAL NOT NULL DEFAULT 0,
                event_count INTEGER NOT NULL DEFAULT 0,
                zero_loss INTEGER NOT NULL DEFAULT 0,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                last_used REAL NOT NULL,
                hits INTEGER NOT NULL DEFAULT 0
            )
        """)
        connection.execute(
            "CREATE INDEX idx_segmentation_policy_last_used "
            "ON segmentation_policies(last_used)"
        )
        connection.execute(
            "INSERT INTO segmentation_policies VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (SIGNATURE, HEADER_REGEX, "ai-discovery", 0.91, 100.0, 37.5, 2, 1, 10, 20, 30, 7),
        )

    def snapshot():
        with sqlite3.connect(database) as connection:
            schema = connection.execute(
                "SELECT type, name, tbl_name, sql FROM sqlite_master ORDER BY type, name"
            ).fetchall()
            connection.row_factory = sqlite3.Row
            rows = connection.execute("SELECT * FROM segmentation_policies").fetchall()
        assert len(rows) == 1
        return schema, dict(rows[0])

    schema_before, policy_before = snapshot()
    monkeypatch.setattr(
        env.segmentation.SegmentationPolicyRegistry, "put",
        env.forbid("segmentation policy rewrite on a verified hit"),
    )
    monkeypatch.setattr(
        env.segmentation.RegexValidator, "validate",
        env.forbid("revalidation of a stored verified segmentation policy"),
    )
    # Real registries here; both are confined to the same temporary database.
    pipeline = env.segmentation.SegmentationPipeline(enable_ai=False, registry_path=str(database))
    assert list(pipeline.iter_events(path)) == EVENTS
    result = pipeline.last_result
    assert result["registry_hit"] == "sqlite"
    assert result["verified"] is True
    assert result["format_signature"] == SIGNATURE
    assert result["regex"] == HEADER_REGEX
    assert result["policy_source"] == "ai-discovery"
    assert result["confidence"] == 0.91
    assert result["validation"]["parser_validation"]["success_ratio"] == 37.5
    assert result["ai_calls"] == 0

    schema_after, policy_after = snapshot()
    assert policy_after["hits"] == policy_before["hits"] + 1
    assert policy_after["last_used"] >= policy_before["last_used"]
    stable_fields = set(policy_before) - {"hits", "last_used"}
    assert {key: policy_after[key] for key in stable_fields} == {
        key: policy_before[key] for key in stable_fields
    }
    tables = {row[1] for row in schema_after if row[0] == "table"}
    assert tables == {"segmentation_policies"}, f"Unexpected policy tables: {tables}"
    assert schema_after == schema_before
    assert env.forbidden_calls == []
