"""Bounded source orchestration with real segmentation, parser, templates and downstream."""

import builtins
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import socket
import sqlite3
from types import SimpleNamespace

import pytest


OBSERVED = datetime(2026, 10, 4, 9, tzinfo=timezone.utc)
HEADER = "2026-01-02T03:04:05Z ERROR database connection error"
NEXT = "2026-01-02T03:04:06Z ERROR retry exhausted"
CONT = "    detail: connection refused  "
REGEX = r"^\d{4}-\d{2}-\d{2}T"


@pytest.fixture
def harness(monkeypatch, tmp_path):
    monkeypatch.setenv('AIOPS_TIME_DEBUG', 'false')
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[2] / "src" / "backend"))

    def forbidden(*args, **kwargs):
        pytest.fail("Network, LLM, SQLite, and policy discovery are forbidden")

    for owner, names in (
        (socket.socket, ("connect", "connect_ex")), (socket, ("create_connection", "getaddrinfo")),
        (sqlite3, ("connect",)), (sqlite3.dbapi2, ("connect",)),
    ):
        for name in names:
            monkeypatch.setattr(owner, name, forbidden)
    import requests
    import ai_engine

    monkeypatch.setattr(requests.sessions.Session, "request", forbidden)
    monkeypatch.setattr(ai_engine, "call_ai_agent", forbidden)
    original_import = builtins.__import__

    def source_neutral_import(name, globals=None, locals=None, fromlist=(), level=0):
        candidates = {name} | {name + "." + item for item in fromlist or ()}
        if any(item.startswith("ingestion_layer.opensearch") for item in candidates):
            pytest.fail("Core processing must not import OpenSearch adapters")
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", source_neutral_import)
    import full_pipeline_v2 as full
    import parser_layer.parser_pipeline as parsing
    import parser_layer.canonical_event_builder as builder
    import parser_layer.policy.policy_discovery as parser_discovery
    import segmentation_layer.header_discovery as header_discovery
    import segmentation_layer.segmentation_pipeline as segmentation
    import segmentation_layer.regex_validator as validation
    import rca_layer.rca_engine as rca
    from segmentation_layer.multiline_assembler import MultilineAssembler
    from segmentation_layer.segmentation_session import SegmentationSession

    for module in (parser_discovery, header_discovery, segmentation, rca):
        monkeypatch.setattr(module, "call_ai_agent", forbidden)
    monkeypatch.setattr(segmentation.SegmentationPipeline, "prepare", forbidden)
    monkeypatch.setattr(header_discovery.HeaderDiscovery, "discover", forbidden)
    monkeypatch.setattr(validation.RegexValidator, "validate", forbidden)
    monkeypatch.setattr(parsing, "ParserPolicyRegistry", lambda *a, **k: SimpleNamespace(
        get=forbidden, save=forbidden, invalidate=forbidden,
    ))
    monkeypatch.setattr(parsing, "ParserPolicyDiscovery", lambda: SimpleNamespace(
        signature=forbidden, discover=forbidden, validate=forbidden, enabled=forbidden,
    ))
    monkeypatch.setattr(parsing.ParserPipeline, "prepare", forbidden)

    class FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return OBSERVED

    monkeypatch.setattr(builder, "datetime", FixedDatetime)

    class FileSegmenter:
        def __init__(self):
            self.last_result = {"sentinel": "file-only"}
            self.file_calls = []

        def iter_events(self, path):
            self.file_calls.append(path)
            yield from MultilineAssembler().iter_events(path, REGEX)

    class RecordingParser(parsing.ParserPipeline):
        def __init__(self):
            super().__init__(ai_enabled=False)
            self.calls, self.outcomes = [], []

        def process(self, raw, **kwargs):
            self.calls.append(raw)
            return super().process(raw, **kwargs)

        def process_with_outcome(self, raw, **kwargs):
            outcome = super().process_with_outcome(raw, **kwargs)
            self.outcomes.append(outcome)
            return outcome

    class RecordingTemplater(full.TemplatePipeline):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self.inputs, self.saved = [], 0

        def process(self, event):
            self.inputs.append(deepcopy(event))
            return super().process(event)

        def save_state(self):
            self.saved += 1
            return super().save_state()

    class RecordingDownstream(full.DownstreamAIOpsPipeline):
        def process(self, events):
            self.inputs = list(events)
            return super().process(self.inputs)

    sessions = []

    class RecordingSession(SegmentationSession):
        def __init__(self, provider):
            super().__init__(provider)
            self.pages = []
            self.close_calls = 0
            sessions.append(self)

        def feed_page(self, page):
            self.pages.append(page)
            return super().feed_page(page)

        def close(self):
            self.close_calls += 1
            return super().close()

        def close_stream(self, key):
            pytest.fail("Orchestration must not cut streams at page boundaries")

    monkeypatch.setattr(full, "SegmentationPipeline", FileSegmenter)
    monkeypatch.setattr(full, "ParserPipeline", RecordingParser)
    monkeypatch.setattr(full, "TemplatePipeline", RecordingTemplater)
    monkeypatch.setattr(full, "DownstreamAIOpsPipeline", RecordingDownstream)
    monkeypatch.setattr(full, "SegmentationSession", RecordingSession, raising=False)
    pipelines = []

    def pipeline():
        directory = tmp_path / str(len(pipelines))
        instance = full.FullAIOpsPipelineV2(
            template_state=str(directory / "templates.json"),
            drain_state=str(directory / "drain.bin"), use_ai_rca=False,
        )
        pipelines.append(instance)
        return instance

    return SimpleNamespace(pipeline=pipeline, sessions=sessions, forbidden=forbidden,
                           parser=parsing.ParserPipeline, full=full)


def record(number, raw=HEADER, **identity):
    from ingestion_layer.contracts import Framing, IngestedLogRecord, SourceReference, StreamIdentity

    coordinates = dict(source_scope="cluster", namespace="ns", workload="app", pod="pod",
                       pod_instance="pod-uid", container="container", container_instance="run", channel="stdout")
    return IngestedLogRecord(
        raw, SourceReference("profile", "partition-1", str(number), "generation-1", 2**80 + number),
        source_timestamp_raw="2026-01-02T03:04:05.123456789Z", source_timestamp=OBSERVED,
        stream_identity=StreamIdentity(**(coordinates | identity)),
        retrieval_order=(100 - number, 2**80 + number), first_observed_at=OBSERVED,
        framing=Framing.PHYSICAL_LINE,
        metadata=(("password", "secret-metadata"), ("raw_message", raw), ("cursor", "secret-cursor")),
    )


def page(*records):
    from ingestion_layer.contracts import SourcePage

    return SourcePage(tuple(records), True, next_cursor="secret-cursor", cycle_budget_reached=True)


def provider(key, first_record):
    from segmentation_layer.contracts import SegmentationPolicy

    return SegmentationPolicy("test-policy", REGEX, "offline-validation")


def provenance(event):
    return event["attributes"]["source_provenance"]


def ids(evidence):
    return [item["source_reference"]["record_id"] for item in evidence["contributors"]]


def test_one_page_runs_the_real_chain_and_explicit_close_once(harness):
    pipeline = harness.pipeline()
    input_page = page(record(1), record(2, CONT), record(3, NEXT))
    result = pipeline.process_ingested_pages([input_page], policy_provider=provider)
    assert pipeline.parser.calls == [HEADER + "\n" + CONT, NEXT]
    assert result["stats"]["segmented"] == result["stats"]["parsed"] == result["stats"]["templated"] == 2
    assert sum(signal["count"] for signal in result["signals"]) == 2
    assert len(pipeline.templater.inputs) == len(pipeline.downstream.inputs) == 2
    assert result["incidents"] and result["rca"] and result["plans"]
    assert pipeline.templater.saved == 1
    assert Path(pipeline.templater.state_path).exists()
    assert Path(pipeline.templater.candidate_state_path).exists()
    assert [provenance(event)["emission_reason"] for event in pipeline.downstream.inputs] == ["next_header", "analysis_end"]
    assert len(harness.sessions) == 1
    session = harness.sessions[0]
    assert session.pages == [input_page]
    assert session.closed and session.active_stream_count == session.pending_record_count == 0
    assert pipeline.segmenter.file_calls == []
    assert pipeline.segmenter.last_result == {"sentinel": "file-only"}


def test_cross_page_state_remains_pending_until_same_stream_header(harness):
    pipeline = harness.pipeline()
    first, second = page(record(1), record(2, CONT)), page(record(3, "\tmore"), record(4, NEXT))

    def supplied_pages():
        yield first
        assert pipeline.parser.calls == []
        assert harness.sessions[0].pending_record_count == 2
        assert not harness.sessions[0].closed
        yield page()  # Neither empty/exhausted pages nor cursor flags end analysis.
        assert pipeline.parser.calls == []
        yield second
        assert pipeline.parser.calls == [HEADER + "\n" + CONT + "\n\tmore"]

    result = pipeline.process_ingested_pages(supplied_pages(), policy_provider=provider)
    assert pipeline.parser.calls == [HEADER + "\n" + CONT + "\n\tmore", NEXT]
    assert result["ingestion_diagnostics"]["pages_read"] == 3
    assert result["ingestion_diagnostics"]["records_read"] == 4
    assert ids(provenance(pipeline.downstream.inputs[0])) == ["1", "2", "3"]


@pytest.mark.parametrize("coordinate", ["source_scope", "pod_instance", "container_instance", "channel"])
def test_interleaved_incarnations_and_channels_remain_isolated(harness, coordinate):
    pipeline = harness.pipeline()
    other = {coordinate: "stderr" if coordinate == "channel" else "other"}
    inputs = [page(record(1), record(2, NEXT, **other)),
              page(record(3, "  other tail", **other), record(4, CONT))]
    result = pipeline.process_ingested_pages(inputs, policy_provider=provider)
    assert pipeline.parser.calls == [HEADER + "\n" + CONT, NEXT + "\n  other tail"]
    assert [ids(provenance(event)) for event in pipeline.templater.inputs] == [["1", "4"], ["2", "3"]]
    assert [provenance(event)["stream_key"][coordinate] for event in pipeline.downstream.inputs] == [
        getattr(inputs[0].records[0].stream_identity, coordinate), other[coordinate],
    ]
    assert len(result["event_provenance"]) == 2


@pytest.mark.parametrize("reason", ["missing_stream_identity", "incomplete_stream_identity", "no_policy", "unsupported_framing", "blank_context"])
def test_dispositions_are_diagnostics_not_parser_inputs(harness, reason):
    from ingestion_layer.contracts import Framing

    pipeline = harness.pipeline()
    rejected = record(1)
    if reason == "missing_stream_identity":
        rejected = replace(rejected, stream_identity=None)
    elif reason == "incomplete_stream_identity":
        rejected = record(1, container_instance=None)
    elif reason == "unsupported_framing":
        rejected = replace(rejected, framing=Framing.LOGICAL_EVENT)
    elif reason == "blank_context":
        rejected = record(1, " \t\r\n")

    def binding(key, first):
        return None if reason == "no_policy" and first is rejected else provider(key, first)

    result = pipeline.process_ingested_pages([page(rejected, record(2, NEXT))], policy_provider=binding)
    assert pipeline.parser.calls == [NEXT]
    diagnostics = result["ingestion_diagnostics"]
    assert diagnostics["records_read"] == 2
    assert diagnostics["unassembled_count"] == 1
    assert diagnostics["unassembled_by_reason"] == {reason: 1}
    assert diagnostics["unassembled_samples"][0]["reason"] == reason
    assert diagnostics["unassembled_samples"][0]["record"]["source_reference"]["record_id"] == "1"
    serialized = json.dumps(diagnostics, default=str)
    assert HEADER not in serialized and "secret-metadata" not in serialized and "secret-cursor" not in serialized


def test_ordered_lossless_provenance_includes_omitted_blanks_but_not_raw_or_metadata(harness):
    pipeline = harness.pipeline()
    original = (record(9), record(3, " \t"), record(1, CONT))
    result = pipeline.process_ingested_pages([page(*original)], policy_provider=provider)
    event = pipeline.downstream.inputs[0]
    evidence = provenance(event)
    assert ids(evidence) == ["9", "3", "1"]
    assert [item["included"] for item in evidence["contributors"]] == [True, False, True]
    assert [item["ordinal"] for item in evidence["contributors"]] == [1, 2, 3]
    for source, diagnostic in zip(original, evidence["contributors"]):
        assert diagnostic["source_reference"] == {
            "source_scope": "profile", "source_partition": "partition-1", "record_id": source.source_reference.record_id,
            "generation": "generation-1", "version": source.source_reference.version,
        }
        assert diagnostic["retrieval_order"] == source.retrieval_order
        assert diagnostic["source_timestamp_raw"] == source.source_timestamp_raw
        assert diagnostic["source_timestamp"] == OBSERVED.isoformat()
        assert diagnostic["first_observed_at"] == OBSERVED.isoformat()
    assert event["raw"] == HEADER + "\n" + CONT
    assert result["event_provenance"] == [{"event_id": event["event_id"], "provenance": evidence}]
    serialized = json.dumps(evidence)
    for excluded in (HEADER, CONT, "secret-cursor", "secret-metadata", "password", "raw_message"):
        assert excluded not in serialized
    assert "source_provenance" not in pipeline.parser.outcomes[0].event["attributes"]


def test_provenance_attribute_collision_preserves_source_values(harness):
    pipeline = harness.pipeline()
    raw = json.dumps({"timestamp": "2026-01-02T03:04:05Z", "level": "ERROR", "message": "database connection error",
                      "source_provenance": {"user": "value"}, "source_source_provenance": "existing"})
    pipeline.process_ingested_pages([page(record(1, raw))], policy_provider=provider)
    attributes = pipeline.downstream.inputs[0]["attributes"]
    assert ids(attributes["source_provenance"]) == ["1"]
    assert attributes["source_source_provenance"] == "existing"
    assert attributes["source_source_source_provenance"] == {"user": "value"}


def test_provenance_does_not_change_event_ids_template_ids_or_parser_outcomes(harness):
    from parser_layer.contracts import Delivery, Recognition
    from parser_layer.timestamp.source_policy import TimestampContext

    raw = "2026-01-02T03:04:05Z INFO operation started\n    ERROR continuation detail"
    direct = harness.parser(ai_enabled=False).process_with_outcome(raw, timestamp_context=TimestampContext(
        source_record_time=OBSERVED, source_record_raw=record(1).source_timestamp_raw))
    pipeline = harness.pipeline()
    pipeline.process_ingested_pages([page(record(1, raw))], policy_provider=provider)
    parsed = pipeline.templater.inputs[0]
    without_provenance = deepcopy(parsed)
    del without_provenance["attributes"]["source_provenance"]
    assert without_provenance == direct.event
    assert parsed["event_id"] == "19a9130bc0afe"
    assert pipeline.parser.outcomes[0] == direct
    assert direct.recognition is Recognition.BUILTIN_RECOGNIZED and direct.delivery is Delivery.PARSED
    control = harness.pipeline()
    expected_template = control.templater.process(direct.event)
    assert pipeline.downstream.inputs[0]["template_id"] == expected_template.template_id
    assert pipeline.downstream.inputs[0]["template"] == expected_template.template


@pytest.mark.parametrize("mode", ["plain", "custom", "custom_rejection", "parser_error", "empty_fields"])
def test_existing_parser_fallback_and_custom_outcomes_are_unchanged(harness, monkeypatch, mode):
    from parser_layer.contracts import Delivery, Recognition

    pipeline = harness.pipeline()
    raw = "opaque message\n    continuation"
    if mode in ("custom", "custom_rejection"):
        fields = {"message": "custom extracted body", "attributes": {}, "severity": "DEBUG"}
        pipeline.parser.custom_parser = SimpleNamespace(parse=lambda text: fields if mode == "custom" else None)
    elif mode == "parser_error":
        def broken(text):
            raise ValueError("deliberate parser failure")
        monkeypatch.setattr(pipeline.parser.detector, "detect", broken)
    elif mode == "empty_fields":
        monkeypatch.setattr(pipeline.parser.parsers["plain_text"], "parse", lambda text: None)
    pipeline.process_ingested_pages([page(record(1, raw))], policy_provider=provider)
    outcome = pipeline.parser.outcomes[0]
    assert outcome.event["raw"] == raw
    assert pipeline.parser.calls == [raw]
    assert outcome.delivery is (Delivery.PARSED if mode in ("plain", "custom") else Delivery.FALLBACK)
    assert outcome.recognition is (Recognition.PLAIN_TEXT if mode == "plain" else Recognition.PARSER_ERROR if mode == "parser_error" else None)
    assert pipeline.templater.inputs[0]["event_id"] == outcome.event["event_id"]


def test_file_path_preserves_result_contract_with_explicit_analysis_time(harness, tmp_path):
    pipeline = harness.pipeline()
    log_path = tmp_path / "legacy.log"
    log_path.write_text("2026-01-02T03:04:05Z INFO operation started\n    ERROR continuation detail\n", encoding="utf-8")
    result = pipeline.process_file(str(log_path))
    assert set(result) == {"case_analysis", "case_analysis_error", "signals", "qualified_signals", "correlations",
                           "incidents", "rca", "plans", "stats", "pipeline_trace", "analysis_time"}
    assert result['analysis_time'] == {'analysis_reference_time_ms': 1767323045000, 'timestamp_basis': 'source'}
    assert result["stats"]["segmented"] == result["stats"]["parsed"] == result["stats"]["templated"] == 1
    assert result["pipeline_trace"]["sample_limit"] == 200
    assert result["pipeline_trace"]["parser"]["items"][0]["event_id"] == "19a9130bc0afe"
    assert result["pipeline_trace"]["parser"]["items"][0]["attributes"] == {}
    assert pipeline.segmenter.file_calls == [str(log_path)] and harness.sessions == []
    assert pipeline.templater.saved == 1


def test_source_and_file_results_match_except_additive_provenance(harness, tmp_path):
    source, file_pipeline = harness.pipeline(), harness.pipeline()
    inputs = [record(1), record(2, CONT), record(3, NEXT)]
    path = tmp_path / "same.log"
    path.write_text("\n".join(item.raw_text for item in inputs) + "\n", encoding="utf-8")
    expected = file_pipeline.process_file(str(path))
    actual = source.process_ingested_pages([page(*inputs)], policy_provider=provider)
    del actual["event_provenance"], actual["ingestion_diagnostics"]
    for stage in ("parser", "template"):
        for event in actual["pipeline_trace"][stage]["items"]:
            event["attributes"].pop("source_provenance", None)
            time_provenance = dict(event['timestamp_provenance'])
            event['timestamp_provenance'] = time_provenance
            assert time_provenance['basis'] == 'message_explicit'
            assert time_provenance['source_record_time'] == OBSERVED.isoformat()
            assert time_provenance['source_record_timestamp_raw'] == inputs[0].source_timestamp_raw
            time_provenance['source_record_time'] = None
            time_provenance['source_record_timestamp_raw'] = None
    assert actual == expected


def test_structured_alarms_still_bypass_text_and_template_learning(harness, monkeypatch):
    pipeline = harness.pipeline()
    monkeypatch.setattr(pipeline.segmenter, "iter_events", harness.forbidden)
    monkeypatch.setattr(pipeline.parser, "process", harness.forbidden)
    monkeypatch.setattr(pipeline.templater, "process", harness.forbidden)
    alarm = dict(alarm_id="alarm-1", timestamp="2026-01-02T03:04:05Z", severity="ERROR",
                 service="orders-db", host="db-01", alarm_type="disk_full", message="data volume full")
    result = pipeline.process_structured_alarms([alarm])
    event = pipeline.downstream.inputs[0]
    assert event["event_id"] == "alarm-1"
    assert event["template_id"] == "alarm:" + hashlib.sha1(b"orders-db|disk_full").hexdigest()[:12]
    assert event["template_source"] == "structured-alarm-schema"
    assert result["stats"]["structured_alarm_fast_path"] is True
    assert "event_provenance" not in result and "source_provenance" not in event["attributes"]
    assert harness.sessions == [] and pipeline.templater.saved == 0


def test_topology_is_explicit_and_isolated_across_reused_pipeline(harness):
    from input_package_layer.topology import TopologyContext

    pipeline = harness.pipeline()
    inventory = {"host": "db-01", "servis": "orders-db", "veri_merkezi": "analysis-only"}
    topology = TopologyContext([inventory], [{"kaynak_servis": "orders-api", "hedef_servis": "orders-db"}])
    raw = '{"timestamp":"2026-01-02T03:04:05Z","level":"ERROR","host":"db-01","service":"orders-db","message":"database connection error"}'
    pipeline.downstream.set_context(TopologyContext([{"host": "db-01", "veri_merkezi": "old"}]))
    with_context = pipeline.process_ingested_pages([page(record(1, raw))], policy_provider=provider, topology=topology)
    assert with_context["incidents"][0]["context"]["host_inventory"] == [inventory]
    without = pipeline.process_ingested_pages([page(record(2, raw))], policy_provider=provider)
    assert without["incidents"][0]["context"]["host_inventory"] == []
    assert without["incidents"][0]["context"]["dependencies"] == []
    for component in (pipeline.downstream, pipeline.downstream.correlator, pipeline.downstream.enricher, pipeline.downstream.incidents):
        assert component.topology is None
    assert with_context["incidents"][0]["context"]["host_inventory"] == [inventory]


def test_sessions_are_analysis_local_while_template_learning_persists(harness):
    pipeline = harness.pipeline()
    first = pipeline.process_ingested_pages([page(record(1))], policy_provider=provider)
    learned = dict(pipeline.templater.registry._templates)
    second = pipeline.process_ingested_pages([page(record(2, CONT))], policy_provider=provider)
    assert pipeline.parser.calls == [HEADER, CONT]
    assert ids(second["event_provenance"][0]["provenance"]) == ["2"]
    assert ids(first["event_provenance"][0]["provenance"]) == ["1"]
    assert len(harness.sessions) == 2 and harness.sessions[0] is not harness.sessions[1]
    assert all(session.closed and session.active_stream_count == session.pending_record_count == 0 for session in harness.sessions)
    assert all(value not in harness.sessions for value in vars(pipeline).values())
    assert learned.items() <= pipeline.templater.registry._templates.items()
    assert pipeline.templater.saved == 2


def test_no_sorting_or_deduplication_and_completion_order_precedes_tail_order(harness):
    pipeline = harness.pipeline()
    repeated = record(3, NEXT, channel="stderr")
    inputs = [record(8), record(7, CONT), record(4, HEADER, channel="stderr"), repeated, repeated]
    result = pipeline.process_ingested_pages([page(*inputs)], policy_provider=provider)
    assert pipeline.parser.calls == [HEADER, NEXT, HEADER + "\n" + CONT, NEXT]
    assert [ids(entry["provenance"]) for entry in result["event_provenance"]] == [["4"], ["3"], ["8", "7"], ["3"]]
    assert [entry["provenance"]["emission_reason"] for entry in result["event_provenance"]] == [
        "next_header", "next_header", "analysis_end", "analysis_end",
    ]


def test_diagnostic_samples_are_capped_without_losing_event_provenance(harness):
    pipeline = harness.pipeline()
    inputs = [replace(record(i), stream_identity=None) for i in range(250)] + [record(i) for i in range(250, 475)]
    result = pipeline.process_ingested_pages((page(item) for item in inputs), policy_provider=provider)
    diagnostics = result["ingestion_diagnostics"]
    assert diagnostics["pages_read"] == diagnostics["records_read"] == 475
    assert diagnostics["unassembled_count"] == 250
    assert diagnostics["unassembled_by_reason"] == {"missing_stream_identity": 250}
    assert diagnostics["sample_limit"] == len(diagnostics["unassembled_samples"]) == 200
    assert len(result["pipeline_trace"]["parser"]["items"]) == 200
    assert len(result["event_provenance"]) == 225
    assert ids(result["event_provenance"][-1]["provenance"]) == ["474"]


@pytest.mark.parametrize("inputs", [[], [()]])
def test_empty_analysis_returns_existing_result_shape_and_no_events(harness, inputs):
    pipeline = harness.pipeline()
    result = pipeline.process_ingested_pages([page(*items) for items in inputs], policy_provider=provider)
    assert pipeline.parser.calls == [] and result["event_provenance"] == []
    assert result["ingestion_diagnostics"]["pages_read"] == len(inputs)
    assert result["ingestion_diagnostics"]["records_read"] == 0
    assert result["stats"]["segmented"] == result["stats"]["templated"] == 0
    assert harness.sessions[0].closed


@pytest.mark.parametrize("failure", ["input", "parser", "template", "downstream"])
def test_failures_release_session_state_and_propagate_without_retry(harness, monkeypatch, failure):
    pipeline = harness.pipeline()

    def broken(*args, **kwargs):
        raise RuntimeError("deliberate analysis failure")

    if failure != "input":
        component = {"parser": pipeline.parser, "template": pipeline.templater, "downstream": pipeline.downstream}[failure]
        monkeypatch.setattr(component, "process", broken)

    def inputs():
        yield page(record(1), record(2, NEXT))
        if failure == "input":
            broken()

    with pytest.raises(RuntimeError, match="deliberate analysis failure"):
        pipeline.process_ingested_pages(inputs(), policy_provider=provider)
    session = harness.sessions[0]
    assert session.closed and session.active_stream_count == session.pending_record_count == 0
    assert pipeline.templater.saved == 0


@pytest.mark.parametrize("fails", [False, True])
def test_source_context_is_released_before_a_following_structured_analysis(harness, fails):
    from input_package_layer.topology import TopologyContext

    pipeline = harness.pipeline()
    topology = TopologyContext([{"host": "db-01", "veri_merkezi": "source-only"}])

    def inputs():
        yield page(record(1))
        if fails:
            raise RuntimeError("deliberate input failure")

    if fails:
        with pytest.raises(RuntimeError, match="deliberate input failure"):
            pipeline.process_ingested_pages(inputs(), policy_provider=provider, topology=topology)
    else:
        pipeline.process_ingested_pages(inputs(), policy_provider=provider, topology=topology)
    result = pipeline.process_structured_alarms([dict(
        alarm_id="separate-analysis", timestamp="2026-01-02T03:04:05Z", severity="ERROR",
        service="orders-db", host="db-01", alarm_type="disk_full", message="data volume full",
    )])
    assert result["incidents"][0]["context"]["host_inventory"] == []
    assert pipeline.downstream.topology is None


@pytest.mark.parametrize("stage", ["parser", "template"])
def test_existing_none_results_are_skipped_but_provenance_remains(harness, monkeypatch, stage):
    pipeline = harness.pipeline()
    target = pipeline.parser if stage == "parser" else pipeline.templater
    calls = []

    def no_result(value, **kwargs):
        calls.append(value)
        return None

    monkeypatch.setattr(target, "process", no_result)
    result = pipeline.process_ingested_pages([page(record(1))], policy_provider=provider)
    assert len(calls) == 1
    assert result["stats"]["segmented"] == 1 and result["stats"]["templated"] == 0
    assert result["stats"]["parsed"] == (0 if stage == "parser" else 1)
    assert pipeline.downstream.inputs == []
    assert ids(result["event_provenance"][0]["provenance"]) == ["1"]
    assert (result["event_provenance"][0]["event_id"] is None) == (stage == "parser")


def test_caller_policy_controls_boundaries_without_bootstrap(harness):
    from segmentation_layer.contracts import SegmentationPolicy, StreamKey

    pipeline = harness.pipeline()
    policy = SegmentationPolicy("custom", r"^BEGIN ", "caller-prevalidated")
    calls = []

    def binding(key, first):
        calls.append((key, first))
        return policy

    first = record(1, "BEGIN first")
    pipeline.process_ingested_pages([page(first, record(2, "body detail")), page(record(3, "BEGIN second"))],
                                    policy_provider=binding)
    assert pipeline.parser.calls == ["BEGIN first\nbody detail", "BEGIN second"]
    assert calls == [(StreamKey.from_identity(first.stream_identity), first)]


def test_core_layers_have_no_opensearch_imports(harness):
    import ast

    backend = Path(__file__).resolve().parents[2] / "src" / "backend"
    paths = [backend / "full_pipeline_v2.py", backend / "downstream_pipeline.py"]
    for directory in ("segmentation_layer", "parser_layer", "template_layer"):
        paths.extend((backend / directory).rglob("*.py"))
    for path in paths:
        tree = ast.parse(path.read_text(encoding="utf-8-sig"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                assert all("opensearch" not in alias.name.lower() for alias in node.names), path
            elif isinstance(node, ast.ImportFrom):
                assert "opensearch" not in (node.module or "").lower(), path
                assert all("opensearch" not in alias.name.lower() for alias in node.names), path
