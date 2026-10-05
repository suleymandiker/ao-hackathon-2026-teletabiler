"""Repeat-analysis probes with fixed templates, no external or production state."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import socket
import sqlite3

import pytest


@pytest.fixture(autouse=True)
def isolated(monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[2] / 'src' / 'backend'))

    def forbidden(*args, **kwargs):
        pytest.fail('No network, LLM or production SQLite in determinism tests')

    import requests
    import ai_engine
    import rca_layer.rca_engine as rca
    for owner, names in (
        (socket.socket, ('connect', 'connect_ex')),
        (socket, ('create_connection', 'getaddrinfo')),
        (requests.sessions.Session, ('request', 'send')),
        (sqlite3, ('connect',)), (sqlite3.dbapi2, ('connect',)),
    ):
        for name in names:
            monkeypatch.setattr(owner, name, forbidden)
    monkeypatch.setattr(ai_engine, 'call_ai_agent', forbidden)
    monkeypatch.setattr(rca, 'call_ai_agent', forbidden)


def events():
    return [dict(event_id=f'event-{kind}-{index}', timestamp=None,
                 template_id=f'template-{kind}', template=f'{kind} failure',
                 template_reliable=True, severity_text='WARNING',
                 service_name='test-service', component=kind)
            for index in range(2) for kind in ('database', 'connection')]


def decisions(result):
    """Compare production order directly, including identities and decisions."""
    return json.dumps({
        'signals': [(s['signal_id'], s['count'], s['qualified'], s['qualification_score'])
                    for s in result['signals']],
        'qualified': [s['signal_id'] for s in result['qualified_signals']],
        'correlations': result['correlations'],
        'incidents': result['incidents'],
        'rca': result['rca'],
        'stats': result['stats'],
    }, ensure_ascii=False, separators=(',', ':'))


@pytest.mark.parametrize('seconds', [(10, 59, 130), (3598, 3601, 7200), (86398, 86401, 172800)])
def test_same_historical_input_is_stable_across_three_observation_clocks(seconds):
    from downstream_pipeline import DownstreamAIOpsPipeline
    pipeline = DownstreamAIOpsPipeline(use_ai_rca=False)
    data = events()
    before = deepcopy(data)
    outputs = []
    for second in seconds:
        observed = datetime(2026, 10, 5, tzinfo=timezone.utc) + timedelta(seconds=second)
        rows = [dict(row, observed_timestamp=observed + timedelta(seconds=index))
                for index, row in enumerate(data)]
        outputs.append(pipeline.process(rows))
    assert data == before
    assert decisions(outputs[0]) == decisions(outputs[1]) == decisions(outputs[2])


def test_source_timed_input_repeats_without_downstream_state_leakage():
    from downstream_pipeline import DownstreamAIOpsPipeline
    pipeline = DownstreamAIOpsPipeline(use_ai_rca=False)
    rows = [dict(row, timestamp='2026-10-05T00:00:10Z') for row in events()]
    original = deepcopy(rows)
    outputs = []
    for _ in range(3):
        outputs.append(pipeline.process(rows))
        pipeline.process([dict(row, template_id='unrelated', service_name='other') for row in rows])
    assert decisions(outputs[0]) == decisions(outputs[1]) == decisions(outputs[2])
    assert rows == original
    assert outputs[0]['stats']['signal_candidates'] == 2
    assert outputs[0]['stats']['qualified_signals'] == 2
    assert outputs[0]['stats']['correlations'] == 1
    assert outputs[0]['stats']['incidents'] == 1


def test_same_file_repeats_through_canonical_builder_and_persistent_templates(monkeypatch, tmp_path):
    from types import SimpleNamespace
    import parser_layer.canonical_event_builder as canonical
    from parser_layer.parsers.plain_text_parser import PlainTextParser
    from full_pipeline_v2 import FullAIOpsPipelineV2
    from downstream_pipeline import DownstreamAIOpsPipeline
    from template_layer.template_pipeline import TemplatePipeline

    # Deliberately source-local clock: the parser must not invent a timezone.
    raw = '[2026-10-05 00:00:10] WARNING database failure'
    source = tmp_path / 'historical.log'
    source.write_text((raw + '\n') * 2, encoding='utf-8')
    parser = PlainTextParser()
    builder = canonical.CanonicalEventBuilder()
    pipeline = FullAIOpsPipelineV2.__new__(FullAIOpsPipelineV2)
    pipeline.segmenter = SimpleNamespace(iter_events=lambda path: Path(path).read_text(encoding='utf-8').splitlines())
    pipeline.parser = SimpleNamespace(process=lambda event: builder.build(parser.parse(event), event))
    pipeline.templater = TemplatePipeline(state_path=tmp_path / 'templates.json',
                                          candidate_state_path=tmp_path / 'drain.bin')
    pipeline.downstream = DownstreamAIOpsPipeline(use_ai_rca=False)
    outputs = []
    for second in (10, 59, 130):
        reference = datetime(2026, 10, 5, tzinfo=timezone.utc) + timedelta(seconds=second)
        ticks = iter(reference + timedelta(seconds=offset) for offset in range(2))
        monkeypatch.setattr(canonical, 'datetime', SimpleNamespace(now=lambda tz: next(ticks)))
        outputs.append(pipeline.process_file(str(source)))
    assert all(output['stats']['segmented'] == output['stats']['parsed'] == output['stats']['templated'] == 2
               for output in outputs)
    assert all(event['timestamp'] is None for output in outputs for event in output['pipeline_trace']['parser']['items'])
    assert pipeline.templater.template_count == 1
    assert pipeline.templater.last_decision['source'] == 'validated-registry'
    assert decisions(outputs[0]) == decisions(outputs[1]) == decisions(outputs[2])


def test_equal_source_times_have_explicit_stable_ties():
    from downstream_pipeline import DownstreamAIOpsPipeline
    pipeline = DownstreamAIOpsPipeline(use_ai_rca=False)
    rows = [dict(row, timestamp='2026-10-05T00:00:10Z') for row in events()]
    outputs = [pipeline.process(order) for order in (rows, list(reversed(rows)), rows[1:] + rows[:1])]
    assert decisions(outputs[0]) == decisions(outputs[1]) == decisions(outputs[2])


def test_real_template_learning_reuses_authoritative_mapping(tmp_path):
    from template_layer.template_pipeline import TemplatePipeline
    state = tmp_path / 'templates.json'
    drain = tmp_path / 'drain.bin'
    pipeline = TemplatePipeline(state_path=state, candidate_state_path=drain)
    event = {'message': 'database connection failure request_id=123456'}
    first = pipeline.process(event)
    assert pipeline.last_decision['promoted']
    assert pipeline.save_state()
    second = pipeline.process(event)
    assert pipeline.last_decision['source'] == 'validated-registry'
    reloaded = TemplatePipeline(state_path=state, candidate_state_path=drain)
    third = reloaded.process(event)
    assert reloaded.last_decision['source'] == 'validated-registry'
    assert first == second == third
    assert pipeline.template_count == reloaded.template_count == 1


@pytest.mark.parametrize('timed', [0, 2, 4])
def test_mixed_time_inputs_keep_local_reference_without_fabricated_chronology(timed):
    from downstream_pipeline import DownstreamAIOpsPipeline
    pipeline = DownstreamAIOpsPipeline(use_ai_rca=False)
    rows = [dict(row, severity_text='ERROR', timestamp=100 + index if index < timed else None)
            for index, row in enumerate(events())]
    results = []
    for day in (5, 6, 7):
        data = [dict(row, observed_timestamp=datetime(2026, 10, day, tzinfo=timezone.utc)) for row in rows]
        original = deepcopy(data)
        results.append(pipeline.process(iter(data)))
        assert data == original
        pipeline.process([dict(rows[0], timestamp=999999)])
    assert results[0] == results[1] == results[2]
    assert results[0]['analysis_time'] == {
        'analysis_reference_time_ms': (100 + timed - 1) * 1000 if timed else None,
        'timestamp_basis': 'source' if timed else 'unresolved',
    }
    untimed = [s for s in results[0]['signals'] if not s['timestamp_resolved']]
    for signal in untimed:
        assert signal['signal_id'].endswith(':untimed')
        assert all(signal[field] is None for field in ('first_seen_ms', 'last_seen_ms', 'window_start_ms', 'window_end_ms'))
        assert signal['qualified']  # Existing severity/error/count weights still apply.
    untimed_ids = {s['signal_id'] for s in untimed}
    assert all(edge['source'] not in untimed_ids and edge['target'] not in untimed_ids
               for edge in results[0]['correlations'])
    for incident in results[0]['incidents']:
        if untimed_ids.intersection(incident['signal_ids']):
            assert incident['start_ms'] is incident['end_ms'] is None


@pytest.mark.parametrize('value,expected', [(None, None), ('', None), ('invalid', None),
    ('2026-10-05T12:00:00', None), (False, None), (float('nan'), None), (float('inf'), None),
    (0, 0), ('1970-01-01T00:00:00Z', 0), ('1970-01-01T03:00:00+03:00', 0)])
def test_timestamp_conversion_never_uses_observation_or_machine_clock(value, expected):
    from downstream_pipeline import DownstreamAIOpsPipeline
    result = DownstreamAIOpsPipeline().process([dict(events()[0], timestamp=value,
        observed_timestamp=datetime(2026, 10, 5, tzinfo=timezone.utc))])
    assert result['signals'][0]['first_seen_ms'] == expected
    assert result['signals'][0]['timestamp_resolved'] is (expected is not None)
    assert result['analysis_time']['analysis_reference_time_ms'] == expected


@pytest.mark.parametrize('root_time,dependent_time,precedence', [(1000, 1000, False), (1000, 2000, True), (2000, 1000, False)])
def test_topology_ties_preserve_scores_but_never_invent_precedence(root_time, dependent_time, precedence):
    from correlation_layer.correlator import SignalCorrelator
    from input_package_layer.topology import TopologyContext
    topology = TopologyContext(dependencies=[{'kaynak_servis': 'api', 'hedef_servis': 'db'}])
    signals = [dict(signal_id='db-root', service_name='db', alarm_type='disk_full',
                    first_seen_ms=root_time, last_seen_ms=root_time, timestamp_resolved=True),
               dict(signal_id='api-symptom', service_name='api', alarm_type='timeout',
                    first_seen_ms=dependent_time, last_seen_ms=dependent_time, timestamp_resolved=True)]
    results = [SignalCorrelator(topology=topology).correlate(order) for order in (signals, list(reversed(signals)))]
    assert results[0] == results[1]
    edge = results[0][0]
    assert (edge['source'], edge['target'], edge['score']) == ('db-root', 'api-symptom', .82)
    assert edge['time_gap_ms'] == abs(root_time - dependent_time)
    assert ('nedensel_zaman_sırası' in edge['evidence']) is precedence


def test_equal_time_uses_explicit_source_ordinal_before_identifier():
    from downstream_pipeline import DownstreamAIOpsPipeline
    rows = [dict(row, timestamp=100, source_order=2 if row['component'] == 'connection' else 1)
            for row in events()]
    results = [DownstreamAIOpsPipeline().process(order) for order in (rows, list(reversed(rows)))]
    assert decisions(results[0]) == decisions(results[1])
    assert results[0]['correlations'][0]['source'] == 'sig:template-database:test-service:60000'


def test_exact_rca_ties_have_stable_identity_order():
    from rca_layer.rca_engine import DeterministicRCAEngine
    rows = [dict(signal_id=sid, severity_min=3, first_seen_ms=100, qualification_score=.8)
            for sid in ('z', 'b', 'a', 'x', 'c', 'd')]
    incident = {'incident_id': 'incident', 'signal_ids': [row['signal_id'] for row in rows]}
    result = DeterministicRCAEngine().analyze([incident], [], rows)
    assert [row['signal_id'] for row in result[0]['root_cause_candidates']] == ['a', 'b', 'c', 'd', 'x']
    assert all(row['score'] == .65 for row in result[0]['root_cause_candidates'])


def test_topology_path_ties_ignore_adjacency_container_order():
    from input_package_layer.topology import TopologyContext
    topology = TopologyContext()
    paths = []
    for order in (['b', 'a'], ['a', 'b']):
        topology.depends_on = {'api': order, 'a': ['db'], 'b': ['db']}
        paths.append(topology.dependency_path('api', 'db'))
    assert paths == [['api', 'a', 'db']] * 2


def test_page_ingestion_preserves_source_time_and_order_without_observation_fallback(monkeypatch, tmp_path):
    from types import SimpleNamespace
    from full_pipeline_v2 import FullAIOpsPipelineV2
    from ingestion_layer.contracts import Framing, IngestedLogRecord, SourceReference, StreamIdentity, SourcePage
    from segmentation_layer.contracts import SegmentationPolicy
    from parser_layer.canonical_event_builder import CanonicalEventBuilder
    from parser_layer.parsers.json_parser import JsonParser
    from downstream_pipeline import DownstreamAIOpsPipeline
    from template_layer.template_pipeline import TemplatePipeline
    parser, builder = JsonParser(), CanonicalEventBuilder()
    pipeline = FullAIOpsPipelineV2.__new__(FullAIOpsPipelineV2)
    pipeline.parser = SimpleNamespace(process=lambda raw, **kwargs: builder.build(parser.parse(raw), raw, **kwargs))
    pipeline.templater = TemplatePipeline(state_path=tmp_path / 'templates.json', candidate_state_path=tmp_path / 'drain.bin')
    pipeline.downstream = DownstreamAIOpsPipeline()
    records = tuple(IngestedLogRecord(
        raw_text=json.dumps({'@timestamp': stamp, 'message': 'database failure', 'severity': 'ERROR'}),
        source_reference=SourceReference('search', 'index', str(index)),
        source_timestamp=datetime(2026, 10, 5, tzinfo=timezone.utc),
        first_observed_at=datetime(2026, 10, 6, tzinfo=timezone.utc), retrieval_order=(index,),
        stream_identity=StreamIdentity('search', pod_instance='pod', container_instance='container', channel='stdout'),
        framing=Framing.PHYSICAL_LINE,
    ) for index, stamp in enumerate(('2026-10-05T00:00:00Z', None)))
    result = pipeline.process_ingested_pages([SourcePage(records, True)],
        policy_provider=lambda *args: SegmentationPolicy('test', r'^\{', 'fixture'))
    assert result['analysis_time']['analysis_reference_time_ms'] == 1791158400000
    # The second event now uses the explicitly authorized source-record fallback;
    # the later observation clock must still never enter occurrence time.
    assert [s['timestamp_resolved'] for s in result['signals']] == [True]
    assert result['signals'][0]['count'] == 2
    assert result['signals'][0]['first_seen_ms'] == 1791158400000
    assert result['signals'][0]['timestamp_basis_counts'] == {'message_explicit': 1, 'source_record': 1}
    assert result['event_provenance'][0]['provenance']['contributors'][0]['retrieval_order'] == (0,)
