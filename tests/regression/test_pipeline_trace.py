"""Actual execution observations, transactional publication and offline read-only UI."""
from copy import deepcopy
from datetime import timedelta
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from test_deployment_monitoring import (BASE, offline, repo, pipeline, definition, record, page, executor_for)


@pytest.fixture
def traced_run(repo, pipeline, monkeypatch):
    from downstream_pipeline import DownstreamAIOpsPipeline
    import rca_layer.rca_engine as rca
    monkeypatch.setattr(rca, 'call_ai_agent', lambda *a, **k: pytest.fail('No live expert'))
    pipeline.downstream = DownstreamAIOpsPipeline(use_ai_rca=False)
    monitor = repo.create(definition(), enabled=True, now=BASE)
    run = repo.claim(BASE + timedelta(minutes=17))
    records = [record('a', 1, 'ERROR: normal operation'), record('b', 2, '    traceback detail  '),
               record('c', 3, 'ERROR: next operation')]
    executor, queries = executor_for(pipeline, records)
    result = executor.execute(run, set())
    return SimpleNamespace(run=run, monitor=monitor, result=result, records=records,
                           trace=result.presentation['detailed_pipeline_trace'], executor=executor, queries=queries)


def test_completed_trace_is_in_same_atomic_result_and_watermark(repo, traced_run):
    t = traced_run
    repo.succeed(t.run, t.result.presentation, t.result.counts, t.result.receipts, now=BASE + timedelta(minutes=17))
    saved = repo.result(t.run.id)['detailed_pipeline_trace']
    assert saved == json.loads(json.dumps(t.trace))
    assert saved['run_id'] == t.run.id and saved['trace_complete']
    assert repo.get(t.monitor.id).last_successful_end == t.run.window.end
    assert repo.history(t.monitor.id)[0].counts == t.result.counts
    assert len(t.queries) == 1
    assert len(json.dumps(saved).encode()) < 25000


def test_trace_rolls_back_with_result_receipts_and_watermark(repo, traced_run):
    t = traced_run
    with sqlite3.connect(repo.path) as db:
        db.execute("CREATE TRIGGER fail_trace_receipt BEFORE INSERT ON monitor_receipts BEGIN SELECT RAISE(ABORT, 'synthetic'); END")
    with pytest.raises(sqlite3.IntegrityError):
        repo.succeed(t.run, t.result.presentation, t.result.counts, t.result.receipts, now=BASE)
    assert repo.result(t.run.id) is None
    assert repo.get(t.monitor.id).last_successful_end is None


@pytest.mark.parametrize('failure', ['acquisition', 'pipeline', 'persistence'])
def test_failed_run_has_no_completed_trace(repo, pipeline, failure):
    from monitoring.worker import MonitorWorker
    from monitoring.errors import MonitoringError
    monitor = repo.create(definition(), enabled=True, now=BASE)
    records = [record('a', 1, 'ERROR: normal')]
    if failure == 'acquisition':
        records = lambda query: (_ for _ in ()).throw(MonitoringError('OPENSEARCH_QUERY'))
    elif failure == 'pipeline':
        pipeline.parser.process_with_outcome = lambda *a, **k: (_ for _ in ()).throw(ValueError('synthetic'))
    else:
        with sqlite3.connect(repo.path) as db:
            db.execute("CREATE TRIGGER fail_trace_result BEFORE INSERT ON monitor_results BEGIN SELECT RAISE(ABORT, 'synthetic'); END")
    executor, _ = executor_for(pipeline, records)
    worker = MonitorWorker(repo, executor, clock=lambda: BASE + timedelta(minutes=17), log=lambda *a, **k: None)
    assert worker.tick() == 0
    run = repo.history(monitor.id)[0]
    assert run.status.value == 'FAILED' and repo.result(run.id) is None
    assert repo.get(monitor.id).last_successful_end is None


def test_acquisition_order_contributors_boundary_and_actual_parser_fields(traced_run, pipeline):
    stages = traced_run.trace['stages']
    acquired = stages['acquisition']
    assert [r['ref'] for r in acquired] == ['record:1', 'record:2', 'record:3']
    assert [r['data']['retrieval_order'] for r in acquired] == [[1, 'a'], [2, 'b'], [3, 'c']]
    first, last = stages['segmentation']
    assert first['parents'] == ('record:1', 'record:2')
    assert first['data']['text'] == 'ERROR: normal operation\n    traceback detail  '
    assert first['data']['physical_record_count'] == 2
    assert first['data']['emission_reason'] == 'next_header'
    assert first['data']['boundary_status'] == 'complete'
    assert last['data']['emission_reason'] == 'analysis_end'
    assert last['data']['boundary_status'] == 'possible_incomplete'
    assert first['data']['policy_id'] == 'id'
    assert [c['source_reference_hash'] for c in first['data']['contributors']] == [r['data']['source_reference_hash'] for r in acquired[:2]]
    parsed = stages['parsing'][0]
    assert parsed['parents'] == ('logical:1',)
    assert parsed['data']['recognition'] == 'PLAIN_TEXT'
    assert parsed['data']['delivery'] == 'PARSED'
    actual = pipeline.rows[0]
    assert parsed['data']['canonical']['message'] == actual['message']
    assert parsed['data']['canonical']['timestamp'] == actual['timestamp'].isoformat()
    assert parsed['data']['canonical']['resource'] == actual['resource']
    assert parsed['data']['canonical']['severity'] == actual['severity']
    assert parsed['data']['canonical']['timestamp_provenance']['basis'] == 'source_record'
    assert stages['patterns'][0]['parents'] == ('canonical:1',)
    assert stages['patterns'][0]['data']['template_id'] == 'fixed-template'
    signal = stages['signal'][0]
    assert signal['parents'] == ('occurrence:1', 'occurrence:2')
    assert signal['data']['qualified'] is False
    assert signal['data'] == traced_run.result.presentation['signals'][0]
    assert signal['data']['qualification_details']['score_threshold'] == .6
    assert signal['data']['qualification_details']['score_threshold_met'] is False


def test_full_membership_beyond_representative_sample(repo, pipeline):
    from downstream_pipeline import DownstreamAIOpsPipeline
    pipeline.downstream = DownstreamAIOpsPipeline(use_ai_rca=False)
    monitor = repo.create(definition(), enabled=True, now=BASE)
    executor, _ = executor_for(pipeline, [record(str(n), n, 'ERROR: same event') for n in range(1, 13)])
    result = executor.execute(repo.claim(BASE + timedelta(minutes=17)), set())
    trace = result.presentation['detailed_pipeline_trace']
    signal = trace['stages']['signal'][0]
    assert len(signal['data']['event_ids']) == 8
    assert len(signal['parents']) == signal['data']['count'] == 12
    assert trace['totals']['patterns'] == 12
    assert {i['data']['template_id'] for i in trace['stages']['patterns']} == {'fixed-template'}


@pytest.mark.parametrize('qualified', [False, True])
def test_capture_does_not_change_any_processing_outputs(repo, pipeline, qualified):
    from downstream_pipeline import DownstreamAIOpsPipeline
    from segmentation_layer.contracts import SegmentationPolicy
    from monitoring.trace import TraceCollector
    from monitoring.execution import source_time
    pipeline.downstream = DownstreamAIOpsPipeline(use_ai_rca=False)
    if qualified:
        pipeline.templater.process = lambda event: SimpleNamespace(
            template_id='pattern:' + event['event_id'], template='connection failure', reliable=True)
    records = [record('a', 1, 'ERROR: one'), record('b', 2, 'ERROR: two')]
    pages = [page(records)]
    options = dict(policy_provider=lambda *a: SegmentationPolicy('id', '^ERROR:', 'offline'))
    pipeline.parser.builder._counter = 0
    before = pipeline.process_ingested_pages(pages, **options)
    pipeline.parser.builder._counter = 0
    trace = TraceCollector('run')
    for r in records:
        trace.acquisition(r, timestamp=source_time(r), inside_window=True, overlap=False, duplicate=False)
    after = pipeline.process_ingested_pages(pages, observer=trace, **options)
    for key in ('stats', 'signals', 'qualified_signals', 'correlations', 'incidents', 'rca', 'plans'):
        assert after[key] == before[key]
    assert [r['template_id'] for r in after['signals']] == [r['template_id'] for r in before['signals']]
    payload = trace.finish(after)
    assert payload['stages']['signal'][0]['data']['qualified'] is qualified
    if qualified:
        assert len(payload['stages']['correlation']) == 1
        edge = payload['stages']['correlation'][0]
        assert edge['parents'] == tuple('signal:' + edge['data'][k] for k in ('source', 'target'))
        incident = payload['stages']['incident'][0]
        assert set(incident['parents']) == {i['ref'] for i in payload['stages']['signal']} | {edge['ref']}
        rca = payload['stages']['rca'][0]
        assert rca['data']['kind'] == 'deterministic' and rca['parents'] == (incident['ref'],)
        assert rca['data']['root_cause_candidates'] == after['rca'][0]['root_cause_candidates']
    else:
        assert all(not payload['stages'][k] for k in ('correlation', 'incident', 'rca'))


@pytest.mark.parametrize('text,secret', [
    ('Authorization: Bearer secret-bearer', 'secret-bearer'),
    ('x-api-key: secret-api', 'secret-api'), ('password=secret-password', 'secret-password'),
    ('user_id=embedded-credential', 'embedded-credential'),
    ('JWT eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.signature', 'eyJhbGci'),
    ('Cookie: session=secret-cookie', 'secret-cookie'),
    ('credentials=secret-credentials', 'secret-credentials'),
    ('unlabelled sk-abcdefghijklmnopqrstuvwxyz', 'sk-abcdefghijklmnopqrstuvwxyz'),
    ('Authorization: Basic dXNlcjpwYXNzd29yZA==', 'dXNlcjpwYXNzd29yZA'),
    ('https://user:secret-url@host.invalid', 'secret-url'),
])
def test_secrets_redacted_before_persistence_and_not_in_collector(text, secret):
    from monitoring.trace import TraceCollector
    collector = TraceCollector('run')
    collector.add('acquisition', 'record:1', {'text': text, 'nested': {'user_id': secret}})
    assert secret not in repr(collector.trace)
    assert secret not in json.dumps(collector.finish({}))


@pytest.mark.parametrize('mode', ['text', 'items', 'bytes', 'fields', 'depth'])
def test_budgets_are_explicit_deterministic_and_preserve_prefix(mode):
    from monitoring.trace import TraceCollector, TraceLimits, encoded
    limits = TraceLimits(**{'text': dict(max_text_chars=12), 'items': dict(max_items_per_stage=2),
                           'bytes': dict(max_bytes=5000), 'fields': dict(max_fields_per_item=3),
                           'depth': dict(max_depth=1)}[mode])
    results = []
    for _ in range(2):
        trace = TraceCollector('run', limits=limits)
        for n in range(8):
            trace.add('acquisition', f'record:{n}', {'text': '界' * 150, 'nested': {'a': {'b': [1, 2, 3]}}})
        results.append(trace.finish({}))
    assert results[0] == results[1]
    result = results[0]
    assert not result['trace_complete']
    assert len(encoded(result).encode()) <= limits.max_bytes
    assert result['totals']['acquisition'] == 8
    rows = result['stages']['acquisition']
    assert [r['ref'] for r in rows] == [f'record:{n}' for n in range(len(rows))]
    assert len(rows) + result['omitted_items']['acquisition'] == 8
    if mode == 'text':
        assert all(r['preview_truncated'] for r in rows)
    if mode in ('fields', 'depth'):
        assert all(r['omitted_fields'] > 0 for r in rows)


def test_observer_does_not_mutate_borrowed_data():
    from monitoring.trace import TraceCollector
    value = {'text': 'password=private', 'nested': ['one', {'api_key': 'private'}]}
    before = deepcopy(value)
    trace = TraceCollector('run')
    trace.add('acquisition', 'record:1', value)
    assert value == before


def test_text_limit_keeps_authoritative_identity():
    from monitoring.trace import TraceCollector, TraceLimits
    trace = TraceCollector('run', limits=TraceLimits(max_text_chars=8))
    trace.add('patterns', 'occurrence:1', dict(template_id='authoritative-pattern-identity', template='long pattern example'))
    item = trace.finish({})['stages']['patterns'][0]
    assert item['data']['template_id'] == 'authoritative-pattern-identity'
    assert item['preview_truncated']


@pytest.mark.parametrize('count', [2000, 10000])
def test_trace_retained_memory_is_bounded_after_budget_exhaustion(count):
    import tracemalloc
    from monitoring.trace import TraceCollector, TraceLimits, encoded
    # Covers both the default 100*20 acquisition bound and the permitted 500*20 maximum.
    tracemalloc.start()
    try:
        trace = TraceCollector('run', limits=TraceLimits(max_bytes=32768))
        text = 'x' * 9000
        for n in range(count):
            trace.add('acquisition', f'record:{n}', {'text': text})
        result = trace.finish({})
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert peak < 2 * 1024 * 1024
    assert len(encoded(result).encode()) <= 32768
    assert result['totals']['acquisition'] == count
    assert result['omitted_items']['acquisition'] == count - len(result['stages']['acquisition'])


def test_capture_does_not_stop_at_legacy_200_sample_limit(repo, pipeline):
    from monitoring.trace import TraceCollector
    from segmentation_layer.contracts import SegmentationPolicy
    records = [record(str(n), n, 'ERROR: normal') for n in range(250)]
    trace = TraceCollector('run')
    for source in records:
        trace.acquisition(source, timestamp=BASE, inside_window=True, overlap=False, duplicate=False)
    result = pipeline.process_ingested_pages([page(records)], policy_provider=lambda *a: SegmentationPolicy('id', '^ERROR:', 'offline'), observer=trace)
    saved = trace.finish(result)
    assert len(result['pipeline_trace']['parser']['items']) == 200
    assert len(saved['stages']['parsing']) == len(saved['stages']['patterns']) == 250
    assert saved['trace_complete']


def test_acquisition_overlap_duplicates_and_ownership_disposition(repo, pipeline):
    monitor = repo.create(definition(), enabled=True, now=BASE)
    rows = [record('left', -10, 'ERROR: context'), record('a', 1, 'ERROR: owned'),
            record('a', 1, 'ERROR: owned'), record('right', 910, 'ERROR: context')]
    executor, _ = executor_for(pipeline, rows)
    result = executor.execute(repo.claim(BASE + timedelta(minutes=17)), set())
    trace = result.presentation['detailed_pipeline_trace']
    assert [i['data']['acquisition_overlap'] for i in trace['stages']['acquisition']] == [True, False, False, True]
    assert [i['data']['duplicate'] for i in trace['stages']['acquisition']] == [False, False, True, False]
    assert [i['data']['admitted'] for i in trace['stages']['segmentation']] == [False, True, False]
    assert trace['stages']['parsing'][0]['parents'] == ('logical:2',)
    assert result.counts.logical_events == 1


def test_unassembled_blank_retains_source_link(pipeline):
    from monitoring.trace import TraceCollector
    from segmentation_layer.contracts import SegmentationPolicy
    rows = [record('blank', 1, ''), record('a', 2, 'ERROR: normal')]
    trace = TraceCollector('run')
    for source in rows:
        trace.acquisition(source, timestamp=BASE, inside_window=True, overlap=False, duplicate=False)
    result = pipeline.process_ingested_pages([page(rows)],
        policy_provider=lambda *a: SegmentationPolicy('id', '^ERROR:', 'offline'), observer=trace)
    item = trace.finish(result)['stages']['segmentation'][0]
    assert item['data']['disposition'] == 'blank_context'
    assert item['parents'] == ('record:1',)


def test_link_budget_and_sensitive_field_budget_are_explicit():
    from monitoring.trace import TraceCollector, TraceLimits
    trace = TraceCollector('run', limits=TraceLimits(max_fields_per_item=3))
    trace.add('incident', 'incident:1', {f'field{n}_token': 'private' for n in range(1000)},
              (f'correlation:{n}' for n in range(1000)))
    item = trace.finish({})['stages']['incident'][0]
    assert len(item['parents']) == 3 and item['omitted_links'] == 997
    assert len(item['data']) < 3 and item['omitted_fields'] > 997


def test_known_configured_secret_is_redacted_in_all_trace_fields():
    from monitoring.trace import TraceCollector
    secret = 'configured-private-credential'
    trace = TraceCollector('run', secrets=(secret,))
    trace.add('acquisition', 'record:1', {'text': secret, 'nested': {'arbitrary': secret}})
    assert secret not in json.dumps(trace.finish({}))


def test_trace_source_hash_reuses_receipt_identity_for_non_ascii_ids():
    from monitoring.trace import TraceCollector
    from monitoring.execution import reference_key
    source = record('olay-ş-界', 1, 'ERROR: normal')
    assert TraceCollector.reference(source) == reference_key(source)


def test_lineage_and_read_only_filters(traced_run, monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[2] / 'src' / 'frontend'))
    from trace_views import lineage, filter_items, load_trace, summary_row
    trace = load_trace(traced_run.result.presentation, traced_run.run.id, str)
    rows, terminal = lineage(trace, 'record:2')
    assert [row['ref'] for row in rows['segmentation']] == ['logical:1']
    assert [row['ref'] for row in rows['parsing']] == ['canonical:1']
    assert [row['ref'] for row in rows['patterns']] == ['occurrence:1']
    assert len(rows['signal']) == 1 and terminal == 'Suppressed at Signal stage.'
    assert not rows['correlation'] and not rows['incident'] and not rows['rca']
    assert len(filter_items(trace['stages']['signal'], decision='SUPPRESSED')) == 1
    assert not filter_items(trace['stages']['signal'], decision='QUALIFIED')
    assert len(filter_items(trace['stages']['segmentation'], 'possible_incomplete')) == 1
    assert summary_row(trace['stages']['segmentation'][-1])['Boundary status'] == 'possible_incomplete'
    assert load_trace(traced_run.result.presentation, 'different-run', str) is None
    assert load_trace({}, traced_run.run.id, str) is None


def test_actual_expert_input_captured_without_changing_prompt_or_ranking(monkeypatch):
    from monitoring.trace import TraceCollector
    import rca_layer.rca_engine as engine_module
    from test_rca_expert import case, answer
    calls = []
    def fake(agent, prompt, content, **kwargs):
        calls.append(content)
        return json.dumps(answer()), .1, {'finish_reason': 'stop'}
    monkeypatch.setattr(engine_module, 'call_ai_agent', fake)
    engine = engine_module.ExpertRCAEngine(enabled=True)
    trace = TraceCollector('run')
    base = engine.base.analyze(*case())
    result = engine.analyze(*case(), observer=trace)
    payload = trace.finish(dict(rca=result, case_analysis=engine.last_case_analysis))
    expert = next(i for i in payload['stages']['rca'] if i['data']['kind'] == 'expert_input')
    assert expert['data']['evidence'] == json.loads(calls[0])
    deterministic = next(i for i in payload['stages']['rca'] if i['data']['kind'] == 'deterministic')
    assert deterministic['data']['root_cause_candidates'] == base[0]['root_cause_candidates']
    assert any(i['data']['kind'] == 'expert_interpretation' for i in payload['stages']['rca'])
    assert len(calls) == 1
