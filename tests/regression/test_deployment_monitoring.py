"""Offline monitoring persistence/scheduling and real pipeline boundary tests."""
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
import socket
import sqlite3
import json
from types import SimpleNamespace

import pytest

BASE = datetime(2026, 10, 5, 12, tzinfo=timezone.utc)
DOCUMENT_UUID = '11111111-2222-4333-8444-555555555555'


@pytest.fixture(autouse=True)
def offline(monkeypatch, tmp_path):
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[2] / 'src' / 'backend'))
    import requests
    import ai_engine
    import full_pipeline_v2
    import ingestion_layer.contracts as contracts
    from template_layer.engine import DrainCandidateMiner, ValidatedTemplateRegistry

    def forbidden(*args, **kwargs):
        pytest.fail('Monitoring regressions must not use real network, LLM or production learning state')

    for owner, names in ((socket.socket, ('connect', 'connect_ex')),
                         (socket, ('create_connection', 'getaddrinfo')),
                         (requests.sessions.Session, ('request', 'send')),
                         (DrainCandidateMiner, ('__init__',)), (ValidatedTemplateRegistry, ('__init__',))):
        for name in names:
            monkeypatch.setattr(owner, name, forbidden)
    monkeypatch.setattr(ai_engine, 'call_ai_agent', forbidden)
    monkeypatch.setattr(full_pipeline_v2, 'SourcePage', contracts.SourcePage)
    original = sqlite3.connect

    def temporary_only(path, *args, **kwargs):
        assert Path(path).resolve().is_relative_to(tmp_path.resolve()), 'Only temporary monitoring databases are allowed'
        return original(path, *args, **kwargs)

    monkeypatch.setattr(sqlite3, 'connect', temporary_only)
    monkeypatch.setattr(sqlite3.dbapi2, 'connect', temporary_only)


@pytest.fixture
def repo(tmp_path):
    from monitoring.repository import SQLiteMonitorRepository
    return SQLiteMonitorRepository(tmp_path / 'monitor.sqlite3')


def definition(**changes):
    from monitoring.domain import MonitorDefinition
    return replace(MonitorDefinition('test-monitor', 'test-profile', 'cluster', 'ns', 'app', BASE,
                                     window_seconds=900, interval_seconds=900), **changes)


def empty_execution():
    from monitoring.execution import ExecutionResult
    from monitoring.domain import RunCounts
    return ExecutionResult({'stats': {}, 'signals': [], 'source_summary': {}}, RunCounts(), {})


def worker(repo, now, executor=None):
    from monitoring.worker import MonitorWorker
    calls, logs = [], []
    def execute(run, consumed):
        calls.append(run)
        return executor(run, consumed) if executor else empty_execution()
    instance = MonitorWorker(repo, SimpleNamespace(execute=execute), clock=lambda: now[0],
                             log=lambda event, **values: logs.append((event, values)))
    return instance, calls, logs


def test_create_read_edit_and_schema_is_separate(repo):
    monitor = repo.create(definition(), now=BASE)
    assert not monitor.enabled and monitor.status.value == 'PAUSED'
    assert monitor.definition.source_timezone is None
    edited = repo.update(monitor.id, replace(monitor.definition, name='edited', source_timezone='UTC'),
                         revision=monitor.revision, now=BASE)
    assert repo.list() == [edited]
    with sqlite3.connect(repo.path) as db:
        assert db.execute('PRAGMA user_version').fetchone()[0] == 4
        assert {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")} == {
            'monitors', 'monitor_runs', 'monitor_results', 'monitor_receipts',
            'monitor_run_metrics', 'monitor_pattern_metrics', 'monitor_acquisition_shards',
            'monitor_run_dedupe'}
    with pytest.raises(ValueError, match='changed'):
        repo.update(monitor.id, definition(), revision=monitor.revision, now=BASE)


def test_v2_migration_preserves_results_and_defaults_diagnostics(repo):
    from monitoring.repository import SQLiteMonitorRepository
    monitor = repo.create(definition(), enabled=True, now=BASE)
    worker(repo, [BASE + timedelta(minutes=17)])[0].tick()
    run = repo.history(monitor.id)[0]
    before = repo.result(run.id)
    with sqlite3.connect(repo.path) as db:
        db.execute('ALTER TABLE monitor_runs DROP COLUMN acquisition_diagnostics')
        db.execute('PRAGMA user_version=2')
        stored = db.execute('SELECT * FROM monitor_runs').fetchone()
    reopened = SQLiteMonitorRepository(repo.path)
    assert reopened.result(run.id) == before and reopened.history(monitor.id) == [run]
    assert reopened.get(monitor.id).last_successful_end == run.window.end
    assert reopened.acquisition_diagnostics(run.id) is None
    with sqlite3.connect(repo.path) as db:
        assert db.execute('PRAGMA user_version').fetchone()[0] == 4
        assert db.execute('SELECT * FROM monitor_runs').fetchone()[:-1] == stored


@pytest.mark.parametrize('changes', [
    {'source_timezone': 'Invalid/Zone'}, {'initial_start': BASE.replace(tzinfo=None)},
    {'window_seconds': 0}, {'interval_seconds': -1}, {'overlap_seconds': 901},
    {'page_size': 501}, {'max_pages': 21}, {'ingestion_delay_seconds': -1},
    {'cluster_id': ''}, {'name': 'bad\nname'}, {'window_seconds': 86400},
    {'document_cluster_id': ''}, {'document_cluster_id': '  '}, {'document_cluster_id': 123},
    {'document_cluster_id': 'invalid\nvalue'}, {'document_cluster_id': 'x' * 201},
])
def test_definition_validation(changes):
    with pytest.raises(ValueError):
        definition(**changes)


def test_exact_windows_ingestion_delay_and_disabled_due_selection(repo):
    from monitoring.domain import next_window, safe_at
    monitor = repo.create(definition(), now=BASE)
    window = next_window(monitor)
    assert window.start == BASE and window.end == BASE + timedelta(minutes=15)
    assert safe_at(window, monitor.definition) == BASE + timedelta(minutes=17)
    assert repo.claim(BASE + timedelta(hours=1)) is None
    repo.set_enabled(monitor.id, True, now=BASE)
    assert repo.claim(BASE + timedelta(minutes=17) - timedelta(microseconds=1)) is None
    assert repo.get(monitor.id).last_successful_end is None
    run = repo.claim(BASE + timedelta(minutes=17))
    assert run.window == window
    assert repo.claim(BASE + timedelta(hours=1)) is None  # durable running claim


def test_success_pause_resume_history_and_watermark(repo):
    monitor = repo.create(definition(), enabled=True, now=BASE)
    clock = [BASE + timedelta(minutes=17)]
    scheduler, calls, _ = worker(repo, clock)
    assert scheduler.tick() == 1
    successful = repo.get(monitor.id)
    assert successful.last_successful_end == BASE + timedelta(minutes=15)
    assert successful.next_run_at == BASE + timedelta(minutes=32)
    repo.set_enabled(monitor.id, False, now=clock[0])
    clock[0] += timedelta(hours=1)
    assert scheduler.tick() == 0
    assert len(repo.history(monitor.id)) == 1
    paused = repo.get(monitor.id)
    assert paused.last_successful_end == successful.last_successful_end
    assert paused.definition == monitor.definition and paused.status.value == 'PAUSED'
    repo.set_enabled(monitor.id, True, now=clock[0])
    assert scheduler.tick() == 1
    assert calls[-1].window.start == successful.last_successful_end
    assert repo.result(calls[0].id) is not None


def test_archive_preserves_history_evidence_watermark_and_duplicate_name(repo):
    monitor = repo.create(definition(), enabled=True, now=BASE)
    run = repo.claim(BASE + timedelta(minutes=17))
    from monitoring.domain import RunCounts
    result = {'investigation': 'persisted', 'detailed_pipeline_trace': {'persisted': True}}
    repo.succeed(run, result, RunCounts(logical_events=3), {'receipt': BASE.isoformat()}, now=BASE)
    duplicate = repo.create(definition(), enabled=True, now=BASE)
    before, history = repo.get(monitor.id), repo.history(monitor.id)
    receipts = repo.consumed(monitor.id, BASE)
    archived = repo.archive(monitor.id, now=BASE)
    assert archived.archived and not archived.enabled and archived.status.value == 'ARCHIVED'
    assert archived.definition == before.definition
    assert archived.last_successful_end == before.last_successful_end
    assert archived.next_run_at == before.next_run_at
    assert repo.history(monitor.id) == history and repo.result(run.id) == result
    assert repo.consumed(monitor.id, BASE) == receipts
    assert repo.list() == [duplicate]
    assert {m.id for m in repo.list(include_archived=True)} == {monitor.id, duplicate.id}
    assert repo.get(duplicate.id) == duplicate
    claimed = repo.claim(BASE + timedelta(hours=1))
    assert claimed.monitor_id == duplicate.id
    assert repo.claim(BASE + timedelta(hours=2)) is None
    with pytest.raises(ValueError, match='archived'):
        repo.set_enabled(monitor.id, True, now=BASE)
    with pytest.raises(ValueError, match='archived'):
        repo.update(monitor.id, definition(name='changed'), revision=archived.revision, now=BASE)


@pytest.mark.parametrize('completion', ['success', 'failure', 'recovery'])
def test_archive_during_execution_cannot_reactivate_or_retry(repo, completion):
    from monitoring.domain import RunCounts
    from monitoring.repository import SQLiteMonitorRepository
    monitor = repo.create(definition(), enabled=True, now=BASE)
    run = repo.claim(BASE + timedelta(minutes=17))
    repo.archive(monitor.id, now=BASE)
    if completion == 'success':
        repo.succeed(run, {'recorded': True}, RunCounts(), {}, now=BASE)
        assert repo.result(run.id) == {'recorded': True}
        assert repo.get(monitor.id).last_successful_end == run.window.end
    elif completion == 'failure':
        repo.fail(run, 'OPENSEARCH_TIMEOUT', now=BASE)
    else:
        repo.recover_running(BASE)
    reopened = SQLiteMonitorRepository(repo.path)
    archived = reopened.get(monitor.id)
    assert archived.archived and not archived.enabled and archived.status.value == 'ARCHIVED'
    assert len(reopened.history(monitor.id)) == 1
    assert reopened.claim(BASE + timedelta(days=1)) is None
    assert worker(reopened, [BASE + timedelta(days=1)])[0].tick(max_runs=5) == 0


def test_archive_is_idempotent_and_claim_guard_is_independent_of_enabled(repo):
    monitor = repo.create(definition(), enabled=True, now=BASE)
    archived = repo.archive(monitor.id, now=BASE)
    assert repo.archive(monitor.id, now=BASE + timedelta(hours=1)) == archived
    # Even an inconsistent enabled flag cannot bypass the archive lifecycle.
    with sqlite3.connect(repo.path) as db:
        db.execute('UPDATE monitors SET enabled=1 WHERE id=?', (monitor.id,))
    assert repo.claim(BASE + timedelta(days=1)) is None


def test_v1_migration_defaults_existing_monitors_and_preserves_all_audit_rows(repo, tmp_path):
    from monitoring.repository import SQLiteMonitorRepository
    monitor = repo.create(definition(), enabled=True, now=BASE)
    scheduler, calls, _ = worker(repo, [BASE + timedelta(minutes=17)])
    scheduler.tick()
    # Build a populated v1 database, rather than migrating an empty fixture.
    legacy_path = tmp_path / 'legacy-v1.sqlite3'
    with sqlite3.connect(repo.path) as source, sqlite3.connect(legacy_path) as legacy:
        for table in ('monitors', 'monitor_runs', 'monitor_results', 'monitor_receipts'):
            schema = source.execute("SELECT sql FROM sqlite_master WHERE name=?", (table,)).fetchone()[0]
            if table == 'monitors':
                schema = schema.replace(', archived INTEGER NOT NULL DEFAULT 0', '')
            if table == 'monitor_runs':
                schema = schema.replace(', acquisition_diagnostics TEXT', '')
            legacy.execute(schema)
            columns = [row[1] for row in source.execute(f'PRAGMA table_info({table})') if row[1] not in ('archived', 'acquisition_diagnostics')]
            records = source.execute(f'SELECT {",".join(columns)} FROM {table}').fetchall()
            legacy.executemany(f'INSERT INTO {table} VALUES ({",".join("?" for _ in columns)})', records)
        legacy.execute('PRAGMA user_version=1')
    with sqlite3.connect(legacy_path) as db:
        before = {table: db.execute(f'SELECT * FROM {table}').fetchall()
                  for table in ('monitor_runs', 'monitor_results', 'monitor_receipts')}
        assert 'archived' not in {row[1] for row in db.execute('PRAGMA table_info(monitors)')}
    migrated = SQLiteMonitorRepository(legacy_path)
    loaded = migrated.get(monitor.id)
    assert not loaded.archived and loaded == repo.get(monitor.id)
    assert migrated.result(calls[0].id) == repo.result(calls[0].id)
    assert migrated.history(monitor.id) == repo.history(monitor.id)
    with sqlite3.connect(legacy_path) as db:
        assert db.execute('PRAGMA user_version').fetchone()[0] == 4
        for table, records in before.items():
            rows = db.execute(f'SELECT * FROM {table}').fetchall()
            assert ([row[:-1] for row in rows] if table == 'monitor_runs' else rows) == records
        assert db.execute('SELECT acquisition_diagnostics FROM monitor_runs').fetchone()[0] is None
    migrated.archive(monitor.id, now=BASE)
    assert migrated.result(calls[0].id) == repo.result(calls[0].id)
    assert SQLiteMonitorRepository(legacy_path).get(monitor.id).archived


def test_failed_window_retry_and_restart_do_not_duplicate(repo):
    from monitoring.errors import MonitoringError
    from monitoring.repository import SQLiteMonitorRepository
    monitor = repo.create(definition(), enabled=True, now=BASE)
    clock = [BASE + timedelta(minutes=17)]
    failing, _, _ = worker(repo, clock, lambda *a: (_ for _ in ()).throw(MonitoringError('OPENSEARCH_TIMEOUT')))
    assert failing.tick() == 0
    failed = repo.history(monitor.id)[0]
    assert failed.status.value == 'FAILED' and failed.error_category == 'OPENSEARCH_TIMEOUT'
    assert repo.get(monitor.id).last_successful_end is None
    assert repo.result(failed.id) is None
    reopened = SQLiteMonitorRepository(repo.path)
    clock[0] += timedelta(minutes=15)
    scheduler, calls, _ = worker(reopened, clock)
    assert scheduler.tick() == 1
    retried = reopened.history(monitor.id)[0]
    assert retried.id == failed.id and retried.attempts == 2
    assert retried.status.value == 'SUCCESS'
    assert len(reopened.history(monitor.id)) == 1
    with sqlite3.connect(repo.path) as db:
        assert db.execute('SELECT count(*) FROM monitor_results').fetchone()[0] == 1
        duplicate = list(db.execute('SELECT * FROM monitor_runs').fetchone())
        duplicate[0] = 'different-run-id'
        with pytest.raises(sqlite3.IntegrityError, match='monitor_runs.monitor_id'):
            db.execute('INSERT INTO monitor_runs VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', duplicate)
    with pytest.raises(ValueError, match='claim'):
        repo.succeed(calls[0], {}, retried.counts, {}, now=clock[0])


def test_interrupted_claim_recovery_fences_old_owner(repo):
    from monitoring.repository import SQLiteMonitorRepository
    from monitoring.domain import RunCounts
    monitor = repo.create(definition(), enabled=True, now=BASE)
    now = BASE + timedelta(hours=1)
    abandoned = repo.claim(now)
    reopened = SQLiteMonitorRepository(repo.path)
    assert reopened.claim(now) is None
    reopened.recover_running(now)
    retry = reopened.claim(now)
    assert retry.id == abandoned.id and retry.claim_token != abandoned.claim_token
    with pytest.raises(ValueError):
        repo.succeed(abandoned, {}, RunCounts(), {}, now=now)
    reopened.succeed(retry, {}, RunCounts(), {}, now=now)
    assert repo.get(monitor.id).last_successful_end == BASE + timedelta(minutes=15)


def test_backlog_is_sequential_bounded_and_limited_per_tick(repo):
    monitor = repo.create(definition(), enabled=True, now=BASE)
    clock = [BASE + timedelta(hours=1, minutes=2)]
    scheduler, calls, _ = worker(repo, clock)
    assert scheduler.tick(max_runs=2) == 2
    assert scheduler.tick(max_runs=10) == 2
    assert [(r.window.start, r.window.end) for r in calls] == [
        (BASE + timedelta(minutes=15 * i), BASE + timedelta(minutes=15 * (i + 1))) for i in range(4)]
    assert repo.get(monitor.id).last_successful_end == BASE + timedelta(hours=1)


def test_result_receipt_watermark_transaction_rolls_back(repo):
    from monitoring.domain import RunCounts
    monitor = repo.create(definition(), enabled=True, now=BASE)
    run = repo.claim(BASE + timedelta(minutes=17))
    with sqlite3.connect(repo.path) as db:
        db.execute("CREATE TRIGGER fail_receipt BEFORE INSERT ON monitor_receipts BEGIN SELECT RAISE(ABORT, 'synthetic'); END")
    with pytest.raises(sqlite3.IntegrityError):
        repo.succeed(run, {'safe': 1}, RunCounts(), {'reference': BASE.isoformat()}, now=BASE)
    assert repo.result(run.id) is None
    assert repo.get(monitor.id).last_successful_end is None
    assert repo.history(monitor.id)[0].status.value == 'RUNNING'


def test_worker_failure_is_safe_and_other_monitors_continue(repo):
    first = repo.create(definition(), enabled=True, now=BASE)
    second = repo.create(definition(name='other'), enabled=True, now=BASE)
    def execute(run, consumed):
        if run.monitor_id == first.id:
            raise RuntimeError('Authorization: Bearer super-secret; password=credential; https://secret-host.invalid')
        return empty_execution()
    scheduler, _, logs = worker(repo, [BASE + timedelta(minutes=17)], execute)
    assert scheduler.tick(max_runs=2) == 1
    stored = str(repo.history(first.id)) + str(repo.get(first.id)) + str(logs)
    for secret in ('super-secret', 'Authorization', 'credential;', 'secret-host'):
        assert secret not in stored
    assert repo.history(second.id)[0].status.value == 'SUCCESS'


def test_pause_during_execution_finishes_current_only(repo):
    monitor = repo.create(definition(), enabled=True, now=BASE)
    def execute(run, consumed):
        repo.set_enabled(monitor.id, False, now=BASE)
        return empty_execution()
    scheduler, calls, _ = worker(repo, [BASE + timedelta(hours=1)], execute)
    assert scheduler.tick(max_runs=5) == 1 and len(calls) == 1
    assert repo.get(monitor.id).status.value == 'PAUSED'
    assert repo.get(monitor.id).last_successful_end == BASE + timedelta(minutes=15)


def test_scope_immutable_after_run_but_name_interval_editable(repo):
    monitor = repo.create(definition(), enabled=True, now=BASE)
    worker(repo, [BASE + timedelta(minutes=17)])[0].tick()
    monitor = repo.get(monitor.id)
    with pytest.raises(ValueError, match='first run'):
        repo.update(monitor.id, definition(workload='new-app'), revision=monitor.revision, now=BASE)
    with pytest.raises(ValueError, match='first run'):
        repo.update(monitor.id, definition(document_cluster_id=DOCUMENT_UUID), revision=monitor.revision, now=BASE)
    edited = repo.update(monitor.id, definition(name='renamed', interval_seconds=1800), revision=monitor.revision, now=BASE)
    assert edited.last_successful_end == monitor.last_successful_end


def record(key, seconds, text, *, pod='pod-id', channel='stdout', container_id='container-id'):
    from ingestion_layer.contracts import IngestedLogRecord, SourceReference, StreamIdentity, Framing
    return IngestedLogRecord(
        raw_text=text, source_reference=SourceReference('test-profile', 'logs-test', key),
        source_timestamp_raw=(BASE + timedelta(seconds=seconds)).isoformat(), source_timestamp_field='@timestamp',
        stream_identity=StreamIdentity(source_scope='cluster', namespace='ns', workload='app', pod='pod-' + pod,
                                       pod_instance=pod, container='main', container_instance=container_id, channel=channel),
        retrieval_order=(seconds, key), framing=Framing.PHYSICAL_LINE)


def page(records, *, exhausted=True, cursor=None):
    from ingestion_layer.contracts import SourcePage
    return SourcePage(tuple(records), interval_exhausted=exhausted, next_cursor=cursor,
                      page_limit_reached=not exhausted, cycle_budget_reached=False)


@pytest.fixture
def pipeline(monkeypatch):
    import full_pipeline_v2 as full
    from parser_layer.parser_pipeline import ParserPipeline
    # Real parser routes and canonical timestamp authority, no persisted policy.
    monkeypatch.setattr('parser_layer.parser_pipeline.ParserPolicyRegistry', lambda *args: SimpleNamespace())
    monkeypatch.setattr('parser_layer.parser_pipeline.ParserPolicyDiscovery', lambda: SimpleNamespace())
    pipeline = full.FullAIOpsPipelineV2.__new__(full.FullAIOpsPipelineV2)
    pipeline.parser = ParserPipeline(ai_enabled=False)
    rows = []
    class Templater:
        last_decision = {}
        def process(self, event):
            rows.append(event)
            return SimpleNamespace(template_id='fixed-template', template='safe template', reliable=True)
        def save_state(self):
            pass
    pipeline.templater = Templater()
    pipeline.downstream = SimpleNamespace(set_context=lambda context: None,
                                         process=lambda events, **options: {'stats': {}, 'signals': [], 'rca': [], 'incidents': []})
    pipeline.rows = rows
    return pipeline


def executor_for(pipeline, records_or_source, *, config=None):
    from monitoring.execution import OpenSearchMonitorExecutor, source_time
    from ingestion_layer.opensearch_config import OpenSearchConfig, OpenSearchFieldMapping
    from opensearch_application import VerifiedPolicy
    from segmentation_layer.contracts import SegmentationPolicy
    config = config or OpenSearchConfig(('https://synthetic.invalid',), 'test-user', 'test-password', True, True,
                                        'logs-*', 1, 1, 'test-profile', OpenSearchFieldMapping(), 100)
    queries = []
    class Client:
        def __enter__(self):
            return self
        def __exit__(self, *args):
            pass
    class Source:
        def read_page(self, **query):
            queries.append(query)
            if callable(records_or_source):
                return records_or_source(query)
            return page(r for r in records_or_source if query['start'] <= source_time(r) < query['end'])
    policy = VerifiedPolicy('policy-safe', SegmentationPolicy('id', r'^ERROR:', 'verified-fixture'))
    executor = OpenSearchMonitorExecutor(lambda: pipeline, connection_loader=lambda size: config,
                                         client_factory=lambda cfg: Client(), source_factory=lambda client: Source(),
                                         policy_loader=lambda: (policy,))
    return executor, queries


def test_monitor_pipeline_timestamp_fallback_is_per_event_and_timezone_propagates(repo, pipeline):
    records = [record('a', 3, 'ERROR: database failure'), record('b', 4, '    at trace'),
               record('c', 7, 'ERROR: connection timeout')]
    monitor = repo.create(definition(source_timezone='Europe/Istanbul'), enabled=True, now=BASE)
    run = repo.claim(BASE + timedelta(minutes=17))
    executor, queries = executor_for(pipeline, records)
    result = executor.execute(run, set())
    assert result.counts.logical_events == 2
    assert [row['timestamp'] for row in pipeline.rows] == [BASE + timedelta(seconds=3), BASE + timedelta(seconds=7)]
    for row in pipeline.rows:
        provenance = row['timestamp_provenance']
        assert provenance['source_timezone'] == 'Europe/Istanbul'
        assert provenance['basis'] == 'source_record' and provenance['source_record_field'] == '@timestamp'
    assert queries[0]['start'] == BASE - timedelta(seconds=60)
    assert queries[0]['end'] == BASE + timedelta(minutes=16)
    assert queries[0]['cluster_id'] is None and queries[0]['workload'] == 'app'


def test_overlap_multiline_is_assembled_once_across_windows_and_restart(repo, pipeline):
    from monitoring.worker import MonitorWorker
    from monitoring.repository import SQLiteMonitorRepository
    records = [record('a', 890, 'ERROR: failure'), record('b', 903, '    at continuation'),
               record('c', 910, 'ERROR: second failure'), record('d', 920, 'ERROR: third failure')]
    monitor = repo.create(definition(), enabled=True, now=BASE)
    executor, _ = executor_for(pipeline, records)
    scheduler = MonitorWorker(repo, executor, clock=lambda: BASE + timedelta(minutes=17), log=lambda *a, **k: None)
    assert scheduler.tick() == 1
    assert len(pipeline.rows) == 1
    assert pipeline.rows[0]['attributes']['source_provenance']['contributors'][0]['source_reference']['record_id'] == 'a'
    assert len(pipeline.rows[0]['attributes']['source_provenance']['contributors']) == 2
    reopened = SQLiteMonitorRepository(repo.path)
    assert MonitorWorker(reopened, executor, clock=lambda: BASE + timedelta(minutes=32), log=lambda *a, **k: None).tick() == 1
    assert len(pipeline.rows) == 3  # first event never delivered twice to learning/signals
    assert sum(r.counts.logical_events for r in repo.history(monitor.id)) == 3
    assert repo.history(monitor.id)[0].definition.source_timezone is None


def test_source_reference_dedupe_preserves_pod_container_channel_streams(repo, pipeline):
    records = [record('a', 1, 'ERROR: one'), record('a', 1, 'ERROR: one'), record('b', 2, 'ERROR: two'),
               record('c', 3, 'ERROR: pod changed', pod='new'), record('d', 4, 'ERROR: stderr', channel='stderr'),
               record('e', 5, 'ERROR: container changed', container_id='new-container')]
    monitor = repo.create(definition(), enabled=True, now=BASE)
    executor, _ = executor_for(pipeline, records)
    result = executor.execute(repo.claim(BASE + timedelta(minutes=17)), set())
    assert result.counts.events_retrieved == 6 and result.counts.unique_records == 5
    assert result.counts.logical_events == 5
    assert result.presentation['source_summary']['assembled_stream_count'] == 4
    assert len(result.receipts) == 5


def test_long_boundary_orphan_and_tail_are_explicit_and_not_recounted(repo, pipeline):
    records = [record('orphan', -50, '    at old trace'), record('a', 2, 'ERROR: start'), record('b', 5, 'ERROR: tail')]
    repo.create(definition(), enabled=True, now=BASE)
    executor, _ = executor_for(pipeline, records)
    result = executor.execute(repo.claim(BASE + timedelta(minutes=17)), set())
    diagnostics = result.presentation['source_summary']['window_assembly']
    assert diagnostics['orphan_events'] == 1 and diagnostics['boundary_tail_events'] == 1
    assert result.counts.logical_events == 2


def test_boundary_quality_survives_owned_event_provenance_and_persistence(repo, pipeline):
    from monitoring.worker import MonitorWorker
    records = [record('context', -50, 'ERROR: context'), record('a', 1, 'ERROR: first'),
               record('b', 2, '    continuation'), record('c', 3, 'ERROR: tail')]
    monitor = repo.create(definition(), enabled=True, now=BASE)
    executor, _ = executor_for(pipeline, records)
    assert MonitorWorker(repo, executor, clock=lambda: BASE + timedelta(minutes=17), log=lambda *a, **k: None).tick() == 1
    run = repo.history(monitor.id)[0]
    quality = repo.result(run.id)['source_summary']['boundary_quality']
    assert quality['counts'] == dict(complete=1, possible_incomplete=1, confirmed_truncated=0)
    assert quality['total_events'] == run.counts.logical_events == 2
    assert [row['boundary_status'] for row in quality['event_statuses']] == ['complete', 'possible_incomplete']
    assert [row['attributes']['source_provenance']['boundary_status'] for row in pipeline.rows] == ['complete', 'possible_incomplete']
    affected, = quality['affected_events']
    assert affected['event_id'] == pipeline.rows[-1]['event_id']
    assert affected['event_ordinal'] == 2 and affected['emission_reason'] == 'analysis_end'
    assert affected['pod_instance'] == 'pod-id' and affected['container_instance'] == 'container-id'
    assert affected['channel'] == 'stdout' and affected['physical_record_count'] == 1
    assert affected['source_start_time'] == affected['last_source_time'] == (BASE + timedelta(seconds=3)).isoformat()
    assert repo.get(monitor.id).last_successful_end == run.window.end


def test_boundary_details_are_bounded_but_statuses_and_analysis_keep_every_event(repo, pipeline):
    from monitoring.boundary_quality import BOUNDARY_DIAGNOSTIC_LIMIT
    count = BOUNDARY_DIAGNOSTIC_LIMIT + 5
    records = [record(str(i), i, 'ERROR: independent event', pod=str(i)) for i in range(count)]
    pages = [page(records[i:i + 100], exhausted=i + 100 >= count, cursor=str(i + 100)) for i in range(0, count, 100)]
    executor, _ = executor_for(pipeline, lambda query: pages.pop(0))
    repo.create(definition(), enabled=True, now=BASE)
    result = executor.execute(repo.claim(BASE + timedelta(minutes=17)), set())
    quality = result.presentation['source_summary']['boundary_quality']
    assert quality['counts'] == dict(complete=0, possible_incomplete=count, confirmed_truncated=0)
    assert quality['total_events'] == result.counts.logical_events == len(pipeline.rows) == count
    assert len(quality['event_statuses']) == count
    assert len(quality['affected_events']) == quality['sample_limit'] == BOUNDARY_DIAGNOSTIC_LIMIT
    assert quality['event_statuses'][-1]['event_ordinal'] == count
    assert len({row['stream_id'] for row in quality['affected_events']}) == BOUNDARY_DIAGNOSTIC_LIMIT


def test_boundary_diagnostics_never_copy_payloads_credentials_or_raw_references(repo, pipeline, monkeypatch, capsys):
    from monitoring.execution import reference_key
    from monitoring.worker import MonitorWorker
    monkeypatch.setenv('SAKA_API_KEY', 'synthetic-boundary-ai-key')
    secret_text = 'Authorization: Bearer synthetic-auth; api_key=private-api-key; customer_payload=private-body'
    records = [record('a', 1, 'ERROR: ' + secret_text), record('b', 2, 'ERROR: independent'),
               record('private-source-id', 3, 'ERROR: ' + secret_text, pod='test-password'),
               record('c', 4, '   private continuation', pod='test-password')]
    records[-1] = replace(records[-1], metadata=(('Authorization', 'Bearer secret-metadata'),))
    jwt = 'eyJboundary.eyJcustomer.private-signature'
    records[-2:] = [replace(row, stream_identity=replace(row.stream_identity,
                       container_instance='synthetic-boundary-ai-key', channel='Bearer private-channel; ' + jwt)) for row in records[-2:]]
    monitor = repo.create(definition(), enabled=True, now=BASE)
    executor, _ = executor_for(pipeline, records)
    assert MonitorWorker(repo, executor, clock=lambda: BASE + timedelta(minutes=17)).tick() == 1
    quality = repo.result(repo.history(monitor.id)[0].id)['source_summary']['boundary_quality']
    affected = quality['affected_events'][-1]
    assert affected['pod_instance'] == affected['container_instance'] == '[redacted]'
    assert affected['channel'] == '[redacted]; [redacted]'
    assert affected['first_record_reference'] == reference_key(records[-2])
    assert affected['last_record_reference'] == reference_key(records[-1])
    assert affected['physical_record_count'] == 2
    assert affected['source_start_time'] == (BASE + timedelta(seconds=3)).isoformat()
    assert affected['last_source_time'] == (BASE + timedelta(seconds=4)).isoformat()
    captured = capsys.readouterr()
    inspected = str(quality) + captured.out + captured.err
    for secret in ('Authorization', 'private-api-key', 'private-body', 'synthetic-auth', 'private-source-id',
                   'secret-metadata', 'test-password', 'synthetic-boundary-ai-key', 'private-channel', 'private continuation', jwt):
        assert secret not in inspected
    assert 'event_statuses' not in captured.out and 'affected_events' not in captured.out


@pytest.mark.parametrize('failure,category', [('timeout', 'OPENSEARCH_TIMEOUT'), ('auth', 'OPENSEARCH_AUTH'),
                                             ('cap', 'ACQUISITION_LIMIT'), ('malformed', 'OPENSEARCH_QUERY')])
def test_acquisition_failures_persist_without_watermark_or_pipeline(repo, failure, category):
    from ingestion_layer.opensearch_client import OpenSearchClientError
    from monitoring.worker import MonitorWorker
    def source(query):
        if failure == 'timeout':
            raise OpenSearchClientError('OpenSearch connection or timeout failure')
        if failure == 'auth':
            raise OpenSearchClientError('OpenSearch HTTP failure (403)')
        return page([record('a', 1, 'ERROR: failure')], exhausted=False, cursor='opaque' if failure == 'cap' else None)
    def no_pipeline(*args, **kwargs):
        pytest.fail('Acquisition failure must precede all pipeline/learning activity')
    executor, queries = executor_for(SimpleNamespace(process_ingested_pages=no_pipeline), source)
    monitor = repo.create(definition(max_pages=1), enabled=True, now=BASE)
    scheduler = MonitorWorker(repo, executor, clock=lambda: BASE + timedelta(minutes=17), log=lambda *a, **k: None)
    assert scheduler.tick() == 0
    assert repo.get(monitor.id).last_successful_end is None
    assert repo.history(monitor.id)[0].error_category == category
    assert len(queries) == 1
    run = repo.history(monitor.id)[0]
    diagnostics = repo.acquisition_diagnostics(run.id)
    assert diagnostics['source_profile'] == 'test-profile'
    assert diagnostics['namespace'] == 'ns' and diagnostics['workload'] == 'app'
    assert diagnostics['document_cluster_filter_active'] is False
    assert diagnostics['budget_reached'] is (failure == 'cap')
    assert diagnostics['records_read'] == (1 if failure in ('cap', 'malformed') else 0)
    assert not any(value in str(diagnostics) for value in ('test-password', 'test-user', 'opaque', 'ERROR: failure'))
    assert repo.result(run.id) is None
    # Retrying the same logical window starts a fresh attempt snapshot.
    retry = repo.claim(BASE + timedelta(minutes=32))
    assert retry.id == run.id and retry.attempts == 2
    assert repo.acquisition_diagnostics(run.id) is None


def test_empty_window_succeeds_without_policy_and_config_error_is_safe(repo, pipeline):
    from monitoring.errors import MonitoringError
    repo.create(definition(), enabled=True, now=BASE)
    run = repo.claim(BASE + timedelta(minutes=17))
    executor, _ = executor_for(pipeline, [])
    executor.policy_loader = lambda: pytest.fail('Empty windows need no policy')
    assert executor.execute(run, set()).counts.logical_events == 0
    executor.connection_loader = lambda size: (_ for _ in ()).throw(ValueError('password=test-secret'))
    with pytest.raises(MonitoringError, match='configured OpenSearch') as caught:
        executor.execute(run, set())
    assert caught.value.category == 'OPENSEARCH_CONFIG' and 'test-secret' not in str(caught.value)


def test_exact_opensearch_query_document_cluster_filter_and_sort():
    from ingestion_layer.opensearch_source import OpenSearchSource
    from ingestion_layer.opensearch_config import OpenSearchConfig, OpenSearchFieldMapping
    queries = []
    config = OpenSearchConfig(('https://fake.invalid',), 'fake', 'secret', True, True, 'logs-*', 1, 1,
                              'profile', OpenSearchFieldMapping(), 100)
    client = SimpleNamespace(config=config, post_json=lambda path, query: queries.append(query) or
                              {'timed_out': False, '_shards': {'failed': 0}, 'hits': {'hits': []}})
    source = OpenSearchSource(client)
    source.read_page(start=BASE, end=BASE + timedelta(minutes=15), namespace='ns', workload='app', cluster_id=DOCUMENT_UUID)
    filters = queries[0]['query']['bool']['filter']
    assert filters[0] == {'range': {'@timestamp': {'gte': BASE.isoformat(), 'lt': (BASE + timedelta(minutes=15)).isoformat()}}}
    assert {'term': {'openshift.cluster_id.keyword': DOCUMENT_UUID}} in filters
    assert {'term': {'kubernetes.labels.app.keyword': 'app'}} in filters
    assert queries[0]['sort'] == [{'@timestamp': 'asc'}, {'openshift.sequence': 'asc'}]


def test_explicit_environment_loading_keeps_process_precedence(monkeypatch, tmp_path):
    from application_environment import load_environment
    path = tmp_path / '.env'
    path.write_text('OPENSEARCH_SOURCE_SCOPE=from-file\nOPENSEARCH_INDEX=logs-test\n', encoding='utf-8')
    monkeypatch.setenv('OPENSEARCH_SOURCE_SCOPE', 'from-process')
    monkeypatch.delenv('OPENSEARCH_INDEX', raising=False)
    load_environment(path)
    import os
    assert os.environ['OPENSEARCH_SOURCE_SCOPE'] == 'from-process'
    assert os.environ['OPENSEARCH_INDEX'] == 'logs-test'


def test_exclusive_worker_lock_releases_on_exit(tmp_path):
    from monitoring.runtime import exclusive_worker
    path = tmp_path / 'worker.lock'
    with exclusive_worker(path):
        with pytest.raises(OSError):
            with exclusive_worker(path):
                pytest.fail('A second worker must not obtain ownership')
    with exclusive_worker(path):
        pass


def test_overlap_does_not_inflate_real_downstream_signal_counts(repo, pipeline):
    from downstream_pipeline import DownstreamAIOpsPipeline
    from monitoring.worker import MonitorWorker
    pipeline.downstream = DownstreamAIOpsPipeline(use_ai_rca=False)
    records = [record('a', 890, 'ERROR: database failure'), record('b', 903, '    at continuation'),
               record('c', 910, 'ERROR: database failure'), record('d', 920, 'ERROR: database failure')]
    monitor = repo.create(definition(), enabled=True, now=BASE)
    executor, _ = executor_for(pipeline, records)
    assert MonitorWorker(repo, executor, clock=lambda: BASE + timedelta(minutes=32), log=lambda *a, **k: None).tick(max_runs=2) == 2
    results = [repo.result(r.id) for r in repo.history(monitor.id)]
    assert sum(s['count'] for result in results for s in result['signals']) == 3


def test_worker_output_and_persisted_projection_exclude_legacy_secrets(repo, pipeline, monkeypatch, capsys):
    import logging
    from monitoring.worker import MonitorWorker
    monkeypatch.setenv('SAKA_API_KEY', 'synthetic-ai-key')
    monkeypatch.setenv('RCA_DEBUG', 'true')
    original = pipeline.process_ingested_pages
    def process(*args, **kwargs):
        print('Authorization: Bearer synthetic-ai-key; raw private log')
        logging.getLogger('legacy').error('password=test-password')
        result = original(*args, **kwargs)
        result['case_analysis'] = {'description': 'synthetic-ai-key test-password', 'synthetic-ai-key': 'untrusted field name'}
        return result
    pipeline.process_ingested_pages = process
    namespace = 'test-password Authorization: Bearer synthetic-ai-key'
    monitor = repo.create(definition(namespace=namespace), enabled=True, now=BASE)
    records = [record('a', 1, 'ERROR: failure'), record('b', 2, 'ERROR: failure')]
    records = [replace(row, stream_identity=replace(row.stream_identity, namespace=namespace)) for row in records]
    executor, _ = executor_for(pipeline, records)
    previous_disable = logging.root.manager.disable
    assert MonitorWorker(repo, executor, clock=lambda: BASE + timedelta(minutes=17)).tick() == 1
    captured = capsys.readouterr()
    stored = str(repo.result(repo.history(monitor.id)[0].id))
    for secret in ('Authorization', 'synthetic-ai-key', 'test-password', 'raw private log'):
        assert secret not in captured.out + captured.err + stored
    assert '[MONITOR]' in captured.out and 'SUCCESS' in captured.out
    assert '[redacted]' in repo.result(repo.history(monitor.id)[0].id)['source_summary']['namespace']
    assert logging.root.manager.disable == previous_disable


def test_acquisition_page_bound_applies_before_pipeline_and_uses_original_cursor(repo):
    from monitoring.errors import MonitoringError
    pages = [page([record(str(i), i, 'ERROR: failure')], exhausted=False, cursor='cursor-' + str(i)) for i in range(3)]
    def source(query):
        return pages.pop(0)
    executor, queries = executor_for(SimpleNamespace(process_ingested_pages=lambda *a, **k: pytest.fail('No partial learning')), source)
    repo.create(definition(max_pages=3, page_size=1), enabled=True, now=BASE)
    with pytest.raises(MonitoringError) as caught:
        executor.execute(repo.claim(BASE + timedelta(minutes=17)), set())
    assert caught.value.category == 'ACQUISITION_LIMIT'
    assert [q['cursor'] for q in queries] == [None, 'cursor-0', 'cursor-1']
    assert len(queries) == 3


def test_cli_once_is_independent_and_disabled_monitors_never_construct_pipeline(repo, monkeypatch):
    import importlib.util
    path = Path(__file__).resolve().parents[2] / 'tools' / 'monitor_worker.py'
    spec = importlib.util.spec_from_file_location('monitor_cli_test', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monitor = repo.create(definition(), now=BASE)
    monkeypatch.setenv('AIOPS_MONITOR_DB', str(repo.path))
    monkeypatch.setattr(module, 'load_environment', lambda: None)
    monkeypatch.setattr(module, 'system_clock', lambda: BASE + timedelta(minutes=17))
    monkeypatch.setattr(module.time, 'sleep', lambda *a: pytest.fail('No sleeps in once mode'))
    monkeypatch.setattr(module, 'pipeline', lambda: pytest.fail('Disabled monitor must not construct pipeline'))
    assert module.main(['--once']) == 0
    assert repo.history(monitor.id) == []


def test_receipts_remain_bounded_to_overlap_horizon(repo):
    from monitoring.domain import RunCounts
    monitor = repo.create(definition(), enabled=True, now=BASE)
    for index in range(5):
        run = repo.claim(BASE + timedelta(hours=2))
        receipts = {f'{index}-old': run.window.start.isoformat(),
                    f'{index}-boundary': (run.window.end - timedelta(seconds=10)).isoformat()}
        repo.succeed(run, {}, RunCounts(), receipts, now=BASE + timedelta(hours=2))
        with sqlite3.connect(repo.path) as db:
            assert db.execute('SELECT count(*) FROM monitor_receipts').fetchone()[0] == 1
    assert len(repo.history(monitor.id)) == 5


@pytest.mark.parametrize('case', ['learning_store', 'future_version'])
def test_schema_initialization_refuses_unrelated_or_newer_state(tmp_path, case):
    from monitoring.repository import SQLiteMonitorRepository
    path = tmp_path / 'existing.sqlite3'
    with sqlite3.connect(path) as db:
        if case == 'learning_store':
            db.execute('CREATE TABLE policies (value TEXT)')
            db.execute("INSERT INTO policies VALUES ('preserve-me')")
        else:
            db.execute('PRAGMA user_version=999')
    before = path.read_bytes()
    with pytest.raises(ValueError):
        SQLiteMonitorRepository(path)
    assert path.read_bytes() == before


@pytest.mark.parametrize('start', [BASE.replace(hour=0), BASE.replace(hour=23, minute=45)])
@pytest.mark.parametrize('shard_failure', [False, True])
def test_worker_daily_indices_use_both_overlaps_and_preserve_profile(repo, pipeline, start, shard_failure):
    from ingestion_layer.opensearch_config import OpenSearchConfig, OpenSearchFieldMapping
    from monitoring.execution import OpenSearchMonitorExecutor
    from monitoring.worker import MonitorWorker
    config = OpenSearchConfig(('https://synthetic.invalid',), 'reader', 'synthetic-password', True, True,
                              'daily-cluster*', 1, 1, 'test-profile', OpenSearchFieldMapping(), 100, index_strategy='daily_utc')
    calls = []
    class Client:
        def __init__(self, supplied):
            self.config = supplied
        def __enter__(self):
            return self
        def __exit__(self, *args):
            pass
        def post_json(self, path, query, *, params=None):
            if path.endswith('/_field_caps'):
                fields = params['fields'].split(',')
                return {'fields': {name: {'keyword': {'searchable': True, 'aggregatable': True}}
                                   for name in fields if name.endswith('.keyword')}}
            calls.append((path, query))
            return {'timed_out': False, '_shards': {'failed': int(shard_failure)}, 'hits': {'hits': []}}
    executor = OpenSearchMonitorExecutor(lambda: pipeline, connection_loader=lambda size: config, client_factory=Client,
                                         policy_loader=lambda: pytest.fail('Empty acquisition must not read policies'))
    monitor = repo.create(definition(initial_start=start, container='main'), enabled=True, now=start)
    assert MonitorWorker(repo, executor, clock=lambda: start + timedelta(minutes=17), log=lambda *a, **k: None).tick() == int(not shard_failure)
    expected = ('daily-cluster-2026.10.04,daily-cluster-2026.10.05' if start.hour == 0
                else 'daily-cluster-2026.10.05,daily-cluster-2026.10.06')
    path, query = calls[0]
    assert len(calls) == 1 and path == '/' + expected + '/_search'
    assert query['query']['bool']['filter'] == [
        {'range': {'@timestamp': {'gte': (start - timedelta(seconds=60)).isoformat(),
                                 'lt': (start + timedelta(minutes=16)).isoformat()}}},
        {'term': {'kubernetes.namespace_name.keyword': 'ns'}},
        {'term': {'kubernetes.labels.app.keyword': 'app'}},
        {'term': {'kubernetes.container_name.keyword': 'main'}},
    ]
    assert config.index_expression == 'daily-cluster*'  # immutable profile configuration
    assert repo.get(monitor.id).definition == monitor.definition
    run = repo.history(monitor.id)[0]
    assert run.definition.source_profile == 'test-profile'
    if shard_failure:
        assert run.status.value == 'FAILED' and run.error_category == 'OPENSEARCH_QUERY'
        assert repo.get(monitor.id).last_successful_end is None
        assert repo.result(run.id) is None
    else:
        summary = repo.result(run.id)['source_summary']
        assert summary['resolved_index'] == expected and summary['index_expression'] == 'daily-cluster*'
        assert summary['index_strategy'] == 'daily_utc'
        assert (summary['namespace'], summary['workload'], summary['container']) == ('ns', 'app', 'main')
        assert repo.get(monitor.id).last_successful_end == start + timedelta(minutes=15)


@pytest.fixture
def source_scope_executor(pipeline):
    """Real query builder and hit mapping; fake transport applies document filters."""
    from ingestion_layer.opensearch_config import OpenSearchConfig, OpenSearchFieldMapping
    from monitoring.execution import OpenSearchMonitorExecutor
    from opensearch_application import VerifiedPolicy
    from segmentation_layer.contracts import SegmentationPolicy
    config = OpenSearchConfig(('https://synthetic.invalid',), 'synthetic-reader', 'synthetic-password', True, True,
                              'gocpbmgpup1*', 1, 1, 'gocpbmgpup1', OpenSearchFieldMapping(), 100,
                              index_strategy='daily_utc')
    document_id = DOCUMENT_UUID
    scope = {'openshift.cluster_id.keyword': document_id,
             'kubernetes.namespace_name.keyword': 'ai-voice',
             'kubernetes.labels.app.keyword': 'aihub-foya-stt-apis-http',
             'kubernetes.container_name.keyword': 'aihub-foya-stt-apis-http'}
    calls, hits = [], []
    start = BASE.replace(hour=21, minute=38)
    for sequence in (1, 2):
        timestamp = (start + timedelta(seconds=sequence)).isoformat()
        hits.append({'_index': 'gocpbmgpup1-2026.10.05', '_id': str(sequence), 'sort': [timestamp, sequence],
                     '_source': {'@timestamp': timestamp, 'message': 'ERROR: synthetic failure',
                                 'openshift': {'cluster_id': document_id, 'sequence': sequence},
                                 'kubernetes': {'namespace_name': 'ai-voice', 'labels': {'app': 'aihub-foya-stt-apis-http'},
                                                'pod_name': 'synthetic-pod', 'pod_id': 'synthetic-pod-id',
                                                'container_name': 'aihub-foya-stt-apis-http',
                                                'container_id': 'synthetic-container-id', 'container_iostream': 'stdout'}}})
    class Client:
        def __init__(self, supplied):
            assert supplied is config
            self.config = supplied
        def __enter__(self):
            return self
        def __exit__(self, *args):
            pass
        def post_json(self, path, query, *, params=None):
            if path.endswith('/_field_caps'):
                fields = params['fields'].split(',')
                return {'fields': {name: {'keyword': {'searchable': True, 'aggregatable': True}}
                                   for name in fields if name.endswith('.keyword')}}
            calls.append((path, query))
            matches = all(scope[key] == value for item in query['query']['bool']['filter']
                          for key, value in item.get('term', {}).items())
            return {'timed_out': False, '_shards': {'failed': 0}, 'hits': {'hits': hits if matches else []}}
    policy = VerifiedPolicy('policy-safe', SegmentationPolicy('id', r'^ERROR:', 'verified-fixture'))
    executor = OpenSearchMonitorExecutor(lambda: pipeline, connection_loader=lambda size: config, client_factory=Client,
                                         policy_loader=lambda: (policy,))
    monitor_definition = definition(source_profile=config.source_scope, cluster_id=config.source_scope,
                                    namespace='ai-voice', workload='aihub-foya-stt-apis-http',
                                    container='aihub-foya-stt-apis-http', initial_start=start)
    return SimpleNamespace(executor=executor, calls=calls, hits=hits, definition=monitor_definition,
                           document_id=document_id, config=config)


@pytest.mark.parametrize('explicit_uuid', [False, True])
def test_worker_source_alias_does_not_filter_document_uuid_and_advances_window(
        repo, pipeline, source_scope_executor, explicit_uuid, capsys):
    from monitoring.worker import MonitorWorker
    fixture = source_scope_executor
    configured = replace(fixture.definition, document_cluster_id=fixture.document_id) if explicit_uuid else fixture.definition
    monitor = repo.create(configured, enabled=True, now=BASE)
    scheduler = MonitorWorker(repo, fixture.executor, clock=lambda: fixture.definition.initial_start + timedelta(minutes=17))
    completed = scheduler.tick()
    path, query = fixture.calls[0]
    assert path == '/gocpbmgpup1-2026.10.05/_search'
    expected_filters = [
        {'range': {'@timestamp': {'gte': '2026-10-05T21:37:00+00:00', 'lt': '2026-10-05T21:54:00+00:00'}}},
        {'term': {'kubernetes.namespace_name.keyword': 'ai-voice'}},
        {'term': {'kubernetes.labels.app.keyword': 'aihub-foya-stt-apis-http'}},
        {'term': {'kubernetes.container_name.keyword': 'aihub-foya-stt-apis-http'}},
    ]
    if explicit_uuid:
        expected_filters.append({'term': {'openshift.cluster_id.keyword': fixture.document_id}})
    assert query['query']['bool']['filter'] == expected_filters
    run = repo.history(monitor.id)[0]
    assert completed == 1 and run.status.value == 'SUCCESS'
    assert run.counts.events_retrieved == 2 and run.counts.parsed_events == 2
    assert len(pipeline.rows) == 2
    assert repo.get(monitor.id).last_successful_end == fixture.definition.initial_start + timedelta(minutes=15)
    summary = repo.result(run.id)['source_summary']
    expected_scope = dict(source_scope='gocpbmgpup1', index_expression='gocpbmgpup1*',
                          resolved_index='gocpbmgpup1-2026.10.05',
                          retrieval_start='2026-10-05T21:37:00+00:00', retrieval_end='2026-10-05T21:54:00+00:00',
                          namespace='ai-voice', workload='aihub-foya-stt-apis-http', container='aihub-foya-stt-apis-http',
                          document_cluster_id=fixture.document_id if explicit_uuid else None)
    assert {key: summary[key] for key in expected_scope} == expected_scope
    assert summary['cluster_alias'] == 'gocpbmgpup1' and summary['source_profile'] == 'gocpbmgpup1'
    assert 'cluster_id' not in summary  # no ambiguous identity in new diagnostics
    captured = capsys.readouterr()
    logs = [json.loads(line.removeprefix('[MONITOR] ')) for line in captured.out.splitlines()]
    acquired = next(row for row in logs if row['event'] == 'ACQUIRED')
    assert {key: acquired[key] for key in expected_scope} == expected_scope
    stored_and_logged = str(summary) + captured.out + captured.err
    for secret in ('synthetic-reader', 'synthetic-password', 'synthetic.invalid', 'Authorization'):
        assert secret not in stored_and_logged
    assert 'headers' not in summary and all('headers' not in row for row in logs)


def test_returned_document_uuid_is_not_validated_against_logical_alias(repo, pipeline):
    records = [record(str(i), i, 'ERROR: synthetic failure') for i in (1, 2)]
    records = [replace(row, stream_identity=replace(row.stream_identity, source_scope='document-uuid')) for row in records]
    repo.create(definition(), enabled=True, now=BASE)
    executor, _ = executor_for(pipeline, records)
    result = executor.execute(repo.claim(BASE + timedelta(minutes=17)), set())
    assert result.counts.logical_events == 2


def test_source_profile_mismatch_fails_before_acquisition(repo, source_scope_executor):
    from monitoring.worker import MonitorWorker
    fixture = source_scope_executor
    monitor = repo.create(replace(fixture.definition, source_profile='other-profile'), enabled=True, now=BASE)
    assert MonitorWorker(repo, fixture.executor, clock=lambda: BASE + timedelta(days=1), log=lambda *a, **k: None).tick() == 0
    assert fixture.calls == []
    assert repo.history(monitor.id)[0].error_category == 'OPENSEARCH_CONFIG'
    assert repo.get(monitor.id).last_successful_end is None


@pytest.mark.parametrize('mismatch', ['source_profile', 'document_cluster_id', 'namespace', 'workload', 'container'])
def test_out_of_scope_returned_records_fail_without_watermark(repo, mismatch):
    from monitoring.worker import MonitorWorker
    row = record('a', 1, 'ERROR: failure')
    row = replace(row, stream_identity=replace(row.stream_identity, source_scope=DOCUMENT_UUID))
    if mismatch == 'source_profile':
        row = replace(row, source_reference=replace(row.source_reference, source_scope='wrong-profile'))
    else:
        field = 'source_scope' if mismatch == 'document_cluster_id' else mismatch
        row = replace(row, stream_identity=replace(row.stream_identity, **{field: 'wrong-value'}))
    def no_pipeline(*args, **kwargs):
        pytest.fail('Out-of-scope acquisition must fail before the pipeline')
    executor, _ = executor_for(SimpleNamespace(process_ingested_pages=no_pipeline), [row])
    monitor = repo.create(definition(container='main', document_cluster_id=DOCUMENT_UUID), enabled=True, now=BASE)
    assert MonitorWorker(repo, executor, clock=lambda: BASE + timedelta(minutes=17), log=lambda *a, **k: None).tick() == 0
    run = repo.history(monitor.id)[0]
    assert run.status.value == 'FAILED' and run.error_category == 'OPENSEARCH_QUERY'
    assert repo.result(run.id) is None and repo.get(monitor.id).last_successful_end is None


def test_correctly_scoped_quiet_window_succeeds_and_advances_watermark(repo, pipeline, source_scope_executor):
    from monitoring.worker import MonitorWorker
    fixture = source_scope_executor
    fixture.hits.clear()
    fixture.executor.policy_loader = lambda: pytest.fail('Quiet windows need no policy')
    monitor = repo.create(fixture.definition, enabled=True, now=BASE)
    assert MonitorWorker(repo, fixture.executor, clock=lambda: BASE + timedelta(days=1), log=lambda *a, **k: None).tick() == 1
    run = repo.history(monitor.id)[0]
    assert run.status.value == 'SUCCESS' and run.counts.events_retrieved == 0
    assert repo.result(run.id)['source_summary']['document_cluster_id'] is None
    assert repo.result(run.id)['source_summary']['boundary_quality']['counts'] == dict(
        complete=0, possible_incomplete=0, confirmed_truncated=0)
    assert repo.get(monitor.id).last_successful_end == run.window.end
    assert pipeline.rows == []


def test_legacy_monitor_json_keeps_alias_and_history_without_migration(repo, source_scope_executor):
    from monitoring.repository import SQLiteMonitorRepository
    from monitoring.worker import MonitorWorker
    fixture = source_scope_executor
    monitor = repo.create(fixture.definition, enabled=True, now=BASE)
    historical = worker(repo, [BASE + timedelta(days=1)])[0]
    assert historical.tick() == 1
    run = repo.history(monitor.id)[0]
    with sqlite3.connect(repo.path) as db:
        legacy = json.loads(db.execute('SELECT definition FROM monitors WHERE id=?', (monitor.id,)).fetchone()[0])
        legacy.pop('document_cluster_id')
        payload = json.dumps(legacy)
        db.execute('UPDATE monitors SET definition=? WHERE id=?', (payload, monitor.id))
        db.execute('UPDATE monitor_runs SET definition=? WHERE id=?', (payload, run.id))
        history_before = db.execute('SELECT * FROM monitor_runs WHERE id=?', (run.id,)).fetchone()
        result_before = db.execute('SELECT * FROM monitor_results WHERE run_id=?', (run.id,)).fetchone()
    reopened = SQLiteMonitorRepository(repo.path)
    loaded = reopened.get(monitor.id)
    assert loaded.definition.cluster_id == loaded.definition.cluster_alias == 'gocpbmgpup1'
    assert loaded.definition.source_profile == 'gocpbmgpup1' and loaded.definition.document_cluster_id is None
    assert reopened.history(monitor.id)[0].definition == loaded.definition
    reopened.update(monitor.id, replace(loaded.definition, name='renamed'), revision=loaded.revision, now=BASE)
    with sqlite3.connect(repo.path) as db:
        assert db.execute('PRAGMA user_version').fetchone()[0] == 4
        assert db.execute('SELECT * FROM monitor_runs WHERE id=?', (run.id,)).fetchone() == history_before
        assert db.execute('SELECT * FROM monitor_results WHERE run_id=?', (run.id,)).fetchone() == result_before
    # A separate legacy definition that has never run uses the corrected scope.
    new = repo.create(fixture.definition, enabled=True, now=BASE)
    repo.set_enabled(monitor.id, False, now=BASE)
    with sqlite3.connect(repo.path) as db:
        db.execute('UPDATE monitors SET definition=? WHERE id=?', (payload, new.id))
    assert MonitorWorker(reopened, fixture.executor, clock=lambda: BASE + timedelta(days=1), log=lambda *a, **k: None).tick() == 1
    assert reopened.history(new.id)[0].counts.events_retrieved == 2


def test_aida_keyword_base_mapping_worker_acquires_known_document_and_persists_query(repo, pipeline):
    from test_monitor_target import KeywordClient
    from monitoring.execution import OpenSearchMonitorExecutor
    from monitoring.worker import MonitorWorker
    from opensearch_application import VerifiedPolicy
    from segmentation_layer.contracts import SegmentationPolicy
    class Client(KeywordClient):
        def __init__(self, config):
            super().__init__()
            self.config = config
        def __enter__(self):
            return self
        def __exit__(self, *args):
            pass
        def post_json(self, path, query, *, params=None):
            from copy import deepcopy
            payload = super().post_json(path, query, params=params)
            if path.endswith('/_search') and payload['hits']['hits']:
                second = deepcopy(payload['hits']['hits'][0])
                second['_id'] = 'second-header-for-policy-validation'
                second['_source']['@timestamp'] = '2026-10-07T06:15:52Z'
                second['_source']['openshift']['sequence'] = 124
                second['sort'] = [1791353752000, 124]
                payload['hits']['hits'].append(second)
            return payload
    config = KeywordClient().config
    start = datetime(2026, 10, 7, 6, 1, tzinfo=timezone.utc)
    monitor = repo.create(definition(source_profile='gocpbmgpup1', cluster_id='gocpbmgpup1',
                                    namespace='ai-document-assistant', workload='aida-agent-http',
                                    initial_start=start), enabled=True, now=start)
    executor = OpenSearchMonitorExecutor(lambda: pipeline, connection_loader=lambda size: config,
                                        client_factory=Client, policy_loader=lambda: (
                                            VerifiedPolicy('info', SegmentationPolicy('info', r'^INFO', 'fixture')),))
    completed = MonitorWorker(repo, executor, clock=lambda: start + timedelta(minutes=17), log=lambda *a, **k: None).tick()
    assert completed == 1, repo.history(monitor.id)[0].error_category
    run = repo.history(monitor.id)[0]
    assert run.status.value == 'SUCCESS' and run.counts.events_retrieved == 2
    assert run.counts.logical_events == 2 and run.counts.parsed_events == 2
    assert repo.get(monitor.id).last_successful_end == start + timedelta(minutes=15)
    summary = repo.result(run.id)['source_summary']
    assert summary['resolved_index'] == 'gocpbmgpup1-2026.10.07'
    assert summary['document_cluster_filter_active'] is False
    assert summary['sort_fields'] == ['@timestamp', 'openshift.sequence']
    assert summary['effective_query']['bool']['filter'][1:] == [
        {'term': {'kubernetes.namespace_name': 'ai-document-assistant'}},
        {'term': {'kubernetes.labels.app': 'aida-agent-http'}}]
    assert summary['pages_read'] == 1 and summary['records_read'] == 2
    assert summary['budget_reached'] is False
    assert 'fixture-password' not in str(summary) and 'fixture-reader' not in str(summary)
