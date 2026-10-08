"""Compact monitoring persistence and isolated dedupe regressions."""

from datetime import datetime, timedelta, timezone
from pathlib import Path
import sqlite3
import sys

import pytest


BACKEND = Path(__file__).resolve().parents[2] / 'src' / 'backend'
sys.path.insert(0, str(BACKEND))

from monitoring.domain import MonitorDefinition, RunCounts
from monitoring.repository import SQLiteMonitorRepository


BASE = datetime(2026, 10, 7, 21, 0, tzinfo=timezone.utc)


def definition():
    return MonitorDefinition('synthetic', 'test-profile', 'cluster', 'namespace', 'workload',
                             BASE, window_seconds=300, interval_seconds=300)


def test_read_connection_has_no_writer_transaction_and_constructor_can_skip_ddl(tmp_path):
    path = tmp_path / 'monitor.sqlite3'
    writer = SQLiteMonitorRepository(path, compact_mode=True)
    with writer._read_connection() as db:
        assert db.execute('PRAGMA query_only').fetchone()[0] == 1
        assert not db.in_transaction
        with pytest.raises(sqlite3.OperationalError):
            db.execute('CREATE TABLE forbidden (id INTEGER)')
    version = path.stat().st_mtime_ns
    reader = SQLiteMonitorRepository(path, initialize=False)
    assert reader.list_monitor_summaries() == []
    assert path.stat().st_mtime_ns == version
    with writer._transaction() as db:
        assert db.in_transaction
        assert db.execute('PRAGMA foreign_keys').fetchone()[0] == 1
        assert reader.list_monitor_summaries() == []
    with sqlite3.connect(path) as db:
        assert db.execute('PRAGMA journal_mode').fetchone()[0].lower() == 'wal'
        assert db.execute('PRAGMA user_version').fetchone()[0] == 5


def test_future_schema_is_rejected_on_ui_read_path(tmp_path):
    path = tmp_path / 'monitor.sqlite3'
    SQLiteMonitorRepository(path)
    with sqlite3.connect(path) as db:
        db.execute('PRAGMA user_version=99')
    with pytest.raises(ValueError):
        SQLiteMonitorRepository(path, initialize=False)


def test_compact_success_publishes_atomic_state_without_legacy_detail(tmp_path):
    repo = SQLiteMonitorRepository(tmp_path / 'monitor.sqlite3', compact_mode=True)
    monitor = repo.create(definition(), enabled=True, now=BASE)
    run = repo.claim(BASE + timedelta(minutes=7))
    counts = RunCounts(events_retrieved=7, unique_records=7, logical_events=5,
                       parsed_events=5, template_count=1, signal_candidates=1,
                       qualified_signals=1, incidents=1)
    result = {'raw': 'raw-customer-secret',
              'incidents': [{'incident_id': 'incident-1', 'severity': 'ERROR'}],
              'signals': [{'signal_id': 'signal-1', 'qualified': True,
                           'monitor_anomaly': {'kind': 'total_volume', 'rule': 'median_mad_and_ratio'},
                           'template_id': 'template-1', 'count': 7}]}
    metrics = {'total_physical_logs': 7, 'severity_counts': {'ERROR': 5},
               'pod_counts_exact': True, 'container_counts_exact': True,
               'pods': {'pod-1': 7}, 'containers': {'container-1': 7}}
    pattern = {'template_id': 'template-1', 'count': 5, 'template': 'bounded analytical label'}
    repo.succeed(run, result, counts, {}, now=BASE + timedelta(minutes=8),
                 metrics=metrics, pattern_metrics=(pattern,))
    assert repo.get(monitor.id).last_successful_end == run.window.end
    assert repo.recent_runs(monitor.id)[0].status.value == 'SUCCESS'
    summary, = repo.list_monitor_summaries()
    assert summary['physical_logs'] == 7 and summary['findings'] == 2
    assert repo.latest_finding(monitor.id)['kind'] == 'incident'
    assert repo.successful_metrics(monitor.id, run.window.end)[0]['total_physical_logs'] == 7
    assert repo.successful_patterns(monitor.id, run.window.end)[0]['template-1']['count'] == 5
    with sqlite3.connect(repo.path) as db:
        for table in ('monitor_results', 'monitor_run_metrics', 'monitor_pattern_metrics',
                      'monitor_acquisition_shards', 'monitor_run_dedupe'):
            assert db.execute(f'SELECT count(*) FROM {table}').fetchone()[0] == 0
        stored = str(db.execute('SELECT payload FROM monitor_baseline_state').fetchall()) + str(
            db.execute('SELECT * FROM monitor_findings').fetchall())
        assert 'raw-customer-secret' not in stored


def test_compact_failure_does_not_publish_baseline_findings_or_watermark(tmp_path):
    repo = SQLiteMonitorRepository(tmp_path / 'monitor.sqlite3', compact_mode=True)
    monitor = repo.create(definition(), enabled=True, now=BASE)
    run = repo.claim(BASE + timedelta(minutes=7))
    repo.fail(run, 'PIPELINE', now=BASE + timedelta(minutes=8),
              diagnostics={'pipeline_stage': 'parsing', 'exception_type': 'ValueError',
                           'effective_query': {'secret': 'synthetic-private-query'}})
    assert repo.get(monitor.id).last_successful_end is None
    assert repo.successful_metrics(monitor.id, run.window.end) == []
    assert repo.latest_finding(monitor.id) is None
    assert repo.recent_runs(monitor.id)[0].status.value == 'FAILED'
    assert 'synthetic-private-query' not in str(repo.acquisition_diagnostics(run.id))


def test_isolated_ledger_spills_exactly_without_shared_db_rows(tmp_path, monkeypatch):
    from monitoring import run_ledger
    monkeypatch.setattr(run_ledger, 'MAX_MEMORY_REFERENCES', 1)
    repo = SQLiteMonitorRepository(tmp_path / 'monitor.sqlite3', compact_mode=True)
    monitor = repo.create(definition(), enabled=True, now=BASE)
    run = repo.claim(BASE + timedelta(minutes=7))
    ledger = repo.acquisition_ledger(run, run.window.start)
    try:
        with ledger:
            with ledger.page():
                assert not ledger.seen('source-ref-1', run.window.end.isoformat())
                assert not ledger.seen('source-ref-2', run.window.end.isoformat())
                assert ledger.seen('source-ref-1', run.window.end.isoformat())
                ledger.mark_owned(('source-ref-1',))
        assert list(ledger.items()) == [('source-ref-1', run.window.end.isoformat())]
        with sqlite3.connect(repo.path) as db:
            assert db.execute('SELECT count(*) FROM monitor_run_dedupe').fetchone()[0] == 0
    finally:
        temporary_path = Path(ledger.temp.name) if ledger.temp is not None else None
        ledger.cleanup()
        if temporary_path is not None:
            assert not temporary_path.exists()


def test_maintenance_preserves_control_and_baseline_state(tmp_path):
    repo = SQLiteMonitorRepository(tmp_path / 'monitor.sqlite3', compact_mode=True)
    monitor = repo.create(definition(), enabled=True, now=BASE)
    run = repo.claim(BASE + timedelta(minutes=7))
    repo.succeed(run, {}, RunCounts(), {}, now=BASE + timedelta(minutes=8),
                 metrics={'total_physical_logs': 0}, pattern_metrics=())
    details = repo.maintenance(BASE + timedelta(days=8), batch=10, optimize=True)
    assert details['runs_removed'] == 1
    assert repo.get(monitor.id).last_successful_end == run.window.end
    assert repo.successful_metrics(monitor.id, BASE + timedelta(days=8))[0]['total_physical_logs'] == 0
    assert repo.recent_runs(monitor.id) == []


@pytest.mark.parametrize('reason', ['CONNECTION_TIMEOUT', 'HTTP_429', 'HTTP_5XX'])
def test_transient_page_failure_retries_without_partial_publication(reason):
    from ingestion_layer.contracts import SourcePage
    from ingestion_layer.opensearch_client import OpenSearchClientError
    from monitoring.sharded_acquisition import AcquisitionShard, StreamingAcquisition

    class Source:
        calls = 0

        def read_page(self, **kwargs):
            self.calls += 1
            if self.calls == 1:
                raise OpenSearchClientError('safe transport failure', reason, True)
            return SourcePage((), True, None, False, False)

    class Metrics:
        def count(self, *args):
            return 0

    source = Source()
    sleeps = []
    acquisition = StreamingAcquisition(source, Metrics(), definition(), log=lambda *a, **k: None,
                                       sleep=sleeps.append)
    assert list(acquisition.pages((AcquisitionShard(BASE, BASE + timedelta(minutes=5)),)))
    assert source.calls == 2 and sleeps == [0.1]
    assert acquisition.shards_completed == 1


def test_repeated_search_timeout_splits_exact_half_open_shards():
    from ingestion_layer.contracts import SourcePage
    from ingestion_layer.opensearch_source import OpenSearchSourceError
    from monitoring.sharded_acquisition import AcquisitionShard, StreamingAcquisition

    class Source:
        def read_page(self, *, start, end, **kwargs):
            if end - start > timedelta(minutes=3):
                raise OpenSearchSourceError('Search timed out or lacks completion evidence')
            return SourcePage((), True, None, False, False)

    class Metrics:
        def count(self, *args):
            return 0

    acquisition = StreamingAcquisition(Source(), Metrics(), definition(), log=lambda *a, **k: None,
                                       sleep=lambda delay: None)
    assert len(list(acquisition.pages((AcquisitionShard(BASE, BASE + timedelta(minutes=5)),)))) == 2
    assert acquisition.shards_completed == 2
    assert acquisition.pages_read == 2


def test_invalid_cursor_and_required_field_are_nonretryable():
    from ingestion_layer.opensearch_source import OpenSearchSourceError
    from monitoring.sharded_acquisition import AcquisitionShard, StreamingAcquisition

    assert OpenSearchSourceError('Invalid or incompatible OpenSearch cursor').reason_code == 'INVALID_CURSOR'
    missing = OpenSearchSourceError('Missing or non-string message')
    assert missing.reason_code == 'MISSING_REQUIRED_FIELD' and not missing.retryable

    class Source:
        calls = 0

        def read_page(self, **kwargs):
            self.calls += 1
            raise OpenSearchSourceError('Invalid or incompatible OpenSearch cursor')

    class Metrics:
        def count(self, *args):
            return 0

    source = Source()
    acquisition = StreamingAcquisition(source, Metrics(), definition(), log=lambda *a, **k: None,
                                       sleep=lambda delay: pytest.fail('No retry is allowed'))
    with pytest.raises(OpenSearchSourceError):
        list(acquisition.pages((AcquisitionShard(BASE, BASE + timedelta(minutes=5)),)))
    assert source.calls == 1
    assert acquisition.last_state['failure_category'] == 'INVALID_CURSOR'


def test_nonretryable_failure_blocks_same_window_after_three_attempts(tmp_path):
    repo = SQLiteMonitorRepository(tmp_path / 'monitor.sqlite3', compact_mode=True)
    monitor = repo.create(definition(), enabled=True, now=BASE)
    for attempt in range(1, 4):
        now = BASE + timedelta(minutes=7 * attempt)
        run = repo.claim(now)
        assert run is not None and run.attempts == attempt
        repo.fail(run, 'OPENSEARCH_QUERY', now=now,
                  diagnostics={'reason_code': 'INVALID_CURSOR', 'source_profile': 'synthetic'})
        assert repo.get(monitor.id).last_successful_end is None
        assert repo.successful_metrics(monitor.id, now) == []
        assert repo.latest_finding(monitor.id) is None
    blocked = repo.get(monitor.id)
    assert blocked.status.value == 'BLOCKED'
    assert repo.list_monitor_summaries()[0]['safe_reason_code'] == 'INVALID_CURSOR'
    assert repo.claim(BASE + timedelta(minutes=30)) is None
    retry = repo.claim(blocked.next_run_at)
    assert retry.id == run.id and retry.window == run.window and retry.attempts == 4
    repo.succeed(retry, {}, RunCounts(), {}, now=blocked.next_run_at,
                 metrics={'total_physical_logs': 0}, pattern_metrics=())
    assert repo.get(monitor.id).status.value == 'ACTIVE'
    assert repo.get(monitor.id).last_successful_end == retry.window.end


def test_legacy_attempt_count_does_not_shortcut_new_reason_streak(tmp_path):
    repo = SQLiteMonitorRepository(tmp_path / 'monitor.sqlite3', compact_mode=True)
    monitor = repo.create(definition(), enabled=True, now=BASE)
    run = repo.claim(BASE + timedelta(minutes=7))
    with repo._transaction() as db:
        db.execute('UPDATE monitor_runs SET attempts=63 WHERE id=?', (run.id,))
    for number in range(1, 4):
        now = BASE + timedelta(minutes=7 * number)
        if number > 1:
            run = repo.claim(now)
        repo.fail(run, 'OPENSEARCH_QUERY', now=now,
                  diagnostics={'reason_code': 'INVALID_RESPONSE', 'error_stage': 'content_acquisition'})
        assert repo.get(monitor.id).status.value == ('BLOCKED' if number == 3 else 'ERROR')
        with repo._read_connection() as db:
            row = db.execute('SELECT safe_reason_code,error_stage,reason_streak FROM monitor_runs WHERE id=?',
                             (run.id,)).fetchone()
        assert tuple(row) == ('INVALID_RESPONSE', 'content_acquisition', number)
    assert repo.get(monitor.id).last_successful_end is None


def test_reason_change_restarts_blocking_streak_and_success_clears_it(tmp_path):
    repo = SQLiteMonitorRepository(tmp_path / 'monitor.sqlite3', compact_mode=True)
    monitor = repo.create(definition(), enabled=True, now=BASE)
    for number, reason in enumerate(('INVALID_CURSOR', 'INVALID_CURSOR',
                                     'INVALID_RESPONSE', 'INVALID_RESPONSE',
                                     'INVALID_RESPONSE'), 1):
        now = BASE + timedelta(minutes=7 * number)
        run = repo.claim(now)
        repo.fail(run, 'OPENSEARCH_QUERY', now=now, diagnostics={'reason_code': reason})
        with repo._read_connection() as db:
            streak = db.execute('SELECT reason_streak FROM monitor_runs WHERE id=?', (run.id,)).fetchone()[0]
        assert streak == (number if number <= 2 else number - 2)
        assert repo.get(monitor.id).status.value == ('BLOCKED' if number == 5 else 'ERROR')
    blocked = repo.get(monitor.id)
    assert blocked.next_run_at >= now + timedelta(hours=1)
    assert repo.claim(now + timedelta(minutes=10)) is None
    retry = repo.claim(blocked.next_run_at)
    assert retry.id == run.id and retry.window == run.window
    repo.succeed(retry, {}, RunCounts(), {}, now=blocked.next_run_at,
                 metrics={'total_physical_logs': 0}, pattern_metrics=())
    assert repo.get(monitor.id).status.value == 'ACTIVE'
    assert repo.get(monitor.id).last_successful_end == run.window.end
    with repo._read_connection() as db:
        assert db.execute('SELECT reason_streak FROM monitor_runs WHERE id=?', (run.id,)).fetchone()[0] == 0


@pytest.mark.parametrize('reason,retryable', [
    ('SEARCH_TIMEOUT', True), ('HTTP_429', True), ('HTTP_5XX', True),
    ('SHARD_FAILURE', True), ('INVALID_RESPONSE', False),
    ('QUERY_FAILURE_UNKNOWN', False),
])
def test_right_child_count_failure_has_safe_planning_stage(reason, retryable):
    from ingestion_layer.contracts import SourcePage
    from ingestion_layer.opensearch_client import OpenSearchClientError
    from monitoring.sharded_acquisition import AcquisitionShard, StreamingAcquisition

    events = []
    class Source:
        def read_page(self, **kwargs):
            return SourcePage((), True, None, False, False)
    class Metrics:
        right_calls = 0
        def count(self, start, end, definition):
            if start == BASE:
                return 3000 if end == BASE + timedelta(minutes=5) else 0
            if (start != BASE + timedelta(minutes=2, seconds=30) or
                    end != BASE + timedelta(minutes=5)):
                return 0
            self.right_calls += 1
            raise OpenSearchClientError('synthetic-private-response', reason, retryable)
    metrics = Metrics()
    acquisition = StreamingAcquisition(Source(), metrics, definition(),
                                       log=lambda event, **values: events.append((event, values)),
                                       sleep=lambda delay: None)
    if retryable:
        assert len(list(acquisition.pages((AcquisitionShard(BASE, BASE + timedelta(minutes=5)),)))) == 3
        assert acquisition.shards_completed == 3
        assert any(event == 'ACQUISITION_SPLIT' and values['shard_id'] == '0R'
                   for event, values in events)
    else:
        with pytest.raises(OpenSearchClientError):
            list(acquisition.pages((AcquisitionShard(BASE, BASE + timedelta(minutes=5)),)))
        assert acquisition.shards_completed == 1
        assert acquisition.last_state['shard_id'] == '0R'
        assert acquisition.last_state['error_stage'] == 'shard_planning'
        assert acquisition.last_state['failure_category'] == reason
    assert ('ACQUISITION_SHARD_COMPLETE', {'shard_id': '0L', 'pages_read': 1,
            'records_read': 0, 'unique_records': 0}) in events
    assert any(event == 'ACQUISITION_SHARD_START' and values['shard_id'] == '0R'
               for event, values in events)
    assert metrics.right_calls == (3 if retryable else 1)
    assert 'synthetic-private-response' not in str(events)


@pytest.mark.parametrize('payload,reason', [
    ({}, 'INVALID_RESPONSE'),
    ({'timed_out': False}, 'INVALID_RESPONSE'),
    ({'timed_out': False, '_shards': {'failed': 0}, 'hits': []}, 'INVALID_RESPONSE'),
    ({'timed_out': False, '_shards': {'failed': 1}}, 'SHARD_FAILURE'),
    ({'timed_out': True, '_shards': {'failed': 0}}, 'SEARCH_TIMEOUT'),
])
def test_metrics_count_classifies_structural_and_transient_responses(payload, reason):
    from ingestion_layer.opensearch_config import OpenSearchConfig, OpenSearchFieldMapping
    from ingestion_layer.opensearch_source import OpenSearchSourceError
    from monitoring.metrics_source import OpenSearchMetricsSource

    config = OpenSearchConfig(('https://synthetic.invalid',), 'user', 'password', True,
                              True, 'logs-*', 1, 1, 'test-profile', OpenSearchFieldMapping(), 100)
    class Client:
        def __init__(self):
            self.config = config
        def post_json(self, path, body, params=None):
            return payload
    with pytest.raises(OpenSearchSourceError) as captured:
        OpenSearchMetricsSource(Client()).count(BASE, BASE + timedelta(minutes=5), definition())
    assert captured.value.reason_code == reason
    assert captured.value.retryable is (reason in {'SEARCH_TIMEOUT', 'SHARD_FAILURE'})


@pytest.mark.parametrize('message,reason', [
    ('Invalid or incompatible OpenSearch cursor', 'INVALID_CURSOR'),
    ('Missing or non-string message', 'MISSING_REQUIRED_FIELD'),
    ('Missing or invalid source timestamp', 'INVALID_TIMESTAMP'),
    ('Missing or non-integer sequence', 'INVALID_SEQUENCE'),
    ('Search response must be an object', 'INVALID_RESPONSE'),
])
def test_required_source_contract_failures_have_nonretryable_safe_reasons(message, reason):
    from ingestion_layer.opensearch_source import OpenSearchSourceError
    error = OpenSearchSourceError(message)
    assert error.reason_code == reason
    assert error.retryable is False


def test_v4_history_backfills_equivalent_ordered_baseline(tmp_path):
    path = tmp_path / 'monitor.sqlite3'
    legacy = SQLiteMonitorRepository(path)
    monitor = legacy.create(definition(), enabled=True, now=BASE)
    for index in range(6):
        stamp = BASE + timedelta(minutes=7 + 5 * index)
        run = legacy.claim(stamp)
        assert run is not None
        legacy.succeed(run, {'stats': {}, 'signals': []}, RunCounts(events_retrieved=10 + index), {},
                       now=stamp,
                       metrics={'total_physical_logs': 10 + index,
                                'severity_counts': {'ERROR': index},
                                'pod_counts_exact': True, 'container_counts_exact': True,
                                'pods': {'pod': index}, 'containers': {'container': index}},
                       pattern_metrics=({'template_id': 'tpl_1', 'template': 'static',
                                         'count': index + 1},))
    before = BASE + timedelta(hours=1)
    old_metrics = legacy.successful_metrics(monitor.id, before)
    old_patterns = legacy.successful_patterns(monitor.id, before)
    with sqlite3.connect(path) as db:
        db.execute('DELETE FROM monitor_baseline_state')
        db.execute('PRAGMA user_version=4')
    migrated = SQLiteMonitorRepository(path, compact_mode=True)
    assert migrated.successful_metrics(monitor.id, before) == old_metrics
    assert migrated.successful_patterns(monitor.id, before) == old_patterns
    assert migrated.get(monitor.id).last_successful_end == legacy.get(monitor.id).last_successful_end


def test_normal_compact_success_uses_one_transaction_four_write_statements(tmp_path, monkeypatch):
    from monitoring import repository as module
    repo = SQLiteMonitorRepository(tmp_path / 'monitor.sqlite3', compact_mode=True)
    monitor = repo.create(definition(), enabled=True, now=BASE)
    run = repo.claim(BASE + timedelta(minutes=7))
    original = sqlite3.connect
    statements = []
    def traced(*args, **kwargs):
        db = original(*args, **kwargs)
        db.set_trace_callback(statements.append)
        return db
    monkeypatch.setattr(module.sqlite3, 'connect', traced)
    repo.succeed(run, {}, RunCounts(), {}, now=BASE + timedelta(minutes=8),
                 metrics={'total_physical_logs': 0}, pattern_metrics=())
    sql = [item.lstrip().upper() for item in statements]
    assert sum(item.startswith('BEGIN IMMEDIATE') for item in sql) == 1
    assert sum(item.startswith('COMMIT') for item in sql) == 1
    writes = [item for item in sql if item.startswith(('INSERT ', 'UPDATE ', 'DELETE '))]
    assert len(writes) == 4
    assert not any('MONITOR_RESULTS' in item or 'MONITOR_RUN_METRICS' in item or
                   'MONITOR_PATTERN_METRICS' in item or 'MONITOR_ACQUISITION_SHARDS' in item or
                   'MONITOR_RUN_DEDUPE' in item for item in writes)


def test_disposable_debug_template_snapshot_does_not_write_learning_files(tmp_path):
    from template_layer.template_pipeline import TemplatePipeline
    state = tmp_path / 'templates.json'
    candidate = tmp_path / 'drain.bin'
    normal = TemplatePipeline(state_path=state, candidate_state_path=candidate)
    normal.process({'message': 'Alpha request completed'})
    normal.save_state()
    before = {path: path.read_bytes() for path in (state, candidate) if path.exists()}
    snapshot = TemplatePipeline.read_only_snapshot(state, candidate)
    snapshot.process({'message': 'Beta request failed'})
    assert {path: path.read_bytes() for path in before} == before
    assert snapshot.state_path is None and snapshot.candidate_state_path is None


def test_exact_replay_repository_boundary_has_zero_monitoring_writes(tmp_path, monkeypatch):
    from monitoring import debug
    from monitoring.execution import ExecutionResult
    from monitoring import repository as module
    repo = SQLiteMonitorRepository(tmp_path / 'monitor.sqlite3', compact_mode=True)
    monitor = repo.create(definition(), enabled=True, now=BASE)
    run = repo.claim(BASE + timedelta(minutes=7))
    original = sqlite3.connect
    statements = []
    def traced(*args, **kwargs):
        db = original(*args, **kwargs)
        if Path(str(args[0])).name.startswith('monitor.sqlite3') or kwargs.get('uri'):
            db.set_trace_callback(statements.append)
        return db
    monkeypatch.setattr(module.sqlite3, 'connect', traced)

    class Executor:
        def __init__(self, pipeline_factory, **kwargs):
            assert kwargs['read_only'] is True
            self.repository = kwargs['repository']

        def execute(self, selected, consumed):
            assert selected.id == run.id and consumed == set()
            assert self.repository.successful_metrics(monitor.id, run.window.start) == []
            ledger = self.repository.acquisition_ledger(run, run.window.start)
            with ledger:
                assert not ledger.consumed('synthetic-reference')
            return ExecutionResult({'stats': {'logical_events': 0}}, RunCounts(), ledger)

    monkeypatch.setattr(debug, 'OpenSearchMonitorExecutor', Executor)
    assert debug.replay_window(repo, run) == {'stats': {'logical_events': 0}}
    sql = [item.lstrip().upper() for item in statements]
    assert not any(item.startswith(('INSERT ', 'UPDATE ', 'DELETE ', 'CREATE ', 'ALTER ',
                                    'BEGIN IMMEDIATE')) for item in sql)
    assert repo.get(monitor.id).last_successful_end is None
