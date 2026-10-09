"""Synthetic acquisition coverage; no transport, learning files, or real logs."""
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'src' / 'backend'))

from ingestion_layer.contracts import (Framing, IngestedLogRecord, SourcePage,
                                       SourceReference, StreamIdentity)
from ingestion_layer.opensearch_config import OpenSearchConfig, OpenSearchFieldMapping
from monitoring.domain import MonitorDefinition
from monitoring.metrics_source import WindowMetrics
from monitoring.repository import SQLiteMonitorRepository
from monitoring.sharded_acquisition import AcquisitionShard
from monitoring.worker import MonitorWorker


BASE = datetime(2026, 10, 9, 4, 17, tzinfo=timezone.utc)


def test_acquisition_lanes_skip_empty_overlap():
    from monitoring.execution import acquisition_lanes

    end = BASE + timedelta(minutes=5)
    window = SimpleNamespace(start=BASE, end=end)
    assert [(lane.name, lane.start, lane.end, lane.owned)
            for lane in acquisition_lanes(BASE, window, end)] == [
                ('OWNED_WINDOW', BASE, end, True)]
    assert [(lane.name, lane.owned) for lane in acquisition_lanes(
        BASE - timedelta(minutes=1), window, end + timedelta(minutes=1))] == [
            ('PRE_OVERLAP', False), ('OWNED_WINDOW', True), ('POST_OVERLAP', False)]


@pytest.mark.parametrize('consume_all,duplicate_cross_shard,recount_fails',
                         [(True, False, False), (False, False, False),
                          (True, True, False), (True, False, True), (True, True, True)])
def test_valid_records_and_incomplete_page_handoff_are_distinguished(tmp_path, monkeypatch,
                                                                      consume_all,
                                                                      duplicate_cross_shard,
                                                                      recount_fails):
    import monitoring.execution as execution

    repo = SQLiteMonitorRepository(tmp_path / 'monitor.sqlite3', compact_mode=True)
    monitor = repo.create(MonitorDefinition('synthetic', 'profile', 'alias', 'ns', 'app', BASE,
                                           window_seconds=900, interval_seconds=900),
                          enabled=True, now=BASE)
    run_end = BASE + timedelta(minutes=15)
    retrieval_start = BASE - timedelta(minutes=1)
    events = []
    recount_windows = []

    class Client:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

    class Metrics:
        requests = 1

        def __init__(self, client):
            pass

        def window(self, run):
            return WindowMetrics(36_000, (), {}, {}, 0, 0, True, True, {})

        def count(self, start, end, definition):
            if start == BASE and end == run_end:
                recount_windows.append((start, end))
                if recount_fails:
                    raise RuntimeError('synthetic-private-recount-error')
                return 36_001
            return 1200 if BASE <= start and end <= run_end else 0

    class Source:
        def __init__(self, client):
            pass

        def read_page(self, *, start, end, cursor=None, **kwargs):
            count = 1200 if BASE <= start and end <= run_end else 0
            offset = 0 if cursor is None else int(cursor)
            limit = min(offset + 100, count)
            shard_id = int((start - retrieval_start).total_seconds() // 30)
            rows = tuple(IngestedLogRecord(
                raw_text='synthetic-private-log-body',
                source_reference=SourceReference('profile', 'synthetic-index',
                                                 ('2-0' if duplicate_cross_shard and
                                                  shard_id == 3 and number == 0 else
                                                  f'{shard_id}-{number}')),
                source_timestamp_raw=(start + timedelta(microseconds=number)).isoformat(),
                stream_identity=StreamIdentity('document-cluster', namespace='ns', workload='app',
                                               pod='pod', pod_instance='pod-id', container='main',
                                               container_instance='container-id', channel='stdout'),
                retrieval_order=(start.isoformat(), number), framing=Framing.PHYSICAL_LINE,
            ) for number in range(offset, limit))
            return SourcePage(rows, interval_exhausted=len(rows) < 100,
                              next_cursor=str(limit) if len(rows) == 100 else None,
                              page_limit_reached=len(rows) == 100, cycle_budget_reached=False)

    def shards(start, end, *args):
        assert (start, end) in {
            (retrieval_start, BASE),
            (BASE, run_end),
            (run_end, run_end + timedelta(minutes=1)),
        }
        first = int((start - retrieval_start).total_seconds() // 30)
        last = int((end - retrieval_start).total_seconds() // 30)
        return tuple(AcquisitionShard(start + timedelta(seconds=30 * index),
                                      start + timedelta(seconds=30 * (index + 1)),
                                      shard_id=str(first + index)) for index in range(last - first))

    class Pipeline:
        def process_ingested_pages(self, pages, **kwargs):
            if consume_all:
                consumed = sum(len(page.records) for page in pages)
                assert consumed == 36_000 - int(duplicate_cross_shard)
            else:
                next(pages)
            return {'stats': {'segmented': 0, 'parsed': 0, 'templated': 0}}

    monkeypatch.setattr(execution, 'OpenSearchMetricsSource', Metrics)
    monkeypatch.setattr(execution, 'plan_shards', shards)
    monkeypatch.setattr(execution, 'VerifiedPolicyResolver', lambda: SimpleNamespace(
        resolve=lambda *args: SimpleNamespace(snapshot=object(), diagnostics=lambda: {})))
    config = OpenSearchConfig(('https://synthetic.invalid',), 'synthetic', 'synthetic',
                              True, True, 'logs-*', 1, 1, 'profile', OpenSearchFieldMapping(), 100)
    executor = execution.OpenSearchMonitorExecutor(
        Pipeline, connection_loader=lambda size: config, client_factory=lambda cfg: Client(),
        source_factory=Source, policy_loader=lambda: (), repository=repo,
        log=lambda event, **values: events.append((event, values)))
    worker = MonitorWorker(repo, executor, clock=lambda: BASE + timedelta(minutes=17),
                           log=lambda event, **values: events.append((event, values)))

    assert worker.tick() == int(consume_all and not duplicate_cross_shard), [
        (values.get('category'), values.get('pipeline_stage'),
         values.get('exception_type'), values.get('exception_file'), values.get('exception_line'))
        for event, values in events if event == 'FAILED']
    run = repo.history(monitor.id)[0]
    assert recount_windows == ([(BASE, run_end)] if consume_all else [])
    if consume_all and not duplicate_cross_shard:
        assert run.status.value == 'SUCCESS'
        assert (run.counts.events_retrieved, run.counts.unique_records) == (36_000, 36_000)
        assert repo.get(monitor.id).last_successful_end == run_end
        assert sum(event == 'ACQUISITION_SHARD_COMPLETE' for event, _ in events) == 34
        assert not any(event == 'FAILED' for event, _ in events)
        recount = repo.acquisition_diagnostics(run.id)
        assert recount == {'metrics_count_before': 36_000,
                           'metrics_count_after': None if recount_fails else 36_001,
                           'unique_inside_window': 36_000}
        assert 'metrics_recount' not in repo.successful_metrics(
            monitor.id, run_end + timedelta(seconds=1))[0]
    else:
        assert run.status.value == 'FAILED' and repo.get(monitor.id).last_successful_end is None
        assert repo.acquisition_diagnostics(run.id).get('validation_site') == 'PAGE_HANDOFF', (
            run.error_category, repo.acquisition_diagnostics(run.id).get('reason_code'),
            repo.acquisition_diagnostics(run.id).get('error_stage'))
        assert repo.acquisition_diagnostics(run.id)['validation_reason'] == 'COUNT_MISMATCH'
        count = repo.acquisition_diagnostics(run.id)['count_mismatch']
        assert repo.acquisition_diagnostics(run.id)['metrics_recount'] == {
            'metrics_count_before': 36_000,
            'metrics_count_after': 36_001 if consume_all and not recount_fails else None,
            'unique_inside_window': 35_999 if duplicate_cross_shard else count['unique_inside_window'],
        }
        assert set(count) == {'metrics_total', 'unique_inside_window', 'unique_total',
                              'duplicate_records', 'acquisition', 'stream_exhausted',
                              'last_shard'}
        assert set(count['acquisition']) == {'pages_read', 'records_read', 'shards_seen',
                                             'shards_completed', 'completed_unique_records'}
        assert set(count['last_shard']) == {'shard_id', 'status'}
        assert set(repo.acquisition_diagnostics(run.id)['last_shard']) == {'shard_id', 'status'}
        assert count['metrics_total'] == 36_000
        assert count['stream_exhausted'] is consume_all
        assert count['duplicate_records'] == int(duplicate_cross_shard)
        assert count['unique_total'] == count['unique_inside_window']
        if duplicate_cross_shard:
            assert count['unique_inside_window'] == 35_999
            assert count['acquisition']['completed_unique_records'] == 36_000
        else:
            assert count['unique_inside_window'] < 36_000
        assert all(type(value) is int for value in (
            count['metrics_total'], count['unique_inside_window'], count['unique_total'],
            count['duplicate_records'], *count['acquisition'].values()))
        assert 'synthetic-private-log-body' not in str(repo.acquisition_diagnostics(run.id))
        assert 'synthetic-private-log-body' not in str(events)
        assert 'synthetic-private-recount-error' not in str(repo.acquisition_diagnostics(run.id))
        assert 'synthetic-private-recount-error' not in str(events)
        assert 'next_cursor' not in str(repo.acquisition_diagnostics(run.id))
        assert any(event == 'FAILED' and values.get('validation_reason') == 'COUNT_MISMATCH'
                   for event, values in events)
        assert repo.result(run.id) is None
        retry = repo.claim(BASE + timedelta(minutes=33))
        assert retry.id == run.id and retry.window == run.window
    with repo._read_connection() as db:
        assert db.execute('SELECT COUNT(*) FROM monitor_results').fetchone()[0] == 0
        assert db.execute('SELECT COUNT(*) FROM monitor_acquisition_shards').fetchone()[0] == 0
        if not consume_all:
            assert db.execute('SELECT COUNT(*) FROM monitor_baseline_state').fetchone()[0] == 0
            assert db.execute('SELECT COUNT(*) FROM monitor_findings').fetchone()[0] == 0


def test_untrusted_validation_labels_and_raw_diagnostics_are_not_persisted(tmp_path):
    from monitoring.errors import MonitoringError

    private = 'synthetic-private-document-and-exception'
    invalid = MonitoringError('OPENSEARCH_QUERY', reason_code='INVALID_RESPONSE',
                              validation_site=private, validation_reason=private)
    assert invalid.validation_site is None and invalid.validation_reason is None

    repo = SQLiteMonitorRepository(tmp_path / 'monitor.sqlite3', compact_mode=True)
    monitor = repo.create(MonitorDefinition('synthetic', 'profile', 'alias', 'ns', 'app', BASE,
                                           window_seconds=900, interval_seconds=900),
                          enabled=True, now=BASE)
    run = repo.claim(BASE + timedelta(minutes=17))
    diagnostics = {'reason_code': 'INVALID_RESPONSE', 'error_stage': 'content_acquisition',
                   'validation_site': private, 'validation_reason': private,
                   'query': private, 'response_body': private, 'Raw_Hit': private,
                   'headers': private, 'exception_message': private, 'password': private}
    repo.fail(run, 'OPENSEARCH_QUERY', now=BASE + timedelta(minutes=17),
              diagnostics=diagnostics)
    persisted = repo.acquisition_diagnostics(run.id)
    assert persisted['reason_code'] == 'INVALID_RESPONSE'
    assert persisted['error_stage'] == 'content_acquisition'
    assert 'validation_site' not in persisted and 'validation_reason' not in persisted
    assert private not in str(persisted)
    assert repo.get(monitor.id).last_successful_end is None


@pytest.mark.parametrize('owned_count', [100, 101])
def test_exact_owned_lane_controls_count_and_preserves_overlap_context(
        tmp_path, monkeypatch, owned_count):
    import monitoring.execution as execution
    from segmentation_layer.contracts import AssembledEvent, SegmentationPolicy
    from segmentation_layer.segmentation_session import SegmentationSession

    end = BASE + timedelta(minutes=5)
    before = BASE - timedelta(minutes=1)
    after = end + timedelta(minutes=1)
    identity = StreamIdentity('document-cluster', namespace='ns', workload='app',
                              pod='pod', pod_instance='pod-id', container='main',
                              container_instance='container-id', channel='stdout')

    def row(label, number, stamp):
        header = number == 0 if label == 'pre' else number == 1
        return IngestedLogRecord(
            raw_text=('ERROR: ' if header else '  ') + 'synthetic-private-record',
            source_reference=SourceReference('profile', 'synthetic-index', f'{label}-{number}'),
            source_timestamp_raw=stamp.isoformat(), stream_identity=identity,
            retrieval_order=(label, number), framing=Framing.PHYSICAL_LINE)

    lane_rows = {
        (before, BASE): (row('pre', 0, BASE), row('pre', 1, BASE)),
        (BASE, end): tuple(row('owned', number,
                                BASE - timedelta(microseconds=1) if number == 0 else
                                BASE + timedelta(seconds=number))
                           for number in range(owned_count)),
        (end, after): (row('post', 0, end - timedelta(microseconds=1)),
                       row('post', 1, end - timedelta(microseconds=1))),
    }
    content_ranges = []
    count_ranges = []
    processed = []
    events = []

    class Client:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

    class Metrics:
        requests = 1

        def __init__(self, client):
            pass

        def window(self, run):
            return WindowMetrics(100, (), {}, {}, 0, 0, True, True, {})

        def count(self, start, stop, definition):
            count_ranges.append((start, stop))
            return len(lane_rows[(start, stop)])

    class Source:
        def __init__(self, client):
            pass

        def read_page(self, *, start, end, cursor=None, page_size=100, **kwargs):
            content_ranges.append((start, end))
            rows = lane_rows[(start, end)]
            offset = int(cursor) if cursor is not None else 0
            selected = rows[offset:offset + page_size]
            following = offset + len(selected)
            return SourcePage(selected, interval_exhausted=following == len(rows),
                              next_cursor=str(following) if following < len(rows) else None,
                              page_limit_reached=following < len(rows), cycle_budget_reached=False)

    class Pipeline:
        def process_ingested_pages(self, pages, **kwargs):
            acquired_pages = tuple(pages)
            processed.append(tuple(record for page in acquired_pages for record in page.records))
            selected = processed[-1]
            assert len(selected) == owned_count + 4
            markers = [record.metadata[-1][-1] for record in selected]
            assert markers == [False, False] + [True] * owned_count + [False, False]
            session = SegmentationSession(
                lambda *args: SegmentationPolicy('synthetic', r'^ERROR:', 'synthetic'))
            assembled = []
            for page in acquired_pages:
                assembled.extend(item for item in session.feed_page(page)
                                 if isinstance(item, AssembledEvent))
            assembled.extend(session.close())
            assert len(assembled) == 3
            assert tuple(record.source_reference.record_id for record in assembled[0].records) == (
                'pre-0', 'pre-1', 'owned-0')
            assert assembled[1].records[0].source_reference.record_id == 'owned-1'
            assert assembled[1].records[-1].source_reference.record_id == 'post-0'
            owns = kwargs['assembled_event_filter']
            assert [owns(event) for event in assembled] == [False, True, False]
            return {'stats': {'segmented': 0, 'parsed': 0, 'templated': 0}}

    monkeypatch.setattr(execution, 'OpenSearchMetricsSource', Metrics)
    monkeypatch.setattr(execution, 'VerifiedPolicyResolver', lambda: SimpleNamespace(
        resolve=lambda *args: SimpleNamespace(snapshot=object(), diagnostics=lambda: {})))
    repo = SQLiteMonitorRepository(tmp_path / 'monitor.sqlite3', compact_mode=True)
    monitor = repo.create(MonitorDefinition('synthetic', 'profile', 'alias', 'ns', 'app', BASE,
                                           window_seconds=300, interval_seconds=300,
                                           overlap_seconds=60), enabled=True, now=BASE)
    config = OpenSearchConfig(('https://synthetic.invalid',), 'synthetic', 'synthetic',
                              True, True, 'logs-*', 1, 1, 'profile', OpenSearchFieldMapping(), 100)
    executor = execution.OpenSearchMonitorExecutor(
        Pipeline, connection_loader=lambda size: config, client_factory=lambda cfg: Client(),
        source_factory=Source, policy_loader=lambda: (), repository=repo,
        log=lambda event, **values: events.append((event, values)))
    worker = MonitorWorker(repo, executor, clock=lambda: end + timedelta(minutes=2),
                           log=lambda event, **values: events.append((event, values)))

    assert worker.tick() == int(owned_count == 100)
    assert list(dict.fromkeys(content_ranges)) == list(lane_rows)
    assert count_ranges[:3] == list(lane_rows)
    assert count_ranges[-1] == (BASE, end)
    assert len(processed) == 1
    run = repo.history(monitor.id)[0]
    if owned_count == 100:
        assert run.status.value == 'SUCCESS'
        assert repo.get(monitor.id).last_successful_end == end
        assert repo.acquisition_diagnostics(run.id) == {
            'metrics_count_before': 100, 'metrics_count_after': 100,
            'unique_inside_window': 100}
    else:
        assert run.status.value == 'FAILED'
        assert repo.get(monitor.id).last_successful_end is None
        assert repo.acquisition_diagnostics(run.id)['validation_reason'] == 'COUNT_MISMATCH'
        assert repo.acquisition_diagnostics(run.id)['metrics_recount'] == {
            'metrics_count_before': 100, 'metrics_count_after': 101,
            'unique_inside_window': 101}
        with repo._read_connection() as db:
            for table in ('monitor_baseline_state', 'monitor_findings', 'monitor_receipts'):
                assert db.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0] == 0
    assert 'synthetic-private-record' not in str(repo.acquisition_diagnostics(run.id))
    assert 'synthetic-private-record' not in str(events)
