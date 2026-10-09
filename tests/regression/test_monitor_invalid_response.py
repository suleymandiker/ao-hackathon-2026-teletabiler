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


@pytest.mark.parametrize('consume_all', [True, False])
def test_valid_records_and_incomplete_page_handoff_are_distinguished(tmp_path, monkeypatch,
                                                                      consume_all):
    import monitoring.execution as execution

    repo = SQLiteMonitorRepository(tmp_path / 'monitor.sqlite3', compact_mode=True)
    monitor = repo.create(MonitorDefinition('synthetic', 'profile', 'alias', 'ns', 'app', BASE,
                                           window_seconds=900, interval_seconds=900),
                          enabled=True, now=BASE)
    run_end = BASE + timedelta(minutes=15)
    retrieval_start = BASE - timedelta(minutes=1)
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
            return WindowMetrics(36_000, (), {}, {}, 0, 0, True, True, {})

        def count(self, start, end, definition):
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
                raw_text='synthetic',
                source_reference=SourceReference('profile', 'synthetic-index',
                                                 f'{shard_id}-{number}'),
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
        assert start == retrieval_start and end == BASE + timedelta(minutes=16)
        return tuple(AcquisitionShard(start + timedelta(seconds=30 * index),
                                      start + timedelta(seconds=30 * (index + 1)),
                                      shard_id=str(index)) for index in range(34))

    class Pipeline:
        def process_ingested_pages(self, pages, **kwargs):
            if consume_all:
                consumed = sum(len(page.records) for page in pages)
                assert consumed == 36_000
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

    assert worker.tick() == int(consume_all), [
        (values.get('category'), values.get('pipeline_stage'),
         values.get('exception_type'), values.get('exception_file'), values.get('exception_line'))
        for event, values in events if event == 'FAILED']
    run = repo.history(monitor.id)[0]
    if consume_all:
        assert run.status.value == 'SUCCESS'
        assert (run.counts.events_retrieved, run.counts.unique_records) == (36_000, 36_000)
        assert repo.get(monitor.id).last_successful_end == run_end
        assert sum(event == 'ACQUISITION_SHARD_COMPLETE' for event, _ in events) == 34
        assert not any(event == 'FAILED' for event, _ in events)
    else:
        assert run.status.value == 'FAILED' and repo.get(monitor.id).last_successful_end is None
        assert repo.acquisition_diagnostics(run.id)['validation_site'] == 'PAGE_HANDOFF'
        assert repo.acquisition_diagnostics(run.id)['validation_reason'] == 'COUNT_MISMATCH'
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
