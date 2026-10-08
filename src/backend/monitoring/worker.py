"""Independent single-worker scheduler. No Streamlit dependency."""
from datetime import datetime, timedelta, timezone
import json
import time
from typing import Callable

from monitoring.errors import MonitoringError
from monitoring.repository import MonitorRepository


def system_clock():
    return datetime.now(timezone.utc)


def structured_log(event, **values):
    print('[MONITOR] ' + json.dumps({'event': event, **values}, sort_keys=True), flush=True)


class MonitorWorker:
    def __init__(self, repository: MonitorRepository, executor, *, clock: Callable = system_clock, log=structured_log):
        self.repository, self.executor, self.clock, self.log = repository, executor, clock, log

    def tick(self, *, max_runs=1, now=None):
        if type(max_runs) is not int or not 1 <= max_runs <= 100:
            raise ValueError('Each tick must be bounded to 1..100 runs')
        claim_now = self.clock() if now is None else now
        completed = 0
        for _ in range(max_runs):
            try:
                run = self.repository.claim(claim_now)
            except Exception:
                self.log('FAILED', category='PERSISTENCE')
                break
            if run is None:
                break
            self.log('START', monitor_id=run.monitor_id, run_id=run.id,
                     window_start=run.window.start.isoformat(), window_end=run.window.end.isoformat())
            started = time.monotonic()
            category = 'PERSISTENCE'
            try:
                since = run.window.start - timedelta(seconds=run.definition.overlap_seconds)
                consumed = (set() if getattr(self.executor, 'repository', None) is self.repository
                            else self.repository.consumed(run.monitor_id, since))
                category = 'PIPELINE'
                result = self.executor.execute(run, consumed)
                # Copy only effective scope fields from the executor's redacted
                # presentation, never a connection object or raw query payload.
                summary = result.presentation.get('source_summary', {})
                scope = {key: summary[key] for key in (
                    'source_scope', 'index_expression', 'resolved_index', 'retrieval_start', 'retrieval_end',
                    'namespace', 'workload', 'container', 'document_cluster_id',
                ) if key in summary}
                self.log('ACQUIRED', records=result.counts.events_retrieved, unique_records=result.counts.unique_records, **scope)
                metrics = getattr(result, 'metrics', None)
                if metrics is not None:
                    elapsed = max(time.monotonic() - started, 0.001)
                    window_seconds = max((run.window.end - run.window.start).total_seconds(), 1)
                    metrics.update(
                        run_duration_seconds=round(elapsed, 3),
                        pipeline_duration_seconds=round(max(0, elapsed - metrics.get('acquisition_duration_seconds', 0)), 3),
                        processing_logs_per_second=round(metrics['total_physical_logs'] / elapsed, 3),
                        incoming_logs_per_second=round(metrics['total_physical_logs'] / window_seconds, 3),
                        scheduler_lag_seconds=max(0, round((run.actual_started_at - run.window.end).total_seconds(), 3)),
                        watermark_lag_seconds=max(0, round((self.clock() - run.window.end).total_seconds(), 3)))
                self.log('PIPELINE', candidates=result.counts.signal_candidates, qualified=result.counts.qualified_signals,
                         correlations=result.counts.correlations, incidents=result.counts.incidents)
                category = 'PERSISTENCE'
                if metrics is None:
                    self.repository.succeed(run, result.presentation, result.counts, result.receipts,
                                            now=self.clock())
                else:
                    self.repository.succeed(run, result.presentation, result.counts, result.receipts,
                                            now=self.clock(), metrics=result.metrics,
                                            pattern_metrics=result.pattern_metrics)
                self.log('SUCCESS', run_id=run.id, duration_seconds=max(0, (self.clock() - run.actual_started_at).total_seconds()))
                completed += 1
            except Exception as error:
                if getattr(self.executor, 'repository', None) is self.repository:
                    clear = getattr(getattr(self.executor, 'pipeline_factory', None), 'cache_clear', None)
                    if callable(clear):
                        clear()
                if isinstance(error, MonitoringError):
                    category = error.category
                try:
                    diagnostics = getattr(error, 'acquisition_diagnostics', None) if isinstance(error, MonitoringError) else None
                    if diagnostics is None:
                        self.repository.fail(run, category, now=self.clock())
                    else:
                        self.repository.fail(run, category, now=self.clock(), diagnostics=diagnostics)
                except Exception:
                    category = 'PERSISTENCE'
                self.log('FAILED', run_id=run.id, category=category)
        return completed
