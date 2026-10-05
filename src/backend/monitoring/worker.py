"""Independent single-worker scheduler. No Streamlit dependency."""
from datetime import datetime, timedelta, timezone
import json
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

    def tick(self, *, max_runs=1):
        if type(max_runs) is not int or not 1 <= max_runs <= 100:
            raise ValueError('Each tick must be bounded to 1..100 runs')
        completed = 0
        for _ in range(max_runs):
            try:
                run = self.repository.claim(self.clock())
            except Exception:
                self.log('FAILED', category='PERSISTENCE')
                break
            if run is None:
                break
            self.log('START', monitor_id=run.monitor_id, run_id=run.id,
                     window_start=run.window.start.isoformat(), window_end=run.window.end.isoformat())
            category = 'PERSISTENCE'
            try:
                since = run.window.start - timedelta(seconds=run.definition.overlap_seconds)
                consumed = self.repository.consumed(run.monitor_id, since)
                category = 'PIPELINE'
                result = self.executor.execute(run, consumed)
                self.log('ACQUIRED', records=result.counts.events_retrieved, unique_records=result.counts.unique_records)
                self.log('PIPELINE', candidates=result.counts.signal_candidates, qualified=result.counts.qualified_signals,
                         correlations=result.counts.correlations, incidents=result.counts.incidents)
                category = 'PERSISTENCE'
                self.repository.succeed(run, result.presentation, result.counts, result.receipts, now=self.clock())
                self.log('SUCCESS', run_id=run.id, duration_seconds=max(0, (self.clock() - run.actual_started_at).total_seconds()))
                completed += 1
            except Exception as error:
                if isinstance(error, MonitoringError):
                    category = error.category
                try:
                    self.repository.fail(run, category, now=self.clock())
                except Exception:
                    category = 'PERSISTENCE'
                self.log('FAILED', run_id=run.id, category=category)
        return completed
