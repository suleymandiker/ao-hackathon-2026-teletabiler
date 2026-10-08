"""Run separately from Streamlit: python tools/monitor_worker.py --once."""
import argparse
from contextlib import ExitStack
from functools import lru_cache
import math
from pathlib import Path
import os
import sqlite3
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src' / 'backend'))

from application_environment import load_environment
from data_paths import monitoring_data_dir
from monitoring.execution import OpenSearchMonitorExecutor
from monitoring.repository import SQLiteMonitorRepository
from monitoring.runtime import WorkerLockBusy, database_path, exclusive_worker
from monitoring.worker import MonitorWorker, structured_log, system_clock


monotonic_clock = time.monotonic


@lru_cache(maxsize=1)
def pipeline():
    from full_pipeline_v2 import FullAIOpsPipelineV2
    # A worker owns its persistent learning files. A separately cached manual
    # pipeline must not overwrite these files with stale in-memory state.
    learning = Path(os.environ.get('AIOPS_MONITOR_LEARNING_DIR') or monitoring_data_dir() / 'learning')
    learning.mkdir(parents=True, exist_ok=True)
    return FullAIOpsPipelineV2(template_state=str(learning / 'templates.json'),
                              drain_state=str(learning / 'drain.bin'), use_ai_rca=True)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--once', action='store_true', help='One scheduler tick; at most --max-runs bounded runs')
    parser.add_argument('--max-runs', type=int, default=1, choices=range(1, 101), metavar='1..100')
    parser.add_argument('--poll-seconds', type=int, default=10, choices=range(1, 61), metavar='1..60')
    parser.add_argument('--heartbeat-seconds', type=int, default=60, choices=range(10, 3601), metavar='10..3600')
    args = parser.parse_args(argv)
    load_environment()
    path = database_path()
    structured_log('WORKER_START', database=str(path), once=args.once, max_runs=args.max_runs)
    try:
        with ExitStack() as stack:
            try:
                stack.enter_context(exclusive_worker(path.with_suffix('.worker.lock')))
            except WorkerLockBusy:
                structured_log('WORKER_LOCK_BUSY', message='Another monitor worker already owns the scheduler lock.')
                return 1
            except OSError:
                structured_log('STOPPED', category='WORKER_LOCK_OR_STORAGE',
                               summary='Check worker ownership and monitoring storage; only one worker is supported.')
                return 1
            structured_log('WORKER_LOCK_ACQUIRED')
            try:
                repository = SQLiteMonitorRepository(path, compact_mode=True)
                repository.recover_running(system_clock())
            except (OSError, sqlite3.Error, ValueError):
                structured_log('STOPPED', category='WORKER_LOCK_OR_STORAGE',
                               summary='Check worker ownership and monitoring storage; only one worker is supported.')
                return 1
            worker = None
            last_idle_at = None
            last_due_at = None
            last_maintenance_at = None
            last_optimize_at = None
            while True:
                now = system_clock()
                summary = repository.schedule_summary(now)
                elapsed = monotonic_clock()
                if last_maintenance_at is None:
                    last_maintenance_at = last_optimize_at = elapsed
                if summary.due_monitors and (last_due_at is None or elapsed - last_due_at >= args.heartbeat_seconds):
                    structured_log('WORKER_DUE', due_monitors=summary.due_monitors)
                    last_due_at = elapsed
                completed = 0
                if summary.due_monitors:
                    if worker is None:
                        worker = MonitorWorker(repository,
                            OpenSearchMonitorExecutor(pipeline, repository=repository),
                            clock=system_clock)
                    completed = worker.tick(max_runs=args.max_runs, now=now)
                if not completed and not summary.due_monitors and (
                    args.once or last_idle_at is None or elapsed - last_idle_at >= args.heartbeat_seconds
                ):
                    next_due = summary.next_due_at
                    structured_log('WORKER_IDLE', enabled_monitors=summary.enabled_monitors,
                                   due_monitors=0, next_due_at=next_due.isoformat() if next_due else None,
                                   next_due_in_seconds=max(0, math.ceil((next_due - now).total_seconds())) if next_due else None)
                    last_idle_at = elapsed
                if args.once:
                    return 0
                if elapsed - last_maintenance_at >= 3600:
                    optimize = elapsed - last_optimize_at >= 86_400
                    try:
                        details = repository.maintenance(now, optimize=optimize)
                        structured_log('WORKER_MAINTENANCE', **details)
                    except Exception:
                        structured_log('WORKER_MAINTENANCE_FAILED', category='PERSISTENCE')
                    last_maintenance_at = elapsed
                    if optimize:
                        last_optimize_at = elapsed
                time.sleep(args.poll_seconds)
    except KeyboardInterrupt:
        structured_log('WORKER_STOP', reason='interrupt')
        return 0


if __name__ == '__main__':
    raise SystemExit(main())
