"""Run separately from Streamlit: python tools/monitor_worker.py --once."""
import argparse
from functools import lru_cache
from pathlib import Path
import os
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src' / 'backend'))

from application_environment import load_environment
from data_paths import monitoring_data_dir
from monitoring.execution import OpenSearchMonitorExecutor
from monitoring.repository import SQLiteMonitorRepository
from monitoring.runtime import database_path, exclusive_worker
from monitoring.worker import MonitorWorker, structured_log, system_clock


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
    args = parser.parse_args(argv)
    load_environment()
    path = database_path()
    try:
        with exclusive_worker(path.with_suffix('.worker.lock')):
            repository = SQLiteMonitorRepository(path)
            repository.recover_running(system_clock())
            worker = MonitorWorker(repository, OpenSearchMonitorExecutor(pipeline))
            while True:
                worker.tick(max_runs=args.max_runs)
                if args.once:
                    return 0
                time.sleep(args.poll_seconds)
    except KeyboardInterrupt:
        return 0
    except Exception:
        structured_log('STOPPED', category='WORKER_LOCK_OR_STORAGE',
                       summary='Check worker ownership and monitoring storage; only one worker is supported.')
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
