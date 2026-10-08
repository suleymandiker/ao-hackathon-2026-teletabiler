"""Operational worker logs leave scheduler and watermark decisions unchanged."""

from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path

import pytest


NOW = datetime(2026, 10, 7, 21, 0, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def backend_imports(monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[2] / 'src' / 'backend'))


@pytest.fixture
def worker_module(monkeypatch):
    path = Path(__file__).resolve().parents[2] / 'tools' / 'monitor_worker.py'
    spec = importlib.util.spec_from_file_location('observable_monitor_cli_test', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, 'load_environment', lambda: None)
    monkeypatch.setattr(module, 'system_clock', lambda: NOW)
    monkeypatch.setattr(module.time, 'sleep', lambda *args: pytest.fail('Unexpected real sleep'))
    monkeypatch.setattr(module, 'pipeline', lambda: pytest.fail('Pipeline must not be constructed by idle worker'))
    return module


def definition(initial_start=NOW):
    from monitoring.domain import MonitorDefinition

    return MonitorDefinition('observable', 'profile', 'cluster-alias', 'ns', 'app', initial_start)


def events(capsys):
    output = capsys.readouterr().out.splitlines()
    assert all(line.startswith('[MONITOR] ') for line in output)
    return [json.loads(line[len('[MONITOR] '):]) for line in output]


def test_schedule_summary_is_read_only_and_uses_claim_readiness(tmp_path):
    from monitoring.repository import SQLiteMonitorRepository

    repo = SQLiteMonitorRepository(tmp_path / 'monitor.sqlite3')
    enabled = repo.create(definition(), enabled=True, now=NOW)
    repo.create(definition(), enabled=False, now=NOW)
    early = repo.schedule_summary(NOW)
    assert early.enabled_monitors == 1
    assert early.due_monitors == 0
    assert early.next_due_at == NOW + timedelta(minutes=17)
    assert repo.schedule_summary(NOW + timedelta(minutes=17)).due_monitors == 1
    assert repo.history(enabled.id) == []
    assert repo.get(enabled.id).last_successful_end is None

    claimed = repo.claim(NOW + timedelta(minutes=17))
    running = repo.schedule_summary(NOW + timedelta(minutes=17))
    assert running.enabled_monitors == 1
    assert running.due_monitors == 0
    assert running.next_due_at is None
    assert repo.history(enabled.id)[0].id == claimed.id


def test_once_idle_reports_start_lock_and_next_due_without_early_run(worker_module, tmp_path, monkeypatch, capsys):
    from monitoring.repository import SQLiteMonitorRepository

    repo = SQLiteMonitorRepository(tmp_path / 'monitor.sqlite3')
    monitor = repo.create(definition(), enabled=True, now=NOW)
    monkeypatch.setattr(worker_module, 'database_path', lambda: repo.path)
    monkeypatch.setattr(worker_module, 'SQLiteMonitorRepository', lambda path: repo)
    assert worker_module.main(['--once', '--max-runs', '1']) == 0
    logged = events(capsys)
    assert [row['event'] for row in logged] == ['WORKER_START', 'WORKER_LOCK_ACQUIRED', 'WORKER_IDLE']
    assert logged[0] == {'event': 'WORKER_START', 'database': str(repo.path), 'once': True, 'max_runs': 1}
    assert logged[-1] == {
        'event': 'WORKER_IDLE', 'enabled_monitors': 1, 'due_monitors': 0,
        'next_due_at': (NOW + timedelta(minutes=17)).isoformat(), 'next_due_in_seconds': 1020,
    }
    assert repo.history(monitor.id) == []
    assert repo.get(monitor.id).last_successful_end is None


def test_second_worker_reports_busy_without_running_scheduler(worker_module, tmp_path, monkeypatch, capsys):
    from monitoring.runtime import exclusive_worker

    path = tmp_path / 'monitor.sqlite3'
    monkeypatch.setattr(worker_module, 'database_path', lambda: path)
    monkeypatch.setattr(worker_module, 'SQLiteMonitorRepository',
                        lambda path: pytest.fail('Busy worker must not open repository'))
    with exclusive_worker(path.with_suffix('.worker.lock')):
        assert worker_module.main(['--once']) == 1
    logged = events(capsys)
    assert [row['event'] for row in logged] == ['WORKER_START', 'WORKER_LOCK_BUSY']
    assert logged[-1]['message'] == 'Another monitor worker already owns the scheduler lock.'
    assert path.with_suffix('.worker.lock').exists()


def test_long_running_idle_heartbeat_is_bounded(worker_module, tmp_path, monkeypatch, capsys):
    from monitoring.repository import SQLiteMonitorRepository

    repo = SQLiteMonitorRepository(tmp_path / 'monitor.sqlite3')
    repo.create(definition(), enabled=True, now=NOW)
    monkeypatch.setattr(worker_module, 'database_path', lambda: repo.path)
    monkeypatch.setattr(worker_module, 'SQLiteMonitorRepository', lambda path: repo)
    elapsed = iter((0, 30, 60))
    monkeypatch.setattr(worker_module, 'monotonic_clock', lambda: next(elapsed))
    sleeps = []

    def stop_after_three_polls(seconds):
        sleeps.append(seconds)
        if len(sleeps) == 3:
            raise KeyboardInterrupt

    monkeypatch.setattr(worker_module.time, 'sleep', stop_after_three_polls)
    assert worker_module.main(['--poll-seconds', '10']) == 0
    logged = events(capsys)
    assert [row['event'] for row in logged] == [
        'WORKER_START', 'WORKER_LOCK_ACQUIRED', 'WORKER_IDLE', 'WORKER_IDLE', 'WORKER_STOP',
    ]
    assert [row['next_due_at'] for row in logged if row['event'] == 'WORKER_IDLE'] == [
        (NOW + timedelta(minutes=17)).isoformat(),
        (NOW + timedelta(minutes=17)).isoformat(),
    ]
    assert logged[-1]['reason'] == 'interrupt'
    assert sleeps == [10, 10, 10]


def test_due_once_retains_execution_logs_and_watermark(worker_module, tmp_path, monkeypatch, capsys):
    from monitoring.domain import RunCounts
    from monitoring.execution import ExecutionResult
    from monitoring.repository import SQLiteMonitorRepository

    repo = SQLiteMonitorRepository(tmp_path / 'monitor.sqlite3')
    monitor = repo.create(definition(NOW - timedelta(minutes=17)), enabled=True, now=NOW)
    monkeypatch.setattr(worker_module, 'database_path', lambda: repo.path)
    monkeypatch.setattr(worker_module, 'SQLiteMonitorRepository', lambda path: repo)

    class Executor:
        def execute(self, run, consumed):
            assert consumed == set()
            return ExecutionResult({'source_summary': {}}, RunCounts(), {})

    monkeypatch.setattr(worker_module, 'OpenSearchMonitorExecutor', lambda pipeline, repository=None: Executor())
    assert worker_module.main(['--once']) == 0
    logged = events(capsys)
    assert [row['event'] for row in logged] == [
        'WORKER_START', 'WORKER_LOCK_ACQUIRED', 'WORKER_DUE',
        'START', 'ACQUIRED', 'PIPELINE', 'SUCCESS',
    ]
    assert logged[2]['due_monitors'] == 1
    assert repo.history(monitor.id)[0].status.value == 'SUCCESS'
    assert repo.get(monitor.id).last_successful_end == NOW - timedelta(minutes=2)
