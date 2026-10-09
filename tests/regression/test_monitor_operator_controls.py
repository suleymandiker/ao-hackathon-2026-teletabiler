"""Durable operator intent never substitutes for window completeness or ownership."""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'src' / 'backend'))

from monitoring.domain import MonitorDefinition, RunCounts, Window, next_window, safe_at
from monitoring.repository import SQLiteMonitorRepository


NOW = datetime(2026, 10, 9, 12, tzinfo=timezone.utc)


@pytest.fixture
def repo(tmp_path):
    return SQLiteMonitorRepository(tmp_path / 'monitor.sqlite3', compact_mode=True)


def create(repo, *, enabled=True):
    return repo.create(MonitorDefinition('synthetic', 'profile', 'scope', 'ns', 'app', NOW,
                                       window_seconds=300, interval_seconds=300),
                       enabled=enabled, now=NOW)


def succeed(repo, run, now=NOW):
    repo.succeed(run, {}, RunCounts(), {}, now=now,
                 metrics={'total_physical_logs': 0}, pattern_metrics=())


def fail(repo, run, now=NOW):
    repo.fail(run, 'OPENSEARCH_QUERY', now=now,
              diagnostics={'reason_code': 'INVALID_CURSOR', 'error_stage': 'content_acquisition'})


def test_fresh_run_now_is_safe_immediate_and_does_not_rewrite_cadence(repo):
    monitor = create(repo)
    assert repo.claim(NOW) is None
    with repo._transaction() as db:
        db.execute('UPDATE monitors SET next_run_at=? WHERE id=?',
                   ((NOW + timedelta(days=1)).isoformat(), monitor.id))
    before = repo.get(monitor.id)
    requested = repo.request_run_now(monitor.id, now=NOW)
    assert requested.next_run_at == before.next_run_at
    assert requested.definition == before.definition
    assert requested.created_at == before.created_at
    assert requested.last_successful_end is None
    summary = repo.schedule_summary(NOW)
    assert summary.due_monitors == 1 and summary.next_due_at == NOW
    run = repo.claim(NOW)
    assert run.window == Window(NOW - timedelta(minutes=7), NOW - timedelta(minutes=2))
    assert safe_at(run.window, run.definition) == NOW
    assert repo.get(monitor.id).manual_requested_at is None
    assert repo.claim(NOW) is None
    succeed(repo, run)
    assert repo.get(monitor.id).last_successful_end == run.window.end


def test_requests_coalesce_survive_restart_and_fence_claim(repo):
    monitor = create(repo)
    first = repo.request_run_now(monitor.id, now=NOW)
    second = repo.request_run_now(monitor.id, now=NOW + timedelta(seconds=1))
    assert second.manual_requested_at == NOW and second.revision == first.revision
    reopened = SQLiteMonitorRepository(repo.path, compact_mode=True)
    assert next_window(reopened.get(monitor.id)) == next_window(first)
    run = reopened.claim(NOW + timedelta(seconds=2))
    assert run.window == next_window(first)
    repo.request_run_now(monitor.id, now=NOW + timedelta(seconds=3))
    assert repo.get(monitor.id).manual_requested_at is None
    assert repo.claim(NOW + timedelta(seconds=3)) is None
    assert len(repo.history(monitor.id)) == 1
    reopened.recover_running(NOW + timedelta(seconds=4))
    repo.request_run_now(monitor.id, now=NOW + timedelta(seconds=5))
    retry = repo.claim(NOW + timedelta(seconds=5))
    assert retry.id == run.id and retry.window == run.window
    with pytest.raises(ValueError, match='claim no longer owned'):
        succeed(repo, run)
    succeed(repo, retry, NOW + timedelta(seconds=5))


def test_pause_cancels_intent_resume_atomically_requests(repo):
    monitor = create(repo)
    repo.request_run_now(monitor.id, now=NOW)
    cadence = repo.get(monitor.id).next_run_at
    paused = repo.set_enabled(monitor.id, False, now=NOW)
    assert paused.status.value == 'PAUSED' and paused.manual_requested_at is None
    assert paused.next_run_at == cadence
    assert repo.claim(NOW + timedelta(days=1)) is None
    with pytest.raises(ValueError):
        repo.request_run_now(monitor.id, now=NOW)
    resumed = repo.resume_now(monitor.id, now=NOW)
    assert resumed.enabled and resumed.manual_requested_at == NOW
    assert resumed.next_run_at == cadence
    assert repo.schedule_summary(NOW).due_monitors == 1
    assert repo.claim(NOW) is not None


@pytest.mark.parametrize('action,expected', [('pause', 'PAUSED'), ('archive', 'ARCHIVED')])
@pytest.mark.parametrize('finish', ['success', 'failure'])
def test_operator_lifecycle_wins_over_inflight_finalization(repo, action, expected, finish):
    monitor = create(repo)
    repo.request_run_now(monitor.id, now=NOW)
    run = repo.claim(NOW)
    if action == 'pause':
        repo.set_enabled(monitor.id, False, now=NOW)
    else:
        repo.archive(monitor.id, now=NOW)
    (succeed if finish == 'success' else fail)(repo, run)
    after = repo.get(monitor.id)
    assert after.status.value == expected and not after.enabled
    assert after.manual_requested_at is None
    assert after.last_successful_end == (run.window.end if finish == 'success' else None)
    assert repo.claim(NOW + timedelta(days=1)) is None
    if action == 'archive':
        with pytest.raises(ValueError):
            repo.resume_now(monitor.id, now=NOW)


def test_blocked_retry_same_window_retains_streak_then_success_resumes_sequentially(repo):
    monitor = create(repo)
    original = None
    for attempt in range(1, 5):
        before = repo.get(monitor.id)
        repo.request_run_now(monitor.id, now=NOW)
        assert repo.get(monitor.id).next_run_at == before.next_run_at
        run = repo.claim(NOW)
        original = original or run
        assert (run.id, run.window) == (original.id, original.window)
        assert run.attempts == attempt
        fail(repo, run)
        assert repo.get(monitor.id).last_successful_end is None
        assert repo.claim(NOW) is None  # automatic backoff still applies
        with repo._read_connection() as db:
            assert db.execute('SELECT reason_streak FROM monitor_runs').fetchone()[0] == attempt
            assert db.execute('SELECT COUNT(*) FROM monitor_baseline_state').fetchone()[0] == 0
            assert db.execute('SELECT COUNT(*) FROM monitor_findings').fetchone()[0] == 0
    assert repo.get(monitor.id).status.value == 'BLOCKED'
    assert repo.get(monitor.id).next_run_at == NOW + timedelta(hours=1)
    repo.request_run_now(monitor.id, now=NOW)
    retry = repo.claim(NOW)
    succeed(repo, retry)
    after = repo.get(monitor.id)
    assert after.status.value == 'ACTIVE'
    assert after.pending_window is None and after.manual_requested_at is None
    assert after.last_successful_end == original.window.end
    repo.request_run_now(monitor.id, now=NOW)
    assert repo.schedule_summary(NOW).due_monitors == 0
    assert repo.schedule_summary(NOW).next_due_at == NOW + timedelta(minutes=5)
    assert repo.claim(NOW) is None  # request cannot bypass ingestion safety
    following = repo.claim(NOW + timedelta(minutes=5))
    assert following.window.start == original.window.end
    assert safe_at(following.window, following.definition) == NOW + timedelta(minutes=5)


def test_automatic_cadence_and_initial_start_are_unchanged(repo):
    monitor = create(repo)
    expected = Window(NOW, NOW + timedelta(minutes=5))
    assert next_window(monitor) == expected
    assert repo.schedule_summary(NOW).next_due_at == NOW + timedelta(minutes=7)
    assert repo.claim(NOW + timedelta(minutes=7) - timedelta(microseconds=1)) is None
    run = repo.claim(NOW + timedelta(minutes=7))
    assert run.window == expected
    succeed(repo, run, NOW + timedelta(minutes=7))
    assert repo.get(monitor.id).next_run_at == NOW + timedelta(minutes=12)
    assert repo.claim(NOW + timedelta(minutes=11)) is None


def test_pending_failed_identity_survives_retention_and_pause(repo):
    monitor = create(repo)
    repo.request_run_now(monitor.id, now=NOW)
    run = repo.claim(NOW)
    fail(repo, run)
    repo.set_enabled(monitor.id, False, now=NOW)
    later = NOW + timedelta(days=10)
    repo.maintenance(later)
    assert repo.history(monitor.id)[0].id == run.id
    repo.resume_now(monitor.id, now=later)
    retry = repo.claim(later)
    assert retry.id == run.id and retry.window == run.window


def test_request_prevents_scope_edit_but_allows_name_and_interval(repo):
    from dataclasses import replace
    monitor = create(repo)
    monitor = repo.request_run_now(monitor.id, now=NOW)
    with pytest.raises(ValueError):
        repo.update(monitor.id, replace(monitor.definition, workload='changed'),
                    revision=monitor.revision, now=NOW)
    updated = repo.update(monitor.id, replace(monitor.definition, name='renamed'),
                          revision=monitor.revision, now=NOW)
    assert next_window(updated) == next_window(monitor)


def test_v5_migration_preserves_pending_failure_and_statistics(repo):
    monitor = create(repo)
    run = repo.claim(NOW + timedelta(minutes=7))
    fail(repo, run, NOW + timedelta(minutes=7))
    with repo._transaction() as db:
        for column in ('manual_requested_at', 'pending_window_start', 'pending_window_end'):
            db.execute(f'ALTER TABLE monitors DROP COLUMN {column}')
        db.execute('ALTER TABLE monitor_runs DROP COLUMN reason_streak')
        db.execute('PRAGMA user_version=5')
        db.execute('ANALYZE')
        assert db.execute("SELECT 1 FROM sqlite_master WHERE name='sqlite_stat1'").fetchone()
    migrated = SQLiteMonitorRepository(repo.path, compact_mode=True)
    before = migrated.get(monitor.id)
    assert before.definition == monitor.definition and before.last_successful_end is None
    assert before.pending_window == run.window
    migrated.request_run_now(monitor.id, now=NOW + timedelta(minutes=8))
    retry = migrated.claim(NOW + timedelta(minutes=8))
    assert retry.id == run.id and retry.window == run.window
    with migrated._read_connection() as db:
        assert db.execute('PRAGMA user_version').fetchone()[0] == 6
        assert db.execute('SELECT reason_streak FROM monitor_runs').fetchone()[0] == 0


def test_pending_resume_during_running_does_not_queue_another_run(repo):
    monitor = create(repo)
    repo.request_run_now(monitor.id, now=NOW)
    run = repo.claim(NOW)
    repo.set_enabled(monitor.id, False, now=NOW)
    resumed = repo.resume_now(monitor.id, now=NOW)
    assert resumed.status.value == 'RUNNING' and resumed.manual_requested_at is None
    succeed(repo, run)
    assert repo.claim(NOW) is None


def test_competing_connections_claim_manual_window_only_once(repo):
    from concurrent.futures import ThreadPoolExecutor
    monitor = create(repo)
    repo.request_run_now(monitor.id, now=NOW)
    other = SQLiteMonitorRepository(repo.path, initialize=False, compact_mode=True)
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda repository: repository.claim(NOW), [repo, other]))
    assert sum(run is not None for run in results) == 1
    assert len(repo.history(monitor.id)) == 1


def test_v5_migration_preserves_streak_baseline_receipts_archive_and_watermark(repo):
    monitor = create(repo)
    run = repo.claim(NOW + timedelta(minutes=7))
    succeed(repo, run, NOW + timedelta(minutes=7))
    failed = repo.claim(NOW + timedelta(minutes=12))
    fail(repo, failed, NOW + timedelta(minutes=12))
    archived = create(repo, enabled=False)
    repo.archive(archived.id, now=NOW)
    with repo._transaction() as db:
        db.execute('INSERT INTO monitor_receipts VALUES (?,?,?)',
                   (monitor.id, 'synthetic-reference', run.window.end.isoformat()))
        baseline = db.execute('SELECT payload FROM monitor_baseline_state').fetchone()[0]
        for column in ('manual_requested_at', 'pending_window_start', 'pending_window_end'):
            db.execute(f'ALTER TABLE monitors DROP COLUMN {column}')
        db.execute('PRAGMA user_version=5')
    reopened = SQLiteMonitorRepository(repo.path, compact_mode=True)
    assert reopened.get(archived.id).archived
    current = reopened.get(monitor.id)
    assert current.last_successful_end == run.window.end and current.pending_window == failed.window
    with reopened._read_connection() as db:
        assert db.execute('SELECT payload FROM monitor_baseline_state').fetchone()[0] == baseline
        assert db.execute('SELECT COUNT(*) FROM monitor_receipts').fetchone()[0] == 1
        assert db.execute('SELECT reason_streak FROM monitor_runs WHERE id=?',
                          (failed.id,)).fetchone()[0] == 1
    reopened.request_run_now(monitor.id, now=NOW + timedelta(minutes=12))
    retry = reopened.claim(NOW + timedelta(minutes=12))
    assert retry.id == failed.id
    fail(reopened, retry, NOW + timedelta(minutes=12))
    with reopened._read_connection() as db:
        assert db.execute('SELECT reason_streak FROM monitor_runs WHERE id=?',
                          (failed.id,)).fetchone()[0] == 2


def test_run_now_bypasses_cadence_for_established_safe_window(repo):
    from dataclasses import replace
    monitor = create(repo)
    monitor = repo.update(monitor.id, replace(monitor.definition, interval_seconds=3600),
                          revision=monitor.revision, now=NOW)
    repo.request_run_now(monitor.id, now=NOW)
    first = repo.claim(NOW)
    succeed(repo, first)
    safe = NOW + timedelta(minutes=5)
    cadence = repo.get(monitor.id).next_run_at
    assert cadence == NOW + timedelta(hours=1)
    assert repo.claim(safe) is None
    repo.request_run_now(monitor.id, now=safe)
    assert repo.get(monitor.id).next_run_at == cadence
    assert repo.schedule_summary(safe).due_monitors == 1
    run = repo.claim(safe)
    assert run.window.start == first.window.end
    assert safe_at(run.window, run.definition) == safe


def test_archive_cancels_unclaimed_request(repo):
    monitor = create(repo)
    repo.request_run_now(monitor.id, now=NOW)
    repo.archive(monitor.id, now=NOW)
    assert repo.get(monitor.id).manual_requested_at is None
    assert repo.schedule_summary(NOW).due_monitors == 0
    assert repo.claim(NOW + timedelta(days=1)) is None
    assert repo.history(monitor.id) == []


def test_watermark_keeps_scope_locked_after_history_retention(repo):
    from dataclasses import replace
    monitor = create(repo)
    repo.request_run_now(monitor.id, now=NOW)
    succeed(repo, repo.claim(NOW))
    repo.maintenance(NOW + timedelta(days=10))
    assert repo.history(monitor.id) == []
    current = repo.get(monitor.id)
    with pytest.raises(ValueError):
        repo.update(monitor.id, replace(current.definition, workload='different'),
                    revision=current.revision, now=NOW + timedelta(days=10))
