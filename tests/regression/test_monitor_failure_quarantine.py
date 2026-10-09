"""Production regression tests for monitoring failure classification, quarantine,
processing cursor vs contiguous watermark, and gap repair lifecycle.
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'src' / 'backend'))

from monitoring.domain import MonitorDefinition, RunCounts, Window, RunStatus, MonitorStatus
from monitoring.repository import SQLiteMonitorRepository


NOW = datetime(2026, 10, 9, 12, tzinfo=timezone.utc)
STEP = timedelta(minutes=5)


@pytest.fixture
def repo(tmp_path):
    return SQLiteMonitorRepository(tmp_path / 'monitor.sqlite3', compact_mode=True)


def create_monitor(repo, *, initial_start=NOW, enabled=True):
    return repo.create(
        MonitorDefinition('test-mon', 'test-profile', 'test-scope', 'test-ns', 'test-app',
                          initial_start=initial_start, window_seconds=300, interval_seconds=300),
        enabled=enabled, now=NOW
    )


def succeed_run(repo, run, now=NOW, physical_logs=10):
    receipt_time = (run.window.end - timedelta(seconds=10)).isoformat()
    repo.succeed(
        run,
        {'signals': [], 'incidents': []},
        RunCounts(events_retrieved=physical_logs, logical_events=physical_logs),
        {'receipt-1': receipt_time},
        now=now,
        metrics={'total_physical_logs': physical_logs},
        pattern_metrics=()
    )


def fail_run(repo, run, category, now=NOW, diagnostics=None):
    repo.fail(run, category, now=now, diagnostics=diagnostics)


def test_invariant_a_and_h_window_local_failures_lead_to_quarantine_and_advance_cursor(repo):
    """Invariant H: 3 terminal WINDOW_LOCAL failures quarantine the window.
    Invariant A: processing_cursor advances monotonically past the quarantined window.
    Invariant B: QUARANTINED is never counted as SUCCESS.
    """
    monitor = create_monitor(repo)
    # 1st claim: window [NOW, NOW + 5m)
    t1 = NOW + timedelta(minutes=10)
    run1 = repo.claim(t1)
    assert run1 is not None and run1.window == Window(NOW, NOW + STEP)
    fail_run(repo, run1, 'ACQUISITION_LIMIT', now=t1)

    r1_after = repo.history(monitor.id)[0]
    assert r1_after.status == RunStatus.FAILED
    assert r1_after.attempts == 1
    assert r1_after.window_local_failures == 1
    assert repo.get(monitor.id).status == MonitorStatus.ERROR

    # 2nd claim (retry same pending window after cadence)
    t2 = t1 + STEP
    run2 = repo.claim(t2)
    assert run2 is not None and run2.window == run1.window
    assert run2.attempts == 2
    fail_run(repo, run2, 'OPENSEARCH_QUERY', now=t2,
             diagnostics={'reason_code': 'INVALID_TIMESTAMP', 'error_stage': 'content_acquisition'})

    r2_after = repo.history(monitor.id)[0]
    assert r2_after.status == RunStatus.FAILED
    assert r2_after.attempts == 2
    assert r2_after.window_local_failures == 2

    # 3rd claim (retry same pending window) -> reaches threshold
    t3 = t2 + STEP
    run3 = repo.claim(t3)
    assert run3 is not None and run3.window == run1.window
    assert run3.attempts == 3
    fail_run(repo, run3, 'PIPELINE', now=t3,
             diagnostics={'error_stage': 'parsing'})

    r3_after = repo.history(monitor.id)[0]
    assert r3_after.status == RunStatus.QUARANTINED
    assert r3_after.attempts == 3
    assert r3_after.window_local_failures == 3

    m_after = repo.get(monitor.id)
    # Monitor remains ACTIVE, not BLOCKED
    assert m_after.status == MonitorStatus.ACTIVE
    # Invariant B: last_successful_end is NOT advanced
    assert m_after.last_successful_end is None
    # Invariant A: processing_cursor advanced to the end of quarantined window
    assert m_after.processing_cursor == run1.window.end
    assert m_after.pending_window is None

    # Check open gaps
    gaps = repo.open_gaps(monitor.id)
    assert len(gaps) == 1
    assert gaps[0]['status'] == 'OPEN'
    assert gaps[0]['window_start'] == run1.window.start.isoformat()
    assert gaps[0]['window_end'] == run1.window.end.isoformat()
    assert gaps[0]['failure_classification'] == 'WINDOW_LOCAL'


def test_invariants_b_c_d_e_gap_freezes_watermark_and_fast_forwards_on_repair(repo):
    """Invariant C: Unresolved gap prevents last_successful_end from advancing past the gap.
    Invariant D: Repairing the gap fast-forwards watermark across contiguous pre-existing successes.
    Invariant E: Historical gap repair never rewinds processing_cursor.
    """
    monitor = create_monitor(repo)

    # 1. First window [NOW, NOW+5m) gets QUARANTINED
    for i in range(3):
        t = NOW + timedelta(minutes=10 + i * 5)
        run = repo.claim(t)
        assert run is not None
        fail_run(repo, run, 'ACQUISITION_LIMIT', now=t)
    
    assert repo.get(monitor.id).processing_cursor == NOW + STEP
    assert repo.get(monitor.id).last_successful_end is None

    # 2. Sequential catch-up: claim next window [NOW+5m, NOW+10m) from cursor
    t_w2 = NOW + timedelta(minutes=25)
    run_w2 = repo.claim(t_w2)
    assert run_w2 is not None and run_w2.window == Window(NOW + STEP, NOW + 2 * STEP)
    succeed_run(repo, run_w2, now=t_w2)

    # Invariant C: Watermark remains None because [NOW, NOW+5m) is an open gap!
    m2 = repo.get(monitor.id)
    assert m2.last_successful_end is None
    assert m2.processing_cursor == NOW + 2 * STEP

    # 3. Next window [NOW+10m, NOW+15m) succeeds as well
    t_w3 = NOW + timedelta(minutes=30)
    run_w3 = repo.claim(t_w3)
    assert run_w3 is not None and run_w3.window == Window(NOW + 2 * STEP, NOW + 3 * STEP)
    succeed_run(repo, run_w3, now=t_w3)

    m3 = repo.get(monitor.id)
    assert m3.last_successful_end is None
    assert m3.processing_cursor == NOW + 3 * STEP

    # 4. Operator triggers targeted Retry Now on quarantined window [NOW, NOW+5m)
    t_ret = NOW + timedelta(minutes=31)
    repo.request_retry_window(monitor.id, Window(NOW, NOW + STEP), now=t_ret)
    retry_run = repo.claim(t_ret)
    assert retry_run is not None and retry_run.window == Window(NOW, NOW + STEP)

    # Succeed historical gap repair!
    succeed_run(repo, retry_run, now=t_ret)

    m_repaired = repo.get(monitor.id)
    # Invariant D: Watermark jumps forward across w1, w2, and w3!
    assert m_repaired.last_successful_end == NOW + 3 * STEP
    # Invariant E: processing_cursor did NOT rewind to NOW+5m, remained at NOW+15m
    assert m_repaired.processing_cursor == NOW + 3 * STEP
    # Gap is now resolved
    assert len(repo.open_gaps(monitor.id)) == 0


def test_invariant_f_failed_and_quarantined_runs_produce_no_side_effects(repo):
    """Invariant F: Failed/quarantined runs publish zero findings, receipts, or baseline updates."""
    monitor = create_monitor(repo)
    run = repo.claim(NOW + timedelta(minutes=10))
    assert run is not None
    fail_run(repo, run, 'ACQUISITION_LIMIT', now=NOW + timedelta(minutes=10))

    with repo._read_connection() as db:
        assert db.execute('SELECT COUNT(*) FROM monitor_findings').fetchone()[0] == 0
        assert db.execute('SELECT COUNT(*) FROM monitor_receipts').fetchone()[0] == 0
        assert db.execute('SELECT COUNT(*) FROM monitor_baseline_state').fetchone()[0] == 0


def test_invariant_g_transient_infra_does_not_increment_window_local_failures(repo):
    """Invariant G: TRANSIENT_INFRA errors increment attempts but never window_local_failures."""
    monitor = create_monitor(repo)
    for i in range(3):
        t = NOW + timedelta(minutes=10 + i * 5)
        run = repo.claim(t)
        assert run is not None
        fail_run(repo, run, 'OPENSEARCH_TIMEOUT', now=t,
                 diagnostics={'reason_code': 'SEARCH_TIMEOUT', 'error_stage': 'content_acquisition'})

    r_after = repo.history(monitor.id)[0]
    assert r_after.status == RunStatus.FAILED
    assert r_after.attempts == 3
    assert r_after.window_local_failures == 0
    # Monitor remains ERROR with backoff, not BLOCKED or QUARANTINED
    assert repo.get(monitor.id).status == MonitorStatus.ERROR


def test_invariant_i_and_j_unknown_escalation_and_reset(repo):
    """Invariant I: 3 identical UNKNOWN safe signatures escalate to ESCALATED_SYSTEMIC and block the monitor.
    Invariant J: A different signature resets the streak and prevents premature escalation.
    """
    monitor = create_monitor(repo)

    # 1. Attempt 1 with signature A
    t1 = NOW + timedelta(minutes=10)
    run1 = repo.claim(t1)
    assert run1 is not None
    fail_run(repo, run1, 'UNKNOWN', now=t1,
             diagnostics={'reason_code': 'QUERY_FAILURE_UNKNOWN', 'error_stage': 'content_acquisition'})
    assert repo.history(monitor.id)[0].reason_streak == 1
    assert repo.get(monitor.id).status == MonitorStatus.ERROR

    # 2. Attempt 2 with signature B (different stage) -> Invariant J: streak resets to 1!
    t2 = t1 + STEP
    run2 = repo.claim(t2)
    assert run2 is not None
    fail_run(repo, run2, 'UNKNOWN', now=t2,
             diagnostics={'reason_code': 'QUERY_FAILURE_UNKNOWN', 'error_stage': 'shard_planning'})
    assert repo.history(monitor.id)[0].reason_streak == 1
    assert repo.get(monitor.id).status == MonitorStatus.ERROR

    # 3. Attempt 3 with signature B -> streak becomes 2
    t3 = t2 + STEP
    run3 = repo.claim(t3)
    assert run3 is not None
    fail_run(repo, run3, 'UNKNOWN', now=t3,
             diagnostics={'reason_code': 'QUERY_FAILURE_UNKNOWN', 'error_stage': 'shard_planning'})
    assert repo.history(monitor.id)[0].reason_streak == 2
    assert repo.get(monitor.id).status == MonitorStatus.ERROR

    # 4. Attempt 4 with signature B -> streak reaches 3 -> ESCALATED_SYSTEMIC -> BLOCKED!
    t4 = t3 + STEP
    run4 = repo.claim(t4)
    assert run4 is not None
    fail_run(repo, run4, 'UNKNOWN', now=t4,
             diagnostics={'reason_code': 'QUERY_FAILURE_UNKNOWN', 'error_stage': 'shard_planning'})
    r4_after = repo.history(monitor.id)[0]
    assert r4_after.reason_streak == 3
    assert r4_after.failure_classification == 'ESCALATED_SYSTEMIC'
    # Monitor is now BLOCKED
    assert repo.get(monitor.id).status == MonitorStatus.BLOCKED


def test_systemic_failure_immediately_blocks_monitor_without_quarantine(repo):
    """Global auth/config/mapping errors immediately block the monitor without incrementing local failure counter."""
    monitor = create_monitor(repo)
    run = repo.claim(NOW + timedelta(minutes=10))
    assert run is not None
    fail_run(repo, run, 'OPENSEARCH_AUTH', now=NOW + timedelta(minutes=10),
             diagnostics={'reason_code': 'AUTH_FAILURE'})

    r_after = repo.history(monitor.id)[0]
    assert r_after.status == RunStatus.FAILED
    assert r_after.failure_classification == 'SYSTEMIC'
    assert r_after.window_local_failures == 0
    assert repo.get(monitor.id).status == MonitorStatus.BLOCKED
    assert len(repo.open_gaps(monitor.id)) == 0


def test_invariant_k_migration_v6_to_v7_preserves_pending_windows(repo):
    """Invariant K: v6 to v7 migration sets processing_cursor = COALESCE(pending_window_start, ...)
    and never skips a pending or failing window.
    """
    monitor = create_monitor(repo)
    run = repo.claim(NOW + timedelta(minutes=10))
    assert run is not None
    fail_run(repo, run, 'OPENSEARCH_TIMEOUT', now=NOW + timedelta(minutes=10))

    # Manually revert schema version to 6 and drop v7 columns to simulate upgrade
    with repo._transaction() as db:
        db.execute('ALTER TABLE monitors DROP COLUMN processing_cursor')
        db.execute('ALTER TABLE monitors DROP COLUMN target_retry_window_start')
        db.execute('ALTER TABLE monitors DROP COLUMN target_retry_window_end')
        db.execute('ALTER TABLE monitor_runs DROP COLUMN window_local_failures')
        db.execute('ALTER TABLE monitor_runs DROP COLUMN failure_classification')
        db.execute('ALTER TABLE monitor_runs DROP COLUMN last_failure_signature')
        db.execute('DROP TABLE monitor_gaps')
        db.execute('PRAGMA user_version=6')

    # Reopen repository to trigger v6 -> v7 migration
    reopened = SQLiteMonitorRepository(repo.path, compact_mode=True)
    m = reopened.get(monitor.id)
    # processing_cursor must be equal to pending_window_start so the window is not skipped!
    assert m.processing_cursor == run.window.start
    assert m.pending_window == run.window

    with reopened._read_connection() as db:
        assert db.execute('PRAGMA user_version').fetchone()[0] == 7
        assert db.execute('SELECT COUNT(*) FROM monitor_gaps').fetchone()[0] == 0


def test_invariant_l_restart_preserves_cursor_and_gap_state(repo):
    """Invariant L: Process restart preserves processing_cursor, gaps, and targeted retry intent."""
    monitor = create_monitor(repo)
    for i in range(3):
        t = NOW + timedelta(minutes=10 + i * 5)
        run = repo.claim(t)
        assert run is not None
        fail_run(repo, run, 'ACQUISITION_LIMIT', now=t)

    repo.request_retry_window(monitor.id, Window(NOW, NOW + STEP), now=NOW + timedelta(minutes=30))

    # Reopen repo
    reopened = SQLiteMonitorRepository(repo.path, compact_mode=True)
    m = reopened.get(monitor.id)
    assert m.processing_cursor == NOW + STEP
    assert m.target_retry_window == Window(NOW, NOW + STEP)
    assert len(reopened.open_gaps(monitor.id)) == 1


def test_invariant_m_n_o_historical_repair_receipts_baseline_and_audit_counters(repo):
    """Invariant M: Historical repair does not write to active overlap receipts horizon.
    Invariant N: Baseline ring is ordered by window_end descending and strictly bounded.
    Invariant O: Successful repair preserves cumulative attempts and window_local_failures.
    """
    monitor = create_monitor(repo)

    # Quarantine w1 [NOW, NOW+5m)
    for i in range(3):
        t = NOW + timedelta(minutes=10 + i * 5)
        run = repo.claim(t)
        assert run is not None
        fail_run(repo, run, 'ACQUISITION_LIMIT', now=t)

    # Forward success w2 [NOW+5m, NOW+10m)
    t_w2 = NOW + timedelta(minutes=25)
    run_w2 = repo.claim(t_w2)
    assert run_w2 is not None
    succeed_run(repo, run_w2, now=t_w2, physical_logs=20)

    with repo._read_connection() as db:
        initial_receipts = db.execute('SELECT COUNT(*) FROM monitor_receipts').fetchone()[0]
        assert initial_receipts > 0

    # Historical repair w1 [NOW, NOW+5m)
    t_repair = NOW + timedelta(minutes=30)
    repo.request_retry_window(monitor.id, Window(NOW, NOW + STEP), now=t_repair)
    repair_run = repo.claim(t_repair)
    assert repair_run is not None
    assert repair_run.attempts == 4  # 3 previous attempts + 1 retry claim
    assert repair_run.window_local_failures == 3

    succeed_run(repo, repair_run, now=t_repair, physical_logs=15)

    # Invariant O: Audit counters preserved after SUCCESS!
    rep_after = repo.history(monitor.id)[1]
    assert rep_after.status == RunStatus.SUCCESS
    assert rep_after.attempts == 4
    assert rep_after.window_local_failures == 3
    assert rep_after.error_category is None  # Active error state cleared

    # Invariant M: Receipt count did NOT increase from historical repair!
    with repo._read_connection() as db:
        after_receipts = db.execute('SELECT COUNT(*) FROM monitor_receipts').fetchone()[0]
        assert after_receipts == initial_receipts

        # Invariant N: Baseline ring has w2 first, w1 second (chronological descending)
        baseline_row = db.execute('SELECT payload FROM monitor_baseline_state WHERE monitor_id=?',
                                  (monitor.id,)).fetchone()
        import json
        payload = json.loads(baseline_row[0])
        metrics_ring = payload['metrics']
        assert len(metrics_ring) == 2
        assert metrics_ring[0]['window_end'] == (NOW + 2 * STEP).isoformat()  # w2 is later
        assert metrics_ring[1]['window_end'] == (NOW + STEP).isoformat()      # w1 is earlier
