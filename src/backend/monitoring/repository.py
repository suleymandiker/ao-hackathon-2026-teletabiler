"""Transactional monitoring persistence, separate from learning/policy stores.

The protocol is the migration boundary for a future PostgreSQL implementation.
One success transaction owns the result, reference receipts and watermark.
"""
from contextlib import contextmanager
from dataclasses import asdict, dataclass, replace
from datetime import date, datetime, timedelta, timezone
from enum import Enum
import json
from pathlib import Path
import sqlite3
from typing import Protocol

from monitoring.domain import (
    DeploymentMonitor, MonitorDefinition, MonitorRun, MonitorStatus, RunCounts,
    RunStatus, Window, new_id, next_window, safe_at, utc,
)
from monitoring.errors import MESSAGES


class MonitorRepository(Protocol):
    def create(self, definition: MonitorDefinition, *, enabled: bool, now: datetime) -> DeploymentMonitor: ...
    def get(self, monitor_id: str) -> DeploymentMonitor: ...
    def list(self, *, include_archived: bool = False) -> list[DeploymentMonitor]: ...
    def update(self, monitor_id: str, definition: MonitorDefinition, *, revision: int, now: datetime) -> DeploymentMonitor: ...
    def set_enabled(self, monitor_id: str, enabled: bool, *, now: datetime) -> DeploymentMonitor: ...
    def archive(self, monitor_id: str, *, now: datetime) -> DeploymentMonitor: ...
    def claim(self, now: datetime) -> MonitorRun | None: ...
    def schedule_summary(self, now: datetime) -> 'ScheduleSummary': ...
    def recover_running(self, now: datetime) -> None: ...
    def succeed(self, run: MonitorRun, result: dict, counts: RunCounts, receipts: dict[str, str], *,
                now: datetime, metrics: dict | None = None, pattern_metrics=()) -> None: ...
    def fail(self, run: MonitorRun, category: str, *, now: datetime, diagnostics: dict | None = None) -> None: ...
    def history(self, monitor_id: str, limit: int = 100) -> list[MonitorRun]: ...
    def result(self, run_id: str) -> dict | None: ...
    def acquisition_diagnostics(self, run_id: str) -> dict | None: ...
    def consumed(self, monitor_id: str, since: datetime) -> set[str]: ...
    def record_shard(self, run: MonitorRun, shard, status: str, **counts) -> None: ...
    def acquisition_ledger(self, run: MonitorRun, since: datetime): ...
    def successful_metrics(self, monitor_id: str, before: datetime, limit: int = 30) -> list[dict]: ...
    def successful_patterns(self, monitor_id: str, before: datetime, limit: int = 5) -> list[dict]: ...
    def acquisition_shards(self, run_id: str) -> list[dict]: ...


@dataclass(frozen=True)
class ScheduleSummary:
    enabled_monitors: int
    due_monitors: int
    next_due_at: datetime | None


def _json_safe(value):
    """Convert only supported monitoring payload types at the storage boundary."""
    if value is None or (isinstance(value, (bool, int, float, str)) and not isinstance(value, Enum)):
        return value
    if isinstance(value, datetime):
        return utc(value).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Enum):
        return _json_safe(value.value)
    if isinstance(value, dict):
        return {key: _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    raise TypeError(f'Object of type {type(value).__name__} is not JSON serializable')


def encode(value):
    return json.dumps(_json_safe(value), ensure_ascii=False, allow_nan=False, separators=(',', ':'))


def definition_json(definition):
    value = asdict(definition)
    value['initial_start'] = definition.initial_start.isoformat()
    return encode(value)


def read_definition(value):
    value = json.loads(value)
    value['initial_start'] = datetime.fromisoformat(value['initial_start'])
    return MonitorDefinition(**value)


def instant(value):
    return datetime.fromisoformat(value) if value else None


class SQLiteMonitorRepository:
    def __init__(self, path):
        self.path = Path(path).resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._transaction() as db:
            version = db.execute('PRAGMA user_version').fetchone()[0]
            if version not in (0, 1, 2, 3, 4):
                raise ValueError('Unsupported monitoring schema version')
            # Refuse accidental use of an existing policy/template database.
            names = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if names - {'monitors', 'monitor_runs', 'monitor_results', 'monitor_receipts',
                         'monitor_run_metrics', 'monitor_pattern_metrics', 'monitor_acquisition_shards',
                         'monitor_run_dedupe'}:
                raise ValueError('Monitoring requires its own database')
            statements = (
                '''CREATE TABLE IF NOT EXISTS monitors (
                    id TEXT PRIMARY KEY, definition TEXT NOT NULL, enabled INTEGER NOT NULL,
                    status TEXT NOT NULL, last_successful_end TEXT, next_run_at TEXT NOT NULL,
                    created_at TEXT NOT NULL, updated_at TEXT NOT NULL, revision INTEGER NOT NULL DEFAULT 0,
                    error_category TEXT, error_summary TEXT)''',
                '''CREATE TABLE IF NOT EXISTS monitor_runs (
                    id TEXT PRIMARY KEY, monitor_id TEXT NOT NULL REFERENCES monitors(id),
                    window_start TEXT NOT NULL, window_end TEXT NOT NULL, definition TEXT NOT NULL,
                    started_at TEXT NOT NULL, finished_at TEXT, created_at TEXT NOT NULL,
                    status TEXT NOT NULL, claim_token TEXT NOT NULL, attempts INTEGER NOT NULL,
                    counts TEXT NOT NULL, result_reference TEXT, error_category TEXT, error_summary TEXT,
                    UNIQUE(monitor_id, window_start, window_end))''',
                '''CREATE TABLE IF NOT EXISTS monitor_results (
                    run_id TEXT PRIMARY KEY REFERENCES monitor_runs(id), payload TEXT NOT NULL)''',
                '''CREATE TABLE IF NOT EXISTS monitor_receipts (
                    monitor_id TEXT NOT NULL REFERENCES monitors(id), reference_hash TEXT NOT NULL,
                    source_time TEXT NOT NULL, PRIMARY KEY(monitor_id, reference_hash))''',
                '''CREATE TABLE IF NOT EXISTS monitor_run_metrics (
                    run_id TEXT PRIMARY KEY REFERENCES monitor_runs(id), payload TEXT NOT NULL)''',
                '''CREATE TABLE IF NOT EXISTS monitor_pattern_metrics (
                    run_id TEXT NOT NULL REFERENCES monitor_runs(id), template_id TEXT NOT NULL,
                    payload TEXT NOT NULL, PRIMARY KEY(run_id, template_id))''',
                '''CREATE TABLE IF NOT EXISTS monitor_acquisition_shards (
                    run_id TEXT NOT NULL REFERENCES monitor_runs(id), attempt INTEGER NOT NULL,
                    shard_id TEXT NOT NULL, start_at TEXT NOT NULL, end_at TEXT NOT NULL,
                    depth INTEGER NOT NULL, status TEXT NOT NULL, pages_read INTEGER NOT NULL DEFAULT 0,
                    records_read INTEGER NOT NULL DEFAULT 0, unique_records INTEGER NOT NULL DEFAULT 0,
                    completed_at TEXT, failure_category TEXT,
                    PRIMARY KEY(run_id, attempt, shard_id))''',
                '''CREATE TABLE IF NOT EXISTS monitor_run_dedupe (
                    run_id TEXT NOT NULL REFERENCES monitor_runs(id), reference_hash TEXT NOT NULL,
                    source_time TEXT NOT NULL, owned INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY(run_id, reference_hash))''',
                'CREATE INDEX IF NOT EXISTS monitor_due ON monitors(enabled, next_run_at)',
                'CREATE INDEX IF NOT EXISTS receipt_time ON monitor_receipts(monitor_id, source_time)',
                'CREATE INDEX IF NOT EXISTS monitor_metrics_history ON monitor_runs(monitor_id,status,window_end)',
            )
            for statement in statements:
                db.execute(statement)
            # Additive v2 migration: existing definitions, runs, results and
            # receipts are untouched. Learning stores are separate databases.
            columns = {row['name'] for row in db.execute('PRAGMA table_info(monitors)')}
            if 'archived' not in columns:
                db.execute('ALTER TABLE monitors ADD COLUMN archived INTEGER NOT NULL DEFAULT 0')
            # Additive v3: latest failed-attempt acquisition snapshot. Existing
            # runs default to NULL; historical results/evidence are untouched.
            run_columns = {row['name'] for row in db.execute('PRAGMA table_info(monitor_runs)')}
            if 'acquisition_diagnostics' not in run_columns:
                db.execute('ALTER TABLE monitor_runs ADD COLUMN acquisition_diagnostics TEXT')
            db.execute('PRAGMA user_version=4')

    @contextmanager
    def _transaction(self):
        db = sqlite3.connect(self.path, timeout=10)
        db.row_factory = sqlite3.Row
        try:
            db.execute('PRAGMA foreign_keys=ON')
            db.execute('BEGIN IMMEDIATE')
            yield db
            db.commit()
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()

    @staticmethod
    def _monitor(row):
        return DeploymentMonitor(
            row['id'], read_definition(row['definition']), bool(row['enabled']), MonitorStatus(row['status']),
            instant(row['last_successful_end']), instant(row['next_run_at']), instant(row['created_at']),
            instant(row['updated_at']), row['revision'], row['error_category'], row['error_summary'], bool(row['archived']))

    @staticmethod
    def _run(row):
        return MonitorRun(
            row['id'], row['monitor_id'], Window(instant(row['window_start']), instant(row['window_end'])),
            read_definition(row['definition']), instant(row['started_at']), instant(row['finished_at']),
            instant(row['created_at']), RunStatus(row['status']), row['claim_token'], row['attempts'],
            RunCounts(**json.loads(row['counts'])), row['result_reference'], row['error_category'], row['error_summary'])

    def create(self, definition, *, enabled=False, now):
        now = utc(now).isoformat()
        monitor_id = new_id()
        with self._transaction() as db:
            db.execute('''INSERT INTO monitors
                (id,definition,enabled,status,last_successful_end,next_run_at,created_at,updated_at,revision,error_category,error_summary)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)''', (
                monitor_id, definition_json(definition), int(enabled), 'ACTIVE' if enabled else 'PAUSED',
                None, now, now, now, 0, None, None))
        return self.get(monitor_id)

    def get(self, monitor_id):
        with self._transaction() as db:
            row = db.execute('SELECT * FROM monitors WHERE id=?', (monitor_id,)).fetchone()
            if row is None:
                raise KeyError('Monitor not found')
            return self._monitor(row)

    def list(self, *, include_archived=False):
        with self._transaction() as db:
            return [self._monitor(row) for row in db.execute(
                'SELECT * FROM monitors WHERE (? OR archived=0) ORDER BY created_at,id', (include_archived,))]

    def update(self, monitor_id, definition, *, revision, now):
        with self._transaction() as db:
            old = self._monitor(db.execute('SELECT * FROM monitors WHERE id=?', (monitor_id,)).fetchone())
            if old.archived:
                raise ValueError('Cannot edit an archived monitor')
            if old.revision != revision or old.status == MonitorStatus.RUNNING:
                raise ValueError('Monitor changed or is running; refresh before editing')
            has_history = db.execute('SELECT 1 FROM monitor_runs WHERE monitor_id=? LIMIT 1', (monitor_id,)).fetchone()
            if has_history and replace(definition, name=old.definition.name, interval_seconds=old.definition.interval_seconds) != old.definition:
                raise ValueError('After the first run, only name and interval can change; create a new monitor for a new scope or policy')
            db.execute('UPDATE monitors SET definition=?, updated_at=?, revision=revision+1 WHERE id=?',
                       (definition_json(definition), utc(now).isoformat(), monitor_id))
        return self.get(monitor_id)

    def set_enabled(self, monitor_id, enabled, *, now):
        now = utc(now).isoformat()
        with self._transaction() as db:
            old = db.execute('SELECT archived FROM monitors WHERE id=?', (monitor_id,)).fetchone()
            if old is None:
                raise KeyError('Monitor not found')
            if old['archived']:
                raise ValueError('Cannot enable or pause an archived monitor')
            db.execute('''UPDATE monitors SET enabled=?, status=CASE
                WHEN ?=0 THEN 'PAUSED'
                WHEN EXISTS(SELECT 1 FROM monitor_runs WHERE monitor_id=? AND status='RUNNING') THEN 'RUNNING'
                ELSE 'ACTIVE' END, next_run_at=?, updated_at=?, revision=revision+1 WHERE id=?''',
                (int(enabled), int(enabled), monitor_id, now, now, monitor_id))
        return self.get(monitor_id)

    def archive(self, monitor_id, *, now):
        """Stop future claims, retaining audit evidence and any in-flight run.

        Like pause, an owned run may finish persisting its result. It cannot
        reactivate the monitor. Repeated archive requests are harmless.
        """
        with self._transaction() as db:
            db.execute("""UPDATE monitors SET archived=1,enabled=0,status='ARCHIVED',
                updated_at=?,revision=revision+1 WHERE id=? AND archived=0""",
                (utc(now).isoformat(), monitor_id))
        return self.get(monitor_id)

    def claim(self, now):
        now = utc(now)
        with self._transaction() as db:
            rows = db.execute('''SELECT * FROM monitors WHERE enabled=1 AND archived=0 AND next_run_at<=?
                AND NOT EXISTS(SELECT 1 FROM monitor_runs WHERE monitor_id=monitors.id AND status='RUNNING')
                ORDER BY updated_at,next_run_at,id''', (now.isoformat(),)).fetchall()
            for row in rows:
                monitor = self._monitor(row)
                window = next_window(monitor)
                if safe_at(window, monitor.definition) > now:
                    continue
                existing = db.execute('SELECT * FROM monitor_runs WHERE monitor_id=? AND window_start=? AND window_end=?',
                                      (monitor.id, window.start.isoformat(), window.end.isoformat())).fetchone()
                if existing and existing['status'] == 'SUCCESS':
                    continue
                token = new_id()
                if existing:
                    run_id = existing['id']
                    db.execute('DELETE FROM monitor_run_dedupe WHERE run_id=?', (run_id,))
                    db.execute("""UPDATE monitor_runs SET status='RUNNING', claim_token=?, started_at=?,
                         finished_at=NULL, attempts=attempts+1, error_category=NULL,error_summary=NULL,
                         acquisition_diagnostics=NULL WHERE id=?""",
                        (token, now.isoformat(), run_id))
                else:
                    run_id = new_id()
                    db.execute('''INSERT INTO monitor_runs (id,monitor_id,window_start,window_end,definition,
                        started_at,finished_at,created_at,status,claim_token,attempts,counts,result_reference,error_category,error_summary)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', (
                        run_id, monitor.id, window.start.isoformat(), window.end.isoformat(), definition_json(monitor.definition),
                        now.isoformat(), None, now.isoformat(), 'RUNNING', token, 1, encode(asdict(RunCounts())), None, None, None))
                db.execute("UPDATE monitors SET status='RUNNING',updated_at=?,revision=revision+1 WHERE id=?", (now.isoformat(), monitor.id))
                return self._run(db.execute('SELECT * FROM monitor_runs WHERE id=?', (run_id,)).fetchone())
        return None

    def schedule_summary(self, now):
        """Read scheduling state without claiming runs or changing persisted state."""
        now = utc(now)
        db = sqlite3.connect(self.path, timeout=10)
        db.row_factory = sqlite3.Row
        try:
            db.execute('PRAGMA query_only=ON')
            enabled = db.execute('SELECT COUNT(*) FROM monitors WHERE enabled=1 AND archived=0').fetchone()[0]
            rows = db.execute('''SELECT * FROM monitors WHERE enabled=1 AND archived=0
                AND NOT EXISTS(SELECT 1 FROM monitor_runs WHERE monitor_id=monitors.id AND status='RUNNING')
                ORDER BY next_run_at,id''')
            due = 0
            next_due_at = None
            for row in rows:
                monitor = self._monitor(row)
                window = next_window(monitor)
                existing = db.execute('SELECT status FROM monitor_runs WHERE monitor_id=? AND window_start=? AND window_end=?',
                                      (monitor.id, window.start.isoformat(), window.end.isoformat())).fetchone()
                if existing and existing['status'] == 'SUCCESS':
                    continue
                ready_at = max(monitor.next_run_at, safe_at(window, monitor.definition))
                if ready_at <= now:
                    due += 1
                if next_due_at is None or ready_at < next_due_at:
                    next_due_at = ready_at
            return ScheduleSummary(enabled, due, next_due_at)
        finally:
            db.close()

    @staticmethod
    def _owned(db, run):
        return db.execute("SELECT 1 FROM monitor_runs WHERE id=? AND status='RUNNING' AND claim_token=?",
                          (run.id, run.claim_token)).fetchone() is not None

    def succeed(self, run, result, counts, receipts, *, now, metrics=None, pattern_metrics=()):
        payload = encode(result)  # Fail before any writes if serialization fails.
        now = utc(now)
        with self._transaction() as db:
            if not self._owned(db, run):
                raise ValueError('Run claim no longer owned')
            monitor = self._monitor(db.execute('SELECT * FROM monitors WHERE id=?', (run.monitor_id,)).fetchone())
            if next_window(monitor) != run.window:
                raise ValueError('Watermark does not match claimed window')
            db.execute('INSERT INTO monitor_results VALUES (?,?)', (run.id, payload))
            db.executemany('INSERT INTO monitor_receipts VALUES (?,?,?)',
                           [(run.monitor_id, key, value) for key, value in receipts.items()])
            db.execute('''INSERT OR IGNORE INTO monitor_receipts(monitor_id,reference_hash,source_time)
                SELECT ?,reference_hash,source_time FROM monitor_run_dedupe
                WHERE run_id=? AND owned=1''', (run.monitor_id, run.id))
            db.execute('DELETE FROM monitor_run_dedupe WHERE run_id=?', (run.id,))
            if metrics is not None:
                db.execute('INSERT OR REPLACE INTO monitor_run_metrics VALUES (?,?)', (run.id, encode(metrics)))
                db.executemany('INSERT OR REPLACE INTO monitor_pattern_metrics VALUES (?,?,?)',
                               [(run.id, item['template_id'], encode(item)) for item in pattern_metrics])
            db.execute("""UPDATE monitor_runs SET status='SUCCESS',finished_at=?,counts=?,result_reference=? WHERE id=?""",
                       (now.isoformat(), encode(asdict(counts)), run.id, run.id))
            following = Window(run.window.end, run.window.end + timedelta(seconds=monitor.definition.window_seconds))
            # Catch up promptly, but only one bounded window per claim.
            due = now if safe_at(following, monitor.definition) <= now else max(
                safe_at(following, monitor.definition), now + timedelta(seconds=monitor.definition.interval_seconds))
            db.execute('''UPDATE monitors SET last_successful_end=?,next_run_at=?,status=?,
                error_category=NULL,error_summary=NULL,updated_at=?,revision=revision+1 WHERE id=?''',
                (run.window.end.isoformat(), due.isoformat(),
                 'ARCHIVED' if monitor.archived else 'ACTIVE' if monitor.enabled else 'PAUSED', now.isoformat(), run.monitor_id))
            # Only overlap receipts can be needed again; history/results are durable.
            cutoff = run.window.end - timedelta(seconds=monitor.definition.overlap_seconds)
            db.execute('DELETE FROM monitor_receipts WHERE monitor_id=? AND source_time<?', (run.monitor_id, cutoff.isoformat()))

    def fail(self, run, category, *, now, diagnostics=None):
        category = category if category in MESSAGES else 'UNKNOWN'
        payload = None
        if diagnostics is not None:
            # Executor already removes runtime credentials. Reapply the existing
            # safe projection at the storage boundary for alternate callers.
            from opensearch_application import presentation_result
            payload = encode(presentation_result({}, diagnostics, None)['source_summary'])
        now = utc(now)
        with self._transaction() as db:
            if not self._owned(db, run):
                return
            db.execute('DELETE FROM monitor_run_dedupe WHERE run_id=?', (run.id,))
            monitor = self._monitor(db.execute('SELECT * FROM monitors WHERE id=?', (run.monitor_id,)).fetchone())
            db.execute("""UPDATE monitor_runs SET status='FAILED',finished_at=?,error_category=?,error_summary=?,
                       acquisition_diagnostics=? WHERE id=?""",
                       (now.isoformat(), category, MESSAGES[category], payload, run.id))
            db.execute('''UPDATE monitors SET status=?,next_run_at=?,error_category=?,error_summary=?,updated_at=?,
                revision=revision+1 WHERE id=?''', ('ARCHIVED' if monitor.archived else 'ERROR' if monitor.enabled else 'PAUSED',
                (now + timedelta(seconds=monitor.definition.interval_seconds)).isoformat(), category, MESSAGES[category], now.isoformat(), run.monitor_id))

    def recover_running(self, now):
        """Call ONLY after obtaining exclusive worker ownership, never from UI."""
        with self._transaction() as db:
            runs = [self._run(row) for row in db.execute("SELECT * FROM monitor_runs WHERE status='RUNNING'")]
        for run in runs:
            self.fail(run, 'INTERRUPTED', now=now)
        with self._transaction() as db:
            db.execute("UPDATE monitors SET next_run_at=? WHERE error_category='INTERRUPTED' AND archived=0", (utc(now).isoformat(),))

    def history(self, monitor_id, limit=100):
        if not 1 <= limit <= 1000:
            raise ValueError('History page must contain 1 to 1000 runs')
        with self._transaction() as db:
            return [self._run(row) for row in db.execute('SELECT * FROM monitor_runs WHERE monitor_id=? ORDER BY window_start DESC LIMIT ?', (monitor_id, limit))]

    def result(self, run_id):
        with self._transaction() as db:
            row = db.execute('SELECT payload FROM monitor_results WHERE run_id=?', (run_id,)).fetchone()
            return json.loads(row[0]) if row else None

    def acquisition_diagnostics(self, run_id):
        with self._transaction() as db:
            row = db.execute('SELECT acquisition_diagnostics FROM monitor_runs WHERE id=?', (run_id,)).fetchone()
            return json.loads(row[0]) if row and row[0] else None

    def consumed(self, monitor_id, since):
        with self._transaction() as db:
            return {row[0] for row in db.execute('SELECT reference_hash FROM monitor_receipts WHERE monitor_id=? AND source_time>=?',
                                               (monitor_id, utc(since).isoformat()))}

    def acquisition_ledger(self, run, since):
        return RunDedupeLedger(self.path, run.id, run.monitor_id, utc(since).isoformat())

    def record_shard(self, run, shard, status, *, pages=0, records=0, unique=0, failure=None):
        with self._transaction() as db:
            if not self._owned(db, run):
                raise ValueError('Run claim no longer owned')
            db.execute('''INSERT INTO monitor_acquisition_shards
                (run_id,attempt,shard_id,start_at,end_at,depth,status,pages_read,records_read,
                 unique_records,completed_at,failure_category) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(run_id,attempt,shard_id) DO UPDATE SET
                status=excluded.status,pages_read=excluded.pages_read,records_read=excluded.records_read,
                unique_records=excluded.unique_records,completed_at=excluded.completed_at,
                failure_category=excluded.failure_category''',
                (run.id, run.attempts, shard.shard_id, shard.start.isoformat(), shard.end.isoformat(),
                 shard.depth, status, pages, records, unique,
                 datetime.now(timezone.utc).isoformat() if status in ('COMPLETE', 'FAILED', 'SPLIT') else None,
                 failure))

    def acquisition_shards(self, run_id):
        with self._transaction() as db:
            return [dict(row) for row in db.execute('''SELECT shard_id,start_at,end_at,depth,status,
                pages_read,records_read,unique_records,completed_at,failure_category,attempt
                FROM monitor_acquisition_shards WHERE run_id=? ORDER BY attempt,shard_id''', (run_id,))]

    def successful_metrics(self, monitor_id, before, limit=30):
        if not 1 <= limit <= 100:
            raise ValueError('Metrics history limit must be 1..100')
        with self._transaction() as db:
            return [json.loads(row[0]) for row in db.execute('''SELECT m.payload FROM monitor_run_metrics m
                JOIN monitor_runs r ON r.id=m.run_id WHERE r.monitor_id=? AND r.status='SUCCESS'
                AND r.window_end<=? ORDER BY r.window_end DESC LIMIT ?''',
                (monitor_id, utc(before).isoformat(), limit))]

    def successful_patterns(self, monitor_id, before, limit=5):
        with self._transaction() as db:
            run_ids = [row[0] for row in db.execute('''SELECT r.id FROM monitor_runs r
                JOIN monitor_run_metrics m ON m.run_id=r.id WHERE r.monitor_id=? AND r.status='SUCCESS'
                AND r.window_end<=? ORDER BY r.window_end DESC LIMIT ?''',
                (monitor_id, utc(before).isoformat(), limit))]
            return [{row['template_id']: json.loads(row['payload']) for row in db.execute(
                     'SELECT template_id,payload FROM monitor_pattern_metrics WHERE run_id=?', (run_id,))}
                    for run_id in run_ids]


class RunDedupeLedger:
    """Disk-backed reference hashes; one transaction per processed page."""

    def __init__(self, path, run_id, monitor_id, since):
        self.db = sqlite3.connect(path, timeout=10)
        self.run_id, self.monitor_id, self.since = run_id, monitor_id, since

    def __enter__(self):
        return self

    def __exit__(self, *args):
        if self.db.in_transaction:
            self.db.rollback()
        self.db.close()

    @contextmanager
    def page(self):
        self.db.execute('BEGIN IMMEDIATE')
        try:
            yield
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise

    def seen(self, key, source_time):
        cursor = self.db.execute('INSERT OR IGNORE INTO monitor_run_dedupe VALUES (?,?,?,0)',
                                 (self.run_id, key, source_time))
        return cursor.rowcount == 0

    def consumed(self, key):
        return self.db.execute('''SELECT 1 FROM monitor_receipts
            WHERE monitor_id=? AND reference_hash=? AND source_time>=?''',
            (self.monitor_id, key, self.since)).fetchone() is not None

    def owned(self, key):
        row = self.db.execute('SELECT owned FROM monitor_run_dedupe WHERE run_id=? AND reference_hash=?',
                              (self.run_id, key)).fetchone()
        return row is not None and row[0] == 1

    def mark_owned(self, keys):
        self.db.executemany('UPDATE monitor_run_dedupe SET owned=1 WHERE run_id=? AND reference_hash=?',
                            [(self.run_id, key) for key in keys])
