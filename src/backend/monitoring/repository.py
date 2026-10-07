"""Transactional monitoring persistence, separate from learning/policy stores.

The protocol is the migration boundary for a future PostgreSQL implementation.
One success transaction owns the result, reference receipts and watermark.
"""
from contextlib import contextmanager
from dataclasses import asdict, replace
from datetime import datetime, timedelta
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
    def recover_running(self, now: datetime) -> None: ...
    def succeed(self, run: MonitorRun, result: dict, counts: RunCounts, receipts: dict[str, str], *, now: datetime) -> None: ...
    def fail(self, run: MonitorRun, category: str, *, now: datetime) -> None: ...
    def history(self, monitor_id: str, limit: int = 100) -> list[MonitorRun]: ...
    def result(self, run_id: str) -> dict | None: ...
    def consumed(self, monitor_id: str, since: datetime) -> set[str]: ...


def encode(value):
    return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(',', ':'))


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
            if version not in (0, 1, 2):
                raise ValueError('Unsupported monitoring schema version')
            # Refuse accidental use of an existing policy/template database.
            names = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if names - {'monitors', 'monitor_runs', 'monitor_results', 'monitor_receipts'}:
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
                'CREATE INDEX IF NOT EXISTS monitor_due ON monitors(enabled, next_run_at)',
                'CREATE INDEX IF NOT EXISTS receipt_time ON monitor_receipts(monitor_id, source_time)',
            )
            for statement in statements:
                db.execute(statement)
            # Additive v2 migration: existing definitions, runs, results and
            # receipts are untouched. Learning stores are separate databases.
            columns = {row['name'] for row in db.execute('PRAGMA table_info(monitors)')}
            if 'archived' not in columns:
                db.execute('ALTER TABLE monitors ADD COLUMN archived INTEGER NOT NULL DEFAULT 0')
            db.execute('PRAGMA user_version=2')

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
                ORDER BY next_run_at,id''', (now.isoformat(),)).fetchall()
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
                    db.execute("""UPDATE monitor_runs SET status='RUNNING', claim_token=?, started_at=?,
                        finished_at=NULL, attempts=attempts+1, error_category=NULL,error_summary=NULL WHERE id=?""",
                        (token, now.isoformat(), run_id))
                else:
                    run_id = new_id()
                    db.execute('INSERT INTO monitor_runs VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', (
                        run_id, monitor.id, window.start.isoformat(), window.end.isoformat(), definition_json(monitor.definition),
                        now.isoformat(), None, now.isoformat(), 'RUNNING', token, 1, encode(asdict(RunCounts())), None, None, None))
                db.execute("UPDATE monitors SET status='RUNNING',updated_at=?,revision=revision+1 WHERE id=?", (now.isoformat(), monitor.id))
                return self._run(db.execute('SELECT * FROM monitor_runs WHERE id=?', (run_id,)).fetchone())
        return None

    @staticmethod
    def _owned(db, run):
        return db.execute("SELECT 1 FROM monitor_runs WHERE id=? AND status='RUNNING' AND claim_token=?",
                          (run.id, run.claim_token)).fetchone() is not None

    def succeed(self, run, result, counts, receipts, *, now):
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

    def fail(self, run, category, *, now):
        category = category if category in MESSAGES else 'UNKNOWN'
        now = utc(now)
        with self._transaction() as db:
            if not self._owned(db, run):
                return
            monitor = self._monitor(db.execute('SELECT * FROM monitors WHERE id=?', (run.monitor_id,)).fetchone())
            db.execute("UPDATE monitor_runs SET status='FAILED',finished_at=?,error_category=?,error_summary=? WHERE id=?",
                       (now.isoformat(), category, MESSAGES[category], run.id))
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

    def consumed(self, monitor_id, since):
        with self._transaction() as db:
            return {row[0] for row in db.execute('SELECT reference_hash FROM monitor_receipts WHERE monitor_id=? AND source_time>=?',
                                               (monitor_id, utc(since).isoformat()))}
