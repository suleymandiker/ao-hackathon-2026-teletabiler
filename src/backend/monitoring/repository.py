"""Transactional monitoring persistence, separate from learning/policy stores.

The protocol is the migration boundary for a future PostgreSQL implementation.
One success transaction owns the result, reference receipts and watermark.
"""
from contextlib import contextmanager
from dataclasses import asdict, dataclass, replace
from datetime import date, datetime, timedelta, timezone
from enum import Enum
import hashlib
import json
from pathlib import Path
import shutil
import sqlite3
from typing import Protocol

from monitoring.domain import (
    DeploymentMonitor, MonitorDefinition, MonitorRun, MonitorStatus, RunCounts,
    RunStatus, Window, new_id, next_window, safe_at, utc, ready_at,
)
from monitoring.errors import MESSAGES


class MonitoringSchemaError(ValueError):
    """Stable monitoring database ownership/version failure."""

    def __init__(self, message, reason_code):
        self.reason_code = (reason_code if reason_code in
                            ('OWNERSHIP_MISMATCH', 'UNSUPPORTED_SCHEMA_VERSION') else 'SCHEMA_INVALID')
        super().__init__(message)


_MONITORING_TABLES = frozenset({
    'monitors', 'monitor_runs', 'monitor_results', 'monitor_receipts',
    'monitor_run_metrics', 'monitor_pattern_metrics', 'monitor_acquisition_shards',
    'monitor_run_dedupe', 'monitor_findings', 'monitor_baseline_state',
})


def _application_table_names(db):
    """Exclude SQLite-reserved metadata, while retaining every application table."""
    return {row[0] for row in db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND lower(name) NOT GLOB 'sqlite_*'")}


def _validate_monitoring_ownership(db):
    if _application_table_names(db) - _MONITORING_TABLES:
        raise MonitoringSchemaError('Monitoring requires its own database', 'OWNERSHIP_MISMATCH')


class MonitorRepository(Protocol):
    def create(self, definition: MonitorDefinition, *, enabled: bool, now: datetime) -> DeploymentMonitor: ...
    def get(self, monitor_id: str) -> DeploymentMonitor: ...
    def list(self, *, include_archived: bool = False) -> list[DeploymentMonitor]: ...
    def update(self, monitor_id: str, definition: MonitorDefinition, *, revision: int, now: datetime) -> DeploymentMonitor: ...
    def set_enabled(self, monitor_id: str, enabled: bool, *, now: datetime) -> DeploymentMonitor: ...
    def request_run_now(self, monitor_id: str, *, now: datetime) -> DeploymentMonitor: ...
    def resume_now(self, monitor_id: str, *, now: datetime) -> DeploymentMonitor: ...
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


MAX_FINDINGS_PER_RUN = 10
MAX_BASELINE_BYTES = 2_000_000
METRIC_BASELINE_FIELDS = ('total_physical_logs', 'pods', 'containers',
                          'pod_counts_exact', 'container_counts_exact', 'severity_counts')


def _baseline_metric(value):
    return {key: value[key] for key in METRIC_BASELINE_FIELDS if key in value}


def _baseline_patterns(values):
    from evidence_redaction import redact_text
    return {str(row['template_id']): {'template_id': str(row['template_id']),
            'count': int(row['count']),
            'template': redact_text(str(row.get('template') or ''))[:128]}
            for row in values}


def _compact_findings(run, result, now):
    from evidence_redaction import redact_text
    candidates = []
    for row in result.get('incidents', ()):
        identifier = hashlib.sha256(str(row.get('incident_id') or '').encode()).hexdigest()[:32]
        candidates.append(('incident', identifier, row, 0))
    for row in result.get('signals', ()):
        if row.get('qualified') is True and row.get('monitor_anomaly'):
            identifier = hashlib.sha256(str(row.get('signal_id') or '').encode()).hexdigest()[:32]
            candidates.append(('anomaly', identifier, row, 1))
    candidates.sort(key=lambda item: (item[3], item[1]))
    output = []
    for kind, identifier, row, _ in candidates[:MAX_FINDINGS_PER_RUN]:
        anomaly_kind = (row.get('monitor_anomaly') or {}).get('kind')
        allowed_kinds = frozenset({'total_volume', 'log_rate', 'pod_volume', 'container_volume',
                                   'pattern_frequency', 'new_pattern', 'pattern_disappearance',
                                   'error_severity', 'pod_error_concentration',
                                   'container_error_concentration'})
        title = ('Incident' if kind == 'incident' else
                 anomaly_kind if isinstance(anomaly_kind, str) and anomaly_kind in allowed_kinds else 'Anomaly')
        safe_title = redact_text(title)[:160]
        reason = ((row.get('monitor_anomaly') or {}).get('rule') if kind == 'anomaly' else None)
        allowed_rules = frozenset({'median_mad_and_ratio',
                                   'new_template_and_absent_from_recent_history',
                                   'at_least_90_percent_of_parsed_errors'})
        reason = reason if isinstance(reason, str) and reason in allowed_rules else None
        evidence = row.get('representative_evidence') or ()
        locators = []
        for item in evidence[:3]:
            if not isinstance(item, dict):
                continue
            index, document = item.get('index'), item.get('document_id')
            if isinstance(index, str) and isinstance(document, str):
                locators.append({'index': redact_text(index)[:160],
                                 'document_id': redact_text(document)[:160]})
        digest = hashlib.sha256(f'{run.id}:{kind}:{identifier}'.encode()).hexdigest()
        output.append((digest, run.monitor_id, run.id, run.window.start.isoformat(),
                       run.window.end.isoformat(), kind,
                       row.get('severity') if row.get('severity') in
                       ('CRITICAL', 'ERROR', 'WARNING', 'WARN', 'INFO', 'DEBUG') else 'UNKNOWN',
                       safe_title, reason, int(row.get('count') or 1),
                       str(row.get('first_seen_ms') or '') or None,
                       str(row.get('last_seen_ms') or '') or None,
                       hashlib.sha256(str(row.get('template_id') or '').encode()).hexdigest()[:32]
                       if row.get('template_id') else None,
                       identifier if kind == 'anomaly' else None,
                       identifier if kind == 'incident' else None,
                       encode(locators), now.isoformat()))
    return output


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
    def __init__(self, path, *, initialize=True, compact_mode=False):
        self.path = Path(path).resolve()
        self.compact_mode = compact_mode
        if initialize:
            self.initialize()
        else:
            with self._read_connection() as db:
                version = db.execute('PRAGMA user_version').fetchone()[0]
                if version not in (4, 5, 6):
                    raise MonitoringSchemaError('Unsupported monitoring schema version',
                                                'UNSUPPORTED_SCHEMA_VERSION')

    def initialize(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        # Journal mode is changed only by the explicit initializer, never by UI reads.
        with sqlite3.connect(self.path, timeout=10) as setup:
            setup.execute('PRAGMA busy_timeout=10000')
            version = setup.execute('PRAGMA user_version').fetchone()[0]
            if version not in (0, 1, 2, 3, 4, 5, 6):
                raise MonitoringSchemaError('Unsupported monitoring schema version',
                                            'UNSUPPORTED_SCHEMA_VERSION')
            _validate_monitoring_ownership(setup)
            setup.execute('PRAGMA journal_mode=WAL')
            setup.execute('PRAGMA synchronous=NORMAL')
        with self._transaction() as db:
            version = db.execute('PRAGMA user_version').fetchone()[0]
            if version not in (0, 1, 2, 3, 4, 5, 6):
                raise MonitoringSchemaError('Unsupported monitoring schema version',
                                            'UNSUPPORTED_SCHEMA_VERSION')
            _validate_monitoring_ownership(db)
            if version in (5, 6):
                columns = {row['name'] for row in db.execute('PRAGMA table_info(monitor_runs)')}
                if 'reason_streak' not in columns:
                    db.execute('ALTER TABLE monitor_runs ADD COLUMN reason_streak INTEGER NOT NULL DEFAULT 0')
                self._migrate_operator_controls(db)
                return
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
            monitor_columns = {row['name'] for row in db.execute('PRAGMA table_info(monitors)')}
            latest_fields = {
                'latest_run_id': 'TEXT', 'latest_status': 'TEXT',
                'latest_completed_at': 'TEXT', 'latest_physical_logs': 'INTEGER',
                'latest_logical_events': 'INTEGER', 'latest_pattern_count': 'INTEGER',
                'latest_signal_count': 'INTEGER', 'latest_qualified_signal_count': 'INTEGER',
                'latest_incident_count': 'INTEGER', 'latest_finding_count': 'INTEGER',
                'latest_duration_ms': 'INTEGER',
            }
            for name, kind in latest_fields.items():
                if name not in monitor_columns:
                    db.execute(f'ALTER TABLE monitors ADD COLUMN {name} {kind}')
            run_columns = {row['name'] for row in db.execute('PRAGMA table_info(monitor_runs)')}
            run_fields = {
                'physical_logs': 'INTEGER', 'pattern_count': 'INTEGER',
                'duration_ms': 'INTEGER', 'error_stage': 'TEXT',
                'safe_reason_code': 'TEXT',
                'reason_streak': 'INTEGER NOT NULL DEFAULT 0',
            }
            for name, kind in run_fields.items():
                if name not in run_columns:
                    db.execute(f'ALTER TABLE monitor_runs ADD COLUMN {name} {kind}')
            db.execute('''CREATE TABLE IF NOT EXISTS monitor_findings (
                id TEXT PRIMARY KEY, monitor_id TEXT NOT NULL REFERENCES monitors(id),
                run_id TEXT REFERENCES monitor_runs(id) ON DELETE SET NULL,
                window_start TEXT NOT NULL, window_end TEXT NOT NULL,
                kind TEXT NOT NULL, severity TEXT NOT NULL, title TEXT NOT NULL,
                reason_code TEXT, occurrence_count INTEGER NOT NULL,
                first_seen TEXT, last_seen TEXT,
                template_id TEXT, signal_id TEXT, incident_id TEXT,
                source_locators TEXT NOT NULL, created_at TEXT NOT NULL)''')
            db.execute('''CREATE TABLE IF NOT EXISTS monitor_baseline_state (
                monitor_id TEXT PRIMARY KEY REFERENCES monitors(id),
                version INTEGER NOT NULL, payload TEXT NOT NULL, updated_at TEXT NOT NULL)''')
            db.execute('CREATE INDEX IF NOT EXISTS monitor_runs_recent ON monitor_runs(monitor_id,window_start DESC)')
            db.execute('CREATE INDEX IF NOT EXISTS monitor_findings_recent ON monitor_findings(monitor_id,created_at DESC)')
            if version > 0:
                self._backfill_compact(db)
            self._migrate_operator_controls(db)

    @staticmethod
    def _migrate_operator_controls(db):
        """v6: bounded intent plus a pinned interval; no historical timestamp edits."""
        columns = {row['name'] for row in db.execute('PRAGMA table_info(monitors)')}
        for name in ('manual_requested_at', 'pending_window_start', 'pending_window_end'):
            if name not in columns:
                db.execute(f'ALTER TABLE monitors ADD COLUMN {name} TEXT')
        if db.execute('PRAGMA user_version').fetchone()[0] < 6:
            # A legacy failed/running first window must never be replaced by a
            # fresh-window manual request, even before any watermark exists.
            db.execute('''UPDATE monitors SET (pending_window_start,pending_window_end) =
                (SELECT window_start,window_end FROM monitor_runs r
                 WHERE r.monitor_id=monitors.id AND r.status IN ('FAILED','RUNNING')
                 AND r.window_start=COALESCE(monitors.last_successful_end,
                                            json_extract(monitors.definition,'$.initial_start'))
                 ORDER BY r.window_start DESC LIMIT 1)
                WHERE pending_window_start IS NULL''')
            db.execute('PRAGMA user_version=6')

    @staticmethod
    def _backfill_compact(db):
        for monitor_id, in db.execute('SELECT id FROM monitors').fetchall():
            metrics = [dict(window_end=row['window_end'], payload=_baseline_metric(json.loads(row['payload'])))
                       for row in db.execute('''SELECT r.window_end,m.payload FROM monitor_run_metrics m
                           JOIN monitor_runs r ON r.id=m.run_id WHERE r.monitor_id=? AND r.status='SUCCESS'
                           ORDER BY r.window_end DESC LIMIT 30''', (monitor_id,))]
            pattern_rows = db.execute('''SELECT r.id,r.window_end FROM monitor_runs r
                JOIN monitor_run_metrics m ON m.run_id=r.id WHERE r.monitor_id=? AND r.status='SUCCESS'
                ORDER BY r.window_end DESC LIMIT 5''', (monitor_id,)).fetchall()
            patterns = [dict(window_end=row['window_end'], payload=_baseline_patterns(
                        [json.loads(item['payload']) for item in db.execute(
                            'SELECT payload FROM monitor_pattern_metrics WHERE run_id=?', (row['id'],))]))
                        for row in pattern_rows]
            if metrics:
                db.execute('INSERT OR IGNORE INTO monitor_baseline_state VALUES (?,?,?,?)',
                           (monitor_id, 1, encode(dict(metrics=metrics, patterns=patterns)),
                            metrics[0]['window_end']))
            historical = db.execute('''SELECT id,counts,started_at,finished_at FROM monitor_runs
                WHERE monitor_id=?''', (monitor_id,)).fetchall()
            for run_row in historical:
                counts = RunCounts(**json.loads(run_row['counts']))
                duration = (max(0, int((instant(run_row['finished_at']) -
                            instant(run_row['started_at'])).total_seconds() * 1000))
                            if run_row['finished_at'] else None)
                db.execute('''UPDATE monitor_runs SET physical_logs=?,duration_ms=? WHERE id=?''',
                           (counts.events_retrieved, duration, run_row['id']))
            latest = db.execute('''SELECT * FROM monitor_runs WHERE monitor_id=?
                ORDER BY window_start DESC LIMIT 1''', (monitor_id,)).fetchone()
            if latest:
                counts = RunCounts(**json.loads(latest['counts']))
                result_row = db.execute('SELECT payload FROM monitor_results WHERE run_id=?',
                                        (latest['id'],)).fetchone()
                latest_result = json.loads(result_row[0]) if result_row else None
                pattern_count = ((latest_result.get('source_summary') or {}).get('pattern_count')
                                 if latest_result else None)
                if latest_result and latest['status'] == 'SUCCESS':
                    findings = _compact_findings(SQLiteMonitorRepository._run(latest), latest_result,
                                                 instant(latest['finished_at']))
                    db.executemany('INSERT OR IGNORE INTO monitor_findings VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                                   findings)
                else:
                    findings = ()
                db.execute('''UPDATE monitors SET latest_run_id=?,latest_status=?,latest_completed_at=?,
                    latest_physical_logs=?,latest_logical_events=?,latest_signal_count=?,
                    latest_qualified_signal_count=?,latest_incident_count=?,latest_pattern_count=?,
                    latest_finding_count=?,latest_duration_ms=? WHERE id=?''',
                    (latest['id'], latest['status'], latest['finished_at'], counts.events_retrieved,
                     counts.logical_events, counts.signal_candidates, counts.qualified_signals,
                     counts.incidents, pattern_count, len(findings), latest['duration_ms'], monitor_id))

    @contextmanager
    def _read_connection(self):
        db = sqlite3.connect(self.path.as_uri() + '?mode=ro', uri=True, timeout=10)
        db.row_factory = sqlite3.Row
        try:
            db.execute('PRAGMA foreign_keys=ON')
            db.execute('PRAGMA busy_timeout=10000')
            db.execute('PRAGMA synchronous=NORMAL')
            db.execute('PRAGMA query_only=ON')
            yield db
        finally:
            db.close()

    @contextmanager
    def _transaction(self):
        db = sqlite3.connect(self.path, timeout=10)
        db.row_factory = sqlite3.Row
        try:
            db.execute('PRAGMA foreign_keys=ON')
            db.execute('PRAGMA busy_timeout=10000')
            db.execute('PRAGMA synchronous=NORMAL')
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
            instant(row['updated_at']), row['revision'], row['error_category'], row['error_summary'], bool(row['archived']),
            instant(row['manual_requested_at']) if 'manual_requested_at' in row.keys() else None,
            Window(instant(row['pending_window_start']), instant(row['pending_window_end']))
            if 'pending_window_start' in row.keys() and row['pending_window_start'] else None)

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
        with self._read_connection() as db:
            row = db.execute('SELECT * FROM monitors WHERE id=?', (monitor_id,)).fetchone()
            if row is None:
                raise KeyError('Monitor not found')
            return self._monitor(row)

    def list(self, *, include_archived=False):
        with self._read_connection() as db:
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
            established = has_history or old.last_successful_end or old.manual_requested_at or old.pending_window
            if established and replace(definition, name=old.definition.name,
                                       interval_seconds=old.definition.interval_seconds) != old.definition:
                raise ValueError('After the first run, only name and interval can change; create a new monitor for a new scope or policy')
            db.execute('UPDATE monitors SET definition=?, updated_at=?, revision=revision+1 WHERE id=?',
                       (definition_json(definition), utc(now).isoformat(), monitor_id))
        return self.get(monitor_id)

    def set_enabled(self, monitor_id, enabled, *, now):
        """Lifecycle-only compatibility API. Operator resume uses resume_now()."""
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
                ELSE 'ACTIVE' END, manual_requested_at=CASE WHEN ?=0 THEN NULL ELSE manual_requested_at END,
                updated_at=?, revision=revision+1 WHERE id=?''',
                (int(enabled), int(enabled), monitor_id, int(enabled), now, monitor_id))
        return self.get(monitor_id)

    def request_run_now(self, monitor_id, *, now):
        """Run/Retry Now: coalesce durable intent without changing cadence."""
        return self._request_run(monitor_id, now=now, resume=False)

    def resume_now(self, monitor_id, *, now):
        """Atomically enable and request work. An owned run simply continues."""
        return self._request_run(monitor_id, now=now, resume=True)

    def _request_run(self, monitor_id, *, now, resume):
        stamp = utc(now).isoformat()
        with self._transaction() as db:
            row = db.execute('SELECT * FROM monitors WHERE id=?', (monitor_id,)).fetchone()
            if row is None:
                raise KeyError('Monitor not found')
            if row['archived'] or (not row['enabled'] and not resume):
                raise ValueError('Monitor must be active; resume a paused monitor first')
            running = db.execute("SELECT 1 FROM monitor_runs WHERE monitor_id=? AND status='RUNNING'",
                                 (monitor_id,)).fetchone()
            if resume and not row['enabled']:
                db.execute('''UPDATE monitors SET enabled=1,status=?,manual_requested_at=?,
                    updated_at=?,revision=revision+1 WHERE id=?''',
                    ('RUNNING' if running else 'ACTIVE', None if running else stamp, stamp, monitor_id))
            elif not running and row['manual_requested_at'] is None:
                db.execute('''UPDATE monitors SET manual_requested_at=?,updated_at=?,
                    revision=revision+1 WHERE id=?''', (stamp, stamp, monitor_id))
        return self.get(monitor_id)

    def archive(self, monitor_id, *, now):
        """Stop future claims, retaining audit evidence and any in-flight run.

        Like pause, an owned run may finish persisting its result. It cannot
        reactivate the monitor. Repeated archive requests are harmless.
        """
        with self._transaction() as db:
            db.execute("""UPDATE monitors SET archived=1,enabled=0,status='ARCHIVED',
                manual_requested_at=NULL,updated_at=?,revision=revision+1 WHERE id=? AND archived=0""",
                (utc(now).isoformat(), monitor_id))
        return self.get(monitor_id)

    def claim(self, now):
        now = utc(now)
        with self._transaction() as db:
            rows = db.execute('''SELECT * FROM monitors WHERE enabled=1 AND archived=0
                AND (next_run_at<=? OR manual_requested_at IS NOT NULL)
                AND NOT EXISTS(SELECT 1 FROM monitor_runs WHERE monitor_id=monitors.id AND status='RUNNING')
                ORDER BY updated_at,next_run_at,id''', (now.isoformat(),)).fetchall()
            for row in rows:
                monitor = self._monitor(row)
                window = next_window(monitor)
                if ready_at(monitor, now) > now:
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
                db.execute("""UPDATE monitors SET status='RUNNING',manual_requested_at=NULL,
                    pending_window_start=?,pending_window_end=?,updated_at=?,revision=revision+1 WHERE id=?""",
                    (window.start.isoformat(), window.end.isoformat(), now.isoformat(), monitor.id))
                return self._run(db.execute('SELECT * FROM monitor_runs WHERE id=?', (run_id,)).fetchone())
        return None

    def schedule_summary(self, now):
        """Read scheduling state without claiming runs or changing persisted state."""
        now = utc(now)
        with self._read_connection() as db:
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
                ready = ready_at(monitor, now)
                if ready <= now:
                    due += 1
                if next_due_at is None or ready < next_due_at:
                    next_due_at = ready
            return ScheduleSummary(enabled, due, next_due_at)

    @staticmethod
    def _owned(db, run):
        return db.execute("SELECT 1 FROM monitor_runs WHERE id=? AND status='RUNNING' AND claim_token=?",
                          (run.id, run.claim_token)).fetchone() is not None

    def succeed(self, run, result, counts, receipts, *, now, metrics=None, pattern_metrics=()):
        if self.compact_mode and metrics is not None:
            return self._succeed_compact(run, result, counts, receipts, now=now,
                                         metrics=metrics, pattern_metrics=pattern_metrics)
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
                pending_window_start=NULL,pending_window_end=NULL,
                error_category=NULL,error_summary=NULL,updated_at=?,revision=revision+1 WHERE id=?''',
                (run.window.end.isoformat(), due.isoformat(),
                 'ARCHIVED' if monitor.archived else 'ACTIVE' if monitor.enabled else 'PAUSED', now.isoformat(), run.monitor_id))
            # Only overlap receipts can be needed again; history/results are durable.
            cutoff = run.window.end - timedelta(seconds=monitor.definition.overlap_seconds)
            db.execute('DELETE FROM monitor_receipts WHERE monitor_id=? AND source_time<?', (run.monitor_id, cutoff.isoformat()))

    def _succeed_compact(self, run, result, counts, receipts, *, now, metrics, pattern_metrics):
        now = utc(now)
        patterns = tuple(pattern_metrics)
        findings = _compact_findings(run, result, now)
        metric_payload = _baseline_metric(metrics)
        pattern_payload = _baseline_patterns(patterns)
        with self._transaction() as db:
            if not self._owned(db, run):
                raise ValueError('Run claim no longer owned')
            monitor = self._monitor(db.execute('SELECT * FROM monitors WHERE id=?',
                                               (run.monitor_id,)).fetchone())
            if next_window(monitor) != run.window:
                raise ValueError('Watermark does not match claimed window')
            row = db.execute('SELECT payload FROM monitor_baseline_state WHERE monitor_id=?',
                             (run.monitor_id,)).fetchone()
            baseline = json.loads(row[0]) if row else {'metrics': [], 'patterns': []}
            baseline['metrics'] = ([{'window_end': run.window.end.isoformat(), 'payload': metric_payload}]
                                   + baseline['metrics'])[:30]
            baseline['patterns'] = ([{'window_end': run.window.end.isoformat(), 'payload': pattern_payload}]
                                    + baseline['patterns'])[:5]
            baseline_json = encode(baseline)
            if len(baseline_json.encode('utf-8')) > MAX_BASELINE_BYTES:
                raise ValueError('Monitoring baseline state limit reached')
            duration_ms = max(0, int((now - run.actual_started_at).total_seconds() * 1000))
            db.execute('''UPDATE monitor_runs SET status='SUCCESS',finished_at=?,counts=?,
                physical_logs=?,pattern_count=?,duration_ms=?,result_reference=NULL,
                error_category=NULL,error_summary=NULL,acquisition_diagnostics=NULL,
                error_stage=NULL,safe_reason_code=NULL,reason_streak=0 WHERE id=?''',
                (now.isoformat(), encode(asdict(counts)), int(metrics['total_physical_logs']),
                 len(patterns), duration_ms, run.id))
            following = Window(run.window.end, run.window.end + timedelta(seconds=monitor.definition.window_seconds))
            due = now if safe_at(following, monitor.definition) <= now else max(
                safe_at(following, monitor.definition), now + timedelta(seconds=monitor.definition.interval_seconds))
            db.execute('''UPDATE monitors SET last_successful_end=?,next_run_at=?,status=?,
                pending_window_start=NULL,pending_window_end=NULL,
                error_category=NULL,error_summary=NULL,updated_at=?,revision=revision+1,
                latest_run_id=?,latest_status='SUCCESS',latest_completed_at=?,latest_physical_logs=?,
                latest_logical_events=?,latest_pattern_count=?,latest_signal_count=?,
                latest_qualified_signal_count=?,latest_incident_count=?,latest_finding_count=?,
                latest_duration_ms=? WHERE id=?''',
                (run.window.end.isoformat(), due.isoformat(),
                 'ARCHIVED' if monitor.archived else 'ACTIVE' if monitor.enabled else 'PAUSED',
                 now.isoformat(), run.id, now.isoformat(), int(metrics['total_physical_logs']),
                 counts.logical_events, len(patterns), counts.signal_candidates,
                 counts.qualified_signals, counts.incidents, len(findings), duration_ms, run.monitor_id))
            db.execute('''INSERT INTO monitor_baseline_state VALUES (?,?,?,?)
                ON CONFLICT(monitor_id) DO UPDATE SET version=excluded.version,
                payload=excluded.payload,updated_at=excluded.updated_at''',
                (run.monitor_id, 1, baseline_json, now.isoformat()))
            if findings:
                db.executemany('''INSERT INTO monitor_findings VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', findings)
            if receipts:
                db.executemany('INSERT OR IGNORE INTO monitor_receipts VALUES (?,?,?)',
                               ((run.monitor_id, key, stamp) for key, stamp in receipts.items()))
            cutoff = run.window.end - timedelta(seconds=monitor.definition.overlap_seconds)
            db.execute('DELETE FROM monitor_receipts WHERE monitor_id=? AND source_time<?',
                       (run.monitor_id, cutoff.isoformat()))

    def fail(self, run, category, *, now, diagnostics=None):
        category = category if category in MESSAGES else 'UNKNOWN'
        payload = None
        if diagnostics is not None:
            # Executor already removes runtime credentials. Reapply the existing
            # safe projection at the storage boundary for alternate callers.
            from opensearch_application import presentation_result
            safe = {key: value for key, value in diagnostics.items()
                    if key not in ('effective_query', 'query', 'request_body', 'response_body')}
            payload = encode(presentation_result({}, safe, None)['source_summary'])
        now = utc(now)
        from monitoring.errors import (ACQUISITION_REASONS, NON_RETRYABLE_ACQUISITION_REASONS,
                                       PIPELINE_STAGES)
        with self._transaction() as db:
            if not self._owned(db, run):
                return
            if not self.compact_mode:
                db.execute('DELETE FROM monitor_run_dedupe WHERE run_id=?', (run.id,))
            monitor = self._monitor(db.execute('SELECT * FROM monitors WHERE id=?', (run.monitor_id,)).fetchone())
            previous = db.execute('SELECT safe_reason_code,reason_streak FROM monitor_runs WHERE id=?',
                                  (run.id,)).fetchone()
            reason = diagnostics.get('reason_code') if isinstance(diagnostics, dict) else None
            if category.startswith('OPENSEARCH') and reason not in ACQUISITION_REASONS:
                reason = 'QUERY_FAILURE_UNKNOWN'
            elif reason not in ACQUISITION_REASONS:
                reason = None
            stage = (diagnostics.get('error_stage') or diagnostics.get('pipeline_stage')) if diagnostics else None
            stage = stage if stage in PIPELINE_STAGES else None
            streak = ((previous['reason_streak'] + 1 if previous['safe_reason_code'] == reason else 1)
                      if reason in NON_RETRYABLE_ACQUISITION_REASONS else 0)
            blocked = streak >= 3
            db.execute("""UPDATE monitor_runs SET status='FAILED',finished_at=?,error_category=?,error_summary=?,
                       acquisition_diagnostics=?,error_stage=?,safe_reason_code=?,reason_streak=? WHERE id=?""",
                       (now.isoformat(), category, MESSAGES[category], payload,
                        stage, reason, streak,
                        run.id))
            db.execute('''UPDATE monitors SET status=?,next_run_at=?,error_category=?,error_summary=?,updated_at=?,
                revision=revision+1,latest_run_id=?,latest_status='FAILED',latest_completed_at=? WHERE id=?''',
                ('ARCHIVED' if monitor.archived else 'BLOCKED' if blocked and monitor.enabled
                 else 'ERROR' if monitor.enabled else 'PAUSED',
                 (now + timedelta(seconds=max(3600, monitor.definition.interval_seconds)
                                  if blocked else monitor.definition.interval_seconds)).isoformat(), category,
                 MESSAGES[category], now.isoformat(), run.id, now.isoformat(), run.monitor_id))

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
        with self._read_connection() as db:
            return [self._run(row) for row in db.execute('SELECT * FROM monitor_runs WHERE monitor_id=? ORDER BY window_start DESC LIMIT ?', (monitor_id, limit))]

    @staticmethod
    def _summary(row):
        return dict(monitor=SQLiteMonitorRepository._monitor(row),
                    latest_run_id=row['latest_run_id'], latest_status=row['latest_status'],
                    latest_completed_at=instant(row['latest_completed_at']),
                    physical_logs=row['latest_physical_logs'], logical_events=row['latest_logical_events'],
                    patterns=row['latest_pattern_count'], signals=row['latest_signal_count'],
                    qualified_signals=row['latest_qualified_signal_count'],
                    incidents=row['latest_incident_count'], findings=row['latest_finding_count'],
                    duration_ms=row['latest_duration_ms'],
                    safe_reason_code=row['latest_reason_code'])

    def list_monitor_summaries(self, *, include_archived=False):
        with self._read_connection() as db:
            return [self._summary(row) for row in db.execute(
                '''SELECT monitors.*, (SELECT safe_reason_code FROM monitor_runs
                    WHERE id=monitors.latest_run_id) AS latest_reason_code
                    FROM monitors WHERE (? OR archived=0) ORDER BY created_at,id''',
                (include_archived,))]

    def get_monitor_summary(self, monitor_id):
        with self._read_connection() as db:
            row = db.execute('''SELECT monitors.*, (SELECT safe_reason_code FROM monitor_runs
                WHERE id=monitors.latest_run_id) AS latest_reason_code
                FROM monitors WHERE id=?''', (monitor_id,)).fetchone()
            if row is None:
                raise KeyError('Monitor not found')
            return self._summary(row)

    def recent_runs(self, monitor_id, *, limit=10, before=None):
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError('Recent run page must contain 1..100 rows')
        with self._read_connection() as db:
            if before is None:
                rows = db.execute('''SELECT * FROM monitor_runs WHERE monitor_id=?
                    ORDER BY window_start DESC,id DESC LIMIT ?''', (monitor_id, limit))
            else:
                rows = db.execute('''SELECT * FROM monitor_runs WHERE monitor_id=? AND
                    (window_start,id)<(?,?) ORDER BY window_start DESC,id DESC LIMIT ?''',
                    (monitor_id, before[0], before[1], limit))
            return [self._run(row) for row in rows]

    def latest_finding(self, monitor_id):
        with self._read_connection() as db:
            row = db.execute('''SELECT kind,severity,title,reason_code,occurrence_count,
                window_start,window_end FROM monitor_findings WHERE monitor_id=?
                ORDER BY created_at DESC, CASE kind WHEN 'incident' THEN 0 ELSE 1 END,
                    severity DESC,id LIMIT 1''', (monitor_id,)).fetchone()
            return dict(row) if row else None

    def result(self, run_id):
        with self._read_connection() as db:
            row = db.execute('SELECT payload FROM monitor_results WHERE run_id=?', (run_id,)).fetchone()
            return json.loads(row[0]) if row else None

    def acquisition_diagnostics(self, run_id):
        with self._read_connection() as db:
            row = db.execute('SELECT acquisition_diagnostics FROM monitor_runs WHERE id=?', (run_id,)).fetchone()
            return json.loads(row[0]) if row and row[0] else None

    def consumed(self, monitor_id, since):
        with self._read_connection() as db:
            return {row[0] for row in db.execute('SELECT reference_hash FROM monitor_receipts WHERE monitor_id=? AND source_time>=?',
                                               (monitor_id, utc(since).isoformat()))}

    def acquisition_ledger(self, run, since):
        if self.compact_mode:
            from monitoring.run_ledger import IsolatedRunLedger
            cutoff = run.window.end - timedelta(seconds=run.definition.overlap_seconds)
            return IsolatedRunLedger(self.path, run.id, run.monitor_id, utc(since).isoformat(),
                                     cutoff.isoformat())
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
        with self._read_connection() as db:
            return [dict(row) for row in db.execute('''SELECT shard_id,start_at,end_at,depth,status,
                pages_read,records_read,unique_records,completed_at,failure_category,attempt
                FROM monitor_acquisition_shards WHERE run_id=? ORDER BY attempt,shard_id''', (run_id,))]

    def successful_metrics(self, monitor_id, before, limit=30):
        if not 1 <= limit <= 100:
            raise ValueError('Metrics history limit must be 1..100')
        with self._read_connection() as db:
            state = db.execute('SELECT payload FROM monitor_baseline_state WHERE monitor_id=?',
                               (monitor_id,)).fetchone()
            if state:
                entries = json.loads(state[0])['metrics']
                return [item['payload'] for item in entries if item['window_end'] <= utc(before).isoformat()][:limit]
            return [json.loads(row[0]) for row in db.execute('''SELECT m.payload FROM monitor_run_metrics m
                JOIN monitor_runs r ON r.id=m.run_id WHERE r.monitor_id=? AND r.status='SUCCESS'
                AND r.window_end<=? ORDER BY r.window_end DESC LIMIT ?''',
                (monitor_id, utc(before).isoformat(), limit))]

    def successful_patterns(self, monitor_id, before, limit=5):
        with self._read_connection() as db:
            state = db.execute('SELECT payload FROM monitor_baseline_state WHERE monitor_id=?',
                               (monitor_id,)).fetchone()
            if state:
                entries = json.loads(state[0])['patterns']
                return [item['payload'] for item in entries if item['window_end'] <= utc(before).isoformat()][:limit]
            run_ids = [row[0] for row in db.execute('''SELECT r.id FROM monitor_runs r
                JOIN monitor_run_metrics m ON m.run_id=r.id WHERE r.monitor_id=? AND r.status='SUCCESS'
                AND r.window_end<=? ORDER BY r.window_end DESC LIMIT ?''',
                (monitor_id, utc(before).isoformat(), limit))]
            return [{row['template_id']: json.loads(row['payload']) for row in db.execute(
                     'SELECT template_id,payload FROM monitor_pattern_metrics WHERE run_id=?', (run_id,))}
                    for run_id in run_ids]

    def maintenance(self, now, *, optimize=False, batch=100):
        """Bounded terminal-history cleanup; never touches learning stores or active runs."""
        if type(batch) is not int or not 1 <= batch <= 500:
            raise ValueError('Maintenance batch must contain 1..500 rows')
        now = utc(now)
        run_cutoff = (now - timedelta(days=7)).isoformat()
        finding_cutoff = (now - timedelta(days=30)).isoformat()
        detail_cutoff = (now - timedelta(days=3)).isoformat()
        with self._transaction() as db:
            ids = [row[0] for row in db.execute('''SELECT id FROM monitor_runs
                WHERE status!='RUNNING' AND finished_at<?
                AND NOT EXISTS(SELECT 1 FROM monitors m WHERE m.id=monitor_runs.monitor_id
                    AND m.pending_window_start=monitor_runs.window_start
                    AND m.pending_window_end=monitor_runs.window_end)
                ORDER BY finished_at LIMIT ?''',
                (run_cutoff, batch))]
            for run_id in ids:
                db.execute('DELETE FROM monitor_results WHERE run_id=?', (run_id,))
                db.execute('DELETE FROM monitor_pattern_metrics WHERE run_id=?', (run_id,))
                db.execute('DELETE FROM monitor_run_metrics WHERE run_id=?', (run_id,))
                db.execute('DELETE FROM monitor_acquisition_shards WHERE run_id=?', (run_id,))
                db.execute('DELETE FROM monitor_run_dedupe WHERE run_id=?', (run_id,))
                db.execute('UPDATE monitor_findings SET run_id=NULL WHERE run_id=?', (run_id,))
                db.execute('DELETE FROM monitor_runs WHERE id=?', (run_id,))
            findings = [row[0] for row in db.execute('''SELECT id FROM monitor_findings
                WHERE created_at<? ORDER BY created_at LIMIT ?''', (finding_cutoff, batch))]
            db.executemany('DELETE FROM monitor_findings WHERE id=?', ((key,) for key in findings))
            failures = [row[0] for row in db.execute('''SELECT id FROM monitor_runs
                WHERE status='FAILED' AND finished_at<? AND acquisition_diagnostics IS NOT NULL
                ORDER BY finished_at LIMIT ?''', (detail_cutoff, batch))]
            db.executemany('UPDATE monitor_runs SET acquisition_diagnostics=NULL WHERE id=?',
                           ((key,) for key in failures))
            old_shards = [tuple(row) for row in db.execute('''SELECT s.run_id,s.attempt,s.shard_id
                FROM monitor_acquisition_shards s JOIN monitor_runs r ON r.id=s.run_id
                WHERE r.status!='RUNNING' AND r.finished_at<? LIMIT ?''', (detail_cutoff, batch))]
            db.executemany('''DELETE FROM monitor_acquisition_shards
                WHERE run_id=? AND attempt=? AND shard_id=?''', old_shards)
            for row in db.execute('''SELECT id,last_successful_end,definition FROM monitors
                                   WHERE last_successful_end IS NOT NULL LIMIT ?''', (batch,)):
                definition = read_definition(row['definition'])
                cutoff = (instant(row['last_successful_end']) -
                          timedelta(seconds=definition.overlap_seconds)).isoformat()
                stale = [entry[0] for entry in db.execute('''SELECT reference_hash FROM monitor_receipts
                    WHERE monitor_id=? AND source_time<? LIMIT ?''', (row['id'], cutoff, batch))]
                db.executemany('DELETE FROM monitor_receipts WHERE monitor_id=? AND reference_hash=?',
                               ((row['id'], key) for key in stale))
        if optimize:
            with sqlite3.connect(self.path, timeout=10) as db:
                db.execute('PRAGMA busy_timeout=10000')
                db.execute('PRAGMA optimize')
                db.execute('PRAGMA wal_checkpoint(PASSIVE)')
                mode = db.execute('PRAGMA auto_vacuum').fetchone()[0]
                free = db.execute('PRAGMA freelist_count').fetchone()[0]
                if mode == 2 and free > 1000:
                    db.execute('PRAGMA incremental_vacuum(100)')
        self._cleanup_stale_ledger_directories(now)
        return dict(runs_removed=len(ids), findings_removed=len(findings),
                    failure_details_removed=len(failures), shards_removed=len(old_shards),
                    database_bytes=self._database_bytes())

    def _database_bytes(self):
        return sum(path.stat().st_size for path in
                   (self.path, Path(str(self.path) + '-wal'), Path(str(self.path) + '-shm'))
                   if path.exists())

    def _cleanup_stale_ledger_directories(self, now):
        root = (self.path.parent / 'tmp').resolve()
        if not root.is_dir():
            return
        with self._read_connection() as db:
            active = {row[0] for row in db.execute("SELECT id FROM monitor_runs WHERE status='RUNNING'")}
        for path in root.iterdir():
            target = path.resolve()
            if not path.name.startswith('run-') or not target.is_relative_to(root) or not path.is_dir():
                continue
            if any(path.name.startswith(f'run-{run_id}-') for run_id in active):
                continue
            if now.timestamp() - path.stat().st_mtime > 86_400:
                shutil.rmtree(target)


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
