"""Typed monitoring state. Scheduling time is never event occurrence time."""
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from uuid import uuid4

from parser_layer.timestamp.source_policy import TimestampSourcePolicy


def utc(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise ValueError('An aware timestamp is required')
    return value.astimezone(timezone.utc)


class MonitorStatus(str, Enum):
    ACTIVE = 'ACTIVE'
    PAUSED = 'PAUSED'
    RUNNING = 'RUNNING'
    ERROR = 'ERROR'
    BLOCKED = 'BLOCKED'
    ARCHIVED = 'ARCHIVED'


class RunStatus(str, Enum):
    RUNNING = 'RUNNING'
    SUCCESS = 'SUCCESS'
    FAILED = 'FAILED'


@dataclass(frozen=True)
class MonitorDefinition:
    """Source profile and logical cluster alias are not document UUIDs.

    ``cluster_id`` retains its legacy constructor/JSON name for stored monitors.
    Only an explicit ``document_cluster_id`` may constrain openshift.cluster_id.
    """
    name: str
    source_profile: str
    cluster_id: str
    namespace: str
    workload: str
    initial_start: datetime
    container: str | None = None
    interval_seconds: int = 900
    window_seconds: int = 900
    ingestion_delay_seconds: int = 60
    overlap_seconds: int = 60
    source_timezone: str | None = None
    page_size: int = 100
    max_pages: int = 20
    document_cluster_id: str | None = None

    @property
    def cluster_alias(self) -> str:
        return self.cluster_id

    def __post_init__(self):
        for name in ('name', 'source_profile', 'cluster_id', 'namespace', 'workload'):
            value = getattr(self, name)
            if type(value) is not str or not value.strip() or len(value) > 200 or any(ord(c) < 32 for c in value):
                raise ValueError('Monitor names and scope must be nonempty single-line text (max 200 characters)')
        if self.container is not None and (type(self.container) is not str or not self.container.strip()):
            raise ValueError('Container must be nonempty or omitted')
        if self.document_cluster_id is not None:
            value = self.document_cluster_id
            if type(value) is not str or not value.strip() or len(value) > 200 or any(ord(c) < 32 for c in value):
                raise ValueError('Document cluster ID must be single-line text (max 200 characters) or omitted')
        for name, low, high in (
            ('interval_seconds', 1, 86400), ('window_seconds', 1, 86400),
            ('ingestion_delay_seconds', 0, 86400), ('overlap_seconds', 0, 3600),
            ('page_size', 1, 500), ('max_pages', 1, 20),
        ):
            value = getattr(self, name)
            if type(value) is not int or not low <= value <= high:
                raise ValueError(f'{name} must be between {low} and {high}')
        if self.overlap_seconds > self.window_seconds or self.window_seconds + 2 * self.overlap_seconds > 86400:
            raise ValueError('Overlap must fit within one window; total retrieval must fit within 24 hours')
        TimestampSourcePolicy(self.source_timezone)
        object.__setattr__(self, 'initial_start', utc(self.initial_start))


@dataclass(frozen=True)
class DeploymentMonitor:
    id: str
    definition: MonitorDefinition
    enabled: bool
    status: MonitorStatus
    last_successful_end: datetime | None
    next_run_at: datetime
    created_at: datetime
    updated_at: datetime
    revision: int = 0
    last_error_category: str | None = None
    last_error_summary: str | None = None
    archived: bool = False
    manual_requested_at: datetime | None = None
    pending_window: 'Window | None' = None


@dataclass(frozen=True)
class Window:
    start: datetime
    end: datetime

    def __post_init__(self):
        object.__setattr__(self, 'start', utc(self.start))
        object.__setattr__(self, 'end', utc(self.end))
        if self.start >= self.end:
            raise ValueError('Window must satisfy start < end')


def next_window(monitor: DeploymentMonitor) -> Window:
    # Once claimed, an interval remains authoritative through failure/restart.
    if monitor.pending_window is not None:
        return monitor.pending_window
    if monitor.last_successful_end is None and monitor.manual_requested_at is not None:
        end = monitor.manual_requested_at - timedelta(
            seconds=monitor.definition.ingestion_delay_seconds + monitor.definition.overlap_seconds)
        return Window(end - timedelta(seconds=monitor.definition.window_seconds), end)
    start = monitor.last_successful_end or monitor.definition.initial_start
    return Window(start, start + timedelta(seconds=monitor.definition.window_seconds))


def safe_at(window: Window, definition: MonitorDefinition) -> datetime:
    # The lookahead used to finish boundary events must also be indexed/stable.
    return window.end + timedelta(seconds=definition.ingestion_delay_seconds + definition.overlap_seconds)


def ready_at(monitor: DeploymentMonitor, now: datetime) -> datetime:
    """Shared readiness formula: operator intent bypasses cadence, never safety.

    Lifecycle and running ownership are checked separately by the repository.
    """
    cadence = utc(now) if monitor.manual_requested_at is not None else monitor.next_run_at
    return max(cadence, safe_at(next_window(monitor), monitor.definition))


@dataclass(frozen=True)
class RunCounts:
    events_retrieved: int = 0
    unique_records: int = 0
    logical_events: int = 0
    parsed_events: int = 0
    template_count: int = 0
    signal_candidates: int = 0
    qualified_signals: int = 0
    correlations: int = 0
    incidents: int = 0
    rca: int = 0


@dataclass(frozen=True)
class MonitorRun:
    id: str
    monitor_id: str
    window: Window
    definition: MonitorDefinition
    actual_started_at: datetime
    actual_finished_at: datetime | None
    created_at: datetime
    status: RunStatus
    claim_token: str = field(repr=False)
    attempts: int = 1
    counts: RunCounts = field(default_factory=RunCounts)
    result_reference: str | None = None
    error_category: str | None = None
    error_summary: str | None = None


def new_id() -> str:
    return uuid4().hex
