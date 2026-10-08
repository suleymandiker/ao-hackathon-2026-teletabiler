"""Ordered, restartable half-open acquisition shards with bounded page buffers."""

from dataclasses import dataclass
from datetime import datetime, timedelta
import time

from ingestion_layer.opensearch_source import OpenSearchSourceError
from ingestion_layer.opensearch_client import OpenSearchClientError
from monitoring.errors import MonitoringError
from monitoring.identity import reference_key


TARGET_SHARD_RECORDS = 20_000
MAX_SPLIT_DEPTH = 24
MAX_SHARDS_PER_RUN = 4_096
MAX_REQUEST_PAGES_PER_RUN = 100_000
MAX_BUFFERED_RECORDS = 10_000
MAX_BUFFERED_CHARS = 8_000_000
MIN_SHARD_DURATION = timedelta(microseconds=2)
MAX_TRANSIENT_ATTEMPTS = 3
RETRY_BACKOFF_SECONDS = (0.1, 0.2)
SPLITTABLE_PRESSURE_REASONS = frozenset({'SEARCH_TIMEOUT', 'SHARD_FAILURE', 'HTTP_429', 'HTTP_5XX'})


def retry_read(operation, *, sleep=time.sleep):
    """Retry only explicitly transient read failures; never retry malformed evidence."""
    for attempt in range(MAX_TRANSIENT_ATTEMPTS):
        try:
            return operation()
        except (OpenSearchClientError, OpenSearchSourceError) as error:
            if not error.retryable or attempt == MAX_TRANSIENT_ATTEMPTS - 1:
                raise
            sleep(RETRY_BACKOFF_SECONDS[attempt])


@dataclass(frozen=True)
class AcquisitionShard:
    start: datetime
    end: datetime
    depth: int = 0
    shard_id: str = '0'

    def split(self):
        midpoint = self.start + (self.end - self.start) / 2
        if midpoint <= self.start or midpoint >= self.end:
            raise MonitoringError('ACQUISITION_LIMIT')
        return (AcquisitionShard(self.start, midpoint, self.depth + 1, self.shard_id + 'L'),
                AcquisitionShard(midpoint, self.end, self.depth + 1, self.shard_id + 'R'))


def plan_shards(start, end, metrics, page_size, max_pages):
    """Use exact-window minute volumes as hints; overlap is still acquired."""
    target = min(TARGET_SHARD_RECORDS, MAX_BUFFERED_RECORDS,
                 max(page_size, page_size * (max_pages - 1)))
    counts = {stamp: count for stamp, count in metrics.buckets}
    boundaries = [start]
    minute = start.replace(second=0, microsecond=0) + timedelta(minutes=1)
    while minute < end:
        boundaries.append(minute)
        minute += timedelta(minutes=1)
    boundaries.append(end)
    planned = []
    current_start = boundaries[0]
    current_count = 0
    for left, right in zip(boundaries, boundaries[1:]):
        bucket = int(left.replace(second=0, microsecond=0).timestamp() * 1000)
        estimate = counts.get(bucket, 0)
        if current_count and current_count + estimate > target:
            planned.append((current_start, left))
            current_start, current_count = left, 0
        current_count += estimate
        if current_count >= target:
            planned.append((current_start, right))
            current_start, current_count = right, 0
    if current_start < end:
        planned.append((current_start, end))
    return tuple(AcquisitionShard(left, right, 0, str(n)) for n, (left, right) in enumerate(planned))


class StreamingAcquisition:
    """Validate a whole small shard before releasing its pages to the processor."""

    def __init__(self, source, metrics_source, definition, *, log, shard_sink=None, sleep=time.sleep):
        self.source = source
        self.metrics_source = metrics_source
        self.definition = definition
        self.log = log
        self.shard_sink = shard_sink
        self.sleep = sleep
        self.pages_read = 0
        self.records_read = 0
        self.shards_completed = 0
        self.completed_unique_records = 0
        self.shards_seen = 0
        self.last_state = None
        self.acquisition_duration_seconds = 0.0

    def _state(self, shard, status, *, pages=0, records=0, unique=0, failure=None, stage=None):
        self.last_state = dict(shard_id=shard.shard_id, start=shard.start.isoformat(),
                               end=shard.end.isoformat(), depth=shard.depth,
                               status=status, pages_read=pages, records_read=records,
                               unique_records=unique, failure_category=failure, error_stage=stage)
        if self.shard_sink is not None:
            self.shard_sink(shard, status, pages=pages, records=records, unique=unique, failure=failure)

    def _can_split(self, shard):
        return shard.depth < MAX_SPLIT_DEPTH and shard.end - shard.start >= MIN_SHARD_DURATION

    def pages(self, shards):
        for shard in shards:
            yield from self._acquire(shard)

    def _request(self, operation):
        return retry_read(operation, sleep=self.sleep)

    def _acquire(self, shard):
        self.shards_seen += 1
        if self.shards_seen > MAX_SHARDS_PER_RUN:
            self._state(shard, 'FAILED', failure='ACQUISITION_LIMIT')
            raise MonitoringError('ACQUISITION_LIMIT')
        self.log('ACQUISITION_SHARD_START', shard_id=shard.shard_id,
                 start=shard.start.isoformat(), end=shard.end.isoformat(), depth=shard.depth)
        self._state(shard, 'RUNNING')
        counted_at = time.monotonic()
        count_error = None
        try:
            expected = self._request(lambda: self.metrics_source.count(shard.start, shard.end, self.definition))
        except Exception as error:
            count_error = error
        finally:
            self.acquisition_duration_seconds += time.monotonic() - counted_at
        if count_error is not None:
            reason = getattr(count_error, 'reason_code', 'QUERY_FAILURE_UNKNOWN')
            if reason in SPLITTABLE_PRESSURE_REASONS and self._can_split(shard):
                yield from self._split(shard)
                return
            self._state(shard, 'FAILED', failure=reason, stage='shard_planning')
            raise count_error
        capacity = min(MAX_BUFFERED_RECORDS,
                       self.definition.page_size * max(1, self.definition.max_pages - 1))
        if expected > capacity:
            yield from self._split(shard)
            return
        buffered = []
        unique = set()
        cursor = None
        exhausted = False
        records = 0
        chars = 0
        for page_number in range(1, self.definition.max_pages + 1):
            if self.pages_read >= MAX_REQUEST_PAGES_PER_RUN:
                self._state(shard, 'FAILED', pages=page_number - 1, records=records,
                            unique=len(unique), failure='ACQUISITION_LIMIT')
                raise MonitoringError('ACQUISITION_LIMIT')
            requested_at = time.monotonic()
            split_for_pressure = False
            try:
                page = self._request(lambda: self.source.read_page(start=shard.start, end=shard.end,
                    namespace=self.definition.namespace, workload=self.definition.workload,
                    container=self.definition.container, cluster_id=self.definition.document_cluster_id,
                    page_size=self.definition.page_size, cursor=cursor))
            except Exception as error:
                reason = getattr(error, 'reason_code', 'QUERY_FAILURE_UNKNOWN')
                if reason in SPLITTABLE_PRESSURE_REASONS and self._can_split(shard):
                    split_for_pressure = True
                else:
                    self._state(shard, 'FAILED', pages=page_number - 1, records=records,
                                unique=len(unique), failure=reason, stage='content_acquisition')
                    raise
            finally:
                self.acquisition_duration_seconds += time.monotonic() - requested_at
            if split_for_pressure:
                buffered.clear()
                yield from self._split(shard, pages=page_number - 1, records=records,
                                       unique=len(unique))
                return
            self.pages_read += 1
            self.records_read += len(page.records)
            records += len(page.records)
            chars += sum(len(record.raw_text) for record in page.records)
            if records > MAX_BUFFERED_RECORDS or chars > MAX_BUFFERED_CHARS:
                buffered.clear()
                yield from self._split(shard, pages=page_number, records=records,
                                       unique=len(unique))
                return
            for record in page.records:
                unique.add(reference_key(record))
            buffered.append(page)
            if page.interval_exhausted:
                exhausted = True
                break
            if page.next_cursor is None or page.next_cursor == cursor:
                self._state(shard, 'FAILED', pages=page_number, records=records,
                            unique=len(unique), failure='INVALID_CURSOR', stage='content_acquisition')
                raise OpenSearchSourceError('Acquisition continuation unavailable')
            cursor = page.next_cursor
        if not exhausted or len(unique) < expected:
            # Nothing from this shard has entered segmentation yet.
            buffered.clear()
            yield from self._split(shard, pages=min(self.definition.max_pages, page_number),
                                   records=records, unique=len(unique))
            return
        if len(unique) != expected:
            self._state(shard, 'FAILED', pages=page_number, records=records,
                        unique=len(unique), failure='INVALID_RESPONSE', stage='content_acquisition')
            raise OpenSearchSourceError('Source changed during exact shard acquisition')
        self._state(shard, 'COMPLETE', pages=page_number, records=records, unique=len(unique))
        self.log('ACQUISITION_SHARD_COMPLETE', shard_id=shard.shard_id,
                 pages_read=page_number, records_read=records, unique_records=len(unique))
        self.shards_completed += 1
        self.completed_unique_records += len(unique)
        yield from buffered

    def _split(self, shard, *, pages=0, records=0, unique=0):
        if not self._can_split(shard):
            self._state(shard, 'FAILED', pages=pages, records=records,
                        unique=unique, failure='ACQUISITION_LIMIT')
            raise MonitoringError('ACQUISITION_LIMIT')
        left, right = shard.split()
        self._state(shard, 'SPLIT', pages=pages, records=records, unique=unique)
        self.log('ACQUISITION_SPLIT', shard_id=shard.shard_id, depth=shard.depth,
                 start=shard.start.isoformat(), split=left.end.isoformat(), end=shard.end.isoformat())
        yield from self._acquire(left)
        yield from self._acquire(right)
