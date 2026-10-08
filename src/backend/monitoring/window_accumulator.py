"""Compact, bounded analytical state for one monitored logical window."""

from collections import Counter
from dataclasses import dataclass, field
import hashlib
import json

from aggregation_layer.aggregator import _infer_identity, _resource, _severity_number
from analysis_time import source_time_ms
from evidence_redaction import redact_text
from monitoring.errors import MonitoringLimitError
from parser_layer.timestamp.source_policy import BASES


MAX_STREAMS = 10_000
MAX_PATTERNS = 1_000
MAX_SIGNAL_GROUPS = 1_000
MAX_DISTRIBUTION_KEYS = 32
MAX_EVENT_REFERENCES = 6
MAX_BOUNDARY_SAMPLES = 200
MAX_TEMPLATE_CHARS = 8192
MAX_STORED_TEMPLATE_CHARS = 512
TEMPLATE_TRUNCATION_MARKER = ' [template truncated] '


def bounded_template(template, limit):
    """Keep deterministic head and tail context without retaining excess text."""
    if len(template) <= limit:
        return template
    if limit <= len(TEMPLATE_TRUNCATION_MARKER):
        return TEMPLATE_TRUNCATION_MARKER[:limit]
    remaining = limit - len(TEMPLATE_TRUNCATION_MARKER)
    head = (remaining + 1) // 2
    tail = remaining // 2
    return template[:head] + TEMPLATE_TRUNCATION_MARKER + (template[-tail:] if tail else '')


def _count(mapping, key):
    key = str(key or 'unknown')
    if key in mapping or len(mapping) < MAX_DISTRIBUTION_KEYS:
        mapping[key] = mapping.get(key, 0) + 1
    else:
        mapping['[other]'] = mapping.get('[other]', 0) + 1


def _reference(record):
    ref = record.source_reference
    values = (ref.source_scope, ref.source_partition, ref.record_id)
    digest = hashlib.sha256(json.dumps(values, separators=(',', ':')).encode()).hexdigest()
    index, document = redact_text(ref.source_partition), redact_text(ref.record_id)
    return dict(reference_hash=digest, index=index if index == ref.source_partition else None,
                document_id=document if document == ref.record_id else None)


@dataclass
class _Pattern:
    template_id: str
    template: str
    template_truncated: bool = False
    original_template_chars: int = 0
    count: int = 0
    first_seen_ms: int | None = None
    last_seen_ms: int | None = None
    severity_counts: Counter = field(default_factory=Counter)
    pods: dict = field(default_factory=dict)
    containers: dict = field(default_factory=dict)
    new_count: int = 0
    evidence: list = field(default_factory=list)

    def add(self, row, *, new=False):
        self.count += 1
        self.new_count += bool(new)
        stamp = source_time_ms(row.get('timestamp'))
        if stamp is not None:
            self.first_seen_ms = stamp if self.first_seen_ms is None else min(self.first_seen_ms, stamp)
            self.last_seen_ms = stamp if self.last_seen_ms is None else max(self.last_seen_ms, stamp)
        self.severity_counts[str(row.get('severity_text') or row.get('severity') or 'UNKNOWN').upper()] += 1
        stream = (row.get('attributes') or {}).get('source_provenance', {}).get('stream_key') or {}
        _count(self.pods, stream.get('pod_instance'))
        _count(self.containers, stream.get('container_instance'))
        contributors = (row.get('attributes') or {}).get('source_provenance', {}).get('contributors') or ()
        if len(self.evidence) < MAX_EVENT_REFERENCES and contributors:
            source = contributors[0]['source_reference']
            index, document = redact_text(source['source_partition']), redact_text(source['record_id'])
            self.evidence.append(dict(timestamp=row.get('timestamp'),
                                      index=index if index == source['source_partition'] else None,
                                      document_id=document if document == source['record_id'] else None))

    def to_dict(self):
        stored_template = (bounded_template(self.template, MAX_STORED_TEMPLATE_CHARS)
                           if self.template_truncated else self.template[:MAX_STORED_TEMPLATE_CHARS])
        return dict(template_id=self.template_id,
                    template=redact_text(stored_template), count=self.count,
                    template_truncated=self.template_truncated,
                    original_template_chars=self.original_template_chars,
                    first_seen_ms=self.first_seen_ms, last_seen_ms=self.last_seen_ms,
                    severity_counts=dict(self.severity_counts), pods=self.pods, containers=self.containers,
                    new_count=self.new_count, representative_evidence=self.evidence)


@dataclass
class _SignalGroup:
    first: dict
    count: int = 0
    reliable: int = 0
    severity_min: int = 7
    first_seen_ms: int | None = None
    last_seen_ms: int | None = None
    event_ids: list = field(default_factory=list)
    hosts: set = field(default_factory=set)
    timestamp_basis_counts: Counter = field(default_factory=Counter)

    def add(self, row):
        self.count += 1
        self.reliable += bool(row.get('template_reliable', row.get('reliable', True)))
        self.severity_min = min(self.severity_min, _severity_number(row))
        stamp = source_time_ms(row.get('timestamp'))
        if stamp is not None:
            self.first_seen_ms = stamp if self.first_seen_ms is None else min(self.first_seen_ms, stamp)
            self.last_seen_ms = stamp if self.last_seen_ms is None else max(self.last_seen_ms, stamp)
        if len(self.event_ids) < 8:
            self.event_ids.append(str(row.get('event_id') or ''))
        host = _resource(row, 'host')
        if host != 'unknown' and len(self.hosts) < MAX_DISTRIBUTION_KEYS:
            self.hosts.add(host)
        basis = (row.get('timestamp_provenance') or {}).get('basis')
        if basis in BASES:
            self.timestamp_basis_counts[basis] += 1


class WindowAccumulator:
    """One-run counters and compact groups; never retain an event body list."""

    def __init__(self, window_seconds=60):
        self.window_ms = max(1, int(window_seconds)) * 1000
        self.patterns = {}
        self.groups = {}
        self.severity_counts = Counter()
        self.error_pods = {}
        self.error_containers = {}
        self.parsed_events = 0
        self.latest_event_ms = None
        self.boundary_counts = Counter()
        self.boundary_total = 0
        self.boundary_tails = 0
        self.boundary_samples = []
        self.anomaly_context = None
        self.streams = set()
        self.stream_counts = {}
        self.truncated_template_count = 0
        self.max_template_chars_observed = 0

    def configure_anomalies(self, run, metrics, history, pattern_history):
        self.anomaly_context = (run, metrics, history, pattern_history)

    def observe_boundary(self, assembled, event_id):
        self.boundary_total += 1
        status = assembled.boundary_status
        self.boundary_counts[status] += 1
        self.boundary_tails += assembled.emission_reason == 'analysis_end'
        stream = assembled.stream_key
        stream_id = hashlib.sha256(json.dumps((stream.source_scope, stream.pod_instance,
                                                stream.container_instance, stream.channel),
                                               separators=(',', ':')).encode()).hexdigest()
        if len(self.streams) >= MAX_STREAMS and stream_id not in self.streams:
            raise MonitoringLimitError('STREAM_CARDINALITY_LIMIT', len(self.streams) + 1, MAX_STREAMS)
        self.streams.add(stream_id)
        _count(self.stream_counts, stream_id)
        if status == 'complete' or len(self.boundary_samples) >= MAX_BOUNDARY_SAMPLES:
            return
        first = next(record for record, evidence in zip(assembled.records, assembled.evidence) if evidence.included)
        last = assembled.records[-1]
        self.boundary_samples.append(dict(event_ordinal=self.boundary_total, event_id=event_id,
            boundary_status=status, emission_reason=assembled.emission_reason, stream_id=stream_id,
            pod_instance=redact_text(stream.pod_instance), container_instance=redact_text(stream.container_instance),
            channel=redact_text(stream.channel), physical_record_count=len(assembled.records),
            first_record_reference=_reference(first)['reference_hash'],
            last_record_reference=_reference(last)['reference_hash']))

    def observe_parsed(self, event):
        self.parsed_events += 1
        severity = str(event.get('severity_text') or event.get('severity') or 'UNKNOWN').upper()
        self.severity_counts[severity] += 1
        if severity in ('ERROR', 'CRITICAL', 'FATAL'):
            stream = (event.get('attributes') or {}).get('source_provenance', {}).get('stream_key') or {}
            _count(self.error_pods, stream.get('pod_instance'))
            _count(self.error_containers, stream.get('container_instance'))
        stamp = source_time_ms(event.get('timestamp'))
        if stamp is not None:
            self.latest_event_ms = stamp if self.latest_event_ms is None else max(self.latest_event_ms, stamp)

    def add(self, row, *, new=False):
        tid = str(row['template_id'])
        template = str(row.get('template') or '')
        original_chars = len(template)
        self.max_template_chars_observed = max(self.max_template_chars_observed, original_chars)
        if tid not in self.patterns:
            if len(self.patterns) >= MAX_PATTERNS:
                raise MonitoringLimitError('PATTERN_CARDINALITY_LIMIT', len(self.patterns) + 1, MAX_PATTERNS)
            truncated = original_chars > MAX_TEMPLATE_CHARS
            self.truncated_template_count += int(truncated)
            self.patterns[tid] = _Pattern(tid, bounded_template(template, MAX_TEMPLATE_CHARS),
                                          template_truncated=truncated,
                                          original_template_chars=original_chars)
        self.patterns[tid].add(row, new=new)

        stamp = source_time_ms(row.get('timestamp'))
        bucket = stamp - stamp % self.window_ms if stamp is not None else None
        service, component = _infer_identity(row)
        host = _resource(row, 'host')
        identity = service if service != 'unknown' else component if component != 'unknown' else host
        alarm_type = str((row.get('attributes') or {}).get('alarm_type') or '').strip()
        key = (alarm_type or tid, tid, identity, bucket)
        if key not in self.groups:
            if len(self.groups) >= MAX_SIGNAL_GROUPS:
                raise MonitoringLimitError('SIGNAL_CARDINALITY_LIMIT', len(self.groups) + 1, MAX_SIGNAL_GROUPS)
            self.groups[key] = _SignalGroup(dict(template_id=tid, template=self.patterns[tid].template,
                service_name=service, component=component, host=host,
                namespace=str(row.get('namespace') or (row.get('resource') or {}).get('namespace') or 'unknown'),
                cluster_name=str(row.get('cluster_name') or (row.get('resource') or {}).get('cluster_name') or 'unknown'),
                alarm_type=alarm_type, source_order=row.get('source_order')))
        self.groups[key].add(row)

    def signals(self):
        output = []
        for (semantic, tid, identity, bucket), group in self.groups.items():
            first = group.first
            scope = '/'.join(value for value in (first['cluster_name'], first['namespace'], first['service_name'])
                             if value != 'unknown') or (first['component'] if first['component'] != 'unknown' else
                                                     first['host'] if first['host'] != 'unknown' else 'genel')
            output.append(dict(signal_id=f"sig:{tid}:{identity}:{bucket if bucket is not None else 'untimed'}",
                template_id=tid, template=first['template'], scope=scope,
                window_start_ms=bucket, window_end_ms=bucket + self.window_ms if bucket is not None else None,
                count=group.count, first_seen_ms=group.first_seen_ms, last_seen_ms=group.last_seen_ms,
                severity_min=group.severity_min, service_name=first['service_name'], component=first['component'],
                namespace=first['namespace'], cluster_name=first['cluster_name'], host=first['host'],
                hosts=sorted(group.hosts), event_ids=group.event_ids,
                reliable_ratio=round(group.reliable / group.count, 4), burst_score=round(min(1, group.count / 10), 4),
                timestamp_resolved=bucket is not None, alarm_type=first['alarm_type'], source_severity_max=0,
                source_systems=[], data_centers=[], racks=[], environments=[], business_criticalities=[],
                source_order=first['source_order'], timestamp_basis_counts=dict(group.timestamp_basis_counts)))
            output[-1]['representative_evidence'] = self.patterns[tid].evidence
        if self.anomaly_context is not None:
            from monitoring.anomalies import anomaly_signals
            output.extend(anomaly_signals(*self.anomaly_context[:2], self,
                                          *self.anomaly_context[2:]))
        return sorted(output, key=lambda row: (row['window_start_ms'] is None,
                       row['window_start_ms'] or 0, row['signal_id']))

    def boundary_quality(self):
        return dict(total_events=self.boundary_total,
                    counts={key: self.boundary_counts[key] for key in
                            ('complete', 'possible_incomplete', 'confirmed_truncated')},
                    analysis_end_events=self.boundary_tails, event_statuses=[],
                    omitted_event_statuses=self.boundary_total, affected_events=self.boundary_samples,
                    sample_limit=MAX_BOUNDARY_SAMPLES)

    def pattern_metrics(self):
        return [pattern.to_dict() for pattern in self.patterns.values()]

    def template_diagnostics(self):
        """Count distinct retained patterns requiring analytical text truncation."""
        return dict(truncated_template_count=self.truncated_template_count,
                    max_template_chars_observed=self.max_template_chars_observed)
