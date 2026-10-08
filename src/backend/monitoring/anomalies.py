"""Deterministic signals from compact successful-window history."""

from collections import Counter
import hashlib
from statistics import median


MIN_BASELINE_WINDOWS = 5
BASELINE_HORIZON = 30
MAD_MULTIPLIER = 6
SPIKE_RATIO = 3.0
COLLAPSE_RATIO = 0.25
MAX_ANOMALY_SIGNALS = 200


def _baseline(values):
    if len(values) < MIN_BASELINE_WINDOWS:
        return None
    center = median(values)
    deviation = median(abs(item - center) for item in values)
    return center, deviation


def _change(value, values, *, allow_collapse=True):
    baseline = _baseline(values)
    if baseline is None:
        return None
    center, deviation = baseline
    high = max(center * SPIKE_RATIO, center + MAD_MULTIPLIER * deviation)
    low = min(center * COLLAPSE_RATIO, center - MAD_MULTIPLIER * deviation)
    if value > high and value >= 5:
        direction, threshold = 'spike', high
    elif allow_collapse and center >= 5 and value < low:
        direction, threshold = 'collapse', low
    else:
        return None
    return dict(direction=direction, current=value, baseline=center,
                baseline_mad=deviation, ratio=(value / center if center else None),
                delta=value - center, threshold=threshold,
                baseline_observations=len(values), rule='median_mad_and_ratio')


def anomaly_signals(run, metrics, accumulator, history, pattern_history):
    """Return bounded aggregate candidates; cold starts establish baseline only."""
    start_ms = int(run.window.start.timestamp() * 1000)
    end_ms = int(run.window.end.timestamp() * 1000)
    namespace = run.definition.namespace
    workload = run.definition.workload
    output = []

    def add(kind, details, *, template_id=None, template=None, severity=4, identity=None):
        if details is None or len(output) >= MAX_ANOMALY_SIGNALS:
            return
        label = template or kind.replace('_', ' ')
        suffix = template_id or (hashlib.sha256(identity.encode()).hexdigest()[:16] if identity else 'total')
        output.append(dict(signal_id=f'monitor:{run.id}:{kind}:{suffix}',
            template_id=template_id or f'monitor:{kind}', template=label,
            scope=f'{namespace}/{workload}', window_start_ms=start_ms,
            window_end_ms=end_ms, count=1, first_seen_ms=start_ms,
            last_seen_ms=end_ms, severity_min=severity, service_name=workload,
            component=workload, namespace=namespace, cluster_name='unknown',
            host='unknown', hosts=[], event_ids=[], reliable_ratio=1.0,
            burst_score=0.0, timestamp_resolved=True, alarm_type='',
            source_severity_max=0, source_systems=[], data_centers=[], racks=[],
            environments=[], business_criticalities=[], source_order=None,
            timestamp_basis_counts={}, monitor_anomaly=dict(kind=kind, **details),
            representative_evidence=(accumulator.patterns[template_id].evidence
                                     if template_id in accumulator.patterns else [])))

    total = metrics.total
    totals = [item['total_physical_logs'] for item in history]
    volume = _change(total, totals)
    add('total_volume', volume)
    seconds = max(1, (run.window.end - run.window.start).total_seconds())
    current_rate = total / seconds
    rates = [item['total_physical_logs'] / seconds for item in history]
    rate_change = _change(current_rate, rates)
    add('log_rate', rate_change)
    for kind, key, flag in (('pod_volume', 'pods', 'pod_counts_exact'),
                            ('container_volume', 'containers', 'container_counts_exact')):
        if not getattr(metrics, flag) or not all(item.get(flag) for item in history):
            continue
        for identity, count in getattr(metrics, key).items():
            values = [item.get(key, {}).get(identity, 0) for item in history]
            change = _change(count, values, allow_collapse=False)
            if change is not None:
                add(kind, dict(identity=identity, **change), identity=identity)

    current_patterns = accumulator.patterns
    common = Counter()
    for window in pattern_history:
        for tid in window:
            common[tid] += 1
    for tid, pattern in current_patterns.items():
        values = [window.get(tid, {}).get('count', 0) for window in pattern_history]
        change = _change(pattern.count, values, allow_collapse=False)
        if change is not None:
            add('pattern_frequency', change, template_id=tid,
                template=pattern.template, severity=3)
        elif pattern.new_count > 0 and common[tid] == 0 and len(pattern_history) >= MIN_BASELINE_WINDOWS:
            add('new_pattern', dict(direction='new', current=pattern.count,
                baseline=0, baseline_observations=len(pattern_history),
                threshold=1, rule='new_template_and_absent_from_recent_history'), template_id=tid,
                template=pattern.template)
    for tid, frequency in common.items():
        if frequency < MIN_BASELINE_WINDOWS or tid in current_patterns:
            continue
        values = [window.get(tid, {}).get('count', 0) for window in pattern_history]
        change = _change(0, values)
        if change is not None:
            add('pattern_disappearance', change, template_id=tid,
                template=pattern_history[0].get(tid, {}).get('template', tid))

    current_error = sum(accumulator.severity_counts.get(level, 0) for level in ('ERROR', 'CRITICAL', 'FATAL'))
    error_history = [sum(item.get('severity_counts', {}).get(level, 0)
                         for level in ('ERROR', 'CRITICAL', 'FATAL')) for item in history]
    add('error_severity', _change(current_error, error_history, allow_collapse=False), severity=3)
    if current_error >= 10:
        for kind, distribution in (('pod_error_concentration', accumulator.error_pods),
                                   ('container_error_concentration', accumulator.error_containers)):
            for identity, count in sorted(distribution.items(), key=lambda row: (-row[1], row[0]))[:1]:
                if identity not in ('unknown', '[other]') and count / current_error >= 0.9:
                    add(kind, dict(direction='concentration', identity=identity,
                        current=count, total_errors=current_error,
                        ratio=count / current_error, threshold=0.9,
                        rule='at_least_90_percent_of_parsed_errors'), severity=3)
    return output
