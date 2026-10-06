"""Metadata-only monitoring projection of existing per-event boundary evidence.

All owned events retain a compact status for future eligibility checks; detailed
affected-event samples use the pipeline's existing 200-row diagnostic budget.
The caller must apply the existing presentation/credential redaction before
persistence or display. No raw text, raw timestamps or transport data is copied.
"""
import hashlib
import json

from parser_layer.timestamp.source_policy import TimestampContext, resolve_event_time


BOUNDARY_DIAGNOSTIC_LIMIT = 200


def _fingerprint(values):
    return hashlib.sha256(json.dumps(values, separators=(',', ':')).encode()).hexdigest()


def _reference(contributor):
    reference = contributor['source_reference']
    return _fingerprint([reference[key] for key in ('source_scope', 'source_partition', 'record_id')])


def _source_time(contributor):
    instant = resolve_event_time(None, TimestampContext(
        source_record_time=contributor.get('source_timestamp'),
        source_record_raw=contributor.get('source_timestamp_raw'),
    )).source_record_time
    return instant.isoformat() if instant is not None else None


def build_boundary_quality(event_provenance):
    """Project selected assembly outputs only, before any UI sample truncation.

    event_ordinal is one-based within this run, including parser-None outputs.
    event_id alone need not be unique. Counts/statuses cover every owned logical
    event; bounded detail samples must never be treated as an eligibility list.
    """
    counts = dict(complete=0, possible_incomplete=0, confirmed_truncated=0)
    statuses, affected = [], []
    analysis_end = 0
    for ordinal, entry in enumerate(event_provenance, 1):
        provenance = entry['provenance']
        status = provenance['boundary_status']
        counts[status] += 1
        reason = provenance['emission_reason']
        analysis_end += reason == 'analysis_end'
        row = dict(event_ordinal=ordinal, event_id=entry['event_id'], boundary_status=status)
        statuses.append(row)
        if status == 'complete' or len(affected) >= BOUNDARY_DIAGNOSTIC_LIMIT:
            continue
        contributors = provenance['contributors']
        first = next(item for item in contributors if item['included'])
        last = contributors[-1]
        stream = provenance['stream_key']
        affected.append({
            **row, 'emission_reason': reason,
            'stream_id': _fingerprint([stream[key] for key in
                                      ('source_scope', 'pod_instance', 'container_instance', 'channel')]),
            'pod_instance': stream['pod_instance'], 'container_instance': stream['container_instance'],
            'channel': stream['channel'], 'source_start_time': _source_time(first),
            'last_source_time': _source_time(last), 'physical_record_count': len(contributors),
            'first_record_reference': _reference(first), 'last_record_reference': _reference(last),
        })
    return dict(total_events=len(statuses), counts=counts, analysis_end_events=analysis_end,
                event_statuses=statuses, affected_events=affected, sample_limit=BOUNDARY_DIAGNOSTIC_LIMIT)
