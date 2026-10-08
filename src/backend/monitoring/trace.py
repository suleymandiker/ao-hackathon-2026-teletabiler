"""Bounded, sanitized observations of one run; not a processing contract.

No raw logs survive a callback. Links are recorded at execution time; the UI
only traverses them. Budgets keep prefixes in authoritative order, never sample.
"""
from dataclasses import asdict, dataclass, field
from datetime import datetime
from enum import Enum
import hashlib
import json
import math
from itertools import chain
from typing import Any

from evidence_redaction import redact_text, SENSITIVE_KEY
from segmentation_layer.contracts import UnassembledRecord

STAGES = ('acquisition', 'segmentation', 'parsing', 'patterns', 'signal', 'correlation', 'incident', 'rca')


def encoded(value):
    return json.dumps(value, ensure_ascii=False, separators=(',', ':'), allow_nan=False)


@dataclass(frozen=True)
class TraceLimits:
    max_items_per_stage: int = 2000
    max_bytes: int = 16 * 1024 * 1024
    max_text_chars: int = 8192
    max_fields_per_item: int = 16000
    max_depth: int = 12

    def __post_init__(self):
        if any(type(v) is not int or v < 1 for v in asdict(self).values()) or self.max_bytes < 4096:
            raise ValueError('Trace limits must be positive; max_bytes must allow a 4096-byte envelope')


@dataclass(frozen=True)
class TraceItem:
    ref: str
    ordinal: int
    parents: tuple[str, ...]
    data: dict[str, Any]
    preview_truncated: bool = False
    omitted_fields: int = 0
    omitted_links: int = 0


@dataclass
class PipelineTrace:
    run_id: str
    limits: TraceLimits
    schema_version: int = 1
    stages: dict[str, list[TraceItem]] = field(default_factory=lambda: {s: [] for s in STAGES})
    totals: dict[str, int] = field(default_factory=lambda: dict.fromkeys(STAGES, 0))
    omitted_items: dict[str, int] = field(default_factory=lambda: dict.fromkeys(STAGES, 0))
    trace_complete: bool = True


class TraceCollector:
    def __init__(self, run_id, *, limits=None, secrets=()):
        self.trace = PipelineTrace(run_id, limits or TraceLimits())
        self.secrets = tuple(s for s in secrets if s)
        self.used_bytes = 4096  # reserved for the fixed envelope and counts
        self.closed_stages = set()
        self.record_refs = {}
        self.logical_refs = {}
        self.members = {}
        self.owned_count = 0

    def identity(self, value):
        # Keep authoritative IDs normally. Unsafe or oversized IDs use a stable
        # presentation hash; domain objects/IDs are never changed.
        value = str(value)
        clean = redact_text(value, self.secrets)
        return ('redacted-id:' + hashlib.sha256(value.encode()).hexdigest()
                if clean != value or len(value) > 512 else value)

    def sanitize(self, value):
        truncated, omitted, visited = False, 0, 0

        def clean(item, depth=0):
            nonlocal truncated, omitted, visited
            visited += 1
            if depth > self.trace.limits.max_depth or visited > self.trace.limits.max_fields_per_item:
                omitted += 1
                return '[evidence limit]'
            if isinstance(item, dict):
                result = {}
                for key, child in item.items():
                    if visited >= self.trace.limits.max_fields_per_item:
                        omitted += len(item) - len(result)
                        break
                    safe_key = self.identity(key)
                    if SENSITIVE_KEY.fullmatch(str(key)):
                        visited += 1
                        result[safe_key] = '[redacted]'
                    elif isinstance(child, str) and (str(key).endswith('_id') or key in (
                            'source', 'target', 'source_reference_hash', 'record_ref', 'kind',
                            'boundary_status', 'emission_reason', 'recognition', 'delivery', 'disposition', 'rule')):
                        visited += 1
                        result[safe_key] = self.identity(child.value if isinstance(child, Enum) else child)
                    else:
                        result[safe_key] = clean(child, depth + 1)
                return result
            if isinstance(item, (list, tuple)):
                result = []
                for child in item:
                    if visited >= self.trace.limits.max_fields_per_item:
                        omitted += len(item) - len(result)
                        break
                    result.append(clean(child, depth + 1))
                return result
            if isinstance(item, datetime):
                return item.isoformat()
            if isinstance(item, Enum):
                return item.value
            if isinstance(item, str):
                text = redact_text(item, self.secrets)
                if len(text) > self.trace.limits.max_text_chars:
                    truncated = True
                    text = text[:self.trace.limits.max_text_chars]
                return text
            if item is None or type(item) in (int, bool):
                return item
            if type(item) is float and math.isfinite(item):
                return item
            omitted += 1
            return '[unsupported evidence]'

        return clean(value), truncated, omitted

    def add(self, stage, ref, data, parents=()):
        trace = self.trace
        trace.totals[stage] += 1
        ref = self.identity(ref)
        if stage in self.closed_stages or len(trace.stages[stage]) >= trace.limits.max_items_per_stage:
            trace.omitted_items[stage] += 1
            trace.trace_complete = False
            return ref
        safe, truncated, omitted = self.sanitize(data)
        links, omitted_links = [], 0
        for parent in parents:
            if len(links) < trace.limits.max_fields_per_item:
                links.append(self.identity(parent))
            else:
                omitted_links += 1
        item = TraceItem(ref, trace.totals[stage], tuple(links), safe, truncated, omitted, omitted_links)
        size = len(encoded(asdict(item)).encode('utf-8')) + 1
        if self.used_bytes + size > trace.limits.max_bytes:
            self.closed_stages.add(stage)
            trace.omitted_items[stage] += 1
            trace.trace_complete = False
        else:
            trace.stages[stage].append(item)
            self.used_bytes += size
            if truncated or omitted or item.omitted_links:
                trace.trace_complete = False
        return ref

    @staticmethod
    def reference(record):
        r = record.source_reference
        # Same receipt/boundary identity, including the intentional version exclusion.
        return hashlib.sha256(json.dumps([r.source_scope, r.source_partition, r.record_id],
                                         separators=(',', ':')).encode()).hexdigest()

    def acquisition(self, record, *, timestamp, inside_window, overlap, duplicate):
        ref = 'record:' + str(self.trace.totals['acquisition'] + 1)
        key = self.reference(record)
        if not duplicate and len(self.record_refs) < self.trace.limits.max_items_per_stage:
            self.record_refs[key] = ref
        self.add('acquisition', ref, dict(
            source_reference_hash=key, source_timestamp=timestamp,
            source_timestamp_raw=record.source_timestamp_raw, retrieval_order=record.retrieval_order,
            stream=asdict(record.stream_identity) if record.stream_identity else None,
            text=record.raw_text, inside_window=inside_window, acquisition_overlap=overlap,
            duplicate=duplicate, framing=record.framing.value))

    def __call__(self, stage, value):
        if stage == 'segmentation':
            output, admitted = value
            ref = 'logical:' + str(self.trace.totals['segmentation'] + 1)
            if isinstance(output, UnassembledRecord):
                self.add(stage, ref, dict(disposition=output.reason, admitted=False,
                         decision=asdict(output.evidence) if output.evidence else None,
                         policy_id=output.policy.policy_id if output.policy else None),
                         [self.record_refs.get(self.reference(output.record),
                                               'record-hash:' + self.reference(output.record))])
                return
            parents = [self.record_refs.get(self.reference(r), 'record-hash:' + self.reference(r))
                       for r in output.records[:self.trace.limits.max_fields_per_item]]
            first = next(r for r, e in zip(output.records, output.evidence) if e.included)
            last = output.records[-1]
            if admitted and len(self.logical_refs) < self.trace.limits.max_items_per_stage:
                self.logical_refs[self.owned_count] = ref
            if admitted:
                self.owned_count += 1
            self.add(stage, ref, dict(text=output.text, admitted=admitted,
                stream=asdict(output.records[0].stream_identity), stream_key=asdict(output.stream_key),
                physical_record_count=len(output.records), emission_reason=output.emission_reason,
                analysis_event_ordinal=self.owned_count if admitted else None,
                boundary_status=output.boundary_status, policy_id=output.policy.policy_id,
                validation_reference=output.policy.validation_reference,
                source_start_timestamp=first.source_timestamp or first.source_timestamp_raw,
                source_end_timestamp=last.source_timestamp or last.source_timestamp_raw,
                contributors=[dict(record_ref=p, source_reference_hash=self.reference(r), **asdict(e))
                              for p, r, e in zip(parents, output.records, output.evidence)]), parents)
        elif stage == 'parsing':
            order, event, outcome = value
            # Deliberately exclude processing clocks, duplicate raw and already
            # captured source provenance. All other fields are actual outputs.
            canonical = {k: v for k, v in (event or {}).items() if k not in ('raw', 'observed_timestamp')}
            if 'attributes' in canonical:
                canonical['attributes'] = {k: v for k, v in canonical['attributes'].items() if k != 'source_provenance'}
            data = dict(canonical=canonical)
            if outcome is not None:
                data.update(recognition=outcome.recognition, delivery=outcome.delivery,
                            parser_id=outcome.parser_id, reason_code=outcome.reason_code)
            self.add(stage, f'canonical:{order + 1}', data,
                     [self.logical_refs.get(order, f'logical:{order + 1}')])
        elif stage == 'patterns':
            order = value['source_order']
            self.add(stage, f'occurrence:{order + 1}',
                     {k: value[k] for k in ('event_id', 'template_id', 'template', 'template_reliable',
                                            'template_source', 'template_reason') if k in value},
                     [f'canonical:{order + 1}'])
        elif stage == 'membership':
            sid, rows = value
            self.members[sid] = tuple(f"occurrence:{row['source_order'] + 1}" for row in rows)
        elif stage == 'expert_input':
            pack = value
            self.add('rca', 'expert:input', dict(kind='expert_input', evidence=json.loads(pack.serialized),
                     signal_aliases=pack.signal_aliases, signal_members=pack.signal_members,
                     incident_aliases=pack.incident_aliases, diagnostics=dict(pack.diagnostics)),
                     ['incident:' + iid for iid, _ in pack.incident_aliases])

    def finish(self, result):
        for row in result.get('signals', []):
            self.add('signal', 'signal:' + row['signal_id'], row, self.members.get(row['signal_id'], ()))
        for ordinal, row in enumerate(result.get('correlations', []), 1):
            # The correlation domain has endpoint IDs but no correlation_id.
            ref = 'correlation:' + str(ordinal)
            self.add('correlation', ref, row, ['signal:' + row['source'], 'signal:' + row['target']])
        for row in result.get('incidents', []):
            ids = set(row.get('signal_ids', []))
            local = (f'correlation:{n}' for n, edge in enumerate(result.get('correlations', []), 1)
                     if edge['source'] in ids and edge['target'] in ids)
            self.add('incident', 'incident:' + row['incident_id'], row,
                     chain(('signal:' + sid for sid in row.get('signal_ids', [])), local))
        for row in result.get('rca', []):
            self.add('rca', 'rca:' + row['incident_id'], dict(kind='deterministic', **row),
                     ['incident:' + row['incident_id']])
        if result.get('case_analysis'):
            self.add('rca', 'expert:result', dict(kind='expert_interpretation', **result['case_analysis']), ['expert:input'])
        elif result.get('case_analysis_error'):
            self.add('rca', 'expert:status', dict(kind='expert_status', status='Expert interpretation unavailable; deterministic RCA retained.'),
                     ['expert:input'] if any(i.ref == 'expert:input' for i in self.trace.stages['rca']) else [])
        payload = asdict(self.trace)
        # Includes envelope in the actual encoded budget, even for empty runs.
        assert len(encoded(payload).encode('utf-8')) <= self.trace.limits.max_bytes
        return payload
