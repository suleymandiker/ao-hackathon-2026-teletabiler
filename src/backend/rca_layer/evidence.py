"""Read-only, deterministic preparation of compact RCA prompt evidence.

More evidence is not automatically better evidence. Only the prompt representation
is aggregated; backend identities, decisions and persisted templates stay intact.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field, fields
import json
import math
import os
import re


def serialize(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)


def number(value):
    return value if type(value) in (int, float) and math.isfinite(value) and value >= 0 else None


def text(value, limit=120):
    if not isinstance(value, str) or value.strip().lower() in ('', 'unknown', 'null', 'none', 'genel'):
        return ''
    value = re.sub(r'(?i)\b(?:authorization|password|api[_-]?key|(?:access[_-]?)?token|secret)["\']?\s*[:=]\s*[^\r\n,;]+', '[redacted]', value)
    value = re.sub(r'(?i)\bbearer\s+\S+', '[redacted]', value)
    value = re.sub(r'\beyJ[\w-]+\.[\w-]+\.[\w-]+\b', '[redacted]', value)
    return ' '.join(value.split())[:limit]


def strings(values, limit=4):
    return sorted({clean for value in values if (clean := text(value))})[:limit]


def pattern(value, limit):
    if not isinstance(value, str):
        return '', False
    # A normalized exception line is more useful than traceback frames. Otherwise
    # use the first template line; no raw/message/event fields are consulted.
    error = re.search(r'(?m)^\s*([\w.]+(?:Error|Exception):[^\r\n]*)', value)
    summary = error.group(1) if error else value.split('\n', 1)[0]
    bounded = text(summary, limit)
    return bounded, len(summary) > limit or '\n' in value or summary != value


@dataclass(frozen=True)
class EvidenceLimits:
    max_signals: int = 8
    max_correlations: int = 10
    max_root_candidates: int = 3
    max_pattern_chars: int = 240
    max_input_chars: int = 12000
    max_input_bytes: int = 24000

    def __post_init__(self):
        if any(type(getattr(self, item.name)) is not int or getattr(self, item.name) < 1 for item in fields(self)):
            raise ValueError('Invalid RCA evidence limits')

    @classmethod
    def from_env(cls):
        return cls(**{item.name: int(os.environ.get('RCA_EVIDENCE_' + item.name.upper(), item.default))
                      for item in fields(cls)})


class EvidenceBudgetError(ValueError):
    """Required evidence cannot fit; skip the expert instead of cutting JSON."""


@dataclass(frozen=True)
class RCAEvidencePack:
    serialized: str = field(repr=False)
    signal_aliases: tuple[tuple[str, str], ...] = field(repr=False)
    signal_members: tuple[tuple[str, tuple[str, ...]], ...] = field(repr=False)
    incident_aliases: tuple[tuple[str, str], ...] = field(repr=False)
    signal_incidents: tuple[tuple[str, tuple[str, ...]], ...] = field(repr=False)
    diagnostics: tuple[tuple[str, int | bool], ...]


class RCAEvidenceSelector:
    def __init__(self, limits=None):
        self.limits = limits or EvidenceLimits.from_env()

    def build(self, incidents, correlations, signals, deterministic_rca, *, max_incidents=5,
              overhead_chars=0, overhead_bytes=0):
        limits = self.limits
        selected_incidents = sorted(incidents, key=lambda row: (
            number(row.get('severity_min')) or 7, number(row.get('start_ms')) or 0, row['incident_id']))[:max_incidents]
        incident_aliases = {row['incident_id']: f'I{index}' for index, row in enumerate(selected_incidents, 1)}
        memberships = defaultdict(set)
        for incident in selected_incidents:
            for sid in incident.get('signal_ids', []):
                memberships[sid].add(incident_aliases[incident['incident_id']])
        by_id = {row['signal_id']: row for row in signals
                 if row.get('signal_id') in memberships and row.get('qualified') is True}
        rca_map = {row['incident_id']: row for row in deterministic_rca}
        hints = []
        for incident in selected_incidents:
            for rank, candidate in enumerate(rca_map.get(incident['incident_id'], {}).get('root_cause_candidates', [])):
                if candidate.get('signal_id') in by_id:
                    hints.append((rank, incident_aliases[incident['incident_id']], candidate))
        hints.sort(key=lambda item: (item[0], item[1]))  # Existing rank, no new RCA ranking.
        ranks = {}
        for rank, ref, candidate in hints:
            ranks.setdefault(candidate['signal_id'], (rank, ref))
        probable = {row.get('probable_root', {}).get('signal_id') for row in selected_incidents}
        grouped = defaultdict(list)
        for sid, row in sorted(by_id.items()):
            # Reuse authoritative template identity, never derive a new family
            # from display-text similarity. Members must belong to the same
            # selected incidents so every reference resolves to relevant evidence.
            key = (row.get('template_id') or sid,) + tuple(row.get(name) or '' for name in
                   ('service_name', 'component', 'namespace', 'cluster_name', 'scope', 'alarm_type')) + (
                       tuple(sorted(memberships[sid])),)
            grouped[key].append(row)
        groups, sid_group = {}, {}
        for key, rows in grouped.items():
            def priority(row):
                return (ranks.get(row['signal_id'], (math.inf, '')), row['signal_id'] not in probable,
                        number(row.get('severity_min')) or 7,
                        row.get('first_seen_ms', math.inf) if row.get('timestamp_resolved') is True else math.inf,
                        -(number(row.get('qualification_score')) or 0), row['signal_id'])
            representative = min(rows, key=priority)
            groups[key] = dict(rows=rows, representative=representative, priority=priority(representative), degree=0)
            for row in rows:
                sid_group[row['signal_id']] = key
        if not groups:
            raise EvidenceBudgetError('no_selected_evidence')
        local_edges = []
        for edge in correlations:
            source, target = edge.get('source'), edge.get('target')
            if source in by_id and target in by_id and memberships[source] & memberships[target]:
                local_edges.append(edge)
                groups[sid_group[source]]['degree'] += 1
                groups[sid_group[target]]['degree'] += 1

        root_groups = []
        selected_hints = []
        for _, ref, candidate in hints:
            key = sid_group[candidate['signal_id']]
            if key not in root_groups:
                root_groups.append(key)
                selected_hints.append((ref, candidate, key))
            if len(root_groups) >= min(limits.max_root_candidates, limits.max_signals):
                break
        chosen = list(root_groups)
        seen_components = {key[1:3] for key in chosen}
        seen_patterns = {key[0] for key in chosen}
        while len(chosen) < min(limits.max_signals, len(groups)):
            remaining = (key for key in groups if key not in chosen)
            key = min(remaining, key=lambda key: (
                groups[key]['representative']['signal_id'] not in probable,
                key[1:3] in seen_components, key[0] in seen_patterns,
                groups[key]['priority'][2], -groups[key]['degree'],
                -max(number(row.get('qualification_score')) or 0 for row in groups[key]['rows']),
                groups[key]['priority'][3], key))
            chosen.append(key)
            seen_components.add(key[1:3])
            seen_patterns.add(key[0])
        aliases = {key: f'S{index}' for index, key in enumerate(chosen, 1)}
        signal_views = []
        for key in chosen:
            rows, representative = groups[key]['rows'], groups[key]['representative']
            summary, truncated = pattern(representative.get('template'), limits.max_pattern_chars)
            windows = {(row.get('window_start_ms'), row.get('window_end_ms')) for row in rows
                       if row.get('timestamp_resolved') is True
                       and number(row.get('window_start_ms')) is not None
                       and number(row.get('window_end_ms')) is not None}
            item = dict(ref=aliases[key], incident_refs=sorted({ref for row in rows for ref in memberships[row['signal_id']]}),
                        signal_windows=len(windows))
            if len(rows) != len(windows):
                item['represented_signal_count'] = len(rows)
            if summary:
                item.update(pattern=summary, pattern_truncated=truncated)
            counts = [number(row.get('count')) for row in rows]
            if all(value is not None for value in counts):
                item['total_occurrences'] = sum(counts)
            for name, reducer in (('severity_min', min), ('qualification_score', max)):
                values = [number(row.get(name)) for row in rows]
                if all(value is not None for value in values):
                    item[name] = reducer(values)
            if all(row.get('timestamp_resolved') is True for row in rows):
                for name, reducer in (('first_seen_ms', min), ('last_seen_ms', max)):
                    values = [number(row.get(name)) for row in rows]
                    if all(value is not None for value in values):
                        item[name] = reducer(values)
            for name in ('service_name', 'component', 'alarm_type'):
                if value := text(representative.get(name)):
                    item[name] = value
            item['evidence'] = strings(value for row in rows for value in row.get('qualification_evidence', []))
            context = {}
            for name in ('hosts', 'data_centers', 'racks', 'business_criticalities'):
                if values := strings(value for row in rows for value in row.get(name, [])):
                    context[name] = values
            if context:
                item['context'] = context
            if not item['evidence']:
                del item['evidence']
            signal_views.append(item)

        incident_views = []
        for incident in selected_incidents:
            item = dict(ref=incident_aliases[incident['incident_id']])
            for name in ('severity_min', 'confidence', 'event_count', 'signal_count'):
                if number(incident.get(name)) is not None:
                    item[name] = incident[name]
            root = incident.get('probable_root') or {}
            root_view = {name: value for name in ('kind', 'entity', 'alarm_type') if (value := text(root.get(name)))}
            if root_view:
                item['probable_root'] = root_view
            context = incident.get('context') or {}
            compact_context = {name: values for name in ('services', 'components', 'hosts', 'data_centers', 'racks', 'business_criticalities')
                               if (values := strings(context.get(name, [])))}
            paths = sorted({tuple(strings_path) for dep in context.get('dependencies', [])
                            if isinstance(dep, dict) and (strings_path := self._path(dep.get('path')))})[:4]
            if paths:
                compact_context['dependency_paths'] = paths
            if compact_context:
                item['context'] = compact_context
            incident_views.append(item)
        root_views = [dict(incident_ref=ref, signal_ref=aliases[key],
                           **({'score': candidate['score']} if number(candidate.get('score')) is not None else {}),
                           evidence=strings(candidate.get('evidence', [])))
                      for ref, candidate, key in selected_hints]
        edges = self._edges(local_edges, sid_group, aliases, set(root_groups), groups)
        payload = dict(version=1, incidents=incident_views, signals=signal_views,
                       correlations=edges, root_candidates=root_views)
        reduced = self._compact(payload, overhead_chars, overhead_bytes)
        serialized = serialize(payload)
        kept = {row['ref'] for row in payload['signals']}
        diagnostics = dict(incidents_total=len(incidents), incidents_selected=len(incident_views), signals_total=len(by_id),
                           signals_selected=len(kept), signal_groups=len(groups), correlations_total=len(local_edges),
                           correlations_selected=len(payload['correlations']), root_candidates_selected=len(payload['root_candidates']),
                           serialized_chars=len(serialized), serialized_bytes=len(serialized.encode('utf-8')),
                           input_chars=len(serialized) + overhead_chars, input_bytes=len(serialized.encode('utf-8')) + overhead_bytes,
                           approximate_tokens=math.ceil((len(serialized) + overhead_chars) / 4),
                           compacted=bool(reduced or len(kept) < len(by_id) or len(edges) < len(local_edges)
                                          or any(row.get('pattern_truncated') for row in signal_views)))
        return RCAEvidencePack(serialized,
            tuple((aliases[key], groups[key]['representative']['signal_id']) for key in chosen if aliases[key] in kept),
            tuple((aliases[key], tuple(row['signal_id'] for row in groups[key]['rows'])) for key in chosen if aliases[key] in kept),
            tuple((alias, iid) for iid, alias in incident_aliases.items()),
            tuple((row['ref'], tuple(row['incident_refs'])) for row in payload['signals']), tuple(diagnostics.items()))

    @staticmethod
    def _path(value):
        if not isinstance(value, (list, tuple)) or not 2 <= len(value) <= 8:
            return []
        result = [text(node) for node in value]
        return result if all(result) else []  # Never reconnect a partially omitted path.

    def _edges(self, edges, sid_group, aliases, roots, groups):
        best = {}
        for edge in edges:
            source, target = sid_group[edge['source']], sid_group[edge['target']]
            if source == target or source not in aliases or target not in aliases:
                continue
            item = dict(source=aliases[source], target=aliases[target], evidence=strings(edge.get('evidence', [])))
            for name in ('score', 'time_gap_ms'):
                if number(edge.get(name)) is not None:
                    item[name] = edge[name]
            if path := self._path(edge.get('dependency_path')):
                item['dependency_path'] = path
            # One strongest, deterministic representative per directed group pair.
            priority = (-(number(edge.get('score')) or 0), -bool(path), -len(item['evidence']),
                        item.get('time_gap_ms', math.inf), serialize(item))
            pair = (source, target)
            if pair not in best or priority < best[pair][0]:
                best[pair] = (priority, item, tuple(sorted({value for value in edge.get('evidence', [])
                                                          if isinstance(value, str)})))
        # A cap is not a fill target. Equivalent evidence between the same
        # directed service/component/scope/incident groups adds no new fact just
        # because its signal alias or gap differs. Preserve distinct dependency
        # paths and root roles; choose the strongest/shortest stable representative.
        families = {}
        for pair, entry in best.items():
            item = entry[1]
            family = (pair[0][1:], pair[1][1:], entry[2], tuple(item.get('dependency_path', [])),
                      pair[0] in roots, pair[1] in roots)
            if family not in families or (entry[0], pair) < (best[families[family]][0], families[family]):
                families[family] = pair
        best = {pair: best[pair] for pair in families.values()}
        selected, reasons, components = [], set(), set()
        while best and len(selected) < self.limits.max_correlations:
            def priority(pair):
                item = best[pair][1]
                service_pair = (pair[0][1:3], pair[1][1:3])
                return (not bool(item.get('dependency_path')), not any(key in roots for key in pair),
                        not bool(set(item['evidence']) - reasons), service_pair in components,
                        -(item.get('score') or 0), item.get('time_gap_ms', math.inf), pair)
            pair = min(best, key=priority)
            item = best.pop(pair)[1]
            selected.append(item)
            reasons.update(item['evidence'])
            components.add((pair[0][1:3], pair[1][1:3]))
        return selected

    def _compact(self, payload, overhead_chars, overhead_bytes):
        def fits():
            value = serialize(payload)
            return (len(value) + overhead_chars <= self.limits.max_input_chars
                    and len(value.encode('utf-8')) + overhead_bytes <= self.limits.max_input_bytes)

        def drop_signal(ref):
            payload['signals'][:] = [row for row in payload['signals'] if row['ref'] != ref]
            payload['correlations'][:] = [row for row in payload['correlations'] if ref not in (row['source'], row['target'])]

        if fits():
            return False
        for row in payload['incidents'] + payload['signals']:
            row.pop('context', None)
        while not fits() and len(payload['correlations']) > 1:
            payload['correlations'].pop()
        roots = {row['signal_ref'] for row in payload['root_candidates']}
        protected = roots | {ref for edge in payload['correlations'] for ref in (edge['source'], edge['target'])}
        for row in reversed(payload['signals'][:]):
            if not fits() and row['ref'] not in protected:
                drop_signal(row['ref'])
        if not fits():
            for row in payload['signals']:
                if len(row.get('pattern', '')) > 80:
                    row['pattern'] = row['pattern'][:80]
                    row['pattern_truncated'] = True
        while not fits() and len(payload['root_candidates']) > 1:
            removed = payload['root_candidates'].pop()['signal_ref']
            if removed not in {row['signal_ref'] for row in payload['root_candidates']}:
                drop_signal(removed)
        if not fits():
            payload['correlations'].clear()
            roots = {row['signal_ref'] for row in payload['root_candidates']}
            # Keep at least one representative when deterministic hints are absent.
            protected = roots or {payload['signals'][0]['ref']}
            for row in reversed(payload['signals'][:]):
                if not fits() and row['ref'] not in protected:
                    drop_signal(row['ref'])
        if not fits():
            raise EvidenceBudgetError('required_evidence_exceeds_budget')
        return True
