"""Deterministic evidence selection, traceability, budget and input immutability."""
from copy import deepcopy
from dataclasses import replace
import json
import math
import random

import pytest

from test_rca_expert import isolated, gateway


def many_signals(groups=12, windows=3):
    signals = []
    for group in range(groups):
        for window in range(windows):
            signals.append(dict(signal_id=f'sig:{group:03}:{window:02}:' + 'long-internal-id-' * 8,
                template_id=f'template-{group}', template=f'Pattern {group}: failure in component',
                count=group + 1, qualified=True, severity_min=3 if group < 3 else 4,
                qualification_score=.1 if group == 0 else .8, qualification_evidence=['repeated', 'reliable'],
                service_name=f'service-{group}', component=f'component-{group}', scope=f'ns/service-{group}',
                first_seen_ms=100000 + window * 60000 + group, last_seen_ms=100100 + window * 60000 + group,
                timestamp_resolved=True, window_start_ms=window * 60000, window_end_ms=(window + 1) * 60000,
                hosts=['unknown'], data_centers=['DC1'], raw='RAW-MUST-NOT-APPEAR', event_ids=['EVENT-MUST-NOT-APPEAR'],
                metadata={'Authorization': 'SECRET-MUST-NOT-APPEAR'}))
    ids = [row['signal_id'] for row in signals]
    incident = dict(incident_id='long-incident-id-' * 15, signal_ids=ids, severity_min=3,
                    event_count=sum(row['count'] for row in signals), signal_count=len(signals), confidence=.75,
                    probable_root={'kind': 'service', 'entity': 'service-0', 'signal_id': ids[0]},
                    context={'hosts': [], 'racks': ['unknown'], 'services': ['service-0', 'service-1'],
                             'dependencies': [{'path': ['service-1', 'service-0'], 'raw': 'PRIVATE-PATH-METADATA'}]})
    edges = [dict(source=source, target=target, score=.7, time_gap_ms=100,
                  evidence=['same_service', 'temporal'], raw='PRIVATE-EDGE')
             for index, source in enumerate(ids) for target in ids[index + 1:]]
    edges[0]['dependency_path'] = ['service-1', 'service-0']
    roots = [dict(incident_id=incident['incident_id'], root_cause_candidates=[
        dict(signal_id=ids[index * windows], score=.9 - index * .1, evidence=['ranked-root']) for index in range(3)])]
    return [incident], edges, signals, roots


def build(data, limits=None, **kwargs):
    from rca_layer.evidence import RCAEvidenceSelector
    return RCAEvidenceSelector(limits).build(*data, **kwargs)


def test_selection_and_aliases_are_stable_under_input_reordering(isolated):
    data = many_signals()
    expected = build(data)
    shuffled = deepcopy(data)
    random.Random(42).shuffle(shuffled[1])
    random.Random(23).shuffle(shuffled[2])
    shuffled[0][0]['signal_ids'].reverse()
    actual = build(shuffled)
    assert actual == expected
    assert list(dict(actual.signal_aliases)) == [f'S{index}' for index in range(1, 9)]


def test_roots_precede_score_and_equivalent_windows_are_aggregated(isolated):
    data = many_signals()
    pack = build(data)
    payload = json.loads(pack.serialized)
    selected = payload['signals']
    assert [row['service_name'] for row in selected[:3]] == ['service-0', 'service-1', 'service-2']
    assert len({row['component'] for row in selected}) == 8
    assert selected[0]['qualification_score'] == .1
    assert selected[0]['signal_windows'] == 3 and selected[0]['total_occurrences'] == 3
    assert selected[0]['first_seen_ms'] == 100000 and selected[0]['last_seen_ms'] == 220100
    assert [row['signal_ref'] for row in payload['root_candidates']] == ['S1', 'S2', 'S3']
    assert [row['score'] for row in payload['root_candidates']] == [.9, .8, .7]
    assert dict(pack.signal_aliases)['S1'] == data[2][0]['signal_id']
    assert dict(pack.signal_members)['S1'] == tuple(row['signal_id'] for row in data[2][:3])


def test_equal_scores_have_stable_identity_tiebreak_and_component_diversity(isolated):
    data = many_signals(groups=14, windows=1)
    data[3][0]['root_cause_candidates'] = []
    data[0][0].pop('probable_root')
    for row in data[2]:
        row.update(severity_min=4, qualification_score=.8, first_seen_ms=100000, last_seen_ms=100100)
    one = build(data)
    data[2].reverse()
    data[1].reverse()
    assert build(data).serialized == one.serialized
    assert len({row['component'] for row in json.loads(one.serialized)['signals']}) == 8


def test_duplicate_candidate_groups_do_not_repeat_root_hints(isolated):
    data = many_signals()
    roots = data[3][0]['root_cause_candidates']
    roots.insert(1, dict(roots[0], signal_id=data[2][1]['signal_id']))
    payload = json.loads(build(data).serialized)
    assert len(payload['root_candidates']) == 3
    assert len({row['signal_ref'] for row in payload['root_candidates']}) == 3


def test_correlations_are_bounded_deduplicated_and_root_relevant(isolated):
    data = many_signals()
    root_edge = next(edge for edge in data[1] if edge['source'] == data[2][0]['signal_id'] and edge['target'] == data[2][3]['signal_id'])
    root_edge.update(score=.99, evidence=['dependency', 'precedence'], dependency_path=['service-1', 'service-0'])
    payload = json.loads(build(data).serialized)
    selected = {row['ref'] for row in payload['signals']}
    edges = payload['correlations']
    assert len(edges) == 10
    assert all(edge['source'] in selected and edge['target'] in selected for edge in edges)
    assert len({(edge['source'], edge['target']) for edge in edges}) == len(edges)
    assert edges[0]['source'] == 'S1' and edges[0]['target'] == 'S2'
    assert edges[0]['dependency_path'] == ['service-1', 'service-0']


def test_unknown_context_and_private_payloads_are_omitted(isolated):
    data = many_signals()
    before = deepcopy(data)
    pack = build(data)
    payload = json.loads(pack.serialized)
    assert payload['incidents'][0]['context']['dependency_paths'] == [['service-1', 'service-0']]
    for forbidden in ('unknown', 'RAW-MUST', 'EVENT-MUST', 'SECRET-MUST', 'PRIVATE-PATH', 'PRIVATE-EDGE', 'signal_ids', 'sig:', 'long-incident-id'):
        assert forbidden not in pack.serialized
    assert data == before
    assert 'sig:' not in repr(pack) and 'Pattern' not in repr(pack)


def test_unresolved_time_is_not_promoted_to_source_time(isolated):
    data = many_signals()
    data[2][0]['timestamp_resolved'] = False
    root = json.loads(build(data).serialized)['signals'][0]
    assert 'first_seen_ms' not in root and 'last_seen_ms' not in root


def test_template_is_bounded_and_traceback_body_is_not_sent(isolated):
    from rca_layer.evidence import EvidenceLimits
    data = many_signals()
    data[2][0]['template'] = 'Traceback (most recent call last):\n  File "/private/path"\nProgrammingError: ' + 'a' * 5000
    pack = build(data, replace(EvidenceLimits(), max_pattern_chars=96))
    root = json.loads(pack.serialized)['signals'][0]
    assert root['pattern'].startswith('ProgrammingError:') and len(root['pattern']) == 96
    assert root['pattern_truncated'] is True
    assert '/private/path' not in pack.serialized and 'Traceback' not in pack.serialized


@pytest.mark.parametrize('limit', [2500, 4000, 6000])
def test_stress_compaction_produces_valid_json_and_retains_required_root(isolated, limit):
    from rca_layer.evidence import EvidenceLimits
    data = many_signals(groups=40, windows=4)
    for row in data[2]:
        row['template'] += ' Unicode ş hata ' * 100
    limits = replace(EvidenceLimits(), max_input_chars=limit, max_input_bytes=limit * 2)
    pack = build(data, limits)
    payload = json.loads(pack.serialized)
    assert len(pack.serialized) <= limit and len(pack.serialized.encode('utf-8')) <= limit * 2
    assert payload['incidents'][0]['event_count'] == data[0][0]['event_count']
    assert payload['root_candidates'][0]['signal_ref'] == 'S1'
    assert payload['signals'][0]['ref'] == 'S1'
    assert pack == build(data, limits)
    assert dict(pack.diagnostics)['compacted'] is True


def test_optional_context_and_edges_are_removed_before_root_candidates(isolated):
    from rca_layer.evidence import EvidenceLimits
    data = many_signals()
    payload = json.loads(build(data, replace(EvidenceLimits(), max_input_chars=2800)).serialized)
    assert 'context' not in payload['incidents'][0]
    assert len(payload['correlations']) == 1
    assert [row['signal_ref'] for row in payload['root_candidates']] == ['S1', 'S2', 'S3']


def test_size_diagnostics_include_prompt_and_schema_overhead(isolated):
    pack = build(many_signals(), overhead_chars=500, overhead_bytes=800)
    diagnostics = dict(pack.diagnostics)
    assert diagnostics['serialized_chars'] == len(pack.serialized)
    assert diagnostics['serialized_bytes'] == len(pack.serialized.encode('utf-8'))
    assert diagnostics['input_chars'] == len(pack.serialized) + 500
    assert diagnostics['input_bytes'] == len(pack.serialized.encode('utf-8')) + 800
    assert diagnostics['approximate_tokens'] == math.ceil(diagnostics['input_chars'] / 4)
    assert 'prompt_tokens' not in diagnostics


def test_impossibly_small_budget_never_cuts_json_or_calls_model(isolated):
    from rca_layer.evidence import EvidenceLimits, EvidenceBudgetError
    with pytest.raises(EvidenceBudgetError):
        build(many_signals(), replace(EvidenceLimits(), max_input_chars=100))


def test_changed_evidence_changes_serialized_cache_input(isolated):
    data = many_signals()
    before = build(data).serialized
    data[2][0]['count'] += 1
    assert before != build(data).serialized


def test_limits_are_centrally_configurable(isolated, monkeypatch):
    from rca_layer.evidence import EvidenceLimits
    monkeypatch.setenv('RCA_EVIDENCE_MAX_SIGNALS', '4')
    monkeypatch.setenv('RCA_EVIDENCE_MAX_CORRELATIONS', '2')
    limits = EvidenceLimits.from_env()
    payload = json.loads(build(many_signals(), limits).serialized)
    assert len(payload['signals']) == 4 and len(payload['correlations']) == 2


def test_cross_incident_edges_and_unqualified_signals_are_not_selected(isolated):
    data = many_signals(groups=4, windows=1)
    data[2][-1]['qualified'] = False
    data[0][0]['signal_ids'] = [row['signal_id'] for row in data[2][:2]]
    other = dict(data[0][0], incident_id='other', signal_ids=[data[2][2]['signal_id']])
    data[0].append(other)
    pack = build(data)
    membership = dict(pack.signal_incidents)
    assert len(pack.signal_aliases) == 3
    assert all(set(membership[edge['source']]) & set(membership[edge['target']]) for edge in json.loads(pack.serialized)['correlations'])
