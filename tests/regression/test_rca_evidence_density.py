"""Expert-only grouping and edge diversity; identities and graph are immutable."""
from copy import deepcopy
import json

import pytest

from test_rca_expert import isolated, gateway, fake_gateway, response_body, answer


def redundant_case(shared_balance_identity=False):
    patterns = [
        'ProgrammingError in chatbot_param_list File "/app/sqlalchemy/engine.py"',
        'ProgrammingError in chatbot_param_list',
        'SOAP retry AssetReadException',
        *[f'[BALANCE] FAILED AssetReadException duration={duration}ms' for duration in (648, 682, 694, 701, 714)],
    ]
    signals = [dict(signal_id=f'real-{index}', template_id=f'template-{index}', template=pattern,
        service_name='service', component='error_handlers' if index < 2 else 'soap_client' if index == 2 else 'service_executor',
        scope='service', qualified=True, qualification_score=.8, qualification_evidence=['açık_hata_semantiği'],
        severity_min=3, count=42 if index < 2 else 12 if index == 2 else 1,
        first_seen_ms=1000 + index, last_seen_ms=1000 + index, timestamp_resolved=True,
        window_start_ms=0, window_end_ms=60000) for index, pattern in enumerate(patterns)]
    if shared_balance_identity:
        for row in signals[3:]:
            row['template_id'] = 'authoritative-balance-family'
            row['template'] = '[BALANCE] FAILED AssetReadException duration=<NUM>ms'
    incident = dict(incident_id='incident', signal_ids=[row['signal_id'] for row in signals],
        severity_min=3, event_count=101, signal_count=8, start_ms=1000,
        probable_root={'signal_id': 'real-0', 'entity': 'service'})
    edges = [dict(source=a['signal_id'], target=b['signal_id'], score=.58,
                  evidence=['aynı_servis', 'zaman_yakınlığı'], time_gap_ms=b['first_seen_ms'] - a['first_seen_ms'])
             for index, a in enumerate(signals) for b in signals[index + 1:]]
    rca = [dict(incident_id='incident', root_cause_candidates=[dict(signal_id=f'real-{index}', score=.9-index*.1,
                                                                 evidence=['ranked-root']) for index in range(3)])]
    return [incident], edges, signals, rca


def build(data):
    from rca_layer.evidence import RCAEvidenceSelector, EvidenceLimits
    return RCAEvidenceSelector(EvidenceLimits()).build(*data)


@pytest.mark.parametrize('shared', [False, True])
def test_balance_variants_group_only_with_authoritative_identity(shared):
    data = redundant_case(shared)
    before = deepcopy(data)
    pack = build(data)
    balance = [row for row in json.loads(pack.serialized)['signals'] if row['component'] == 'service_executor']
    assert len(balance) == (1 if shared else 5)
    if shared:
        assert balance[0]['represented_signal_count'] == 5
        assert balance[0]['signal_windows'] == 1
        assert balance[0]['total_occurrences'] == 5
        assert dict(pack.signal_members)[balance[0]['ref']] == tuple(f'real-{index}' for index in range(3, 8))
    assert data == before
    assert [row['signal_id'] for row in data[2]] == [f'real-{index}' for index in range(8)]
    assert pack == build(data)
    data[1].reverse()
    data[2].reverse()
    assert pack == build(data)


@pytest.mark.parametrize('shared', [False, True])
def test_programming_errors_require_identity_proof_not_similar_text(shared):
    data = redundant_case()
    if shared:
        data[2][1]['template_id'] = data[2][0]['template_id']
    pack = build(data)
    rows = [row for row in json.loads(pack.serialized)['signals'] if row['component'] == 'error_handlers']
    assert len(rows) == (1 if shared else 2)
    if shared:
        assert rows[0]['total_occurrences'] == 84
        assert dict(pack.signal_members)[rows[0]['ref']] == ('real-0', 'real-1')
    # Deterministic root order/scores remain authoritative outside compression.
    roots = json.loads(pack.serialized)['root_candidates']
    assert [root['score'] for root in roots] == ([.9, .7] if shared else [.9, .8, .7])


def test_same_template_in_distinct_incidents_does_not_mix_group_membership():
    data = redundant_case(True)
    data[0][0]['signal_ids'] = ['real-3']
    data[0].append(dict(data[0][0], incident_id='other', signal_ids=['real-4']))
    pack = build(data)
    assert len(pack.signal_members) == 2
    assert {members for _, members in pack.signal_members} == {('real-3',), ('real-4',)}
    assert not json.loads(pack.serialized)['correlations']


def test_group_reference_resolves_all_original_signals_after_strict_validation():
    from rca_layer.expert_output import RCAExpertOutputValidator
    pack = build(redundant_case(True))
    ref = next(ref for ref, members in pack.signal_members if len(members) == 5)
    response = answer()
    response['kanit_referanslari'] = [ref]
    validated = RCAExpertOutputValidator().validate(json.dumps(response), pack, 'stop')
    assert validated['kanit_sinyal_idleri'] == [f'real-{index}' for index in range(3, 8)]
    assert validated['etkilenen_olaylar'] == ['incident']
    assert 'real-' not in pack.serialized


def test_redundant_edges_are_a_cap_not_a_fill_target_and_keep_component_diversity():
    data = redundant_case()
    original = deepcopy(data)
    pack = build(data)
    payload = json.loads(pack.serialized)
    assert len(payload['signals']) == 8
    assert len(payload['correlations']) == 5 < 10
    by_ref = {row['ref']: row for row in payload['signals']}
    pairs = {(by_ref[edge['source']]['component'], by_ref[edge['target']]['component']) for edge in payload['correlations']}
    assert pairs == {('error_handlers', 'error_handlers'), ('error_handlers', 'soap_client'),
                     ('error_handlers', 'service_executor'), ('soap_client', 'service_executor'),
                     ('service_executor', 'service_executor')}
    assert data == original  # Includes the complete actual graph and deterministic RCA.


def test_distinct_topology_evidence_and_strongest_representative_survive():
    data = redundant_case()
    chosen = next(edge for edge in data[1] if (edge['source'], edge['target']) == ('real-4', 'real-7'))
    chosen.update(score=.99, evidence=['servis_bağımlılığı', 'nedensel_zaman_sırası'], dependency_path=['api', 'db'])
    pack = build(data)
    edges = json.loads(pack.serialized)['correlations']
    assert any(edge.get('dependency_path') == ['api', 'db'] and edge['score'] == .99 for edge in edges)
    assert len(edges) == 6
    refs = dict(pack.signal_aliases)
    assert all(edge['source'] in refs and edge['target'] in refs for edge in edges)
    data[1].reverse()
    assert build(data) == pack


def test_equivalent_edges_prefer_score_then_numeric_gap_then_stable_pair():
    data = redundant_case()
    variants = [edge for edge in data[1] if edge['source'] in ('real-0', 'real-1') and edge['target'] in ('real-3', 'real-4')]
    for edge in variants:
        edge.update(score=.95, time_gap_ms=10)
    variants[-1]['time_gap_ms'] = 2
    pack = build(data)
    refs = dict(pack.signal_aliases)
    selected = [(refs[edge['source']], refs[edge['target']]) for edge in json.loads(pack.serialized)['correlations']]
    assert (variants[-1]['source'], variants[-1]['target']) in selected
    data[1].reverse()
    assert build(data) == pack


def test_debug_prints_compact_pack_with_unchanged_glm_request(gateway, monkeypatch, capsys):
    from rca_layer.expert_output import response_format
    pack = build(redundant_case(True))
    call, requests, _ = fake_gateway(monkeypatch, gateway, [(200, response_body())])
    monkeypatch.setenv('RCA_DEBUG', 'true')
    prompt = gateway[0].load_prompt('rca_expert.md')
    call('Ajan_2_RCA_Expert', prompt, pack.serialized, temperature=0.0, max_tokens=1600, response_format=response_format())
    output = capsys.readouterr().out
    assert f'[RCA DEBUG] EVIDENCE PACK\n{pack.serialized}\n' in output
    assert 'include_reasoning=absent' in output
    assert 'Authorization' not in output and 'synthetic-api-key' not in output
    assert requests[0]['response_format'] == response_format()
    assert requests[0]['chat_template_kwargs'] == {'enable_thinking': False}
    assert requests[0]['max_tokens'] == 1600 and 'include_reasoning' not in requests[0]
