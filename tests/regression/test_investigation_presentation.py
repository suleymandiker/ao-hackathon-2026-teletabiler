"""Presentation derives supported facts, never decisions or raw payload dumps."""
from copy import deepcopy
from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest

from test_opensearch_application import app, local_catalog_dir, SECRET, RAW, CURSOR


@pytest.fixture
def presenter(app, monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[2] / 'src' / 'frontend'))
    from presentation import InvestigationPresenter
    return InvestigationPresenter(lambda value: app.module.safe_text(value, app.module.load_connection()))


def result_fixture():
    rows = [dict(signal_id=f'signal-{index}', template_id='unchanged-template', template='HTTP request completed',
                 count=count, qualified=False, qualification_reason='gürültü_olarak_bastırıldı',
                 severity_min=6, service_name='checkout', hosts=['pod-one'], event_ids=[f'event-{index}'],
                 timestamp_resolved=True, first_seen_ms=1791104400000 + index * 60000,
                 last_seen_ms=1791104401000 + index * 60000,
                 qualification_evidence=['güvenilir_şablon'], qualification_score=.1)
            for index, count in enumerate([3, 2])]
    return dict(stats=dict(segmented=5, parsed=5, templated=5, signal_candidates=2, qualified_signals=0,
                           noise_suppressed=2, correlations=0, incidents=0, rca=0), signals=rows,
                qualified_signals=[], correlations=[], incidents=[], rca=[],
                source_summary=dict(records_read=9, pages_read=2, record_budget=300, budget_reached=False,
                                    assembled_stream_count=1, unassembled_count=1,
                                    unassembled_by_reason={'no_policy': 1}))


def build(presenter, result=None):
    return presenter.build(result or result_fixture(), source='OpenShift logları', target='ns / checkout')


def test_technical_details_show_resolved_index_and_base_pattern(presenter):
    result = result_fixture()
    result['source_summary'].update(index_expression='gocpbmgpup1*', resolved_index='gocpbmgpup1-2026.10.05')
    details = dict(build(presenter, result).technical)
    assert details['İndeks'] == 'gocpbmgpup1-2026.10.05'
    assert details['Kaynak indeks deseni'] == 'gocpbmgpup1*'


def test_legacy_technical_details_keep_base_index_without_inventing_resolved_scope(presenter):
    result = result_fixture()
    result['source_summary'].update(index_expression='gocpbmgpup1*')
    details = dict(build(presenter, result).technical)
    assert details['İndeks'] == 'gocpbmgpup1*'
    assert 'Kaynak indeks deseni' not in details
    assert 'Document OpenShift cluster UUID' not in details


def test_technical_details_show_explicit_document_uuid(presenter):
    result = result_fixture()
    result['source_summary'].update(source_scope='logical-alias', document_cluster_id='11111111-2222-4333-8444-555555555555')
    details = dict(build(presenter, result).technical)
    assert details['Kaynak kapsamı'] == 'logical-alias'
    assert details['Document OpenShift cluster UUID'] == '11111111-2222-4333-8444-555555555555'


def test_pipeline_counts_are_real_and_suppression_is_attention(presenter):
    view = build(presenter)
    assert [stage.value for stage in view.stages] == ['9', '5', '5', '1', '2 → 0', '0', '0', '0']
    assert [stage.status for stage in view.stages] == ['success', 'attention', 'success', 'success', 'attention', 'inactive', 'inactive', 'inactive']
    assert view.tone == 'success' and '2 adayın tamamı' in view.explanation
    assert dict(view.stages[1].details)['Uygun format politikası yok'] == '1'


@pytest.mark.parametrize('key', ['correlations', 'incidents', 'rca'])
def test_zero_output_is_not_processing_failure(presenter, key):
    view = build(presenter)
    stage = next(item for item in view.stages if item.key == {'correlations': 'correlation', 'incidents': 'incident'}.get(key, key))
    assert stage.value == '0' and stage.status == 'inactive'


def test_missing_contract_fields_are_omitted_instead_of_fabricated(presenter):
    view = presenter.build({}, source='Dosya / paket')
    assert 'incident bulunamadı' not in view.title
    assert not view.counts and not view.timeline and not view.patterns and not view.incidents
    assert all(stage.value == '—' for stage in view.stages)
    assert all('Fallback' not in dict(stage.details) for stage in view.stages)


def test_reason_counts_group_only_backend_reasons(presenter):
    source = result_fixture()
    view = build(presenter, source)
    assert view.reason_counts == (('Gürültü olarak bastırıldı', 2),)
    assert not any(text in str(view.reason_counts) for text in ('eşik', 'zaman etkisi', 'severity'))
    del source['signals'][1]['qualification_reason']
    assert build(presenter, source).reason_counts == (('Gürültü olarak bastırıldı', 1),)


def test_patterns_group_by_existing_identity_without_changing_inputs(presenter):
    source = result_fixture()
    before = deepcopy(source)
    view = build(presenter, source)
    pattern, = view.patterns
    assert pattern.pattern_id == 'unchanged-template' and pattern.count == 5
    assert pattern.outcome == 'Suppressed' and pattern.services == ('checkout',)
    assert pattern.first_seen == '2026-10-04T09:00:00+00:00'
    assert pattern.last_seen == '2026-10-04T09:01:01+00:00'
    assert source == before
    with pytest.raises(FrozenInstanceError):
        view.title = 'mutated'


@pytest.mark.parametrize('resolved', [False, None])
def test_unresolved_timestamps_do_not_create_fake_timeline(presenter, resolved):
    source = result_fixture()
    source['signals'][0]['timestamp_resolved'] = resolved
    view = build(presenter, source)
    assert view.patterns[0].first_seen is None and view.patterns[0].last_seen is None
    assert not view.timeline


def test_timeline_contains_only_actual_pattern_observations(presenter):
    view = build(presenter)
    assert len(view.timeline) == 2
    assert all('Pattern' in item.label for item in view.timeline)
    assert not any('oluşturuldu' in item.label or 'RCA' in item.label for item in view.timeline)


def incident_fixture(severity=2):
    return dict(incident_id='incident-one', severity_min=severity, signal_ids=['signal-0'], template_ids=['unchanged-template'],
                signal_count=1, event_count=3, services=['checkout'], status='aday',
                probable_root=dict(entity='checkout', alarm_type='db_conn_pool'))


@pytest.mark.parametrize('severity,label,tone', [(2, 'CRITICAL', 'danger'), (3, 'ERROR', 'danger'),
                                              (4, 'WARNING', 'attention'), (6, 'INFO', 'info')])
def test_incident_severity_and_membership_follow_backend(presenter, severity, label, tone):
    source = result_fixture()
    source['incidents'] = [incident_fixture(severity)]
    source['stats']['incidents'] = 1
    view = build(presenter, source)
    incident, = view.incidents
    assert incident.severity == label and incident.tone == tone
    assert incident.incident_id == 'incident-one'
    assert view.patterns[0].outcome == 'Incident evidence'
    assert not incident.topology and not incident.hypotheses
    assert 'confidence' not in str(incident)


def test_topology_is_conditional_and_rca_is_a_backend_hypothesis(presenter):
    source = result_fixture()
    source['incidents'] = [dict(incident_fixture(), context={'dependencies': [{'path': ['checkout', 'database']}]})]
    source['stats']['incidents'] = 1
    source['rca'] = [dict(incident_id='incident-one', root_cause_candidates=[dict(signal_id='signal-0', score=.7,
                                                                                  evidence=['güvenilir_şablon'])])]
    incident, = build(presenter, source).incidents
    assert incident.topology == (('checkout', 'database'),)
    assert incident.hypotheses == (('HTTP request completed', 'güvenilir şablon'),)
    assert ('RCA aday skoru · HTTP request completed', '0.7') in incident.technical


def test_file_and_structured_alarm_paths_do_not_invent_opensearch_metrics(presenter):
    source = result_fixture()
    del source['source_summary']
    view = presenter.build(source, source='Dosya / paket')
    assert view.stages[0].value == '—'
    assert 'Alınan sayfa' not in dict(view.stages[0].details)
    source['stats']['structured_alarm_fast_path'] = True
    view = presenter.build(source, source='Dosya / paket')
    assert view.stages[0].value == '5'
    assert view.stages[1].value == view.stages[2].value == 'Bypass'
    assert view.stages[1].status == view.stages[2].status == 'inactive'


def test_evidence_redaction_and_display_limits_do_not_change_totals(presenter):
    from presentation import DISPLAY_LIMIT, EVIDENCE_LIMIT, TIMELINE_LIMIT
    source = result_fixture()
    row = source['signals'][0]
    row.update(template=f'HTTP {SECRET} password=synthetic-leak', raw=RAW, cursor=CURSOR,
               Authorization='Bearer synthetic-token', arbitrary_metadata={'body': 'raw-body'})
    row['event_ids'] = [f'event-{index}' for index in range(100)]
    source['pipeline_trace'] = {'parser': {'items': [{'raw': RAW}]}}
    source['event_provenance'] = [{'raw': RAW}]
    source['signals'] = [dict(row, signal_id=f's{index}', template_id=f't{index}') for index in range(DISPLAY_LIMIT + 5)]
    source['stats']['signal_candidates'] = DISPLAY_LIMIT + 5
    view = build(presenter, source)
    assert len(view.patterns) == len(view.signals) == DISPLAY_LIMIT
    assert len(view.patterns[0].evidence) == EVIDENCE_LIMIT
    assert len(view.timeline) <= TIMELINE_LIMIT
    assert dict(view.counts)['pattern'] == str(DISPLAY_LIMIT + 5)
    for secret in (SECRET, RAW, CURSOR, 'synthetic-leak', 'synthetic-token', 'raw-body', 'Authorization'):
        assert secret not in str(view)


def test_html_primitives_escape_all_log_derived_text(presenter):
    import theme
    text = '<img src=x onerror=alert(1)>'
    assert text not in theme.pattern(text)
    assert '&lt;img' in theme.pattern(text)
    assert text not in theme.empty(text, text)
    assert text not in theme.header(text, text)


def test_expert_commentary_is_allowlisted_bounded_and_redacted(presenter):
    source = result_fixture()
    source['case_analysis'] = {'durum_ozeti': f'Değerlendirme {SECRET}', 'guven': .9,
                               'onerilen_incelemeler': ['Kanıtı inceleyin'] * 30,
                               'unexpected': RAW, 'Authorization': 'Bearer token-value'}
    view = build(presenter, source)
    assert len(view.expert_commentary) == 21
    assert dict(view.expert_commentary)['Uzman özeti'] == 'Değerlendirme [redacted]'
    assert not any(text in str(view.expert_commentary) for text in (SECRET, RAW, 'token-value', '0.9'))
