"""Streamlit opens persisted evidence only, with all external work blocked."""
from datetime import timedelta

import pytest

from test_monitoring_ui import monitoring_ui, ui, BASE
from test_streamlit_sources import assert_safe


@pytest.fixture
def trace_app(monitoring_ui, request):
    ui, repo, monitor = monitoring_ui
    from monitoring.domain import RunCounts
    from monitoring.trace import TraceCollector, TraceLimits
    repo.set_enabled(monitor.id, True, now=BASE)
    run = repo.claim(BASE + timedelta(minutes=17))
    limits = TraceLimits(max_fields_per_item=1) if getattr(request, 'param', None) == 'limited' else None
    collector = TraceCollector(run.id, limits=limits)
    collector.add('acquisition', 'record:1', dict(text='ERROR: user_id=private-api-value', source_timestamp=BASE.isoformat()))
    collector.add('segmentation', 'logical:1', dict(text='ERROR: user_id=private-api-value\n    frame', admitted=True,
                  boundary_status='possible_incomplete', emission_reason='analysis_end', physical_record_count=1), ['record:1'])
    collector.add('parsing', 'canonical:1', dict(canonical=dict(event_id='event-1', message='ERROR: user_id=private-api-value',
                  severity='ERROR', timestamp=BASE.isoformat()), delivery='PARSED'), ['logical:1'])
    collector.add('patterns', 'occurrence:1', dict(template_id='pattern-1', template='normal pattern'), ['canonical:1'])
    collector.members['signal-1'] = ('occurrence:1',)
    signal = dict(signal_id='signal-1', template_id='pattern-1', template='normal pattern', count=1,
                  qualified=False, qualification_score=.05, qualification_reason='actual_backend_reason',
                  qualification_evidence=['actual_backend_evidence'])
    result = dict(stats=dict(segmented=1, parsed=1, templated=1, signal_candidates=1, qualified_signals=0,
                            noise_suppressed=1, correlations=0, incidents=0, rca=0),
                  signals=[signal], correlations=[], incidents=[], rca=[], source_summary={'records_read': 1})
    if getattr(request, 'param', None) == 'expert':
        signal['qualified'] = True
        result['incidents'] = [dict(incident_id='incident-1', signal_ids=['signal-1'], severity_min=3)]
        result['rca'] = [dict(incident_id='incident-1', analysis_source='deterministik',
                             root_cause_candidates=[dict(signal_id='signal-1', score=.8, evidence=['actual_backend_evidence'])])]
        result['case_analysis'] = dict(durum_ozeti='Recorded expert interpretation')
        collector.add('rca', 'expert:input', dict(kind='expert_input', evidence=dict(recorded_signal='signal-1')),
                      ['incident:incident-1'])
        result['stats'].update(qualified_signals=1, noise_suppressed=0, incidents=1, rca=1)
    result['detailed_pipeline_trace'] = collector.finish(result)
    repo.succeed(run, result, RunCounts(logical_events=1), {}, now=BASE + timedelta(minutes=17))
    app = ui.app()
    app.button(key='start_monitors').click().run()
    return app, ui, run, result


@pytest.mark.parametrize('index,stage', list(enumerate(('acquisition', 'segmentation', 'parsing', 'patterns',
                                                       'signal', 'correlation', 'incident', 'rca'))))
def test_each_stage_renders_persisted_details_without_analysis(trace_app, index, stage):
    app, ui, run, result = trace_app
    app.selectbox(key='pipeline_stage').set_value(index).run()
    assert not app.exception
    if stage in ('correlation', 'incident', 'rca'):
        assert any(f'No {stage} objects were produced' in item.value for item in app.info)
    else:
        key = f'trace_{run.id}_{stage}'
        ref = result['detailed_pipeline_trace']['stages'][stage][0]['ref']
        app.selectbox(key=key + '_item').set_value(ref).run()
        assert not app.exception
        assert any('Selected item: ' + ref == item.value for item in app.text)
        if stage == 'signal':
            assert any('actual_backend_reason' in str(item.value) for item in app.dataframe)
            app.selectbox(key=key + '_related').set_value('canonical:1').run()
            assert not app.exception
            assert any(item.value == 'Selected item: canonical:1' for item in app.text)
            app.selectbox(key=key + '_decision').set_value('QUALIFIED').run()
            assert any('No stored items match' in item.value for item in app.info)
    assert not ui.sources and not ui.files and not ui.packages
    assert_safe(app)
    assert 'private-api-value' not in str(app)


def test_event_lineage_shows_suppression_and_boundary(trace_app):
    app, ui, run, _ = trace_app
    app.selectbox(key=f'trace_{run.id}_lineage').set_value('record:1').run()
    assert not app.exception
    assert any('Suppressed at Signal stage.' == item.value for item in app.info)
    frames = ' '.join(item.value.to_json() for item in app.dataframe)
    for value in ('logical:1', 'canonical:1', 'occurrence:1', 'signal:signal-1', 'possible_incomplete'):
        assert value in frames
    app.selectbox(key=f'trace_{run.id}_lineage_detail').set_value('logical:1').run()
    assert not app.exception
    assert 'private-api-value' not in str(app)
    assert not ui.sources and not ui.files and not ui.packages


@pytest.mark.parametrize('trace_app', ['expert'], indirect=True)
def test_opening_nonempty_rca_only_displays_persisted_expert_and_deterministic_evidence(trace_app):
    app, ui, run, _ = trace_app
    app.selectbox(key='pipeline_stage').set_value(7).run()
    key = f'trace_{run.id}_rca_item'
    app.selectbox(key=key).set_value('rca:incident-1').run()
    assert not app.exception
    assert any('Deterministic RCA conclusion' in item.value for item in app.info)
    app.selectbox(key=key).set_value('expert:input').run()
    assert not app.exception
    assert any('Optional expert interpretation / submitted evidence' in item.value for item in app.info)
    app.selectbox(key=key).set_value('expert:result').run()
    assert not app.exception
    assert any('Recorded expert interpretation' in item.value.to_json() for item in app.dataframe)
    assert not ui.sources and not ui.files and not ui.packages


@pytest.mark.parametrize('trace_app', ['limited'], indirect=True)
@pytest.mark.parametrize('stage', [2, 3])
def test_budget_omitted_fields_remain_inspectable_with_coverage_notice(trace_app, stage):
    app, _, _, _ = trace_app
    app.selectbox(key='pipeline_stage').set_value(stage).run()
    assert not app.exception
    assert any('Trace coverage is limited' in item.value for item in app.warning)


def test_historical_run_is_readable_with_explicit_unavailable_message(monitoring_ui):
    ui, repo, monitor = monitoring_ui
    from monitoring.domain import RunCounts
    repo.set_enabled(monitor.id, True, now=BASE)
    run = repo.claim(BASE + timedelta(minutes=17))
    repo.succeed(run, {'stats': {}, 'signals': [], 'incidents': []}, RunCounts(), {}, now=BASE)
    app = ui.app()
    app.button(key='start_monitors').click().run()
    assert not app.exception
    assert any(item.value == 'Detailed pipeline trace was not stored for this historical run.' for item in app.info)
    assert repo.result(run.id) == {'stats': {}, 'signals': [], 'incidents': []}
