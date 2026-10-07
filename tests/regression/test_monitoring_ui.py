"""Real control-plane rendering with temporary SQLite and blocked external I/O."""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sqlite3

import pytest

from test_streamlit_sources import ui, assert_safe

REAL_CONNECT = sqlite3.connect
BASE = datetime(2026, 10, 5, 12, tzinfo=timezone.utc)


@pytest.fixture
def monitoring_ui(ui, monkeypatch, tmp_path):
    def connect(path, *args, **kwargs):
        assert Path(path).resolve().is_relative_to(tmp_path.resolve())
        return REAL_CONNECT(path, *args, **kwargs)
    monkeypatch.setattr(sqlite3, 'connect', connect)
    monkeypatch.setattr(sqlite3.dbapi2, 'connect', connect)
    monkeypatch.setenv('AIOPS_MONITOR_DB', str(tmp_path / 'monitor.sqlite3'))
    from monitoring.repository import SQLiteMonitorRepository
    from monitoring.domain import MonitorDefinition
    import monitoring_views
    monkeypatch.setattr(monitoring_views, 'now', lambda: BASE)
    repo = SQLiteMonitorRepository(tmp_path / 'monitor.sqlite3')
    monitor = repo.create(MonitorDefinition('saved-monitor', 'profile', 'cluster', 'ns', 'app', BASE), now=BASE)
    return ui, repo, monitor


def test_persisted_monitors_render_without_source_config_and_toggle(monitoring_ui, monkeypatch):
    ui, repo, monitor = monitoring_ui
    monkeypatch.delenv('OPENSEARCH_PASSWORD')
    app = ui.app()
    app.button(key='start_monitors').click().run()
    app.button(key='monitor_row_' + monitor.id).click().run()
    assert not app.exception
    assert app.title[0].value == 'Deployment Monitors'
    assert any('Source unavailable' in item.value for item in app.warning)
    assert any('PAUSED' in item.value for item in app.text)
    assert len(repo.history(monitor.id)) == 0
    app.button(key='toggle_monitor_' + monitor.id).click().run()
    assert repo.get(monitor.id).enabled
    app.button(key='toggle_monitor_' + monitor.id).click().run()
    assert not repo.get(monitor.id).enabled
    assert not ui.sources and not ui.files and not ui.packages
    assert_safe(app)


def test_successful_run_history_uses_investigation_workbench(monitoring_ui):
    ui, repo, monitor = monitoring_ui
    from monitoring.domain import RunCounts
    repo.set_enabled(monitor.id, True, now=BASE)
    run = repo.claim(BASE + timedelta(minutes=17))
    summary = dict(records_read=7, source_scope='gocpbmgpup1', index_expression='gocpbmgpup1*',
                   resolved_index='gocpbmgpup1-2026.10.05', retrieval_start='2026-10-05T11:59:00+00:00',
                   retrieval_end='2026-10-05T12:16:00+00:00', namespace='ns', workload='app', container='main',
                   document_cluster_id=None)
    safe = ui.boundary.presentation_result(ui.payload, summary, ui.boundary.load_connection())
    repo.succeed(run, safe, RunCounts(events_retrieved=7, logical_events=7), {}, now=BASE + timedelta(minutes=17))
    app = ui.app()
    app.button(key='start_monitors').click().run()
    assert not app.exception
    app.button(key='monitor_row_' + monitor.id).click().run()
    assert app.selectbox(key='selected_run_id').value == run.id
    assert any(item.value == 'Investigation sonucu' for item in app.title)
    assert any('Last successful end: 2026-10-05T12:15:00+00:00' == item.value for item in app.text)
    technical = next(item.value for item in app.dataframe
                     if 'Gösterge' in item.value and 'İndeks' in item.value['Gösterge'].values)
    details = dict(zip(technical['Gösterge'], technical['Değer']))
    assert details['İndeks'] == summary['resolved_index']
    assert details['Kaynak indeks deseni'] == summary['index_expression']
    assert details['Kaynak kapsamı'] == 'gocpbmgpup1'
    assert details['Sorgu başlangıcı'] == summary['retrieval_start']
    assert details['Sorgu sonu (hariç)'] == summary['retrieval_end']
    assert details['Document OpenShift cluster UUID'] == 'Absent (no filter)'
    assert_safe(app)


@pytest.mark.parametrize('document_cluster_id', [None, '11111111-2222-4333-8444-555555555555'])
def test_create_disabled_and_edit_monitor_from_form(monitoring_ui, document_cluster_id):
    ui, repo, _ = monitoring_ui
    app = ui.app()
    app.button(key='start_monitors').click().run()
    app.button(key='new_monitor').click().run()
    app.text_input(key='monitor_name_new').set_value('created-in-ui')
    # Only the explicitly opened create form is rendered.
    def first(label):
        return next(item for item in app.text_input if item.label == label)
    assert first('Source profile / scope').value == 'profile'
    assert first('Document OpenShift cluster UUID (optional)').value == ''
    first('Logical cluster alias').set_value('logical-alias')
    if document_cluster_id:
        first('Document OpenShift cluster UUID (optional)').set_value(document_cluster_id)
    first('Namespace').set_value('ns')
    first('Deployment / workload').set_value('app')
    app.button(key='save_monitor_new').click().run()
    assert not app.exception
    monitors = repo.list()
    assert len(monitors) == 2
    created = next(m for m in monitors if m.definition.name == 'created-in-ui')
    assert not created.enabled and created.definition.source_timezone is None
    assert created.definition.cluster_id == created.definition.cluster_alias == 'logical-alias'
    assert created.definition.document_cluster_id == document_cluster_id
    assert app.session_state['expanded_monitor_id'] == created.id
    assert not app.session_state['create_form_open']
    assert not any(item.key == 'monitor_name_new' for item in app.text_input)
    app.button(key='edit_monitor_' + created.id).click().run()
    app.text_input(key='monitor_name_' + created.id).set_value('edited-in-ui')
    app.button(key='save_monitor_' + created.id).click().run()
    assert not app.exception
    assert repo.get(created.id).definition.name == 'edited-in-ui'
    assert repo.get(created.id).definition.document_cluster_id == document_cluster_id
    assert app.session_state['monitor_edit_mode'] is None
    assert not repo.history(created.id) and not ui.sources


def test_manual_upload_navigation_remains_available(monitoring_ui):
    ui, _, _ = monitoring_ui
    app = ui.app()
    assert any('Manual Investigation / Test / Smoke' == item.value for item in app.subheader)
    app.button(key='start_file').click().run()
    assert not app.exception
    assert app.radio(key='analysis_source').value == 'Dosya / paket'
    assert app.selectbox(key='file_source_timezone').value == 'Unknown'


def test_monitor_rows_toggle_one_expanded_id_and_refresh_preserves_it(monitoring_ui):
    ui, repo, first = monitoring_ui
    second = repo.create(first.definition, now=BASE)
    app = ui.app()
    app.button(key='start_monitors').click().run()
    assert not app.exception
    assert len([button for button in app.button if button.key.startswith('monitor_row_')]) == 2
    assert not any(box.label == 'Inspect monitor' for box in app.selectbox)
    assert app.session_state['expanded_monitor_id'] is None
    assert not app.dataframe
    app.button(key='monitor_row_' + first.id).click().run()
    assert app.session_state['expanded_monitor_id'] == first.id
    app.button(key='refresh_monitors').click().run()
    assert app.session_state['expanded_monitor_id'] == first.id
    app.button(key='monitor_row_' + second.id).click().run()
    assert app.session_state['expanded_monitor_id'] == second.id
    assert len([item for item in app.subheader if item.value == 'Status']) == 1
    app.button(key='monitor_row_' + second.id).click().run()
    assert app.session_state['expanded_monitor_id'] is None
    assert not any(item.value == 'Run History' for item in app.subheader)
    assert not ui.sources and not ui.files and not ui.packages
    assert_safe(app)


def test_create_form_toggle_cancel_and_reopen_reset_without_mutation(monitoring_ui):
    ui, repo, monitor = monitoring_ui
    app = ui.app()
    app.button(key='start_monitors').click().run()
    assert not app.session_state['create_form_open'] and not app.text_input
    app.button(key='new_monitor').click().run()
    assert app.session_state['create_form_open']
    app.text_input(key='monitor_name_new').set_value('abandoned')
    app.button(key='cancel_monitor_new').click().run()
    assert not app.session_state['create_form_open']
    assert repo.list() == [monitor]
    app.button(key='new_monitor').click().run()
    assert app.text_input(key='monitor_name_new').value == ''
    app.button(key='new_monitor').click().run()
    assert not app.session_state['create_form_open'] and not app.text_input
    assert repo.list() == [monitor]
    assert_safe(app)


def test_creation_resets_fields_and_refreshes_clickable_list(monitoring_ui):
    ui, repo, _ = monitoring_ui
    app = ui.app()
    app.button(key='start_monitors').click().run()
    app.button(key='new_monitor').click().run()
    app.text_input(key='monitor_name_new').set_value('new monitor')
    app.text_input(key='monitor_field_new_namespace').set_value('other-namespace')
    app.text_input(key='monitor_field_new_workload').set_value('other-workload')
    app.button(key='save_monitor_new').click().run()
    assert not app.exception
    created = next(item for item in repo.list() if item.definition.name == 'new monitor')
    assert app.session_state['expanded_monitor_id'] == created.id
    assert not app.session_state['create_form_open']
    assert app.button(key='monitor_row_' + created.id)
    app.button(key='new_monitor').click().run()
    assert app.text_input(key='monitor_name_new').value == ''
    assert app.text_input(key='monitor_field_new_namespace').value == ''
    assert app.text_input(key='monitor_field_new_workload').value == ''
    assert not ui.sources and not ui.files and not ui.packages
    assert_safe(app)


@pytest.mark.parametrize('action', ['edit', 'cancel_edit', 'pause', 'enable'])
def test_duplicate_name_actions_use_expanded_monitor_id(monitoring_ui, action):
    ui, repo, first = monitoring_ui
    second = repo.create(first.definition, enabled=action == 'pause', now=BASE)
    app = ui.app()
    app.button(key='start_monitors').click().run()
    app.button(key='monitor_row_' + second.id).click().run()
    assert app.session_state['expanded_monitor_id'] == second.id
    if action in ('edit', 'cancel_edit'):
        app.button(key='edit_monitor_' + second.id).click().run()
        assert app.session_state['monitor_edit_mode'] == second.id
        app.text_input(key='monitor_name_' + second.id).set_value('changed second')
        app.text_input(key='monitor_field_' + second.id + '_namespace').set_value('changed-ns')
        if action == 'cancel_edit':
            app.button(key='cancel_monitor_' + second.id).click().run()
            assert repo.get(second.id) == second
            app.button(key='edit_monitor_' + second.id).click().run()
            assert app.text_input(key='monitor_name_' + second.id).value == second.definition.name
            assert app.text_input(key='monitor_field_' + second.id + '_namespace').value == 'ns'
        else:
            app.button(key='save_monitor_' + second.id).click().run()
            assert repo.get(second.id).definition.name == 'changed second'
            assert repo.get(second.id).definition.namespace == 'changed-ns'
            assert app.session_state['monitor_edit_mode'] is None
    else:
        app.button(key='toggle_monitor_' + second.id).click().run()
        assert repo.get(second.id).enabled == (action == 'enable')
    assert repo.get(first.id) == first
    assert app.session_state['expanded_monitor_id'] == second.id
    assert not ui.sources and not ui.files and not ui.packages
    assert_safe(app)


def test_archive_confirmation_cancel_and_show_archived_keep_duplicate_independent(monitoring_ui):
    ui, repo, monitor = monitoring_ui
    duplicate = repo.create(monitor.definition, now=BASE)
    app = ui.app()
    app.button(key='start_monitors').click().run()
    app.button(key='monitor_row_' + monitor.id).click().run()
    assert not any(button.label == 'Confirm delete' for button in app.button)
    app.button(key='delete_monitor_' + monitor.id).click().run()
    assert repo.get(monitor.id) == monitor
    assert app.session_state['delete_confirmation_monitor_id'] == monitor.id
    assert any(item.value == 'This removes the monitor from active monitoring. Historical runs and evidence will be preserved.'
               for item in app.warning)
    app.button(key='cancel_delete_' + monitor.id).click().run()
    assert repo.get(monitor.id) == monitor
    assert app.session_state['delete_confirmation_monitor_id'] is None
    app.button(key='delete_monitor_' + monitor.id).click().run()
    app.button(key='confirm_delete_' + monitor.id).click().run()
    assert repo.get(monitor.id).archived and not repo.get(monitor.id).enabled
    assert app.session_state['expanded_monitor_id'] is None
    assert app.session_state['selected_run_id'] is None
    assert app.session_state['delete_confirmation_monitor_id'] is None
    assert not any(button.key == 'monitor_row_' + monitor.id for button in app.button)
    assert repo.get(duplicate.id) == duplicate
    app.checkbox(key='show_archived').check().run()
    app.button(key='monitor_row_' + monitor.id).click().run()
    assert any('ARCHIVED' in item.value for item in app.text)
    assert not any(button.key in {'toggle_monitor_' + monitor.id, 'edit_monitor_' + monitor.id,
                                 'delete_monitor_' + monitor.id} for button in app.button)
    app.checkbox(key='show_archived').uncheck().run()
    assert app.session_state['expanded_monitor_id'] is None
    assert not ui.sources and not ui.files and not ui.packages
    assert_safe(app)


def test_changing_monitors_clears_edit_delete_and_run_context(monitoring_ui):
    ui, repo, first = monitoring_ui
    second = repo.create(first.definition, now=BASE)
    app = ui.app()
    app.button(key='start_monitors').click().run()
    app.button(key='monitor_row_' + first.id).click().run()
    app.button(key='edit_monitor_' + first.id).click().run()
    app.button(key='monitor_row_' + second.id).click().run()
    assert app.session_state['monitor_edit_mode'] is None
    assert not app.text_input
    app.button(key='delete_monitor_' + second.id).click().run()
    app.button(key='monitor_row_' + first.id).click().run()
    assert app.session_state['delete_confirmation_monitor_id'] is None
    assert not any(button.label == 'Confirm delete' for button in app.button)
    assert repo.list(include_archived=True) == sorted([first, second], key=lambda item: (item.created_at, item.id))


def test_empty_list_and_archived_only_list_are_clear(monitoring_ui):
    ui, repo, monitor = monitoring_ui
    repo.archive(monitor.id, now=BASE)
    app = ui.app()
    app.button(key='start_monitors').click().run()
    assert any('No monitors to show' in item.value for item in app.info)
    app.button(key='new_monitor').click().run()
    assert app.session_state['create_form_open'] and app.text_input(key='monitor_name_new')
    app.button(key='cancel_monitor_new').click().run()
    app.checkbox(key='show_archived').check().run()
    app.button(key='monitor_row_' + monitor.id).click().run()
    assert any('No runs have been recorded' in item.value for item in app.info)
    assert_safe(app)


def test_failed_run_and_no_successful_investigation_empty_state(monitoring_ui):
    ui, repo, monitor = monitoring_ui
    repo.set_enabled(monitor.id, True, now=BASE)
    run = repo.claim(BASE + timedelta(minutes=17))
    repo.fail(run, 'OPENSEARCH_TIMEOUT', now=BASE)
    app = ui.app()
    app.button(key='start_monitors').click().run()
    app.button(key='monitor_row_' + monitor.id).click().run()
    assert any('Last run: FAILED' in item.value for item in app.text)
    assert any('OPENSEARCH_TIMEOUT' in item.value for item in app.error)
    assert any('No successful runs' in item.value for item in app.info)
    assert not any(item.key == 'selected_run_id' for item in app.selectbox)
    assert repo.result(run.id) is None
    assert_safe(app)


def test_status_refresh_never_constructs_pipeline_or_queries_source(monitoring_ui, monkeypatch):
    ui, repo, monitor = monitoring_ui
    import full_pipeline_v2
    import monitoring.execution
    def forbidden(*args, **kwargs):
        pytest.fail('Monitor management must only read persisted evidence')
    monkeypatch.setattr(full_pipeline_v2, 'FullAIOpsPipelineV2', forbidden)
    monkeypatch.setattr(ui.boundary, 'run_analysis', forbidden)
    monkeypatch.setattr(monitoring.execution.OpenSearchMonitorExecutor, 'execute', forbidden)
    app = ui.app()
    app.button(key='start_monitors').click().run()
    app.button(key='monitor_row_' + monitor.id).click().run()
    app.button(key='refresh_monitors').click().run()
    assert app.session_state['expanded_monitor_id'] == monitor.id
    assert repo.get(monitor.id) == monitor
    assert not ui.sources and not ui.files and not ui.packages
    assert_safe(app)


@pytest.mark.parametrize('possible,confirmed', [(2, 0), (0, 0), (1, 2)])
def test_boundary_warning_quantifies_possible_incomplete_without_claiming_truncation(monitoring_ui, possible, confirmed):
    ui, repo, monitor = monitoring_ui
    from monitoring.domain import RunCounts
    repo.set_enabled(monitor.id, True, now=BASE)
    run = repo.claim(BASE + timedelta(minutes=17))
    # Confirmed rows exercise future display support; the current session does
    # not manufacture this status from any of its lifecycle emission reasons.
    summary = dict(window_assembly=dict(boundary_tail_events=possible), boundary_quality=dict(
        total_events=198, counts=dict(complete=198 - possible - confirmed, possible_incomplete=possible, confirmed_truncated=confirmed),
        analysis_end_events=possible, event_statuses=[], affected_events=[], sample_limit=200))
    safe = ui.boundary.presentation_result(ui.payload, summary, ui.boundary.load_connection())
    repo.succeed(run, safe, RunCounts(logical_events=198), {}, now=BASE + timedelta(minutes=17))
    app = ui.app()
    app.button(key='start_monitors').click().run()
    assert not app.exception
    app.button(key='monitor_row_' + monitor.id).click().run()
    elements = app.error if confirmed else app.warning if possible else app.caption
    text = next(item.value for item in elements if 'Boundary completeness' in item.value)
    assert f'{possible} of 198' in text and 'possible_incomplete' in text
    assert f'Confirmed truncation: {confirmed}' in text
    assert 'long events may be incomplete' not in text
    if not confirmed:
        assert not app.error
    if not possible and not confirmed:
        assert not app.warning


@pytest.mark.parametrize('tails', [0, 2])
def test_reopening_historical_boundary_counts_does_not_rewrite_run_or_invent_details(monitoring_ui, tails):
    ui, repo, monitor = monitoring_ui
    from monitoring.domain import RunCounts
    repo.set_enabled(monitor.id, True, now=BASE)
    run = repo.claim(BASE + timedelta(minutes=17))
    summary = dict(window_assembly=dict(boundary_tail_events=tails, orphan_events=1, overlap_conflicts=1))
    safe = ui.boundary.presentation_result(ui.payload, summary, ui.boundary.load_connection())
    repo.succeed(run, safe, RunCounts(logical_events=198), {}, now=BASE + timedelta(minutes=17))
    original = repo.result(run.id)
    app = ui.app()
    app.button(key='start_monitors').click().run()
    assert not app.exception and not app.error
    app.button(key='monitor_row_' + monitor.id).click().run()
    elements = app.warning if tails else app.caption
    text = next(item.value for item in elements if 'Boundary completeness' in item.value)
    assert f'{tails} of 198' in text and 'Confirmed truncation: 0 recorded' in text
    assert any('Event-level boundary details were not stored' in item.value for item in app.caption)
    assert any('1 orphan events excluded; 1 overlap conflicts' in item.value for item in app.info)
    if not tails:
        assert not app.warning
    assert not any(item.label == 'Boundary-affected events (metadata only)' for item in app.expander)
    assert repo.result(run.id) == original and repo.get(monitor.id).last_successful_end == run.window.end


def test_boundary_event_table_is_bounded_allowlisted_and_redacted(monitoring_ui):
    ui, repo, monitor = monitoring_ui
    from monitoring.boundary_quality import BOUNDARY_DIAGNOSTIC_LIMIT
    from monitoring.domain import RunCounts
    repo.set_enabled(monitor.id, True, now=BASE)
    run = repo.claim(BASE + timedelta(minutes=17))
    count = BOUNDARY_DIAGNOSTIC_LIMIT + 5
    rows = [dict(event_ordinal=i + 1, event_id=f'event-{i}', boundary_status='possible_incomplete',
                 emission_reason='analysis_end', pod_instance='pod-uid', container_instance='container-uid', channel='stdout',
                 raw='private-payload', Authorization='Bearer private-auth', arbitrary_field='private-extra') for i in range(count)]
    rows[0]['pod_instance'] = 'pod password=private-password'
    summary = dict(boundary_quality=dict(total_events=count, counts=dict(complete=0, possible_incomplete=count, confirmed_truncated=0),
                   analysis_end_events=count, event_statuses=rows, affected_events=rows, sample_limit=BOUNDARY_DIAGNOSTIC_LIMIT))
    safe = ui.boundary.presentation_result(ui.payload, summary, ui.boundary.load_connection())
    repo.succeed(run, safe, RunCounts(logical_events=count), {}, now=BASE + timedelta(minutes=17))
    app = ui.app()
    app.button(key='start_monitors').click().run()
    assert not app.exception
    app.button(key='monitor_row_' + monitor.id).click().run()
    table = next(item.value for item in app.dataframe if 'boundary_status' in item.value)
    assert len(table) == BOUNDARY_DIAGNOSTIC_LIMIT
    assert table.iloc[0]['event_id'] == 'event-0' and table.iloc[0]['pod_instance'] == 'pod [redacted]'
    assert table.iloc[-1]['event_ordinal'] == BOUNDARY_DIAGNOSTIC_LIMIT
    assert not {'raw', 'Authorization', 'arbitrary_field'} & set(table.columns)
    # The general JSON expander must not bypass the dedicated table's row cap.
    assert all('event_statuses' not in item.value and 'affected_events' not in item.value for item in app.json)
    assert any(f'Showing {BOUNDARY_DIAGNOSTIC_LIMIT} of {count}' in item.value for item in app.caption)
    for secret in ('private-payload', 'private-auth', 'private-extra', 'private-password'):
        assert secret not in str(table)
    assert_safe(app)
