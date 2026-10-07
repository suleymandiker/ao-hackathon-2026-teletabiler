"""Monitoring control plane: persisted state only; never runs a worker/query."""
from dataclasses import asdict
from datetime import datetime, timedelta, timezone

import streamlit as st
from monitoring.domain import MonitorDefinition
from monitoring.boundary_quality import BOUNDARY_DIAGNOSTIC_LIMIT
from monitoring.repository import SQLiteMonitorRepository
from monitoring.runtime import database_path
from presentation import InvestigationPresenter
import investigation_views as views
from trace_views import load_trace


def repository():
    return SQLiteMonitorRepository(database_path())


def now():
    return datetime.now(timezone.utc)


def clear_monitor_selection():
    st.session_state['expanded_monitor_id'] = None
    st.session_state['selected_run_id'] = None
    st.session_state['monitor_edit_mode'] = None
    st.session_state['delete_confirmation_monitor_id'] = None


def toggle_details(monitor_id):
    selected = st.session_state['expanded_monitor_id']
    clear_monitor_selection()
    if selected != monitor_id:
        st.session_state['expanded_monitor_id'] = monitor_id


def reset_form(key):
    # Called before rendering widgets, so reopening uses current persisted
    # values rather than an abandoned edit or the previous creation's inputs.
    prefix = 'monitor_field_' + key + '_'
    for widget in list(st.session_state):
        if widget.startswith(prefix) or widget == 'monitor_name_' + key:
            del st.session_state[widget]


def toggle_create():
    reset_form('new')
    st.session_state['create_form_open'] = not st.session_state['create_form_open']


def open_edit(monitor_id):
    reset_form(monitor_id)
    st.session_state['monitor_edit_mode'] = monitor_id
    st.session_state['delete_confirmation_monitor_id'] = None


def boundary_details(summary, logical_events, redact):
    quality = summary.get('boundary_quality')
    cuts = summary.get('window_assembly', {})
    if quality is None:
        if 'boundary_tail_events' not in cuts:
            st.caption('Boundary completeness was not recorded for this historical run.')
            return
        # Historical monitoring retained the exact owned analysis-end count,
        # but discarded per-event provenance. Do not invent event-level rows.
        possible = cuts['boundary_tail_events']
        total, confirmed, analysis_end = logical_events, 0, possible
    else:
        total = quality['total_events']
        possible, confirmed = (quality['counts'][key] for key in ('possible_incomplete', 'confirmed_truncated'))
        analysis_end = quality['analysis_end_events']
    message = (f'Boundary completeness: {possible} of {total} logical events are marked possible_incomplete '
               f'({analysis_end} emitted at analysis end). Confirmed truncation: {confirmed} recorded.')
    if confirmed:
        st.error(message)
    elif possible:
        st.warning(message)
    else:
        st.caption(message)
    if quality is None:
        st.caption('Historical run: counts use recorded analysis-end tails. Event-level boundary details were not stored; history is unchanged.')
    elif quality['affected_events']:
        with st.expander('Boundary-affected events (metadata only)'):
            columns = ('event_ordinal', 'event_id', 'boundary_status', 'emission_reason', 'stream_id',
                       'pod_instance', 'container_instance', 'channel', 'source_start_time', 'last_source_time',
                       'physical_record_count', 'first_record_reference', 'last_record_reference')
            rows = [{key: redact(str(row[key])) if isinstance(row.get(key), str) else row.get(key)
                     for key in columns} for row in quality['affected_events'][:BOUNDARY_DIAGNOSTIC_LIMIT]]
            st.caption(f'Showing {len(rows)} of {possible + confirmed} affected events. '
                       'Pod/container instances identify the stream; source references are hashes. Raw log bodies are omitted.')
            st.dataframe(rows, hide_index=True, width='stretch')
    if cuts.get('orphan_events', 0) or cuts.get('overlap_conflicts', 0):
        st.info(f"Window assembly: {cuts.get('orphan_events', 0)} orphan events excluded; "
                f"{cuts.get('overlap_conflicts', 0)} overlap conflicts. These counters do not establish confirmed truncation.")


def monitor_form(repo, connection, redact, monitor=None):
    old = monitor.definition if monitor else None
    key = monitor.id if monitor else 'new'
    def field(name):
        return 'monitor_field_' + key + '_' + name

    with st.form('monitor_form_' + key):
        name = st.text_input('Monitor name', value=old.name if old else '', key='monitor_name_' + key)
        profile = st.text_input('Source profile / scope', value=old.source_profile if old else (connection.source_scope if connection else ''),
                                key=field('profile'), help='Must match OPENSEARCH_SOURCE_SCOPE. Connection and index settings stay in application configuration.')
        cluster_alias = st.text_input('Logical cluster alias', value=old.cluster_alias if old else (connection.source_scope if connection else ''),
                                      key=field('alias'), help='Logical source name, retained for existing monitors. This does not filter document cluster UUIDs.')
        document_cluster_id = st.text_input('Document OpenShift cluster UUID (optional)',
                                            value=(old.document_cluster_id or '') if old else '',
                                            key=field('document_cluster_id'), help='Only enter a verified openshift.cluster_id from documents. Leave blank to omit this filter; never infer it from the alias or index.')
        namespace = st.text_input('Namespace', value=old.namespace if old else '', key=field('namespace'))
        workload = st.text_input('Deployment / workload', value=old.workload if old else '',
                                 key=field('workload'), help='Exact configured workload label; do not use an ephemeral pod name.')
        container = st.text_input('Container (optional)', value=(old.container or '') if old else '', key=field('container'))
        left, right = st.columns(2)
        with left:
            interval = st.number_input('Interval (seconds)', min_value=1, max_value=86400, value=old.interval_seconds if old else 900, key=field('interval'))
            window = st.number_input('Analysis window (seconds)', min_value=1, max_value=86400, value=old.window_seconds if old else 900, key=field('window'))
            delay = st.number_input('Ingestion delay (seconds)', min_value=0, max_value=86400, value=old.ingestion_delay_seconds if old else 60, key=field('delay'))
        with right:
            overlap = st.number_input('Boundary overlap (seconds)', min_value=0, max_value=3600, value=old.overlap_seconds if old else 60, key=field('overlap'))
            zone = st.text_input('Source timezone (IANA; blank = Unknown)', value=(old.source_timezone or '') if old else '',
                                 key=field('zone'), help='For a validated UTC source enter UTC explicitly. OpenSearch record time is not shifted.')
            initial = old.initial_start if old else now().replace(second=0, microsecond=0) - timedelta(minutes=17)
            start = st.text_input('Initial window start (aware ISO timestamp)', value=initial.isoformat(), key=field('start'))
        with st.expander('Acquisition limits'):
            page_size = st.number_input('Page size', min_value=1, max_value=500, value=old.page_size if old else 100, key=field('page_size'))
            max_pages = st.number_input('Maximum pages', min_value=1, max_value=20, value=old.max_pages if old else 20, key=field('max_pages'))
        enabled = st.checkbox('Enabled', value=monitor.enabled if monitor else False, key=field('enabled'))
        if monitor:
            st.caption('After the first run, scope, window, timezone and acquisition settings are immutable. Create a new monitor to change them.')
        submitted = st.form_submit_button('Save monitor', key='save_monitor_' + key)
        cancelled = st.form_submit_button('Cancel', key='cancel_monitor_' + key)
    if cancelled:
        if monitor:
            st.session_state['monitor_edit_mode'] = None
        else:
            st.session_state['create_form_open'] = False
        st.rerun()
    if submitted:
        try:
            definition = MonitorDefinition(name, profile, cluster_alias, namespace, workload,
                                           datetime.fromisoformat(start.replace('Z', '+00:00')), container or None,
                                           interval, window, delay, overlap, zone or None, page_size, max_pages,
                                           document_cluster_id=document_cluster_id or None)
            if monitor:
                repo.update(monitor.id, definition, revision=monitor.revision, now=now())
                if enabled != monitor.enabled:
                    repo.set_enabled(monitor.id, enabled, now=now())
                st.session_state['monitor_edit_mode'] = None
            else:
                created = repo.create(definition, enabled=enabled, now=now())
                clear_monitor_selection()
                st.session_state['expanded_monitor_id'] = created.id
                st.session_state['create_form_open'] = False
            st.success('Monitor saved. Execution belongs to the separate worker.')
            st.rerun()
        except (ValueError, TypeError):
            st.error('Check required scope, aware start time, IANA timezone and bounds. Refresh if edited elsewhere; existing run scope cannot change.')
        except Exception:
            st.error('Monitoring storage is unavailable. Check database permissions and configuration.')


def render(connection, redact):
    st.title('Deployment Monitors')
    st.caption('Define monitoring here. Start the independent worker to execute bounded windows; Refresh reads persisted status.')
    for key, default in dict(expanded_monitor_id=None, selected_run_id=None, monitor_edit_mode=None,
                             create_form_open=False, show_archived=False,
                             delete_confirmation_monitor_id=None).items():
        st.session_state.setdefault(key, default)
    if connection is None:
        st.warning('Source unavailable: OpenSearch configuration is missing or invalid. Persisted monitors and history remain available. Configure OPENSEARCH_* in the application environment or repository .env, then restart the UI and worker.')
    else:
        st.info('Source configuration loaded. Connectivity is checked by the worker during acquisition.')
    st.button('Refresh status', key='refresh_monitors')
    st.checkbox('Show archived monitors', key='show_archived')
    try:
        repo = repository()
        monitors = repo.list(include_archived=st.session_state['show_archived'])
    except Exception:
        st.error('Monitoring storage is unavailable. Check AIOPS_MONITOR_DB and directory permissions.')
        return
    if st.session_state['expanded_monitor_id'] not in {monitor.id for monitor in monitors}:
        clear_monitor_selection()
    st.button(('▾' if st.session_state['create_form_open'] else '▸') + ' New Monitor',
              key='new_monitor', on_click=toggle_create)
    if st.session_state['create_form_open']:
        with st.container(border=True):
            monitor_form(repo, connection, redact)
    if not monitors:
        st.info('No monitors to show. Create a monitor with New Monitor, or show archived monitors.')
        return
    for monitor in monitors:
        latest = repo.history(monitor.id, limit=1)
        run = latest[0] if latest else None
        expanded = st.session_state['expanded_monitor_id'] == monitor.id
        with st.container(border=True):
            st.button(('▾ ' if expanded else '▸ ') + redact(monitor.definition.name),
                      key='monitor_row_' + monitor.id, on_click=toggle_details, args=(monitor.id,), width='stretch')
            last_run = f'{run.status.value} · {run.actual_started_at.isoformat()}' if run else 'None'
            st.caption(f'{monitor.status.value} · {redact(monitor.definition.namespace)} / '
                       f'{redact(monitor.definition.workload)} · Last run: {last_run} · '
                       f'Every {monitor.definition.interval_seconds}s')
            if expanded:
                monitor_details(repo, connection, redact, monitor, run)


def monitor_details(repo, connection, redact, monitor, latest):
    st.subheader('Status')
    st.caption('Monitor ID: ' + monitor.id)
    st.text(f'{monitor.status.value} | enabled={monitor.enabled}')
    st.text('Last run: ' + (f'{latest.status.value} · {latest.actual_started_at.isoformat()}' if latest else 'None'))
    st.text('Last successful end: ' + (monitor.last_successful_end.isoformat() if monitor.last_successful_end else 'None'))
    st.text('Next run: ' + (monitor.next_run_at.isoformat() if monitor.enabled and not monitor.archived else 'Not scheduled'))
    st.text(f'Interval / window: {monitor.definition.interval_seconds}s / {monitor.definition.window_seconds}s')
    st.text('Source timezone: ' + (monitor.definition.source_timezone or 'Unknown'))
    if monitor.archived:
        st.info('ARCHIVED — removed from active monitoring. Historical runs and evidence are preserved.')
    elif not monitor.enabled:
        st.info('Paused monitor. Historical runs and evidence remain available; no new windows are scheduled.')
    if monitor.last_error_summary:
        st.error(redact((monitor.last_error_category or 'UNKNOWN') + ': ' + monitor.last_error_summary))
    st.subheader('Source Configuration')
    definition = monitor.definition
    st.text('source_scope: ' + redact(definition.source_profile))
    st.caption('Index settings come from current application configuration; each investigation retains its run-time source settings.')
    st.text('Index pattern: ' + (redact(connection.index_expression) if connection else 'Unavailable (source configuration missing)'))
    st.text('Index strategy: ' + (connection.index_strategy if connection else 'Unavailable (source configuration missing)'))
    st.text('Logical cluster alias: ' + redact(definition.cluster_alias))
    st.text('Namespace: ' + redact(definition.namespace))
    st.text('Deployment / workload: ' + redact(definition.workload))
    st.text('Container: ' + redact(definition.container or 'All'))
    if definition.document_cluster_id:
        st.text('document_cluster_id: ' + redact(definition.document_cluster_id))
    st.subheader('Actions')
    if not monitor.archived:
        left, middle, right = st.columns(3)
        with left:
            if st.button('Pause monitor' if monitor.enabled else 'Enable monitor', key='toggle_monitor_' + monitor.id):
                try:
                    repo.set_enabled(monitor.id, not monitor.enabled, now=now())
                    st.rerun()
                except (ValueError, KeyError):
                    st.error('Monitor changed; refresh before changing its status.')
        with middle:
            st.button('Edit monitor', key='edit_monitor_' + monitor.id, on_click=open_edit, args=(monitor.id,))
        with right:
            if st.button('Delete monitor', key='delete_monitor_' + monitor.id):
                st.session_state['delete_confirmation_monitor_id'] = monitor.id
                st.session_state['monitor_edit_mode'] = None
        if st.session_state['delete_confirmation_monitor_id'] == monitor.id:
            st.warning('This removes the monitor from active monitoring. Historical runs and evidence will be preserved.')
            cancel, confirm = st.columns(2)
            with cancel:
                if st.button('Cancel', key='cancel_delete_' + monitor.id):
                    st.session_state['delete_confirmation_monitor_id'] = None
                    st.rerun()
            with confirm:
                if st.button('Confirm delete', key='confirm_delete_' + monitor.id):
                    repo.archive(monitor.id, now=now())
                    clear_monitor_selection()
                    st.rerun()
        if st.session_state['monitor_edit_mode'] == monitor.id:
            monitor_form(repo, connection, redact, monitor)
    else:
        st.caption('Archived monitors are read-only.')
    st.subheader('Run History')
    runs = repo.history(monitor.id)
    if not runs:
        st.info('No runs have been recorded for this monitor.')
        return
    st.caption('Most recent 100 windows. Retries reuse the same run; attempts are counted.')
    st.dataframe([dict(Window=f'[{r.window.start.isoformat()}, {r.window.end.isoformat()})',
                       Started=r.actual_started_at.isoformat(),
                       Duration=(r.actual_finished_at - r.actual_started_at).total_seconds() if r.actual_finished_at else None,
                       Status=r.status.value, Attempts=r.attempts, **asdict(r.counts), Error=r.error_category or '') for r in runs],
                 hide_index=True, width='stretch')
    st.subheader('Investigation')
    completed = [r for r in runs if r.result_reference]
    if completed:
        options = [r.id for r in completed]
        if st.session_state['selected_run_id'] not in options:
            st.session_state['selected_run_id'] = options[0]
        run_id = st.selectbox('Open completed run', options,
                              format_func=lambda key: next(r.window.start.isoformat() for r in completed if r.id == key), key='selected_run_id')
        result = repo.result(run_id)
        if result is not None:
            summary = result.get('source_summary', {})
            with st.expander('Window, source policy and boundary diagnostics'):
                # The full per-event status contract is persisted, but this
                # general view must not bypass the affected-event display cap.
                st.json({key: value for key, value in summary.items() if key != 'boundary_quality'})
            run = next(item for item in completed if item.id == run_id)
            boundary_details(summary, run.counts.logical_events, redact)
            model = InvestigationPresenter(redact).build(result, source='Deployment Monitor',
                                                         target=redact(monitor.definition.namespace + ' / ' + monitor.definition.workload))
            st.subheader('Pipeline Trace / Evidence Explorer')
            views.investigation(model, trace=load_trace(result, run_id, redact), trace_key='trace_' + run_id)
        else:
            st.info('The persisted result for this run is unavailable.')
    else:
        st.info('No successful runs with a persisted investigation yet.')
