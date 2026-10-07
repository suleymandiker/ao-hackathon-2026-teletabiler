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


def filter_monitors(monitors, query='', status='All'):
    """Filter already loaded definitions only; enabled ERROR/RUNNING are active."""
    words = query.casefold().split()
    return [m for m in monitors
            if all(word in ' '.join((m.definition.name, m.definition.namespace,
                                      m.definition.workload)).casefold() for word in words)
            and (status == 'All' or status == 'Archived' and m.archived
                 or status == 'Active' and m.enabled and not m.archived
                 or status == 'Paused' and not m.enabled and not m.archived)]


def render(connection, redact):
    import monitoring_style as style
    st.markdown(style.CSS, unsafe_allow_html=True)
    for key, default in dict(expanded_monitor_id=None, selected_run_id=None, monitor_edit_mode=None,
                             create_form_open=False, show_archived=False,
                             delete_confirmation_monitor_id=None).items():
        st.session_state.setdefault(key, default)
    # Preserve the ID even when a search temporarily hides its widget.
    st.session_state['selected_run_id'] = st.session_state['selected_run_id']
    title, refresh, create = st.columns([5, 1.3, 1.8], vertical_alignment='center')
    with title:
        st.title('Deployment Monitors')
    with refresh:
        st.button('Refresh', key='refresh_monitors', width='stretch')
    with create:
        st.button('+ New Monitor', key='new_monitor', type='primary', on_click=toggle_create, width='stretch')
    search, status, archived = st.columns([3, 1.2, 1.6], vertical_alignment='bottom')
    with search:
        query = st.text_input('Search monitors', key='monitor_search', placeholder='Search name, namespace or workload…')
    with status:
        selected_status = st.selectbox('Status', ['All', 'Active', 'Paused', 'Archived'], key='monitor_status_filter')
    with archived:
        st.checkbox('Show archived', key='show_archived',
                    help='Archived status also includes archived monitors automatically.')
    if connection is None:
        st.warning('Source unavailable: OpenSearch configuration is missing or invalid. Persisted monitors and history remain available. Configure OPENSEARCH_* in the application environment or repository .env, then restart the UI and worker.')
    try:
        repo = repository()
        monitors = repo.list(include_archived=st.session_state['show_archived'] or selected_status == 'Archived')
    except Exception:
        st.error('Monitoring storage is unavailable. Check AIOPS_MONITOR_DB and directory permissions.')
        return
    if st.session_state['expanded_monitor_id'] not in {monitor.id for monitor in monitors}:
        clear_monitor_selection()
    if st.session_state['create_form_open']:
        with st.container(border=True):
            st.subheader('New monitor')
            st.caption('Choose a workload and schedule. Execution is handled by the independent worker.')
            monitor_form(repo, connection, redact)
    if not monitors:
        st.info('No monitors to show. Create a monitor with New Monitor, or show archived monitors.')
        return
    visible = filter_monitors(monitors, query, selected_status)
    st.caption(f'{len(visible)} of {len(monitors)} monitors · Persisted status · Schedule times in UTC')
    if not visible:
        st.info('No monitors match these filters. Clear search or change Status.')
    for monitor in visible:
        latest = repo.history(monitor.id, limit=1)
        run = latest[0] if latest else None
        expanded = st.session_state['expanded_monitor_id'] == monitor.id
        with st.container(border=True, key='monitor_card_' + monitor.id):
            with st.container(key='monitor_header_' + monitor.id):
                st.markdown(style.monitor_row(monitor, run, expanded, redact), unsafe_allow_html=True)
                st.button(('Collapse ' if expanded else 'Expand ') + redact(monitor.definition.name),
                          key='monitor_row_' + monitor.id, on_click=toggle_details,
                          args=(monitor.id,), width='stretch')
            if expanded:
                monitor_details(repo, connection, redact, monitor, run)


def monitor_actions(repo, redact, monitor):
    if monitor.archived:
        st.caption('Archived monitors are read-only.')
        return
    left, right = st.columns([1, 3])
    with left:
        if st.button('Pause monitor' if monitor.enabled else 'Enable monitor', key='toggle_monitor_' + monitor.id):
            try:
                repo.set_enabled(monitor.id, not monitor.enabled, now=now())
                st.rerun()
            except (ValueError, KeyError):
                st.error('Monitor changed; refresh before changing its status.')
    with right:
        st.button('Edit monitor', key='edit_monitor_' + monitor.id, on_click=open_edit, args=(monitor.id,))
    with st.expander('Administration · Archive monitor'):
        st.caption('Monitoring stops. Historical runs and evidence remain preserved.')
        if st.button('Archive monitor', key='delete_monitor_' + monitor.id):
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
                if st.button('Confirm archive', key='confirm_delete_' + monitor.id):
                    repo.archive(monitor.id, now=now())
                    clear_monitor_selection()
                    st.rerun()


def configuration(connection, redact, monitor):
    import monitoring_style as style
    d = monitor.definition
    left, right = st.columns(2)
    with left:
        st.markdown(style.fields([
            ('Source scope', d.source_profile), ('Namespace', d.namespace), ('Workload', d.workload),
            ('Container', d.container or 'All'), ('Logical cluster alias', d.cluster_alias),
            ('Document cluster UUID', d.document_cluster_id or 'No filter'),
        ], redact), unsafe_allow_html=True)
    with right:
        st.markdown(style.fields([
            ('Window', f'{d.window_seconds} s'), ('Interval', f'{d.interval_seconds} s'),
            ('Ingestion delay', f'{d.ingestion_delay_seconds} s'), ('Overlap', f'{d.overlap_seconds} s'),
            ('Source timezone', d.source_timezone or 'Unknown'),
            ('Initial start', d.initial_start.isoformat()), ('Page size / maximum pages', f'{d.page_size} / {d.max_pages}'),
        ], redact), unsafe_allow_html=True)
    with st.expander('Source and monitor details'):
        st.text('Monitor ID: ' + monitor.id)
        st.text('Index pattern: ' + (redact(connection.index_expression) if connection else 'Unavailable'))
        st.text('Index strategy: ' + (connection.index_strategy if connection else 'Unavailable'))
        st.caption('Index settings come from current application configuration; each investigation retains its run-time source settings.')
    if not monitor.archived:
        st.button('Edit configuration', key='edit_configuration_' + monitor.id,
                  on_click=open_edit, args=(monitor.id,))


def monitor_details(repo, connection, redact, monitor, latest):
    import monitoring_style as style
    from trace_views import event_lineage
    overview, history, investigation, trace_panel, config = st.tabs(
        ['Overview', 'Runs', 'Investigation', 'Pipeline Trace', 'Configuration'],
        key='monitor_tabs_' + monitor.id, on_change='rerun')
    with overview:
        st.subheader('Status')
        st.markdown(style.kpis(monitor, latest), unsafe_allow_html=True)
        d = monitor.definition
        st.markdown(style.fields([
            ('Monitoring state', monitor.status.value), ('Source scope', d.source_profile),
            ('Namespace / workload', d.namespace + ' / ' + d.workload), ('Container', d.container or 'All'),
        ], redact), unsafe_allow_html=True)
        if monitor.archived:
            st.info('ARCHIVED — removed from active monitoring. Historical runs and evidence are preserved.')
        elif not monitor.enabled:
            st.info('Paused monitor. Historical runs and evidence remain available; no new windows are scheduled.')
        if monitor.last_error_summary:
            st.error(redact((monitor.last_error_category or 'UNKNOWN') + ': ' + monitor.last_error_summary))
        monitor_actions(repo, redact, monitor)
    with config:
        configuration(connection, redact, monitor)
    # Render a single shared edit form, including when opened from Overview.
    if not monitor.archived and st.session_state['monitor_edit_mode'] == monitor.id:
        with st.container(border=True):
            st.subheader('Edit monitor')
            monitor_form(repo, connection, redact, monitor)
    runs = repo.history(monitor.id)
    with history:
        st.subheader('Run History')
        if not runs:
            st.info('No runs have been recorded for this monitor.')
        else:
            st.caption('Most recent 100 windows · Newest first · UTC · Duration in seconds')
            st.dataframe([dict(Window=f'{r.window.start.isoformat()} → {r.window.end.isoformat()}',
                               Status=r.status.value,
                               Duration=(r.actual_finished_at - r.actual_started_at).total_seconds() if r.actual_finished_at else None,
                               Records=r.counts.events_retrieved, **{'Logical Events': r.counts.logical_events},
                               Signals=r.counts.qualified_signals, Incidents=r.counts.incidents) for r in runs],
                         hide_index=True, width='stretch')
            with st.expander('Run diagnostics · attempts and errors'):
                st.dataframe([dict(Run=r.id, Started=r.actual_started_at.isoformat(),
                                   Finished=r.actual_finished_at.isoformat() if r.actual_finished_at else 'In progress',
                                   Attempts=r.attempts, Error=redact(r.error_category or ''),
                                   Detail=redact(r.error_summary or ''), **asdict(r.counts)) for r in runs],
                             hide_index=True, width='stretch')
    completed = [r for r in runs if r.result_reference]
    if not completed:
        for panel in (investigation, trace_panel):
            with panel:
                st.info('No successful runs with a persisted investigation yet.')
        return
    with history:
        options = [r.id for r in completed]
        if st.session_state['selected_run_id'] not in options:
            st.session_state['selected_run_id'] = options[0]
        run_id = st.selectbox('Open completed run', options,
                              format_func=lambda key: next(r.window.start.isoformat() + ' · ' + r.status.value + ' · ' + r.id[:8]
                                                           for r in completed if r.id == key), key='selected_run_id')
        st.caption('This selection controls Investigation and Pipeline Trace. Only persisted results are opened.')
        run = next(item for item in completed if item.id == run_id)
        with st.expander('Selected run details'):
            st.text('Run ID: ' + run.id)
            st.text('Started: ' + run.actual_started_at.isoformat())
            st.text('Finished: ' + (run.actual_finished_at.isoformat() if run.actual_finished_at else 'In progress'))
            st.text('Attempts: ' + str(run.attempts))
            st.json(asdict(run.counts))
    result = repo.result(run_id)
    for panel in (investigation, trace_panel):
        with panel:
            st.caption(f'Selected run: {run.id} · {run.status.value} · {run.window.start.isoformat()} → {run.window.end.isoformat()}')
            st.caption('Change the selected window in Runs.')
            if result is None:
                st.info('The persisted result for this run is unavailable.')
    if result is None:
        return
    model = InvestigationPresenter(redact).build(result, source='Deployment Monitor',
                                                 target=redact(monitor.definition.namespace + ' / ' + monitor.definition.workload))
    with investigation:
        summary = result.get('source_summary', {})
        with st.expander('Window, source policy and boundary diagnostics'):
            st.json({key: value for key, value in summary.items() if key != 'boundary_quality'})
        boundary_details(summary, run.counts.logical_events, redact)
        views.investigation(model, include_pipeline=False)
    with trace_panel:
        trace = load_trace(result, run_id, redact)
        st.subheader('Pipeline Trace / Evidence Explorer')
        views.stage_details(model, trace=trace, trace_key='trace_' + run_id)
        event_lineage(trace, key='trace_' + run_id)
