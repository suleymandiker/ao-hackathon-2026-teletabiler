"""Monitor control plane; creation queries targets, history reads persisted evidence."""
from dataclasses import asdict, replace
from datetime import datetime, timedelta, timezone

import streamlit as st
from monitoring.domain import MonitorDefinition
from monitoring.boundary_quality import BOUNDARY_DIAGNOSTIC_LIMIT
from monitoring.repository import SQLiteMonitorRepository
from monitoring.runtime import database_path
from monitoring.targets import TargetExplorer, TargetValidationError
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
    st.session_state['investigate_run_id'] = None
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
    st.session_state['monitor_clone_definition'] = None
    st.session_state['create_form_open'] = not st.session_state['create_form_open']


def clone_target(definition):
    reset_form('new')
    st.session_state['monitor_clone_definition'] = definition
    st.session_state['create_form_open'] = True
    st.session_state['monitor_edit_mode'] = None


INTERVALS = {'5 minutes': 300, '15 minutes': 900, '30 minutes': 1800, '1 hour': 3600}


def dependent_selector(label, options, key, preferred=None, redact=str):
    values = list(options.values)
    if options.truncated:
        st.caption(f'{label}: showing the first 200 values from the last 24 hours.')
    if st.session_state.get(key) not in values:
        st.session_state.pop(key, None)
    index = values.index(preferred) if preferred in values else None
    return st.selectbox(label, values, index=index, key=key, format_func=redact,
                        placeholder='Select ' + label.lower(), disabled=not values)


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
    old = monitor.definition if monitor else st.session_state.get('monitor_clone_definition')
    key = monitor.id if monitor else 'new'
    def field(name):
        return 'monitor_field_' + key + '_' + name
    locked = bool(monitor and repo.history(monitor.id, limit=1))
    namespace, workload, container = (old.namespace, old.workload, old.container) if old else (None, None, None)
    valid = False
    if monitor:
        name = st.text_input('Monitor name', value=old.name, key='monitor_name_' + key)
    if locked or (monitor and connection is None):
        st.text_input('Namespace 🔒', value=redact(namespace), disabled=True, key=field('namespace'))
        st.text_input('Workload 🔒', value=redact(workload), disabled=True, key=field('workload'))
        st.caption('Target cannot be changed after monitoring history exists.' if locked else 'Source unavailable; target changes require source validation.')
        valid = True
    else:
        try:
            if connection is None:
                raise TargetValidationError('Source unavailable. Configure the default source before creating a monitor.')
            with TargetExplorer(connection, now()) as explorer:
                namespace = dependent_selector('Namespace', explorer.options('namespace'), field('namespace'), namespace, redact)
                if namespace:
                    workload = dependent_selector('Workload', explorer.options('workload', namespace=namespace), field('workload'), workload, redact)
                else:
                    workload = None
                    st.session_state.pop(field('workload'), None)
                with st.expander('Advanced settings'):
                    container_options = explorer.options('container', namespace=namespace, workload=workload) if workload else None
                    choices = [None] + (list(container_options.values) if container_options else [])
                    if st.session_state.get(field('container')) not in choices:
                        st.session_state.pop(field('container'), None)
                    container = st.selectbox('Container filter', choices,
                                             index=choices.index(container) if container in choices else 0,
                                             format_func=lambda value: redact(value) if value else 'All containers', key=field('container'))
                    if container_options and container_options.truncated:
                        st.caption('Container filter: showing the first 200 values.')
                    start = monitoring_start(field, old if monitor else None)
                if namespace and workload:
                    validation = explorer.validate(namespace, workload, container)
                    valid = True
                    if validation.count:
                        st.success(f'Logs found · {validation.count:,} logs in the last 15 minutes')
                        st.caption('Latest log: ' + age(validation.latest) + ' · ' + validation.latest.strftime('%Y-%m-%d %H:%M:%S UTC'))
                        st.caption('Sample log')
                        st.code(redact(validation.sample), language=None)
                    else:
                        st.info('Workload exists but currently appears quiet: no logs in the last 15 minutes.')
                elif namespace:
                    st.info('Select a workload available in this namespace during the last 24 hours.')
                else:
                    st.info('Select a namespace. Discovery uses the last 24 hours; quiet targets outside that window may not be listed.')
        except TargetValidationError as error:
            st.error(str(error))
    interval_choices = dict(INTERVALS)
    if monitor and old.interval_seconds not in interval_choices.values():
        interval_choices[f'Current schedule ({old.interval_seconds / 60:g} minutes)'] = old.interval_seconds
    selected_interval = old.interval_seconds if old else 900
    labels = list(interval_choices)
    interval = interval_choices[st.selectbox('Run every', labels,
                                index=next((i for i, label in enumerate(labels) if interval_choices[label] == selected_interval), 1),
                                key=field('interval'))]
    if monitor:
        enabled = st.checkbox('Monitoring enabled', value=monitor.enabled, key=field('enabled'))
        if locked:
            st.caption('Run every controls scheduling. The established analysis window and watermark remain unchanged.')
    else:
        enabled = True
    submitted = st.button('Save monitor' if monitor else 'Create monitor', key='save_monitor_' + key,
                          type='primary', disabled=not valid or (monitor is not None and monitor.status.value == 'RUNNING'))
    cancelled = st.button('Cancel', key='cancel_monitor_' + key)
    if cancelled:
        if monitor:
            st.session_state['monitor_edit_mode'] = None
        else:
            st.session_state['create_form_open'] = False
        st.rerun()
    if submitted:
        try:
            if not locked and connection is not None and start > now():
                raise ValueError('Historical start must not be in the future')
            if monitor:
                changes = dict(name=name, interval_seconds=interval)
                if not locked and connection is not None:
                    changes.update(namespace=namespace, workload=workload, container=container,
                                   initial_start=start, window_seconds=interval)
                definition = replace(old, **changes)
            else:
                definition = MonitorDefinition(workload, connection.source_scope, connection.source_scope,
                                               namespace, workload, start, container=container,
                                               interval_seconds=interval, window_seconds=interval,
                                               source_timezone=old.source_timezone if old else None)
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
                st.session_state['monitor_clone_definition'] = None
            st.success('Monitor saved. Execution belongs to the separate worker.')
            st.rerun()
        except (ValueError, TypeError):
            st.error('Check the target and historical start time. Refresh if this monitor was changed elsewhere.')
        except Exception:
            st.error('Monitoring storage is unavailable. Check database permissions and configuration.')


def monitoring_start(field, old):
    mode = st.radio('Start monitoring', ['From now', 'Historical backfill'], index=1 if old else 0, key=field('start_mode'))
    if mode == 'From now':
        return now()
    initial = old.initial_start if old else now() - timedelta(hours=1)
    date = st.date_input('Historical start date (UTC)', value=initial.date(), max_value=now().date(), key=field('start_date'))
    time = st.time_input('Historical start time (UTC)', value=initial.time().replace(tzinfo=None), key=field('start_time'))
    return datetime.combine(date, time, tzinfo=timezone.utc)


def age(value):
    if value is None:
        return 'Never'
    seconds = max(0, int((now() - value).total_seconds()))
    return f'{seconds} seconds ago' if seconds < 60 else f'{seconds // 60} minutes ago' if seconds < 3600 else f'{seconds // 3600} hours ago'


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
            st.subheader('Create Deployment Monitor')
            st.caption('Choose what to monitor. The platform handles acquisition and analysis.')
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
    st.markdown(style.fields([
        ('Monitor name', d.name), ('Namespace', d.namespace), ('Workload', d.workload),
        ('Run every', style.interval(d.interval_seconds)),
        ('Monitoring', 'Archived' if monitor.archived else 'Enabled' if monitor.enabled else 'Paused'),
    ], redact), unsafe_allow_html=True)
    with st.expander('Advanced information'):
        st.markdown(style.fields([
            ('Source', d.source_profile), ('Container filter', d.container or 'All containers'),
            ('Started', d.initial_start.isoformat()),
            ('Watermark', monitor.last_successful_end.isoformat() if monitor.last_successful_end else 'Not yet established'),
            ('Monitor ID', monitor.id),
        ], redact), unsafe_allow_html=True)
        st.caption('Acquisition diagnostics are stored with each run in Runs → Technical diagnostics.')
    if not monitor.archived:
        st.button('Create monitor with different target', key='clone_monitor_' + monitor.id,
                  on_click=clone_target, args=(d,))


def monitor_details(repo, connection, redact, monitor, latest):
    import monitoring_style as style
    from trace_views import event_lineage
    overview, findings, history, config = st.tabs(
        ['Overview', 'Findings', 'Runs', 'Settings'],
        key='monitor_tabs_' + monitor.id, on_change='rerun')
    with overview:
        st.subheader('Status')
        st.markdown(style.kpis(monitor, latest), unsafe_allow_html=True)
        st.caption('Log flow reflects the latest acquisition window, including bounded overlap; it is not a live probe.')
        if monitor.archived:
            st.info('ARCHIVED — removed from active monitoring. Historical runs and evidence are preserved.')
        elif not monitor.enabled:
            st.info('Paused monitor. Historical runs and evidence remain available; no new windows are scheduled.')
        if monitor.last_error_summary:
            st.error(redact((monitor.last_error_category or 'UNKNOWN') + ': ' + monitor.last_error_summary))
    with config:
        configuration(connection, redact, monitor)
        monitor_actions(repo, redact, monitor)
    # Target edits and safe scheduling changes stay in Settings.
    if not monitor.archived and st.session_state['monitor_edit_mode'] == monitor.id:
        with config:
            with st.container(border=True):
                st.subheader('Edit monitor')
                monitor_form(repo, connection, redact, monitor)
    runs = repo.history(monitor.id)
    latest_completed = next((r for r in runs if r.status.value == 'SUCCESS' and r.result_reference), None)
    latest_result = repo.result(latest_completed.id) if latest_completed else None
    latest_model = (InvestigationPresenter(redact).build(latest_result, source='Deployment Monitor',
                    target=redact(monitor.definition.namespace + ' / ' + monitor.definition.workload))
                    if latest_result else None)
    with overview:
        if latest_model:
            # Presenter groups authoritative persisted pattern IDs; no decisions are rerun.
            pattern_count = latest_result.get('source_summary', {}).get('pattern_count')
            st.metric('Patterns', pattern_count if pattern_count is not None else len(latest_model.patterns))
            if pattern_count is None:
                st.caption('Historical pattern count reflects the bounded persisted signal summaries.')
            if latest_completed != latest:
                st.caption('Findings below are from the last successful analysis: ' + age(latest_completed.actual_finished_at))
            important = [p for p in latest_model.patterns if any(s.outcome in ('Qualified', 'Incident evidence') for s in p.signals)]
            if latest_model.incidents:
                st.subheader('Latest finding')
                st.text(latest_model.incidents[0].title)
            elif important:
                st.subheader('Latest finding')
                st.text(important[0].text)
                st.caption(f'{important[0].count} occurrences · First seen {important[0].first_seen or "Unknown"} · Last seen {important[0].last_seen or "Unknown"}')
            if latest_model.incidents or important:
                st.button('Investigate', key='latest_finding_' + monitor.id,
                          on_click=select_investigation, args=(monitor.id, latest_completed.id))
    with findings:
        st.subheader('Findings')
        if latest_model:
            st.caption('Last successful analysis · ' + age(latest_completed.actual_finished_at))
            render_findings(latest_result, latest_model, monitor, latest_completed, redact)
        else:
            st.info('No successful runs with persisted findings yet.')
    with history:
        st.subheader('Run History')
        if not runs:
            st.info('No runs have been recorded for this monitor.')
        else:
            st.caption('Most recent 100 windows · Newest first · UTC')
            st.dataframe([dict(Window=f'{r.window.start:%d %b %H:%M} → {r.window.end:%d %b %H:%M}',
                               Status=r.status.value,
                               Logs=r.counts.events_retrieved if r.status.value == 'SUCCESS' else None, **{'Logical Events': r.counts.logical_events},
                               Signals=r.counts.qualified_signals, Incidents=r.counts.incidents) for r in runs],
                         hide_index=True, width='stretch')
            with st.expander('Run diagnostics · attempts and errors'):
                st.dataframe([dict(Run=r.id, Started=r.actual_started_at.isoformat(),
                                   Finished=r.actual_finished_at.isoformat() if r.actual_finished_at else 'In progress',
                                   Attempts=r.attempts, Error=redact(r.error_category or ''),
                                   Detail=redact(r.error_summary or ''), **asdict(r.counts)) for r in runs],
                             hide_index=True, width='stretch')
    if not runs:
        return
    with history:
        options = [r.id for r in runs]
        if st.session_state['selected_run_id'] not in options:
            st.session_state['selected_run_id'] = options[0]
        run_id = st.selectbox('Open run', options,
                              format_func=lambda key: next(r.window.start.isoformat() + ' · ' + r.status.value + ' · ' + r.id[:8]
                                                           for r in runs if r.id == key), key='selected_run_id')
        st.caption('Only persisted results from this exact run are opened.')
        run = next(item for item in runs if item.id == run_id)
        with st.expander('Selected run details'):
            st.text('Run ID: ' + run.id)
            st.text('Started: ' + run.actual_started_at.isoformat())
            st.text('Finished: ' + (run.actual_finished_at.isoformat() if run.actual_finished_at else 'In progress'))
            st.text('Attempts: ' + str(run.attempts))
            st.json(asdict(run.counts))
    with history:
        result = repo.result(run_id)
        st.caption(f'Selected run: {run.id} · {run.status.value} · {run.window.start.isoformat()} → {run.window.end.isoformat()}')
        if result is None:
            st.info('No persisted investigation was stored for this run.')
            with st.expander('Technical diagnostics'):
                diagnostics = repo.acquisition_diagnostics(run_id)
                if diagnostics is not None:
                    st.caption('Acquisition diagnostics from this run’s latest failed attempt.')
                    if diagnostics.get('query_executed') is False:
                        st.caption('The acquisition query was not sent. Shown filters are the configured candidate fields.')
                    st.json(safe_diagnostics(diagnostics, redact))
                    return
                d = run.definition
                st.json(safe_diagnostics(dict(source_profile=d.source_profile, start=run.window.start.isoformat(),
                       end=run.window.end.isoformat(), namespace=d.namespace, workload=d.workload,
                       container=d.container, document_cluster_filter_active=d.document_cluster_id is not None,
                       budget_reached=run.error_category == 'ACQUISITION_LIMIT'), redact))
                st.caption('These are the persisted run target and failure status. Resolved indices, query and acquisition counts were not stored.')
            return
        model = InvestigationPresenter(redact).build(result, source='Deployment Monitor',
                                                     target=redact(monitor.definition.namespace + ' / ' + monitor.definition.workload))
        summary = result.get('source_summary', {})
        with st.expander('Acquisition'):
            st.text(f'{run.counts.events_retrieved:,} logs retrieved · {summary.get("pages_read", "Unknown")} pages')
        with st.expander('Investigation', expanded=st.session_state.get('investigate_run_id') == run_id):
            views.investigation(model, include_pipeline=False)
        with st.expander('Technical diagnostics'):
            st.json(safe_diagnostics(summary, redact))
            boundary_details(summary, run.counts.logical_events, redact)
            trace = load_trace(result, run_id, redact)
            st.subheader('Pipeline Trace / Evidence Explorer')
            views.stage_details(model, trace=trace, trace_key='trace_' + run_id)
            event_lineage(trace, key='trace_' + run_id)


def select_investigation(monitor_id, run_id):
    st.session_state['selected_run_id'] = run_id
    st.session_state['investigate_run_id'] = run_id
    st.session_state['monitor_tabs_' + monitor_id] = 'Runs'


def safe_diagnostics(summary, redact):
    keys = ('source_profile', 'source_scope', 'resolved_index', 'index_expression', 'index_strategy',
            'start', 'end', 'retrieval_start', 'retrieval_end', 'namespace', 'workload', 'container',
            'document_cluster_filter_active', 'mapping_verified', 'query_executed', 'sort_fields', 'effective_query', 'pages_read', 'records_read',
            'unique_records', 'duplicate_records', 'budget_reached', 'stop_reason', 'policy_selection',
            'window_assembly', 'source_timezone')
    def clean(value):
        if isinstance(value, dict):
            return {redact(str(k)): clean(v) for k, v in value.items()}
        if isinstance(value, list):
            return [clean(v) for v in value]
        return redact(value) if isinstance(value, str) else value
    return {key: clean(summary[key]) for key in keys if key in summary}


def render_findings(result, model, monitor, run, redact):
    qualified = {s.get('signal_id') for s in result.get('signals', []) if s.get('qualified') is True}
    qualified.update(s.get('signal_id') for s in result.get('qualified_signals', []) if isinstance(s, dict))
    related = {sid: row.get('incident_id') for row in result.get('incidents', []) for sid in row.get('signal_ids', [])}
    rows = []
    for pattern in model.patterns:
        for signal in pattern.signals:
            if signal.signal_id in qualified or signal.signal_id in related:
                rows.append({'Finding': pattern.text, 'Occurrences': signal.count, 'Severity': pattern.severity,
                             'First seen': pattern.first_seen, 'Last seen': pattern.last_seen,
                             'Related incident': redact(str(related.get(signal.signal_id, ''))), 'Signal': signal.signal_id})
    if rows:
        st.dataframe(rows, hide_index=True, width='stretch')
        st.caption('Bounded persisted finding summaries; full evidence is available in the investigation.')
    elif not model.incidents:
        st.info('No qualified signals or incidents in the last successful analysis.')
    for incident in model.incidents:
        st.text(incident.title + ' · ' + incident.severity)
    if rows or model.incidents:
        st.button('Investigate', key='findings_investigate_' + monitor.id,
                  on_click=select_investigation, args=(monitor.id, run.id))
