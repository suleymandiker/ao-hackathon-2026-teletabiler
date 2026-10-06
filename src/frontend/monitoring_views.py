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


def repository():
    return SQLiteMonitorRepository(database_path())


def now():
    return datetime.now(timezone.utc)


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
    with st.form('monitor_form_' + key):
        name = st.text_input('Monitor name', value=old.name if old else '', key='monitor_name_' + key)
        profile = st.text_input('Source profile / scope', value=old.source_profile if old else (connection.source_scope if connection else ''),
                                help='Must match OPENSEARCH_SOURCE_SCOPE. Connection and index settings stay in application configuration.')
        cluster_alias = st.text_input('Logical cluster alias', value=old.cluster_alias if old else (connection.source_scope if connection else ''),
                                      help='Logical source name, retained for existing monitors. This does not filter document cluster UUIDs.')
        document_cluster_id = st.text_input('Document OpenShift cluster UUID (optional)',
                                            value=(old.document_cluster_id or '') if old else '',
                                            help='Only enter a verified openshift.cluster_id from documents. Leave blank to omit this filter; never infer it from the alias or index.')
        namespace = st.text_input('Namespace', value=old.namespace if old else '')
        workload = st.text_input('Deployment / workload', value=old.workload if old else '',
                                 help='Exact configured workload label; do not use an ephemeral pod name.')
        container = st.text_input('Container (optional)', value=(old.container or '') if old else '')
        left, right = st.columns(2)
        with left:
            interval = st.number_input('Interval (seconds)', min_value=1, max_value=86400, value=old.interval_seconds if old else 900)
            window = st.number_input('Analysis window (seconds)', min_value=1, max_value=86400, value=old.window_seconds if old else 900)
            delay = st.number_input('Ingestion delay (seconds)', min_value=0, max_value=86400, value=old.ingestion_delay_seconds if old else 60)
        with right:
            overlap = st.number_input('Boundary overlap (seconds)', min_value=0, max_value=3600, value=old.overlap_seconds if old else 60)
            zone = st.text_input('Source timezone (IANA; blank = Unknown)', value=(old.source_timezone or '') if old else '',
                                 help='For a validated UTC source enter UTC explicitly. OpenSearch record time is not shifted.')
            initial = old.initial_start if old else now().replace(second=0, microsecond=0) - timedelta(minutes=17)
            start = st.text_input('Initial window start (aware ISO timestamp)', value=initial.isoformat())
        with st.expander('Acquisition limits'):
            page_size = st.number_input('Page size', min_value=1, max_value=500, value=old.page_size if old else 100)
            max_pages = st.number_input('Maximum pages', min_value=1, max_value=20, value=old.max_pages if old else 20)
        enabled = st.checkbox('Enabled', value=monitor.enabled if monitor else False)
        if monitor:
            st.caption('After the first run, scope, window, timezone and acquisition settings are immutable. Create a new monitor to change them.')
        submitted = st.form_submit_button('Save monitor', key='save_monitor_' + key)
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
            else:
                repo.create(definition, enabled=enabled, now=now())
            st.success('Monitor saved. Execution belongs to the separate worker.')
            st.rerun()
        except (ValueError, TypeError):
            st.error('Check required scope, aware start time, IANA timezone and bounds. Refresh if edited elsewhere; existing run scope cannot change.')
        except Exception:
            st.error('Monitoring storage is unavailable. Check database permissions and configuration.')


def render(connection, redact):
    st.title('Deployment Monitors')
    st.caption('Define monitoring here. Start the independent worker to execute bounded windows; Refresh reads persisted status.')
    if connection is None:
        st.warning('Source unavailable: OpenSearch configuration is missing or invalid. Persisted monitors and history remain available. Configure OPENSEARCH_* in the application environment or repository .env, then restart the UI and worker.')
    else:
        st.info('Source configuration loaded. Connectivity is checked by the worker during acquisition.')
    st.button('Refresh status', key='refresh_monitors')
    try:
        repo = repository()
        monitors = repo.list()
    except Exception:
        st.error('Monitoring storage is unavailable. Check AIOPS_MONITOR_DB and directory permissions.')
        return
    with st.expander('New Monitor', expanded=not monitors):
        monitor_form(repo, connection, redact)
    if not monitors:
        st.info('No monitors yet. Create a disabled monitor first for a bounded smoke test.')
        return
    rows = []
    for monitor in monitors:
        latest = repo.history(monitor.id, limit=1)
        run = latest[0] if latest else None
        rows.append(dict(Name=redact(monitor.definition.name), Source_scope=redact(monitor.definition.source_profile),
                         Cluster_alias=redact(monitor.definition.cluster_alias),
                         Namespace=redact(monitor.definition.namespace), Deployment=redact(monitor.definition.workload),
                         Container=redact(monitor.definition.container or ''), Enabled=monitor.enabled, Status=monitor.status.value,
                         Interval=monitor.definition.interval_seconds, Last_run=run.status.value if run else '',
                         Next_run=monitor.next_run_at.isoformat(), Incidents=run.counts.incidents if run else 0))
    st.dataframe(rows, hide_index=True, width='stretch')
    selected = st.selectbox('Inspect monitor', [monitor.id for monitor in monitors],
                            format_func=lambda key: next(redact(m.definition.name) for m in monitors if m.id == key), key='inspect_monitor')
    monitor = next(m for m in monitors if m.id == selected)
    st.subheader(redact(monitor.definition.name))
    st.text(f'{monitor.status.value} | enabled={monitor.enabled}')
    st.text('Last successful end: ' + (monitor.last_successful_end.isoformat() if monitor.last_successful_end else 'None'))
    st.text('Source timezone: ' + (monitor.definition.source_timezone or 'Unknown'))
    if monitor.last_error_summary:
        st.error(monitor.last_error_category + ': ' + monitor.last_error_summary)
    if st.button('Pause monitor' if monitor.enabled else 'Enable monitor', key='toggle_monitor'):
        repo.set_enabled(monitor.id, not monitor.enabled, now=now())
        st.rerun()
    with st.expander('Edit monitor'):
        monitor_form(repo, connection, redact, monitor)
    st.subheader('Run History')
    runs = repo.history(monitor.id)
    if not runs:
        st.info('No runs. Paused monitors do not execute.')
        return
    st.caption('Most recent 100 windows. Retries reuse the same run; attempts are counted.')
    st.dataframe([dict(Window=f'[{r.window.start.isoformat()}, {r.window.end.isoformat()})',
                       Started=r.actual_started_at.isoformat(),
                       Duration=(r.actual_finished_at - r.actual_started_at).total_seconds() if r.actual_finished_at else None,
                       Status=r.status.value, Attempts=r.attempts, **asdict(r.counts), Error=r.error_category or '') for r in runs],
                 hide_index=True, width='stretch')
    successful = [r for r in runs if r.result_reference]
    if successful:
        run_id = st.selectbox('Open investigation', [r.id for r in successful],
                              format_func=lambda key: next(r.window.start.isoformat() for r in successful if r.id == key), key='monitor_run')
        result = repo.result(run_id)
        if result is not None:
            summary = result.get('source_summary', {})
            with st.expander('Window, source policy and boundary diagnostics'):
                # The full per-event status contract is persisted, but this
                # general view must not bypass the affected-event display cap.
                st.json({key: value for key, value in summary.items() if key != 'boundary_quality'})
            run = next(item for item in successful if item.id == run_id)
            boundary_details(summary, run.counts.logical_events, redact)
            model = InvestigationPresenter(redact).build(result, source='Deployment Monitor',
                                                         target=redact(monitor.definition.namespace + ' / ' + monitor.definition.workload))
            views.investigation(model)
