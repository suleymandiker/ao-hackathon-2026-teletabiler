"""On-demand monitoring diagnostics; trace lives only in Streamlit session state."""

import streamlit as st


def render_debug(repository, monitor, runs, redact):
    from monitoring.debug import representative_debug, replay_window
    st.subheader('Debug Pipeline')
    st.caption('Run explicitly. Debug results stay in this browser session and do not change monitoring state.')
    if not runs:
        st.info('No recent run is available for debugging.')
        return
    options = {run.id: run for run in runs}
    run_id = st.selectbox('Window', list(options), key='debug_run_' + monitor.id,
        format_func=lambda key: options[key].window.start.isoformat())
    selection_key = 'debug_selection_' + monitor.id
    if st.session_state.get(selection_key) != run_id:
        st.session_state[selection_key] = run_id
        st.session_state.pop('debug_payload_' + monitor.id, None)
    mode = st.radio('Mode', ['Representative Debug', 'Exact Window Replay'],
                    key='debug_mode_' + monitor.id, horizontal=True)
    if st.button('Analyze Sample' if mode == 'Representative Debug' else 'Replay Full Window',
                 key='run_debug_' + monitor.id):
        try:
            selected = options[run_id]
            payload = (representative_debug(selected) if mode == 'Representative Debug'
                       else replay_window(repository, selected))
            st.session_state['debug_payload_' + monitor.id] = (run_id, mode, payload)
        except Exception:
            st.error('Debug execution failed. Monitoring state was not changed.')
    stored = st.session_state.get('debug_payload_' + monitor.id)
    if not stored or stored[:2] != (run_id, mode):
        return
    payload = stored[2]
    st.write('Window:', options[run_id].window.start.isoformat(),
             'to', options[run_id].window.end.isoformat())
    st.json(payload.get('stats', {}))
    if mode == 'Representative Debug':
        for ordinal, case in enumerate(payload.get('cases', ()), 1):
            with st.expander(f'Representative case {ordinal}'):
                for line in case['stream_context']:
                    st.code(redact(line))
    trace = payload.get('trace') or payload.get('detailed_pipeline_trace')
    if trace:
        from trace_views import event_lineage
        from investigation_views import stage_details
        from presentation import InvestigationPresenter
        model = InvestigationPresenter(redact).build(payload, source='Debug Pipeline')
        stage_details(model, trace=trace, trace_key='debug_' + run_id)
        event_lineage(trace, key='debug_' + run_id)
