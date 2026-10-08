"""Debug Pipeline is nested, explicit, and browser-session-only."""

from datetime import timedelta

from test_monitoring_ui import monitoring_ui, BASE
from test_streamlit_sources import ui, assert_safe


def _historical_run(repo, monitor):
    run = repo.claim(BASE + timedelta(minutes=17))
    repo.fail(run, 'OPENSEARCH_TIMEOUT', now=BASE + timedelta(minutes=17))
    return run


def test_representative_debug_is_explicit_and_memory_only(monitoring_ui, monkeypatch):
    ui, repo, monitor = monitoring_ui
    run = _historical_run(repo, monitor)
    import monitoring.debug as debug
    calls = []
    def sample(selected):
        calls.append(selected.id)
        return {'stats': {'sampled_physical_records': 1},
                'cases': [{'source_position': 0, 'stream_context': ['redacted sample']}],
                'trace': None}
    monkeypatch.setattr(debug, 'representative_debug', sample)
    app = ui.app()
    assert not calls
    app.button(key='monitor_row_' + monitor.id).click().run()
    assert not calls
    app.button(key='debug_' + monitor.id).click().run()
    assert not calls
    app.button(key='run_debug_' + monitor.id).click().run()
    assert calls == [run.id]
    assert app.session_state['debug_payload_' + monitor.id][2]['cases'][0]['source_position'] == 0
    assert repo.get(monitor.id).last_successful_end is None
    del app.session_state['debug_payload_' + monitor.id]
    app.run()
    assert not [item for item in app.code if 'redacted sample' in item.value]
    assert_safe(app)


def test_exact_replay_is_explicit_and_does_not_publish(monitoring_ui, monkeypatch):
    ui, repo, monitor = monitoring_ui
    run = _historical_run(repo, monitor)
    import monitoring.debug as debug
    calls = []
    def replay(read_repo, selected):
        calls.append((read_repo.path, selected.window))
        return {'stats': {'logical_events': 2}, 'trace': None}
    monkeypatch.setattr(debug, 'replay_window', replay)
    app = ui.app()
    app.button(key='monitor_row_' + monitor.id).click().run()
    app.button(key='debug_' + monitor.id).click().run()
    app.radio(key='debug_mode_' + monitor.id).set_value('Exact Window Replay').run()
    assert not calls
    app.button(key='run_debug_' + monitor.id).click().run()
    assert calls == [(repo.path, run.window)]
    assert repo.result(run.id) is None
    assert repo.get(monitor.id).last_successful_end is None
    assert_safe(app)
