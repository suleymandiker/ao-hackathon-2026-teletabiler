"""Monitor-only control plane and compact query regressions."""

from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse, unquote
import sqlite3

import pytest

from test_streamlit_sources import ui, assert_safe


BASE = datetime(2026, 10, 5, 12, tzinfo=timezone.utc)
REAL_CONNECT = sqlite3.connect


@pytest.fixture
def monitoring_ui(ui, monkeypatch, tmp_path):
    def connect(path, *args, **kwargs):
        if kwargs.get('uri'):
            path = unquote(urlparse(path).path).lstrip('/')
        assert Path(path).resolve().is_relative_to(tmp_path.resolve())
        return REAL_CONNECT(path, *args, **kwargs)

    monkeypatch.setattr(sqlite3, 'connect', connect)
    monkeypatch.setattr(sqlite3.dbapi2, 'connect', connect)
    monkeypatch.setenv('AIOPS_MONITOR_DB', str(tmp_path / 'monitor.sqlite3'))
    from monitoring.repository import SQLiteMonitorRepository
    from monitoring.domain import MonitorDefinition
    import monitoring_views

    monkeypatch.setattr(monitoring_views, 'now', lambda: BASE)
    repo = SQLiteMonitorRepository(tmp_path / 'monitor.sqlite3', compact_mode=True)
    monitor = repo.create(MonitorDefinition('synthetic', 'profile', 'cluster', 'ns', 'app', BASE),
                          enabled=True, now=BASE)
    return ui, repo, monitor


def test_home_one_list_query_and_rerender_snapshot(monitoring_ui, monkeypatch):
    ui, repo, monitor = monitoring_ui
    from monitoring.repository import SQLiteMonitorRepository
    calls = []
    original = SQLiteMonitorRepository.list_monitor_summaries
    def counted(self, **kwargs):
        calls.append(kwargs)
        return original(self, **kwargs)
    monkeypatch.setattr(SQLiteMonitorRepository, 'list_monitor_summaries', counted)
    app = ui.app()
    assert app.title[0].value == 'Deployment Monitors'
    assert len(calls) == 1
    app.text_input(key='monitor_search').set_value('app').run()
    app.selectbox(key='monitor_status_filter').set_value('Active').run()
    assert len(calls) == 1
    assert app.button(key='monitor_row_' + monitor.id)
    app.button(key='refresh_monitors').click().run()
    assert len(calls) == 2
    assert_safe(app)


def test_detail_only_loads_compact_recent_runs(monitoring_ui, monkeypatch):
    ui, repo, monitor = monitoring_ui
    from monitoring.repository import SQLiteMonitorRepository
    calls = []
    for name in ('recent_runs', 'result', 'acquisition_diagnostics', 'acquisition_shards'):
        original = getattr(SQLiteMonitorRepository, name)
        def wrap(method, label):
            def counted(self, *args, **kwargs):
                calls.append((label, kwargs))
                return method(self, *args, **kwargs)
            return counted
        monkeypatch.setattr(SQLiteMonitorRepository, name, wrap(original, name))
    app = ui.app()
    app.button(key='monitor_row_' + monitor.id).click().run()
    assert calls == [('recent_runs', {'limit': 10})]
    assert not app.tabs
    app.text_input(key='monitor_search').set_value('app').run()
    assert calls == [('recent_runs', {'limit': 10})]
    assert_safe(app)


def test_pause_and_refresh_invalidate_snapshot(monitoring_ui, monkeypatch):
    ui, repo, monitor = monitoring_ui
    from monitoring.repository import SQLiteMonitorRepository
    calls = []
    original = SQLiteMonitorRepository.list_monitor_summaries
    def counted(self, **kwargs):
        calls.append(1)
        return original(self, **kwargs)
    monkeypatch.setattr(SQLiteMonitorRepository, 'list_monitor_summaries', counted)
    app = ui.app()
    app.button(key='monitor_row_' + monitor.id).click().run()
    assert len(calls) == 1
    app.button(key='toggle_monitor_' + monitor.id).click().run()
    assert not repo.get(monitor.id).enabled
    assert len(calls) == 2
    assert_safe(app)


def test_run_now_writes_once_and_resume_requests_without_pipeline(monitoring_ui, monkeypatch):
    ui, repo, monitor = monitoring_ui
    from monitoring.repository import SQLiteMonitorRepository
    import monitoring.execution
    monkeypatch.setattr(monitoring.execution.OpenSearchMonitorExecutor, 'execute',
                        lambda *a, **kw: pytest.fail('UI must never execute monitoring'))
    writes = []
    original = SQLiteMonitorRepository.request_run_now

    def request(self, *args, **kwargs):
        writes.append(1)
        return original(self, *args, **kwargs)

    monkeypatch.setattr(SQLiteMonitorRepository, 'request_run_now', request)
    app = ui.app()
    app.button(key='monitor_row_' + monitor.id).click().run()
    assert app.button(key='run_now_' + monitor.id).label == 'Run Now'
    app.button(key='run_now_' + monitor.id).click().run()
    assert writes == [1] and repo.get(monitor.id).manual_requested_at == BASE
    assert any('Run requested' in row.value for row in app.success)
    assert app.button(key='run_now_' + monitor.id).disabled
    app.text_input(key='monitor_search').set_value('app').run()
    assert writes == [1] and repo.history(monitor.id) == []
    app.button(key='toggle_monitor_' + monitor.id).click().run()
    assert repo.get(monitor.id).manual_requested_at is None
    assert app.button(key='toggle_monitor_' + monitor.id).label == 'Resume Now'
    assert not [b for b in app.button if b.key == 'run_now_' + monitor.id]
    app.button(key='toggle_monitor_' + monitor.id).click().run()
    assert repo.get(monitor.id).enabled
    assert repo.get(monitor.id).manual_requested_at == BASE
    assert repo.history(monitor.id) == []
    assert_safe(app)


def test_running_and_archived_controls(monitoring_ui):
    ui, repo, monitor = monitoring_ui
    repo.request_run_now(monitor.id, now=BASE)
    run = repo.claim(BASE)
    app = ui.app()
    app.button(key='monitor_row_' + monitor.id).click().run()
    assert not [b for b in app.button if b.key == 'run_now_' + monitor.id]
    assert any('Current run may finish' in item.value for item in app.info)
    app.button(key='toggle_monitor_' + monitor.id).click().run()
    assert repo.get(monitor.id).status.value == 'PAUSED'
    assert repo.history(monitor.id)[0].id == run.id
    app.button(key='archive_' + monitor.id).click().run()
    app.button(key='confirm_archive_' + monitor.id).click().run()
    app.checkbox(key='show_archived').check().run()
    app.button(key='monitor_row_' + monitor.id).click().run()
    assert not [b for b in app.button if b.key in
                ('run_now_' + monitor.id, 'toggle_monitor_' + monitor.id)]
    assert_safe(app)


def test_compact_failure_reason_and_blocked_window_are_safe(monitoring_ui):
    ui, repo, monitor = monitoring_ui
    for attempt in range(3):
        stamp = BASE + timedelta(minutes=17 + 16 * attempt)
        run = repo.claim(stamp)
        repo.fail(run, 'OPENSEARCH_QUERY', now=stamp,
                  diagnostics={'reason_code': 'INVALID_CURSOR', 'password': 'password=secret'})
    app = ui.app()
    app.button(key='monitor_row_' + monitor.id).click().run()
    assert any('ATTENTION / BLOCKED' in item.value and 'INVALID_CURSOR' in item.value
               for item in app.warning)
    assert any('Blocked window:' in item.value for item in app.caption)
    assert app.button(key='run_now_' + monitor.id).label == 'Retry Now'
    app.button(key='run_now_' + monitor.id).click().run()
    assert repo.get(monitor.id).manual_requested_at == BASE
    assert repo.get(monitor.id).last_successful_end is None
    assert 'password=secret' not in str(app)
    assert 'password=secret' not in str(repo.acquisition_diagnostics(run.id))
    assert_safe(app)


def test_archived_monitor_absent_until_explicit_filter(monitoring_ui):
    ui, repo, monitor = monitoring_ui
    repo.archive(monitor.id, now=BASE)
    app = ui.app()
    assert not [button for button in app.button if button.key == 'monitor_row_' + monitor.id]
    app.checkbox(key='show_archived').check().run()
    assert app.button(key='monitor_row_' + monitor.id)
    assert_safe(app)


def test_older_runs_require_one_explicit_page_query(monitoring_ui, monkeypatch):
    ui, repo, monitor = monitoring_ui
    from monitoring.domain import RunCounts
    from monitoring.repository import SQLiteMonitorRepository
    for index in range(11):
        stamp = BASE + timedelta(minutes=17 + 15 * index)
        run = repo.claim(stamp)
        assert run is not None
        repo.succeed(run, {}, RunCounts(), {}, now=stamp,
                     metrics={'total_physical_logs': 0}, pattern_metrics=())
    calls = []
    original = SQLiteMonitorRepository.recent_runs
    def counted(self, *args, **kwargs):
        calls.append(kwargs)
        return original(self, *args, **kwargs)
    monkeypatch.setattr(SQLiteMonitorRepository, 'recent_runs', counted)
    app = ui.app()
    app.button(key='monitor_row_' + monitor.id).click().run()
    assert calls == [{'limit': 10}]
    app.button(key='older_' + monitor.id).click().run()
    assert len(calls) == 2 and calls[1]['limit'] == 10 and 'before' in calls[1]
    app.text_input(key='monitor_search').set_value('app').run()
    assert len(calls) == 2
    assert_safe(app)
