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
    assert not app.exception
    assert app.title[0].value == 'Deployment Monitors'
    assert any('Source unavailable' in item.value for item in app.warning)
    assert any('PAUSED' in item.value for item in app.text)
    assert len(repo.history(monitor.id)) == 0
    app.button(key='toggle_monitor').click().run()
    assert repo.get(monitor.id).enabled
    app.button(key='toggle_monitor').click().run()
    assert not repo.get(monitor.id).enabled
    assert not ui.sources and not ui.files and not ui.packages
    assert_safe(app)


def test_successful_run_history_uses_investigation_workbench(monitoring_ui):
    ui, repo, monitor = monitoring_ui
    from monitoring.domain import RunCounts
    repo.set_enabled(monitor.id, True, now=BASE)
    run = repo.claim(BASE + timedelta(minutes=17))
    safe = ui.boundary.presentation_result(ui.payload, {'records_read': 7}, ui.boundary.load_connection())
    repo.succeed(run, safe, RunCounts(events_retrieved=7, logical_events=7), {}, now=BASE + timedelta(minutes=17))
    app = ui.app()
    app.button(key='start_monitors').click().run()
    assert not app.exception
    assert app.selectbox(key='monitor_run').value == run.id
    assert any(item.value == 'Investigation sonucu' for item in app.title)
    assert any('Last successful end: 2026-10-05T12:15:00+00:00' == item.value for item in app.text)
    assert_safe(app)


def test_create_disabled_and_edit_monitor_from_form(monitoring_ui):
    ui, repo, _ = monitoring_ui
    app = ui.app()
    app.button(key='start_monitors').click().run()
    app.text_input(key='monitor_name_new').set_value('created-in-ui')
    # Form widgets are identified by labels; two forms exist on this screen.
    def first(label):
        return next(item for item in app.text_input if item.label == label)
    first('Cluster').set_value('cluster')
    first('Namespace').set_value('ns')
    first('Deployment / workload').set_value('app')
    app.button(key='save_monitor_new').click().run()
    assert not app.exception
    monitors = repo.list()
    assert len(monitors) == 2
    created = next(m for m in monitors if m.definition.name == 'created-in-ui')
    assert not created.enabled and created.definition.source_timezone is None
    app.selectbox(key='inspect_monitor').select(created.id).run()
    app.text_input(key='monitor_name_' + created.id).set_value('edited-in-ui')
    app.button(key='save_monitor_' + created.id).click().run()
    assert not app.exception
    assert repo.get(created.id).definition.name == 'edited-in-ui'
    assert not repo.history(created.id) and not ui.sources


def test_manual_upload_navigation_remains_available(monitoring_ui):
    ui, _, _ = monitoring_ui
    app = ui.app()
    assert any('Manual Investigation / Test / Smoke' == item.value for item in app.subheader)
    app.button(key='start_file').click().run()
    assert not app.exception
    assert app.radio(key='analysis_source').value == 'Dosya / paket'
    assert app.selectbox(key='file_source_timezone').value == 'Unknown'
