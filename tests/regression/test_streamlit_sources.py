"""Monitor-only Streamlit entrypoint with all external I/O blocked."""

from pathlib import Path
from types import SimpleNamespace
import sqlite3
from urllib.parse import urlparse, unquote

import pytest


ROOT = Path(__file__).resolve().parents[2]
APP = ROOT / 'src' / 'frontend' / 'streamlit_app.py'


@pytest.fixture
def ui(monkeypatch, tmp_path):
    monkeypatch.syspath_prepend(str(ROOT / 'src' / 'backend'))
    monkeypatch.syspath_prepend(str(ROOT / 'src' / 'frontend'))
    import requests
    import streamlit as st
    from streamlit.testing.v1 import AppTest
    import application_environment

    monkeypatch.setattr(application_environment, 'load_environment', lambda: None)
    def forbidden(*args, **kwargs):
        pytest.fail('The monitor home must not access network, SQLite, or learning state')

    monkeypatch.setattr(requests.sessions.Session, 'request', forbidden)
    monkeypatch.setattr(requests.sessions.Session, 'send', forbidden)
    real_connect = sqlite3.connect
    def temporary_only(path, *args, **kwargs):
        if kwargs.get('uri'):
            path = unquote(urlparse(path).path).lstrip('/')
        assert Path(path).resolve().is_relative_to(tmp_path.resolve())
        return real_connect(path, *args, **kwargs)
    monkeypatch.setattr(sqlite3, 'connect', temporary_only)
    monkeypatch.setattr(sqlite3.dbapi2, 'connect', temporary_only)
    monkeypatch.setenv('AIOPS_MONITOR_DB', str(tmp_path / 'missing.sqlite3'))
    st.cache_resource.clear()
    yield SimpleNamespace(app=lambda: AppTest.from_file(str(APP), default_timeout=10).run(), st=st)
    st.cache_resource.clear()


def assert_safe(app):
    assert not app.exception
    rendered = str(app)
    for marker in ('Authorization:', 'Bearer secret', 'password=secret', 'Traceback (most recent call last)'):
        assert marker not in rendered


def test_default_entrypoint_has_only_monitor_product(ui):
    app = ui.app()
    assert not app.exception
    assert app.title[0].value == 'Deployment Monitors'
    assert not app.radio
    rendered = str(app)
    for obsolete in ('Overview', 'Investigations', 'Log Patterns', 'Incidents',
                     'Manual Investigation / Test / Smoke', 'OpenShift configured',
                     'OpenShift Investigation', 'File / Package Analysis'):
        assert obsolete not in rendered
    assert any('Monitoring storage is temporarily unavailable' in item.value for item in app.error)
    assert_safe(app)
