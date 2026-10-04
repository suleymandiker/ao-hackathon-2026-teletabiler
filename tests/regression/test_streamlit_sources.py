"""Real Streamlit AppTest routing with fake acquisition/analysis boundaries."""
import asyncio
from contextlib import closing
import http.client
import json
import os
from pathlib import Path
import socket
import sqlite3
from types import SimpleNamespace
import urllib.request

import pytest


ROOT = Path(__file__).resolve().parents[2]
APP = ROOT / 'src' / 'frontend' / 'streamlit_app.py'
SECRET = 'never-render-this-password'
RAW = 'never-render-raw-log'


def result_fixture():
    return dict(stats={'segmented': 1, 'parsed': 1, 'templated': 1}, signals=[], qualified_signals=[],
                correlations=[], incidents=[], rca=[], plans=[], case_analysis=None, case_analysis_error=None,
                pipeline_trace={'segmentation': {'count': 1, 'items': [RAW]},
                                'parser': {'count': 1, 'items': [{'raw': RAW}]}, 'template': {'count': 1, 'items': []}},
                ingestion_diagnostics={'unassembled_count': 1, 'unassembled_by_reason': {'no_policy': 1},
                                       'unassembled_samples': [{'raw': RAW}]}, event_provenance=[])


@pytest.fixture
def ui(monkeypatch, tmp_path):
    monkeypatch.syspath_prepend(str(ROOT / 'src' / 'backend'))
    monkeypatch.syspath_prepend(str(ROOT / 'src' / 'frontend'))
    import requests
    import streamlit as st
    from streamlit.testing.v1 import AppTest
    import ai_engine
    import full_pipeline_v2
    import opensearch_application as boundary
    from segmentation_layer.contracts import SegmentationPolicy
    from template_layer.engine import DrainCandidateMiner, ValidatedTemplateRegistry

    def forbidden(*args, **kwargs):
        pytest.fail('UI tests must not use network, LLM, SQLite or production learning state')

    def loopback_only(original):
        def guarded(sock, address):
            # Windows asyncio's socketpair uses literal loopback TCP addresses.
            # HTTP is blocked separately, including HTTP directed at localhost.
            if (sock.family not in (socket.AF_INET, socket.AF_INET6)
                    or not isinstance(address, tuple) or not address
                    or address[0] not in ('127.0.0.1', '::1')):
                forbidden()
            return original(sock, address)
        return guarded

    for name in ('connect', 'connect_ex'):
        monkeypatch.setattr(socket.socket, name, loopback_only(getattr(socket.socket, name)))
    for owner, names in ((socket, ('create_connection', 'getaddrinfo')),
                         (requests.sessions.Session, ('request', 'send')),
                         (http.client.HTTPConnection, ('connect',)), (http.client.HTTPSConnection, ('connect',)),
                         (sqlite3, ('connect',)), (sqlite3.dbapi2, ('connect',)),
                         (DrainCandidateMiner, ('__init__',)), (ValidatedTemplateRegistry, ('__init__',))):
        for name in names:
            monkeypatch.setattr(owner, name, forbidden)
    monkeypatch.setattr(ai_engine, 'call_ai_agent', forbidden)
    for name in list(os.environ):
        if name.startswith('OPENSEARCH_'):
            monkeypatch.delenv(name)
    for name, value in dict(HOSTS='https://private-host.invalid', USERNAME='private-user', PASSWORD=SECRET,
                            USE_SSL='true', VERIFY_CERTS='true', SOURCE_SCOPE='profile', INDEX='logs-*',
                            SMOKE_NAMESPACE='ns', SMOKE_WORKLOAD='app').items():
        monkeypatch.setenv('OPENSEARCH_' + name, value)
    monkeypatch.setenv('AIOPS_POLICY_REGISTRY_PATH', str(tmp_path / 'unused.sqlite3'))
    selected = boundary.VerifiedPolicy('policy-safe-id', SegmentationPolicy('private-signature', '^BEGIN ', 'verified'))
    monkeypatch.setattr(boundary, 'list_verified_policies', lambda: (selected,))
    file_calls, package_calls, source_calls = [], [], []

    class Pipeline:
        def process_file(self, path):
            file_calls.append((Path(path).read_text(encoding='utf-8'), path))
            return result_fixture()
        def process_package(self, path):
            package_calls.append((Path(path).read_bytes(), path))
            return result_fixture()
        def process_ingested_pages(self, *args, **kwargs):
            pytest.fail('Acquisition is mocked at the application boundary in UI tests')

    pipeline = Pipeline()
    monkeypatch.setattr(full_pipeline_v2, 'FullAIOpsPipelineV2', lambda **kwargs: pipeline)

    def run_analysis(factory, request, policy):
        assert factory() is pipeline
        source_calls.append((request, policy))
        summary = dict(pages_read=3, records_read=201, start=request.start.isoformat(), end=request.end.isoformat(),
                       budget_reached=True, stop_reason='page_budget', max_pages=request.max_pages,
                       page_size=request.page_size, record_budget=request.max_pages * request.page_size)
        return boundary.presentation_result(result_fixture(), summary, boundary.load_connection())

    real_run_analysis = boundary.run_analysis
    monkeypatch.setattr(boundary, 'run_analysis', run_analysis)
    st.cache_resource.clear()
    yield SimpleNamespace(app=lambda: AppTest.from_file(str(APP), default_timeout=10).run(), st=st,
                          boundary=boundary, selected=selected, files=file_calls, packages=package_calls, sources=source_calls,
                          real_run_analysis=real_run_analysis)
    st.cache_resource.clear()


def select_openshift(app):
    app.radio(key='analysis_source').set_value('OpenShift logları').run()
    return app


def state_has(app, key):
    try:
        app.session_state[key]
    except KeyError:
        return False
    return True


def assert_safe(app):
    assert not app.exception
    # Inspect owned values explicitly as well as the rendered app/state so this
    # check does not depend on how a Streamlit version represents session state.
    retained = {key: app.session_state[key] for key in ('result', 'result_source') if state_has(app, key)}
    rendered = str(app) + str(app.session_state) + str(retained)
    for forbidden in (SECRET, 'private-user', 'private-host.invalid', 'private-signature', '^BEGIN ', RAW):
        assert forbidden not in rendered


def test_framework_event_loop_and_socketpair_work(ui):
    with closing(asyncio.new_event_loop()) as loop:
        assert not loop.is_closed()
    reader, writer = socket.socketpair()
    with reader, writer:
        reader.settimeout(2)
        writer.sendall(b'wakeup')
        assert reader.recv(6) == b'wakeup'


@pytest.mark.parametrize('family,host', [(socket.AF_INET, '127.0.0.1'), (socket.AF_INET6, '::1')])
@pytest.mark.parametrize('method', ['connect', 'connect_ex'])
def test_framework_loopback_connections_are_allowed(ui, family, host, method):
    with socket.socket(family) as listener, socket.socket(family) as writer:
        try:
            listener.bind((host, 0))
        except OSError:
            if family == socket.AF_INET6:
                pytest.skip('IPv6 loopback is unavailable on this host')
            raise
        listener.listen(1)
        listener.settimeout(2)
        writer.settimeout(2)
        result = getattr(writer, method)(listener.getsockname())
        assert result == (0 if method == 'connect_ex' else None)
        reader, _ = listener.accept()
        with reader:
            reader.settimeout(2)
            writer.sendall(b'wakeup')
            assert reader.recv(6) == b'wakeup'


@pytest.mark.parametrize('family,host', [(socket.AF_INET, '192.0.2.1'), (socket.AF_INET6, '2001:db8::1'),
                                       (socket.AF_INET, 'example.invalid')])
@pytest.mark.parametrize('method', ['connect', 'connect_ex'])
def test_non_loopback_socket_connections_remain_forbidden(ui, family, host, method):
    with socket.socket(family) as connection:
        connection.settimeout(0.1)
        with pytest.raises(pytest.fail.Exception, match='UI tests must not use network'):
            getattr(connection, method)((host, 443))


@pytest.mark.parametrize('host', ['example.invalid', '127.0.0.1', '[::1]'])
@pytest.mark.parametrize('scheme', ['http', 'https'])
def test_http_boundaries_block_external_and_loopback_requests(ui, host, scheme):
    import requests

    url = f'{scheme}://{host}/'
    with requests.Session() as session:
        with pytest.raises(pytest.fail.Exception, match='UI tests must not use network'):
            session.get(url)
        with pytest.raises(pytest.fail.Exception, match='UI tests must not use network'):
            session.send(requests.Request('GET', url).prepare())
    with pytest.raises(pytest.fail.Exception, match='UI tests must not use network'):
        urllib.request.urlopen(url, timeout=0.1)
    connection_type = http.client.HTTPSConnection if scheme == 'https' else http.client.HTTPConnection
    with closing(connection_type(host, timeout=0.1)) as connection:
        with pytest.raises(pytest.fail.Exception, match='UI tests must not use network'):
            connection.request('GET', '/')


@pytest.mark.parametrize('host', ['example.invalid', '127.0.0.1', '[::1]'])
def test_unmocked_opensearch_analysis_cannot_issue_http(ui, monkeypatch, host):
    monkeypatch.setenv('OPENSEARCH_HOSTS', f'https://{host}')
    request = ui.boundary.make_request('ns', 'app', '', 15, 100, 3, '2026-10-04T09:00:00+00:00')

    def unexpected_pipeline():
        pytest.fail('Network must be blocked before pipeline construction')

    # Exercise the real application, source and client, bypassing only the UI fake.
    with pytest.raises(pytest.fail.Exception, match='UI tests must not use network'):
        ui.real_run_analysis(unexpected_pipeline, request, ui.selected)


def test_llm_sqlite_and_learning_state_remain_forbidden(ui, tmp_path):
    import ai_engine
    from template_layer.engine import DrainCandidateMiner, ValidatedTemplateRegistry

    for action in (lambda: ai_engine.call_ai_agent('unused'),
                   lambda: sqlite3.connect(tmp_path / 'unused.sqlite3'),
                   lambda: sqlite3.dbapi2.connect(tmp_path / 'unused.sqlite3'),
                   DrainCandidateMiner, ValidatedTemplateRegistry):
        with pytest.raises(pytest.fail.Exception, match='UI tests must not use network'):
            action()
    assert not (tmp_path / 'unused.sqlite3').exists()


def test_default_source_keeps_upload_ui_and_no_analysis(ui):
    app = ui.app()
    assert not app.exception
    assert app.radio(key='analysis_source').value == 'Dosya / paket'
    assert len(app.get('file_uploader')) == 1
    assert not app.text_input and not ui.sources and not ui.files and not ui.packages


def test_openshift_requires_explicit_policy_then_routes_and_shows_summary(ui):
    app = select_openshift(ui.app())
    assert not app.get('file_uploader')
    assert app.selectbox(key='os_policy').value is None
    assert app.button[0].disabled
    app.selectbox(key='os_policy').select('policy-safe-id').run()
    app.text_input(key='os_end').set_value('2026-10-04T12:00:00+03:00').run()
    assert not app.button[0].disabled
    app.button[0].click().run()
    assert len(ui.sources) == 1 and not ui.files and not ui.packages
    request, selected = ui.sources[0]
    assert selected is ui.selected and request.namespace == 'ns' and request.workload == 'app'
    assert request.page_size == 100 and request.max_pages == 3
    assert request.end.isoformat() == '2026-10-04T09:00:00+00:00'
    assert any('sayfa sınırına' in item.value for item in app.warning)
    assert any('Kaynak Özeti' in item.value for item in app.markdown)
    assert_safe(app)
    for key in ('cursor', 'next_cursor', 'segmentation', 'segmentation_session', 'policy_provider'):
        assert not state_has(app, key)
    stored = app.session_state['result']
    assert 'event_provenance' not in stored and 'ingestion_diagnostics' not in stored
    assert all(stage['items'] == [] for stage in stored['pipeline_trace'].values())
    app.selectbox(key='view').select('Katman İzleme').run()
    assert any('ham loglar' in item.value for item in app.info)
    assert_safe(app)


@pytest.mark.parametrize('case', ['config', 'policy', 'query'])
def test_missing_or_invalid_input_disables_analysis_safely(ui, monkeypatch, case):
    if case == 'config':
        monkeypatch.delenv('OPENSEARCH_PASSWORD')
    elif case == 'policy':
        monkeypatch.setattr(ui.boundary, 'list_verified_policies', lambda: ())
    app = select_openshift(ui.app())
    if case != 'policy':
        app.selectbox(key='os_policy').select('policy-safe-id').run()
    if case == 'query':
        app.text_input(key='os_namespace').set_value('').run()
    assert app.button[0].disabled and not ui.sources
    assert_safe(app)


@pytest.mark.parametrize('code', ['no_records', 'connection_error', 'partial_search', 'analysis_error'])
def test_safe_errors_clear_stale_result_and_do_not_dump_exception(ui, monkeypatch, code):
    app = select_openshift(ui.app())
    app.selectbox(key='os_policy').select('policy-safe-id').run()
    app.button[0].click().run()
    assert app.session_state['result']

    def fail(*args):
        if code == 'analysis_error': raise RuntimeError(SECRET + RAW)
        raise ui.boundary.ApplicationError(code)

    monkeypatch.setattr(ui.boundary, 'run_analysis', fail)
    app.button[0].click().run()
    assert not state_has(app, 'result')
    assert not state_has(app, 'result_source')
    messages = app.info if code == 'no_records' else app.error
    assert any(ui.boundary.MESSAGES[code] == item.value for item in messages)
    assert_safe(app)


@pytest.mark.parametrize('name,content,route', [
    ('sample.log', b'unchanged raw log\n', 'file'),
    ('package.zip', b'fake zip passed through', 'package'),
    ('alarms.json', b'[{"alarm_id":"a","alarm_type":"disk_full","severity":5}]', 'file'),
    ('alarms.csv', b'alarm_id,alarm_type,severity\na,disk_full,5\n', 'file'),
])
def test_upload_routes_keep_conversion_package_bypass_and_cleanup(ui, monkeypatch, name, content, route):
    upload = SimpleNamespace(name=name, getvalue=lambda: content)
    monkeypatch.setattr(ui.st, 'file_uploader', lambda *a, **k: upload)
    app = ui.app()
    app.button[0].click().run()
    assert not app.exception and not ui.sources
    calls = ui.packages if route == 'package' else ui.files
    assert len(calls) == 1 and not Path(calls[0][1]).exists()
    if route == 'package': assert calls[0][0] == content and not ui.files
    elif name.endswith('.log'): assert calls[0][0] == content.decode()
    else:
        row = json.loads(calls[0][0])
        assert row['severity'] == 'CRITICAL' and row['source_severity'] == 5


def test_source_switch_does_not_show_the_other_sources_result(ui):
    app = select_openshift(ui.app())
    app.selectbox(key='os_policy').select('policy-safe-id').run()
    app.button[0].click().run()
    app.radio(key='analysis_source').set_value('Dosya / paket').run()
    assert not app.exception
    assert not any('Kaynak Özeti' in item.value for item in app.markdown)
    assert any('Analiz bekleniyor' in item.value for item in app.markdown)


def test_shared_pipeline_lock_prevents_overlapping_analysis(ui, monkeypatch):
    original = ui.boundary.run_analysis
    observed = []

    def inspect_lock(factory, request, policy):
        lock = factory.__wrapped__.__globals__['get_analysis_lock']()
        assert not lock.acquire(blocking=False)
        observed.append(lock)
        return original(factory, request, policy)

    monkeypatch.setattr(ui.boundary, 'run_analysis', inspect_lock)
    app = select_openshift(ui.app())
    app.selectbox(key='os_policy').select('policy-safe-id').run()
    app.button[0].click().run()
    assert not app.exception and len(observed) == 1
    lock = observed[0]
    assert lock.acquire(blocking=False)  # Released after the successful invocation.
    try:
        app.button[0].click().run()
        assert len(ui.sources) == 1
        assert any(item.value == ui.boundary.MESSAGES['busy'] for item in app.warning)
    finally:
        lock.release()


def test_removed_policy_is_not_silently_replaced_on_rerun(ui, monkeypatch):
    from dataclasses import replace

    app = select_openshift(ui.app())
    app.selectbox(key='os_policy').select('policy-safe-id').run()
    assert not app.button[0].disabled
    monkeypatch.setattr(ui.boundary, 'list_verified_policies',
                        lambda: (replace(ui.selected, selection_id='policy-different'),))
    app.run()
    assert app.selectbox(key='os_policy').value is None
    assert app.button[0].disabled and not ui.sources
    assert_safe(app)
