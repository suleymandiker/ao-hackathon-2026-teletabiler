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
    return dict(stats={'segmented': 7, 'parsed': 7, 'templated': 7, 'signal_candidates': 1,
                       'qualified_signals': 0, 'noise_suppressed': 1, 'incidents': 0, 'correlations': 0, 'rca': 0},
                signals=[dict(signal_id='signal-one', template_id='template-one', template='HTTP request completed',
                              count=7, severity_min=6, qualified=False, qualification_reason='gürültü_olarak_bastırıldı',
                              qualification_evidence=['güvenilir_şablon'], service_name='checkout',
                              event_ids=['event-one'], timestamp_resolved=True, first_seen_ms=1791104400000,
                              last_seen_ms=1791104403000)], qualified_signals=[],
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
    payload = result_fixture()

    class Pipeline:
        def process_file(self, path):
            file_calls.append((Path(path).read_text(encoding='utf-8'), path))
            return payload
        def process_package(self, path):
            package_calls.append((Path(path).read_bytes(), path))
            return payload
        def process_ingested_pages(self, *args, **kwargs):
            pytest.fail('Acquisition is mocked at the application boundary in UI tests')

    pipeline = Pipeline()
    monkeypatch.setattr(full_pipeline_v2, 'FullAIOpsPipelineV2', lambda **kwargs: pipeline)

    def run_analysis(factory, request, policy=None, *, automatic=False):
        assert automatic and policy is None
        assert factory() is pipeline
        source_calls.append(request)
        summary = dict(pages_read=3, records_read=201, start=request.start.isoformat(), end=request.end.isoformat(),
                       budget_reached=True, stop_reason='page_budget', max_pages=request.max_pages,
                       page_size=request.page_size, record_budget=request.max_pages * request.page_size)
        return boundary.presentation_result(payload, summary, boundary.load_connection())

    real_run_analysis = boundary.run_analysis
    monkeypatch.setattr(boundary, 'run_analysis', run_analysis)
    st.cache_resource.clear()
    yield SimpleNamespace(app=lambda: AppTest.from_file(str(APP), default_timeout=10).run(), st=st,
                          boundary=boundary, selected=selected, files=file_calls, packages=package_calls, sources=source_calls,
                          real_run_analysis=real_run_analysis, payload=payload)
    st.cache_resource.clear()


def select_openshift(app):
    app.button(key='start_openshift').click().run()
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
        ui.real_run_analysis(unexpected_pipeline, request, automatic=True)


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


def test_overview_loads_exact_logo_and_task_navigation(ui, monkeypatch):
    import hashlib
    logo = ROOT / 'src' / 'frontend' / 'assets' / 'ai-in-ai-logo.png'
    assert hashlib.sha256(logo.read_bytes()).hexdigest() == '442477acf5c107ad74812f04900b0bd3454ee82cb7ed5c17f4929ca9d2257de5'
    loaded = []
    original_image = ui.st.image

    def image(path, **kwargs):
        loaded.append((Path(path), kwargs.get('width')))
        return original_image(path, **kwargs)

    monkeypatch.setattr(ui.st, 'image', image)
    app = ui.app()
    assert not app.exception and app.title[0].value == 'AI-IN-AI Operations'
    assert loaded == [(logo, 161)]
    assert app.radio(key='navigation').options == ['Overview', 'Investigations', 'Log Patterns', 'Incidents']
    assert not ui.sources and not ui.files and not ui.packages
    assert not app.get('file_uploader')


def test_openshift_automatically_routes_and_shows_result_first(ui):
    app = select_openshift(ui.app())
    assert app.title[0].value == 'Yeni Investigation'
    assert not state_has(app, 'os_policy')
    assert all('politik' not in item.label.lower() for item in app.selectbox)
    assert app.selectbox(key='os_lookback').value == 15
    assert not app.button(key='run_analysis').disabled
    app.text_input(key='os_end').set_value('2026-10-04T12:00:00+03:00').run()
    app.button(key='run_analysis').click().run()
    assert len(ui.sources) == 1 and not ui.files and not ui.packages
    request = ui.sources[0]
    assert request.namespace == 'ns' and request.workload == 'app'
    assert request.page_size == 100 and request.max_pages == 3
    assert request.end.isoformat() == '2026-10-04T09:00:00+00:00'
    assert app.title[0].value == 'Investigation sonucu'
    view = app.session_state['result']
    assert view.stages[0].value == '201' and view.stages[4].value == '1 → 0'
    assert view.stages[4].status == 'attention' and view.stages[6].status == 'inactive'
    assert 'incident bulunamadı' in view.title
    assert_safe(app)
    for key in ('cursor', 'next_cursor', 'segmentation', 'segmentation_session', 'policy_provider'):
        assert not state_has(app, key)
    app.selectbox(key='pipeline_stage').select(4).run()
    assert any('backend kararları' in item.value for item in app.caption)
    assert_safe(app)


@pytest.mark.parametrize('case', ['config', 'query', 'policy'])
def test_missing_input_or_removed_policy_blocks_analysis_safely(ui, monkeypatch, case):
    if case == 'config':
        monkeypatch.delenv('OPENSEARCH_PASSWORD')
    app = select_openshift(ui.app())
    if case == 'query':
        app.text_input(key='os_namespace').set_value('').run()
    if case == 'policy':
        monkeypatch.setattr(ui.boundary, 'list_verified_policies', lambda: ())
        monkeypatch.setattr(ui.boundary, 'run_analysis', ui.real_run_analysis)
        app.button(key='run_analysis').click().run()
        assert any(item.value == ui.boundary.MESSAGES['no_policy'] for item in app.error)
        assert not state_has(app, 'result')
    else:
        assert app.button(key='run_analysis').disabled
    assert not ui.sources
    assert_safe(app)


@pytest.mark.parametrize('code', ['no_records', 'connection_error', 'partial_search', 'analysis_error',
                                 'no_policy', 'ambiguous_policy', 'policy_store_error'])
def test_safe_errors_remove_stale_result_and_do_not_dump_exception(ui, monkeypatch, code):
    app = select_openshift(ui.app())
    app.button(key='run_analysis').click().run()
    assert state_has(app, 'result')

    def fail(*args, **kwargs):
        if code == 'analysis_error':
            raise RuntimeError(SECRET + RAW)
        raise ui.boundary.ApplicationError(code)

    monkeypatch.setattr(ui.boundary, 'run_analysis', fail)
    app.button(key='new_investigation_button').click().run()
    app.button(key='run_analysis').click().run()
    assert not state_has(app, 'result') and not state_has(app, 'result_source')
    messages = app.info if code == 'no_records' else app.error
    assert any(item.value == ui.boundary.MESSAGES[code] for item in messages)
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
    app.button(key='start_file').click().run()
    assert app.radio(key='analysis_source').value == 'Dosya / paket'
    app.button(key='run_analysis').click().run()
    assert not app.exception and not ui.sources
    calls = ui.packages if route == 'package' else ui.files
    assert len(calls) == 1 and not Path(calls[0][1]).exists()
    if route == 'package':
        assert calls[0][0] == content and not ui.files
    elif name.endswith('.log'):
        assert calls[0][0] == content.decode()
    else:
        row = json.loads(calls[0][0])
        assert row['severity'] == 'CRITICAL' and row['source_severity'] == 5
    assert app.title[0].value == 'Investigation sonucu'
    assert_safe(app)


def test_source_switch_isolates_results_across_all_exploration_pages(ui):
    app = select_openshift(ui.app())
    app.button(key='run_analysis').click().run()
    app.button(key='new_investigation_button').click().run()
    app.radio(key='analysis_source').set_value('Dosya / paket').run()
    app.radio(key='navigation').set_value('Log Patterns').run()
    assert not app.exception
    assert any('Gösterilecek pattern yok' in item.value for item in app.markdown)
    assert not any('HTTP request completed' in item.value for item in app.markdown)
    app.radio(key='navigation').set_value('Incidents').run()
    assert any('Incident bulunmuyor' in item.value for item in app.markdown)
    app.radio(key='navigation').set_value('Log Patterns').run()
    assert any('Gösterilecek pattern yok' in item.value for item in app.markdown)
    assert not app.dataframe


def test_file_result_remains_available_after_navigation_and_reruns(ui, monkeypatch):
    upload = SimpleNamespace(name='sample.log', getvalue=lambda: b'unchanged log\n')
    monkeypatch.setattr(ui.st, 'file_uploader', lambda *a, **k: upload)
    app = ui.app()
    app.button(key='start_file').click().run()
    app.button(key='run_analysis').click().run()
    app.radio(key='navigation').set_value('Log Patterns').run()
    assert app.dataframe and not app.exception
    app.selectbox(key='pattern_selection').select(0).run()
    assert any('HTTP request completed' in item.value for item in app.markdown)
    app.radio(key='navigation').set_value('Investigations').run()
    assert app.title[0].value == 'Investigation sonucu'
    app.button(key='new_investigation_button').click().run()
    assert app.radio(key='analysis_source').value == 'Dosya / paket'
    assert_safe(app)


def test_shared_pipeline_lock_prevents_overlapping_analysis_and_releases(ui):
    from analysis_runtime import get_analysis_lock
    app = select_openshift(ui.app())
    lock = get_analysis_lock()
    assert lock.acquire(blocking=False)
    try:
        app.button(key='run_analysis').click().run()
        assert not ui.sources
        assert any(item.value == ui.boundary.MESSAGES['busy'] for item in app.warning)
    finally:
        lock.release()
    app.button(key='run_analysis').click().run()
    assert len(ui.sources) == 1 and not app.exception
    assert lock.acquire(blocking=False)
    lock.release()


def test_patterns_detail_uses_safe_evidence_without_raw_dump(ui):
    app = select_openshift(ui.app())
    app.button(key='run_analysis').click().run()
    app.radio(key='navigation').set_value('Log Patterns').run()
    app.selectbox(key='pattern_selection').select(0).run()
    assert not app.exception
    assert any('HTTP request completed' in item.value for item in app.markdown)
    assert any('Pattern → Signal Candidate → Suppressed' == item.value for item in app.caption)
    assert_safe(app)


def test_redaction_and_html_escaping_survive_normal_and_technical_views(ui):
    ui.payload['signals'][0]['template'] = '<script>alert(1)</script> password=unsafe-value'
    ui.payload['signals'][0]['Authorization'] = 'Bearer unsafe-value'
    ui.payload['signals'][0]['cursor'] = 'unsafe-cursor'
    ui.payload['signals'][0]['raw_http_response'] = 'unsafe-body'
    app = select_openshift(ui.app())
    app.button(key='run_analysis').click().run()
    rendered = str(app) + str(app.session_state['result'])
    for secret in ('unsafe-value', 'unsafe-cursor', 'unsafe-body'):
        assert secret not in rendered
    assert any('&lt;script&gt;' in item.value for item in app.markdown)
    assert not any('<script>' in item.value for item in app.markdown)
    assert_safe(app)


def test_incident_detail_uses_actual_severity_rca_and_topology(ui):
    ui.payload['incidents'] = [dict(incident_id='inc-test', severity_min=2, status='aday', signal_ids=['signal-one'],
                                   template_ids=['template-one'], signal_count=1, event_count=7,
                                   services=['checkout', 'database'], probable_root=dict(entity='database', alarm_type='db_conn_pool'),
                                   context={'services': ['checkout', 'database'], 'dependencies': [{'path': ['checkout', 'database']}]})]
    ui.payload['stats']['incidents'] = 1
    ui.payload['rca'] = [dict(incident_id='inc-test', root_cause_candidates=[dict(signal_id='signal-one', score=.75, evidence=['güvenilir_şablon'])])]
    app = select_openshift(ui.app())
    app.button(key='run_analysis').click().run()
    assert not app.exception
    app.radio(key='navigation').set_value('Incidents').run()
    assert any('CRITICAL' in item.value for item in app.markdown)
    assert any(item.value == 'Servis bağımlılıkları' for item in app.subheader)
    assert any('hipotez' in item.value for item in app.caption)
    assert_safe(app)
