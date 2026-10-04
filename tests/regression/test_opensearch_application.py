"""Offline application-boundary checks; only test-owned policy SQLite is allowed."""
from dataclasses import FrozenInstanceError, replace
from contextlib import closing
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import socket
import sqlite3
from tempfile import TemporaryDirectory
from types import SimpleNamespace

import pytest


SECRET = 'synthetic-private-password'
RAW = 'private raw multiline\n    diagnostic'
CURSOR = 'private-cursor'
REGEX = r'^BEGIN '
END = datetime(2026, 10, 4, 9, tzinfo=timezone.utc)


@pytest.fixture
def local_catalog_dir():
    # Citrix TEMP may be UNC; SQLite read-only URIs require a local authority.
    # An explicit parent bypasses TEMP and keeps all state outside src/data.
    test_root = Path(__file__).resolve().parents[2] / 'tests'
    with TemporaryDirectory(prefix='.opensearch-catalog-', dir=test_root) as directory:
        path = Path(directory)
        yield path
    assert not path.exists(), 'Temporary catalog directory was not cleaned up'


@pytest.fixture
def app(monkeypatch, local_catalog_dir, request):
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[2] / 'src' / 'backend'))
    import requests
    import ai_engine
    import opensearch_application as module

    def forbidden(*args, **kwargs):
        pytest.fail('No network, LLM or production policy-state access')

    for owner, names in ((socket.socket, ('connect', 'connect_ex')), (socket, ('create_connection', 'getaddrinfo')),
                         (requests.sessions.Session, ('__init__', 'request'))):
        for name in names:
            monkeypatch.setattr(owner, name, forbidden)
    monkeypatch.setattr(ai_engine, 'call_ai_agent', forbidden)
    for name in list(os.environ):
        if name.startswith('OPENSEARCH_'):
            monkeypatch.delenv(name)
    for name, value in dict(HOSTS='https://example.invalid', USERNAME='private-user', PASSWORD=SECRET,
                            USE_SSL='true', VERIFY_CERTS='true', SOURCE_SCOPE='test-cluster', INDEX='test-*').items():
        monkeypatch.setenv('OPENSEARCH_' + name, value)
    database = local_catalog_dir / getattr(request, 'param', 'policies.sqlite3')
    monkeypatch.setenv('AIOPS_POLICY_REGISTRY_PATH', str(database))
    real_connect = sqlite3.connect

    def guarded_connect(path, *args, **kwargs):
        assert path == database.resolve().as_uri() + '?mode=ro'
        assert kwargs.get('uri') is True
        return real_connect(path, *args, **kwargs)

    monkeypatch.setattr(sqlite3, 'connect', guarded_connect)
    monkeypatch.setattr(sqlite3.dbapi2, 'connect', guarded_connect)

    def seed(rows):
        with closing(real_connect(database)) as con:
            con.execute('CREATE TABLE segmentation_policies(signature TEXT PRIMARY KEY, regex TEXT)')
            con.executemany('INSERT INTO segmentation_policies VALUES(?, ?)', rows)
            con.execute('CREATE TABLE parser_policies(signature TEXT, policy_json TEXT)')
            con.execute('INSERT INTO parser_policies VALUES(?, ?)', ('unverified', 'private payload'))
            con.commit()

    def read_catalog():
        try:
            return module.list_verified_policies()
        except module.ApplicationError as error:
            # Expose the original failure only for this test-owned database.
            # Production retains its safe, suppressed exception chain.
            raise AssertionError('Unable to read the temporary test policy catalog') from error.__context__

    return SimpleNamespace(module=module, database=database, seed=seed, forbidden=forbidden,
                           read_catalog=read_catalog)


def policy(app):
    from segmentation_layer.contracts import SegmentationPolicy
    return app.module.VerifiedPolicy('policy-test', SegmentationPolicy('original-signature', REGEX, 'verified-fixture'))


def request(app, *, max_pages=3, page_size=2):
    return app.module.make_request('ns', 'app', '', 15, page_size, max_pages, END.isoformat())


def source_page(number, *, exhausted=False, empty=False, cursor=CURSOR):
    from ingestion_layer.contracts import Framing, IngestedLogRecord, SourcePage, SourceReference, StreamIdentity
    rec = IngestedLogRecord(RAW, SourceReference('source', 'index', str(number)),
                            stream_identity=StreamIdentity('cluster', pod_instance='pod', container_instance='run', channel='stdout'),
                            framing=Framing.PHYSICAL_LINE)
    return SourcePage(() if empty else (rec,), exhausted, next_cursor=None if exhausted else cursor)


def install(app, monkeypatch, pages, pipeline=None):
    module = app.module
    calls, configs, closed, feeds, policies = [], [], [], [], []
    supplied = iter(pages)

    class Client:
        def __init__(self, config):
            self.config = config
            configs.append(config)
        def __enter__(self): return self
        def __exit__(self, *args): closed.append(True)

    class Source:
        def __init__(self, client): pass
        def read_page(self, **kwargs):
            calls.append(kwargs)
            output = next(supplied)
            if isinstance(output, Exception): raise output
            return output

    class Pipeline:
        def process_ingested_pages(self, pages, *, policy_provider):
            for page in pages:
                feeds.append(page)
                for record in page.records:
                    policies.append(policy_provider(None, record))
            return dict(stats={'segmented': len(policies)}, ingestion_diagnostics={'unassembled_count': 0},
                        event_provenance=[], pipeline_trace={'segmentation': {'count': len(policies), 'items': [RAW]}})
        def process_file(self, *args): pytest.fail('Wrong processing route')
        def process_package(self, *args): pytest.fail('Wrong processing route')

    monkeypatch.setattr(module, 'OpenSearchClient', Client)
    monkeypatch.setattr(module, 'OpenSearchSource', Source)
    return SimpleNamespace(calls=calls, configs=configs, closed=closed, feeds=feeds, policies=policies,
                           factory=lambda: pipeline or Pipeline())


def test_two_pages_use_one_fixed_interval_and_original_cursor_order(app, monkeypatch):
    pages = [source_page(1), source_page(2, exhausted=True)]
    fake = install(app, monkeypatch, pages)
    selected = policy(app)
    result = app.module.run_analysis(fake.factory, request(app), selected)
    assert fake.feeds == pages and fake.closed == [True]
    assert [call['cursor'] for call in fake.calls] == [None, CURSOR]
    assert all(call['end'] == END and call['start'] == datetime(2026, 10, 4, 8, 45, tzinfo=timezone.utc) for call in fake.calls)
    assert all(call['namespace'] == 'ns' and call['workload'] == 'app' and call['page_size'] == 2 for call in fake.calls)
    assert all(snapshot is selected.snapshot for snapshot in fake.policies)
    with pytest.raises(FrozenInstanceError): selected.snapshot.regex_pattern = '.*'
    assert result['source_summary']['stop_reason'] == 'interval_exhausted'
    assert result['source_summary']['records_read'] == 2
    assert RAW not in json.dumps(result)


@pytest.mark.parametrize('last', ['budget', 'exhausted', 'empty', 'missing_cursor', 'empty_cursor'])
def test_finite_budget_and_current_view_stop_without_cursor_interpretation(app, monkeypatch, last):
    end_page = source_page(2, exhausted=last == 'exhausted', empty=last == 'empty',
                           cursor=None if last == 'missing_cursor' else '' if last == 'empty_cursor' else CURSOR)
    fake = install(app, monkeypatch, [source_page(1), end_page])
    result = app.module.run_analysis(fake.factory, request(app, max_pages=2), policy(app))
    assert len(fake.calls) == 2
    assert result['source_summary']['budget_reached'] is (last in ('budget', 'empty_cursor'))
    assert result['source_summary']['record_budget'] == 4


def test_empty_first_page_is_clear_and_does_not_construct_pipeline(app, monkeypatch):
    fake = install(app, monkeypatch, [source_page(1, empty=True, exhausted=True)])
    with pytest.raises(app.module.ApplicationError) as error:
        app.module.run_analysis(app.forbidden, request(app), policy(app))
    assert error.value.code == 'no_records' and len(fake.calls) == 1 and fake.closed == [True]


@pytest.mark.parametrize('name', ['HOSTS', 'USERNAME', 'PASSWORD', 'USE_SSL', 'VERIFY_CERTS', 'SOURCE_SCOPE', 'INDEX'])
def test_missing_configuration_is_safe_before_any_network(app, monkeypatch, name):
    monkeypatch.delenv('OPENSEARCH_' + name)
    with pytest.raises(app.module.ApplicationError) as error:
        app.module.run_analysis(app.forbidden, request(app), policy(app))
    assert error.value.code == 'configuration_error'
    assert SECRET not in app.module.error_message(error.value)


def test_missing_policy_blocks_before_configuration_or_acquisition(app, monkeypatch):
    monkeypatch.setattr(app.module, 'load_connection', app.forbidden)
    with pytest.raises(app.module.ApplicationError) as error:
        app.module.run_analysis(app.forbidden, request(app), None)
    assert error.value.code == 'no_policy'


@pytest.mark.parametrize('field,value', [('page_size', 0), ('page_size', 501), ('max_pages', 0), ('max_pages', 21)])
def test_request_budget_cannot_bypass_ui_limits(app, field, value):
    with pytest.raises(app.module.ApplicationError) as error:
        replace(request(app), **{field: value})
    assert error.value.code == 'query_error'


@pytest.mark.parametrize('lookback,end', [(0, ''), (1441, ''), (15, 'not-a-time'), (15, '2026-10-04T10:00:00')])
def test_invalid_or_unbounded_time_is_rejected(app, lookback, end):
    with pytest.raises(app.module.ApplicationError):
        app.module.make_request('ns', 'app', '', lookback, 100, 3, end)


def test_tls_ca_and_timeout_settings_are_preserved(app, monkeypatch):
    monkeypatch.setenv('OPENSEARCH_CA_BUNDLE', 'configured-ca.pem')
    monkeypatch.setenv('OPENSEARCH_CONNECT_TIMEOUT_SECONDS', '4')
    config = app.module.load_connection()
    assert config.verify_certs is True and config.ca_bundle == 'configured-ca.pem' and config.connect_timeout == 4
    monkeypatch.delenv('OPENSEARCH_CA_BUNDLE')
    monkeypatch.setenv('OPENSEARCH_VERIFY_CERTS', 'false')
    assert app.module.load_connection().verify_certs is False


def test_verified_catalog_is_read_only_and_does_not_leak_regex_or_signature(app):
    app.seed([('private-signature', REGEX), ('broken', '['), ('empty', '')])
    before = app.database.read_bytes()
    policies = app.read_catalog()
    assert len(policies) == 1
    assert policies[0].snapshot.policy_id == 'private-signature'
    assert policies[0].snapshot.regex_pattern == REGEX
    assert policies[0].selection_id.startswith('policy-')
    assert 'private-signature' not in repr(policies) and REGEX not in repr(policies)
    assert app.database.read_bytes() == before
    assert app.read_catalog() == policies


def test_catalog_database_is_in_a_repository_local_test_directory(app):
    test_root = Path(__file__).resolve().parents[2] / 'tests'
    assert app.database.parent.parent == test_root
    assert app.database.parent.name.startswith('.opensearch-catalog-')
    assert app.database.resolve().as_uri().startswith('file:///')


@pytest.mark.parametrize('app', ['policies.sqlite3', 'policies # % ı.sqlite3'], indirect=True)
def test_catalog_connection_rejects_writes_and_preserves_encoded_path_identity(app, monkeypatch):
    signature = '0123456789abcdef01234567'
    app.seed([(signature, REGEX)])
    before = app.database.read_bytes()
    guarded_connect = sqlite3.connect
    write_checks = []

    def check_read_only(*args, **kwargs):
        connection = guarded_connect(*args, **kwargs)
        try:
            with pytest.raises(sqlite3.OperationalError, match='readonly'):
                connection.execute('DELETE FROM segmentation_policies')
        except BaseException:
            connection.close()
            raise
        write_checks.append(True)
        return connection

    monkeypatch.setattr(sqlite3, 'connect', check_read_only)
    selected, = app.read_catalog()
    assert write_checks == [True]
    assert selected.selection_id == 'policy-' + signature
    assert selected.snapshot.policy_id == signature and selected.snapshot.regex_pattern == REGEX
    assert app.database.read_bytes() == before


def test_missing_catalog_does_not_create_sqlite_or_choose_fallback(app):
    assert app.module.list_verified_policies() == ()
    assert not app.database.exists()


def test_corrupt_catalog_is_safe(app):
    app.database.write_text(SECRET)
    with pytest.raises(app.module.ApplicationError) as error:
        app.module.list_verified_policies()
    assert error.value.code == 'policy_store_error' and SECRET not in str(error.value)


@pytest.mark.parametrize('error_kind,code', [('client', 'connection_error'), ('shard', 'partial_search'),
                                           ('timeout', 'partial_search'), ('source', 'source_error'), ('other', 'analysis_error')])
def test_error_categories_never_render_exception_chains(app, monkeypatch, error_kind, code):
    errors = dict(client=app.module.OpenSearchClientError(SECRET),
                  shard=app.module.OpenSearchSourceError('Search shard failure or missing shard status'),
                  timeout=app.module.OpenSearchSourceError('Search timed out or lacks completion evidence'),
                  source=app.module.OpenSearchSourceError(SECRET), other=RuntimeError(SECRET))
    fake = install(app, monkeypatch, [errors[error_kind]])
    with pytest.raises(app.module.ApplicationError) as error:
        app.module.run_analysis(fake.factory, request(app), policy(app))
    assert error.value.code == code and SECRET not in app.module.error_message(error.value)
    assert fake.closed == [True]


def test_projection_removes_raw_traces_provenance_secrets_and_samples(app):
    token = 'eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJzZWNyZXQifQ.signature'
    original = dict(stats={'segmented': 1}, signals=[{'template': SECRET + ' ' + token, 'raw': RAW}],
                    pipeline_trace={'parser': {'count': 1, 'items': [{'raw': RAW, 'attributes': {'cursor': CURSOR}}]}},
                    ingestion_diagnostics={'unassembled_count': 1, 'unassembled_by_reason': {'no_policy': 1},
                                           'unassembled_samples': [{'raw': RAW}]},
                    event_provenance=[{'provenance': {'stream_key': dict(source_scope='scope', pod_instance='pod', container_instance='run', channel='stdout'),
                                                     'contributors': [{'source_reference': CURSOR}]}}],
                    case_analysis_error=SECRET)
    result = app.module.presentation_result(original, {'pages_read': 1}, app.module.load_connection())
    serialized = json.dumps(result)
    for sensitive in (RAW, CURSOR, SECRET, token, 'source_reference', 'contributors', 'unassembled_samples'):
        assert sensitive not in serialized
    assert result['source_summary']['assembled_stream_count'] == 1
    assert result['source_summary']['unassembled_by_reason'] == {'no_policy': 1}
    assert original['signals'][0]['raw'] == RAW


def test_two_invocations_start_fresh_without_cached_cursor(app, monkeypatch):
    fake = install(app, monkeypatch, [source_page(1, exhausted=True), source_page(2, exhausted=True)])
    selected = policy(app)
    for _ in range(2):
        app.module.run_analysis(fake.factory, request(app), selected)
    assert [call['cursor'] for call in fake.calls] == [None, None]
    assert fake.closed == [True, True]


def test_later_page_failure_does_not_return_partial_success(app, monkeypatch):
    fake = install(app, monkeypatch, [source_page(1), app.module.OpenSearchSourceError('Search shard failure or missing shard status')])
    with pytest.raises(app.module.ApplicationError) as error:
        app.module.run_analysis(fake.factory, request(app), policy(app))
    assert error.value.code == 'partial_search'
    assert len(fake.feeds) == 1 and fake.closed == [True]


def test_oversized_page_cannot_bypass_record_budget(app, monkeypatch):
    oversized = source_page(1)
    oversized = replace(oversized, records=oversized.records * 3)
    fake = install(app, monkeypatch, [oversized])
    with pytest.raises(app.module.ApplicationError) as error:
        app.module.run_analysis(fake.factory, request(app, page_size=2), policy(app))
    assert error.value.code == 'source_error' and fake.feeds == []


def test_normal_registry_signature_is_recognizable_without_altering_identity(app):
    signature = '0123456789abcdef01234567'
    app.seed([(signature, REGEX)])
    selected, = app.read_catalog()
    assert selected.selection_id == 'policy-' + signature
    assert selected.snapshot.policy_id == signature


@pytest.mark.parametrize('text', ['"password": "log-secret"', "'api_key'='log-secret'", 'Bearer log-secret',
                                 'access_token=log-secret', 'token: log-secret', 'Authorization: Bearer log-secret'])
def test_credential_assignments_in_derived_text_are_redacted(app, text):
    assert 'log-secret' not in app.module.safe_text(text, app.module.load_connection())
