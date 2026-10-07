"""Offline target discovery and keyword-field acquisition regression."""
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'src' / 'backend'))
from ingestion_layer.opensearch_config import OpenSearchConfig, OpenSearchFieldMapping
from ingestion_layer.opensearch_source import OpenSearchSource


def config():
    return OpenSearchConfig(('https://source.invalid',), 'fixture-reader', 'fixture-password',
                            True, True, 'gocpbmgpup1*', 5, 15, 'gocpbmgpup1',
                            OpenSearchFieldMapping(), 100, index_strategy='daily_utc')


class KeywordClient:
    """Keyword base fields have no .keyword subfields, as in OpenShift indices."""
    def __init__(self):
        self.config = config()
        self.calls = []

    def post_json(self, path, query):
        self.calls.append((path, query))
        if path.endswith('_field_caps'):
            return {'fields': {name: {'keyword': {'type': 'keyword', 'searchable': True,
                                                  'aggregatable': True}} for name in (
                'kubernetes.namespace_name', 'kubernetes.labels.app', 'kubernetes.container_name',
                'openshift.cluster_id')}}
        terms = {key: value for clause in query['query']['bool']['filter']
                 for key, value in clause.get('term', {}).items()}
        matches = terms == {'kubernetes.namespace_name': 'ai-document-assistant',
                            'kubernetes.labels.app': 'aida-agent-http'}
        hit = {'_index': 'gocpbmgpup1-2026.10.07', '_id': 'known-document',
               'sort': [1791353751813, 123], '_source': {
                   '@timestamp': '2026-10-07T06:15:51.813083313Z', 'message': 'INFO Agent completed',
                   'openshift': {'cluster_id': 'document-uuid', 'sequence': 123},
                   'kubernetes': {'namespace_name': 'ai-document-assistant', 'labels': {'app': 'aida-agent-http'},
                                  'pod_name': 'agent-pod', 'pod_id': 'pod-uuid', 'container_name': 'aida-agent-http',
                                  'container_id': 'container-uuid', 'container_iostream': 'stdout'}}}
        return {'timed_out': False, '_shards': {'failed': 0}, 'hits': {'hits': [hit] if matches else []}}


def test_known_aida_document_matches_exact_keyword_base_fields():
    client = KeywordClient()
    # Reproduce the old acquisition independently: terms on nonexistent
    # .keyword fields produce a successful search containing zero records.
    kwargs = dict(start=datetime(2026, 10, 7, 6, 1, tzinfo=timezone.utc),
                  end=datetime(2026, 10, 7, 6, 16, tzinfo=timezone.utc),
                  namespace='ai-document-assistant', workload='aida-agent-http')
    assert not OpenSearchSource(client).read_page(**kwargs).records
    import ingestion_layer.opensearch_source as source_module
    resolve = source_module.resolve_exact_mapping
    client.config = resolve(client, kwargs['start'], kwargs['end'])
    page = OpenSearchSource(client).read_page(**kwargs)
    assert len(page.records) == 1
    assert client.calls[-1][0] == '/gocpbmgpup1-2026.10.07/_search'
    filters = client.calls[-1][1]['query']['bool']['filter']
    assert filters[1:] == [
        {'term': {'kubernetes.namespace_name': 'ai-document-assistant'}},
        {'term': {'kubernetes.labels.app': 'aida-agent-http'}}]
    assert page.records[0].source_timestamp_raw == '2026-10-07T06:15:51.813083313Z'


@pytest.mark.parametrize('kind', ['text', 'missing', 'conflict', 'nonsearchable'])
def test_mapping_failure_is_explicit_instead_of_quiet(kind):
    from ingestion_layer.opensearch_source import resolve_exact_mapping, OpenSearchSourceError
    client = KeywordClient()
    capabilities = {} if kind == 'missing' else {'kubernetes.namespace_name': {
        'text' if kind == 'text' else 'keyword': {'searchable': kind != 'nonsearchable', 'aggregatable': True}}}
    if kind == 'conflict':
        capabilities['kubernetes.namespace_name']['text'] = {'searchable': True}
    client.post_json = lambda *a: {'fields': capabilities}
    with pytest.raises(OpenSearchSourceError):
        resolve_exact_mapping(client, datetime(2026, 10, 7, tzinfo=timezone.utc),
                              datetime(2026, 10, 8, tzinfo=timezone.utc), names=('namespace',))


def test_configured_keyword_subfields_are_preserved():
    from ingestion_layer.opensearch_source import resolve_exact_mapping
    client = KeywordClient()
    client.post_json = lambda path, query: {'fields': {field: {'keyword': {'searchable': True, 'aggregatable': True}}
                                                     for field in query['fields']}}
    actual = resolve_exact_mapping(client, datetime(2026, 10, 7, tzinfo=timezone.utc), datetime(2026, 10, 8, tzinfo=timezone.utc))
    assert actual == config()


class DiscoveryClient(KeywordClient):
    quiet = False
    failed = False

    def close(self):
        pass

    def post_json(self, path, query):
        if path.endswith('_field_caps'):
            return super().post_json(path, query)
        self.calls.append((path, query))
        if self.failed:
            return {'timed_out': False, '_shards': {'failed': 1}}
        if 'aggs' in query:
            field = query['aggs']['targets']['terms']['field']
            key = {'kubernetes.namespace_name': 'ai-document-assistant',
                   'kubernetes.labels.app': 'aida-agent-http', 'kubernetes.container_name': 'main'}[field]
            return {'timed_out': False, '_shards': {'failed': 0}, 'aggregations': {
                'targets': {'buckets': [{'key': key}], 'sum_other_doc_count': 0}}}
        return {'timed_out': False, '_shards': {'failed': 0}, 'hits': {
            'total': {'value': 0 if self.quiet else 3800, 'relation': 'eq'}, 'hits': [] if self.quiet else [{'_source': {
                '@timestamp': '2026-10-07T06:15:51.813083313Z',
                'message': 'INFO Agent completed password=private-password; Authorization: Bearer private-token; fixture-password'}}]}}


def test_dependent_discovery_validation_container_refresh_and_redacted_sample():
    from monitoring.targets import TargetExplorer
    client = DiscoveryClient()
    end = datetime(2026, 10, 7, 6, 16, tzinfo=timezone.utc)
    with TargetExplorer(config(), end, client_factory=lambda c: client) as explorer:
        assert explorer.options('namespace').values == ('ai-document-assistant',)
        assert explorer.options('workload', namespace='ai-document-assistant').values == ('aida-agent-http',)
        assert explorer.options('container', namespace='ai-document-assistant', workload='aida-agent-http').values == ('main',)
        validation = explorer.validate('ai-document-assistant', 'aida-agent-http')
        assert validation.count == 3800 and validation.latest < end
        assert 'Agent completed' in validation.sample
        assert not any(secret in validation.sample for secret in ('private-password', 'private-token', 'fixture-password'))
        explorer.validate('ai-document-assistant', 'aida-agent-http', 'main')
        filters = client.calls[-1][1]['query']['bool']['filter']
        assert filters[-1] == {'term': {'kubernetes.container_name': 'main'}}
        assert filters[0] == {'range': {'@timestamp': {'gte': '2026-10-07T06:01:00+00:00', 'lt': end.isoformat()}}}
        assert client.calls[-1][1]['size'] == 1
        assert client.calls[-1][1]['_source'] == ['@timestamp', 'message']
        assert 'openshift.cluster_id' not in str(filters)


def test_quiet_target_and_failed_query_are_distinct():
    from monitoring.targets import TargetExplorer, TargetValidationError
    client = DiscoveryClient()
    with TargetExplorer(config(), datetime(2026, 10, 7, 6, 16, tzinfo=timezone.utc), client_factory=lambda c: client) as explorer:
        client.quiet = True
        quiet = explorer.validate('ai-document-assistant', 'aida-agent-http')
        assert quiet.count == 0 and quiet.latest is None and quiet.sample is None
        client.failed = True
        with pytest.raises(TargetValidationError, match='validation failed'):
            explorer.validate('ai-document-assistant', 'aida-agent-http')
