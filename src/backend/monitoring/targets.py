"""Bounded, read-only target discovery/validation; no analysis or learning."""
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import os
from urllib.parse import quote

from ingestion_layer.opensearch_client import OpenSearchClient
from ingestion_layer.opensearch_source import resolve_exact_mapping, _path
from ingestion_layer.opensearch_indices import resolve_index_expression
from opensearch_application import safe_text


class TargetValidationError(ValueError):
    """Fixed error only; transport/source payloads are never exposed."""


@dataclass(frozen=True)
class TargetOptions:
    values: tuple[str, ...]
    truncated: bool = False


@dataclass(frozen=True)
class TargetValidation:
    count: int
    latest: datetime | None
    sample: str | None


class TargetExplorer:
    """Discovery looks back 24h; live validation looks back 15m.

    One application-owned client per create/edit interaction. At most 200
    selectable targets per level, one representative log; no raw hit cache.
    Quiet means a discovered target with zero logs in the shorter window.
    """
    def __init__(self, config, end, *, client_factory=OpenSearchClient):
        self.config = config
        self.end = end.astimezone(timezone.utc)
        self.start = self.end - timedelta(hours=24)
        self.client_factory = client_factory

    def __enter__(self):
        self.client = self.client_factory(self.config)
        try:
            self.config = resolve_exact_mapping(self.client, self.start, self.end)
            return self
        except Exception:
            self.client.close()
            raise TargetValidationError('Target/source validation failed. Check source configuration and mapping.') from None

    def __exit__(self, *args):
        self.client.close()

    def _filters(self, start, namespace=None, workload=None, container=None):
        m = self.config.field_mapping
        filters = [{'range': {m.timestamp: {'gte': start.isoformat(), 'lt': self.end.isoformat()}}}]
        for name, value in (('namespace', namespace), ('workload', workload), ('container', container)):
            if value is not None:
                filters.append({'term': {getattr(m, name + '_exact'): value}})
        return filters

    def _search(self, start, body):
        index = resolve_index_expression(self.config.index_expression, start, self.end, strategy=self.config.index_strategy)
        body['timeout'] = f'{int(self.config.request_timeout * 1000)}ms'
        try:
            result = self.client.post_json('/' + quote(index, safe='*,.-_') + '/_search', body)
            if result.get('timed_out') is not False or result.get('_shards', {}).get('failed') != 0:
                raise ValueError('Incomplete search')
            return result
        except Exception:
            raise TargetValidationError('Target/query/source validation failed. Check connection, permissions and mapping.') from None

    def options(self, name, *, namespace=None, workload=None):
        if name not in ('namespace', 'workload', 'container'):
            raise ValueError('Unsupported selector')
        body = {'size': 0, 'query': {'bool': {'filter': self._filters(self.start, namespace, workload)}},
                'aggs': {'targets': {'terms': {'field': getattr(self.config.field_mapping, name + '_exact'),
                                              'size': 200, 'order': {'_key': 'asc'}}}}}
        result = self._search(self.start, body)
        try:
            targets = result['aggregations']['targets']
            values = tuple(row['key'] for row in targets['buckets'])
            if len(values) > 200 or any(type(value) is not str or not value.strip() or len(value) > 200
                                       or any(ord(c) < 32 for c in value) for value in values):
                raise ValueError('Invalid target names')
            return TargetOptions(values, targets.get('sum_other_doc_count', 0) > 0)
        except Exception:
            raise TargetValidationError('Source target discovery returned invalid evidence.') from None

    def validate(self, namespace, workload, container=None):
        start = self.end - timedelta(minutes=15)
        m = self.config.field_mapping
        body = {'size': 1, 'track_total_hits': True,
                'query': {'bool': {'filter': self._filters(start, namespace, workload, container)}},
                '_source': [m.timestamp, m.message],
                'sort': [{m.timestamp: 'desc'}, {m.sequence: 'desc'}]}
        result = self._search(start, body)
        try:
            hits = result['hits']
            total = hits['total']
            count = total['value']
            if total['relation'] != 'eq' or type(count) is not int or count < 0:
                raise ValueError('Incomplete count')
            rows = hits['hits']
            if len(rows) != (1 if count else 0):
                raise ValueError('Missing representative evidence')
            if not count:
                return TargetValidation(0, None, None)
            source = rows[0]['_source']
            timestamp = _path(source, m.timestamp)
            latest = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            if latest.utcoffset() is None or not start <= latest <= self.end:
                raise ValueError('Invalid source time')
            message = _path(source, m.message)
            if type(message) is not str:
                raise ValueError('Missing application message')
            # Redact before truncation so clipped credentials cannot escape.
            sample = safe_text(message, self.config)
            secret = os.environ.get('SAKA_API_KEY')
            if secret:
                sample = sample.replace(secret, '[redacted]')
            return TargetValidation(count, latest.astimezone(timezone.utc), sample[:2000])
        except Exception:
            raise TargetValidationError('Source validation returned incomplete count or sample evidence.') from None
