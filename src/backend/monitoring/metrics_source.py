"""Exact logical-window OpenSearch counts without downloading source bodies."""

from dataclasses import dataclass
from datetime import datetime
import math
from urllib.parse import quote

from ingestion_layer.opensearch_indices import resolve_index_expression
from ingestion_layer.opensearch_source import OpenSearchSourceError


MAX_TIME_BUCKETS = 1_440
MAX_TERMS = 32


@dataclass(frozen=True)
class WindowMetrics:
    total: int
    buckets: tuple[tuple[int, int], ...]
    pods: dict[str, int]
    containers: dict[str, int]
    pod_other_count: int
    container_other_count: int
    pod_counts_exact: bool
    container_counts_exact: bool
    query_scope: dict

    def to_dict(self):
        return dict(total_physical_logs=self.total,
                    time_buckets=[dict(timestamp_ms=stamp, count=count) for stamp, count in self.buckets],
                    pods=self.pods, containers=self.containers,
                    pod_other_count=self.pod_other_count,
                    container_other_count=self.container_other_count,
                    pod_counts_exact=self.pod_counts_exact,
                    container_counts_exact=self.container_counts_exact,
                    query_scope=self.query_scope)


class OpenSearchMetricsSource:
    def __init__(self, client):
        self.client = client
        self.requests = 0

    def _scope(self, start, end, definition):
        config = self.client.config
        mapping = config.field_mapping
        index = resolve_index_expression(config.index_expression, start, end, strategy=config.index_strategy)
        filters = [{'range': {mapping.timestamp: {'gte': start.isoformat(), 'lt': end.isoformat()}}},
                   {'term': {mapping.namespace_exact: definition.namespace}},
                   {'term': {mapping.workload_exact: definition.workload}}]
        if definition.container:
            filters.append({'term': {mapping.container_exact: definition.container}})
        if definition.document_cluster_id:
            filters.append({'term': {mapping.cluster_id_exact: definition.document_cluster_id}})
        return index, filters

    def _exact_field(self, index, base, preferred):
        fields = sorted({base, preferred})
        self.requests += 1
        caps = self.client.post_json('/' + quote(index, safe='*,.-_') + '/_field_caps', {},
                                     params={'fields': ','.join(fields)})
        if not isinstance(caps, dict) or caps.get('failures'):
            raise OpenSearchSourceError('Metrics mapping verification failed')
        available = caps.get('fields') or {}
        for name in (preferred, base):
            types = available.get(name, {})
            if set(types) == {'keyword'} and types['keyword'].get('searchable') is True and types['keyword'].get('aggregatable') is True:
                return name
        return None

    def _query(self, start, end, definition, *, aggregations=None):
        index, filters = self._scope(start, end, definition)
        config = self.client.config
        body = {'size': 0, 'track_total_hits': True, 'query': {'bool': {'filter': filters}},
                'timeout': f'{math.ceil(config.request_timeout * 1000)}ms'}
        if aggregations:
            body['aggs'] = aggregations
        self.requests += 1
        payload = self.client.post_json('/' + quote(index, safe='*,.-_') + '/_search', body)
        if not isinstance(payload, dict) or payload.get('timed_out') is not False:
            raise OpenSearchSourceError('Metrics search timed out or lacks completion evidence')
        shards = payload.get('_shards')
        if not isinstance(shards, dict) or type(shards.get('failed')) is not int or shards['failed']:
            raise OpenSearchSourceError('Metrics shard failure or missing shard status')
        total = (payload.get('hits') or {}).get('total')
        if not isinstance(total, dict) or type(total.get('value')) is not int or total.get('relation') != 'eq':
            raise OpenSearchSourceError('Exact metrics count unavailable')
        return total['value'], payload.get('aggregations') or {}, index

    def count(self, start, end, definition):
        total, _, _ = self._query(start, end, definition)
        return total

    def window(self, run):
        """The only volume baseline query: exact [window.start, window.end)."""
        start, end = run.window.start, run.window.end
        index, filters = self._scope(start, end, run.definition)
        mapping = self.client.config.field_mapping
        pod_field = self._exact_field(index, mapping.pod, mapping.pod_exact)
        container_field = self._exact_field(index, mapping.container, mapping.container_exact)
        aggs = {'by_minute': {'date_histogram': {'field': mapping.timestamp,
                                                 'fixed_interval': '1m', 'min_doc_count': 0}}}
        if pod_field:
            aggs['by_pod'] = {'terms': {'field': pod_field, 'size': MAX_TERMS,
                                        'show_term_doc_count_error': True}}
        if container_field:
            aggs['by_container'] = {'terms': {'field': container_field, 'size': MAX_TERMS,
                                              'show_term_doc_count_error': True}}
        total, values, _ = self._query(start, end, run.definition, aggregations=aggs)
        minute = values.get('by_minute') or {}
        buckets = minute.get('buckets')
        if not isinstance(buckets, list) or len(buckets) > MAX_TIME_BUCKETS:
            raise OpenSearchSourceError('Metrics time buckets unavailable or unbounded')
        times = []
        for bucket in buckets:
            if type(bucket.get('key')) is not int or type(bucket.get('doc_count')) is not int:
                raise OpenSearchSourceError('Invalid metrics time bucket')
            times.append((bucket['key'], bucket['doc_count']))

        def distribution(name):
            if name not in aggs:
                return {}, 0, False
            entry = values.get(name) or {}
            rows = entry.get('buckets')
            other = entry.get('sum_other_doc_count')
            if not isinstance(rows, list) or len(rows) > MAX_TERMS or type(other) is not int:
                raise OpenSearchSourceError('Metrics distribution unavailable')
            result = {}
            for row in rows:
                if type(row.get('key')) is not str or type(row.get('doc_count')) is not int:
                    raise OpenSearchSourceError('Invalid metrics distribution')
                result[row['key']] = row['doc_count']
            return result, other, entry.get('doc_count_error_upper_bound') == 0

        pods, pod_other, pods_exact = distribution('by_pod')
        containers, container_other, containers_exact = distribution('by_container')
        return WindowMetrics(total, tuple(times), pods, containers, pod_other, container_other,
                             pods_exact, containers_exact,
                             dict(start=start.isoformat(), end=end.isoformat(),
                                  namespace=run.definition.namespace, workload=run.definition.workload,
                                  container=run.definition.container,
                                  document_cluster_id=run.definition.document_cluster_id,
                                  index=index, pod_field=pod_field, container_field=container_field))
