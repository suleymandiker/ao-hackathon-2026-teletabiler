"""UTC daily-index selection with fake transport and no external state."""
from dataclasses import replace
from datetime import datetime
import time

import pytest

from test_opensearch_source import isolated_backend, api, config, source_for, page, hit


def instant(value):
    return datetime.fromisoformat(value.replace('Z', '+00:00'))


def test_daily_profile_does_not_query_historical_wildcard(api, config):
    # Explicit source strategy attestation; source must not send the broad
    # historical wildcard when this profile declares UTC daily indices.
    configured = replace(config, index_expression='synthetic-cluster*', index_strategy='daily_utc')
    source, session = source_for(api, configured, page())
    source.read_page(start=instant('2026-10-05T21:15Z'), end=instant('2026-10-05T21:30Z'),
                     namespace='ns', workload='app')
    assert session.calls[0][0].endswith('/synthetic-cluster-2026.10.05/_search')
    assert '*' not in session.calls[0][0] and '2026.07.' not in session.calls[0][0]


@pytest.mark.parametrize('start,end,dates', [
    ('2026-10-05T10:00Z', '2026-10-05T10:15Z', ['2026.10.05']),
    ('2026-10-05T23:55Z', '2026-10-06T00:10Z', ['2026.10.05', '2026.10.06']),
    ('2026-10-05T23:45Z', '2026-10-06T00:00Z', ['2026.10.05']),
    ('2026-10-05T23:59Z', '2026-10-06T00:15Z', ['2026.10.05', '2026.10.06']),
    ('2026-10-05T23:59Z', '2026-10-06T00:00:00.000001Z', ['2026.10.05', '2026.10.06']),
    ('2026-10-05T00:00Z', '2026-10-08T00:00Z', ['2026.10.05', '2026.10.06', '2026.10.07']),
    ('2026-12-31T23:59Z', '2027-01-01T00:01Z', ['2026.12.31', '2027.01.01']),
    ('2024-02-28T23:59Z', '2024-03-01T00:00Z', ['2024.02.28', '2024.02.29']),
    ('2026-10-06T02:55+03:00', '2026-10-06T03:10+03:00', ['2026.10.05', '2026.10.06']),
])
def test_daily_utc_half_open_bounds(start, end, dates):
    from ingestion_layer.opensearch_indices import resolve_index_expression
    assert resolve_index_expression('cluster*', instant(start), instant(end), strategy='daily_utc') == ','.join(
        'cluster-' + day for day in dates)


@pytest.mark.parametrize('zone', ['UTC0', 'GMT-3', 'GMT+8'])
def test_daily_resolution_does_not_use_machine_timezone(monkeypatch, zone):
    from ingestion_layer.opensearch_indices import resolve_index_expression
    try:
        with monkeypatch.context() as context:
            context.setenv('TZ', zone)
            if hasattr(time, 'tzset'):
                time.tzset()
            context.setattr(time, 'localtime', lambda *a: pytest.fail('Machine-local time must not be consulted'))
            assert resolve_index_expression('cluster*', instant('2026-10-06T00:00+03:00'),
                                            instant('2026-10-06T00:15+03:00'), strategy='daily_utc') == 'cluster-2026.10.05'
    finally:
        if hasattr(time, 'tzset'):
            time.tzset()


@pytest.mark.parametrize('expression', ['*', 'logs?', 'logs*,other*', 'logs', 'logs-2026.10.05', 'log*s*', 'logs*/secret'])
def test_daily_strategy_rejects_ambiguous_profiles_without_echoing_them(config, expression):
    with pytest.raises(ValueError, match='one explicit') as caught:
        replace(config, index_expression=expression, index_strategy='daily_utc')
    assert expression not in str(caught.value) or expression == '*'


def test_unknown_strategy_is_not_silently_treated_as_literal(config):
    with pytest.raises(ValueError, match='literal or daily_utc') as caught:
        replace(config, index_strategy='private-invalid-value')
    assert 'private-invalid-value' not in str(caught.value)


@pytest.mark.parametrize('start,end', [('2026-10-05T10:00', '2026-10-05T10:15Z'),
                                     ('2026-10-05T10:00Z', '2026-10-05T10:00Z'),
                                     ('2026-10-05T10:15Z', '2026-10-05T10:00Z')])
def test_invalid_acquisition_bounds_fail(start, end):
    from ingestion_layer.opensearch_indices import resolve_index_expression
    with pytest.raises(ValueError):
        resolve_index_expression('cluster*', instant(start), instant(end), strategy='daily_utc')


def test_other_profiles_keep_literal_wildcards_and_aliases(api, config):
    assert config.index_strategy == 'literal'
    source, session = source_for(api, replace(config, index_expression='arbitrary*,alias'), page())
    source.read_page(start=instant('2026-10-05T23:55Z'), end=instant('2026-10-06T00:10Z'), namespace='ns', workload='app')
    assert session.calls[0][0].endswith('/arbitrary*,alias/_search')


def test_daily_targets_preserve_pagination_order_and_source_reference(api, config):
    configured = replace(config, index_expression='cluster*', index_strategy='daily_utc')
    first = hit('one', 10)
    second = hit('two', 11)
    first['_index'], second['_index'] = 'cluster-2026.10.05', 'cluster-2026.10.06'
    source, session = source_for(api, configured, page(first), page(second), page(first))
    selection = dict(start=instant('2026-10-05T23:55Z'), end=instant('2026-10-06T00:10Z'),
                     namespace='synthetic-ns', workload='synthetic-app', page_size=1)
    one = source.read_page(**selection)
    two = source.read_page(**selection, cursor=one.next_cursor)
    repeated = source.read_page(**selection)
    assert one.records == repeated.records
    assert one.records[0].source_reference.source_partition == 'cluster-2026.10.05'
    assert two.records[0].source_reference.source_partition == 'cluster-2026.10.06'
    assert session.calls[1][1]['json']['search_after'] == first['sort']
    for url, request in session.calls:
        assert url.endswith('/cluster-2026.10.05,cluster-2026.10.06/_search')
        assert request['json']['sort'] == [{'@timestamp': 'asc'}, {'openshift.sequence': 'asc'}]
        assert 'unmapped_type' not in str(request['json'])
    # A change in strategy/target cannot reuse a daily-index cursor.
    broad, unused = source_for(api, replace(config, index_expression='cluster*'))
    with pytest.raises(api.source.OpenSearchSourceError, match='cursor'):
        broad.read_page(**selection, cursor=one.next_cursor)
    assert not unused.calls


def test_relevant_daily_shard_failure_is_still_fatal(api, config):
    configured = replace(config, index_expression='cluster*', index_strategy='daily_utc')
    failed = page()
    failed['_shards'] = {'failed': 1, 'failures': [{'reason': 'No mapping found for [@timestamp]'}]}
    source, session = source_for(api, configured, failed)
    with pytest.raises(api.source.OpenSearchSourceError, match='shard failure'):
        source.read_page(start=instant('2026-10-05T10:00Z'), end=instant('2026-10-05T10:15Z'), namespace='ns', workload='app')
    assert session.calls[0][0].endswith('/cluster-2026.10.05/_search')
