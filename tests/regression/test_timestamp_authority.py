"""Occurrence-time authority, per-source timezone policy and acquisition fallback."""
from datetime import datetime, timezone
import json
from dataclasses import FrozenInstanceError, replace
from types import SimpleNamespace

import pytest

from test_timestamp_provenance import isolated, make_pipeline, quality_counts


HEADER = '2026-09-28 19:30:53,252'
EVENT_TIME = datetime(2026, 9, 28, 19, 30, 53, 252000, tzinfo=timezone.utc)
BUSINESS_LOG = (HEADER + ' INFO payload={"expiry_date":"2026-10-26 22:21:30+03:00",'
                '"activationDate":"2026-08-26T20:54:51.000+03:00"}')


@pytest.fixture(autouse=True)
def current_contracts(isolated, monkeypatch):
    # The ingestion import-isolation suite deliberately reloads its contracts.
    # Bind this harness to those current classes without changing production checks.
    import full_pipeline_v2
    import ingestion_layer.contracts as contracts
    monkeypatch.setattr(full_pipeline_v2, 'SourcePage', contracts.SourcePage)


def test_naive_header_is_not_rescued_by_embedded_business_time():
    from parser_layer.parser_pipeline import ParserPipeline
    event = ParserPipeline().process(BUSINESS_LOG)
    assert event['timestamp'] is None
    assert event['attributes']['raw_timestamp'] == HEADER
    # Timestamp authority must not change existing message/template identity.
    assert event['message'] == '"}'


def test_headerless_payload_date_is_not_event_time():
    from parser_layer.parsers.plain_text_parser import PlainTextParser
    assert PlainTextParser().parse('startup payload expiry_date=2026-10-26T22:21:30+03:00')['timestamp'] is None


@pytest.mark.parametrize('payload', [
    'payload={x=1 timestamp=2026-10-26T22:21:30+03:00}',
    'payload.timestamp=2026-10-26T22:21:30+03:00',
    'message=failure\n    timestamp=2026-10-26T22:21:30+03:00',
])
def test_kv_timestamp_must_be_a_top_level_header_field(payload):
    from parser_layer.parsers.kv_parser import KVParser
    event = KVParser().parse('level=ERROR ' + payload)
    assert event['timestamp'] is None
    assert event['raw_timestamp'] is None


def context(zone=None, **kwargs):
    from parser_layer.timestamp.source_policy import TimestampContext, TimestampSourcePolicy
    return TimestampContext(TimestampSourcePolicy(zone), **kwargs)


@pytest.mark.parametrize('raw,zone,expected,basis', [
    ('2026-09-28T19:30:53.252Z INFO failure', None, EVENT_TIME, 'message_explicit'),
    ('2026-09-28T22:30:53.252+03:00 INFO failure', 'UTC', EVENT_TIME, 'message_explicit'),
    (BUSINESS_LOG, 'UTC', EVENT_TIME, 'message_source_timezone'),
    (BUSINESS_LOG, 'Europe/Istanbul', EVENT_TIME.replace(hour=16), 'message_source_timezone'),
    (BUSINESS_LOG, None, None, 'untimed'),
    ('ERROR startup without time', 'UTC', None, 'untimed'),
    ('Sep 28 19:30:53 host app: ERROR failure', 'UTC', None, 'untimed'),
])
def test_authoritative_time_with_explicit_source_policy(raw, zone, expected, basis):
    from parser_layer.parser_pipeline import ParserPipeline
    event = ParserPipeline().process(raw, timestamp_context=context(zone))
    assert event['timestamp'] == expected
    assert event['timestamp_provenance']['basis'] == basis
    assert event['timestamp_provenance']['source_timezone'] == zone
    assert event['raw'] == raw
    if raw == BUSINESS_LOG:
        assert event['timestamp_provenance']['message_timestamp_raw'] == HEADER
        assert event['message'] == '"}'  # Preserve existing template input, even for this legacy span.


@pytest.mark.parametrize('field', ['expiry_date', 'start_date', 'activationDate', 'next_invoice_date', 'invoice_date', 'paidDate', 'timestamp'])
def test_nested_json_and_business_fields_are_never_promoted(field):
    from parser_layer.parser_pipeline import ParserPipeline
    data = {'level': 'ERROR', 'message': 'failure', 'payload': {field: '2026-09-28T22:00:00+03:00'}}
    if field != 'timestamp':
        data[field] = '2026-10-26T22:21:30+03:00'
    event = ParserPipeline().process(json.dumps(data), timestamp_context=context('UTC'))
    assert event['timestamp'] is None
    assert event['timestamp_provenance']['basis'] == 'untimed'
    assert event['attributes']['payload'] == data['payload']


@pytest.mark.parametrize('raw', [
    '{"timestamp":"2026-09-28T19:30:53.252Z","message":"failure"}',
    '{"@timestamp":"2026-09-28T22:30:53.252+03:00","message":"failure"}',
    'time=2026-09-28T19:30:53.252Z level=ERROR message=failure',
    'timestamp="2026-09-28 19:30:53,252" level=ERROR message=failure',
    '@timestamp=2026-09-28T22:30:53.252+03:00 level=ERROR message=failure',
])
def test_declared_json_and_kv_event_time_fields_work(raw):
    from parser_layer.parser_pipeline import ParserPipeline
    event = ParserPipeline().process(raw, timestamp_context=context('UTC'))
    assert event['timestamp'] == EVENT_TIME


def test_z_and_turkiye_offsets_are_the_same_instant():
    from parser_layer.timestamp.timestamp_normalizer import TimestampNormalizer
    normalizer = TimestampNormalizer()
    assert normalizer.normalize('2026-09-28T19:00:00Z') == normalizer.normalize('2026-09-28T22:00:00+03:00')


@pytest.mark.parametrize('zone,raw', [
    ('Europe/Berlin', '2026-10-25 02:30:00'),  # repeated local clock
    ('Europe/Berlin', '2026-03-29 02:30:00'),  # nonexistent local clock
    ('UTC', '2026-09-28'),                    # no occurrence clock
])
def test_source_policy_never_invents_missing_or_ambiguous_clock(zone, raw):
    from parser_layer.timestamp.source_policy import resolve_event_time
    assert resolve_event_time(raw, context(zone)).timestamp is None


def test_policy_is_immutable_and_naive_datetime_has_no_global_utc_default():
    from parser_layer.timestamp.source_policy import TimestampSourcePolicy, resolve_event_time
    policy = TimestampSourcePolicy('Europe/Istanbul')
    with pytest.raises(FrozenInstanceError):
        policy.source_timezone = 'UTC'
    assert resolve_event_time(datetime(2026, 9, 28, 19)).timestamp is None
    assert resolve_event_time(datetime(2026, 9, 28, 19), context('Europe/Istanbul')).timestamp == datetime(2026, 9, 28, 16, tzinfo=timezone.utc)


def source_record(raw, stamp='2026-09-28T19:32:11Z', record_id='1'):
    from ingestion_layer.contracts import Framing, IngestedLogRecord, SourceReference, StreamIdentity
    return IngestedLogRecord(raw, SourceReference('fixture', 'index', record_id),
        source_timestamp_raw=stamp, source_timestamp_field='@timestamp',
        stream_identity=StreamIdentity('fixture', pod_instance='pod', container_instance='container', channel='stdout'),
        first_observed_at=datetime(2040, 1, 1, tzinfo=timezone.utc), framing=Framing.PHYSICAL_LINE)


def process_records(pipeline, records, zone=None):
    from ingestion_layer.contracts import SourcePage
    from segmentation_layer.contracts import SegmentationPolicy
    return pipeline.process_ingested_pages([SourcePage(tuple(records), True)], source_timezone=zone,
        policy_provider=lambda *args: SegmentationPolicy('fixture', r'^\S', 'offline fixture'))


@pytest.mark.parametrize('message,zone,expected,basis', [
    ('ERROR startup without time', None, datetime(2026, 9, 28, 19, 32, 11, tzinfo=timezone.utc), 'source_record'),
    (BUSINESS_LOG, None, datetime(2026, 9, 28, 19, 32, 11, tzinfo=timezone.utc), 'source_record'),
    (BUSINESS_LOG, 'UTC', EVENT_TIME, 'message_source_timezone'),
    ('2026-09-28T22:30:53.252+03:00 ERROR failure', 'Europe/Istanbul', EVENT_TIME, 'message_explicit'),
])
def test_message_precedence_and_source_record_fallback(tmp_path, message, zone, expected, basis):
    result = process_records(make_pipeline(tmp_path), [source_record(message)], zone)
    parsed = result['pipeline_trace']['parser']['items'][0]
    templated = result['pipeline_trace']['template']['items'][0]
    assert parsed['timestamp'] == templated['timestamp'] == expected
    provenance = parsed['timestamp_provenance']
    assert provenance == templated['timestamp_provenance']
    assert provenance['basis'] == basis
    assert provenance['source_record_time'] == '2026-09-28T19:32:11+00:00'
    assert provenance['source_record_field'] == '@timestamp'
    assert result['signals'][0]['timestamp_basis_counts'] == {basis: 1}
    assert result['signals'][0]['first_seen_ms'] == int(expected.timestamp() * 1000)


def test_continuation_record_cannot_supply_event_fallback(tmp_path):
    result = process_records(make_pipeline(tmp_path), [
        source_record('ERROR startup without clock', None),
        source_record('    continuation payload time', record_id='2')], 'UTC')
    assert result['pipeline_trace']['parser']['items'][0]['timestamp'] is None
    assert result['signals'][0]['timestamp_resolved'] is False


def test_opensearch_mapping_preserves_fallback_field_and_utc_instant(tmp_path):
    from ingestion_layer.opensearch_source import OpenSearchSource
    from ingestion_layer.opensearch_config import OpenSearchFieldMapping
    from test_opensearch_source import hit
    mapper = OpenSearchSource.__new__(OpenSearchSource)
    mapper._config = SimpleNamespace(field_mapping=OpenSearchFieldMapping(), source_scope='fixture')
    data = hit(raw='ERROR startup without time')
    data['_source']['@timestamp'] = '2026-09-28T19:32:11Z'
    record = mapper._map_hit(data)
    assert record.source_timestamp_field == '@timestamp'
    result = process_records(make_pipeline(tmp_path), [record])
    event = result['pipeline_trace']['parser']['items'][0]
    assert event['timestamp'] == datetime(2026, 9, 28, 19, 32, 11, tzinfo=timezone.utc)
    assert event['timestamp_provenance']['basis'] == 'source_record'


def real_shapes(tmp_path):
    source = tmp_path / 'errors.log'
    source.write_text('\n'.join([
        'ERROR startup failure without clock',
        *['2026-09-28 19:31:56,641 [ERROR] [orders-service] ProgrammingError in chatbot_param_list'] * 5,
        '2026-09-28 19:32:11,408 [ERROR] [orders-service] call_soap_method failed',
        '2026-09-28 19:32:11,408 [ERROR] [orders-service] [BALANCE] FAILED',
    ]) + '\n', encoding='utf-8')
    return source


def test_file_policy_recovers_error_times_and_resets_per_analysis(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv('AIOPS_TIME_DEBUG', 'true')
    pipeline = make_pipeline(tmp_path)
    source = real_shapes(tmp_path)
    enabled = pipeline.process_file(str(source), source_timezone='UTC')
    counts = quality_counts(capsys.readouterr().out)
    assert counts['timestamp_basis_message_source_timezone'] == counts['naive_header_resolved_by_source_policy'] == 7
    assert counts['timestamp_basis_untimed'] == 1
    assert counts['qualified_signals_timed'] == 3
    assert counts['qualified_signals_untimed'] == 1
    assert counts['parsed_to_downstream_timestamp_lost'] == counts['parsed_to_downstream_timestamp_changed'] == 0
    events = enabled['pipeline_trace']['template']['items']
    assert [e['timestamp'] for e in events] == [None,
        *[datetime(2026, 9, 28, 19, 31, 56, 641000, tzinfo=timezone.utc)] * 5,
        datetime(2026, 9, 28, 19, 32, 11, 408000, tzinfo=timezone.utc),
        datetime(2026, 9, 28, 19, 32, 11, 408000, tzinfo=timezone.utc)]
    other = pipeline.process_file(str(source), source_timezone='Europe/Istanbul')
    unknown = pipeline.process_file(str(source))
    assert other['pipeline_trace']['template']['items'][1]['timestamp'].hour == 16
    assert all(e['timestamp'] is None for e in unknown['pipeline_trace']['template']['items'])
    assert [e['template_id'] for e in unknown['pipeline_trace']['template']['items']] == [e['template_id'] for e in events]


def test_same_policy_is_deterministic_across_clocks_and_restart(tmp_path, monkeypatch):
    import parser_layer.canonical_event_builder as canonical
    from test_downstream_determinism import decisions
    source = real_shapes(tmp_path)
    pipeline = make_pipeline(tmp_path)
    outputs = []
    for year in (2026, 2030, 2040):
        monkeypatch.setattr(canonical, 'datetime', SimpleNamespace(now=lambda tz: datetime(year, 1, 1, tzinfo=timezone.utc)))
        if year == 2040:
            pipeline = make_pipeline(tmp_path)  # reload the same temporary learning state
        outputs.append(pipeline.process_file(str(source), source_timezone='UTC'))
    assert decisions(outputs[0]) == decisions(outputs[1]) == decisions(outputs[2])
    assert all('nedensel_zaman_sırası' not in edge['evidence'] for result in outputs for edge in result['correlations'] if edge['time_gap_ms'] == 0)


def test_invalid_source_zone_fails_before_file_or_learning_access(tmp_path):
    pipeline = make_pipeline(tmp_path)
    with pytest.raises(ValueError, match='valid IANA timezone'):
        pipeline.process_file(str(tmp_path / 'does-not-exist.log'), source_timezone='not/a/zone')


def test_upload_timezone_is_forwarded_per_request(tmp_path, monkeypatch):
    from pathlib import Path
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[2] / 'src' / 'frontend'))
    import analysis_runtime
    calls = []
    pipeline = make_pipeline(tmp_path)
    original = pipeline.process_file
    def run(path, **kwargs):
        calls.append(kwargs)
        return original(path, **kwargs)
    monkeypatch.setattr(pipeline, 'process_file', run)
    monkeypatch.setattr(analysis_runtime, 'get_pipeline', lambda: pipeline)
    upload = SimpleNamespace(name='source.log', getvalue=lambda: (HEADER + ' ERROR failure\n').encode())
    first = analysis_runtime.run_uploaded(upload, source_timezone='UTC')
    second = analysis_runtime.run_uploaded(upload)
    assert calls == [{'source_timezone': 'UTC'}, {'source_timezone': None}]
    assert first['signals'][0]['timestamp_resolved'] is True
    assert second['signals'][0]['timestamp_resolved'] is False


def test_time_quality_basis_counts_are_safe_and_complete(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv('AIOPS_TIME_DEBUG', 'true')
    rows = [source_record('2026-09-28T19:00:00Z ERROR failure', None, '1'),
            source_record(HEADER + ' ERROR failure', None, '2'),
            source_record('ERROR source fallback', record_id='3'),
            source_record('ERROR startup Authorization: Bearer private-fixture-secret', None, '4'),
            source_record('{"timestamp":"2026-09-28 19:00:00","message":"failure"}', None, '5')]
    process_records(make_pipeline(tmp_path), rows, 'UTC')
    output = capsys.readouterr().out
    counts = quality_counts(output)
    assert {basis: counts['timestamp_basis_' + basis] for basis in (
        'message_explicit', 'message_source_timezone', 'source_record', 'untimed')} == {
            'message_explicit': 1, 'message_source_timezone': 2, 'source_record': 1, 'untimed': 1}
    assert counts['naive_header_resolved_by_source_policy'] == 1
    assert counts['parsed_timestamp_resolved'] == counts['downstream_timestamp_resolved'] == 4
    assert counts['parsed_timestamp_unresolved'] == 1
    assert 'Authorization' not in output and 'private-fixture-secret' not in output


def test_unresolvable_source_policy_is_reported_without_inventing_time():
    from parser_layer.parser_pipeline import ParserPipeline
    event = ParserPipeline().process('2026-10-25 02:30:00 ERROR failure',
                                     timestamp_context=context('Europe/Berlin'))
    assert event['timestamp'] is None
    assert event['attributes']['timestamp_status'] == 'source_timezone_unresolved'
