"""Factual event time, independently of discovery, observation clocks and network."""
from datetime import datetime, timezone
import json
from pathlib import Path
import socket
import sqlite3
from types import SimpleNamespace

import pytest


UTC_TIME = datetime(2026, 10, 5, 9, 10, 11, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def isolated(monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[2] / 'src' / 'backend'))
    monkeypatch.delenv('AIOPS_TIME_DEBUG', raising=False)
    monkeypatch.setenv('RCA_DEBUG', 'false')

    def forbidden(*args, **kwargs):
        pytest.fail('No network, LLM, discovery or production SQLite in time-quality tests')

    import requests
    import ai_engine
    import rca_layer.rca_engine as rca
    import parser_layer.parser_pipeline as parser
    for owner, names in (
        (socket.socket, ('connect', 'connect_ex')),
        (socket, ('create_connection', 'getaddrinfo')),
        (requests.sessions.Session, ('request', 'send')),
        (sqlite3, ('connect',)), (sqlite3.dbapi2, ('connect',)),
    ):
        for name in names:
            monkeypatch.setattr(owner, name, forbidden)
    monkeypatch.setattr(ai_engine, 'call_ai_agent', forbidden)
    monkeypatch.setattr(rca, 'call_ai_agent', forbidden)
    # Replace persistent dependencies BEFORE constructing ParserPipeline.
    monkeypatch.setattr(parser, 'ParserPolicyRegistry', lambda *a, **kw: SimpleNamespace())
    monkeypatch.setattr(parser, 'ParserPolicyDiscovery', lambda: SimpleNamespace())
    monkeypatch.setattr(parser.ParserPipeline, 'prepare', forbidden)


@pytest.mark.parametrize('stamp', [
    '2026-10-05 09:10:11Z',
    '2026-10-05 12:10:11+03:00',
    '2026-10-05 12:10:11+0300',
    '2026-10-05T12:10:11 +03:00',
])
def test_header_extraction_keeps_the_explicit_timezone(stamp):
    from parser_layer.timestamp.timestamp_normalizer import TimestampNormalizer
    from parser_layer.parser_pipeline import ParserPipeline
    normalizer = TimestampNormalizer()
    # The normalizer already supports these values; extraction must not truncate them.
    assert normalizer.normalize(stamp) == UTC_TIME
    assert normalizer.extract(stamp + ' ERROR database failure') == (UTC_TIME, stamp)
    assert ParserPipeline().process(stamp + ' ERROR database failure')['timestamp'] == UTC_TIME


@pytest.mark.parametrize('suffix', ['ZooKeeper failure', 'Zone failure', '+03:00suffix failure'])
def test_header_extraction_does_not_promote_message_prefix_to_timezone(suffix):
    from parser_layer.timestamp.timestamp_normalizer import TimestampNormalizer
    stamp = '2026-10-05 12:10:11'
    assert TimestampNormalizer().extract(stamp + ' ' + suffix) == (None, stamp)


@pytest.mark.parametrize('key', ['timestamp', 'time', '@timestamp'])
def test_json_epoch_zero_is_factual_source_time(key):
    from parser_layer.parser_pipeline import ParserPipeline
    event = ParserPipeline().process(json.dumps({key: 0, 'level': 'ERROR', 'message': 'failure'}))
    assert event['timestamp'] == datetime(1970, 1, 1, tzinfo=timezone.utc)


@pytest.mark.parametrize('raw,expected_raw,status', [
    ('ERROR 2026-10-05 12:10:11 database failure', '2026-10-05 12:10:11', 'timezone_missing'),
    ('25/10/2026 12:10:11 ERROR database failure', '25/10/2026 12:10:11', 'timezone_missing'),
    ('[Sun Dec 04 04:47:44 2005] [error] database failure', 'Sun Dec 04 04:47:44 2005', 'timezone_missing'),
    ('level=ERROR time="2026-10-05 12:10:11" message="database failure"', '2026-10-05 12:10:11', 'timezone_missing'),
    ('ERROR 10-05 12:10:11 [worker] database failure', '10-05 12:10:11', 'year_missing'),
])
def test_unresolved_timestamp_keeps_parser_evidence(raw, expected_raw, status):
    from parser_layer.parser_pipeline import ParserPipeline
    event = ParserPipeline().process(raw)
    assert event['timestamp'] is None
    assert event['attributes']['raw_timestamp'] == expected_raw
    assert event['attributes']['timestamp_status'] == status


def test_policy_timestamp_group_is_preserved_as_unresolved_evidence():
    from parser_layer.parser_pipeline import ParserPipeline
    from parser_layer.parsers.policy_parser import PolicyParser
    parser = ParserPipeline()
    parser.custom_parser = PolicyParser({
        'regex': r'^CUSTOM clock:(?P<ts>\d{8}-\d+:\d+:\d+:\d+) (?P<body>.*)$',
        'timestamp_group': 'ts', 'message_group': 'body',
    })
    outcome = parser.process_with_outcome('CUSTOM clock:20261005-12:10:11:123 database failure')
    assert outcome.parser_id == 'custom'
    assert outcome.event['timestamp'] is None
    assert outcome.event['attributes']['raw_timestamp'] == '20261005-12:10:11:123'
    assert outcome.event['attributes']['timestamp_status'] == 'timezone_missing'


def test_positional_timestamp_stays_source_local_with_evidence():
    from parser_layer.parser_pipeline import ParserPipeline
    raw = '- 1 2026.10.05 host-1 2026-10-05-12.10.11.123 host-1 APP worker ERROR failure'
    outcome = ParserPipeline().process_with_outcome(raw)
    assert outcome.parser_id == 'positional_structured'
    assert outcome.event['timestamp'] is None
    assert outcome.event['attributes']['raw_timestamp'] == '2026-10-05-12.10.11.123'
    assert outcome.event['attributes']['timestamp_status'] == 'timezone_missing'


@pytest.mark.parametrize('raw,parser_id,expected,status', [
    ('2026-10-05T09:10:11Z ERROR failure', 'structured_text', UTC_TIME, None),
    ('2026-10-05 12:10:11 +03:00 ERROR failure', 'structured_text', UTC_TIME, None),
    ('2026-10-05T05:10:11-0400 ERROR failure', 'structured_text', UTC_TIME, None),
    ('2026-10-05 12:10:11,123+03:00 ERROR failure', 'structured_text', UTC_TIME.replace(microsecond=123000), None),
    ('ERROR 2026-10-05T12:10:11+03:00 failure', 'structured_text', UTC_TIME, None),
    ('2026-10-05 12:10:11 ERROR failure', 'structured_text', None, 'timezone_missing'),
    ('2026-10-05T12:10:11 ERROR failure', 'structured_text', None, 'timezone_missing'),
    ('25/10/2026 12:10:11 ERROR failure', 'structured_text', None, 'timezone_missing'),
    ('10/05/2026 12:10:11 ERROR failure', 'structured_text', None, 'timezone_missing'),
    ('Oct  5 12:10:11 host app: ERROR failure', 'syslog', None, 'year_missing'),
    ('<34>1 2026-10-05T12:10:11+03:00 host app 1 ID - failure', 'syslog', UTC_TIME, None),
    ('192.0.2.1 - - [05/Oct/2026:12:10:11 +0300] "GET / HTTP/1.1" 500 1', 'structured_text', UTC_TIME, None),
    ('time=2026-10-05T12:10:11+03:00 level=ERROR message=failure', 'kv', UTC_TIME, None),
    ('{"time":1791191411000,"level":"ERROR","message":"failure"}', 'json', UTC_TIME, None),
    ('ERROR: failure without a clock', 'plain_text', None, 'missing'),
    ('2026-99-05T12:10:11+03:00 ERROR failure', 'structured_text', None, 'invalid'),
])
def test_physical_line_to_logical_to_canonical_time(tmp_path, raw, parser_id, expected, status):
    from segmentation_layer.multiline_assembler import MultilineAssembler
    from segmentation_layer.segmentation_pipeline import DEFAULT_FALLBACK_REGEX
    from parser_layer.parser_pipeline import ParserPipeline
    from parser_layer.contracts import Delivery
    source = tmp_path / 'source.log'
    source.write_bytes((raw + '\r\n').encode())
    records = list(MultilineAssembler().iter_event_records(str(source), DEFAULT_FALLBACK_REGEX))
    assert len(records) == 1
    assert records[0]['event'] == raw
    outcome = ParserPipeline().process_with_outcome(records[0]['event'])
    assert outcome.parser_id == parser_id
    assert outcome.delivery is Delivery.PARSED
    assert outcome.event['raw'] == raw
    assert outcome.event['timestamp'] == expected
    if status:
        assert outcome.event['attributes']['timestamp_status'] == status


def make_pipeline(tmp_path):
    from full_pipeline_v2 import FullAIOpsPipelineV2
    from parser_layer.parser_pipeline import ParserPipeline
    from segmentation_layer.multiline_assembler import MultilineAssembler
    from segmentation_layer.segmentation_pipeline import DEFAULT_FALLBACK_REGEX
    from template_layer.template_pipeline import TemplatePipeline
    from downstream_pipeline import DownstreamAIOpsPipeline
    pipeline = FullAIOpsPipelineV2.__new__(FullAIOpsPipelineV2)
    pipeline.parser = ParserPipeline()
    pipeline.segmenter = SimpleNamespace(iter_events=lambda path:
        MultilineAssembler().iter_events(path, DEFAULT_FALLBACK_REGEX))
    pipeline.templater = TemplatePipeline(state_path=tmp_path / 'templates.json',
                                         candidate_state_path=tmp_path / 'drain.bin')
    pipeline.downstream = DownstreamAIOpsPipeline(use_ai_rca=False)
    return pipeline


def mixed_file(tmp_path):
    source = tmp_path / 'mixed.log'
    rows = [
        '2026-10-05 12:10:11+03:00 ERROR [orders-service] database failure\n    trace detail',
        '2026-10-05 12:10:12+03:00 ERROR [orders-service] connection refused',
        '2026-10-05 12:10:13 ERROR source local failure',
        'ERROR: isolated failure',
    ]
    source.write_text('\n'.join(rows * 2) + '\n', encoding='utf-8')
    return source


def quality_counts(output):
    block = output.split('[TIME QUALITY]\n', 1)[1]
    return {key: int(value) for line in block.splitlines() if '=' in line and not line.startswith('[')
            for key, value in [line.split('=', 1)]}


def test_mixed_analysis_preserves_time_into_signals_and_rca_windows(tmp_path, monkeypatch, capsys):
    from rca_layer.evidence import RCAEvidenceSelector, EvidenceLimits
    monkeypatch.setenv('AIOPS_TIME_DEBUG', 'true')
    result = make_pipeline(tmp_path).process_file(str(mixed_file(tmp_path)))
    counts = quality_counts(capsys.readouterr().out)
    for key, expected in {
        'events_total': 8, 'source_timestamp_present': 6, 'source_timestamp_not_detected': 2,
        'parsed_events_total': 8, 'parsed_timestamp_resolved': 4, 'parsed_timestamp_unresolved': 4,
        'downstream_events_total': 8, 'downstream_timestamp_resolved': 4,
        'downstream_timestamp_unresolved': 4, 'parsed_to_downstream_timestamp_lost': 0,
        'parsed_to_downstream_timestamp_changed': 0, 'parsed_events_not_delivered': 0,
        'qualified_signals_timed': 2, 'qualified_signals_untimed': 2,
        'parser_structured_text_resolved': 4, 'parser_structured_text_timezone_missing': 2,
        'parser_plain_text_missing': 2,
    }.items():
        assert counts[key] == expected
    parsed = result['pipeline_trace']['parser']['items']
    templated = result['pipeline_trace']['template']['items']
    assert [e['timestamp'] for e in parsed] == [e['timestamp'] for e in templated]
    assert parsed[0]['raw'].splitlines()[1] == '    trace detail'
    assert parsed[0]['timestamp'] == UTC_TIME
    timed = [s for s in result['qualified_signals'] if s['timestamp_resolved']]
    untimed = [s for s in result['qualified_signals'] if not s['timestamp_resolved']]
    expected_start = int(UTC_TIME.timestamp()) * 1000
    assert timed[0]['first_seen_ms'] == expected_start
    assert all(s['window_start_ms'] == expected_start - 11000 for s in timed)
    assert all(s['window_end_ms'] == expected_start + 49000 for s in timed)
    assert all(s['first_seen_ms'] is s['last_seen_ms'] is s['window_start_ms'] is s['window_end_ms'] is None for s in untimed)
    # Existing same-service temporal rule is now satisfiable by two factual times.
    assert len(result['correlations']) == 1
    edge = result['correlations'][0]
    assert edge['time_gap_ms'] == 1000
    assert edge['score'] == .58
    assert {edge['source'], edge['target']} == {s['signal_id'] for s in timed}
    pack = RCAEvidenceSelector(EvidenceLimits()).build(result['incidents'], result['correlations'], result['signals'], result['rca'])
    assert sorted(s['signal_windows'] for s in json.loads(pack.serialized)['signals']) == [0, 0, 1, 1]


@pytest.mark.parametrize('flag', [None, 'false', '0', 'no', 'off', 'unexpected'])
def test_debug_absent_by_default_and_false_values(tmp_path, monkeypatch, capsys, flag):
    if flag is not None:
        monkeypatch.setenv('AIOPS_TIME_DEBUG', flag)
    make_pipeline(tmp_path).process_file(str(mixed_file(tmp_path)))
    output = capsys.readouterr().out
    assert '[PIPELINE]' in output
    assert '[TIME QUALITY]' not in output


@pytest.mark.parametrize('flag', ['1', 'true', 'YES', ' On '])
def test_debug_is_observational_and_counters_do_not_leak(tmp_path, monkeypatch, capsys, flag):
    import parser_layer.canonical_event_builder as canonical
    pipeline = make_pipeline(tmp_path)
    source = mixed_file(tmp_path)
    observed = datetime(2035, 1, 1, tzinfo=timezone.utc)
    monkeypatch.setattr(canonical, 'datetime', SimpleNamespace(now=lambda tz: observed))
    baseline = pipeline.process_file(str(source))
    baseline_log = capsys.readouterr().out
    # Reset only the fixture's canonical counter for exact payload comparison.
    pipeline.parser.builder._counter = 0
    monkeypatch.setenv('AIOPS_TIME_DEBUG', flag)
    enabled = pipeline.process_file(str(source))
    enabled_log = capsys.readouterr().out
    for key in ('analysis_time', 'signals', 'qualified_signals', 'correlations', 'incidents', 'rca', 'plans', 'stats'):
        assert enabled[key] == baseline[key]
    assert [line for line in enabled_log.splitlines() if line.startswith('[PIPELINE]')] == baseline_log.splitlines()
    assert quality_counts(enabled_log)['events_total'] == 8
    # Same cached instance, then empty analysis, then the same file under a new clock.
    pipeline._process_logical_events(iter(()))
    assert quality_counts(capsys.readouterr().out)['events_total'] == 0
    monkeypatch.setattr(canonical, 'datetime', SimpleNamespace(now=lambda tz: datetime(2040, 1, 1, tzinfo=timezone.utc)))
    repeated = pipeline.process_file(str(source))
    assert quality_counts(capsys.readouterr().out) == quality_counts(enabled_log)
    assert repeated['analysis_time'] == baseline['analysis_time']
    assert repeated['correlations'] == baseline['correlations']
    assert [s['signal_id'] for s in repeated['signals']] == [s['signal_id'] for s in baseline['signals']]


def test_diagnostics_never_print_source_values_or_credentials(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv('AIOPS_TIME_DEBUG', 'true')
    monkeypatch.setenv('SAKA_API_KEY', 'fixture-key-never-print')
    monkeypatch.setenv('PRIVATE_ENV_FIXTURE', 'fixture-env-never-print')
    raw = json.dumps({'time': 'Authorization: Bearer fixture-private-token', 'level': 'ERROR',
                      'message': 'fixture-full-sensitive-body password=fixture-password'})
    make_pipeline(tmp_path)._process_logical_events(iter([raw]))
    output = capsys.readouterr().out
    assert quality_counts(output)['timestamp_status_invalid'] == 1
    for secret in ('Authorization', 'Bearer', 'fixture-private-token', 'fixture-key-never-print',
                   'fixture-full-sensitive-body', 'fixture-password', 'fixture-env-never-print'):
        assert secret not in output


def test_structured_fast_path_reports_source_field_without_parsing(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv('AIOPS_TIME_DEBUG', 'true')
    pipeline = make_pipeline(tmp_path)
    def forbidden(*a, **kw):
        pytest.fail('Structured alarms must bypass text parsing')
    monkeypatch.setattr(pipeline.parser, 'process', forbidden)
    monkeypatch.setattr(pipeline.parser, 'process_with_outcome', forbidden)
    result = pipeline.process_structured_alarms([
        dict(alarm_id=str(index), timestamp=value, service='db', alarm_type='disk_full',
             severity='CRITICAL', message='failure') for index, value in enumerate([
                 UTC_TIME.isoformat(), None, '2026-10-05 12:10:11'])])
    counts = quality_counts(capsys.readouterr().out)
    assert counts['events_total'] == 3
    assert counts['source_timestamp_present'] == 2
    assert counts['downstream_timestamp_resolved'] == 1
    assert counts['downstream_timestamp_unresolved'] == 2
    assert counts['parser_structured_alarm_timezone_missing'] == 1
    assert result['stats']['structured_alarm_fast_path'] is True


def test_diagnostics_identify_dropped_and_changed_times(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv('AIOPS_TIME_DEBUG', 'true')
    pipeline = make_pipeline(tmp_path)
    real_process = pipeline.templater.process
    def lose_time(event):
        event['timestamp'] = None
        return real_process(event)
    monkeypatch.setattr(pipeline.templater, 'process', lose_time)
    pipeline._process_logical_events(['2026-10-05T09:10:11Z ERROR failure'])
    counts = quality_counts(capsys.readouterr().out)
    assert counts['parsed_to_downstream_timestamp_lost'] == 1
    assert counts['parsed_to_downstream_timestamp_changed'] == 1
    monkeypatch.setattr(pipeline.templater, 'process', lambda event: None)
    pipeline._process_logical_events(['2026-10-05T09:10:11Z ERROR failure'])
    counts = quality_counts(capsys.readouterr().out)
    assert counts['parsed_resolved_not_delivered'] == 1
    assert counts['downstream_events_total'] == 0
