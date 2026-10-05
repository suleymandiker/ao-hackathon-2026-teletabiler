"""Opt-in, request-local counters. Never print source values or retain events."""
from collections import Counter
import os

from analysis_time import signal_time, source_time_ms
from parser_layer.timestamp.timestamp_evidence import unresolved_timestamp_evidence
from parser_layer.timestamp.source_policy import BASES


class TimeQuality:
    # Output labels are code-owned, including when callers supply arbitrary metadata.
    PARSERS = frozenset(('json', 'syslog', 'kv', 'structured_text',
                         'positional_structured', 'plain_text', 'custom',
                         'safe_fallback', 'structured_alarm', 'unavailable'))
    STATUSES = ('resolved', 'missing', 'timezone_missing', 'year_missing',
                'date_missing', 'unparsed', 'invalid', 'unknown', 'source_timezone_unresolved')

    def __init__(self):
        self.counts = dict.fromkeys((
            'events_total', 'source_timestamp_present', 'source_timestamp_not_detected',
            'parsed_events_total', 'parser_no_event',
            'parsed_timestamp_resolved', 'parsed_timestamp_unresolved',
            'downstream_events_total', 'downstream_timestamp_resolved',
            'downstream_timestamp_unresolved', 'parsed_events_not_delivered',
            'parsed_resolved_not_delivered', 'parsed_to_downstream_timestamp_lost',
            'parsed_to_downstream_timestamp_changed',
        ), 0)
        self.statuses = dict.fromkeys(self.STATUSES, 0)
        self.by_parser = Counter()
        self.bases = dict.fromkeys(BASES, 0)
        self.naive_headers_resolved = 0

    @classmethod
    def from_env(cls):
        return cls() if os.getenv('AIOPS_TIME_DEBUG', '').strip().lower() in (
            '1', 'true', 'yes', 'on',
        ) else None

    def parsed(self, event, parser_id='unavailable', *, structured=False):
        """Observe the existing canonical outcome, without reparsing its log body.

        Presence means a resolved canonical value or detected timestamp evidence;
        absence of detection is not proof that an arbitrary format contains no time.
        Structured alarms bypass parsing and supply their timestamp field directly.
        """
        counts = self.counts
        counts['events_total'] += 1
        if event is None:
            counts['parser_no_event'] += 1
            return None
        counts['parsed_events_total'] += 1
        timestamp = source_time_ms(event.get('timestamp'))
        provenance = event.get('timestamp_provenance') or {}
        basis = provenance.get('basis')
        if basis in self.bases:
            self.bases[basis] += 1
        if basis == 'message_source_timezone' and parser_id in (
            'plain_text', 'structured_text', 'positional_structured', 'syslog', 'custom',
        ):
            self.naive_headers_resolved += 1
        evidence = (unresolved_timestamp_evidence('', provenance.get('message_timestamp_raw', event.get('timestamp')))
                    if structured else event.get('attributes') or {})
        raw = provenance.get('message_timestamp_raw', evidence.get('raw_timestamp'))
        present = timestamp is not None or (raw is not None and raw != '')
        counts['source_timestamp_present' if present else 'source_timestamp_not_detected'] += 1
        resolved = timestamp is not None
        counts['parsed_timestamp_resolved' if resolved else 'parsed_timestamp_unresolved'] += 1
        status = 'resolved' if resolved else evidence.get('timestamp_status', 'unknown')
        if status not in self.STATUSES:
            status = 'unknown'
        if parser_id not in self.PARSERS:
            parser_id = 'unavailable'
        self.statuses[status] += 1
        self.by_parser[(parser_id, status)] += 1
        return timestamp

    def not_delivered(self, parsed_time):
        self.counts['parsed_events_not_delivered'] += 1
        self.counts['parsed_resolved_not_delivered'] += int(parsed_time is not None)

    def downstream(self, event, parsed_time):
        """Observe the exact templated row supplied to downstream aggregation."""
        timestamp = source_time_ms(event.get('timestamp'))
        self.counts['downstream_events_total'] += 1
        self.counts['downstream_timestamp_resolved' if timestamp is not None
                    else 'downstream_timestamp_unresolved'] += 1
        self.counts['parsed_to_downstream_timestamp_lost'] += int(
            parsed_time is not None and timestamp is None)
        self.counts['parsed_to_downstream_timestamp_changed'] += int(timestamp != parsed_time)

    def report(self, result):
        counts = dict(self.counts)
        for prefix, key in (('signal_candidates', 'signals'), ('qualified_signals', 'qualified_signals')):
            signals = result.get(key, ())
            timed = sum(signal_time(s) is not None and signal_time(s, 'last_seen_ms') is not None
                        for s in signals)
            counts[prefix + '_timed'] = timed
            counts[prefix + '_untimed'] = len(signals) - timed
        # A single bounded print contains integers and allowlisted labels only.
        lines = ['[TIME QUALITY]']
        lines.extend(f'{key}={value}' for key, value in counts.items())
        lines.extend(f'timestamp_status_{key}={value}' for key, value in self.statuses.items())
        lines.extend(f'timestamp_basis_{key}={value}' for key, value in self.bases.items())
        lines.append(f'naive_header_resolved_by_source_policy={self.naive_headers_resolved}')
        lines.extend(f'parser_{parser}_{status}={count}'
                     for (parser, status), count in sorted(self.by_parser.items()))
        print('\n'.join(lines))
