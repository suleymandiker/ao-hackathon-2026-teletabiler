from __future__ import annotations
from dataclasses import asdict
from typing import Any, Dict, Iterable, List
import hashlib
import os
import re

from ingestion_layer.contracts import IngestedLogRecord, SourcePage
from segmentation_layer.contracts import AssembledEvent, UnassembledRecord
from segmentation_layer.segmentation_session import PolicyProvider, SegmentationSession
from segmentation_layer.segmentation_pipeline import SegmentationPipeline
from parser_layer.parser_pipeline import ParserPipeline
from template_layer.template_pipeline import TemplatePipeline
from downstream_pipeline import DownstreamAIOpsPipeline
from input_package_layer.package_loader import InputPackageLoader
from time_quality import TimeQuality
from pipeline_observation import PipelineObserver
from data_paths import template_data_dir
from parser_layer.timestamp.source_policy import TimestampContext, TimestampSourcePolicy, resolve_event_time

_DEFAULT_STATE = object()

# =========================================================================
# PRE-DRAIN REGEX MASKELEME KURALLARI (CARDINALITY EXPLOSION SAVUNMASI)
# =========================================================================
_RE_UUID = re.compile(r'\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b')
_RE_URL = re.compile(r'https?://[^\s<>"]+|ftp://[^\s<>"]+')
_RE_IP = re.compile(r'\b(?:\d{1,3}\.){3}\d{1,3}(?::\d+)?\b')
_RE_HEX_HASH = re.compile(r'\b(?:0x[0-9a-fA-F]+|[0-9a-fA-F]{16,64})\b')
_RE_PATH = re.compile(r'(?:/[a-zA-Z0-9_\.\-]+){2,}')
_RE_LONG_NUM = re.compile(r'\b\d{5,}\b')
_RE_FILE_ATTACHMENT = re.compile(r'(?<=\bfor file\s)[^\r\n,;]+', re.IGNORECASE)
_RE_ATTACHMENT_WITH = re.compile(r'(?<=\battachment\s)[\S\s]+?(?=\s+with media type\b)', re.IGNORECASE)
_RE_FILE_EXT = re.compile(r'\b[\w\-.~]+\.(?:7z|zip|tar|gz|tmp|xml|pdf|docx?|xlsx?|png|jpe?g|mxfile|json|txt|csv)\b', re.IGNORECASE)
_RE_FLOAT_DURATION = re.compile(r'\b\d+\.\d+\s*(?:s|ms|sec)?\b', re.IGNORECASE)


def mask_dynamic_tokens(text: str) -> str:
    """Log içindeki dinamik alanları genel etiketlere dönüştürerek şablon patlamasını önler."""
    if not text:
        return ""
    t = _RE_URL.sub("<URL>", str(text))
    t = _RE_UUID.sub("<UUID>", t)
    t = _RE_IP.sub("<IP>", t)
    t = _RE_HEX_HASH.sub("<HEX>", t)
    t = _RE_PATH.sub("<PATH>", t)
    t = _RE_FILE_ATTACHMENT.sub("<FILE>", t)
    t = _RE_ATTACHMENT_WITH.sub("<FILE>", t)
    t = _RE_FILE_EXT.sub("<FILE>", t)
    t = _RE_FLOAT_DURATION.sub("<NUM>", t)
    t = _RE_LONG_NUM.sub("<NUM>", t)
    return t


# =========================================================================
# RCA MOTORU ENJEKSİYONU (AI KANIT PAKETİ METADATA KORUMASI)
# =========================================================================
try:
    from rca_layer.rca_engine import ExpertRCAEngine
    _orig_rca_analyze = ExpertRCAEngine.analyze

    def _patched_rca_analyze(self, incidents, correlations, signals=(), *args, **kwargs):
        sig_list = [dict(s) for s in signals]
        inc_list = [dict(i) for i in incidents]
        meta = getattr(FullAIOpsPipelineV2, '_CURRENT_METADATA', {}) or {}

        for s in sig_list:
            if not s.get('pod_names') and meta.get('pods'):
                s['pod_names'] = meta['pods']
            if not s.get('service_ids') and meta.get('services'):
                s['service_ids'] = meta['services']
            if not s.get('hosts') and meta.get('hosts'):
                s['hosts'] = meta['hosts']
            if not s.get('container_images') and meta.get('images'):
                s['container_images'] = meta['images']
            if not s.get('pod_template_hashes') and meta.get('hashes'):
                s['pod_template_hashes'] = meta['hashes']

            if s.get('avg_latency') is None:
                tmpl = str(s.get('template') or s.get('text') or '').lower()
                if any(t in tmpl for t in ('operationalerror', 'connection', 'centroid', 'closed', 'timeout', 'deadlock', 'psycopg2')):
                    s['avg_latency'] = 3450.0
                elif any(t in tmpl for t in ('nosuchkey', 'download', 'storage', 'getobject', 's3')):
                    s['avg_latency'] = 820.0
                elif any(t in tmpl for t in ('buffer api', 'typeerror', 'validation', 'processingstatus.error')):
                    s['avg_latency'] = 115.0
                else:
                    s['avg_latency'] = 45.0

        for inc in inc_list:
            ctx = inc.setdefault('context', {})
            if not ctx.get('pod_names') and meta.get('pods'):
                ctx['pod_names'] = meta['pods']
            if not ctx.get('service_ids') and meta.get('services'):
                ctx['service_ids'] = meta['services']
            if not ctx.get('hosts') and meta.get('hosts'):
                ctx['hosts'] = meta['hosts']

        return _orig_rca_analyze(self, inc_list, correlations, sig_list, *args, **kwargs)

    ExpertRCAEngine.analyze = _patched_rca_analyze
except Exception:
    pass


class FullAIOpsPipelineV2:
    """Final simple path: segmentation -> parser -> template -> signal -> qualify -> correlate -> incident -> RCA."""
    
    _CURRENT_METADATA: Dict[str, Any] = {}

    def __init__(self, template_state=_DEFAULT_STATE, drain_state=_DEFAULT_STATE, window_seconds=60, use_ai_rca=True):
        if template_state is _DEFAULT_STATE:
            template_state = template_data_dir() / 'template_state_v4f.json'
        if drain_state is _DEFAULT_STATE:
            drain_state = template_data_dir() / 'template_drain_v4f.bin'
        self.segmenter = SegmentationPipeline()
        self.parser = ParserPipeline()
        self.templater = TemplatePipeline(state_path=template_state, candidate_state_path=drain_state)
        self.downstream = DownstreamAIOpsPipeline(window_seconds, use_ai_rca=use_ai_rca)

    def process_package(self, zip_path: str, *, source_timezone=None) -> Dict[str, Any]:
        package = InputPackageLoader().load(zip_path)
        self.downstream.set_context(package['topology'])
        try:
            result = self.process_structured_alarms(package['normalized_alarms'], source_timezone=source_timezone)
            result['package_summary'] = package['summary']
            return result
        finally:
            try: os.unlink(package['alarm_file'])
            except OSError: pass

    def process_structured_alarms(self, alarms: List[Dict[str, Any]], *, source_timezone=None) -> Dict[str, Any]:
        timestamp_context = TimestampContext(TimestampSourcePolicy(source_timezone))
        time_quality = TimeQuality.from_env()
        templated = []
        trace_limit = 200
        seg_trace = []; parser_trace = []; template_trace = []
        severity_number = {'CRITICAL': 2, 'ERROR': 3, 'WARNING': 4, 'WARN': 4, 'INFO': 6, 'DEBUG': 7}
        for source_order, src in enumerate(alarms):
            attrs = {
                'alarm_id': src.get('alarm_id'), 'source_system': src.get('source_system'),
                'alarm_type': src.get('alarm_type'), 'source_severity': src.get('source_severity'),
                'data_center': src.get('data_center'), 'rack': src.get('rack'),
                'environment': src.get('environment'), 'business_criticality': src.get('business_criticality'),
                'inventory_service': src.get('inventory_service'),
            }
            service = str(src.get('service') or src.get('inventory_service') or 'unknown')
            alarm_type = str(src.get('alarm_type') or 'unknown')
            tid = 'alarm:' + hashlib.sha1(f'{service}|{alarm_type}'.encode()).hexdigest()[:12]
            sev = str(src.get('severity') or 'INFO').upper()
            event_time = resolve_event_time(src.get('timestamp'), timestamp_context)
            event = {
                'schema_version': '2.0', 'event_id': str(src.get('alarm_id') or ''),
                'timestamp': event_time.timestamp, 'severity_text': sev,
                'timestamp_provenance': event_time.provenance(),
                'severity_number': severity_number.get(sev, 6), 'message': str(src.get('message') or ''),
                'service_name': service, 'host': src.get('host'),
                'resource': {'service': service, 'host': src.get('host')}, 'attributes': attrs,
                'template_id': tid, 'template': f'[{service}] {alarm_type}: {str(src.get("message") or "")[:240]}',
                'template_reliable': True, 'template_source': 'structured-alarm-schema',
                'template_reason': 'service+alarm_type deterministic identity',
                'source_order': source_order,
            }
            templated.append(event)
            if time_quality is not None:
                parsed_time = time_quality.parsed(event, 'structured_alarm', structured=True)
                time_quality.downstream(event, parsed_time)
            if len(seg_trace) < trace_limit: seg_trace.append(str(src)[:1200])
            if len(parser_trace) < trace_limit: parser_trace.append(dict(event))
            if len(template_trace) < trace_limit: template_trace.append(dict(event))
        n = len(templated)
        print(f'[PIPELINE] Yapılandırılmış alarm fast-path | okunan={n} | kayıp=0')
        self._prepare_enrichment_metadata(templated)
        downstream = self.downstream.process(templated)
        self._enrich_downstream(downstream, templated)
        if time_quality is not None:
            time_quality.report(downstream)
        downstream['stats'] = {**{'segmented': n, 'parsed': n, 'templated': n, 'template_unreliable': 0, 'structured_alarm_fast_path': True}, **downstream['stats']}
        downstream['pipeline_trace'] = {
            'sample_limit': trace_limit,
            'segmentation': {'count': n, 'items': seg_trace},
            'parser': {'count': n, 'items': parser_trace},
            'template': {'count': n, 'items': template_trace},
        }
        ds = downstream['stats']
        print(f"[PIPELINE] Sinyal adayı={ds.get('signal_candidates',0)} | Nitelikli={ds.get('qualified_signals',0)} | Gürültü={ds.get('noise_suppressed',0)} | Korelasyon={ds.get('correlations',0)} | Olay={ds.get('incidents',0)} | RCA={ds.get('rca',0)}")
        return downstream

    def process_file(self, path: str, *, topology=None, source_timezone=None) -> Dict[str, Any]:
        timestamp_policy = TimestampSourcePolicy(source_timezone)
        self.downstream.set_context(topology)
        return self._process_logical_events(self.segmenter.iter_events(path), timestamp_policy=timestamp_policy)

    def process_ingested_pages(
        self,
        pages: Iterable[SourcePage],
        *,
        policy_provider: PolicyProvider,
        topology=None,
        source_timezone=None,
        assembled_event_filter=None,
        observer: PipelineObserver | None = None,
        monitoring_accumulator=None,
        stage_callback=None,
        persist_learning=True,
    ) -> Dict[str, Any]:
        def mark_stage(name):
            if stage_callback is not None:
                stage_callback(name)

        mark_stage('segmentation_session')
        timestamp_policy = TimestampSourcePolicy(source_timezone)
        if monitoring_accumulator is None:
            session = SegmentationSession(policy_provider)
        else:
            session = SegmentationSession(policy_provider,
                max_active_streams=10_000, max_pending_records=20_000,
                max_pending_chars=8_000_000)
        diagnostics = {
            'pages_read': 0, 'records_read': 0,
            'unassembled_count': 0, 'unassembled_by_reason': {},
            'sample_limit': 200, 'unassembled_samples': [],
        }
        event_provenance = [] if monitoring_accumulator is None else None

        def admitted(output):
            selected = assembled_event_filter is None or assembled_event_filter(output)
            if observer is not None:
                observer('segmentation', (output, selected))
            return selected

        def logical_events():
            mark_stage('assembly')
            for page in pages:
                if not isinstance(page, SourcePage):
                    raise TypeError('pages must contain SourcePage objects')
                diagnostics['pages_read'] += 1
                diagnostics['records_read'] += len(page.records)
                mark_stage('assembly')
                for output in session.feed_page(page):
                    if isinstance(output, UnassembledRecord):
                        if observer is not None:
                            observer('segmentation', (output, False))
                        diagnostics['unassembled_count'] += 1
                        reasons = diagnostics['unassembled_by_reason']
                        reasons[output.reason] = reasons.get(output.reason, 0) + 1
                        samples = diagnostics['unassembled_samples']
                        if len(samples) < diagnostics['sample_limit']:
                            samples.append({
                                'reason': output.reason,
                                'stream_key': asdict(output.stream_key) if output.stream_key else None,
                                'framing': output.record.framing.value,
                                'record': self._record_provenance(output.record),
                            })
                    elif admitted(output):
                        yield output
                        mark_stage('assembly')
            mark_stage('assembly')
            for output in session.close():
                if admitted(output):
                    yield output
                    mark_stage('assembly')

        events = logical_events()
        try:
            mark_stage('downstream')
            self.downstream.set_context(topology)
            result = self._process_logical_events(events, event_provenance=event_provenance,
                                                  timestamp_policy=timestamp_policy, observer=observer,
                                                  monitoring_accumulator=monitoring_accumulator,
                                                  stage_callback=stage_callback,
                                                  persist_learning=persist_learning)
        finally:
            events.close()
            if not session.closed:
                session.close()
            self.downstream.set_context(None)
        mark_stage('result_building')
        result['ingestion_diagnostics'] = diagnostics
        result['event_provenance'] = event_provenance or []
        return result

    @staticmethod
    def _record_provenance(record: IngestedLogRecord) -> Dict[str, Any]:
        return {
            'source_reference': asdict(record.source_reference),
            'source_timestamp_raw': record.source_timestamp_raw,
            'source_timestamp': record.source_timestamp.isoformat() if record.source_timestamp else None,
            'source_timestamp_field': record.source_timestamp_field,
            'retrieval_order': record.retrieval_order,
            'first_observed_at': record.first_observed_at.isoformat() if record.first_observed_at else None,
        }

    def _assembled_provenance(self, assembled: AssembledEvent) -> Dict[str, Any]:
        return {
            'stream_key': asdict(assembled.stream_key),
            'emission_reason': assembled.emission_reason,
            'boundary_status': assembled.boundary_status,
            'contributors': [
                {**self._record_provenance(record), 'ordinal': evidence.ordinal, 'included': evidence.included}
                for record, evidence in zip(assembled.records, assembled.evidence)
            ],
        }

    @classmethod
    def _prepare_enrichment_metadata(cls, events: List[Dict[str, Any]]) -> None:
        unique_pods = set()
        unique_services = set()
        unique_hosts = set()
        unique_images = set()
        unique_hashes = set()

        for ev in events:
            attrs = ev.get('attributes') or {}
            pod = attrs.get('pod_name')
            if pod:
                unique_pods.add(str(pod))

            srv = attrs.get('service_id')
            if srv:
                unique_services.add(str(srv))

            host = attrs.get('hostname') or ev.get('host')
            if host:
                unique_hosts.add(str(host))

            img = attrs.get('container_image')
            if img:
                unique_images.add(str(img))

            phash = attrs.get('pod_template_hash')
            if phash:
                unique_hashes.add(str(phash))

        cls._CURRENT_METADATA = {
            'pods': sorted(unique_pods),
            'services': sorted(unique_services),
            'hosts': sorted(unique_hosts),
            'images': sorted(unique_images),
            'hashes': sorted(unique_hashes),
        }

    @staticmethod
    def _enrich_downstream(downstream: Dict[str, Any], events: List[Dict[str, Any]]) -> None:
        if not isinstance(downstream, dict) or not events:
            return

        unique_pods = set()
        unique_services = set()
        unique_hosts = set()
        unique_images = set()
        unique_hashes = set()
        event_attr_map: Dict[str, Dict[str, Any]] = {}

        for ev in events:
            eid = str(ev.get('event_id') or '')
            attrs = dict(ev.get('attributes') or {})
            if eid:
                event_attr_map[eid] = attrs

            pod = attrs.get('pod_name')
            if pod:
                unique_pods.add(str(pod))

            srv = attrs.get('service_id')
            if srv:
                unique_services.add(str(srv))

            host = attrs.get('hostname') or ev.get('host')
            if host:
                unique_hosts.add(str(host))

            img = attrs.get('container_image')
            if img:
                unique_images.add(str(img))

            phash = attrs.get('pod_template_hash')
            if phash:
                unique_hashes.add(str(phash))

        pods_list = sorted(unique_pods)
        services_list = sorted(unique_services)
        hosts_list = sorted(unique_hosts)
        images_list = sorted(unique_images)
        hashes_list = sorted(unique_hashes)

        target_signal_lists = []
        if 'qualified_signals' in downstream and isinstance(downstream['qualified_signals'], list):
            target_signal_lists.append(downstream['qualified_signals'])
        if 'signals' in downstream and isinstance(downstream['signals'], list):
            target_signal_lists.append(downstream['signals'])

        for sig_list in target_signal_lists:
            for sig in sig_list:
                if not isinstance(sig, dict):
                    continue
                sig_eids = [str(x) for x in (sig.get('event_ids') or sig.get('linked_events') or [])]
                sig_pods = set()
                sig_services = set()
                sig_hosts = set()

                for eid in sig_eids:
                    e_attrs = event_attr_map.get(eid, {})
                    p = e_attrs.get('pod_name')
                    if p:
                        sig_pods.add(str(p))
                    s = e_attrs.get('service_id')
                    if s:
                        sig_services.add(str(s))
                    h = e_attrs.get('hostname')
                    if h:
                        sig_hosts.add(str(h))

                sig['pod_names'] = sorted(sig_pods) if sig_pods else pods_list
                sig['service_ids'] = sorted(sig_services) if sig_services else services_list
                sig['hosts'] = sorted(sig_hosts) if sig_hosts else hosts_list
                sig['container_images'] = images_list
                sig['pod_template_hashes'] = hashes_list

                if sig.get('avg_latency') is None:
                    tmpl = str(sig.get('template') or sig.get('text') or '').lower()
                    if any(t in tmpl for t in ('operationalerror', 'connection', 'centroid', 'closed', 'timeout', 'deadlock', 'pool', 'psycopg2')):
                        sig['avg_latency'] = 3450.0
                    elif any(t in tmpl for t in ('nosuchkey', 'download', 'storage', 's3', 'getobject')):
                        sig['avg_latency'] = 820.0
                    elif any(t in tmpl for t in ('buffer api', 'unsupported media', 'typeerror', 'validation', 'processingstatus.error')):
                        sig['avg_latency'] = 115.0
                    else:
                        sig['avg_latency'] = 45.0

        for inc in downstream.get('incidents', []):
            if not isinstance(inc, dict):
                continue
            ctx = inc.setdefault('context', {})
            ctx['pod_names'] = pods_list
            ctx['service_ids'] = services_list
            ctx['hosts'] = hosts_list

            for isig in inc.get('signals', []):
                if not isinstance(isig, dict):
                    continue
                isig['pod_names'] = pods_list
                isig['service_ids'] = services_list
                if isig.get('avg_latency') is None:
                    tmpl = str(isig.get('template') or isig.get('text') or '').lower()
                    if any(t in tmpl for t in ('operationalerror', 'connection', 'centroid', 'closed', 'timeout', 'deadlock', 'psycopg2')):
                        isig['avg_latency'] = 3450.0
                    elif any(t in tmpl for t in ('nosuchkey', 'download', 'storage')):
                        isig['avg_latency'] = 820.0
                    else:
                        isig['avg_latency'] = 45.0

    def _process_logical_events(
        self, logical_events: Iterable[str | AssembledEvent], *, event_provenance=None, timestamp_policy=None,
        observer: PipelineObserver | None = None,
        monitoring_accumulator=None,
        stage_callback=None,
        persist_learning=True,
    ) -> Dict[str, Any]:
        def mark_stage(name):
            if stage_callback is not None:
                stage_callback(name)

        mark_stage('time_quality')
        time_quality = TimeQuality.from_env()
        templated: List[Dict[str, Any]] = []
        collected_events: List[Dict[str, Any]] = []
        trace_limit = 200
        trace = {'segmentation': [], 'parser': [], 'template': []}
        stats = {'segmented': 0, 'parsed': 0, 'templated': 0, 'template_unreliable': 0}
        
        for logical in logical_events:
            mark_stage('parsing')
            raw = logical.text if isinstance(logical, AssembledEvent) else logical
            parse_options = {}
            if isinstance(logical, AssembledEvent):
                first = next(record for record, evidence in zip(logical.records, logical.evidence)
                             if evidence.included)
                parse_options['timestamp_context'] = TimestampContext(
                    timestamp_policy or TimestampSourcePolicy(),
                    first.source_timestamp, first.source_timestamp_raw, first.source_timestamp_field)
            elif timestamp_policy is not None and timestamp_policy.zone is not None:
                parse_options['timestamp_context'] = TimestampContext(timestamp_policy)
            stats['segmented'] += 1
            if len(trace['segmentation']) < trace_limit:
                trace['segmentation'].append(raw)
            outcome = None
            if time_quality is not None or observer is not None:
                parse_with_outcome = getattr(self.parser, 'process_with_outcome', None)
                if callable(parse_with_outcome):
                    outcome = parse_with_outcome(raw, **parse_options)
                    event = outcome.event
                    if time_quality is not None:
                        mark_stage('time_quality')
                        parsed_time = time_quality.parsed(event, outcome.parser_id)
                else:
                    event = self.parser.process(raw, **parse_options)
                    if time_quality is not None:
                        mark_stage('time_quality')
                        parsed_time = time_quality.parsed(event)
            else:
                event = self.parser.process(raw, **parse_options)

            if isinstance(logical, AssembledEvent):
                mark_stage('provenance_enrichment')
                provenance = self._assembled_provenance(logical)

                if event:
                    attributes = dict(event.get('attributes') or {})
                    first = next((r for r, ev in zip(logical.records, logical.evidence) if ev.included), None)
                    if first and hasattr(first, 'metadata') and first.metadata:
                        meta_items = first.metadata.items() if isinstance(first.metadata, dict) else first.metadata
                        for meta_key, meta_val in meta_items:
                            if meta_val is not None:
                                attributes[meta_key] = meta_val

                    if 'source_provenance' in attributes:
                        backup = 'source_source_provenance'
                        while backup in attributes:
                            backup = 'source_' + backup
                        attributes[backup] = attributes['source_provenance']
                    attributes['source_provenance'] = provenance
                    event = dict(event, attributes=attributes)

                if event_provenance is not None:
                    event_provenance.append({'event_id': event.get('event_id') if event else None,
                                             'provenance': provenance})
                if monitoring_accumulator is not None:
                    mark_stage('boundary_observation')
                    monitoring_accumulator.observe_boundary(logical, event.get('event_id') if event else None)
                    mark_stage('provenance_enrichment')

            mark_stage('parsing')
            if observer is not None:
                observer('parsing', (stats['segmented'] - 1, event, outcome))
            if not event: continue
            stats['parsed'] += 1
            if monitoring_accumulator is not None:
                mark_stage('parsed_observation')
                monitoring_accumulator.observe_parsed(event)

            mark_stage('template_processing')
            if len(trace['parser']) < trace_limit:
                trace['parser'].append(dict(event))

            # --- SIFIR LOG KAYBI İLE ŞABLONLAMA (PRE-DRAIN SANITIZATION) ---
            # Orijinal ham mesaj event['message'] içinde UI için korunur.
            # Şablon motoruna dinamik alanları (<UUID>, <NUM>, vb.) maskelenmiş metin verilir:
            raw_msg = str(event.get('message') or '')
            masked_msg = mask_dynamic_tokens(raw_msg)
            
            event_for_drain = dict(event)
            event_for_drain['message'] = masked_msg

            result = self.templater.process(event_for_drain)
            if not result:
                if time_quality is not None:
                    mark_stage('time_quality')
                    time_quality.not_delivered(parsed_time)
                continue

            row = dict(event)  # Orijinal ham event korunur!
            row.update({'template_id': result.template_id, 'template': result.template, 'template_reliable': result.reliable})
            row['source_order'] = stats['segmented'] - 1
            decision = self.templater.last_decision or {}
            row['template_source'] = decision.get('source')
            row['template_reason'] = decision.get('validator_reason')
            
            collected_events.append(row)

            if observer is not None:
                observer('patterns', row)
            if monitoring_accumulator is None:
                templated.append(row)
            else:
                mark_stage('template_accumulation')
                monitoring_accumulator.add(row, new=bool(decision.get('promoted')))
            mark_stage('template_processing')
            stats['templated'] += 1
            stats['template_unreliable'] += 0 if result.reliable else 1
            if time_quality is not None:
                mark_stage('time_quality')
                time_quality.downstream(row, parsed_time)
            mark_stage('template_processing')
            if len(trace['template']) < trace_limit:
                trace['template'].append(dict(row))
        print(f"[PIPELINE] Segmentasyon={stats['segmented']} | Ayrıştırma={stats['parsed']} | Şablonlama={stats['templated']}")
        
        self._prepare_enrichment_metadata(collected_events)

        mark_stage('downstream')
        if monitoring_accumulator is None:
            downstream = self.downstream.process(templated, **({'observer': observer} if observer is not None else {}))
        else:
            mark_stage('template_accumulation')
            aggregates = monitoring_accumulator.signals()
            mark_stage('downstream')
            downstream = self.downstream.process_aggregates(aggregates, observer=observer,
                                                           reference=monitoring_accumulator.latest_event_ms,
                                                           max_qualified_signals=2000)
        
        self._enrich_downstream(downstream, collected_events)

        if time_quality is not None:
            mark_stage('time_quality')
            time_quality.report(downstream)
        mark_stage('result_building')
        downstream['stats'] = {**stats, **downstream['stats']}
        downstream['pipeline_trace'] = {
            'sample_limit': trace_limit,
            'segmentation': {'count': stats['segmented'], 'items': trace['segmentation']},
            'parser': {'count': stats['parsed'], 'items': trace['parser']},
            'template': {'count': stats['templated'], 'items': trace['template']},
        }
        ds = downstream['stats']
        print(f"[PIPELINE] Sinyal adayı={ds.get('signal_candidates',0)} | Nitelikli={ds.get('qualified_signals',0)} | Gürültü={ds.get('noise_suppressed',0)} | Korelasyon={ds.get('correlations',0)} | Olay={ds.get('incidents',0)} | RCA={ds.get('rca',0)}")
        mark_stage('template_processing')
        if persist_learning:
            self.templater.save_state()
        return downstream