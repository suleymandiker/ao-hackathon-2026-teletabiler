from __future__ import annotations
from dataclasses import asdict
from typing import Any, Dict, Iterable, List
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
import os

_DEFAULT_STATE = object()

class FullAIOpsPipelineV2:
    """Final simple path: segmentation -> parser -> template -> signal -> qualify -> correlate -> incident -> RCA."""
    def __init__(self, template_state=_DEFAULT_STATE, drain_state=_DEFAULT_STATE, window_seconds=60, use_ai_rca=True):
        if template_state is _DEFAULT_STATE:
            template_state = template_data_dir() / 'template_state_v4f.json'
        if drain_state is _DEFAULT_STATE:
            drain_state = template_data_dir() / 'template_drain_v4f.bin'
        self.segmenter=SegmentationPipeline(); self.parser=ParserPipeline()
        self.templater=TemplatePipeline(state_path=template_state,candidate_state_path=drain_state)
        self.downstream=DownstreamAIOpsPipeline(window_seconds, use_ai_rca=use_ai_rca)
    def process_package(self, zip_path: str, *, source_timezone=None) -> Dict[str,Any]:
        package = InputPackageLoader().load(zip_path)
        self.downstream.set_context(package['topology'])
        try:
            # Structured hackathon alarms are already records. Do NOT send JSONL through
            # multiline log segmentation: a JSON object per physical line is one alarm.
            result = self.process_structured_alarms(package['normalized_alarms'], source_timezone=source_timezone)
            result['package_summary'] = package['summary']
            return result
        finally:
            try: os.unlink(package['alarm_file'])
            except OSError: pass


    def process_structured_alarms(self, alarms: List[Dict[str, Any]], *, source_timezone=None) -> Dict[str, Any]:
        """Lossless fast path for the hackathon alarm package.

        Every source alarm becomes exactly one canonical/template event.  The parser/template
        layers are still represented deterministically, but generic multiline discovery is
        intentionally bypassed because these inputs are structured records, not raw logs.
        """
        import hashlib
        timestamp_context = TimestampContext(TimestampSourcePolicy(source_timezone))
        time_quality = TimeQuality.from_env()
        templated=[]
        trace_limit=200
        seg_trace=[]; parser_trace=[]; template_trace=[]
        severity_number={'CRITICAL':2,'ERROR':3,'WARNING':4,'WARN':4,'INFO':6,'DEBUG':7}
        for source_order, src in enumerate(alarms):
            attrs={
                'alarm_id':src.get('alarm_id'), 'source_system':src.get('source_system'),
                'alarm_type':src.get('alarm_type'), 'source_severity':src.get('source_severity'),
                'data_center':src.get('data_center'), 'rack':src.get('rack'),
                'environment':src.get('environment'), 'business_criticality':src.get('business_criticality'),
                'inventory_service':src.get('inventory_service'),
            }
            service=str(src.get('service') or src.get('inventory_service') or 'unknown')
            alarm_type=str(src.get('alarm_type') or 'unknown')
            tid='alarm:'+hashlib.sha1(f'{service}|{alarm_type}'.encode()).hexdigest()[:12]
            sev=str(src.get('severity') or 'INFO').upper()
            event_time = resolve_event_time(src.get('timestamp'), timestamp_context)
            event={
                'schema_version':'2.0', 'event_id':str(src.get('alarm_id') or ''),
                'timestamp':event_time.timestamp, 'severity_text':sev,
                'timestamp_provenance':event_time.provenance(),
                'severity_number':severity_number.get(sev,6), 'message':str(src.get('message') or ''),
                'service_name':service, 'host':src.get('host'),
                'resource':{'service':service,'host':src.get('host')}, 'attributes':attrs,
                'template_id':tid, 'template':f'[{service}] {alarm_type}: {str(src.get("message") or "")[:240]}',
                'template_reliable':True, 'template_source':'structured-alarm-schema',
                'template_reason':'service+alarm_type deterministic identity',
                'source_order': source_order,
            }
            templated.append(event)
            if time_quality is not None:
                parsed_time = time_quality.parsed(event, 'structured_alarm', structured=True)
                time_quality.downstream(event, parsed_time)
            if len(seg_trace)<trace_limit: seg_trace.append(str(src)[:1200])
            if len(parser_trace)<trace_limit: parser_trace.append(dict(event))
            if len(template_trace)<trace_limit: template_trace.append(dict(event))
        n=len(templated)
        print(f'[PIPELINE] Yapılandırılmış alarm fast-path | okunan={n} | kayıp=0')
        downstream=self.downstream.process(templated)
        if time_quality is not None:
            time_quality.report(downstream)
        downstream['stats']={**{'segmented':n,'parsed':n,'templated':n,'template_unreliable':0,'structured_alarm_fast_path':True},**downstream['stats']}
        downstream['pipeline_trace']={
            'sample_limit':trace_limit,
            'segmentation':{'count':n,'items':seg_trace},
            'parser':{'count':n,'items':parser_trace},
            'template':{'count':n,'items':template_trace},
        }
        ds=downstream['stats']
        print(f"[PIPELINE] Sinyal adayı={ds.get('signal_candidates',0)} | Nitelikli={ds.get('qualified_signals',0)} | Gürültü={ds.get('noise_suppressed',0)} | Korelasyon={ds.get('correlations',0)} | Olay={ds.get('incidents',0)} | RCA={ds.get('rca',0)}")
        return downstream

    def process_file(self, path: str, *, topology=None, source_timezone=None) -> Dict[str,Any]:
        # Raw analyses use only their explicitly supplied context.
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
    ) -> Dict[str, Any]:
        """Analyze one caller-bounded, finite sequence of already acquired pages.

        The caller supplies Phase 3's deterministic prevalidated policy binding;
        this path never prepares/discovers segmentation or parser policies.
        Existing resident parser configuration and template learning are reused.
        Input is consumed once, in supplied order, without cursor interpretation,
        sorting or deduplication. Iterable exhaustion is explicit analysis end.
        An optional source-neutral assembled_event_filter selects emitted
        assembly outputs before parsing/learning. Monitoring uses this seam for
        window ownership; omitted filters preserve existing batch behavior.
        An optional synchronous observer receives borrowed outputs at existing
        stage boundaries. It must not mutate them; no observer is kept on the
        pipeline or learning state. Monitoring owns its sanitized read model.

        One local session emits events in completion order, followed by close
        tails in first-seen stream order. Dispositions are counted with the first
        200 diagnostic samples, matching the existing trace budget. Every parsed
        event carries source_provenance in attributes; an ordered event_provenance
        list preserves all assembled-event evidence beyond trace/aggregate samples.
        A None event_id in that list means the parser returned no canonical event.

        Pages/emitted AssembledEvents are not retained as an input history, but
        the existing batch downstream, result provenance and one pending event
        per stream consume memory proportional to the supplied finite analysis.
        This is not a continuous or byte-bounded processing API.

        On failure, pending session state is released and the error propagates;
        there is no partial success, replay or rollback of template learning.
        Explicit topology is applied for this invocation and cleared on exit.
        Like the existing pipeline, an instance is not a concurrent request API.
        """
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
            for page in pages:
                if not isinstance(page, SourcePage):
                    raise TypeError('pages must contain SourcePage objects')
                diagnostics['pages_read'] += 1
                diagnostics['records_read'] += len(page.records)
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
            for output in session.close():
                if admitted(output):
                    yield output

        events = logical_events()
        try:
            self.downstream.set_context(topology)
            result = self._process_logical_events(events, event_provenance=event_provenance,
                                                  timestamp_policy=timestamp_policy, observer=observer,
                                                  monitoring_accumulator=monitoring_accumulator)
        finally:
            events.close()
            if not session.closed:
                session.close()
            self.downstream.set_context(None)
        result['ingestion_diagnostics'] = diagnostics
        result['event_provenance'] = event_provenance or []
        return result

    @staticmethod
    def _record_provenance(record: IngestedLogRecord) -> Dict[str, Any]:
        # An explicit acquisition-evidence allowlist: never copy raw text,
        # arbitrary metadata, configuration, policy values or page cursors.
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

    def _process_logical_events(
        self, logical_events: Iterable[str | AssembledEvent], *, event_provenance=None, timestamp_policy=None,
        observer: PipelineObserver | None = None,
        monitoring_accumulator=None,
    ) -> Dict[str, Any]:
        """Shared existing parser/template/downstream flow; no policy preparation."""
        time_quality = TimeQuality.from_env()
        templated: List[Dict[str,Any]]=[]
        trace_limit = 200
        trace = {'segmentation': [], 'parser': [], 'template': []}
        stats={'segmented':0,'parsed':0,'templated':0,'template_unreliable':0}
        for logical in logical_events:
            raw = logical.text if isinstance(logical, AssembledEvent) else logical
            parse_options = {}
            if isinstance(logical, AssembledEvent):
                # The record contributing the first retained line owns this
                # event's source fallback. Never borrow a continuation's clock.
                first = next(record for record, evidence in zip(logical.records, logical.evidence)
                             if evidence.included)
                parse_options['timestamp_context'] = TimestampContext(
                    timestamp_policy or TimestampSourcePolicy(),
                    first.source_timestamp, first.source_timestamp_raw, first.source_timestamp_field)
            elif timestamp_policy is not None and timestamp_policy.zone is not None:
                parse_options['timestamp_context'] = TimestampContext(timestamp_policy)
            stats['segmented']+=1
            if len(trace['segmentation']) < trace_limit:
                trace['segmentation'].append(raw)
            outcome = None
            if time_quality is not None or observer is not None:
                # process() delegates to this outcome contract. Older injected
                # parsers can still use their existing process() method.
                parse_with_outcome = getattr(self.parser, 'process_with_outcome', None)
                if callable(parse_with_outcome):
                    outcome = parse_with_outcome(raw, **parse_options)
                    event = outcome.event
                    if time_quality is not None:
                        parsed_time = time_quality.parsed(event, outcome.parser_id)
                else:
                    event = self.parser.process(raw, **parse_options)
                    if time_quality is not None:
                        parsed_time = time_quality.parsed(event)
            else:
                event=self.parser.process(raw, **parse_options)
            if isinstance(logical, AssembledEvent):
                provenance = self._assembled_provenance(logical)
                if event_provenance is not None:
                    event_provenance.append({'event_id': event.get('event_id') if event else None,
                                             'provenance': provenance})
                if monitoring_accumulator is not None:
                    monitoring_accumulator.observe_boundary(logical, event.get('event_id') if event else None)
                if event:
                    # Attach diagnostics AFTER the builder has assigned identity.
                    # Preserve source attributes on collision, using the same
                    # source_ backup convention as CanonicalEventBuilder.
                    attributes = dict(event.get('attributes') or {})
                    if 'source_provenance' in attributes:
                        backup = 'source_source_provenance'
                        while backup in attributes:
                            backup = 'source_' + backup
                        attributes[backup] = attributes['source_provenance']
                    attributes['source_provenance'] = provenance
                    event = dict(event, attributes=attributes)
            if observer is not None:
                observer('parsing', (stats['segmented'] - 1, event, outcome))
            if not event: continue
            stats['parsed']+=1
            if monitoring_accumulator is not None:
                monitoring_accumulator.observe_parsed(event)
            if len(trace['parser']) < trace_limit:
                trace['parser'].append(dict(event))
            result=self.templater.process(event)
            if not result:
                if time_quality is not None:
                    time_quality.not_delivered(parsed_time)
                continue
            row=dict(event); row.update({'template_id':result.template_id,'template':result.template,'template_reliable':result.reliable})
            # Existing logical-event delivery order, scoped to this invocation.
            # Page retrieval tuples are opaque; preserve the session's supplied
            # completion order instead of reinterpreting provider metadata.
            row['source_order'] = stats['segmented'] - 1
            decision=self.templater.last_decision or {}; row['template_source']=decision.get('source'); row['template_reason']=decision.get('validator_reason')
            if observer is not None:
                observer('patterns', row)
            if monitoring_accumulator is None:
                templated.append(row)
            else:
                monitoring_accumulator.add(row, new=bool(decision.get('promoted')))
            stats['templated']+=1; stats['template_unreliable']+=0 if result.reliable else 1
            if time_quality is not None:
                time_quality.downstream(row, parsed_time)
            if len(trace['template']) < trace_limit:
                trace['template'].append(dict(row))
        print(f"[PIPELINE] Segmentasyon={stats['segmented']} | Ayrıştırma={stats['parsed']} | Şablonlama={stats['templated']}")
        if monitoring_accumulator is None:
            downstream=self.downstream.process(templated, **({'observer': observer} if observer is not None else {}))
        else:
            downstream=self.downstream.process_aggregates(monitoring_accumulator.signals(), observer=observer,
                                                           reference=monitoring_accumulator.latest_event_ms,
                                                           max_qualified_signals=200)
        if time_quality is not None:
            time_quality.report(downstream)
        downstream['stats']={**stats,**downstream['stats']}
        downstream['pipeline_trace'] = {
            'sample_limit': trace_limit,
            'segmentation': {'count': stats['segmented'], 'items': trace['segmentation']},
            'parser': {'count': stats['parsed'], 'items': trace['parser']},
            'template': {'count': stats['templated'], 'items': trace['template']},
        }
        ds=downstream['stats']
        print(f"[PIPELINE] Sinyal adayı={ds.get('signal_candidates',0)} | Nitelikli={ds.get('qualified_signals',0)} | Korelasyon={ds.get('correlations',0)} | Olay={ds.get('incidents',0)} | RCA={ds.get('rca',0)}")
        self.templater.save_state(); return downstream
