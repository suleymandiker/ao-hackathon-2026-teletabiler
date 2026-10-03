from __future__ import annotations
from typing import Any, Dict, List
from segmentation_layer.segmentation_pipeline import SegmentationPipeline
from parser_layer.parser_pipeline import ParserPipeline
from template_layer.template_pipeline import TemplatePipeline
from downstream_pipeline import DownstreamAIOpsPipeline
from input_package_layer.package_loader import InputPackageLoader
import os

class FullAIOpsPipelineV2:
    """Final simple path: segmentation -> parser -> template -> signal -> qualify -> correlate -> incident -> RCA."""
    def __init__(self, template_state='data/template_state_v4f.json', drain_state='data/template_drain_v4f.bin', window_seconds=60, use_ai_rca=True):
        self.segmenter=SegmentationPipeline(); self.parser=ParserPipeline()
        self.templater=TemplatePipeline(state_path=template_state,candidate_state_path=drain_state)
        self.downstream=DownstreamAIOpsPipeline(window_seconds, use_ai_rca=use_ai_rca)
    def process_package(self, zip_path: str) -> Dict[str,Any]:
        package = InputPackageLoader().load(zip_path)
        self.downstream.set_context(package['topology'])
        try:
            # Structured hackathon alarms are already records. Do NOT send JSONL through
            # multiline log segmentation: a JSON object per physical line is one alarm.
            result = self.process_structured_alarms(package['normalized_alarms'])
            result['package_summary'] = package['summary']
            return result
        finally:
            try: os.unlink(package['alarm_file'])
            except OSError: pass


    def process_structured_alarms(self, alarms: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Lossless fast path for the hackathon alarm package.

        Every source alarm becomes exactly one canonical/template event.  The parser/template
        layers are still represented deterministically, but generic multiline discovery is
        intentionally bypassed because these inputs are structured records, not raw logs.
        """
        import hashlib
        templated=[]
        trace_limit=200
        seg_trace=[]; parser_trace=[]; template_trace=[]
        severity_number={'CRITICAL':2,'ERROR':3,'WARNING':4,'WARN':4,'INFO':6,'DEBUG':7}
        for src in alarms:
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
            event={
                'schema_version':'2.0', 'event_id':str(src.get('alarm_id') or ''),
                'timestamp':src.get('timestamp'), 'severity_text':sev,
                'severity_number':severity_number.get(sev,6), 'message':str(src.get('message') or ''),
                'service_name':service, 'host':src.get('host'),
                'resource':{'service':service,'host':src.get('host')}, 'attributes':attrs,
                'template_id':tid, 'template':f'[{service}] {alarm_type}: {str(src.get("message") or "")[:240]}',
                'template_reliable':True, 'template_source':'structured-alarm-schema',
                'template_reason':'service+alarm_type deterministic identity',
            }
            templated.append(event)
            if len(seg_trace)<trace_limit: seg_trace.append(str(src)[:1200])
            if len(parser_trace)<trace_limit: parser_trace.append(dict(event))
            if len(template_trace)<trace_limit: template_trace.append(dict(event))
        n=len(templated)
        print(f'[PIPELINE] Yapılandırılmış alarm fast-path | okunan={n} | kayıp=0')
        downstream=self.downstream.process(templated)
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

    def process_file(self, path: str, *, topology=None) -> Dict[str,Any]:
        # Raw analyses use only their explicitly supplied context.
        self.downstream.set_context(topology)
        templated: List[Dict[str,Any]]=[]
        trace_limit = 200
        trace = {'segmentation': [], 'parser': [], 'template': []}
        stats={'segmented':0,'parsed':0,'templated':0,'template_unreliable':0}
        for raw in self.segmenter.iter_events(path):
            stats['segmented']+=1
            if len(trace['segmentation']) < trace_limit:
                trace['segmentation'].append(raw)
            event=self.parser.process(raw)
            if not event: continue
            stats['parsed']+=1
            if len(trace['parser']) < trace_limit:
                trace['parser'].append(dict(event))
            result=self.templater.process(event)
            if not result: continue
            row=dict(event); row.update({'template_id':result.template_id,'template':result.template,'template_reliable':result.reliable})
            decision=self.templater.last_decision or {}; row['template_source']=decision.get('source'); row['template_reason']=decision.get('validator_reason')
            templated.append(row); stats['templated']+=1; stats['template_unreliable']+=0 if result.reliable else 1
            if len(trace['template']) < trace_limit:
                trace['template'].append(dict(row))
        print(f"[PIPELINE] Segmentasyon={stats['segmented']} | Ayrıştırma={stats['parsed']} | Şablonlama={stats['templated']}")
        downstream=self.downstream.process(templated)
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
