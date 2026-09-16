from __future__ import annotations
from typing import Any, Dict, Iterable, List

class LearningPlanner:
    """Operator action proposal with owner/status tracking; never auto-executes."""
    def plan(self, incidents: Iterable[Dict[str,Any]], rca: Iterable[Dict[str,Any]]) -> List[Dict[str,Any]]:
        rmap={r['incident_id']:r for r in rca}; out=[]
        for inc in incidents:
            severe=int(inc.get('severity_min',7))<=3; confidence=float(inc.get('confidence',0))
            root=inc.get('probable_root') or {}; kind=root.get('kind'); entity=root.get('entity','unknown')
            if kind=='infrastructure': owner='Network Operations'
            elif entity.endswith('-db'): owner='Database Operations'
            elif entity.endswith('-gw') or 'provider' in entity: owner='Integration / Network Operations'
            else: owner=f'{entity} Service Owner' if entity!='unknown' else 'SRE / NOC'
            action='collect_more_evidence'
            if severe and confidence>=.75: action='prepare_operator_review'
            elif severe: action='increase_observation_and_enrich_context'
            out.append({'incident_id':inc['incident_id'],'recommended_next_step':action,
                        'owner':owner,'status':'Açık','opened_at_ms':inc.get('end_ms'),'closed_at_ms':None,
                        'execution_allowed':False,'requires_human_approval':True,
                        'evidence_count':int(inc.get('signal_count',0)),'confidence':confidence,'rca':rmap.get(inc['incident_id'],{})})
        return out
