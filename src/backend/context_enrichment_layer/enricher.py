from __future__ import annotations
from typing import Any, Dict, Iterable, List

class ContextEnricher:
    def __init__(self, topology=None): self.topology=topology

    def enrich(self, incidents:Iterable[Dict[str,Any]], signals:Iterable[Dict[str,Any]])->List[Dict[str,Any]]:
        by_id={s['signal_id']:s for s in signals}; out=[]
        for inc in incidents:
            rows=[by_id[x] for x in inc.get('signal_ids',[]) if x in by_id]; x=dict(inc)
            hosts=sorted({h for r in rows for h in (r.get('hosts') or [r.get('host')]) if h not in (None,'unknown','')})
            services=sorted({r.get('service_name') for r in rows if r.get('service_name') not in (None,'unknown')})
            deps=[]
            if self.topology:
                for src in services:
                    for dst in services:
                        if src==dst: continue
                        p=self.topology.dependency_path(src,dst,max_hops=4)
                        if p and len(p)>1:
                            deps.append({'dependent':src,'root':dst,'path':p,'hops':len(p)-1,
                                         'edge_metadata':[self.topology.edge_meta.get((p[i],p[i+1]),{}) for i in range(len(p)-1)]})
            inventory=[]
            if self.topology:
                for host in hosts:
                    inv=self.topology.host_context(host)
                    if inv: inventory.append(inv)
            x['context']={
                'services':services,'components':sorted({r.get('component') for r in rows if r.get('component') not in (None,'unknown')}),'hosts':hosts,
                'data_centers':sorted({v for r in rows for v in (r.get('data_centers') or [])}),'racks':sorted({v for r in rows for v in (r.get('racks') or [])}),
                'business_criticalities':sorted({v for r in rows for v in (r.get('business_criticalities') or [])}),
                'host_inventory':inventory,'dependencies':deps[:40],
                'templates':[{'template_id':r['template_id'],'alarm_type':r.get('alarm_type'),'template':r.get('template',''),'count':r.get('count',0),'service':r.get('service_name'),'source_severity_max':r.get('source_severity_max')} for r in rows[:30]]}
            out.append(x)
        return out
