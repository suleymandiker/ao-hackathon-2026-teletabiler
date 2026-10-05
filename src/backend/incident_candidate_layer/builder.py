from __future__ import annotations
from collections import defaultdict
from typing import Any, Dict, Iterable, List
from analysis_time import signal_time, time_key, signal_order, order_key, time_span
ROOT_TYPES={'network_down','pkt_loss','disk_full','db_write_fail','db_conn_pool','gc_pressure','oom_risk','ext_unreach','ext_slow'}
NETWORK={'network_down','pkt_loss'}; DATABASE={'disk_full','db_write_fail','db_conn_pool'}; MEMORY={'gc_pressure','oom_risk'}; EXTERNAL={'ext_unreach','ext_slow'}

class IncidentCandidateBuilder:
    """Root-seeded incident construction: symptoms may join a root, but cannot bridge two independent roots."""
    def __init__(self,topology=None): self.topology=topology
    @staticmethod
    def _typ(s): return str(s.get('alarm_type') or '')
    @staticmethod
    def _svc(s): return str(s.get('service_name') or '')
    def _root_related(self,a,b):
        if signal_time(a) is None or signal_time(b) is None: return False
        at,bt=self._typ(a),self._typ(b); gap=abs(int(a.get('first_seen_ms',0))-int(b.get('first_seen_ms',0)))
        if self._svc(a)==self._svc(b) and gap<=900_000:
            return bool(({at,bt}<=MEMORY) or ({at,bt}<=EXTERNAL) or ({at,bt}<=DATABASE) or at==bt)
        adc=set(a.get('data_centers') or []); bdc=set(b.get('data_centers') or []); ar=set(a.get('racks') or []); br=set(b.get('racks') or [])
        if at in NETWORK and bt in NETWORK and adc&bdc and ar&br and gap<=300_000: return True
        if at in DATABASE and bt in DATABASE and self.topology and gap<=600_000:
            return bool(self.topology.relation(self._svc(a),self._svc(b),max_hops=2))
        return False
    def build(self,signals:Iterable[Dict[str,Any]],correlations:Iterable[Dict[str,Any]])->List[Dict[str,Any]]:
        all_s={s['signal_id']:s for s in signals}; q=sorted((s for s in all_s.values() if s.get('qualified')), key=signal_order)
        roots=[s for s in q if self._typ(s) in ROOT_TYPES]
        if not roots:
            # Generic-log fallback: retain the previous correlation-component behavior.
            parent={s['signal_id']:s['signal_id'] for s in q}
            def f(x):
                while parent[x]!=x: parent[x]=parent[parent[x]]; x=parent[x]
                return x
            def u(a,b):
                a,b=f(a),f(b)
                if a!=b: parent[b]=a
            corr=list(correlations)
            for e in corr:
                if e.get('source') in parent and e.get('target') in parent: u(e['source'],e['target'])
            comps=defaultdict(list)
            for row in q: comps[f(row['signal_id'])].append(row)
            out=[]
            for n,rows in enumerate(comps.values(),1):
                if len(rows)==1 and int(rows[0].get('severity_min',7))>3: continue
                start,end=time_span(rows)
                out.append({'incident_id':f'inc:{start if start is not None else "untimed"}:{n}','status':'aday','start_ms':start,'end_ms':end,'event_count':sum(int(r.get('count',0)) for r in rows),'signal_count':len(rows),'correlated':len(rows)>1,'severity_min':min(int(r.get('severity_min',7)) for r in rows),'qualification_score':max(float(r.get('qualification_score',0)) for r in rows),'confidence':.75 if len(rows)>1 else .65,'services':sorted({self._svc(r) for r in rows}),'components':sorted({r.get('component','unknown') for r in rows}),'scopes':sorted({r.get('scope','genel') for r in rows}),'template_ids':sorted({r['template_id'] for r in rows}),'signal_ids':[r['signal_id'] for r in rows],'probable_root':{'kind':'signal','entity':self._svc(rows[0]),'alarm_type':'generic','signal_id':rows[0]['signal_id']},'correlation_count':0})
            return sorted(out,key=lambda x:(time_key(x, 'start_ms'), x['incident_id']))
        parent={s['signal_id']:s['signal_id'] for s in roots}
        def find(x):
            while parent[x]!=x: parent[x]=parent[parent[x]]; x=parent[x]
            return x
        def union(a,b):
            a,b=find(a),find(b)
            if a!=b: parent[b]=a
        for i,a in enumerate(roots):
            for b in roots[i+1:]:
                if self._root_related(a,b): union(a['signal_id'],b['signal_id'])
        groups=defaultdict(list)
        for s in roots: groups[find(s['signal_id'])].append(s)
        corr=list(correlations); edge_by_signal=defaultdict(list)
        for e in corr:
            edge_by_signal[e.get('source')].append(e); edge_by_signal[e.get('target')].append(e)
        root_to_group={s['signal_id']:g for g,rs in groups.items() for s in rs}
        # Attach each non-root symptom to one best root cluster only; symptoms never merge clusters.
        for s in q:
            if s['signal_id'] in root_to_group: continue
            candidates=[]
            for e in edge_by_signal.get(s['signal_id'],[]):
                other=e['target'] if e.get('source')==s['signal_id'] else e['source']
                g=root_to_group.get(other)
                if g:
                    root=all_s[other]
                    dt=abs(signal_time(s)-signal_time(root)) if signal_time(s) is not None and signal_time(root) is not None else float('inf')
                    candidates.append((float(e.get('score',0)),-dt,g))
            if candidates: groups[max(candidates)[2]].append(s)
        out=[]
        for n,rows in enumerate(groups.values(),1):
            if not rows: continue
            ids={r['signal_id'] for r in rows}; local=[e for e in corr if e.get('source') in ids and e.get('target') in ids]
            root_rows=[r for r in rows if self._typ(r) in ROOT_TYPES]
            # Root rank: explicit root type, topology outgoing evidence, severity, earliest.
            outgoing=defaultdict(int); incoming=defaultdict(int)
            for e in local: outgoing[e['source']]+=1; incoming[e['target']]+=1
            root=min(root_rows,key=lambda r:(-outgoing[r['signal_id']]+incoming[r['signal_id']],-int(r.get('source_severity_max',0)),time_key(r),order_key(r)))
            network_rows=[r for r in root_rows if self._typ(r) in NETWORK]; dc={x for r in network_rows for x in (r.get('data_centers') or [])}; racks={x for r in network_rows for x in (r.get('racks') or [])}
            if len(network_rows)>=2 and len(dc)==1 and len(racks)==1 and len({self._svc(r) for r in network_rows})>=2:
                probable={'kind':'infrastructure','entity':f'{next(iter(dc))}/{next(iter(racks))}','alarm_type':'network_failure','signal_id':root['signal_id']}
            else:
                # For DB root clusters prefer the dependency target when present (e.g. billing-db).
                entity=self._svc(root)
                if self.topology and self._typ(root) in DATABASE:
                    svcs={self._svc(r) for r in root_rows}
                    candidates=[svc for svc in svcs if sum(1 for other in svcs if other!=svc and self.topology.dependency_path(other,svc,max_hops=2))>0]
                    if candidates: entity=min(candidates,key=lambda x:(-sum(1 for o in svcs if o!=x and self.topology.dependency_path(o,x,max_hops=2)),x))
                probable={'kind':'service','entity':entity,'alarm_type':self._typ(root),'signal_id':root['signal_id']}
            start,end=time_span(rows); total=sum(int(r.get('count',0)) for r in rows)
            qmax=max(float(r.get('qualification_score',0)) for r in rows); conf=min(1.0,.55+.18*qmax+.12*min(1,len(local)/5))
            out.append({'incident_id':f'inc:{start if start is not None else "untimed"}:{n}','status':'aday','start_ms':start,'end_ms':end,'event_count':total,'signal_count':len(rows),'correlated':bool(local),
                        'severity_min':min(int(r.get('severity_min',7)) for r in rows),'qualification_score':round(qmax,3),'confidence':round(conf,3),
                        'services':sorted({self._svc(r) for r in rows}),'components':sorted({r.get('component','unknown') for r in rows}),'scopes':sorted({r.get('scope','genel') for r in rows}),
                        'template_ids':sorted({r['template_id'] for r in rows}),'signal_ids':[r['signal_id'] for r in rows],'probable_root':probable,'correlation_count':len(local)})
        return sorted(out,key=lambda x:(time_key(x, 'start_ms'), x['incident_id']))
