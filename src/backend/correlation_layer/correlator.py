from __future__ import annotations
from typing import Any, Dict, Iterable, List

NETWORK={'network_down','pkt_loss','conn_refused'}
DATABASE={'disk_full','db_write_fail','db_conn_pool'}
MEMORY={'gc_pressure','oom_risk','mem_high'}
EXTERNAL={'ext_unreach','ext_slow'}
PROPAGATION={'conn_refused','timeout','http_5xx','latency_high','txn_fail','thread_pool','queue_backlog'}

class SignalCorrelator:
    """Topology-aware deterministic correlation for package data; generic same-service fallback remains."""
    def __init__(self,max_gap_seconds:int=300,topology=None):
        self.max_gap_ms=max_gap_seconds*1000; self.topology=topology
    @staticmethod
    def _type(s): return str(s.get('alarm_type') or '').lower()
    @staticmethod
    def _svc(s): return str(s.get('service_name') or '').strip()
    @staticmethod
    def _gap(a,b): return max(0,int(b.get('first_seen_ms',0))-int(a.get('last_seen_ms',0)))
    def correlate(self,signals:Iterable[Dict[str,Any]])->List[Dict[str,Any]]:
        rows=sorted(list(signals),key=lambda x:int(x.get('first_seen_ms',0))); edges=[]
        for i,a in enumerate(rows):
            for b in rows[i+1:]:
                gap=self._gap(a,b)
                if gap>self.max_gap_ms: break
                at,bt=self._type(a),self._type(b); asvc,bsvc=self._svc(a),self._svc(b)
                evidence=[]; score=0.0; direction=(a,b); relation=None
                if not at and not bt:
                    ta=str(a.get('template') or '').lower(); tb=str(b.get('template') or '').lower()
                    if asvc and asvc==bsvc and gap<=300_000:
                        score=.65; evidence=['aynı_servis','5_dakika_içinde']
                    elif (asvc and asvc.lower() in tb) or (bsvc and bsvc.lower() in ta):
                        score=.78; evidence=['log_içi_bağımlılık_referansı','5_dakika_içinde']
                # Same service: different stages of one local failure chain.
                if asvc and asvc==bsvc and gap<=180_000:
                    compatible=(at==bt or at in DATABASE|MEMORY|EXTERNAL|NETWORK|PROPAGATION or bt in PROPAGATION)
                    if compatible:
                        score=.58; evidence=['aynı_servis','zaman_yakınlığı']
                        if at!=bt: score+=.10; evidence.append('semptom_zinciri')
                # Physical network burst: same DC+rack + network alarm family is strong evidence.
                adc=set(a.get('data_centers') or []); bdc=set(b.get('data_centers') or [])
                ar=set(a.get('racks') or []); br=set(b.get('racks') or [])
                if at in NETWORK and bt in NETWORK and adc&bdc and ar&br and gap<=300_000:
                    if .88>score: score=.88; evidence=['aynı_veri_merkezi','aynı_kabin','ağ_alarm_ailesi','5_dakika_içinde']
                # Declared service topology. Direction is root service -> dependent service.
                if self.topology and asvc and bsvc and asvc!=bsvc:
                    relation=self.topology.relation(asvc,bsvc,max_hops=2)
                    if relation:
                        root=relation['root']; dependent=relation['dependent']; hops=relation['hops']
                        root_sig = a if asvc==root else b
                        dep_sig = b if bsvc==dependent else a
                        root_t=self._type(root_sig); dep_t=self._type(dep_sig)
                        # Root-like failures propagate to dependency symptoms. Same generic alarm type alone is not enough.
                        causal_compatible=(root_t in NETWORK|DATABASE|MEMORY|EXTERNAL or dep_t in PROPAGATION)
                        root_first=int(root_sig.get('first_seen_ms',0)) <= int(dep_sig.get('first_seen_ms',0))+60_000
                        temporal=abs(int(root_sig.get('first_seen_ms',0))-int(dep_sig.get('first_seen_ms',0)))<=self.max_gap_ms
                        if causal_compatible and temporal and root_first:
                            topo_score=.82 if hops==1 else .74 if hops==2 else .66
                            if topo_score>score:
                                score=topo_score; direction=(root_sig,dep_sig)
                                evidence=['servis_bağımlılığı',f'{hops}_hop','nedensel_zaman_sırası']
                                if root_t in DATABASE|MEMORY|EXTERNAL|NETWORK: evidence.append('kök_alarm_tipi')
                                if dep_t in PROPAGATION: evidence.append('türev_semptom')
                if score>=.55:
                    src,dst=direction
                    edge={'source':src['signal_id'],'target':dst['signal_id'],'score':round(min(1.0,score),2),'evidence':evidence,
                          'time_gap_ms':abs(int(dst.get('first_seen_ms',0))-int(src.get('first_seen_ms',0))),
                          'source_service':self._svc(src),'target_service':self._svc(dst)}
                    if relation: edge['dependency_path']=relation.get('path',[])
                    edges.append(edge)
        # stable unique pair, keep strongest
        best={}
        for e in edges:
            k=(e['source'],e['target'])
            if k not in best or e['score']>best[k]['score']: best[k]=e
        return list(best.values())
