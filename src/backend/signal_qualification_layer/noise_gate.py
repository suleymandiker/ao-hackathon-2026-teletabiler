from __future__ import annotations
import re
from typing import Any, Dict, Iterable, List

_FAILURE = re.compile(r"\b(timeout|timed out|connection refused|no route to host|unreachable|failed|failure|exception|error|circuit breaker|5\d\d)\b", re.I)
# Strong operational types from the supplied schema. This is semantics, not a hard-coded incident/case.
ROOT_TYPES={'network_down','pkt_loss','disk_full','db_write_fail','db_conn_pool','gc_pressure','oom_risk','ext_unreach','ext_slow'}
SYMPTOM_TYPES={'conn_refused','timeout','http_5xx','latency_high','txn_fail','thread_pool','queue_backlog'}
BACKGROUND_TYPES={'cert_expiry','backup_warn','ntp_drift','log_rotate','disk_warn','cpu_high','mem_high','network_flap'}

class SignalNoiseGate:
    """Explainable gate for structured alarms and generic logs."""
    def __init__(self, min_score: float=.60): self.min_score=float(min_score)
    def qualify(self, signals: Iterable[Dict[str,Any]]) -> List[Dict[str,Any]]:
        out=[]
        for raw in signals:
            s=dict(raw); count=int(s.get('count',0)); sev=int(s.get('severity_min',7)); srcsev=int(s.get('source_severity_max',0) or 0)
            typ=str(s.get('alarm_type') or '').lower(); reliable=float(s.get('reliable_ratio',0)); text=str(s.get('template') or '')
            evidence=[]; score=0.0
            if not typ:
                if sev<=2: score+=.70; evidence.append('kritik_önem_seviyesi')
                elif sev==3: score+=.45; evidence.append('hata_önem_seviyesi')
                elif sev==4: score+=.20; evidence.append('uyarı_önem_seviyesi')
                failure=bool(_FAILURE.search(text))
                if failure: score+=.30; evidence.append('açık_hata_semantiği')
                if count>=20: score+=.25; evidence.append('yüksek_tekrar')
                elif count>=5: score+=.15; evidence.append('tekrarlayan_olay')
                elif count>=2: score+=.05; evidence.append('birden_fazla_olay')
                if reliable>=.90: score+=.05; evidence.append('güvenilir_şablon')
                score=round(min(1.0,score),3); qualified=score>=self.min_score and (sev<=4 or (failure and count>=20))
                s.update(qualification_score=score,qualified=qualified,qualification_evidence=evidence,qualification_reason='nitelikli_sinyal' if qualified else 'gürültü_olarak_bastırıldı'); out.append(s); continue
            if typ in ROOT_TYPES: score+=.48; evidence.append('kök_neden_adayı_alarm_tipi')
            elif typ in SYMPTOM_TYPES: score+=.32; evidence.append('operasyonel_semptom_alarm_tipi')
            elif typ in BACKGROUND_TYPES: score-=.20; evidence.append('arka_plan_alarm_tipi')
            if srcsev>=5 or sev<=2: score+=.35; evidence.append('kritik_önem')
            elif srcsev>=4 or sev==3: score+=.22; evidence.append('yüksek_önem')
            elif srcsev>=3 or sev==4: score+=.08; evidence.append('orta_önem')
            if count>=8: score+=.28; evidence.append('belirgin_patlama')
            elif count>=3: score+=.16; evidence.append('tekrarlayan_olay')
            elif count>=2: score+=.06; evidence.append('birden_fazla_olay')
            if _FAILURE.search(text): score+=.12; evidence.append('açık_hata_semantiği')
            if reliable>=.90: score+=.04; evidence.append('güvenilir_şablon')
            score=round(max(0.0,min(1.0,score)),3)
            # Background telemetry cannot pass merely because a random row has high severity.
            if typ in BACKGROUND_TYPES:
                qualified = count>=10 and srcsev>=4
            else:
                qualified = score>=self.min_score
            s.update(qualification_score=score,qualified=qualified,qualification_evidence=evidence,
                     qualification_reason='nitelikli_sinyal' if qualified else 'gürültü_olarak_bastırıldı')
            out.append(s)
        return out
