from __future__ import annotations
import re
from typing import Any, Dict, Iterable, List

# Başarı ve olağan operasyon kalıpları (Asla arıza sinyali olamazlar)
_RE_SUCCESS = re.compile(
    r"\b(successfully|success|completed|finished|processed request|locked request|"
    r"deleted api-upload|stored image|worker finished|processing request|updating request.*status)\b",
    re.I
)

# Açık teknik hata semantiği
_RE_FAILURE = re.compile(
    r"\b(error|fatal|fail(ed|ure)?|exception|critical|panic|timeout|timed out|"
    r"broken pipe|connection reset|connection refused|operationalerror|nosuchkey|"
    r"rollback|deadlock|oom|oomkilled|unhandled|refused|crash|abort)\b",
    re.I
)


class NoiseGate:
    """Gerçek teknik arızaları gürültüden ve sahte başarı alarmlarından ayıran eleme katmanı."""

    def __init__(self, threshold: float = 0.70):
        self.threshold = threshold

    def qualify(self, signals: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
        out = []

        for s in signals:
            text = str(s.get('template') or s.get('template_pattern') or s.get('text') or s.get('message') or '')
            sev = int(s.get('severity_min') or s.get('severity') or 7)
            sid = str(s.get('signal_id') or '')
            is_monitor = bool(s.get('monitor_anomaly')) or sid.startswith('monitor:')

            # 1. KURAL: Sentetik Monitör Anomalisi Filtresi (Sahte Alarm Kalkanı)
            if is_monitor:
                # Monitörden gelse bile, log seviyesi INFO/DEBUG ise veya başarı kalıbı içeriyorsa ELE
                if (sev >= 5 or _RE_SUCCESS.search(text)) and not _RE_FAILURE.search(text):
                    s.update(
                        qualification_score=0.10,
                        qualified=False,
                        qualification_evidence=['başarılı_işlem_logu_filtrelendi'],
                        qualification_reason='gürültü_olarak_bastırıldı'
                    )
                    out.append(s)
                    continue

                # Gerçek bir hata veya kritik metrik sapmasıysa geçir
                s.update(
                    qualification_score=1.0,
                    qualified=True,
                    qualification_evidence=['deterministic_monitor_baseline'],
                    qualification_reason='nitelikli_sinyal'
                )
                out.append(s)
                continue

            # 2. KURAL: Genel Loglar İçin Deterministik Puanlama
            score = 0.0
            evidence = []

            # A. Log Seviyesi Puanı
            if sev <= 2:  # CRITICAL / ALERT
                score += 0.60
                evidence.append('kritik_önem_seviyesi')
            elif sev <= 3:  # ERROR
                score += 0.55
                evidence.append('hata_önem_seviyesi')
            elif sev <= 4:  # WARNING
                score += 0.20
                evidence.append('uyarı_önem_seviyesi')

            # B. Semantik İçerik Kontrolü
            if _RE_FAILURE.search(text):
                score += 0.30
                evidence.append('açık_hata_semantiği')
            elif _RE_SUCCESS.search(text):
                score -= 0.40  # Başarılı loglara ceza uygula
                evidence.append('başarı_semantiği_cezası')

            # C. Şablon Güvenilirliği
            tid = str(s.get('template_id') or '')
            if tid and not tid.startswith('unknown'):
                score += 0.05
                evidence.append('güvenilir_şablon')

            # ==============================================================
            # D. YENİ KURAL: Population Anomaly (Topluluk Sapması) Kontrolü
            # ==============================================================
            total_count = int(s.get('count', 1))
            pod_names = s.get('pod_names', [])
            pod_hashes = s.get('pod_template_hashes', [])
            
            # Eğer hacim dikkate değerse (örneğin 10'dan fazla log basılmışsa)
            if total_count >= 10:
                # 1. Yalıtılmış Pod Anomalisi: Tüm loglar sadece tek bir poddan geliyorsa
                if len(pod_names) == 1:
                    score += 0.40  # Çok büyük bir bonus
                    evidence.append(f'population_anomaly:isolated_to_single_pod({pod_names[0]})')
                    
                # 2. Sürüm (Canary) Anomalisi: Loglar birden fazla poddan gelse bile sadece tek versiyonda yaşanıyorsa
                elif len(pod_hashes) == 1:
                    score += 0.30
                    evidence.append(f'population_anomaly:isolated_to_single_release({pod_hashes[0]})')
            # ==============================================================

            final_score = round(max(0.0, min(1.0, score)), 2)
            is_qualified = (final_score >= self.threshold)

            s.update(
                qualification_score=final_score,
                qualified=is_qualified,
                qualification_evidence=evidence,
                qualification_reason='nitelikli_sinyal' if is_qualified else 'gürültü_olarak_bastırıldı'
            )
            out.append(s)

        return out


# Uyumluluk alias'ları
SignalQualifier = NoiseGate
SignalNoiseGate = NoiseGate