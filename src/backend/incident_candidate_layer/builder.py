from __future__ import annotations

import hashlib
import json
import os
import re
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List

from analysis_time import signal_time, time_key, signal_order, order_key, time_span
from data_paths import policy_data_dir

# 1. EVRENSEL KÖK ARIZA KALIPLARI (DİL VE KÜTÜPHANE AGNOSTİK)
_RE_ROOT_FAILURE = re.compile(
    r"\b("
    r"operationalerror|databaseerror|sqlexception|deadlock|closed the connection|"
    r"connection refused|broken pipe|connection reset|network is unreachable|"
    r"disk full|no space left|oom|oomkilled|heap space|"
    r"traceback|unicodedecodeerror|valueerror|typeerror|keyerror|indexerror|"
    r"zerodivisionerror|assertionerror|nullpointerexception|segmentation fault|"
    r"panic:|unhandled exception|attributeerror|runtimeerror|"
    r"nosuchkey|failed to upload|failed to download|commit failed|broker unreachable|"
    r"http error downloading|client error|server error|5\d\d|error code"
    r")\b",
    re.I
)

# 2. TÜREV SEMPTOMLAR (ARTÇI ETKİLER)
_RE_SYMPTOM = re.compile(
    r"\b("
    r"timeout|timed out|deadline exceeded|rollback|transaction rolled back|"
    r"retry limit exceeded|circuit breaker|backoff failed|failed for domain|"
    r"document processing failed in worker|maximum context length|attempts failed"
    r")\b",
    re.I
)

# 3. BAŞARILI İŞLEM VE SÜREÇ LOGLARI (GÜRÜLTÜ ENGELLEME)
_RE_SUCCESS_OR_PROGRESS = re.compile(
    r"\b("
    r"successfully|success|completed|finished|processed request|locked request|"
    r"deleted api-upload|stored image|worker finished|parçaya bölündü|"
    r"bölündü|parçalanıyor|başlatıldı|işleniyor|started processing|heartbeat"
    r")\b",
    re.I
)

# 4. ANLAMSIZ LOG PARÇALARI
_RE_NOISE_SNIPPET = re.compile(
    r"^(\s*error\s+severity\s*|\s*error\s*|\s*failed\s*|\s*exception\s*|\s*unknown\s*)$",
    re.I
)

# K8s Pod hash budama regex'i (Evrensel K8s dağıtım kuralı)
_RE_K8S_POD_SUFFIX = re.compile(
    r"-[0-9a-f]{8,10}-[a-z0-9]{4,5}$|"
    r"-[a-z0-9]{5}-[a-z0-9]{5}$|"
    r"-\d+$",
    re.I
)


class ActiveIncidentStore:
    """Aktif olayların durumunu pencereler boyunca canlı tutan kalıcı depo."""

    def __init__(self, ttl_seconds: int = 86400):
        self.ttl_ms = ttl_seconds * 1000
        try:
            self.file_path = policy_data_dir() / "active_incidents.json"
        except Exception:
            self.file_path = Path("data/policy/active_incidents.json")

    def load(self) -> Dict[str, Any]:
        if not self.file_path.exists():
            return {}
        try:
            with open(self.file_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}

    def save(self, data: Dict[str, Any]) -> None:
        try:
            self.file_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.file_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception:
            pass

    def track(self, problem_key: str, start_ms: int, end_ms: int) -> tuple[str, str, bool]:
        store = self.load()
        now_ms = int(time.time() * 1000)
        current_ref_ms = end_ms or now_ms

        store = {
            k: v for k, v in store.items()
            if (current_ref_ms - v.get("last_seen_ms", 0)) <= self.ttl_ms
        }

        if problem_key in store:
            item = store[problem_key]
            item["last_seen_ms"] = current_ref_ms
            item["count"] = item.get("count", 1) + 1
            self.save(store)
            return item["incident_id"], "AKTİF", False
        else:
            inc_hash = hashlib.sha256(problem_key.encode("utf-8")).hexdigest()[:8]
            inc_id = f"inc:{inc_hash}"
            store[problem_key] = {
                "incident_id": inc_id,
                "first_seen_ms": start_ms or current_ref_ms,
                "last_seen_ms": current_ref_ms,
                "count": 1
            }
            self.save(store)
            return inc_id, "AÇIK", True


class IncidentCandidateBuilder:
    """100+ workload için tam agnostik, topolojik ve kaskat birleştirmeli Incident motoru."""

    def __init__(self, topology=None):
        self.topology = topology
        self.store = ActiveIncidentStore(ttl_seconds=86400)

    @classmethod
    def _extract_workload(cls, s: Dict[str, Any]) -> str:
        """İş yükünü daima en üst operasyonel birimden (Deployment/Service) çözer, asla alt kod paketine inmez."""
        # 1. Açık Workload / Deployment alanı
        for field in ('workload', 'service_name', 'service', 'app'):
            val = s.get(field)
            if val and str(val).strip().lower() not in ('', 'none', 'unknown', 'null'):
                return str(val).strip()

        # 2. Kubernetes Pod isimlerinden dinamik iş yükü normalizasyonu
        pod_candidates = []
        if isinstance(s.get('pod_names'), (list, tuple, set)):
            pod_candidates.extend(s['pod_names'])
        for p_field in ('pod_name', 'pod', 'identity', 'host'):
            p_val = s.get(p_field)
            if p_val:
                pod_candidates.append(str(p_val))

        hashes = set()
        if isinstance(s.get('pod_template_hashes'), (list, tuple, set)):
            hashes.update(str(h).strip() for h in s['pod_template_hashes'] if h)

        for pod in pod_candidates:
            pod_str = str(pod).strip()
            if not pod_str or pod_str.lower() in ('', 'none', 'unknown', 'null'):
                continue
            for h in hashes:
                if f"-{h}-" in pod_str:
                    wp = pod_str.split(f"-{h}-")[0]
                    if wp:
                        return wp
            norm = _RE_K8S_POD_SUFFIX.sub("", pod_str)
            if norm and norm != pod_str:
                return norm

        # 3. Kurumsal Service ID
        s_ids = s.get('service_ids')
        if isinstance(s_ids, (list, tuple, set)) and s_ids:
            first_id = str(next(iter(s_ids))).strip()
            if first_id and first_id.lower() not in ('', 'none', 'unknown', 'null'):
                return f"service_{first_id}"

        # 4. Namespace
        ns = s.get('namespace')
        if ns and str(ns).strip().lower() not in ('', 'none', 'unknown', 'null'):
            return str(ns).strip()

        return 'unknown'

    @classmethod
    def _extract_failure_domain(cls, text: str) -> str:
        """Hatanın ana etki alanını (domain) belirler: Storage/Download, Database, Memory, Code."""
        t = text.lower()
        if any(k in t for k in ('download', 'http', 'attachment', 'confluence', 'nosuchkey', 'bucket')):
            return 'external_io'
        if any(k in t for k in ('database', 'psycopg', 'operationalerror', 'deadlock', 'session')):
            return 'database'
        if any(k in t for k in ('oom', 'out of memory', 'heap')):
            return 'resource'
        return 'application'

    def _cluster_signals(self, signals: List[Dict[str, Any]], correlations: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
        """Korelasyon bağları, iş yükü ve etki alanı yakınlığına göre sinyalleri kümeler."""
        parent = {s['signal_id']: s['signal_id'] for s in signals}

        def find(i):
            if parent[i] == i:
                return i
            parent[i] = find(parent[i])
            return parent[i]

        def union(i, j):
            root_i = find(i)
            root_j = find(j)
            if root_i != root_j:
                parent[root_i] = root_j

        # 1. Korelasyon motorundan gelen nedensellik bağlarını birleştir
        for edge in correlations:
            src, dst = edge.get('source'), edge.get('target')
            if src in parent and dst in parent:
                union(src, dst)

        # 2. Aynı İş Yükü (Workload) ve Aynı Hata Alanındaki Sinyalleri Birleştir
        domain_map = defaultdict(list)
        for s in signals:
            workload = self._extract_workload(s)
            text = str(s.get('template') or s.get('text') or s.get('message') or '')
            domain = self._extract_failure_domain(text)
            domain_map[(workload, domain)].append(s['signal_id'])

        for sids in domain_map.values():
            for sid in sids[1:]:
                union(sids[0], sid)

        clusters = defaultdict(list)
        sig_map = {s['signal_id']: s for s in signals}
        for s in signals:
            clusters[find(s['signal_id'])].append(sig_map[s['signal_id']])

        return list(clusters.values())

    @classmethod
    def _qualifies_as_incident(cls, cluster: List[Dict[str, Any]]) -> bool:
        """Dünya Standardı Eşik: İzole, geçici veya gürültü loglarının Incident açmasını engeller."""
        has_root_crash = False
        total_events = 0
        all_pods = set()

        for s in cluster:
            text = str(s.get('template') or s.get('text') or s.get('message') or '').strip()
            total_events += int(s.get('count', 1))

            if isinstance(s.get('pod_names'), (list, tuple, set)):
                all_pods.update(p for p in s['pod_names'] if p and p != 'unknown')
            pod = s.get('pod_name') or s.get('identity')
            if pod and pod != 'unknown':
                all_pods.add(str(pod))

            if _RE_NOISE_SNIPPET.match(text) or len(text) < 15:
                continue

            if _RE_ROOT_FAILURE.search(text) or _RE_SYMPTOM.search(text):
                has_root_crash = True

        if not has_root_crash:
            return False

        # Asgari Etki Eşiği: Ya birden fazla sinyal, ya birden fazla pod, ya da >= 5 tekrarlı hata
        if len(cluster) > 1 or len(all_pods) > 1 or total_events >= 5:
            return True

        return False

    def _select_root_signal(self, cluster_signals: List[Dict[str, Any]], correlations: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Kök arızayı (Örn: cp950 Decode) türev semptomların (Örn: worker failed) önüne geçirir."""
        outgoing = defaultdict(int)
        for edge in correlations:
            outgoing[edge.get('source')] += 1

        def root_priority_score(s: Dict[str, Any]) -> tuple:
            sid = str(s.get('signal_id', ''))
            text = str(s.get('template') or s.get('text') or s.get('message') or '').strip()
            sev = int(s.get('severity_min', 7))

            is_noise = 1 if (_RE_NOISE_SNIPPET.match(text) or len(text) < 15) else 0
            is_progress = 1 if (_RE_SUCCESS_OR_PROGRESS.search(text) and not _RE_ROOT_FAILURE.search(text)) else 0
            is_root_failure = 1 if _RE_ROOT_FAILURE.search(text) else 0
            is_symptom = 1 if _RE_SYMPTOM.search(text) else 0

            return (
                is_noise,
                is_progress,
                -is_root_failure,       # Kök arızalar EN ÖNE (-1)
                is_symptom,             # Türev semptomlar ARKAYA (1)
                sev,                    # ERROR (3) öne
                -outgoing[sid],         # Çok hata tetikleyen öne
                time_key(s),
                order_key(s)
            )

        ranked = sorted(cluster_signals, key=root_priority_score)
        return ranked[0]

    def build(self, signals: Iterable[Dict[str, Any]], correlations: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
        all_s = {s['signal_id']: s for s in signals}
        q = sorted((s for s in all_s.values() if s.get('qualified')), key=signal_order)
        if not q:
            return []

        correlations_list = list(correlations)
        clusters = self._cluster_signals(q, correlations_list)
        incidents = []

        for cluster in clusters:
            if not self._qualifies_as_incident(cluster):
                continue

            root_signal = self._select_root_signal(cluster, correlations_list)
            workload_name = self._extract_workload(root_signal)

            root_tpl = (
                root_signal.get('template')
                or root_signal.get('template_pattern')
                or root_signal.get('text')
                or root_signal.get('message')
                or ''
            ).strip()

            clean_tmpl = re.sub(r'https?://\S+', '<URL>', root_tpl)
            clean_tmpl = re.sub(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', '<UUID>', clean_tmpl, flags=re.I)
            clean_tmpl = re.sub(r'\b0x[0-9a-f]+\b', '<HEX>', clean_tmpl, flags=re.I)
            clean_tmpl = re.sub(r'\b\d+\b', '<NUM>', clean_tmpl)
            clean_tmpl = re.sub(r'\s+', ' ', clean_tmpl).strip().lower()

            domain = self._extract_failure_domain(root_tpl)
            core_signature = f"{workload_name}:{domain}"

            start, end = time_span(cluster)
            incident_id, status, is_new = self.store.track(core_signature, start, end)

            services = sorted({self._extract_workload(s) for s in cluster if self._extract_workload(s) != 'unknown'})
            components = sorted({str(s.get('component') or s.get('logger') or 'unknown') for s in cluster})
            template_ids = sorted({s.get('template_id') for s in cluster if s.get('template_id')})
            signal_ids = [s['signal_id'] for s in cluster]
            total_events = sum(int(s.get('count', 1)) for s in cluster)

            cluster_sids = set(signal_ids)
            local_correlations = [
                e for e in correlations_list
                if e.get('source') in cluster_sids and e.get('target') in cluster_sids
            ]

            incident = {
                'incident_id': incident_id,
                'status': status,
                'is_new': is_new,
                'start_ms': start,
                'end_ms': end,
                'time_window': {'start': start, 'end': end},
                'event_count': total_events,
                'signal_count': len(cluster),
                'correlated': len(local_correlations) > 0,
                'severity_min': min(int(s.get('severity_min', 7)) for s in cluster),
                'qualification_score': round(max(float(s.get('qualification_score', 0)) for s in cluster), 3),
                'confidence': 0.88,
                'services': services,
                'affected_services': services,
                'components': components,
                'scopes': sorted({s.get('scope', 'genel') for s in cluster}),
                'template_ids': template_ids,
                'signal_ids': signal_ids,
                'root_cause_template': root_tpl,
                'description': root_tpl,
                'probable_root': {
                    'kind': 'workload',
                    'entity': workload_name,
                    'domain': domain,
                    'signal_id': root_signal['signal_id']
                },
                'probable_root_signal': root_signal['signal_id'],
                'correlation_count': len(local_correlations)
            }
            incidents.append(incident)

        return incidents