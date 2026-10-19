from __future__ import annotations
import re
from typing import Any, Dict, Iterable, List
from collections import defaultdict
from analysis_time import signal_time, signal_order

# 1. EVRENSEL HATA SINIFLANDIRMA REGEX'LERİ (DİL VE KÜTÜPHANE BAĞIMSIZ)
_RE_NETWORK = re.compile(
    r"\b(connection refused|no route to host|network is unreachable|broken pipe|"
    r"connection reset|socket closed|network down|packet loss|dns resolution failed|"
    r"name resolution|getaddrinfo failed|ssl handshake failed|certificate verify failed)\b",
    re.I
)
_RE_STORAGE = re.compile(
    r"\b(disk full|no space left on device|enospc|read-only file system|io error|"
    r"storage error|bucket not found|nosuchkey|failed to upload|failed to download|"
    r"file not found|permission denied while writing)\b",
    re.I
)
_RE_RESOURCE = re.compile(
    r"\b(out of memory|oom|oomkilled|killed process|heap space|memory limit exceeded|"
    r"thread exhaustion|thread pool full|connection pool exhausted|max clients reached|"
    r"file descriptor limit|too many open files)\b",
    re.I
)
_RE_DATABASE = re.compile(
    r"\b(operationalerror|databaseerror|sqlexception|psycopg|mysql|sqlalchemy|"
    r"mongodb|postgres|redis|deadlock detected|lock wait timeout|server closed the connection|"
    r"terminat(ed|ing) abnormally|relation does not exist|syntax error at or near|"
    r"unique constraint violation|could not connect to server)\b",
    re.I
)
_RE_MESSAGING = re.compile(
    r"\b(kafka|rabbitmq|amqp|celery|consumer group|commit failed|broker unreachable|"
    r"queue full|message rejected|backlog exceeded|partition leader not available)\b",
    re.I
)
_RE_APP_CRASH = re.compile(
    r"\b(traceback \(most recent call last\)|panic:|fatal error|nullpointerexception|"
    r"segmentation fault|unhandled exception|keyerror|typeerror|valueerror|"
    r"indexerror|zerodivisionerror|assertionerror|referenceerror|unicodedecodeerror)\b",
    re.I
)
_RE_PROPAGATION = re.compile(
    r"\b(timeout|timed out|deadline exceeded|request aborted|transaction rolled back|"
    r"rollback\(\)|circuit breaker|retry limit exceeded|backoff failed|"
    r"failed to process request|status to error|http 5\d\d|502 bad gateway|"
    r"503 service unavailable|504 gateway timeout|maximum context length)\b",
    re.I
)

# Kubernetes dağıtım son eklerini budayan dinamik kalıp (Deployment, ReplicaSet, StatefulSet)
_RE_K8S_POD_SUFFIX = re.compile(
    r"-[0-9a-f]{8,10}-[a-z0-9]{4,5}$|"
    r"-[a-z0-9]{5}-[a-z0-9]{5}$|"
    r"-\d+$",
    re.I
)

ROOT_TYPES = {
    'network_error', 'storage_error', 'resource_error',
    'database_error', 'messaging_error', 'app_crash'
}
PROPAGATION_TYPES = {'symptom_cascade'}


class SignalCorrelator:
    """100+ workload için sıfır-hardcode, tam dinamik ve ölçeklenebilir AIOps korelasyon motoru."""

    def __init__(self, max_gap_seconds: int = 300, topology=None):
        self.max_gap_ms = max_gap_seconds * 1000
        self.topology = topology

    @staticmethod
    def _extract_type(text: str, alarm_type: str, severity: int) -> str:
        """Hata metninden ve sinyal alanlarından semantik hata sınıfını çıkarır."""
        if alarm_type and str(alarm_type).lower().strip() not in ('', 'none', 'unknown'):
            return str(alarm_type).lower().strip()
        if _RE_APP_CRASH.search(text): return 'app_crash'
        if _RE_DATABASE.search(text): return 'database_error'
        if _RE_RESOURCE.search(text): return 'resource_error'
        if _RE_NETWORK.search(text): return 'network_error'
        if _RE_STORAGE.search(text): return 'storage_error'
        if _RE_MESSAGING.search(text): return 'messaging_error'
        if _RE_PROPAGATION.search(text): return 'symptom_cascade'
        return 'generic_error' if severity <= 3 else 'informational'

    @classmethod
    def _extract_svc(cls, s: Dict[str, Any]) -> str:
        """Sinyaldeki tüm dinamik özniteliklerden (workload, pod_names, service_ids) kimliği çözer."""
        # 1. Açık servis / workload alanı kontrolü
        for field in ('workload', 'service_name', 'service', 'app', 'component'):
            val = s.get(field)
            if val and str(val).strip().lower() not in ('', 'none', 'unknown', 'null'):
                return str(val).strip()

        # 2. Kubernetes Pod isimlerinden dinamik iş yükü (workload) normalizasyonu
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

            # Sinyalde kayıtlı pod-template-hash varsa doğrudan hash öncesini al
            for h in hashes:
                if f"-{h}-" in pod_str:
                    workload_part = pod_str.split(f"-{h}-")[0]
                    if workload_part:
                        return workload_part

            # Standart K8s regex budama
            normalized = _RE_K8S_POD_SUFFIX.sub("", pod_str)
            if normalized and normalized != pod_str:
                return normalized

        # 3. Kurumsal Service ID (OpenSearch'ten gelen tcmonit_retag_tcserviceid)
        s_ids = s.get('service_ids')
        if isinstance(s_ids, (list, tuple, set)) and s_ids:
            first_id = str(next(iter(s_ids))).strip()
            if first_id and first_id.lower() not in ('', 'none', 'unknown', 'null'):
                return f"service_{first_id}"

        for sid_field in ('service_id', 'tcserviceid', 'tcmonit_retag_tcserviceid'):
            val = s.get(sid_field)
            if val and str(val).strip().lower() not in ('', 'none', 'unknown', 'null'):
                return f"service_{str(val).strip()}"

        # 4. Namespace
        ns = s.get('namespace')
        if ns and str(ns).strip().lower() not in ('', 'none', 'unknown', 'null'):
            return str(ns).strip()

        return ""

    def _rollup_signals(self, signals: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Aynı workload ve hata şablonundaki pod tekrarlarını tek bir özet sinyalde birleştirir."""
        groups = defaultdict(list)
        for s in signals:
            svc = self._extract_svc(s)
            tmpl = str(s.get('template') or s.get('template_id') or s.get('message') or '')[:100]
            group_key = (svc, tmpl) if svc else (str(s.get('signal_id') or id(s)), tmpl)
            groups[group_key].append(s)

        rolled_up = []
        for (svc, tmpl), sig_list in groups.items():
            base = dict(sig_list[0])
            all_pods = set()
            all_service_ids = set()
            total_count = 0
            min_first_seen = float('inf')
            max_last_seen = 0

            for item in sig_list:
                if isinstance(item.get('pod_names'), (list, tuple, set)):
                    all_pods.update(str(p) for p in item['pod_names'] if p)
                pod = item.get('pod_name') or item.get('identity') or item.get('host')
                if pod and str(pod).strip().lower() not in ('', 'none', 'unknown', 'null'):
                    all_pods.add(str(pod))

                if isinstance(item.get('service_ids'), (list, tuple, set)):
                    all_service_ids.update(str(sid) for sid in item['service_ids'] if sid)
                sid = item.get('service_id')
                if sid:
                    all_service_ids.add(str(sid))

                total_count += int(item.get('count', 1))
                fs = int(item.get('first_seen_ms', 0) or 0)
                ls = int(item.get('last_seen_ms', 0) or 0)
                if fs > 0 and fs < min_first_seen: min_first_seen = fs
                if ls > max_last_seen: max_last_seen = ls

            # Çözümlenen dinamik servis adını üst katmanlara da aktar
            if svc:
                base['service_name'] = svc
                base['workload'] = svc

            base['pod_names'] = sorted(list(all_pods))
            base['affected_pod_count'] = len(all_pods)
            base['service_ids'] = sorted(list(all_service_ids))
            base['count'] = total_count
            if min_first_seen != float('inf'): base['first_seen_ms'] = min_first_seen
            if max_last_seen > 0: base['last_seen_ms'] = max_last_seen
            base['aggregated_signal_ids'] = [s.get('signal_id') for s in sig_list if s.get('signal_id')]
            rolled_up.append(base)

        return rolled_up

    def correlate(self, signals: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
        consolidated = self._rollup_signals(signals)
        valid_rows = []
        for s in consolidated:
            if signal_time(s) is None or signal_time(s, 'last_seen_ms') is None:
                continue
            text = str(s.get('template') or s.get('text') or s.get('message') or '')
            s['_cached_text_lower'] = text.lower()
            s['_cached_type'] = self._extract_type(text, str(s.get('alarm_type') or ''), int(s.get('severity_min', 7)))
            s['_cached_svc'] = self._extract_svc(s)
            valid_rows.append(s)

        rows = sorted(valid_rows, key=signal_order)
        edges = []

        for i, a in enumerate(rows):
            for b in rows[i + 1:]:
                gap = max(0, int(b.get('first_seen_ms', 0)) - int(a.get('last_seen_ms', 0)))
                if gap > self.max_gap_ms:
                    break

                at, bt = a['_cached_type'], b['_cached_type']
                asvc, bsvc = a['_cached_svc'], b['_cached_svc']

                # AYNI ŞABLON KONTROLÜ: Aynı hata şablonu kendi kendisinin nedeni olamaz
                if asvc and asvc == bsvc and a.get('template_id') and a.get('template_id') == b.get('template_id'):
                    continue

                evidence = []
                score = 0.0
                direction = (a, b)
                relation = None

                # 1. KURAL: Log İçi Karşılıklı Servis Referansı (Çapraz Servis Bağımlılığı)
                if asvc and len(asvc) >= 3 and asvc.lower() in b['_cached_text_lower']:
                    score = 0.78
                    direction = (a, b)
                    evidence = ['log_içi_bağımlılık_referansı', 'zaman_yakınlığı']
                elif bsvc and len(bsvc) >= 3 and bsvc.lower() in a['_cached_text_lower']:
                    score = 0.78
                    direction = (b, a)
                    evidence = ['log_içi_bağımlılık_referansı', 'zaman_yakınlığı']

                # 2. KURAL: Aynı Servis İçi Semantik Nedensellik (Kök Hata -> Türev Semptom)
                elif asvc and bsvc and asvc == bsvc and gap <= 180_000:
                    if bt in ROOT_TYPES and at in PROPAGATION_TYPES:
                        score = 0.75
                        direction = (b, a)
                        evidence = ['semantik_kök_neden_önceliği', 'artçı_semptom_tetiklenmesi']
                    elif at in ROOT_TYPES and bt in PROPAGATION_TYPES:
                        score = 0.75
                        direction = (a, b)
                        evidence = ['semantik_kök_neden_önceliği', 'artçı_semptom_tetiklenmesi']
                    elif at in ROOT_TYPES and bt in ROOT_TYPES and at != bt:
                        score = 0.65
                        direction = (a, b)
                        evidence = ['farklı_altyapı_katmanı_çöküşü']

                # 3. KURAL: Servis Topolojisi (DAG Graph)
                if self.topology and asvc and bsvc and asvc != bsvc:
                    relation = self.topology.relation(asvc, bsvc, max_hops=2)
                    if relation:
                        root_svc, dep_svc = relation['root'], relation['dependent']
                        hops = relation['hops']
                        root_sig = a if asvc == root_svc else b
                        dep_sig = b if bsvc == dep_svc else a
                        if int(root_sig.get('first_seen_ms', 0)) <= int(dep_sig.get('first_seen_ms', 0)) + 60_000:
                            topo_score = 0.82 if hops == 1 else 0.72
                            if topo_score > score:
                                score = topo_score
                                direction = (root_sig, dep_sig)
                                evidence = ['servis_bağımlılığı_topolojisi', f'{hops}_hop']

                if score >= 0.55 and evidence:
                    src, dst = direction
                    edge = {
                        'source': src['signal_id'],
                        'target': dst['signal_id'],
                        'score': round(min(1.0, score), 2),
                        'evidence': evidence,
                        'time_gap_ms': abs(int(dst.get('first_seen_ms', 0)) - int(src.get('first_seen_ms', 0))),
                        'source_service': src['_cached_svc'] or 'unknown_service',
                        'target_service': dst['_cached_svc'] or 'unknown_service'
                    }
                    if relation:
                        edge['dependency_path'] = relation.get('path', [])
                    edges.append(edge)

        best = {}
        for e in edges:
            k = (e['source'], e['target'])
            if k not in best or e['score'] > best[k]['score']:
                best[k] = e
        return list(best.values())