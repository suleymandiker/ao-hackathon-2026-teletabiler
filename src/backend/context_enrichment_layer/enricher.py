from __future__ import annotations
from typing import Any, Dict, Iterable, List
from collections import defaultdict


class ContextEnricher:
    """Genel loglar ve mikroservis/K8s mimarisi için bağlam zenginleştirme katmanı."""

    def __init__(self, topology=None):
        self.topology = topology

    @staticmethod
    def _svc(r: Dict[str, Any]) -> str:
        """Servis / iş yükü adını genel fallback sırasıyla çözer."""
        svc = (
            r.get('service_name')
            or r.get('service')
            or r.get('workload')
            or r.get('app')
            or r.get('component')
            or r.get('logger')
            or ''
        )
        return str(svc).strip()

    def enrich(self, incidents: Iterable[Dict[str, Any]], signals: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
        by_id = {s['signal_id']: s for s in signals}
        out = []

        for inc in incidents:
            rows = [by_id[x] for x in inc.get('signal_ids', []) if x in by_id]
            x = dict(inc)

            # 1. K8s, Konteyner ve Altyapı Varlıklarının Çıkarılması
            hosts = sorted({
                h for r in rows
                for h in (r.get('hosts') or [r.get('host'), r.get('node'), r.get('node_name')])
                if h not in (None, 'unknown', '')
            })

            # 17 podun tamamını pod_names dahil eksiksiz topla
            pods_set = set()
            for r in rows:
                if isinstance(r.get('pod_names'), (list, tuple, set)):
                    pods_set.update(str(p) for p in r['pod_names'] if p and p != 'unknown')
                for p in (r.get('pods') or [r.get('pod'), r.get('pod_name')]):
                    if p and p != 'unknown':
                        pods_set.add(str(p))
            pods = sorted(pods_set)

            containers = sorted({
                c for r in rows
                for c in (r.get('containers') or [r.get('container'), r.get('container_name')])
                if c not in (None, 'unknown', '')
            })

            namespaces = sorted({
                ns for r in rows
                for ns in (r.get('namespaces') or [r.get('namespace')])
                if ns not in (None, 'unknown', '')
            })

            services = sorted({self._svc(r) for r in rows if self._svc(r) not in ('', 'unknown')})
            components = sorted({
                str(r.get('component') or r.get('logger'))
                for r in rows
                if r.get('component') or r.get('logger')
            })

            # 2. Topoloji ve Servis Bağımlılık Yolları
            deps = []
            if self.topology and services:
                for src in services:
                    for dst in services:
                        if src == dst:
                            continue
                        try:
                            p = self.topology.dependency_path(src, dst, max_hops=4)
                            if p and len(p) > 1:
                                edge_meta = []
                                if hasattr(self.topology, 'edge_meta'):
                                    edge_meta = [self.topology.edge_meta.get((p[i], p[i + 1]), {}) for i in range(len(p) - 1)]
                                deps.append({
                                    'dependent': src,
                                    'root': dst,
                                    'path': p,
                                    'hops': len(p) - 1,
                                    'edge_metadata': edge_meta
                                })
                        except Exception:
                            continue

            # 3. Host Envanteri
            inventory = []
            if self.topology:
                for host in hosts:
                    try:
                        inv = self.topology.host_context(host)
                        if inv:
                            inventory.append(inv)
                    except Exception:
                        continue

            # 4. Şablonların Önceliklendirilmesi ve Tekilleştirilmesi
            def signal_prio(r):
                sev = int(r.get('severity_min', 7))
                is_root = 0 if r.get('signal_id') == inc.get('probable_root_signal') else 1
                return (is_root, sev, -int(r.get('count', 1)))

            sorted_rows = sorted(rows, key=signal_prio)

            unique_templates = {}
            for r in sorted_rows:
                tid = str(r.get('template_id') or r.get('signal_id') or 'unknown')
                cnt = int(r.get('count', 1))

                if tid in unique_templates:
                    unique_templates[tid]['count'] += cnt
                else:
                    unique_templates[tid] = {
                        'template_id': tid,
                        'alarm_type': r.get('alarm_type'),
                        'template': r.get('template') or r.get('template_pattern') or r.get('text') or r.get('message', ''),
                        'count': cnt,
                        'service': self._svc(r),
                        'severity': r.get('severity_min', 3),
                        'source_severity_max': r.get('source_severity_max')
                    }

            templates_payload = list(unique_templates.values())[:20]

            # 5. Incident Bağlamını Zenginleştir
            x['description'] = inc.get('root_cause_template') or inc.get('description', '')
            x['context'] = {
                'services': services,
                'components': components,
                'namespaces': namespaces,
                'pods': pods,
                'containers': containers,
                'hosts': hosts,
                'data_centers': sorted({v for r in rows for v in (r.get('data_centers') or [])}),
                'racks': sorted({v for r in rows for v in (r.get('racks') or [])}),
                'business_criticalities': sorted({v for r in rows for v in (r.get('business_criticalities') or [])}),
                'host_inventory': inventory,
                'dependencies': deps[:40],
                'templates': templates_payload
            }
            out.append(x)

        return out