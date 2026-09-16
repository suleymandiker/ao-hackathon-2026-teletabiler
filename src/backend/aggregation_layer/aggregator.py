from __future__ import annotations
from collections import defaultdict
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Tuple
import re


def _ts_ms(value: Any) -> int:
    if isinstance(value, datetime):
        dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    if isinstance(value, (int, float)):
        return int(value if value > 10_000_000_000 else value * 1000)
    if isinstance(value, str) and value:
        try:
            return int(datetime.fromisoformat(value.replace('Z', '+00:00')).timestamp() * 1000)
        except ValueError:
            pass
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _resource(e: Dict[str, Any], key: str) -> str:
    resource = e.get('resource') or {}
    value = e.get(key) or resource.get(key)
    return str(value).strip() if value not in (None, '') else 'unknown'

_SERVICE_PATTERNS = [
    re.compile(r"\[([a-z0-9][a-z0-9._-]*(?:service|gateway|client|scheduler|controller|kubelet|alertmanager))\]", re.I),
    re.compile(r"\b(?:service|dependency|upstream|provider)[=: ]+['\"]?(?:https?://)?([a-z0-9][a-z0-9._-]+)", re.I),
]
_JAVA_COMPONENT = re.compile(r"\b((?:org|com|io|net)\.[A-Za-z0-9_.$-]+(?:\.[A-Za-z0-9_.$-]+){2,})\b")


def _infer_identity(e: Dict[str, Any]) -> Tuple[str, str]:
    service = str(e.get('service_name') or _resource(e, 'service')).strip()
    component = str(e.get('component') or _resource(e, 'component')).strip()
    text = ' '.join(str(e.get(k) or '') for k in ('message', 'template', 'raw'))
    if service == 'unknown':
        for pat in _SERVICE_PATTERNS:
            m = pat.search(text)
            if m:
                service = m.group(1).split(':')[0]
                break
    if component == 'unknown':
        m = _JAVA_COMPONENT.search(text)
        if m:
            component = m.group(1)
    return service, component


def _severity_number(e: Dict[str, Any]) -> int:
    value = e.get('severity_number')
    if isinstance(value, (int, float)):
        return int(value)
    value = e.get('severity_text') or e.get('severity')
    return {'FATAL': 1, 'CRITICAL': 2, 'ERROR': 3, 'WARNING': 4, 'WARN': 4, 'INFO': 6, 'DEBUG': 7}.get(str(value or 'INFO').upper(), 6)


@dataclass
class SignalAggregate:
    signal_id: str
    template_id: str
    template: str
    scope: str
    window_start_ms: int
    window_end_ms: int
    count: int
    first_seen_ms: int
    last_seen_ms: int
    severity_min: int
    service_name: str
    component: str
    namespace: str
    cluster_name: str
    host: str
    hosts: List[str]
    event_ids: List[str]
    reliable_ratio: float
    burst_score: float
    timestamp_resolved: bool
    alarm_type: str
    source_severity_max: int
    source_systems: List[str]
    data_centers: List[str]
    racks: List[str]
    environments: List[str]
    business_criticalities: List[str]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class SignalAggregator:
    """TemplateEvent'leri 60 saniyelik pencerelerde sinyal adaylarına toplar."""

    def __init__(self, window_seconds: int = 60, representative_limit: int = 8):
        self.window_ms = max(1, int(window_seconds)) * 1000
        self.representative_limit = max(1, int(representative_limit))

    def aggregate(self, events: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
        groups: Dict[Tuple[Any, ...], List[Dict[str, Any]]] = defaultdict(list)
        for e in events:
            tid = str(e.get('template_id') or '').strip()
            if not tid:
                continue
            resolved = e.get('timestamp') not in (None, '')
            ts = _ts_ms(e.get('timestamp') or e.get('observed_timestamp'))
            bucket = ts - ts % self.window_ms
            service, component = _infer_identity(e)
            namespace = str(e.get('namespace') or (e.get('resource') or {}).get('namespace') or 'unknown')
            cluster = str(e.get('cluster_name') or (e.get('resource') or {}).get('cluster_name') or 'unknown')
            host = _resource(e, 'host')
            attrs = e.get('attributes') or {}
            alarm_type = str(attrs.get('alarm_type') or '').strip()
            # Structured alarms have a stable alarm_type. Keep it in the grouping key so
            # unrelated symptoms from the same service/window never collapse together.
            identity = service if service != 'unknown' else component if component != 'unknown' else host
            semantic_key = alarm_type or tid
            groups[(semantic_key, tid, identity, bucket)].append(dict(e, _signal_ts_ms=ts, _resolved=resolved,
                                                        _service=service, _component=component,
                                                        _namespace=namespace, _cluster=cluster, _host=host))

        out: List[Dict[str, Any]] = []
        for (semantic_key, tid, identity, bucket), rows in sorted(groups.items(), key=lambda item: item[0][-1]):
            times = [r['_signal_ts_ms'] for r in rows]
            count = len(rows)
            reliable = sum(bool(r.get('template_reliable', r.get('reliable', True))) for r in rows) / count
            service = rows[0]['_service']; component = rows[0]['_component']; namespace = rows[0]['_namespace']; cluster = rows[0]['_cluster']; host = rows[0]['_host']
            scope_parts = [x for x in (cluster, namespace, service) if x != 'unknown']
            scope = '/'.join(scope_parts) if scope_parts else (component if component != 'unknown' else host if host != 'unknown' else 'genel')
            burst = min(1.0, count / 10.0)
            attrs=[r.get('attributes') or {} for r in rows]
            alarm_type=str(attrs[0].get('alarm_type') or '')
            source_sevs=[]
            for a in attrs:
                try: source_sevs.append(int(a.get('source_severity')))
                except (TypeError,ValueError): pass
            def vals(key): return sorted({str(a.get(key)) for a in attrs if a.get(key) not in (None,'')})
            out.append(SignalAggregate(
                signal_id=f"sig:{tid}:{identity}:{bucket}", template_id=tid,
                template=str(rows[0].get('template') or ''), scope=scope,
                window_start_ms=bucket, window_end_ms=bucket + self.window_ms,
                count=count, first_seen_ms=min(times), last_seen_ms=max(times),
                severity_min=min(_severity_number(r) for r in rows),
                service_name=service, component=component, namespace=namespace, cluster_name=cluster, host=host,
                hosts=sorted({str(r.get('_host')) for r in rows if r.get('_host') not in (None,'unknown','')}),
                event_ids=[str(r.get('event_id') or '') for r in rows[:self.representative_limit]],
                reliable_ratio=round(reliable, 4), burst_score=round(burst, 4),
                timestamp_resolved=all(bool(r['_resolved']) for r in rows),
                alarm_type=alarm_type, source_severity_max=max(source_sevs) if source_sevs else 0,
                source_systems=vals('source_system'), data_centers=vals('data_center'), racks=vals('rack'),
                environments=vals('environment'), business_criticalities=vals('business_criticality'),
            ).to_dict())
        return out
