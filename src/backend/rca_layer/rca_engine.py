from __future__ import annotations

import json
import os
import re
from typing import Any, Dict, Iterable, List

from ai_engine import call_ai_agent, load_prompt


class DeterministicRCAEngine:
    """LLM olmasa da çalışan basit ve açıklanabilir RCA hipotezi."""

    def analyze(
        self,
        incidents: Iterable[Dict[str, Any]],
        correlations: Iterable[Dict[str, Any]],
        signals: Iterable[Dict[str, Any]] = (),
    ) -> List[Dict[str, Any]]:
        signal_map = {s["signal_id"]: s for s in signals}
        edges = list(correlations)
        results = []

        for incident in incidents:
            signal_ids = set(incident.get("signal_ids", []))
            local_edges = [
                e for e in edges
                if e.get("source") in signal_ids and e.get("target") in signal_ids
            ]
            outgoing = {sid: 0 for sid in signal_ids}
            for edge in local_edges:
                outgoing[edge["source"]] = outgoing.get(edge["source"], 0) + 1

            ranked = sorted(
                signal_ids,
                key=lambda sid: (
                    int(signal_map.get(sid, {}).get("severity_min", 7)),
                    int(signal_map.get(sid, {}).get("first_seen_ms", 0)),
                    -outgoing.get(sid, 0),
                    -float(signal_map.get(sid, {}).get("qualification_score", 0)),
                ),
            )

            candidates = []
            for sid in ranked[:5]:
                signal = signal_map.get(sid, {})
                evidence = list(signal.get("qualification_evidence", []))
                if outgoing.get(sid):
                    evidence.append("sonraki_sinyallerle_korelasyon")
                candidates.append({
                    "signal_id": sid,
                    "score": round(min(
                        1.0,
                        0.45
                        + 0.15 * outgoing.get(sid, 0)
                        + 0.25 * float(signal.get("qualification_score", 0)),
                    ), 3),
                    "evidence": evidence,
                })

            results.append({
                "incident_id": incident["incident_id"],
                "rca_status": "hipotez",
                "analysis_source": "deterministik",
                "root_cause_candidates": candidates,
                "confirmed_root_cause": None,
            })
        return results


def _extract_json(text: str) -> Dict[str, Any] | None:
    if not text:
        return None
    cleaned = text.strip()
    cleaned = re.sub(r"^```(?:json)?\\s*", "", cleaned, flags=re.I)
    cleaned = re.sub(r"\\s*```$", "", cleaned)
    try:
        value = json.loads(cleaned)
        return value if isinstance(value, dict) else None
    except Exception:
        start, end = cleaned.find("{"), cleaned.rfind("}")
        if start >= 0 and end > start:
            try:
                value = json.loads(cleaned[start:end + 1])
                return value if isinstance(value, dict) else None
            except Exception:
                pass
    return None


class ExpertRCAEngine:
    """Deterministik RCA + tüm case için tek Qwen uzman yorumu.

    Qwen karar üretmez; deterministik pipeline'ın oluşturduğu olayları ve kanıtları
    yorumlar. Qwen erişilemezse pipeline deterministik RCA ile devam eder.
    """

    def __init__(self, enabled: bool = True, max_incidents: int = 5):
        self.enabled = enabled
        self.max_incidents = max_incidents
        self.base = DeterministicRCAEngine()
        self.last_case_analysis: Dict[str, Any] | None = None
        self.last_ai_error: str | None = None

    @staticmethod
    def _compact_signal(signal: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "signal_id": signal.get("signal_id"),
            "template": str(signal.get("template", ""))[:220],
            "count": signal.get("count"),
            "severity_min": signal.get("severity_min"),
            "qualification_score": signal.get("qualification_score"),
            "scope": signal.get("scope"),
            "service_name": signal.get("service_name"),
            "component": signal.get("component"),
            "host": signal.get("host"),
            "alarm_type": signal.get("alarm_type"),
            "source_severity_max": signal.get("source_severity_max"),
            "data_centers": signal.get("data_centers", []),
            "racks": signal.get("racks", []),
            "evidence": signal.get("qualification_evidence", []),
        }

    def analyze(
        self,
        incidents: Iterable[Dict[str, Any]],
        correlations: Iterable[Dict[str, Any]],
        signals: Iterable[Dict[str, Any]] = (),
    ) -> List[Dict[str, Any]]:
        incidents = list(incidents)
        correlations = list(correlations)
        signals = list(signals)
        self.last_case_analysis = None
        self.last_ai_error = None

        base_results = self.base.analyze(incidents, correlations, signals)

        if not self.enabled:
            print("[RCA] Qwen uzman analizi devre dışı; deterministik RCA kullanılıyor.")
            return base_results
        if not incidents:
            print("[RCA] Olay adayı yok; Qwen çağrısı yapılmadı.")
            return base_results

        selected_incidents = incidents[:self.max_incidents]
        selected_ids = {
            sid
            for incident in selected_incidents
            for sid in incident.get("signal_ids", [])
        }
        signal_map = {s["signal_id"]: s for s in signals}
        selected_signals = [
            self._compact_signal(signal_map[sid])
            for sid in selected_ids
            if sid in signal_map
        ][:20]
        selected_edges = [
            edge for edge in correlations
            if edge.get("source") in selected_ids and edge.get("target") in selected_ids
        ][:30]

        payload = {
            "olaylar": [{
                "incident_id": inc.get("incident_id"),
                "severity_min": inc.get("severity_min"),
                "confidence": inc.get("confidence"),
                "event_count": inc.get("event_count"),
                "signal_count": inc.get("signal_count"),
                "signal_ids": inc.get("signal_ids", []),
                "probable_root": inc.get("probable_root", {}),
                "context": {
                    "services": (inc.get("context", {}) or {}).get("services", []),
                    "hosts": (inc.get("context", {}) or {}).get("hosts", [])[:12],
                    "data_centers": (inc.get("context", {}) or {}).get("data_centers", []),
                    "racks": (inc.get("context", {}) or {}).get("racks", []),
                    "business_criticalities": (inc.get("context", {}) or {}).get("business_criticalities", []),
                    "dependencies": (inc.get("context", {}) or {}).get("dependencies", [])[:12],
                },
            } for inc in selected_incidents],
            "sinyaller": selected_signals,
            "korelasyonlar": selected_edges,
            "deterministik_rca": base_results,
        }

        print(
            f"[RCA] Qwen uzman case analizi başlatılıyor | "
            f"olay={len(selected_incidents)} | sinyal={len(selected_signals)} | "
            f"korelasyon={len(selected_edges)}"
        )
        reply, duration, usage = call_ai_agent(
            "Ajan_2_RCA_Expert",
            load_prompt("rca_expert.md"),
            json.dumps(payload, ensure_ascii=False),
            temperature=0.0,
            max_tokens=max(256, min(1600, int(os.getenv("QWEN_RCA_MAX_TOKENS", "1400")))),
            response_format={"type": "json_object"},
            return_usage=True,
        )
        parsed = _extract_json(reply)
        # Gateway/API errors are JSON too; they are not a successful RCA response.
        if parsed and not any(k in parsed for k in ("durum_ozeti", "kok_neden_hipotezi", "guven", "nedensellik_durumu")):
            parsed = None

        if parsed:
            self.last_case_analysis = {
                **parsed,
                "analysis_source": "qwen_uzman",
                "analysis_model": "saka__glm-53-flash-dynamo-saka",
                "ai_usage": usage,
                "ai_duration_seconds": round(duration, 3),
            }
            for result in base_results:
                result["analysis_source"] = "qwen_destekli"
            print(f"[RCA] Qwen uzman case analizi tamamlandı | sure={duration:.2f}s")
        else:
            self.last_ai_error = str(reply)[:500]
            print(f"[RCA] Qwen çıktısı kullanılamadı; deterministik RCA korunuyor | {self.last_ai_error}")

        return base_results
