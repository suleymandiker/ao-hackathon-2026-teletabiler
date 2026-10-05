from __future__ import annotations

import os
from typing import Any, Dict, Iterable, List

from ai_engine import call_ai_agent, load_prompt, safe_usage, MODELS_CONFIG
from rca_layer.evidence import RCAEvidenceSelector, EvidenceBudgetError, serialize
from rca_layer.expert_output import RCAExpertOutputValidator, ExpertOutputError, response_format
from analysis_time import time_key, order_key


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
                    time_key(signal_map.get(sid, {})),
                    -outgoing.get(sid, 0),
                    -float(signal_map.get(sid, {}).get("qualification_score", 0)),
                    order_key(signal_map.get(sid, {})), sid,
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


class ExpertRCAEngine:
    """Deterministic RCA plus one bounded, validated expert interpretation per case."""

    def __init__(self, enabled: bool = True, max_incidents: int = 5):
        self.enabled = enabled
        self.max_incidents = max_incidents
        self.base = DeterministicRCAEngine()
        self.last_case_analysis: Dict[str, Any] | None = None
        self.last_ai_error: str | None = None
        self.last_expert_diagnostics: Dict[str, Any] = {}

    def analyze(self, incidents: Iterable[Dict[str, Any]], correlations: Iterable[Dict[str, Any]],
                signals: Iterable[Dict[str, Any]] = ()) -> List[Dict[str, Any]]:
        incidents, correlations, signals = list(incidents), list(correlations), list(signals)
        self.last_case_analysis = None
        self.last_ai_error = None
        self.last_expert_diagnostics = {}
        base_results = self.base.analyze(incidents, correlations, signals)
        if not self.enabled or not incidents:
            return base_results
        try:
            prompt, schema = load_prompt('rca_expert.md'), response_format()
            overhead = prompt + serialize(schema)
            pack = RCAEvidenceSelector().build(incidents, correlations, signals, base_results,
                max_incidents=self.max_incidents, overhead_chars=len(overhead), overhead_bytes=len(overhead.encode('utf-8')))
            self.last_expert_diagnostics = dict(pack.diagnostics)
            print('[RCA CONTEXT] ' + ' | '.join(f'{key}={value}' for key, value in pack.diagnostics))
            reply, duration, usage = call_ai_agent(
                'Ajan_2_RCA_Expert', prompt, pack.serialized, temperature=0.0,
                max_tokens=max(256, min(1600, int(os.getenv('QWEN_RCA_MAX_TOKENS', '1400')))),
                response_format=schema, return_usage=True)
            usage = safe_usage(usage)
            duration = duration if type(duration) in (int, float) and 0 <= duration < float('inf') else 0.0
            self.last_expert_diagnostics.update(usage, duration_seconds=round(duration, 3))
            print('[RCA AI] ' + ' | '.join(f'{key}={value}' for key, value in usage.items()) + f' | duration={duration:.3f}s')
            parsed = RCAExpertOutputValidator().validate(reply, pack, usage['finish_reason'])
            self.last_case_analysis = dict(parsed, analysis_source='qwen_uzman', ai_usage=usage,
                                          analysis_model=MODELS_CONFIG['Ajan_2_RCA_Expert']['model_id'],
                                          ai_duration_seconds=round(duration, 3),
                                          expert_diagnostics=dict(self.last_expert_diagnostics))
            # Retain the existing supported/unsupported annotation, without
            # changing any deterministic candidates, scores, evidence or ordering.
            for result in base_results:
                result['analysis_source'] = 'qwen_destekli'
        except (EvidenceBudgetError, ExpertOutputError) as error:
            self.last_ai_error = 'Uzman RCA kabul edilmedi; deterministik RCA korunuyor.'
            self.last_expert_diagnostics['status'] = str(error)
            print('[RCA] expert_rejected; deterministic RCA retained')
        except Exception:
            # No raw gateway reply, template, mapping, URL or exception body in logs.
            self.last_ai_error = 'Uzman RCA tamamlanamadı; deterministik RCA korunuyor.'
            self.last_expert_diagnostics['status'] = 'expert_unavailable'
            print('[RCA] expert_unavailable; deterministic RCA retained')
        return base_results
