from __future__ import annotations

import json
import os
import re
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from ai_engine import call_ai_agent, load_prompt, safe_usage, MODELS_CONFIG
from rca_layer.evidence import RCAEvidenceSelector, EvidenceBudgetError, serialize
from rca_layer.expert_output import RCAExpertOutputValidator, ExpertOutputError, response_format
from analysis_time import time_key, order_key
from pipeline_observation import PipelineObserver
from data_paths import policy_data_dir

_RE_INFRA_ROOT = re.compile(
    r"\b(operationalerror|databaseerror|sqlexception|deadlock|closed the connection|"
    r"connection refused|broken pipe|connection reset|network is unreachable|"
    r"disk full|no space left|oom|oomkilled|panic:|traceback|nullpointerexception)\b",
    re.I
)

_RE_SYMPTOM = re.compile(
    r"\b(timeout|timed out|rollback|transaction|retry|aborted|failed to process)\b",
    re.I
)


class ClosedLoopLearningMemory:
    """P2 - Closed-loop Learning: Operatör geri bildirimlerini okur ve analiz motoruna enjekte eder."""

    def __init__(self):
        for candidate in (
            Path("data/feedback/rca_learning_memory.json"),
            Path("data/policy/rca_learning_memory.json"),
            policy_data_dir() / "rca_learning_memory.json",
        ):
            if candidate.parent.exists():
                self.file_path = candidate
                break
        else:
            self.file_path = Path("data/feedback/rca_learning_memory.json")

    def load_memory(self) -> Dict[str, Any]:
        if not self.file_path.exists():
            return {}
        try:
            with open(self.file_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}

    def get_confirmed_rules(self) -> List[Dict[str, Any]]:
        data = self.load_memory()
        return [
            entry for entry in data.values()
            if isinstance(entry, dict) and entry.get("feedback") == "ONAYLANDI"
        ]

    def match_signal(self, text: str) -> Optional[Dict[str, Any]]:
        text_lower = text.lower()
        for rule in self.get_confirmed_rules():
            diag = str(rule.get("diagnosis_summary", "")).lower()
            root_sig = str(rule.get("confirmed_root_signal", "")).lower()
            keywords = [w for w in re.split(r"\W+", diag) if len(w) > 4]
            matches = sum(1 for kw in keywords if kw in text_lower)
            if matches >= 2 or (root_sig and root_sig in text_lower):
                return rule
        return None

    def get_prompt_context(self) -> str:
        confirmed = self.get_confirmed_rules()
        if not confirmed:
            return ""
        
        lines = [
            "\n[CLOSED-LOOP OPERATÖR GERİ BİLDİRİM BELLEĞİ (DOĞRULANMIŞ ALTIN KURALLAR)]",
            "Aşağıdaki kök neden teşhisleri insan operatör/SRE tarafından geçmişte kesin olarak ONAYLANMIŞTIR:",
        ]
        for idx, rule in enumerate(confirmed[-4:], start=1):
            lines.append(
                f"{idx}. Teşhis: {rule.get('diagnosis_summary', '')} | "
                f"Kök Sinyal: {rule.get('confirmed_root_signal', 'Belirtilmedi')} | "
                f"Operatör Notu: {rule.get('notes') or 'Doğrulandı'}"
            )
        lines.append(
            "KURAL: Mevcut olay bu doğrulanmış kalıplarla uyuşuyorsa, aynı kök nedeni önceliklendir ve güven skorunu artır.\n"
        )
        return "\n".join(lines)


class RCAAnalysisCache:
    """Üretilen AI analizini vaka bazında saklar ve mükerrer çağrıları engeller."""

    def __init__(self):
        try:
            self.file_path = policy_data_dir() / "rca_analysis_cache.json"
        except Exception:
            self.file_path = Path("data/policy/rca_analysis_cache.json")

    def get(self, incident_id: str) -> Optional[Dict[str, Any]]:
        if not self.file_path.exists():
            return None
        try:
            with open(self.file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                return data.get(incident_id)
        except Exception:
            return None

    def put(self, incident_id: str, analysis: Dict[str, Any]) -> None:
        data = {}
        if self.file_path.exists():
            try:
                with open(self.file_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except Exception:
                data = {}
        data[incident_id] = analysis
        try:
            self.file_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.file_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception:
            pass


class DeterministicRCAEngine:
    def __init__(self):
        self.learning_memory = ClosedLoopLearningMemory()

    def analyze(
        self,
        incidents: Iterable[Dict[str, Any]],
        correlations: Iterable[Dict[str, Any]],
        signals: Iterable[Dict[str, Any]] = (),
        topology: Optional[Any] = None
    ) -> List[Dict[str, Any]]:
        signal_map = {s["signal_id"]: s for s in signals}
        edges = list(correlations)
        results = []

        for incident in incidents:
            signal_ids = set(incident.get("signal_ids", []))
            probable_root = incident.get("probable_root_signal")

            local_edges = [
                e for e in edges
                if e.get("source") in signal_ids and e.get("target") in signal_ids
            ]
            outgoing = {sid: 0 for sid in signal_ids}
            for edge in local_edges:
                outgoing[edge["source"]] = outgoing.get(edge["source"], 0) + 1

            def rca_rank_score(sid: str) -> tuple:
                s = signal_map.get(sid, {})
                text = str(s.get("template") or s.get("text") or s.get("message") or "")
                sev = int(s.get("severity_min", 7))

                matched_rule = self.learning_memory.match_signal(text)
                is_operator_confirmed = 0 if matched_rule else 1

                is_builder_choice = 0 if sid == probable_root else 1
                is_synthetic = 1 if sid.startswith("monitor:") or "baseline" in sid else 0
                is_infra = 0 if _RE_INFRA_ROOT.search(text) else 1
                is_symptom = 1 if _RE_SYMPTOM.search(text) else 0

                out_deg = outgoing.get(sid, 0)
                q_score = float(s.get("qualification_score", 0))

                return (
                    is_synthetic,
                    is_operator_confirmed,
                    is_builder_choice,
                    is_infra,
                    is_symptom,
                    sev,
                    -out_deg,
                    time_key(s),
                    -q_score,
                    order_key(s), sid
                )

            ranked = sorted(signal_ids, key=rca_rank_score)

            candidates = []
            for rank_idx, sid in enumerate(ranked[:5]):
                signal = signal_map.get(sid, {})
                evidence = list(signal.get("qualification_evidence", []))
                out_deg = outgoing.get(sid, 0)
                text = str(signal.get("template") or signal.get("text") or "")

                matched_rule = self.learning_memory.match_signal(text)
                if matched_rule:
                    evidence.append("kapalı_döngü_operatör_onaylı")

                if out_deg > 0:
                    evidence.append(f"nedensel_tetikleyici_{out_deg}_bag")

                if matched_rule and rank_idx == 0:
                    final_score = 0.99
                else:
                    base_prob = max(0.40, 0.85 - (rank_idx * 0.12))
                    deg_boost = min(0.15, out_deg * 0.03)
                    final_score = round(min(0.98, base_prob + deg_boost), 3)

                candidates.append({
                    "signal_id": sid,
                    "score": final_score,
                    "evidence": evidence,
                })

            primary_candidate = candidates[0]["signal_id"] if candidates else None

            results.append({
                "incident_id": incident["incident_id"],
                "rca_status": "hipotez",
                "analysis_source": "deterministik",
                "root_cause_candidates": candidates,
                "confirmed_root_cause": primary_candidate,
            })
        return results


class ExpertRCAEngine:
    def __init__(self, enabled: bool = True, max_incidents: int = 5):
        self.enabled = enabled
        self.max_incidents = max_incidents
        self.base = DeterministicRCAEngine()
        self.cache = RCAAnalysisCache()
        self.learning_memory = ClosedLoopLearningMemory()
        self.last_case_analysis: Dict[str, Any] | None = None
        self.last_ai_error: str | None = None
        self.last_expert_diagnostics: Dict[str, Any] = {}

    def analyze(
        self,
        incidents: Iterable[Dict[str, Any]],
        correlations: Iterable[Dict[str, Any]],
        signals: Iterable[Dict[str, Any]] = (),
        *,
        topology: Optional[Any] = None,
        observer: PipelineObserver | None = None
    ) -> List[Dict[str, Any]]:
        incidents_list = list(incidents)
        correlations_list = list(correlations)
        signals_list = list(signals)

        self.last_case_analysis = None
        self.last_ai_error = None
        self.last_expert_diagnostics = {}

        base_results = self.base.analyze(incidents_list, correlations_list, signals_list, topology=topology)
        if not self.enabled or not incidents_list:
            return base_results

        primary_inc = sorted(incidents_list, key=lambda x: int(x.get("severity_min", 7)))[0]
        inc_id = primary_inc.get("incident_id")
        is_new = primary_inc.get("is_new", True)

        if not is_new:
            cached_report = self.cache.get(inc_id)
            if cached_report:
                print(f"[RCA AI BYPASS] Problem halen aktif ({inc_id}) | Tekrar AI'ya gidilmedi | Token=0")
                self.last_case_analysis = cached_report
                self.last_expert_diagnostics = {
                    "status": "incident_active_cached",
                    "ai_calls": 0,
                    "prompt_tokens": 0,
                    "completion_tokens": 0,
                    "total_tokens": 0,
                    "duration_seconds": 0.0
                }
                for result in base_results:
                    if result.get("incident_id") == inc_id:
                        result["analysis_source"] = "qwen_aktif_vaka"
                return base_results

        try:
            prompt, schema = load_prompt('rca_expert.md'), response_format()

            feedback_context = self.learning_memory.get_prompt_context()
            if feedback_context:
                prompt = f"{prompt}\n\n{feedback_context}"

            overhead = prompt + serialize(schema)
            pack = RCAEvidenceSelector().build(
                incidents_list, correlations_list, signals_list, base_results,
                max_incidents=self.max_incidents,
                overhead_chars=len(overhead),
                overhead_bytes=len(overhead.encode('utf-8'))
            )
            self.last_expert_diagnostics = dict(pack.diagnostics)
            if observer is not None:
                observer('expert_input', pack)
            print('[RCA CONTEXT] ' + ' | '.join(f'{key}={value}' for key, value in pack.diagnostics))

            reply, duration, usage = call_ai_agent(
                'Ajan_2_RCA_Expert', prompt, pack.serialized, temperature=0.0,
                max_tokens=max(256, min(1600, int(os.getenv('QWEN_RCA_MAX_TOKENS', '1400')))),
                response_format=schema, return_usage=True
            )

            print("\n=== RAW AI REPLY ===")
            print(reply)
            print("====================\n")
            
            usage = safe_usage(usage)
            duration = duration if type(duration) in (int, float) and 0 <= duration < float('inf') else 0.0
            self.last_expert_diagnostics.update(usage, duration_seconds=round(duration, 3))
            print('[RCA AI] ' + ' | '.join(f'{key}={value}' for key, value in usage.items()) + f' | duration={duration:.3f}s')

            parsed = RCAExpertOutputValidator().validate(reply, pack, usage['finish_reason'])

            confirmed_rules = self.learning_memory.get_confirmed_rules()
            closed_loop_applied = False
            if confirmed_rules:
                diag_lower = str(parsed.get('durum_ozeti', '')).lower() + " " + str(parsed.get('kok_neden_hipotezi', '')).lower()
                for rule in confirmed_rules:
                    rule_diag = str(rule.get('diagnosis_summary', '')).lower()
                    kw_matches = sum(1 for w in re.split(r'\W+', rule_diag) if len(w) > 4 and w in diag_lower)
                    if kw_matches >= 2:
                        parsed['guven'] = min(0.99, max(float(parsed.get('guven', 0.85)), 0.95))
                        parsed.setdefault('karar_gerekcesi', []).append(
                            "Operatör onaylı geçmiş vaka belleğiyle (Closed-loop) tam uyum doğrulandı."
                        )
                        closed_loop_applied = True
                        break

            self.last_case_analysis = dict(
                parsed,
                analysis_source='qwen_uzman_ogrenen' if closed_loop_applied else 'qwen_uzman',
                closed_loop_learned=closed_loop_applied,
                ai_usage=usage,
                analysis_model=MODELS_CONFIG['Ajan_2_RCA_Expert']['model_id'],
                ai_duration_seconds=round(duration, 3),
                expert_diagnostics=dict(self.last_expert_diagnostics)
            )

            self.cache.put(inc_id, self.last_case_analysis)

            for result in base_results:
                if result.get("incident_id") == inc_id:
                    result['analysis_source'] = 'qwen_destekli'
        except (EvidenceBudgetError, ExpertOutputError) as error:
            self.last_ai_error = 'Uzman RCA kabul edilmedi; deterministik RCA korunuyor.'
            self.last_expert_diagnostics['status'] = str(error)
            print('[RCA] expert_rejected; deterministic RCA retained')
        except Exception:
            self.last_ai_error = 'Uzman RCA tamamlanamadı; deterministik RCA korunuyor.'
            self.last_expert_diagnostics['status'] = 'expert_unavailable'
            print('[RCA] expert_unavailable; deterministic RCA retained')

        return base_results