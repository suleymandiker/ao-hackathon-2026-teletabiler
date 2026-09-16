from __future__ import annotations
from typing import Any, Dict, Iterable
from aggregation_layer.aggregator import SignalAggregator
from signal_qualification_layer.noise_gate import SignalNoiseGate
from correlation_layer.correlator import SignalCorrelator
from incident_candidate_layer.builder import IncidentCandidateBuilder
from context_enrichment_layer.enricher import ContextEnricher
from rca_layer.rca_engine import ExpertRCAEngine
from learning_planning_layer.planner import LearningPlanner

class DownstreamAIOpsPipeline:
    """Basit final akış: Sinyal adayı -> Gürültü kapısı -> Korelasyon -> Olay -> RCA -> Plan."""
    def __init__(self, window_seconds: int = 60, use_ai_rca: bool = False):
        self.aggregator = SignalAggregator(window_seconds)
        self.noise_gate = SignalNoiseGate()
        self.correlator = SignalCorrelator()
        self.incidents = IncidentCandidateBuilder()
        self.enricher = ContextEnricher()
        self.rca = ExpertRCAEngine(enabled=use_ai_rca)
        self.planner = LearningPlanner()
        self.topology = None

    def set_context(self, topology=None):
        self.topology = topology
        self.correlator = SignalCorrelator(topology=topology)
        self.enricher = ContextEnricher(topology=topology)
        self.incidents = IncidentCandidateBuilder(topology=topology)

    def process(self, templated_events: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
        signals = self.noise_gate.qualify(self.aggregator.aggregate(templated_events))
        qualified = [s for s in signals if s.get('qualified')]
        correlations = self.correlator.correlate(qualified)
        incidents = self.enricher.enrich(self.incidents.build(signals, correlations), signals)
        rca = self.rca.analyze(incidents, correlations, signals)
        plans = self.planner.plan(incidents, rca)
        return {
            'case_analysis': self.rca.last_case_analysis,
            'case_analysis_error': self.rca.last_ai_error,
            'signals': signals, 'qualified_signals': qualified, 'correlations': correlations,
            'incidents': incidents, 'rca': rca, 'plans': plans,
            'stats': {
                'signal_candidates': len(signals), 'signals': len(signals),
                'qualified_signals': len(qualified), 'noise_suppressed': len(signals) - len(qualified),
                'correlations': len(correlations), 'incidents': len(incidents), 'rca': len(rca), 'plans': len(plans),
            },
        }
