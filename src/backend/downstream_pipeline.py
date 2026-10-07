from __future__ import annotations
from typing import Any, Dict, Iterable
from aggregation_layer.aggregator import SignalAggregator
from signal_qualification_layer.noise_gate import SignalNoiseGate
from correlation_layer.correlator import SignalCorrelator
from incident_candidate_layer.builder import IncidentCandidateBuilder
from context_enrichment_layer.enricher import ContextEnricher
from rca_layer.rca_engine import ExpertRCAEngine
from learning_planning_layer.planner import LearningPlanner
from analysis_time import AnalysisTimeContext, source_time_ms
from pipeline_observation import PipelineObserver

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

    def process(self, templated_events: Iterable[Dict[str, Any]], *, observer: PipelineObserver | None = None) -> Dict[str, Any]:
        reference = None

        def observed_events():
            nonlocal reference
            for event in templated_events:
                timestamp = source_time_ms(event.get('timestamp'))
                if timestamp is not None:
                    reference = timestamp if reference is None else max(reference, timestamp)
                yield event

        options = {'observer': observer} if observer is not None else {}
        signals = self.noise_gate.qualify(self.aggregator.aggregate(observed_events(), **options))
        # No current downstream feature needs recency/decay. Retain the factual
        # reference as local diagnostics, never fill missing event coordinates.
        analysis_time = AnalysisTimeContext(reference)
        qualified = [s for s in signals if s.get('qualified')]
        correlations = self.correlator.correlate(qualified)
        incidents = self.enricher.enrich(self.incidents.build(signals, correlations), signals)
        rca = self.rca.analyze(incidents, correlations, signals, **options)
        plans = self.planner.plan(incidents, rca)
        return {
            'analysis_time': analysis_time.to_dict(),
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
