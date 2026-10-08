# -*- coding: utf-8 -*-
import hashlib
from pathlib import Path

from template_layer.engine import DrainCandidateMiner, ValidatedTemplateRegistry
from template_layer.masker import MessageMasker
from template_layer.models import TemplateResult
from template_layer.structural_generalizer import StructuralTokenGeneralizer
from template_layer.validator import TemplateValidator


class TemplatePipeline:
    """Deterministic mask -> structural generalization -> Drain3 -> validation."""

    def __init__(
        self,
        state_path=None,
        candidate_state_path=None,
        similarity_threshold=0.5,
        max_clusters=50_000,
        max_variable_ratio=0.6,
        min_generalization_confidence=0.95,
    ):
        if candidate_state_path is None and state_path is not None:
            state = Path(state_path)
            candidate_state_path = state.with_name(f"{state.stem}_drain.bin")
        self.state_path = state_path
        self.candidate_state_path = candidate_state_path
        self.masker = MessageMasker()
        self.generalizer = StructuralTokenGeneralizer()
        self.validator = TemplateValidator(
            max_variable_ratio=max_variable_ratio,
            min_generalization_confidence=min_generalization_confidence,
        )
        self.registry = ValidatedTemplateRegistry(state_path, max_clusters)
        self.engine = DrainCandidateMiner(
            candidate_state_path,
            similarity_threshold,
            max_clusters,
        )
        self.last_decision = None

    @classmethod
    def read_only_snapshot(cls, state_path, candidate_state_path):
        """Load learning into a disposable instance whose persistence is memory-only."""
        from drain3 import TemplateMiner
        from drain3.memory_buffer_persistence import MemoryBufferPersistence

        instance = cls(state_path=None, candidate_state_path=None)
        state = Path(state_path)
        candidate = Path(candidate_state_path)
        if state.is_file():
            loaded = ValidatedTemplateRegistry(state, instance.registry.max_templates)
            instance.registry._templates = dict(loaded._templates)
        if candidate.is_file():
            memory = MemoryBufferPersistence()
            memory.state = candidate.read_bytes()
            instance.engine._miner = TemplateMiner(
                persistence_handler=memory, config=instance.engine._miner.config)
        return instance

    @property
    def cluster_count(self):
        return self.engine.cluster_count

    @property
    def template_count(self):
        return self.registry.template_count

    def process(self, canonical_event):
        if not isinstance(canonical_event, dict):
            return None
        message = canonical_event.get("message")
        if message is None or not str(message).strip():
            return None

        deterministic = self.masker.mask(message)

        structural_result = self.generalizer.generalize(deterministic)
        generalized = structural_result.template
        structural_validation = self.validator.validate_structural_generalization(
            deterministic,
            generalized,
            structural_result.evidence,
        )

        # Structural generalization is advisory.  If its evidence or quality
        # gate fails, the exact deterministic representation remains trusted.
        if structural_validation.accepted:
            trusted = generalized
            structural_applied = generalized != deterministic
        else:
            trusted = deterministic
            structural_applied = False

        # Registry keys must use the same normalized representation that is fed
        # to Drain3.  Otherwise structurally equivalent events cannot hit the
        # same authoritative template.
        stored = self.registry.get(trusted)
        if stored is not None:
            self.last_decision = {
                "deterministic_template": deterministic,
                "generalized_template": generalized,
                "structural_applied": structural_applied,
                "structural_validator_accepted": structural_validation.accepted,
                "structural_validator_reason": structural_validation.reason,
                "structural_evidence": self._serialize_evidence(structural_result.evidence),
                "drain_candidate": None,
                "drain_cluster_id": None,
                "validator_accepted": True,
                "validator_reason": "validated-registry-hit",
                "promoted": False,
                "source": "validated-registry",
            }
            return TemplateResult(
                template_id=self._template_id(stored),
                template=stored,
                reliable=True,
            )

        proposal = self.engine.propose(trusted)
        validation = self.validator.validate(trusted, proposal["template"])
        selected = proposal["template"]
        source = "drain3"

        if not validation.accepted:
            # A rejected Drain3 proposal never enters authoritative state.
            # Prefer the structurally generalized representation only when its
            # own evidence and quality checks succeeded.
            if structural_applied:
                fallback = self.validator.validate_generalized_fallback(trusted)
                source = "structural-fallback"
            else:
                fallback = self.validator.validate_deterministic_fallback(deterministic)
                source = "deterministic-fallback"

            selected = trusted
            promotable = fallback.accepted
            reason = f"drain-rejected:{validation.reason};fallback:{fallback.reason}"
        else:
            promotable = True
            reason = validation.reason

        stored, promoted = (
            self.registry.promote(trusted, selected)
            if promotable
            else (None, False)
        )
        reliable = stored is not None
        template = stored if stored is not None else selected

        self.last_decision = {
            "deterministic_template": deterministic,
            "generalized_template": generalized,
            "structural_applied": structural_applied,
            "structural_validator_accepted": structural_validation.accepted,
            "structural_validator_reason": structural_validation.reason,
            "structural_evidence": self._serialize_evidence(structural_result.evidence),
            "drain_candidate": proposal["template"],
            "drain_cluster_id": proposal["cluster_id"],
            "drain_change_type": proposal["change_type"],
            "validator_accepted": validation.accepted,
            "validator_reason": reason,
            "promoted": promoted,
            "source": source,
        }

        return TemplateResult(
            template_id=self._template_id(template),
            template=template,
            reliable=reliable,
        )

    def save_state(self):
        validated_saved = self.registry.save()
        candidate_saved = self.engine.save()
        return validated_saved and candidate_saved

    @staticmethod
    def _serialize_evidence(evidence):
        return [
            {
                "rule": item.rule,
                "original": item.original,
                "replacement": item.replacement,
                "confidence": item.confidence,
            }
            for item in evidence
        ]

    @staticmethod
    def _template_id(template):
        payload = template.encode("utf-8")
        digest = hashlib.blake2b(payload, digest_size=8).hexdigest()
        return f"tpl_{digest}"
