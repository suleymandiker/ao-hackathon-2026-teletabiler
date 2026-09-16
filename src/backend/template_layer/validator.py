# -*- coding: utf-8 -*-
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ValidationResult:
    accepted: bool
    reason: str


class TemplateValidator:
    """Validate deterministic, structural and Drain3 generalization safely."""

    VARIABLE_TOKENS = {"<*>", "<ID>", "<UUID>", "<IP>", "<URL>", "<HEX>", "<NUM>"}

    def __init__(self, max_variable_ratio=0.6, min_generalization_confidence=0.95):
        if not 0.0 <= max_variable_ratio <= 1.0:
            raise ValueError("max_variable_ratio must be between 0 and 1")
        if not 0.0 <= min_generalization_confidence <= 1.0:
            raise ValueError("min_generalization_confidence must be between 0 and 1")
        self.max_variable_ratio = max_variable_ratio
        self.min_generalization_confidence = min_generalization_confidence

    def validate(self, trusted_template, drain_candidate):
        """Drain may only generalize tokens already variable in trusted input."""
        source = trusted_template.split()
        candidate = drain_candidate.split()
        if not source or not candidate:
            return ValidationResult(False, "empty-template")
        if len(source) != len(candidate):
            return ValidationResult(False, "token-count-changed")

        for source_token, candidate_token in zip(source, candidate):
            if source_token == candidate_token:
                continue
            if source_token not in self.VARIABLE_TOKENS:
                return ValidationResult(False, f"semantic-literal-changed:{source_token}")
            if candidate_token not in self.VARIABLE_TOKENS:
                return ValidationResult(False, "variable-became-literal")

        return self._quality_gate(candidate)

    def validate_structural_generalization(
        self,
        deterministic_template,
        generalized_template,
        evidence,
    ):
        """Authorize only mutations explicitly backed by high-confidence evidence.

        We intentionally validate by replaying evidence.  The structural layer
        cannot silently mutate text: applying every recorded original ->
        replacement operation to the deterministic template must reproduce the
        generalized template exactly.
        """
        deterministic = " ".join(str(deterministic_template or "").split())
        generalized = " ".join(str(generalized_template or "").split())

        if deterministic == generalized:
            return ValidationResult(True, "unchanged")
        if not evidence:
            return ValidationResult(False, "generalization-without-evidence")

        replay = deterministic
        for item in evidence:
            confidence = float(getattr(item, "confidence", 0.0))
            original = str(getattr(item, "original", ""))
            replacement = str(getattr(item, "replacement", ""))

            if confidence < self.min_generalization_confidence:
                return ValidationResult(False, "generalization-confidence-too-low")
            if not original or original not in replay:
                return ValidationResult(False, "generalization-evidence-mismatch")
            if not self._replacement_contains_variable(replacement):
                return ValidationResult(False, "generalization-not-variable")

            # One evidence item describes one concrete mutation occurrence.
            replay = replay.replace(original, replacement, 1)

        if replay != generalized:
            return ValidationResult(False, "generalization-replay-mismatch")

        quality = self._quality_gate(generalized.split())
        if not quality.accepted:
            return ValidationResult(False, f"generalization-{quality.reason}")
        return ValidationResult(True, "accepted")

    def validate_deterministic_fallback(self, deterministic_template):
        return self._quality_gate(deterministic_template.split())

    def validate_generalized_fallback(self, generalized_template):
        return self._quality_gate(generalized_template.split())

    def _quality_gate(self, tokens):
        if not tokens:
            return ValidationResult(False, "empty-template")
        variable_count = sum(self._token_is_variable(token) for token in tokens)
        literal_count = len(tokens) - variable_count
        if literal_count == 0:
            return ValidationResult(False, "no-literal-anchor")
        if variable_count / len(tokens) > self.max_variable_ratio:
            return ValidationResult(False, "variable-ratio-too-high")
        return ValidationResult(True, "accepted")

    def _token_is_variable(self, token):
        return token in self.VARIABLE_TOKENS or any(
            marker in token for marker in self.VARIABLE_TOKENS
        )

    def _replacement_contains_variable(self, replacement):
        return any(marker in replacement for marker in self.VARIABLE_TOKENS)
