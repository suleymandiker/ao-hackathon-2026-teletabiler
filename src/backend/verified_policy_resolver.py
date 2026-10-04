"""Read-only application policy selection over an initial, bounded record sample.

This is compatibility evidence, not policy discovery or a new segmentation rule.
The raw classifier's strong-header decisions and continuation vetoes remain the
authority. No parser, registry, acquisition client or mutable session is owned here.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from itertools import islice
import re

from ingestion_layer.contracts import Framing
from segmentation_layer.contracts import SegmentationPolicy, StreamKey
from segmentation_layer.header_classifier import HeaderClassifier, STRONG_HEADER
from segmentation_layer.regex_validator import RegexValidator


MAX_SAMPLE_RECORDS = 200
MAX_SAMPLE_CHARACTERS = 256_000
MAX_VERIFIED_POLICIES = 100


class PolicyResolutionError(ValueError):
    def __init__(self, code='no_policy'):
        self.code = code
        super().__init__(code)


@dataclass(frozen=True)
class PolicyResolution:
    selection_id: str
    snapshot: SegmentationPolicy = field(repr=False)
    sampled_records: int
    matched_headers: int
    compatible_streams: int

    def diagnostics(self):
        return dict(mode='automatic', policy_id=self.selection_id,
                    sampled_records=self.sampled_records, matched_headers=self.matched_headers,
                    compatible_streams=self.compatible_streams)


class VerifiedPolicyResolver:
    """Choose only a clearly supported existing verified snapshot.

    At most 200 records / 256k characters from the first acquisition page are
    inspected, without truncating or rewriting any record. Oversized samples or
    catalogs fail closed. Every represented eligible stream needs positive
    evidence, at least two effective regex headers must exist overall, and no
    independent strong header may be missed. Candidates within 10% of the best
    header support (including identical boundary results) are ambiguous.

    This sample does not establish applicability to every future log format.
    The immutable selection is scoped to this finite investigation only.
    """

    def resolve(self, records, policies):
        sample = tuple(islice(records, MAX_SAMPLE_RECORDS))
        candidates = tuple(islice(policies, MAX_VERIFIED_POLICIES + 1))
        if (not sample or not candidates or len(candidates) > MAX_VERIFIED_POLICIES
                or sum(len(record.raw_text) for record in sample) > MAX_SAMPLE_CHARACTERS):
            raise PolicyResolutionError()

        eligible = [(StreamKey.from_identity(record.stream_identity), record.raw_text)
                    for record in sample if record.framing is Framing.PHYSICAL_LINE]
        eligible = [(key, text) for key, text in eligible if key is not None]
        streams = {key for key, text in eligible if text and not text.isspace()}
        classifier = HeaderClassifier()
        validator = RegexValidator()
        ranked = []
        for policy in candidates:
            pattern = policy.snapshot.regex_pattern
            # Reuse the existing validator's bounded regex admission guard;
            # validate() is file-based and would normalize/log source content.
            if not validator._validate_safety(pattern)['valid']:
                continue
            regex = re.compile(pattern)
            states = {}
            matched_streams = set()
            matches = missed = 0
            for key, text in eligible:
                active, after_blank = states.get(key, (False, False))
                decision = classifier.classify_raw(text, regex, has_current_event=active, after_blank=after_blank)
                if decision.start_new_event and decision.regex_match:
                    matches += 1
                    matched_streams.add(key)
                if decision.classification == STRONG_HEADER and not decision.regex_match:
                    missed += 1
                blank = not text or text.isspace()
                states[key] = (active or not blank, blank)
            if matches >= 2 and missed == 0 and streams and matched_streams == streams:
                ranked.append(PolicyResolution(policy.selection_id, policy.snapshot, len(sample), matches, len(streams)))

        ranked.sort(key=lambda row: (-row.matched_headers, row.selection_id))
        if not ranked:
            raise PolicyResolutionError()
        if len(ranked) > 1 and ranked[1].matched_headers * 10 >= ranked[0].matched_headers * 9:
            raise PolicyResolutionError('ambiguous_policy')
        return ranked[0]
