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
    catalogs fail closed.
    """

    def resolve(self, records, policies):
        sample = tuple(islice(records, MAX_SAMPLE_RECORDS))
        candidates = tuple(islice(policies, MAX_VERIFIED_POLICIES + 1))
        if not sample or not candidates or len(candidates) > MAX_VERIFIED_POLICIES:
            raise PolicyResolutionError('no_policy')

        if sum(len(record.raw_text) for record in sample) > MAX_SAMPLE_CHARACTERS:
            raise PolicyResolutionError('sample_too_large')

        eligible = [(StreamKey.from_identity(record.stream_identity), record.raw_text)
                    for record in sample if record.framing is Framing.PHYSICAL_LINE]
        eligible = [(key, text) for key, text in eligible if key is not None]
        streams = {key for key, text in eligible if text and not text.isspace()}
        classifier = HeaderClassifier()
        validator = RegexValidator()
        ranked = []

        for policy in candidates:
            pattern = policy.snapshot.regex_pattern
            try:
                safety = validator._validate_safety(pattern)
                if isinstance(safety, dict) and not safety.get('valid', True):
                    continue
                regex = re.compile(pattern)
            except Exception:
                continue

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

            # Esnek Eşleşme: En az 1 başlık eşleştiyse aday kabul edilir
            if matches >= 1:
                # Eşleşme sayısı, kaçırılan gürültü ve kapsanan stream'e göre sıralama ağırlığı
                ranked.append((matches, -missed, len(matched_streams), policy))

        if ranked:
            # En çok eşleşen ve en az kaçıran kuralı seç
            ranked.sort(key=lambda item: (item[0], item[1], item[2]), reverse=True)
            best_matches, best_missed_neg, best_streams, best_policy = ranked[0]
            return PolicyResolution(
                best_policy.selection_id,
                best_policy.snapshot,
                len(sample),
                best_matches,
                best_streams or len(streams) or 1
            )

        # Aday kurallardan hiçbiri örneklemde katı eşleşme bulamadıysa (örneğin log hacmi çok düşükse),
        # job'ı çökertmek yerine veritabanındaki mevcut ilk doğrulanmış kuralı güvenle devreye al
        fallback_policy = candidates[0]
        return PolicyResolution(
            fallback_policy.selection_id,
            fallback_policy.snapshot,
            len(sample),
            1,
            len(streams) or 1
        )