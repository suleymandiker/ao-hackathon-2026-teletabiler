"""Small RCA output contract, enforced locally even without gateway schema support."""
from __future__ import annotations

import json
import math


TEXT_LIMITS = {'durum_ozeti': 400, 'kok_neden_hipotezi': 400}
ARRAY_LIMITS = {'etkilenen_olaylar': (5, 16), 'kanit_referanslari': (5, 16),
                'karar_gerekcesi': (3, 160), 'alternatif_hipotezler': (2, 160),
                'onerilen_incelemeler': (4, 160), 'eksik_kanitlar': (4, 160)}
CAUSALITY = ('destekleniyor', 'belirsiz', 'yetersiz')
MAX_OUTPUT_CHARS = 6000


def response_format():
    properties = {name: dict(type='string', minLength=1, maxLength=limit) for name, limit in TEXT_LIMITS.items()}
    properties.update(guven=dict(type='number', minimum=0, maximum=1),
                      nedensellik_durumu=dict(type='string', enum=list(CAUSALITY)))
    for name, (count, length) in ARRAY_LIMITS.items():
        properties[name] = dict(type='array', maxItems=count, items=dict(type='string', minLength=1, maxLength=length))
        if name in ('etkilenen_olaylar', 'kanit_referanslari'):
            properties[name]['minItems'] = 1
    schema = dict(type='object', properties=properties, required=list(properties), additionalProperties=False)
    return dict(type='json_schema', json_schema=dict(name='rca_expert_v2', strict=True, schema=schema))


class ExpertOutputError(ValueError):
    pass


class RCAExpertOutputValidator:
    def validate(self, reply, pack, finish_reason):
        if finish_reason != 'stop':
            raise ExpertOutputError('incomplete_response')
        if not isinstance(reply, str) or not reply.strip() or len(reply) > MAX_OUTPUT_CHARS:
            raise ExpertOutputError('invalid_output_size')

        def unique_object(pairs):
            value = {}
            for key, item in pairs:
                if key in value:
                    raise ValueError('duplicate_key')
                value[key] = item
            return value

        try:
            value = json.loads(reply, object_pairs_hook=unique_object,
                               parse_constant=lambda _: (_ for _ in ()).throw(ValueError('nonfinite')))
        except (ValueError, RecursionError):
            raise ExpertOutputError('invalid_json') from None
        required = set(TEXT_LIMITS) | set(ARRAY_LIMITS) | {'guven', 'nedensellik_durumu'}
        if not isinstance(value, dict) or set(value) != required:
            raise ExpertOutputError('invalid_fields')
        for name, limit in TEXT_LIMITS.items():
            if not self._text(value[name], limit):
                raise ExpertOutputError('invalid_text')
        confidence = value['guven']
        if type(confidence) not in (int, float) or not math.isfinite(confidence) or not 0 <= confidence <= 1:
            raise ExpertOutputError('invalid_confidence')
        if value['nedensellik_durumu'] not in CAUSALITY:
            raise ExpertOutputError('invalid_causality')
        for name, (count, length) in ARRAY_LIMITS.items():
            rows = value[name]
            if not isinstance(rows, list) or len(rows) > count or not all(self._text(row, length) for row in rows):
                raise ExpertOutputError('invalid_array')
        incidents, signals = dict(pack.incident_aliases), dict(pack.signal_aliases)
        memberships = dict(pack.signal_incidents)
        incident_refs, evidence_refs = value['etkilenen_olaylar'], value['kanit_referanslari']
        if (not incident_refs or len(set(incident_refs)) != len(incident_refs)
                or not all(ref in incidents for ref in incident_refs)):
            raise ExpertOutputError('invalid_incident_reference')
        if (not evidence_refs or len(set(evidence_refs)) != len(evidence_refs)
                or not all(ref in signals and set(memberships[ref]) & set(incident_refs) for ref in evidence_refs)):
            raise ExpertOutputError('invalid_evidence_reference')
        # Keep existing Turkish fields and resolve an accepted group reference to
        # every underlying signal. The alias/member mapping never enters a prompt.
        value['etkilenen_olaylar'] = [incidents[ref] for ref in incident_refs]
        members = dict(pack.signal_members)
        value['kanit_sinyal_idleri'] = list(dict.fromkeys(sid for ref in evidence_refs for sid in members[ref]))
        del value['kanit_referanslari']
        return value

    @staticmethod
    def _text(value, limit):
        return isinstance(value, str) and bool(value.strip()) and len(value) <= limit
