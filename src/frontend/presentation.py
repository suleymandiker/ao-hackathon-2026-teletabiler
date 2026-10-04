"""Deterministic, bounded presentation of existing analysis contracts.

No decisions are made here: qualification, correlation, incident membership and
RCA remain backend evidence. Only this allowlisted view model enters UI state.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
import math

from opensearch_application import safe_text


DISPLAY_LIMIT = 200
EVIDENCE_LIMIT = 20
TIMELINE_LIMIT = 40
SEVERITIES = {1: 'FATAL', 2: 'CRITICAL', 3: 'ERROR', 4: 'WARNING', 5: 'NOTICE', 6: 'INFO', 7: 'DEBUG'}
REASONS = {
    'nitelikli_sinyal': 'Nitelikli sinyal', 'gürültü_olarak_bastırıldı': 'Gürültü olarak bastırıldı',
    'missing_stream_identity': 'Akış kimliği yok', 'incomplete_stream_identity': 'Akış kimliği eksik',
    'no_policy': 'Uygun format politikası yok', 'unsupported_framing': 'Desteklenmeyen kayıt sınırı',
    'blank_context': 'Aktif olay dışında boş kayıt',
}


@dataclass(frozen=True)
class PipelineStageView:
    key: str
    name: str
    value: str
    status: str
    description: str
    details: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class SignalView:
    signal_id: str
    pattern_id: str
    service: str
    count: int | None
    outcome: str
    reason: str
    evidence: tuple[str, ...]
    technical: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class PatternView:
    pattern_id: str
    text: str
    count: int | None
    severity: str
    first_seen: str | None
    last_seen: str | None
    outcome: str
    services: tuple[str, ...]
    hosts: tuple[str, ...]
    signals: tuple[SignalView, ...]
    evidence: tuple[str, ...]


@dataclass(frozen=True)
class TimelineItem:
    timestamp: str
    label: str


@dataclass(frozen=True)
class IncidentView:
    incident_id: str
    title: str
    severity: str
    tone: str
    root: str
    root_entity: str
    services: tuple[str, ...]
    metrics: tuple[tuple[str, str], ...]
    hypotheses: tuple[tuple[str, str], ...]
    topology: tuple[tuple[str, ...], ...]
    evidence: tuple[str, ...]
    technical: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class InvestigationViewModel:
    source: str
    target: str
    title: str
    explanation: str
    tone: str
    counts: tuple[tuple[str, str], ...]
    stages: tuple[PipelineStageView, ...]
    patterns: tuple[PatternView, ...]
    signals: tuple[SignalView, ...]
    incidents: tuple[IncidentView, ...]
    correlations: tuple[tuple[str, str, str], ...]
    timeline: tuple[TimelineItem, ...]
    reason_counts: tuple[tuple[str, int], ...]
    technical: tuple[tuple[str, str], ...]
    expert_note: str | None
    expert_commentary: tuple[tuple[str, str], ...] = ()
    display_limit: int = DISPLAY_LIMIT


def _number(value):
    return value if type(value) in (int, float) and math.isfinite(value) and value >= 0 else None


def _count(stats, key, result, list_key=None):
    value = _number(stats.get(key))
    if value is not None:
        return int(value)
    rows = result.get(list_key or key)
    return len(rows) if isinstance(rows, (list, tuple)) else None


def _iso_ms(value):
    if _number(value) is None:
        return None
    try:
        return datetime.fromtimestamp(value / 1000, tz=timezone.utc).isoformat(timespec='seconds')
    except (ValueError, OverflowError, OSError):
        return None


def _outcome(row, incident_signals):
    if row.get('signal_id') in incident_signals:
        return 'Incident evidence'
    if row.get('qualified') is True:
        return 'Qualified'
    if row.get('qualified') is False:
        return 'Suppressed'
    return 'Candidate'


class InvestigationPresenter:
    def __init__(self, redact=safe_text):
        self.redact = redact

    def text(self, value, limit=500):
        # Contract fields are scalar. Never stringify unexpected metadata trees.
        return self.redact(str(value))[:limit] if type(value) in (str, int, float, bool) else ''

    def values(self, values, limit=EVIDENCE_LIMIT):
        return tuple(sorted({self.text(value) for value in values
                             if value not in (None, '', 'unknown')})[:limit])

    def pairs(self, rows):
        return tuple((label, self.text(value)) for label, value in rows if value is not None)

    def signal(self, row, incident_signals):
        return SignalView(
            self.text(row.get('signal_id')), self.text(row.get('template_id')),
            self.text(row.get('service_name')), _number(row.get('count')), _outcome(row, incident_signals),
            self.text(REASONS.get(row.get('qualification_reason'), row.get('qualification_reason', ''))),
            tuple(self.text(item.replace('_', ' ')) for item in row.get('qualification_evidence', [])[:EVIDENCE_LIMIT]
                  if isinstance(item, str)),
            self.pairs([('Nitelik skoru', _number(row.get('qualification_score'))),
                        ('Şablon güvenilirlik oranı', _number(row.get('reliable_ratio')))]),
        )

    def build(self, result, *, source, target=''):
        stats = result.get('stats') or {}
        acquisition = result.get('source_summary') or {}
        signals = result.get('signals') or []
        incidents = result.get('incidents') or []
        correlations = result.get('correlations') or []
        rca = {row.get('incident_id'): row for row in result.get('rca', [])}
        incident_signals = {sid for row in incidents for sid in row.get('signal_ids', [])}
        signal_map = {row.get('signal_id'): row for row in signals}
        groups = defaultdict(list)
        for row in signals:
            if row.get('template_id'):
                groups[row['template_id']].append(row)

        pattern_views = []
        timeline = []
        for tid, rows in groups.items():
            outcomes = {_outcome(row, incident_signals) for row in rows}
            outcome = next(name for name in ('Incident evidence', 'Qualified', 'Candidate', 'Suppressed') if name in outcomes)
            counts = [_number(row.get('count')) for row in rows]
            count = sum(counts) if all(value is not None for value in counts) else None
            severities = [row.get('severity_min') for row in rows if row.get('severity_min') in SEVERITIES]
            resolved = all(row.get('timestamp_resolved') is True for row in rows)
            firsts = [_iso_ms(row.get('first_seen_ms')) for row in rows] if resolved else []
            lasts = [_iso_ms(row.get('last_seen_ms')) for row in rows] if resolved else []
            first = min(firsts) if firsts and all(firsts) else None
            last = max(lasts) if lasts and all(lasts) else None
            text = self.text(rows[0].get('template') or tid)
            if first:
                timeline.append(TimelineItem(first, 'Pattern ilk gözlem: ' + text[:100]))
            if last and last != first:
                timeline.append(TimelineItem(last, 'Pattern son gözlem: ' + text[:100]))
            pattern_views.append(PatternView(
                self.text(tid), text, count, SEVERITIES.get(min(severities), '') if severities else '',
                first, last, outcome, self.values(row.get('service_name') for row in rows),
                self.values(host for row in rows for host in (row.get('hosts') or [])),
                tuple(self.signal(row, incident_signals) for row in rows[:EVIDENCE_LIMIT]),
                self.values(eid for row in rows for eid in row.get('event_ids', [])),
            ))
        pattern_views.sort(key=lambda row: (-(row.count or 0), row.pattern_id))

        incident_views = []
        for incident in incidents[:DISPLAY_LIMIT]:
            context = incident.get('context') or {}
            root = incident.get('probable_root') or {}
            entity, alarm = self.text(root.get('entity')), self.text(root.get('alarm_type', '')).replace('_', ' ')
            root_text = ' · '.join(part for part in (entity, alarm if alarm != 'generic' else '') if part)
            title = self.text(incident.get('title') or incident.get('summary')) or (root_text + ' · Incident adayı').lstrip(' ·')
            severity = SEVERITIES.get(incident.get('severity_min'), '')
            tone = 'danger' if severity in ('CRITICAL', 'FATAL', 'ERROR') else 'attention' if severity == 'WARNING' else 'info'
            hypotheses = []
            technical = [('Incident durumu', incident.get('status'))]
            for candidate in rca.get(incident.get('incident_id'), {}).get('root_cause_candidates', [])[:5]:
                row = signal_map.get(candidate.get('signal_id'), {})
                label = self.text(row.get('template') or candidate.get('signal_id'))
                evidence = ' · '.join(self.text(item).replace('_', ' ') for item in candidate.get('evidence', [])[:EVIDENCE_LIMIT])
                hypotheses.append((label, evidence))
                if _number(candidate.get('score')) is not None:
                    technical.append(('RCA aday skoru · ' + label[:50], candidate['score']))
            paths = []
            for dependency in context.get('dependencies', [])[:40]:
                path = dependency.get('path')
                if isinstance(path, (list, tuple)) and len(path) >= 2 and all(isinstance(node, str) for node in path):
                    paths.append(tuple(self.text(node) for node in path[:20]))
            incident_views.append(IncidentView(
                self.text(incident.get('incident_id')), title, severity, tone, root_text, entity,
                self.values(context.get('services') or incident.get('services') or []),
                self.pairs([('Olay', _number(incident.get('event_count'))), ('Sinyal', _number(incident.get('signal_count'))),
                            ('Pattern', len(set(incident['template_ids'])) if 'template_ids' in incident else None)]),
                tuple(hypotheses), tuple(dict.fromkeys(paths)),
                self.values(eid for sid in incident.get('signal_ids', []) for eid in signal_map.get(sid, {}).get('event_ids', [])),
                self.pairs(technical),
            ))
        severity_rank = {label: number for number, label in SEVERITIES.items()}
        incident_views.sort(key=lambda item: (severity_rank.get(item.severity, 99), item.incident_id))

        segmented = _count(stats, 'segmented', result)
        parsed = _count(stats, 'parsed', result)
        templated = _count(stats, 'templated', result)
        candidates = _count(stats, 'signal_candidates', result, 'signals')
        qualified = _count(stats, 'qualified_signals', result)
        suppressed = _count(stats, 'noise_suppressed', result)
        if suppressed is None and 'signals' in result:
            suppressed = sum(row.get('qualified') is False for row in signals)
        pattern_count = len(groups) if 'signals' in result else None
        incident_count = _count(stats, 'incidents', result)
        correlation_count = _count(stats, 'correlations', result)
        rca_count = _count(stats, 'rca', result)
        physical = _number(acquisition.get('records_read'))
        structured = stats.get('structured_alarm_fast_path') is True
        if structured:
            physical = segmented

        counts = self.pairs([('fiziksel kayıt' if not structured else 'alarm kaydı', physical),
                             ('olay', segmented), ('pattern', pattern_count), ('sinyal adayı', candidates),
                             ('nitelikli sinyal', qualified)])
        if incident_count:
            title = incident_views[0].title if incident_views else f'{incident_count} incident adayı bulundu.'
            explanation = 'Olası kök neden ve etkilenen servisleri kanıtlarıyla birlikte inceleyin.'
            tone = incident_views[0].tone if incident_views else 'attention'
        elif incident_count == 0:
            title = 'Bu analiz kapsamında operasyonel incident bulunamadı.'
            tone = 'success'
            if candidates and suppressed == candidates:
                explanation = f'{candidates} adayın tamamı sinyal değerlendirmesi aşamasında bastırıldı.'
            elif qualified:
                explanation = f'{qualified} nitelikli sinyal bulundu; backend bir incident adayı üretmedi.'
            else:
                explanation = 'Bu sonuç, seçilen veri ve analiz kapsamı için geçerlidir.'
        else:
            title, explanation, tone = 'Analiz sonucu', 'Mevcut sonuç alanları aşağıda gösteriliyor.', 'info'
        if acquisition.get('budget_reached'):
            explanation += ' Kayıt sınırına ulaşıldı; sonuç alınan kayıtlarla sınırlıdır.'

        stages = []

        def stage(key, name, value, description, rows, attention=False, inactive=False):
            status = 'attention' if attention else 'inactive' if inactive or value in (None, 0) else 'success'
            stages.append(PipelineStageView(key, name, '—' if value is None else str(value), status, description, self.pairs(rows)))

        dispositions = [(REASONS.get(reason, reason), count) for reason, count in acquisition.get('unassembled_by_reason', {}).items()]
        stage('acquisition', 'Acquisition', physical, 'Analize alınan kayıtlar',
              [('Fiziksel kayıt', physical), ('Alınan sayfa', acquisition.get('pages_read')),
               ('Olay üreten akış', acquisition.get('assembled_stream_count')),
               ('Kayıt sınırı', acquisition.get('record_budget')),
               ('Sınıra ulaşıldı', 'Evet' if acquisition.get('budget_reached') else 'Hayır') if acquisition else ('Kaynak', source)],
              attention=bool(acquisition.get('budget_reached')))
        stage('segmentation', 'Segmentation', 'Bypass' if structured else segmented,
              'Yapılandırılmış alarmlar doğrudan işlenir' if structured else 'Mantıksal olay sınırları',
              [('Fiziksel girdi', physical), ('Mantıksal olay', segmented), ('Birleştirilemeyen kayıt', acquisition.get('unassembled_count'))] + dispositions,
              attention=bool(acquisition.get('unassembled_count')), inactive=structured)
        stage('parsing', 'Parsing', 'Bypass' if structured else parsed,
              'Yapılandırılmış alanlar kullanılır' if structured else 'Kanonik olaylar',
              [('Kanonik olay', parsed)] + [(label, stats[key]) for key, label in
                (('parser_fallback', 'Fallback'), ('parser_failed', 'Başarısız'), ('parser_ignored', 'Yok sayılan')) if key in stats],
              inactive=structured)
        stage('patterns', 'Patterns', pattern_count, 'Benzersiz log kalıpları',
              [('Benzersiz pattern', pattern_count), ('Şablonlanan olay', templated), ('Güvenilir olmayan şablon', stats.get('template_unreliable'))],
              attention=bool(stats.get('template_unreliable')))
        stage('signal', 'Signal', f'{candidates} → {qualified}' if candidates is not None and qualified is not None else qualified,
              'Adaydan operasyonel sinyale', [('Aday', candidates), ('Nitelikli', qualified), ('Bastırılan', suppressed)],
              attention=bool(suppressed), inactive=candidates == 0)
        stage('correlation', 'Correlation', correlation_count, 'İlişkili sinyaller', [('Korelasyon', correlation_count)])
        stage('incident', 'Incident', incident_count, 'Backend incident adayları', [('Incident adayı', incident_count)])
        stage('rca', 'RCA', rca_count, 'Olası kök neden hipotezleri', [('RCA sonucu', rca_count)])

        technical = [('Kaynak', source)]
        for key, label in (('start', 'Aralık başlangıcı'), ('end', 'Aralık sonu (hariç)'), ('source_scope', 'Kaynak kapsamı'),
                           ('index_expression', 'İndeks'), ('pages_read', 'Alınan sayfa'), ('record_budget', 'Kayıt sınırı')):
            if key in acquisition:
                technical.append((label, acquisition[key]))
        if 'verify_certs' in acquisition:
            technical.append(('TLS doğrulaması', 'Açık' if acquisition['verify_certs'] else 'Kapalı'))
        selection = acquisition.get('policy_selection') or {}
        for key, label in (('mode', 'Policy selection'), ('policy_id', 'Policy ID'), ('sampled_records', 'Örneklenen kayıt'),
                           ('matched_headers', 'Uyumlu başlık'), ('compatible_streams', 'Uyumlu örnek akış')):
            if key in selection:
                technical.append((label, selection[key]))
        reason_counts = Counter(self.text(REASONS.get(row['qualification_reason'], row['qualification_reason']))
                                for row in signals if row.get('qualified') is False and row.get('qualification_reason'))
        expert = result.get('case_analysis') or {}
        commentary = []
        for key, label in (('durum_ozeti', 'Uzman özeti'), ('kok_neden_hipotezi', 'Uzman hipotezi'),
                           ('nedensellik_durumu', 'Nedensellik değerlendirmesi'), ('karar_gerekcesi', 'Karar gerekçesi'),
                           ('alternatif_hipotezler', 'Alternatif hipotezler'), ('onerilen_incelemeler', 'Önerilen incelemeler'),
                           ('eksik_kanitlar', 'Eksik kanıtlar')):
            value = expert.get(key) if isinstance(expert, dict) else None
            if isinstance(value, str):
                commentary.append((label, self.text(value)))
            elif isinstance(value, list):
                commentary.extend((label, self.text(item)) for item in value[:EVIDENCE_LIMIT] if isinstance(item, str))
        return InvestigationViewModel(
            self.text(source), self.text(target), title, explanation, tone, counts, tuple(stages),
            tuple(pattern_views[:DISPLAY_LIMIT]), tuple(self.signal(row, incident_signals) for row in signals[:DISPLAY_LIMIT]),
            tuple(incident_views), tuple((self.text(row.get('source_service')), self.text(row.get('target_service')),
                                        ' · '.join(self.text(item).replace('_', ' ') for item in row.get('evidence', [])[:EVIDENCE_LIMIT]))
                                       for row in correlations[:DISPLAY_LIMIT]),
            tuple(sorted(set(timeline), key=lambda item: (item.timestamp, item.label))[:TIMELINE_LIMIT]),
            tuple(sorted(reason_counts.items())), self.pairs(technical),
            'Uzman yorumu alınamadı; deterministik RCA korunuyor.' if result.get('case_analysis_error') else None,
            expert_commentary=tuple(commentary),
        )
