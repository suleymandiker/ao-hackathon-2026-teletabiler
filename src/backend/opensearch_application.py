"""Application boundary for the finite OpenShift UI; no processing-layer changes.

Connection settings use the acquisition smoke's OPENSEARCH_* conventions.
The configured index is read-only in the UI. The MVP batch budget is at most
20 pages * 500 records over at most 24 hours, because Phase 4 uses a finite
batch downstream. These are record/query bounds, not byte or duration bounds.
No active client, cursor, policy binding or session is cached here.
"""
from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import hashlib
from itertools import chain
import os
from pathlib import Path
import re
import sqlite3

from ingestion_layer.opensearch_config import OpenSearchConfig, OpenSearchFieldMapping
from ingestion_layer.opensearch_client import OpenSearchClient, OpenSearchClientError
from ingestion_layer.opensearch_source import OpenSearchSource, OpenSearchSourceError
from segmentation_layer.contracts import SegmentationPolicy


MAX_PAGE_SIZE = 500
MAX_PAGES = 20
MAX_LOOKBACK_MINUTES = 1440
MESSAGES = {
    'configuration_error': 'OpenSearch yapılandırması eksik veya geçersiz. OPENSEARCH_* ortam ayarlarını kontrol edin.',
    'query_error': 'Namespace, uygulama, zaman aralığı ve analiz sınırlarını kontrol edin.',
    'no_policy': 'Doğrulanmış bir sınır politikası seçin. Uygun politika yoksa bu kaynak için analiz başlatılamaz.',
    'policy_store_error': 'Doğrulanmış politikalar okunamadı. Uygulama politika deposunu kontrol edin.',
    'connection_error': 'OpenSearch bağlantısı veya kimlik doğrulaması başarısız. Bağlantı, yetki ve TLS ayarlarını kontrol edin.',
    'partial_search': 'Arama tamamlanamadı veya shard hatası oluştu. Kısmi analiz sonucu gösterilmedi.',
    'source_error': 'OpenSearch sayfası doğrulanamadı. Kaynak alan eşlemesini ve arama kapsamını kontrol edin.',
    'no_records': 'Seçilen aralık ve filtreler için kayıt bulunamadı.',
    'analysis_error': 'Analiz tamamlanamadı. Sunucu yapılandırmasını kontrol edip yeniden deneyin.',
    'busy': 'Başka bir analiz devam ediyor. Tamamlandığında yeniden deneyin.',
}


class ApplicationError(Exception):
    def __init__(self, code):
        self.code = code if code in MESSAGES else 'analysis_error'
        super().__init__(MESSAGES[self.code])


def error_message(error):
    """Never render arbitrary exception text, even for a forged named error."""
    return MESSAGES.get(getattr(error, 'code', None), MESSAGES['analysis_error'])


def load_connection(page_size=100):
    """Read configuration without opening a client or exposing values on error."""
    def required(name):
        value = os.environ.get(name)
        if not value or not value.strip():
            raise ValueError('missing configuration')
        return value

    def boolean(name):
        value = required(name).strip().lower()
        if value not in ('true', 'false'):
            raise ValueError('invalid boolean')
        return value == 'true'

    try:
        if type(page_size) is not int or not 1 <= page_size <= MAX_PAGE_SIZE:
            raise ValueError('invalid page size')
        return OpenSearchConfig(
            hosts=tuple(part.strip() for part in required('OPENSEARCH_HOSTS').split(',')),
            username=required('OPENSEARCH_USERNAME'), password=required('OPENSEARCH_PASSWORD'),
            use_ssl=boolean('OPENSEARCH_USE_SSL'), verify_certs=boolean('OPENSEARCH_VERIFY_CERTS'),
            source_scope=required('OPENSEARCH_SOURCE_SCOPE'), index_expression=required('OPENSEARCH_INDEX'),
            connect_timeout=float(os.environ.get('OPENSEARCH_CONNECT_TIMEOUT_SECONDS', '5')),
            request_timeout=float(os.environ.get('OPENSEARCH_REQUEST_TIMEOUT_SECONDS', '15')),
            ca_bundle=os.environ.get('OPENSEARCH_CA_BUNDLE') or None,
            field_mapping=OpenSearchFieldMapping(), page_size_limit=page_size,
        )
    except Exception:
        raise ApplicationError('configuration_error') from None


@dataclass(frozen=True)
class VerifiedPolicy:
    selection_id: str
    snapshot: SegmentationPolicy = field(repr=False)


def list_verified_policies(db_path=None):
    """Read only the existing verified segmentation table, without initializing it.

    SegmentationPolicyRegistry.put() admits only verified policies; that table
    has no separate verified column. Parser/legacy tables are never selected.
    No schema changes, hit updates, memory-cache reuse or discovery occur here.
    Existing hexadecimal signatures are safe display identifiers. Nonstandard
    signatures get opaque display IDs; the snapshot retains the original value.
    """
    path = Path(db_path or os.environ.get('AIOPS_POLICY_REGISTRY_PATH') or
                Path(__file__).resolve().parents[1] / 'data' / 'policy_registry.sqlite3')
    try:
        if not path.exists():
            return ()
        with closing(sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True)) as connection:
            exists = connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='segmentation_policies'",
            ).fetchone()
            if not exists:
                return ()
            policies = []
            for signature, regex in connection.execute(
                'SELECT signature, regex FROM segmentation_policies ORDER BY signature',
            ):
                try:
                    snapshot = SegmentationPolicy(signature, regex, 'segmentation-registry:' + signature)
                    re.compile(regex)  # Syntax check only; not policy admission.
                except (TypeError, ValueError, re.error):
                    continue
                display_id = signature if re.fullmatch(r'[0-9a-f]{24}', signature) else hashlib.sha256(signature.encode()).hexdigest()[:24]
                policies.append(VerifiedPolicy('policy-' + display_id, snapshot))
            return tuple(policies)
    except Exception:
        raise ApplicationError('policy_store_error') from None


@dataclass(frozen=True)
class AnalysisRequest:
    namespace: str
    workload: str
    container: str | None
    start: datetime
    end: datetime
    page_size: int = 100
    max_pages: int = 3

    def __post_init__(self):
        valid = (
            all(type(value) is str and bool(value.strip()) for value in (self.namespace, self.workload))
            and (self.container is None or type(self.container) is str and bool(self.container.strip()))
            and type(self.page_size) is int and 1 <= self.page_size <= MAX_PAGE_SIZE
            and type(self.max_pages) is int and 1 <= self.max_pages <= MAX_PAGES
            and all(type(value) is datetime and value.utcoffset() is not None for value in (self.start, self.end))
        )
        if not valid or not timedelta(0) < self.end - self.start <= timedelta(minutes=MAX_LOOKBACK_MINUTES):
            raise ApplicationError('query_error')


def make_request(namespace, workload, container, lookback_minutes, page_size, max_pages, end_text=''):
    try:
        if type(lookback_minutes) is not int or not 1 <= lookback_minutes <= MAX_LOOKBACK_MINUTES:
            raise ValueError('invalid lookback')
        end = datetime.fromisoformat(end_text.replace('Z', '+00:00')) if end_text else datetime.now(timezone.utc)
        if end.utcoffset() is None:
            raise ValueError('timezone required')
        end = end.astimezone(timezone.utc)
        return AnalysisRequest(namespace, workload, container or None,
                               end - timedelta(minutes=lookback_minutes), end, page_size, max_pages)
    except Exception:
        raise ApplicationError('query_error') from None


def bounded_pages(source, request, summary):
    """Yield original pages once; hold only the local opaque continuation cursor."""
    cursor = None
    for number in range(1, request.max_pages + 1):
        page = source.read_page(start=request.start, end=request.end, namespace=request.namespace,
                                workload=request.workload, container=request.container,
                                page_size=request.page_size, cursor=cursor)
        if len(page.records) > request.page_size:
            raise OpenSearchSourceError('Search returned more hits than requested')
        summary['pages_read'] = number
        summary['records_read'] += len(page.records)
        if page.interval_exhausted:
            stop = 'interval_exhausted'
        elif not page.records or page.next_cursor is None:
            stop = 'current_view_exhausted'
        elif number == request.max_pages:
            stop = 'page_budget'
        else:
            stop = None
        summary['stop_reason'] = stop
        summary['budget_reached'] = stop == 'page_budget'
        yield page
        if stop:
            return
        cursor = page.next_cursor


def run_analysis(pipeline_factory, request, policy):
    """Acquire within the budget and pass pages straight to Phase 4.

    The UI serializes access to its shared pipeline (including uploads). No
    partial analysis is returned on failure; normal budget exhaustion closes
    the finite Phase 4 session. Existing template-learning lifecycle is retained.
    """
    if not isinstance(policy, VerifiedPolicy):
        raise ApplicationError('no_policy')
    config = load_connection(request.page_size)
    summary = dict(pages_read=0, records_read=0, stop_reason=None, budget_reached=False,
                   start=request.start.isoformat(), end=request.end.isoformat(),
                   page_size=request.page_size, max_pages=request.max_pages,
                   record_budget=request.page_size * request.max_pages)
    snapshot = policy.snapshot

    def provider(key, first_record):
        return snapshot

    try:
        with OpenSearchClient(config) as client:
            pages = bounded_pages(OpenSearchSource(client), request, summary)
            with closing(pages):
                first = next(pages)
                if not first.records:
                    raise ApplicationError('no_records')
                result = pipeline_factory().process_ingested_pages(chain((first,), pages), policy_provider=provider)
        return presentation_result(result, summary, config)
    except ApplicationError:
        raise
    except OpenSearchClientError:
        raise ApplicationError('connection_error') from None
    except OpenSearchSourceError as error:
        code = 'partial_search' if str(error) in (
            'Search timed out or lacks completion evidence', 'Search shard failure or missing shard status',
        ) else 'source_error'
        raise ApplicationError(code) from None
    except Exception:
        raise ApplicationError('analysis_error') from None


def safe_text(value, config):
    """Defense in depth for display strings, including derived analysis text."""
    text = str(value)
    for secret in sorted((config.password, config.username, *config.hosts), key=len, reverse=True):
        if secret:
            text = text.replace(secret, '[redacted]')
    text = re.sub(
        r'''(?i)\b(?:authorization|password|api[_-]?key|(?:access[_-]?)?token|secret)["']?\s*[:=]\s*(?:"(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*'|[^\r\n,;]+)''',
        '[redacted]', text,
    )
    text = re.sub(r'''(?i)\bbearer\s+[^\s,;"']+''', '[redacted]', text)
    return re.sub(r'\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b', '[redacted]', text)


def presentation_result(result, summary, config):
    """Store/render a presentation copy, never the raw Phase 4 evidence payload.

    Derived templates and operational evidence remain available. Raw canonical
    traces, source provenance and unassembled samples never enter session_state.
    Backend result, event IDs, templates and persistent learning are not mutated.
    """
    excluded = {'raw', 'raw_text', 'message', 'source_provenance', 'event_provenance',
                'attributes', 'source_reference', 'retrieval_order', 'cursor', 'next_cursor',
                'password', 'authorization', 'api_key', 'api-key', 'access_token', 'token', 'secret', 'unassembled_samples'}

    def clean(value):
        if isinstance(value, dict):
            return {safe_text(key, config): clean(item) for key, item in value.items() if key.lower() not in excluded}
        if isinstance(value, (list, tuple)):
            return [clean(item) for item in value]
        return safe_text(value, config) if isinstance(value, str) else value

    shown = {key: clean(result[key]) for key in (
        'stats', 'signals', 'qualified_signals', 'correlations', 'incidents', 'rca', 'plans', 'case_analysis',
    ) if key in result}
    shown['case_analysis_error'] = 'Uzman yorumu alınamadı; deterministik RCA korunuyor.' if result.get('case_analysis_error') else None
    trace = result.get('pipeline_trace', {})
    shown['pipeline_trace'] = {stage: {'count': trace.get(stage, {}).get('count', 0), 'items': []}
                               for stage in ('segmentation', 'parser', 'template')}
    diagnostics = result.get('ingestion_diagnostics', {})
    known_reasons = ('missing_stream_identity', 'incomplete_stream_identity', 'no_policy', 'unsupported_framing', 'blank_context')
    streams = {tuple(entry['provenance']['stream_key'][key] for key in
                     ('source_scope', 'pod_instance', 'container_instance', 'channel'))
               for entry in result.get('event_provenance', [])}
    shown['source_summary'] = {
        **summary, 'assembled_events': result.get('stats', {}).get('segmented', 0),
        'assembled_stream_count': len(streams),
        'unassembled_count': diagnostics.get('unassembled_count', 0),
        'unassembled_by_reason': {reason: diagnostics.get('unassembled_by_reason', {}).get(reason, 0)
                                  for reason in known_reasons if diagnostics.get('unassembled_by_reason', {}).get(reason, 0)},
    }
    return shown
