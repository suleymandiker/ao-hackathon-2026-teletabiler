"""Bounded acquisition and window ownership around the authoritative pipeline."""
from contextlib import contextmanager, redirect_stdout, redirect_stderr
from dataclasses import dataclass, replace
from datetime import timedelta
import hashlib
import json
import logging
import os

import opensearch_application as application
from ingestion_layer.opensearch_client import OpenSearchClient, OpenSearchClientError
from ingestion_layer.opensearch_source import OpenSearchSource, OpenSearchSourceError, resolve_exact_mapping
from ingestion_layer.opensearch_indices import resolve_index_expression
from monitoring.domain import MonitorRun, RunCounts
from monitoring.boundary_quality import build_boundary_quality
from monitoring.errors import MonitoringError
from monitoring.trace import TraceCollector
from parser_layer.timestamp.source_policy import TimestampContext, resolve_event_time
from verified_policy_resolver import VerifiedPolicyResolver, PolicyResolutionError


def reference_key(record):
    reference = record.source_reference
    # A source document update is not a new physical line. Ignore _version.
    return hashlib.sha256(json.dumps((reference.source_scope, reference.source_partition,
                                     reference.record_id), separators=(',', ':')).encode()).hexdigest()


def source_time(record):
    # The acquisition contract deliberately carries raw @timestamp unchanged.
    # Resolve only its absolute instant, without applying a message timezone.
    return resolve_event_time(None, TimestampContext(source_record_time=record.source_timestamp,
                                                      source_record_raw=record.source_timestamp_raw)).source_record_time


class WindowOwnership:
    """Keep context for assembly; select each event before parser/learning.

    First included source-record time assigns ownership, never message time.
    Orphans at the left retrieval cut are reported and excluded. Finite tails
    retain the existing analysis_end semantics and are explicitly counted.
    """
    def __init__(self, run, consumed=()):
        self.run = run
        self.consumed = set(consumed)
        self.receipts = {}
        self.diagnostics = dict(context_events=0, duplicate_events=0, orphan_events=0,
                                boundary_tail_events=0, overlap_conflicts=0)

    def __call__(self, event):
        first, evidence = next((record, evidence) for record, evidence in zip(event.records, event.evidence)
                               if evidence.included)
        first_time = source_time(first)
        if first_time is None:
            raise MonitoringError('OPENSEARCH_QUERY')
        keys = {reference_key(record) for record in event.records}
        duplicates = {key for key in keys if key in self.consumed or key in self.receipts}
        if duplicates:
            self.diagnostics['duplicate_events'] += 1
            if keys - duplicates:
                self.diagnostics['overlap_conflicts'] += 1
            return False
        if not evidence.decision.start_new_event:
            self.diagnostics['orphan_events'] += 1
            return False
        if not self.run.window.start <= first_time < self.run.window.end:
            self.diagnostics['context_events'] += 1
            return False
        if event.emission_reason == 'analysis_end':
            self.diagnostics['boundary_tail_events'] += 1
        self.receipts.update({reference_key(record): source_time(record).isoformat()
                              for record in event.records})
        return True


@dataclass(frozen=True)
class ExecutionResult:
    presentation: dict
    counts: RunCounts
    receipts: dict[str, str]


class _DiscardOutput:
    # Legacy pipeline prints can contain raw RCA debug evidence. Never retain
    # them in memory or forward them from the unattended monitoring worker.
    def write(self, value):
        return len(value)

    def flush(self):
        pass


@contextmanager
def quiet_pipeline():
    # This executor is single-owner/single-threaded. Preconfigured logging
    # handlers can retain old stderr streams, so redirecting prints alone is
    # insufficient to suppress arbitrary legacy exception diagnostics.
    previous = logging.root.manager.disable
    try:
        logging.disable(logging.CRITICAL)
        with redirect_stdout(_DiscardOutput()), redirect_stderr(_DiscardOutput()):
            yield
    finally:
        logging.disable(previous)


def redact_ai_secret(value):
    secret = os.environ.get('SAKA_API_KEY')
    if isinstance(value, dict):
        return {redact_ai_secret(key): redact_ai_secret(item) for key, item in value.items()}
    if isinstance(value, list):
        return [redact_ai_secret(item) for item in value]
    return value.replace(secret, '[redacted]') if secret and isinstance(value, str) else value


class OpenSearchMonitorExecutor:
    def __init__(self, pipeline_factory, *, connection_loader=application.load_connection,
                 client_factory=OpenSearchClient, source_factory=OpenSearchSource,
                 policy_loader=application.list_verified_policies, trace_limits=None):
        self.pipeline_factory = pipeline_factory
        self.connection_loader = connection_loader
        self.client_factory = client_factory
        self.source_factory = source_factory
        self.policy_loader = policy_loader
        self.trace_limits = trace_limits

    def execute(self, run: MonitorRun, consumed: set[str]) -> ExecutionResult:
        definition = run.definition
        try:
            config = self.connection_loader(definition.page_size)
        except Exception:
            raise MonitoringError('OPENSEARCH_CONFIG') from None
        if config.source_scope != definition.source_profile:
            raise MonitoringError('OPENSEARCH_CONFIG')
        trace = TraceCollector(run.id, limits=self.trace_limits,
                               secrets=(config.password, config.username, *config.hosts, os.environ.get('SAKA_API_KEY')))
        overlap = timedelta(seconds=definition.overlap_seconds)
        start, end = run.window.start - overlap, run.window.end + overlap
        resolved_index = resolve_index_expression(config.index_expression, start, end, strategy=config.index_strategy)
        summary = dict(start=run.window.start.isoformat(), end=run.window.end.isoformat(),
                       retrieval_start=start.isoformat(), retrieval_end=end.isoformat(),
                       index_expression=config.index_expression, index_strategy=config.index_strategy,
                       resolved_index=resolved_index, namespace=definition.namespace,
                       workload=definition.workload, container=definition.container,
                       source_timezone=definition.source_timezone, source_profile=definition.source_profile,
                       source_scope=config.source_scope, cluster_alias=definition.cluster_alias,
                       document_cluster_id=definition.document_cluster_id,
                       pages_read=0, records_read=0, unique_records=0,
                       duplicate_records=0, budget_reached=False, stop_reason='interval_exhausted',
                       page_size=definition.page_size, max_pages=definition.max_pages,
                       record_budget=definition.page_size * definition.max_pages)
        ownership = WindowOwnership(run, consumed)
        def query_diagnostics(mapping):
            filters = [{'range': {mapping.timestamp: {'gte': start.isoformat(), 'lt': end.isoformat()}}},
                       {'term': {mapping.namespace_exact: definition.namespace}},
                       {'term': {mapping.workload_exact: definition.workload}}]
            if definition.container:
                filters.append({'term': {mapping.container_exact: definition.container}})
            if definition.document_cluster_id:
                filters.append({'term': {mapping.cluster_id_exact: definition.document_cluster_id}})
            summary.update(effective_query={'bool': {'filter': filters}},
                           document_cluster_filter_active=definition.document_cluster_id is not None,
                           sort_fields=[mapping.timestamp, mapping.sequence])
        query_diagnostics(config.field_mapping)
        summary['mapping_verified'] = False
        summary['query_executed'] = False
        def failure(category):
            summary['unique_records'] = len(seen)
            if category == 'ACQUISITION_LIMIT':
                summary.update(budget_reached=True, stop_reason='acquisition_limit')
            elif category.startswith('OPENSEARCH'):
                summary['stop_reason'] = 'acquisition_failed'
            error = MonitoringError(category)
            error.acquisition_diagnostics = redact_ai_secret(application.presentation_result({}, summary, config)['source_summary'])
            return error
        try:
            # Acquire all bounded pages before any learning. A page cap is a
            # failed run, never a successful partial window/watermark advance.
            pages, seen, cursor = [], set(), None
            with self.client_factory(config) as client:
                if self.source_factory is OpenSearchSource:
                    names = ('namespace', 'workload') + (('container',) if definition.container else ())
                    if definition.document_cluster_id:
                        names += ('cluster_id',)
                    config = resolve_exact_mapping(client, start, end, names=names)
                    client.config = config
                    query_diagnostics(config.field_mapping)
                    summary['mapping_verified'] = True
                source = self.source_factory(client)
                for _ in range(definition.max_pages):
                    summary['query_executed'] = True
                    page = source.read_page(start=start, end=end, namespace=definition.namespace,
                                            workload=definition.workload, container=definition.container,
                                            cluster_id=definition.document_cluster_id, page_size=definition.page_size, cursor=cursor)
                    summary['pages_read'] += 1
                    summary['records_read'] += len(page.records)
                    if len(page.records) > definition.page_size:
                        raise MonitoringError('ACQUISITION_LIMIT')
                    records = []
                    for record in page.records:
                        timestamp = source_time(record)
                        if timestamp is None or not start <= timestamp < end:
                            raise MonitoringError('OPENSEARCH_QUERY')
                        identity = record.stream_identity
                        # SourceReference identifies the acquisition profile;
                        # StreamIdentity carries the document's cluster UUID.
                        if (record.source_reference.source_scope != config.source_scope or identity is None
                                or (definition.document_cluster_id is not None
                                    and identity.source_scope != definition.document_cluster_id)
                                or identity.namespace != definition.namespace or identity.workload != definition.workload
                                or (definition.container and identity.container != definition.container)):
                            raise MonitoringError('OPENSEARCH_QUERY')
                        key = reference_key(record)
                        trace.acquisition(record, timestamp=timestamp,
                                          inside_window=run.window.start <= timestamp < run.window.end,
                                          overlap=not run.window.start <= timestamp < run.window.end,
                                          duplicate=key in seen)
                        if key in seen:
                            summary['duplicate_records'] += 1
                            continue
                        seen.add(key)
                        records.append(record)
                    pages.append(replace(page, records=tuple(records)))
                    if page.interval_exhausted:
                        break
                    if page.cycle_budget_reached:
                        raise MonitoringError('ACQUISITION_LIMIT')
                    if not page.records or page.next_cursor is None or page.next_cursor == cursor:
                        raise MonitoringError('OPENSEARCH_QUERY')
                    cursor = page.next_cursor
                else:
                    raise MonitoringError('ACQUISITION_LIMIT')
            summary['unique_records'] = len(seen)
        except MonitoringError as error:
            raise failure(error.category) from None
        except OpenSearchClientError as error:
            text = str(error)
            category = ('OPENSEARCH_AUTH' if text in ('OpenSearch HTTP failure (401)', 'OpenSearch HTTP failure (403)')
                        else 'OPENSEARCH_TIMEOUT' if text == 'OpenSearch connection or timeout failure'
                        else 'OPENSEARCH_QUERY')
            raise failure(category) from None
        except OpenSearchSourceError as error:
            category = 'OPENSEARCH_TIMEOUT' if str(error) == 'Search timed out or lacks completion evidence' else 'OPENSEARCH_QUERY'
            raise failure(category) from None
        except Exception:
            raise failure('OPENSEARCH_QUERY') from None

        if seen:
            try:
                # Same bounded verified-policy resolver used by manual OpenShift.
                sample = tuple(record for page in pages for record in page.records)[:definition.page_size]
                resolution = VerifiedPolicyResolver().resolve(sample, self.policy_loader())
                summary['policy_selection'] = resolution.diagnostics()
            except (PolicyResolutionError, application.ApplicationError):
                raise failure('POLICY') from None
            snapshot = resolution.snapshot
        else:
            snapshot = None
        try:
            with quiet_pipeline():
                result = self.pipeline_factory().process_ingested_pages(
                    pages, policy_provider=lambda key, record: snapshot,
                    source_timezone=definition.source_timezone, assembled_event_filter=ownership, observer=trace)
            if result.get('ingestion_diagnostics', {}).get('unassembled_count', 0):
                reasons = result['ingestion_diagnostics'].get('unassembled_by_reason', {})
                if any(count for reason, count in reasons.items() if reason != 'blank_context'):
                    raise MonitoringError('POLICY')
            summary['window_assembly'] = ownership.diagnostics
            summary['pattern_count'] = len({row['template_id'] for row in result.get('signals', [])
                                            if row.get('template_id') is not None})
            if 'event_provenance' in result:
                summary['boundary_quality'] = build_boundary_quality(result['event_provenance'])
            stats = result['stats']
            counts = RunCounts(summary['records_read'], len(seen), stats.get('segmented', 0),
                               stats.get('parsed', 0), stats.get('templated', 0),
                               *(stats.get(key, 0) for key in ('signal_candidates', 'qualified_signals', 'correlations', 'incidents', 'rca')))
            # Reuse the established safe presentation projection; never persist
            # canonical raw traces or arbitrary transport/pipeline exceptions.
            presentation = redact_ai_secret(application.presentation_result(result, summary, config))
            presentation['detailed_pipeline_trace'] = trace.finish(result)
            return ExecutionResult(presentation, counts, ownership.receipts)
        except MonitoringError as error:
            raise failure(error.category) from None
        except Exception:
            raise failure('PIPELINE') from None
