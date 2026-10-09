"""Bounded acquisition and window ownership around the authoritative pipeline."""
from contextlib import contextmanager, closing, redirect_stdout, redirect_stderr
from dataclasses import dataclass, replace
from datetime import timedelta
import logging
import os

import opensearch_application as application
from ingestion_layer.opensearch_client import OpenSearchClient, OpenSearchClientError
from ingestion_layer.opensearch_source import (OpenSearchSource, OpenSearchSourceError,
                                               MAX_MONITOR_MESSAGE_CHARS, resolve_exact_mapping)
from ingestion_layer.opensearch_indices import resolve_index_expression
from monitoring.domain import MonitorRun, RunCounts
from monitoring.boundary_quality import build_boundary_quality
from monitoring.errors import (MonitoringError, safe_failure_details, ACQUISITION_REASONS,
                               safe_pipeline_stage, safe_acquisition_validation)
from monitoring.identity import reference_key
from monitoring.trace import TraceCollector
from monitoring.metrics_source import OpenSearchMetricsSource
from monitoring.sharded_acquisition import StreamingAcquisition, plan_shards, retry_read
from monitoring.window_accumulator import WindowAccumulator
from monitoring.worker import structured_log
from parser_layer.timestamp.source_policy import TimestampContext, resolve_event_time
from verified_policy_resolver import (VerifiedPolicyResolver, PolicyResolutionError,
                                      MAX_SAMPLE_RECORDS, MAX_SAMPLE_CHARACTERS)


MAX_POLICY_BUFFER_PAGES = 20


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
    def __init__(self, run, consumed=(), ledger=None):
        self.run = run
        self.consumed = set(consumed) if ledger is None else set()
        self.ledger = ledger
        self.receipts = {}
        self.diagnostics = dict(context_events=0, duplicate_events=0, orphan_events=0,
                                boundary_tail_events=0, overlap_conflicts=0)

    def __call__(self, event):
        first, evidence = next((record, evidence) for record, evidence in zip(event.records, event.evidence)
                               if evidence.included)
        first_time = source_time(first)
        if first_time is None:
            raise MonitoringError('OPENSEARCH_QUERY', reason_code='INVALID_TIMESTAMP')
        keys = {reference_key(record) for record in event.records}
        if self.ledger is not None:
            duplicates = {key for key in keys if self.ledger.consumed(key) or self.ledger.owned(key)}
        else:
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
        if self.ledger is not None:
            self.ledger.mark_owned(keys)
        else:
            self.receipts.update({reference_key(record): source_time(record).isoformat()
                                  for record in event.records})
        return True


@dataclass(frozen=True)
class ExecutionResult:
    presentation: dict
    counts: RunCounts
    receipts: dict[str, str]
    metrics: dict | None = None
    pattern_metrics: tuple = ()


class _DiscardOutput:
    # Legacy pipeline prints can contain raw RCA debug evidence. Never retain
    # them in memory or forward them from the unattended monitoring worker.
    def write(self, value):
        return len(value)

    def flush(self):
        pass


class _NoTrace:
    """Normal compact monitoring does not retain diagnostic stage items."""

    def acquisition(self, *args, **kwargs):
        pass

    def __call__(self, *args, **kwargs):
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
                 policy_loader=application.list_verified_policies, trace_limits=None,
                 repository=None, log=structured_log, read_only=False):
        self.pipeline_factory = pipeline_factory
        self.connection_loader = connection_loader
        self.client_factory = client_factory
        self.source_factory = source_factory
        self.policy_loader = policy_loader
        self.trace_limits = trace_limits
        self.repository = repository
        self.log = log
        self.read_only = read_only

    def _execute_streaming(self, run):
        definition = run.definition
        try:
            config = self.connection_loader(definition.page_size)
        except Exception:
            raise MonitoringError('OPENSEARCH_CONFIG') from None
        if config.source_scope != definition.source_profile:
            raise MonitoringError('OPENSEARCH_CONFIG')
        overlap = timedelta(seconds=definition.overlap_seconds)
        start, end = run.window.start - overlap, run.window.end + overlap
        trace = (_NoTrace() if getattr(self.repository, 'compact_mode', False) and not self.read_only
                 else TraceCollector(run.id, limits=self.trace_limits,
                    secrets=(config.password, config.username, *config.hosts,
                             os.environ.get('SAKA_API_KEY'))))
        summary = dict(start=run.window.start.isoformat(), end=run.window.end.isoformat(),
                       retrieval_start=start.isoformat(), retrieval_end=end.isoformat(),
                       resolved_index=resolve_index_expression(config.index_expression, start, end,
                                                               strategy=config.index_strategy),
                       index_expression=config.index_expression, index_strategy=config.index_strategy,
                       namespace=definition.namespace, workload=definition.workload,
                       container=definition.container, source_scope=config.source_scope,
                       source_profile=definition.source_profile,
                       document_cluster_id=definition.document_cluster_id,
                       document_cluster_filter_active=definition.document_cluster_id is not None,
                       sort_fields=[config.field_mapping.timestamp, config.field_mapping.sequence],
                       page_size=definition.page_size, max_pages_per_shard=definition.max_pages,
                       budget_reached=False, stop_reason='interval_exhausted',
                       mapping_verified=False, query_executed=False)
        acquisition = None
        metrics_source = None
        unique = 0
        duplicates = 0
        pipeline_stage = 'source_setup'

        def mark_pipeline_stage(stage):
            nonlocal pipeline_stage
            pipeline_stage = stage

        def failure(category, *, error=None, reason_code=None, stage=None,
                    validation_site=None, validation_reason=None):
            if acquisition is not None:
                last_shard = acquisition.last_state
                if 'count_mismatch' in summary and last_shard is not None:
                    last_shard = {key: last_shard.get(key) for key in ('shard_id', 'status')}
                summary.update(pages_read=acquisition.pages_read,
                               records_read=acquisition.records_read,
                               acquisition_shards=acquisition.shards_completed,
                               last_shard=last_shard)
            summary.update(unique_records=unique, duplicate_records=duplicates)
            if category == 'ACQUISITION_LIMIT':
                summary.update(budget_reached=True, stop_reason='acquisition_limit')
            elif category.startswith('OPENSEARCH'):
                summary['stop_reason'] = 'acquisition_failed'
            if category == 'PIPELINE' and error is not None:
                summary.update(safe_failure_details(pipeline_stage, error))
            shard_failure = (acquisition.last_state if acquisition is not None and
                             acquisition.last_state is not None and
                             acquisition.last_state.get('status') == 'FAILED' else None)
            if reason_code is None and shard_failure is not None:
                reason_code = shard_failure.get('failure_category')
            if category.startswith('OPENSEARCH') and reason_code not in ACQUISITION_REASONS:
                reason_code = 'QUERY_FAILURE_UNKNOWN'
            if reason_code in ACQUISITION_REASONS:
                summary['reason_code'] = reason_code
            else:
                reason_code = None
            if category.startswith('OPENSEARCH'):
                summary['error_stage'] = safe_pipeline_stage(
                    stage or (shard_failure or {}).get('error_stage') or pipeline_stage)
                summary.update(safe_acquisition_validation(validation_site, validation_reason))
            error = MonitoringError(category, reason_code=reason_code,
                                    stage=summary.get('error_stage'),
                                    validation_site=validation_site,
                                    validation_reason=validation_reason)
            safe_summary = {key: value for key, value in summary.items()
                            if key not in ('effective_query', 'query', 'request_body', 'response_body')}
            error.acquisition_diagnostics = redact_ai_secret(
                application.presentation_result({}, safe_summary, config)['source_summary'])
            return error

        try:
            with self.client_factory(config) as client:
                if self.source_factory is OpenSearchSource:
                    names = ('namespace', 'workload') + (('container',) if definition.container else ())
                    if definition.document_cluster_id:
                        names += ('cluster_id',)
                    config = resolve_exact_mapping(client, start, end, names=names)
                    client.config = config
                    summary['mapping_verified'] = True
                mapping = config.field_mapping
                filters = [{'range': {mapping.timestamp: {'gte': start.isoformat(), 'lt': end.isoformat()}}},
                           {'term': {mapping.namespace_exact: definition.namespace}},
                           {'term': {mapping.workload_exact: definition.workload}}]
                if definition.container:
                    filters.append({'term': {mapping.container_exact: definition.container}})
                if definition.document_cluster_id:
                    filters.append({'term': {mapping.cluster_id_exact: definition.document_cluster_id}})
                summary.update(effective_query={'bool': {'filter': filters}},
                               sort_fields=[mapping.timestamp, mapping.sequence])
                source = (self.source_factory(client, max_message_chars=MAX_MONITOR_MESSAGE_CHARS)
                          if self.source_factory is OpenSearchSource else self.source_factory(client))
                metrics_source = OpenSearchMetricsSource(client)
                summary['query_executed'] = True
                mark_pipeline_stage('metrics_acquisition')
                metrics = retry_read(lambda: metrics_source.window(run))
                self.log('METRICS_ACQUIRED', run_id=run.id, physical_logs=metrics.total,
                         time_buckets=len(metrics.buckets))
                try:
                    history = self.repository.successful_metrics(run.monitor_id, run.window.start)
                    pattern_history = self.repository.successful_patterns(run.monitor_id, run.window.start)
                    ledger_context = self.repository.acquisition_ledger(run, start)
                except Exception:
                    raise MonitoringError('PERSISTENCE') from None
                mark_pipeline_stage('shard_planning')
                shards = plan_shards(start, end, metrics, definition.page_size, definition.max_pages)
                def save_shard(shard, status, **values):
                    try:
                        self.repository.record_shard(run, shard, status, **values)
                    except Exception:
                        raise MonitoringError('PERSISTENCE') from None
                acquisition = StreamingAcquisition(source, metrics_source, definition, log=self.log,
                    shard_sink=None if getattr(self.repository, 'compact_mode', False) else save_shard)
                mark_pipeline_stage('content_acquisition')
                with ledger_context as ledger:
                    ownership = WindowOwnership(run, ledger=ledger)
                    unique_inside_window = 0
                    stream_exhausted = False

                    def pages():
                        nonlocal unique, unique_inside_window, duplicates, stream_exhausted
                        for page in acquisition.pages(shards):
                            if len(page.records) > definition.page_size:
                                raise MonitoringError('ACQUISITION_LIMIT')
                            with ledger.page():
                                selected = []
                                for record in page.records:
                                    stamp = source_time(record)
                                    identity = record.stream_identity
                                    if stamp is None:
                                        raise MonitoringError('OPENSEARCH_QUERY', reason_code='INVALID_TIMESTAMP',
                                                              stage='content_acquisition',
                                                              validation_site='TIMESTAMP',
                                                              validation_reason='INVALID_FORMAT')
                                    if not start <= stamp < end:
                                        raise MonitoringError('OPENSEARCH_QUERY', reason_code='INVALID_TIMESTAMP',
                                                              stage='content_acquisition',
                                                              validation_site='TIMESTAMP',
                                                              validation_reason='OUTSIDE_RETRIEVAL')
                                    if identity is None:
                                        raise MonitoringError('OPENSEARCH_QUERY', reason_code='MISSING_REQUIRED_FIELD',
                                                              stage='content_acquisition',
                                                              validation_site='STREAM_IDENTITY',
                                                              validation_reason='REQUIRED_COMPONENT_MISSING')
                                    if record.source_reference.source_scope != config.source_scope:
                                        raise MonitoringError('OPENSEARCH_QUERY', reason_code='INVALID_RESPONSE',
                                                              stage='content_acquisition',
                                                              validation_site='SOURCE_SCOPE',
                                                              validation_reason='SOURCE_PROFILE_MISMATCH')
                                    for field, reason in (('namespace', 'NAMESPACE_MISMATCH'),
                                                          ('workload', 'WORKLOAD_MISMATCH'),
                                                          ('container', 'CONTAINER_MISMATCH')):
                                        expected = getattr(definition, field)
                                        if expected is not None and getattr(identity, field) != expected:
                                            raise MonitoringError('OPENSEARCH_QUERY', reason_code='INVALID_RESPONSE',
                                                                  stage='content_acquisition',
                                                                  validation_site='STREAM_IDENTITY',
                                                                  validation_reason=reason)
                                    if (definition.document_cluster_id is not None and
                                            identity.source_scope != definition.document_cluster_id):
                                        raise MonitoringError('OPENSEARCH_QUERY', reason_code='INVALID_RESPONSE',
                                                              stage='content_acquisition',
                                                              validation_site='SOURCE_SCOPE',
                                                              validation_reason='DOCUMENT_CLUSTER_MISMATCH')
                                    key = reference_key(record)
                                    repeated = ledger.seen(key, stamp.isoformat())
                                    trace.acquisition(record, timestamp=stamp,
                                        inside_window=run.window.start <= stamp < run.window.end,
                                        overlap=not run.window.start <= stamp < run.window.end,
                                        duplicate=repeated)
                                    if repeated:
                                        duplicates += 1
                                        continue
                                    unique += 1
                                    if run.window.start <= stamp < run.window.end:
                                        unique_inside_window += 1
                                    selected.append(record)
                                yield replace(page, records=tuple(selected))
                        stream_exhausted = True

                    with closing(pages()) as stream:
                        buffered_pages = []
                        sample = []
                        first_empty = None
                        sample_chars = 0
                        while len(buffered_pages) < MAX_POLICY_BUFFER_PAGES and len(sample) < MAX_SAMPLE_RECORDS:
                            page = next(stream, None)
                            if page is None:
                                break
                            if not page.records:
                                if first_empty is None:
                                    first_empty = page
                                continue
                            buffered_pages.append(page)
                            for record in page.records[:MAX_SAMPLE_RECORDS - len(sample)]:
                                sample.append(record)
                                sample_chars += len(record.raw_text)
                            if sample_chars > MAX_SAMPLE_CHARACTERS:
                                break
                        if not buffered_pages:
                            if first_empty is None:
                                raise MonitoringError('OPENSEARCH_QUERY', reason_code='INVALID_RESPONSE',
                                                      stage='content_acquisition',
                                                      validation_site='PAGE_HANDOFF',
                                                      validation_reason='EMPTY_PAGE_STREAM')
                            buffered_pages = [first_empty]
                        if sample and metrics.total:
                            mark_pipeline_stage('policy_resolution')
                            try:
                                resolution = VerifiedPolicyResolver().resolve(
                                    sample, self.policy_loader())
                                summary['policy_selection'] = resolution.diagnostics()
                                snapshot = resolution.snapshot
                            except (PolicyResolutionError, application.ApplicationError):
                                raise MonitoringError('POLICY') from None
                        else:
                            snapshot = None

                        def selected_pages():
                            yield from buffered_pages
                            yield from stream

                        mark_pipeline_stage('accumulator')
                        accumulator = WindowAccumulator(window_seconds=definition.window_seconds)
                        accumulator.configure_anomalies(run, metrics, history, pattern_history)
                        mark_pipeline_stage('pipeline_construction')
                        with quiet_pipeline():
                            arguments = dict(policy_provider=lambda key, record: snapshot,
                                source_timezone=definition.source_timezone,
                                assembled_event_filter=ownership, observer=trace,
                                monitoring_accumulator=accumulator,
                                stage_callback=mark_pipeline_stage)
                            if self.read_only:
                                arguments['persist_learning'] = False
                            result = self.pipeline_factory().process_ingested_pages(selected_pages(), **arguments)
                    metrics_count_after = None
                    if stream_exhausted:
                        try:
                            recount = metrics_source.count(
                                run.window.start, run.window.end, definition)
                            if type(recount) is int and recount >= 0:
                                metrics_count_after = recount
                        except Exception:
                            # This second read is diagnostic only. The original exact
                            # metrics count remains the success/failure authority.
                            pass
                    summary['metrics_recount'] = {
                        'metrics_count_before': metrics.total,
                        'metrics_count_after': metrics_count_after,
                        'unique_inside_window': unique_inside_window,
                    }
                    mark_pipeline_stage('result_building')
                    if result.get('ingestion_diagnostics', {}).get('unassembled_count', 0):
                        reasons = result['ingestion_diagnostics'].get('unassembled_by_reason', {})
                        allowed_reasons = {'blank_context'} if metrics.total else {'blank_context', 'no_policy'}
                        if any(count for reason, count in reasons.items() if reason not in allowed_reasons):
                            raise MonitoringError('POLICY')
                    if unique_inside_window != metrics.total:
                        last_shard = acquisition.last_state
                        summary['count_mismatch'] = {
                            'metrics_total': metrics.total,
                            'unique_inside_window': unique_inside_window,
                            'unique_total': unique,
                            'duplicate_records': duplicates,
                            'acquisition': {
                                'pages_read': acquisition.pages_read,
                                'records_read': acquisition.records_read,
                                'shards_seen': acquisition.shards_seen,
                                'shards_completed': acquisition.shards_completed,
                                'completed_unique_records': acquisition.completed_unique_records,
                            },
                            'stream_exhausted': stream_exhausted,
                            'last_shard': ({key: last_shard.get(key)
                                            for key in ('shard_id', 'status')}
                                           if last_shard is not None else None),
                        }
                        raise MonitoringError('OPENSEARCH_QUERY', reason_code='INVALID_RESPONSE',
                                              stage='content_acquisition',
                                              validation_site='PAGE_HANDOFF',
                                              validation_reason='COUNT_MISMATCH')
                    summary.update(pages_read=acquisition.pages_read,
                                   records_read=acquisition.records_read,
                                   unique_records=unique, duplicate_records=duplicates,
                                   acquisition_shards=acquisition.shards_completed,
                                   opensearch_requests=metrics_source.requests + acquisition.pages_read,
                                   window_assembly=ownership.diagnostics,
                                   boundary_quality=accumulator.boundary_quality(),
                                   assembled_stream_count=len(accumulator.streams),
                                   pattern_count=len(accumulator.patterns),
                                   **accumulator.template_diagnostics(),
                                   exact_window_metrics=metrics.to_dict())
                    stats = result['stats']
                    counts = RunCounts(acquisition.completed_unique_records, unique, stats.get('segmented', 0),
                        stats.get('parsed', 0), stats.get('templated', 0),
                        *(stats.get(key, 0) for key in ('signal_candidates', 'qualified_signals',
                                                       'correlations', 'incidents', 'rca')))
                    presentation = redact_ai_secret(application.presentation_result(result, summary, config))
                    if self.read_only or not getattr(self.repository, 'compact_mode', False):
                        presentation['detailed_pipeline_trace'] = trace.finish(result)
                    compact = metrics.to_dict()
                    compact.update(parsed_events=accumulator.parsed_events,
                                   logical_events=stats.get('segmented', 0),
                                   metrics_recount=summary['metrics_recount'],
                                   severity_counts=dict(accumulator.severity_counts),
                                   error_pods=accumulator.error_pods,
                                   error_containers=accumulator.error_containers,
                                   pattern_count=len(accumulator.patterns),
                                   **accumulator.template_diagnostics(),
                                   pages_read=acquisition.pages_read,
                                   acquisition_shards=acquisition.shards_completed)
                    compact['stream_count'] = len(accumulator.streams)
                    compact['top_stream_event_counts'] = accumulator.stream_counts
                    compact['acquisition_duration_seconds'] = round(acquisition.acquisition_duration_seconds, 3)
                    def sanitize(value):
                        if isinstance(value, dict):
                            return {application.safe_text(key, config): sanitize(item)
                                    for key, item in value.items()}
                        if isinstance(value, (list, tuple)):
                            return [sanitize(item) for item in value]
                        return application.safe_text(value, config) if isinstance(value, str) else value
                    return ExecutionResult(presentation, counts,
                                           ledger if getattr(self.repository, 'compact_mode', False) else {},
                                           sanitize(compact),
                                           tuple(sanitize(item) for item in accumulator.pattern_metrics()))
        except MonitoringError as error:
            raise failure(error.category, reason_code=error.reason_code, stage=error.stage,
                          validation_site=error.validation_site,
                          validation_reason=error.validation_reason) from None
        except OpenSearchClientError as error:
            category = ('OPENSEARCH_AUTH' if error.reason_code == 'AUTH_FAILURE'
                        else 'OPENSEARCH_TIMEOUT' if error.reason_code == 'CONNECTION_TIMEOUT'
                        else 'OPENSEARCH_QUERY')
            raise failure(category, reason_code=error.reason_code) from None
        except OpenSearchSourceError as error:
            category = 'OPENSEARCH_TIMEOUT' if error.reason_code == 'SEARCH_TIMEOUT' else 'OPENSEARCH_QUERY'
            raise failure(category, reason_code=error.reason_code) from None
        except Exception as error:
            raise failure('PIPELINE', error=error) from None

    def execute(self, run: MonitorRun, consumed: set[str]) -> ExecutionResult:
        if self.repository is not None:
            return self._execute_streaming(run)
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
        pipeline_stage = 'policy_resolution'
        def mark_pipeline_stage(stage):
            nonlocal pipeline_stage
            pipeline_stage = stage
        def failure(category, *, error=None):
            summary['unique_records'] = len(seen)
            if category == 'ACQUISITION_LIMIT':
                summary.update(budget_reached=True, stop_reason='acquisition_limit')
            elif category.startswith('OPENSEARCH'):
                summary['stop_reason'] = 'acquisition_failed'
            if category == 'PIPELINE' and error is not None:
                summary.update(safe_failure_details(pipeline_stage, error))
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
            mark_pipeline_stage('pipeline_construction')
            with quiet_pipeline():
                result = self.pipeline_factory().process_ingested_pages(
                    pages, policy_provider=lambda key, record: snapshot,
                    source_timezone=definition.source_timezone, assembled_event_filter=ownership, observer=trace,
                    stage_callback=mark_pipeline_stage)
            mark_pipeline_stage('result_building')
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
        except Exception as error:
            raise failure('PIPELINE', error=error) from None
