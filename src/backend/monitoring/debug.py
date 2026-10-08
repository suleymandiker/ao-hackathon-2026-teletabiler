"""Explicit, read-only monitoring replay and bounded representative inspection."""

from datetime import timedelta
import os

import opensearch_application as application
from full_pipeline_v2 import FullAIOpsPipelineV2
from ingestion_layer.opensearch_client import OpenSearchClient
from ingestion_layer.opensearch_source import (OpenSearchSource, MAX_MONITOR_MESSAGE_CHARS,
                                               resolve_exact_mapping)
from monitoring.execution import OpenSearchMonitorExecutor, quiet_pipeline, source_time
from monitoring.run_ledger import IsolatedRunLedger
from monitoring.trace import TraceCollector
from data_paths import monitoring_data_dir
from verified_policy_resolver import VerifiedPolicyResolver


MAX_REPRESENTATIVE_PAGES = 2
MAX_REPRESENTATIVE_CASES = 10
MAX_CONTEXT_EACH_SIDE = 20


def _debug_pipeline():
    """A disposable learner; existing worker learning files are read, never saved."""
    from pathlib import Path
    learning = Path(os.environ.get('AIOPS_MONITOR_LEARNING_DIR') or
                    monitoring_data_dir() / 'learning')
    pipeline = FullAIOpsPipelineV2(template_state=None, drain_state=None, use_ai_rca=False)
    from template_layer.template_pipeline import TemplatePipeline
    pipeline.templater = TemplatePipeline.read_only_snapshot(
        learning / 'templates.json', learning / 'drain.bin')
    return pipeline


class _ReadOnlyRepository:
    compact_mode = True

    def __init__(self, repository):
        self.repository = repository

    def successful_metrics(self, *args):
        return self.repository.successful_metrics(*args)

    def successful_patterns(self, *args):
        return self.repository.successful_patterns(*args)

    def acquisition_ledger(self, run, since):
        cutoff = run.window.end - timedelta(seconds=run.definition.overlap_seconds)
        return IsolatedRunLedger(self.repository.path, run.id, run.monitor_id,
                                 since.isoformat(), cutoff.isoformat())

    def record_shard(self, *args, **kwargs):
        # Shard state remains inside StreamingAcquisition.last_state.
        return None


def replay_window(repository, run, *, connection_loader=application.load_connection,
                  client_factory=OpenSearchClient, source_factory=OpenSearchSource,
                  pipeline_factory=_debug_pipeline):
    """Return an in-memory trace; never claim, finalize, or write learning state."""
    executor = OpenSearchMonitorExecutor(pipeline_factory, connection_loader=connection_loader,
        client_factory=client_factory, source_factory=source_factory,
        repository=_ReadOnlyRepository(repository), policy_loader=application.list_verified_policies,
        log=lambda *args, **kwargs: None, read_only=True)
    outcome = executor.execute(run, set())
    try:
        return outcome.presentation
    finally:
        cleanup = getattr(outcome.receipts, 'cleanup', None)
        if callable(cleanup):
            cleanup()


def representative_debug(run, *, connection_loader=application.load_connection,
                         client_factory=OpenSearchClient, source_factory=OpenSearchSource,
                         pipeline_factory=_debug_pipeline):
    """Analyze only two bounded pages and retain deterministic same-stream context."""
    definition = run.definition
    config = connection_loader(min(definition.page_size, 200))
    if config.source_scope != definition.source_profile:
        raise ValueError('Debug source profile mismatch')
    overlap = timedelta(seconds=definition.overlap_seconds)
    start, end = run.window.start - overlap, run.window.end + overlap
    with client_factory(config) as client:
        if source_factory is OpenSearchSource:
            names = ('namespace', 'workload') + (('container',) if definition.container else ())
            if definition.document_cluster_id:
                names += ('cluster_id',)
            config = resolve_exact_mapping(client, start, end, names=names)
            client.config = config
        source = (source_factory(client, max_message_chars=MAX_MONITOR_MESSAGE_CHARS)
                  if source_factory is OpenSearchSource else source_factory(client))
        pages = []
        cursor = None
        for _ in range(MAX_REPRESENTATIVE_PAGES):
            page = source.read_page(start=start, end=end, namespace=definition.namespace,
                workload=definition.workload, container=definition.container,
                cluster_id=definition.document_cluster_id, page_size=min(definition.page_size, 200),
                cursor=cursor)
            pages.append(page)
            if page.interval_exhausted or not page.next_cursor:
                break
            cursor = page.next_cursor
        records = [record for page in pages for record in page.records]
        if not records:
            return {'stats': {'sampled_physical_records': 0}, 'cases': [], 'trace': None}
        resolution = VerifiedPolicyResolver().resolve(records[:200], application.list_verified_policies())
        trace = TraceCollector(run.id, secrets=(config.password, config.username, *config.hosts))
        for record in records:
            stamp = source_time(record)
            trace.acquisition(record, timestamp=stamp,
                              inside_window=stamp is not None and run.window.start <= stamp < run.window.end,
                              overlap=stamp is None or not run.window.start <= stamp < run.window.end,
                              duplicate=False)
        with quiet_pipeline():
            result = pipeline_factory().process_ingested_pages(pages,
                policy_provider=lambda key, record: resolution.snapshot,
                source_timezone=definition.source_timezone, observer=trace,
                persist_learning=False)
        positions = {TraceCollector.reference(record): index for index, record in enumerate(records)}
        logical_position = {}
        boundary_positions = []
        for item in trace.trace.stages['segmentation']:
            for parent in item.parents:
                if parent.startswith('record:'):
                    ordinal = int(parent.split(':', 1)[1]) - 1
                    logical_position[item.ref] = ordinal
                    break
                if parent.startswith('record-hash:') and parent[12:] in positions:
                    logical_position[item.ref] = positions[parent[12:]]
                    break
            if item.data.get('boundary_status') != 'complete' and item.ref in logical_position:
                boundary_positions.append(logical_position[item.ref])
        template_positions = {}
        for item in trace.trace.stages['patterns']:
            tid = item.data.get('template_id')
            if tid and tid not in template_positions and item.parents:
                logical_ref = item.parents[0].replace('canonical:', 'logical:')
                if logical_ref in logical_position:
                    template_positions[tid] = logical_position[logical_ref]
        priorities = sorted(result.get('signals', ()),
                            key=lambda row: (row.get('severity_min', 7), -row.get('count', 0),
                                             str(row.get('template_id') or '')))
        chosen = [template_positions[row['template_id']] for row in priorities
                  if row.get('template_id') in template_positions]
        chosen.extend(boundary_positions)
        seen_streams = set()
        for index, record in enumerate(records):
            if record.stream_identity not in seen_streams:
                chosen.append(index)
                seen_streams.add(record.stream_identity)
        chosen.extend(range(len(records)))
        selected = []
        used = set()
        for position in chosen:
            if position in used or len(selected) >= MAX_REPRESENTATIVE_CASES:
                continue
            used.add(position)
            record = records[position]
            stream = record.stream_identity
            same_stream = ([(position, record)] if stream is None else
                           [(index, item) for index, item in enumerate(records)
                            if item.stream_identity == stream])
            before = [item for index, item in same_stream if index < position][-MAX_CONTEXT_EACH_SIDE:]
            after = [item for index, item in same_stream if index > position][:MAX_CONTEXT_EACH_SIDE]
            selected.append({'source_position': position,
                'stream_context': [application.safe_text(item.raw_text, config)[:240]
                                   for item in (*before, record, *after)]})
        shown = application.presentation_result(result,
            {'start': run.window.start.isoformat(), 'end': run.window.end.isoformat(),
             'sampled_records': len(records), 'sample_limit': MAX_REPRESENTATIVE_PAGES * 200}, config)
        return {'stats': shown.get('stats', {}), 'signals': shown.get('signals', []),
                'cases': selected, 'trace': trace.finish(result)}
