"""Only allowlisted errors cross the worker persistence/log boundary."""
MESSAGES = {
    'OPENSEARCH_CONFIG': 'Check the configured OpenSearch source profile and connection settings.',
    'OPENSEARCH_AUTH': 'Check OpenSearch read permissions and credentials in application configuration.',
    'OPENSEARCH_TIMEOUT': 'OpenSearch did not complete in time; the same window will be retried.',
    'OPENSEARCH_QUERY': 'Check source mapping, shard health and exact query scope.',
    'ACQUISITION_LIMIT': 'The bounded acquisition limit was reached; no watermark was advanced.',
    'POLICY': 'A unique verified segmentation policy is required for this source.',
    'PIPELINE': 'Pipeline execution failed; the same window will be retried.',
    'PERSISTENCE': 'Result persistence failed; check monitoring storage.',
    'INTERRUPTED': 'The previous worker stopped before completing this window; retry is pending.',
    'UNKNOWN': 'Monitoring failed; check worker availability and configuration.',
}

PIPELINE_STAGES = frozenset({
    'source_setup', 'metrics_acquisition', 'shard_planning', 'content_acquisition',
    'policy_resolution', 'accumulator', 'pipeline_construction',
    'segmentation_session', 'assembly', 'parsing', 'template_processing',
    'downstream', 'result_building', 'pipeline_execution',
})


def safe_pipeline_stage(stage):
    return stage if isinstance(stage, str) and stage in PIPELINE_STAGES else 'pipeline_execution'


def safe_exception_type(error):
    """Keep only a bounded Python class name, never an exception message."""
    name = type(error).__name__
    return name if len(name) <= 64 and name.isascii() and name.isidentifier() else 'Exception'


def safe_pipeline_details(diagnostics):
    """Select only approved, bounded fields for worker terminal output."""
    if not isinstance(diagnostics, dict):
        return {}
    stage = diagnostics.get('pipeline_stage')
    name = diagnostics.get('exception_type')
    if not isinstance(stage, str) or stage not in PIPELINE_STAGES or not isinstance(name, str):
        return {}
    if len(name) > 64 or not name.isascii() or not name.isidentifier():
        return {}
    return {'pipeline_stage': stage, 'exception_type': name}


class MonitoringError(Exception):
    def __init__(self, category):
        self.category = category if category in MESSAGES else 'UNKNOWN'
        super().__init__(MESSAGES[self.category])
