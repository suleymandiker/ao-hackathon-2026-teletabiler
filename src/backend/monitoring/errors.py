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


class MonitoringError(Exception):
    def __init__(self, category):
        self.category = category if category in MESSAGES else 'UNKNOWN'
        super().__init__(MESSAGES[self.category])
