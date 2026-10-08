"""Only allowlisted errors cross the worker persistence/log boundary."""
from pathlib import Path
import re

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
    'segmentation_session', 'assembly', 'parsing', 'boundary_observation',
    'provenance_enrichment', 'parsed_observation', 'template_processing',
    'template_accumulation', 'time_quality', 'downstream', 'result_building',
    'pipeline_execution',
})

_BACKEND_ROOT = Path(__file__).resolve().parents[1]
_SAFE_PATH_PART = re.compile(r'^[A-Za-z0-9_.-]+$')
_SAFE_FUNCTION = re.compile(r'^[A-Za-z_][A-Za-z0-9_]{0,127}$')
_GENERATED_FUNCTIONS = frozenset({'<module>', '<lambda>', '<genexpr>', '<listcomp>', '<dictcomp>', '<setcomp>'})
LIMIT_REASONS = frozenset({
    'STREAM_CARDINALITY_LIMIT', 'PATTERN_CARDINALITY_LIMIT',
    'TEMPLATE_LENGTH_LIMIT', 'SIGNAL_CARDINALITY_LIMIT',
})
ACQUISITION_REASONS = frozenset({
    'CONNECTION_TIMEOUT', 'SEARCH_TIMEOUT', 'HTTP_429', 'HTTP_5XX', 'SHARD_FAILURE',
    'INVALID_CURSOR', 'INVALID_RESPONSE', 'MISSING_REQUIRED_FIELD',
    'INVALID_TIMESTAMP', 'INVALID_SEQUENCE', 'MAPPING_INCOMPATIBLE',
    'AUTH_FAILURE', 'QUERY_FAILURE_UNKNOWN',
})
NON_RETRYABLE_ACQUISITION_REASONS = ACQUISITION_REASONS - frozenset({
    'CONNECTION_TIMEOUT', 'SEARCH_TIMEOUT', 'HTTP_429', 'HTTP_5XX', 'SHARD_FAILURE',
})


def safe_pipeline_stage(stage):
    return stage if isinstance(stage, str) and stage in PIPELINE_STAGES else 'pipeline_execution'


def safe_exception_type(error):
    """Keep only a bounded Python class name, never an exception message."""
    name = type(error).__name__
    return name if len(name) <= 64 and name.isascii() and name.isidentifier() else 'Exception'


def safe_exception_site(error):
    """Return only the deepest structural location inside src/backend."""
    site = {}
    trace = error.__traceback__
    while trace is not None:
        code = trace.tb_frame.f_code
        try:
            relative = Path(code.co_filename).resolve().relative_to(_BACKEND_ROOT)
        except (OSError, ValueError):
            trace = trace.tb_next
            continue
        label = 'src/backend/' + relative.as_posix()
        function = code.co_name
        line = trace.tb_lineno
        if (len(label) <= 240 and all(part not in ('.', '..') and _SAFE_PATH_PART.fullmatch(part)
                                      for part in relative.parts)
                and (_SAFE_FUNCTION.fullmatch(function) or function in _GENERATED_FUNCTIONS)
                and type(line) is int and line > 0):
            site = {'exception_file': label, 'exception_function': function,
                    'exception_line': line}
        trace = trace.tb_next
    return site


class MonitoringLimitError(ValueError):
    """An intentional bounded-state guard with no source-derived values."""
    __slots__ = ('reason_code', 'observed', 'limit')

    def __init__(self, reason_code, observed, limit):
        if reason_code not in LIMIT_REASONS or type(observed) is not int or type(limit) is not int:
            raise ValueError('Invalid monitoring limit diagnostic')
        self.reason_code, self.observed, self.limit = reason_code, observed, limit
        super().__init__(reason_code)


def safe_failure_details(stage, error):
    details = {'pipeline_stage': safe_pipeline_stage(stage),
               'exception_type': safe_exception_type(error)}
    details.update(safe_exception_site(error))
    if isinstance(error, MonitoringLimitError):
        details.update(reason_code=error.reason_code, observed=error.observed, limit=error.limit)
    return details


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
    details = {'pipeline_stage': stage, 'exception_type': name}
    path = diagnostics.get('exception_file')
    function = diagnostics.get('exception_function')
    line = diagnostics.get('exception_line')
    if (isinstance(path, str) and path.startswith('src/backend/') and len(path) <= 240
            and all(part not in ('.', '..') and _SAFE_PATH_PART.fullmatch(part)
                    for part in path.split('/'))
            and isinstance(function, str)
            and (_SAFE_FUNCTION.fullmatch(function) or function in _GENERATED_FUNCTIONS)
            and type(line) is int and line > 0):
        details.update(exception_file=path, exception_function=function, exception_line=line)
    reason = diagnostics.get('reason_code')
    observed = diagnostics.get('observed')
    limit = diagnostics.get('limit')
    if (isinstance(reason, str) and reason in LIMIT_REASONS
            and type(observed) is int and type(limit) is int and 0 <= observed <= 10**12
            and 0 <= limit <= 10**12):
        details.update(reason_code=reason, observed=observed, limit=limit)
    return details


class MonitoringError(Exception):
    def __init__(self, category, *, reason_code=None):
        self.category = category if category in MESSAGES else 'UNKNOWN'
        self.reason_code = reason_code if reason_code in ACQUISITION_REASONS else None
        super().__init__(MESSAGES[self.category])
