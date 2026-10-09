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

# Fixed structural diagnostics only. Never copy failed record values or exception text.
ACQUISITION_VALIDATION = {
    'TIMESTAMP': frozenset({'INVALID_FORMAT', 'OUTSIDE_RETRIEVAL'}),
    'STREAM_IDENTITY': frozenset({'REQUIRED_COMPONENT_MISSING', 'NAMESPACE_MISMATCH',
                                  'WORKLOAD_MISMATCH', 'CONTAINER_MISMATCH'}),
    'SOURCE_SCOPE': frozenset({'SOURCE_PROFILE_MISMATCH', 'DOCUMENT_CLUSTER_MISMATCH'}),
    'PAGE_HANDOFF': frozenset({'EMPTY_PAGE_STREAM', 'COUNT_MISMATCH'}),
}

FAILURE_CLASSIFICATIONS = frozenset({
    'SYSTEMIC', 'TRANSIENT_INFRA', 'WINDOW_LOCAL', 'UNKNOWN', 'ESCALATED_SYSTEMIC'
})


def safe_acquisition_validation(site, reason):
    if (type(site) is str and type(reason) is str and
            reason in ACQUISITION_VALIDATION.get(site, ())):
        return {'validation_site': site, 'validation_reason': reason}
    return {}


def safe_failure_signature(category, safe_reason_code=None, error_stage=None,
                           validation_site=None, validation_reason=None):
    """Deterministic, bounded structural failure signature. Never includes raw exception text."""
    cat = category if isinstance(category, str) and category in MESSAGES else 'UNKNOWN'
    rc = safe_reason_code if isinstance(safe_reason_code, str) and (
        safe_reason_code in ACQUISITION_REASONS or safe_reason_code in LIMIT_REASONS) else ''
    st = error_stage if isinstance(error_stage, str) and error_stage in PIPELINE_STAGES else ''
    vs = validation_site if isinstance(validation_site, str) and validation_site in ACQUISITION_VALIDATION else ''
    vr = validation_reason if (vs and isinstance(validation_reason, str)
                               and validation_reason in ACQUISITION_VALIDATION.get(vs, ())) else ''
    return f'{cat}:{rc}:{st}:{vs}:{vr}'


def extract_safe_diagnostics(category, diagnostics=None):
    """Extract allowlisted structural diagnostics and deterministic signature."""
    cat = category if isinstance(category, str) and category in MESSAGES else 'UNKNOWN'
    if not isinstance(diagnostics, dict):
        return {
            'category': cat,
            'safe_reason_code': None,
            'error_stage': None,
            'validation_site': None,
            'validation_reason': None,
            'signature': safe_failure_signature(cat),
        }
    reason = diagnostics.get('reason_code')
    if cat.startswith('OPENSEARCH') and reason not in ACQUISITION_REASONS:
        reason = 'QUERY_FAILURE_UNKNOWN'
    elif reason not in ACQUISITION_REASONS and reason not in LIMIT_REASONS:
        reason = None
    stage = diagnostics.get('error_stage') or diagnostics.get('pipeline_stage')
    stage = safe_pipeline_stage(stage) if stage else None
    validation = safe_acquisition_validation(diagnostics.get('validation_site'),
                                            diagnostics.get('validation_reason'))
    site = validation.get('validation_site')
    v_reason = validation.get('validation_reason')
    sig = safe_failure_signature(cat, reason, stage, site, v_reason)
    return {
        'category': cat,
        'safe_reason_code': reason,
        'error_stage': stage,
        'validation_site': site,
        'validation_reason': v_reason,
        'signature': sig,
    }


def classify_failure(category, *, safe_reason_code=None, error_stage=None,
                     validation_site=None, validation_reason=None):
    """Authoritative failure classification matrix."""
    cat = category if isinstance(category, str) and category in MESSAGES else 'UNKNOWN'
    # 1. SYSTEMIC (Cluster, config, auth, or storage errors affecting all windows)
    if cat in ('OPENSEARCH_AUTH', 'OPENSEARCH_CONFIG', 'PERSISTENCE'):
        return 'SYSTEMIC'
    if safe_reason_code in ('AUTH_FAILURE', 'MAPPING_INCOMPATIBLE'):
        return 'SYSTEMIC'
    if validation_site == 'SOURCE_SCOPE':
        return 'SYSTEMIC'
    if error_stage == 'source_setup':
        return 'SYSTEMIC'

    # 2. TRANSIENT_INFRA (Temporary network or cluster timeouts/hiccups)
    if cat in ('OPENSEARCH_TIMEOUT', 'INTERRUPTED'):
        return 'TRANSIENT_INFRA'
    if safe_reason_code in ('CONNECTION_TIMEOUT', 'SEARCH_TIMEOUT', 'HTTP_429', 'HTTP_5XX', 'SHARD_FAILURE'):
        return 'TRANSIENT_INFRA'

    # 3. WINDOW_LOCAL (Errors specific to the log contents/format of this single window)
    if cat == 'ACQUISITION_LIMIT':
        return 'WINDOW_LOCAL'
    if cat == 'POLICY':
        return 'WINDOW_LOCAL'
    if cat == 'PIPELINE' and error_stage in (
        'assembly', 'parsing', 'boundary_observation', 'provenance_enrichment',
        'parsed_observation', 'template_processing', 'template_accumulation',
        'time_quality', 'downstream', 'result_building', 'pipeline_execution', 'accumulator'
    ):
        return 'WINDOW_LOCAL'
    if safe_reason_code in ('INVALID_TIMESTAMP', 'INVALID_SEQUENCE', 'MISSING_REQUIRED_FIELD', 'INVALID_CURSOR'):
        return 'WINDOW_LOCAL'
    if error_stage == 'content_acquisition' and safe_reason_code == 'INVALID_RESPONSE':
        return 'WINDOW_LOCAL'

    # 4. UNKNOWN (Requires bounded retry, escalates to ESCALATED_SYSTEMIC if repeating)
    return 'UNKNOWN'


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
    def __init__(self, category, *, reason_code=None, stage=None,
                 validation_site=None, validation_reason=None):
        self.category = category if category in MESSAGES else 'UNKNOWN'
        self.reason_code = reason_code if reason_code in ACQUISITION_REASONS else None
        self.retryable = self.reason_code in ACQUISITION_REASONS - NON_RETRYABLE_ACQUISITION_REASONS
        self.stage = stage if stage in PIPELINE_STAGES else None
        validation = safe_acquisition_validation(validation_site, validation_reason)
        self.validation_site = validation.get('validation_site')
        self.validation_reason = validation.get('validation_reason')
        super().__init__(MESSAGES[self.category])
