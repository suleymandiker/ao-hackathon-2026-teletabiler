"""Shared application environment parsing for OpenSearch UI, worker and smoke."""
import os

from ingestion_layer.opensearch_config import OpenSearchConfig, OpenSearchFieldMapping


class OpenSearchConfigurationError(ValueError):
    """Fixed configuration diagnostics; never include environment values."""


def load_opensearch_connection(page_size=100):
    """Read explicit connection/profile settings without loading .env or doing I/O."""
    def required(name):
        value = os.environ.get(name)
        if not value or not value.strip():
            raise OpenSearchConfigurationError('Missing environment variable: ' + name)
        return value

    def boolean(name):
        value = required(name).strip().lower()
        if value not in ('true', 'false'):
            raise OpenSearchConfigurationError(name + ' must be true or false')
        return value == 'true'

    try:
        return OpenSearchConfig(
            hosts=tuple(part.strip() for part in required('OPENSEARCH_HOSTS').split(',')),
            username=required('OPENSEARCH_USERNAME'), password=required('OPENSEARCH_PASSWORD'),
            use_ssl=boolean('OPENSEARCH_USE_SSL'), verify_certs=boolean('OPENSEARCH_VERIFY_CERTS'),
            source_scope=required('OPENSEARCH_SOURCE_SCOPE'), index_expression=required('OPENSEARCH_INDEX'),
            connect_timeout=float(os.environ.get('OPENSEARCH_CONNECT_TIMEOUT_SECONDS', '5')),
            request_timeout=float(os.environ.get('OPENSEARCH_REQUEST_TIMEOUT_SECONDS', '15')),
            ca_bundle=os.environ.get('OPENSEARCH_CA_BUNDLE') or None,
            field_mapping=OpenSearchFieldMapping(), page_size_limit=page_size,
            index_strategy=os.environ.get('OPENSEARCH_INDEX_STRATEGY', 'literal'),
        )
    except OpenSearchConfigurationError:
        raise
    except Exception:
        raise OpenSearchConfigurationError('Invalid OpenSearch configuration; check OPENSEARCH_* settings') from None
