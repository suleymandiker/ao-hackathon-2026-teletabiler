"""Explicit application configuration for bounded OpenSearch acquisition."""

from dataclasses import dataclass, field, fields
import math
from urllib.parse import urlsplit
from ingestion_layer.opensearch_indices import validate_index_strategy


def _text(value, name):
    if type(value) is not str or not value.strip():
        raise ValueError(f"{name} must be a nonempty string")


def _host_url(host: str, use_ssl: bool) -> str:
    """Accept an origin or host[:port], never URL credentials or a path."""
    _text(host, "host")
    scheme = "https" if use_ssl else "http"
    url = host if "://" in host else f"{scheme}://{host}"
    valid = False
    try:
        parts = urlsplit(url)
        valid = (
            parts.scheme == scheme and bool(parts.hostname)
            and parts.username is None and parts.password is None
            and parts.path in ("", "/") and not parts.query and not parts.fragment
            and not any(character.isspace() for character in url)
            and "\\" not in url
        )
        # Accessing port also validates the port syntax/range.
        parts.port
    except ValueError:
        valid = False
    if not valid:
        raise ValueError("host must be a credential-free origin matching use_ssl")
    return url.rstrip("/")


@dataclass(frozen=True, slots=True)
class OpenSearchFieldMapping:
    """Source paths and exact-query fields are independent, explicit mappings."""

    timestamp: str = "@timestamp"
    message: str = "message"
    cluster_id: str = "openshift.cluster_id"
    sequence: str = "openshift.sequence"
    namespace: str = "kubernetes.namespace_name"
    workload: str = "kubernetes.labels.app"
    pod: str = "kubernetes.pod_name"
    pod_instance: str = "kubernetes.pod_id"
    pod_owner: str = "kubernetes.pod_owner"
    container: str = "kubernetes.container_name"
    container_instance: str = "kubernetes.container_id"
    channel: str = "kubernetes.container_iostream"
    namespace_exact: str = "kubernetes.namespace_name.keyword"
    workload_exact: str = "kubernetes.labels.app.keyword"
    pod_exact: str = "kubernetes.pod_name.keyword"
    pod_instance_exact: str = "kubernetes.pod_id.keyword"
    container_exact: str = "kubernetes.container_name.keyword"
    container_instance_exact: str = "kubernetes.container_id.keyword"
    channel_exact: str = "kubernetes.container_iostream.keyword"
    cluster_id_exact: str = "openshift.cluster_id.keyword"

    def __post_init__(self):
        for item in fields(self):
            _text(getattr(self, item.name), item.name)


@dataclass(frozen=True, slots=True)
class OpenSearchConfig:
    """No environment lookup or I/O; the application supplies every setting.

    source_scope must identify this source profile consistently across reads.
    It namespaces document references; document cluster IDs identify streams.
    Hosts must address the same logical source. Passwords are never represented.
    """

    hosts: tuple[str, ...]
    username: str
    password: str = field(repr=False)
    use_ssl: bool
    verify_certs: bool
    index_expression: str
    connect_timeout: float
    request_timeout: float
    source_scope: str
    field_mapping: OpenSearchFieldMapping
    page_size_limit: int
    ca_bundle: str | None = None
    index_strategy: str = 'literal'

    def __post_init__(self):
        for name in ("username", "password", "index_expression", "source_scope"):
            _text(getattr(self, name), name)
        validate_index_strategy(self.index_expression, self.index_strategy)
        for name in ("use_ssl", "verify_certs"):
            if type(getattr(self, name)) is not bool:
                raise ValueError(f"{name} must be a bool")
        if type(self.hosts) is not tuple or not self.hosts:
            raise ValueError("hosts must be a nonempty tuple")
        for host in self.hosts:
            _host_url(host, self.use_ssl)
        for name in ("connect_timeout", "request_timeout"):
            value = getattr(self, name)
            if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
                raise ValueError(f"{name} must be finite and positive")
        if type(self.page_size_limit) is not int or self.page_size_limit <= 0:
            raise ValueError("page_size_limit must be a positive integer")
        if type(self.field_mapping) is not OpenSearchFieldMapping:
            raise ValueError("field_mapping must be OpenSearchFieldMapping")
        if self.ca_bundle is not None:
            _text(self.ca_bundle, "ca_bundle")
            if not self.use_ssl or not self.verify_certs:
                raise ValueError("ca_bundle requires use_ssl and verify_certs")
