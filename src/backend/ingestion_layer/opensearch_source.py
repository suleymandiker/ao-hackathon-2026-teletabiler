"""Bounded physical-line acquisition, independent of all processing layers.

Each call retains at most one requested page of records, not an interval history.
This is a record-count bound, not a byte-size bound on individual documents.
Short-page exhaustion describes the current search view only: there is no PIT,
late-arrival guarantee, checkpoint, deduplication or index-generation recovery.
The configured index scope must support the timestamp/sequence ordering; this
module does not establish global key uniqueness across arbitrary index sets.
"""

import base64
import binascii
from dataclasses import asdict
from datetime import datetime, timezone
import hashlib
import json
import math
from urllib.parse import quote

from ingestion_layer import contracts
from ingestion_layer.opensearch_client import OpenSearchClient
from ingestion_layer.opensearch_config import _host_url


class OpenSearchSourceError(ValueError):
    """Invalid read, cursor or page; diagnostics contain no source values."""


_SOURCE_FIELDS = (
    "timestamp", "message", "cluster_id", "sequence", "namespace", "workload",
    "pod", "pod_instance", "pod_owner", "container", "container_instance", "channel",
)
_STREAM_FIELDS = (
    "namespace", "workload", "pod", "pod_instance", "container", "container_instance", "channel",
)
_CURSOR_FORMAT = "opensearch-search-after-v1"
_MISSING = object()


def _nonempty_text(value):
    return type(value) is str and bool(value.strip())


def _order(values):
    # No coercion, timestamp parsing or rounding of returned ordering evidence.
    if type(values) is not list or len(values) != 2:
        raise OpenSearchSourceError("Expected exactly two sort values")
    for value in values:
        if (type(value) not in (str, int, float)
                or (type(value) is float and not math.isfinite(value))
                or (type(value) is str and not value)):
            raise OpenSearchSourceError("Invalid sort value")
    return tuple(values)


def _path(source, path):
    """Read nested source paths, also accepting literal dotted JSON keys."""
    current = source
    parts = path.split(".")
    for offset, part in enumerate(parts):
        if type(current) is not dict:
            return _MISSING
        remainder = ".".join(parts[offset:])
        if remainder in current:
            return current[remainder]
        current = current.get(part, _MISSING)
    return current


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _reject_constant(value):
    raise ValueError("Invalid JSON constant")


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate JSON key")
        result[key] = value
    return result


class OpenSearchSource:
    """Application owns the client lifecycle; this source has no mutable cursor.

    Namespace/workload/container are label selections, not Deployment identity.
    The caller carries the opaque cursor between reads of a fixed interval.
    """

    def __init__(self, client: OpenSearchClient):
        self._client = client
        self._config = client.config

    def read_page(
        self, *, start: datetime, end: datetime,
        namespace: str, workload: str,
        container: str | None = None,
        cursor: str | bytes | None = None,
        page_size: int | None = None,
    ) -> contracts.SourcePage:
        """Read [start, end); fail the entire page on any incomplete evidence.

        Endpoints must be timezone-aware datetimes (never relative date math).
        A full page is not exhausted, even if a subsequent read will be empty.
        Page size may change between reads; cursor binding excludes this limit,
        TLS settings and transport timeouts, which do not change the selection.
        """
        for endpoint in (start, end):
            if type(endpoint) is not datetime or endpoint.utcoffset() is None:
                raise OpenSearchSourceError("Interval endpoints must be aware datetimes")
        start = start.astimezone(timezone.utc)
        end = end.astimezone(timezone.utc)
        if start >= end:
            raise OpenSearchSourceError("Interval must satisfy start < end")
        for value in (namespace, workload):
            if not _nonempty_text(value):
                raise OpenSearchSourceError("Namespace and workload must be nonempty strings")
        if container is not None and not _nonempty_text(container):
            raise OpenSearchSourceError("Container must be a nonempty string")
        size = self._config.page_size_limit if page_size is None else page_size
        if type(size) is not int or not 0 < size <= self._config.page_size_limit:
            raise OpenSearchSourceError("Page size must be positive and within the configured limit")

        mapping = self._config.field_mapping
        filters = [
            {"range": {mapping.timestamp: {
                "gte": start.isoformat(),
                "lt": end.isoformat(),
            }}},
            {"term": {mapping.namespace_exact: namespace}},
            {"term": {mapping.workload_exact: workload}},
        ]
        if container is not None:
            filters.append({"term": {mapping.container_exact: container}})
        query = {
            "size": size,
            "query": {"bool": {"filter": filters}},
            "track_total_hits": False,
            "timeout": f"{math.ceil(self._config.request_timeout * 1000)}ms",
            "_source": [getattr(mapping, name) for name in _SOURCE_FIELDS],
            "sort": [{mapping.timestamp: "asc"}, {mapping.sequence: "asc"}],
        }
        binding = hashlib.sha256(_json({
            "format": _CURSOR_FORMAT,
            "source_scope": self._config.source_scope,
            "hosts": [_host_url(host, self._config.use_ssl) for host in self._config.hosts],
            "username": self._config.username,
            "index": self._config.index_expression,
            "mapping": asdict(mapping),
            "query": query["query"],
            "sort": query["sort"],
        }).encode("utf-8")).hexdigest()
        if cursor is not None:
            query["search_after"] = list(self._decode_cursor(cursor, binding))

        index = quote(self._config.index_expression, safe="*,.-_")
        payload = self._client.post_json(f"/{index}/_search", query)
        if type(payload) is not dict or payload.get("timed_out") is not False:
            raise OpenSearchSourceError("Search timed out or lacks completion evidence")
        shards = payload.get("_shards")
        if (type(shards) is not dict or type(shards.get("failed")) is not int
                or shards["failed"] != 0):
            raise OpenSearchSourceError("Search shard failure or missing shard status")
        hits = payload.get("hits")
        if type(hits) is not dict or type(hits.get("hits")) is not list:
            raise OpenSearchSourceError("Expected hits.hits list")
        hits = hits["hits"]
        if len(hits) > size:
            raise OpenSearchSourceError("Search returned more hits than requested")
        records = tuple(self._map_hit(hit) for hit in hits)
        full = len(records) == size
        next_cursor = None
        if full:
            token = {"format": _CURSOR_FORMAT, "binding": binding,
                     "sort": list(records[-1].retrieval_order)}
            next_cursor = base64.urlsafe_b64encode(_json(token).encode("utf-8")).decode("ascii")
        return contracts.SourcePage(
            records=records, interval_exhausted=not full, next_cursor=next_cursor,
            page_limit_reached=full, cycle_budget_reached=False,
        )

    @staticmethod
    def _decode_cursor(cursor, binding):
        token = None
        try:
            if type(cursor) not in (str, bytes):
                raise ValueError("Invalid cursor type")
            encoded = cursor.encode("ascii") if type(cursor) is str else cursor
            decoded = base64.b64decode(encoded, altchars=b"-_", validate=True)
            token = json.loads(decoded.decode("utf-8"), parse_constant=_reject_constant,
                               object_pairs_hook=_unique_object)
        except (ValueError, UnicodeError, binascii.Error, RecursionError):
            pass
        if (type(token) is not dict or set(token) != {"format", "binding", "sort"}
                or token.get("format") != _CURSOR_FORMAT or token.get("binding") != binding):
            raise OpenSearchSourceError("Invalid or incompatible OpenSearch cursor")
        return _order(token["sort"])

    def _map_hit(self, hit):
        if type(hit) is not dict:
            raise OpenSearchSourceError("Hit must be an object")
        for key in ("_index", "_id"):
            if not _nonempty_text(hit.get(key)):
                raise OpenSearchSourceError(f"Missing or invalid hit {key}")
        source = hit.get("_source")
        if type(source) is not dict:
            raise OpenSearchSourceError("Hit must contain a source object")
        values = {name: _path(source, getattr(self._config.field_mapping, name))
                  for name in _SOURCE_FIELDS}
        if type(values["message"]) is not str:
            raise OpenSearchSourceError("Missing or non-string message")
        timestamp = values["timestamp"]
        if (type(timestamp) not in (str, int, float)
                or (type(timestamp) is str and not timestamp.strip())
                or (type(timestamp) is float and not math.isfinite(timestamp))):
            raise OpenSearchSourceError("Missing or invalid source timestamp")
        for name in ("cluster_id",) + _STREAM_FIELDS:
            if not _nonempty_text(values[name]):
                raise OpenSearchSourceError(f"Missing or invalid stream field: {name}")
        if type(values["sequence"]) is not int:
            raise OpenSearchSourceError("Missing or non-integer sequence")
        order = _order(hit.get("sort"))
        version = hit.get("_version")
        if "_version" in hit and (type(version) is not int or version < 1):
            raise OpenSearchSourceError("Invalid document version")
        metadata = [("sequence", values["sequence"])]
        owner = values["pod_owner"]
        if owner is not _MISSING:
            if (type(owner) not in (str, int, float, bool, type(None))
                    or (type(owner) is float and not math.isfinite(owner))):
                raise OpenSearchSourceError("Invalid scalar pod_owner evidence")
            metadata.append(("pod_owner", owner))
        return contracts.IngestedLogRecord(
            raw_text=values["message"], source_timestamp_raw=timestamp,
            source_reference=contracts.SourceReference(
                source_scope=self._config.source_scope, source_partition=hit["_index"],
                record_id=hit["_id"], version=version,
            ),
            stream_identity=contracts.StreamIdentity(
                source_scope=values["cluster_id"],
                **{name: values[name] for name in _STREAM_FIELDS},
            ),
            retrieval_order=order, framing=contracts.Framing.PHYSICAL_LINE,
            metadata=tuple(metadata),
        )
