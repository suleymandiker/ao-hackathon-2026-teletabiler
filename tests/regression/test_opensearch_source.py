"""Offline acquisition tests: synthetic documents, fake HTTP, no processing state."""

import base64
import builtins
from collections import deque
from copy import deepcopy
from dataclasses import FrozenInstanceError, fields, replace
from datetime import datetime, timedelta, timezone, tzinfo
import importlib
import io
import json
from pathlib import Path
import socket
import sqlite3
import traceback
from types import SimpleNamespace

import pytest
import requests


START = datetime(2026, 10, 4, tzinfo=timezone.utc)
END = START + timedelta(minutes=5)
READ = dict(start=START, end=END, namespace="synthetic-ns", workload="synthetic-app")
PASSWORD = "synthetic-config-password"
RAW = "  synthetic private message\t\n"
AUTHORIZATION = "Authorization: Basic synthetic-auth-value"
SOURCE_PATHS = [
    "@timestamp", "message", "openshift.cluster_id", "openshift.sequence",
    "kubernetes.namespace_name", "kubernetes.labels.app", "kubernetes.pod_name",
    "kubernetes.pod_id", "kubernetes.pod_owner", "kubernetes.container_name",
    "kubernetes.container_id", "kubernetes.container_iostream",
]


class FakeResponse:
    def __init__(self, payload=None, status=200, error=None):
        self.payload = payload
        self.status_code = status
        self.error = error
        self.closed = False

    def json(self):
        if self.error is not None:
            raise self.error
        return self.payload

    @property
    def text(self):
        pytest.fail("Response bodies must not be used in diagnostics")

    def close(self):
        self.closed = True


class FakeSession:
    def __init__(self, *actions):
        self.actions = deque(actions)
        self.calls = []
        self.trust_env = True
        self.closed = False

    def post(self, url, **kwargs):
        self.calls.append((url, deepcopy(kwargs)))
        assert self.actions, "Unexpected request or eager prefetch"
        action = self.actions.popleft()
        if isinstance(action, Exception):
            raise action
        return action

    def close(self):
        self.closed = True


@pytest.fixture(autouse=True)
def isolated_backend(monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[2] / "src" / "backend"))

    def forbidden(*args, **kwargs):
        pytest.fail("OpenSearch acquisition must not access network or processing state")

    for owner, names in (
        (socket.socket, ("connect", "connect_ex")),
        (socket, ("create_connection", "getaddrinfo")),
        (sqlite3, ("connect",)), (sqlite3.dbapi2, ("connect",)),
        (requests.sessions.Session, ("request",)),
    ):
        for name in names:
            monkeypatch.setattr(owner, name, forbidden)
    original_import = builtins.__import__
    blocked = {
        "ai_engine", "parser_layer", "segmentation_layer", "template_layer",
        "aggregation_layer", "signal_qualification_layer", "correlation_layer",
        "incident_candidate_layer", "context_enrichment_layer", "rca_layer",
        "learning_planning_layer", "downstream_pipeline", "full_pipeline_v2",
        "input_package_layer", "opensearchpy", "elasticsearch", "drain3", "streamlit",
    }

    def guarded_import(name, *args, **kwargs):
        if name.split(".")[0] in blocked:
            pytest.fail(f"OpenSearch acquisition must not import {name}")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded_import)
    return forbidden


@pytest.fixture
def api():
    # Import after tripwires; module references also tolerate contract reload tests.
    return SimpleNamespace(
        config=importlib.import_module("ingestion_layer.opensearch_config"),
        client=importlib.import_module("ingestion_layer.opensearch_client"),
        source=importlib.import_module("ingestion_layer.opensearch_source"),
        contracts=importlib.import_module("ingestion_layer.contracts"),
    )


@pytest.fixture
def config(api):
    return api.config.OpenSearchConfig(
        hosts=("https://node-a.invalid:9200", "node-b.invalid:9200"),
        username="synthetic-reader", password=PASSWORD, use_ssl=True, verify_certs=True,
        index_expression="synthetic-logs-2026.10.04", connect_timeout=2.5,
        request_timeout=9, source_scope="synthetic-source-profile",
        field_mapping=api.config.OpenSearchFieldMapping(), page_size_limit=3,
    )


def hit(record_id="00017", sequence=123, raw=RAW):
    return {
        "_index": "synthetic-logs-2026.10.04", "_id": record_id, "_version": 7,
        "sort": [1791072000123, sequence],
        "_source": {
            "@timestamp": "2026-10-04T00:00:00.123456789Z", "message": raw,
            "openshift": {"cluster_id": "synthetic-cluster", "sequence": sequence},
            "kubernetes": {
                "namespace_name": "synthetic-ns", "labels": {"app": "synthetic-app"},
                "pod_name": "synthetic-pod", "pod_id": "synthetic-pod-uid",
                "pod_owner": "synthetic-owner", "container_name": "synthetic-container",
                "container_id": "synthetic-container-uid", "container_iostream": "stdout",
            },
        },
    }


def page(*hits):
    return {"timed_out": False, "_shards": {"failed": 0}, "hits": {"hits": list(hits)}}


def source_for(api, config, *payloads):
    session = FakeSession(*(FakeResponse(payload) for payload in payloads))
    client = api.client.OpenSearchClient(config, session=session)
    return api.source.OpenSearchSource(client), session


def assert_redacted(error):
    rendered = "".join(traceback.format_exception(type(error), error, error.__traceback__))
    for secret in (PASSWORD, RAW, AUTHORIZATION):
        assert secret not in str(error)
        assert secret not in repr(error)
        assert secret not in rendered
    assert error.__cause__ is None
    assert error.__context__ is None


def test_configuration_is_explicit_immutable_and_password_redacted(config):
    assert PASSWORD not in repr(config)
    assert PASSWORD not in str(config)
    assert config.password == PASSWORD
    with pytest.raises(FrozenInstanceError):
        config.page_size_limit = 99
    with pytest.raises(FrozenInstanceError):
        config.field_mapping.namespace_exact = "changed"


@pytest.mark.parametrize("changes", [
    {"hosts": ()}, {"hosts": ["https://node.invalid"]},
    {"hosts": ("https://user:synthetic-config-password@node.invalid",)},
    {"hosts": ("http://node.invalid",)}, {"hosts": ("https://node.invalid/path",)},
    {"hosts": ("https://node.invalid?Authorization=secret",)},
    {"hosts": ("https://node.invalid:bad",)}, {"hosts": ("https://[bad",)},
    {"hosts": ("https://node.invalid/#fragment",)}, {"hosts": ("https://node.invalid\n",)},
    {"username": ""}, {"password": None}, {"index_expression": " "},
    {"source_scope": ""}, {"connect_timeout": 0}, {"connect_timeout": True},
    {"request_timeout": float("inf")}, {"request_timeout": float("nan")},
    {"page_size_limit": 0}, {"page_size_limit": True}, {"page_size_limit": 1.5},
    {"use_ssl": "true"}, {"verify_certs": 1}, {"field_mapping": {}},
    {"ca_bundle": ""}, {"ca_bundle": "synthetic-ca.pem", "verify_certs": False},
])
def test_invalid_configuration_fails_without_value_disclosure(config, changes):
    with pytest.raises(ValueError) as error:
        replace(config, **changes)
    assert_redacted(error.value)


def test_exact_query_and_transport_configuration(api, config):
    source, session = source_for(api, config, page(hit()))
    result = source.read_page(**READ, page_size=2)
    assert len(session.calls) == 1
    url, request = session.calls[0]
    assert url == "https://node-a.invalid:9200/synthetic-logs-2026.10.04/_search"
    assert request == {
        "json": {
            "size": 2,
            "query": {"bool": {"filter": [
                {"range": {"@timestamp": {
                    "gte": "2026-10-04T00:00:00+00:00", "lt": "2026-10-04T00:05:00+00:00",
                }}},
                {"term": {"kubernetes.namespace_name.keyword": "synthetic-ns"}},
                {"term": {"kubernetes.labels.app.keyword": "synthetic-app"}},
            ]}},
            "track_total_hits": False, "timeout": "9000ms", "_source": SOURCE_PATHS,
            "sort": [{"@timestamp": "asc"}, {"openshift.sequence": "asc"}],
        },
        "auth": ("synthetic-reader", PASSWORD), "verify": True,
        "timeout": (2.5, 9), "allow_redirects": False,
    }
    assert session.trust_env is False
    assert result.interval_exhausted is True
    assert result.page_limit_reached is False
    assert result.cycle_budget_reached is False
    assert result.next_cursor is None


def test_optional_container_uses_its_explicit_exact_field(api, config):
    source, session = source_for(api, config, page())
    source.read_page(**READ, container="synthetic-container")
    assert session.calls[0][1]["json"]["query"]["bool"]["filter"][-1] == {
        "term": {"kubernetes.container_name.keyword": "synthetic-container"},
    }


def test_finite_interval_compares_instants_during_repeated_local_hour(api, config):
    class RepeatedHour(tzinfo):
        # Deterministic fold fixture; no operating-system timezone database.
        def utcoffset(self, value):
            return timedelta(hours=2 if value.fold == 0 else 1)

    start = datetime(2026, 10, 25, 2, 30, tzinfo=RepeatedHour(), fold=0)
    end = start.replace(fold=1)
    source, session = source_for(api, config, page())
    source.read_page(**(READ | {"start": start, "end": end}))
    assert session.calls[0][1]["json"]["query"]["bool"]["filter"][0] == {
        "range": {"@timestamp": {
            "gte": "2026-10-25T00:30:00+00:00", "lt": "2026-10-25T01:30:00+00:00",
        }},
    }
    with pytest.raises(api.source.OpenSearchSourceError, match="start < end"):
        source.read_page(**(READ | {"start": end, "end": start}))
    assert len(session.calls) == 1


def test_all_mapping_overrides_are_used_without_keyword_inference(api, config):
    mapping = api.config.OpenSearchFieldMapping(**{
        item.name: f"custom.{item.name}" for item in fields(config.field_mapping)
    })
    original = hit()
    values = [
        original["_source"]["@timestamp"], RAW, "synthetic-cluster", 123,
        "synthetic-ns", "synthetic-app", "synthetic-pod", "synthetic-pod-uid",
        "synthetic-owner", "synthetic-container", "synthetic-container-uid", "stdout",
    ]
    source_names = [item.name for item in fields(mapping) if not item.name.endswith("_exact")]
    original["_source"] = {getattr(mapping, name): value for name, value in zip(source_names, values)}
    source, session = source_for(api, replace(config, field_mapping=mapping), page(original))
    record = source.read_page(**READ, container="synthetic-container").records[0]
    query = session.calls[0][1]["json"]
    assert query["_source"] == [getattr(mapping, name) for name in source_names]
    assert query["sort"] == [{"custom.timestamp": "asc"}, {"custom.sequence": "asc"}]
    assert query["query"]["bool"]["filter"] == [
        {"range": {"custom.timestamp": {"gte": START.isoformat(), "lt": END.isoformat()}}},
        {"term": {"custom.namespace_exact": "synthetic-ns"}},
        {"term": {"custom.workload_exact": "synthetic-app"}},
        {"term": {"custom.container_exact": "synthetic-container"}},
    ]
    assert ".keyword" not in json.dumps(query)
    assert record.raw_text == RAW
    assert record.stream_identity.pod_instance == "synthetic-pod-uid"
    assert record.metadata == (("sequence", 123), ("pod_owner", "synthetic-owner"))


@pytest.mark.parametrize("timestamp_sort", [1791072000123, "0001791072000123", 1791072000123.25])
@pytest.mark.parametrize("cursor_as_bytes", [False, True])
def test_cursor_roundtrip_preserves_order_types_and_large_integer_precision(
    api, config, timestamp_sort, cursor_as_bytes,
):
    original = hit(sequence=2**80 + 123)
    original["sort"][0] = timestamp_sort
    source, session = source_for(api, config, page(original), page())
    first = source.read_page(**READ, page_size=1)
    assert first.records[0].retrieval_order == tuple(original["sort"])
    assert first.page_limit_reached and not first.interval_exhausted
    assert first.next_cursor
    assert "search_after" not in session.calls[0][1]["json"]
    assert PASSWORD not in base64.urlsafe_b64decode(first.next_cursor).decode()
    cursor = first.next_cursor.encode("ascii") if cursor_as_bytes else first.next_cursor
    second = source.read_page(**READ, cursor=cursor, page_size=2)
    after = session.calls[1][1]["json"]["search_after"]
    assert after == original["sort"]
    assert [type(value) for value in after] == [type(value) for value in original["sort"]]
    assert second.records == ()
    assert second.interval_exhausted and not second.page_limit_reached
    assert second.next_cursor is None and not second.cycle_budget_reached


def test_timestamp_ties_remain_distinct_and_next_page_starts_after_last_sequence(api, config):
    source, session = source_for(
        api, config, page(hit("a", 41), hit("b", 42)), page(hit("c", 43)),
    )
    first = source.read_page(**READ, page_size=2)
    second = source.read_page(**READ, page_size=2, cursor=first.next_cursor)
    assert [record.source_reference.record_id for record in first.records + second.records] == ["a", "b", "c"]
    assert [record.retrieval_order[1] for record in first.records + second.records] == [41, 42, 43]
    assert session.calls[1][1]["json"]["search_after"] == [1791072000123, 42]


@pytest.mark.parametrize("changes", [
    {"namespace": "other"}, {"workload": "other"}, {"container": "other"},
    {"start": START + timedelta(seconds=1)}, {"end": END + timedelta(seconds=1)},
])
def test_cursor_rejects_incompatible_selection_before_http(api, config, changes):
    source, session = source_for(api, config, page(hit()))
    cursor = source.read_page(**READ, page_size=1).next_cursor
    with pytest.raises(api.source.OpenSearchSourceError, match="cursor"):
        source.read_page(**(READ | changes), cursor=cursor)
    assert len(session.calls) == 1


@pytest.mark.parametrize("field,value", [
    ("source_scope", "other-profile"), ("index_expression", "other-index"),
    ("hosts", ("https://other.invalid",)), ("username", "other-reader"),
    ("field_mapping", None),
])
def test_cursor_rejects_incompatible_source_configuration(api, config, field, value):
    source, _ = source_for(api, config, page(hit()))
    cursor = source.read_page(**READ, page_size=1).next_cursor
    if field == "field_mapping":
        value = replace(config.field_mapping, pod_instance="different.instance")
    other, session = source_for(api, replace(config, **{field: value}))
    with pytest.raises(api.source.OpenSearchSourceError, match="cursor"):
        other.read_page(**READ, cursor=cursor)
    assert session.calls == []


@pytest.mark.parametrize("cursor", ["", "not-base64!", b"\xff", 3, {}, "e30=", "bnVsbA=="])
def test_malformed_cursor_fails_explicitly_before_http(api, config, cursor):
    source, session = source_for(api, config)
    with pytest.raises(api.source.OpenSearchSourceError, match="cursor") as error:
        source.read_page(**READ, cursor=cursor)
    assert session.calls == []
    assert_redacted(error.value)


@pytest.mark.parametrize("field,value", [
    ("format", "other-v1"), ("binding", "wrong"), ("sort", [1]),
    ("sort", [1, {}]), ("sort", [1, None]), ("extra", True),
])
def test_invalid_cursor_envelope_or_sort_is_rejected(api, config, field, value):
    source, session = source_for(api, config, page(hit()))
    token = source.read_page(**READ, page_size=1).next_cursor
    envelope = json.loads(base64.urlsafe_b64decode(token))
    envelope[field] = value
    cursor = base64.urlsafe_b64encode(json.dumps(envelope).encode()).decode()
    with pytest.raises(api.source.OpenSearchSourceError):
        source.read_page(**READ, cursor=cursor)
    assert len(session.calls) == 1


@pytest.mark.parametrize("raw", [RAW, "", " \t\n ", '{"synthetic": "message stays text"}',
                                 "header\r\n\tcontinuation\r\n", "synthetic \u03a9 \U0001f680"])
def test_raw_text_is_preserved_without_json_parsing_or_multiline_assembly(api, config, raw):
    source, _ = source_for(api, config, page(hit(raw=raw)))
    record = source.read_page(**READ).records[0]
    assert type(record.raw_text) is str
    assert record.raw_text == raw
    assert record.raw_text.encode("utf-8") == raw.encode("utf-8")
    assert record.framing is api.contracts.Framing.PHYSICAL_LINE


def test_source_reference_stream_identity_and_optional_evidence(api, config):
    original = hit()
    source, _ = source_for(api, config, page(original))
    record = source.read_page(**READ).records[0]
    assert record.source_reference == api.contracts.SourceReference(
        config.source_scope, original["_index"], "00017", version=7,
    )
    assert record.stream_identity == api.contracts.StreamIdentity(
        "synthetic-cluster", namespace="synthetic-ns", workload="synthetic-app",
        pod="synthetic-pod", pod_instance="synthetic-pod-uid",
        container="synthetic-container", container_instance="synthetic-container-uid", channel="stdout",
    )
    assert record.stream_identity.workload == "synthetic-app"
    assert record.source_timestamp_raw == "2026-10-04T00:00:00.123456789Z"
    assert record.source_timestamp is None and record.first_observed_at is None
    assert record.mapping_version is None
    assert record.metadata == (("sequence", 123), ("pod_owner", "synthetic-owner"))
    assert not hasattr(record, "event_id")


@pytest.mark.parametrize("field,value", [
    ("container_iostream", "stderr"), ("pod_id", "new-pod-uid"),
    ("container_id", "new-container-uid"),
])
def test_physical_stream_coordinates_remain_distinct(api, config, field, value):
    other = hit("other", 124)
    other["_source"]["kubernetes"][field] = value
    source, _ = source_for(api, config, page(hit(), other))
    records = source.read_page(**READ).records
    assert len({record.stream_identity for record in records}) == 2


def test_missing_optional_owner_and_version_remain_missing(api, config):
    original = hit()
    del original["_version"]
    del original["_source"]["kubernetes"]["pod_owner"]
    source, _ = source_for(api, config, page(original))
    record = source.read_page(**READ).records[0]
    assert record.source_reference.version is None
    assert record.metadata == (("sequence", 123),)


@pytest.mark.parametrize("timestamp", [1791072000123, 1791072000123.25])
def test_numeric_source_timestamp_preserves_type_without_parsing(api, config, timestamp):
    original = hit()
    original["_source"]["@timestamp"] = timestamp
    source, _ = source_for(api, config, page(original))
    record = source.read_page(**READ).records[0]
    assert type(record.source_timestamp_raw) is type(timestamp)
    assert record.source_timestamp_raw == timestamp
    assert record.source_timestamp is None


@pytest.mark.parametrize("changes", [
    {"timed_out": True}, {"timed_out": 0}, {"timed_out": None},
    {"_shards": {"failed": 1, "failures": [{"reason": RAW}]}},
    {"_shards": {"failed": False}}, {"_shards": {}}, {"_shards": None},
    {"hits": {}}, {"hits": []}, {"hits": {"hits": {}}},
])
def test_incomplete_or_malformed_page_rejected_even_with_hits(api, config, changes):
    payload = page(hit()) | changes
    source, session = source_for(api, config, payload)
    with pytest.raises(api.source.OpenSearchSourceError) as error:
        source.read_page(**READ)
    assert_redacted(error.value)
    assert len(session.calls) == 1


@pytest.mark.parametrize("field", ["timed_out", "_shards", "hits"])
def test_required_page_status_cannot_be_missing(api, config, field):
    payload = page(hit())
    del payload[field]
    source, _ = source_for(api, config, payload)
    with pytest.raises(api.source.OpenSearchSourceError):
        source.read_page(**READ)


@pytest.mark.parametrize("path", [
    ("_index",), ("_id",), ("_source",), ("sort",),
    ("_source", "message"), ("_source", "@timestamp"),
    ("_source", "openshift", "cluster_id"), ("_source", "openshift", "sequence"),
    ("_source", "kubernetes", "namespace_name"), ("_source", "kubernetes", "labels", "app"),
    ("_source", "kubernetes", "pod_name"), ("_source", "kubernetes", "pod_id"),
    ("_source", "kubernetes", "container_name"), ("_source", "kubernetes", "container_id"),
    ("_source", "kubernetes", "container_iostream"),
])
def test_missing_required_hit_field_rejects_whole_page(api, config, path):
    malformed = hit("bad", 124)
    parent = malformed
    for key in path[:-1]:
        parent = parent[key]
    del parent[path[-1]]
    source, session = source_for(api, config, page(hit(), malformed), page(hit(), hit("good", 124)))
    with pytest.raises(api.source.OpenSearchSourceError) as error:
        source.read_page(**READ, page_size=2)
    assert_redacted(error.value)
    # Repeating the read has no silently advanced internal cursor or partial result.
    result = source.read_page(**READ, page_size=2)
    assert len(result.records) == 2
    assert session.calls[0] == session.calls[1]


@pytest.mark.parametrize("path,value", [
    (("_index",), ""), (("_id",), 17), (("_source",), []),
    (("_source", "message"), None), (("_source", "message"), {"secret": RAW}),
    (("_source", "message"), 17), (("_source", "message"), [RAW]),
    (("_source", "@timestamp"), None), (("_source", "@timestamp"), True),
    (("_source", "@timestamp"), {}), (("_source", "@timestamp"), ""),
    (("_source", "openshift", "sequence"), "123"),
    (("_source", "openshift", "sequence"), True),
    (("_source", "kubernetes", "pod_id"), ""),
    (("_source", "kubernetes", "pod_owner"), {"secret": RAW}),
    (("_version",), None), (("_version",), True), (("_version",), "7"),
    (("sort",), []), (("sort",), [1]), (("sort",), [1, 2, 3]),
    (("sort",), [1, None]), (("sort",), [1, True]), (("sort",), [1, {}]),
    (("sort",), [float("nan"), 2]), (("sort",), [float("inf"), 2]),
    (("sort",), ["", 2]), (("sort",), (1, 2)),
])
def test_invalid_hit_values_are_rejected_without_coercion(api, config, path, value):
    malformed = hit()
    parent = malformed
    for key in path[:-1]:
        parent = parent[key]
    parent[path[-1]] = value
    source, _ = source_for(api, config, page(malformed))
    with pytest.raises(api.source.OpenSearchSourceError) as error:
        source.read_page(**READ)
    assert_redacted(error.value)


@pytest.mark.parametrize("malformed", [None, [], "synthetic-invalid-hit"])
def test_non_object_hits_fail(api, config, malformed):
    source, _ = source_for(api, config, page(malformed))
    with pytest.raises(api.source.OpenSearchSourceError, match="Hit"):
        source.read_page(**READ)


@pytest.mark.parametrize("changes", [
    {"page_size": 0}, {"page_size": -1}, {"page_size": 4}, {"page_size": True},
    {"page_size": 1.5}, {"namespace": ""}, {"workload": None}, {"container": ""},
    {"start": START.replace(tzinfo=None)}, {"end": "now"}, {"end": START},
    {"start": END + timedelta(seconds=1)},
])
def test_invalid_read_is_rejected_before_request(api, config, changes):
    source, session = source_for(api, config)
    with pytest.raises(api.source.OpenSearchSourceError):
        source.read_page(**(READ | changes))
    assert session.calls == []


def test_default_page_bound_no_prefetch_and_oversized_response_rejected(api, config):
    source, session = source_for(api, config, page(*(hit(str(i), i) for i in range(3))))
    result = source.read_page(**READ)
    assert len(result.records) == config.page_size_limit
    assert result.page_limit_reached and not result.interval_exhausted
    assert session.calls[0][1]["json"]["size"] == config.page_size_limit
    assert len(session.calls) == 1
    source, _ = source_for(api, config, page(*(hit(str(i), i) for i in range(4))))
    with pytest.raises(api.source.OpenSearchSourceError, match="more hits"):
        source.read_page(**READ)


@pytest.mark.parametrize("status", [301, 302, 400, 401, 403, 404, 422])
def test_permanent_http_errors_and_redirects_are_not_retried(api, config, status):
    response = FakeResponse({"secret": RAW, "auth": AUTHORIZATION}, status=status)
    session = FakeSession(response)
    client = api.client.OpenSearchClient(config, session=session)
    with pytest.raises(api.client.OpenSearchClientError, match=str(status)) as error:
        client.post_json("/synthetic/_search", {})
    assert len(session.calls) == 1 and response.closed
    assert_redacted(error.value)


@pytest.mark.parametrize("failure", [
    requests.exceptions.ConnectionError, requests.exceptions.ConnectTimeout,
    requests.exceptions.ReadTimeout,
])
def test_transient_transport_failure_fails_over_to_next_host(api, config, failure):
    response = FakeResponse(page())
    session = FakeSession(failure(PASSWORD + AUTHORIZATION + RAW), response)
    client = api.client.OpenSearchClient(config, session=session)
    assert client.post_json("/synthetic/_search", {}) == page()
    assert [call[0] for call in session.calls] == [
        "https://node-a.invalid:9200/synthetic/_search", "https://node-b.invalid:9200/synthetic/_search",
    ]
    assert response.closed


@pytest.mark.parametrize("status", [429, 500, 502, 503, 504])
def test_transient_http_status_can_fail_over(api, config, status):
    failed, succeeded = FakeResponse({}, status=status), FakeResponse(page())
    session = FakeSession(failed, succeeded)
    client = api.client.OpenSearchClient(config, session=session)
    assert client.post_json("/synthetic/_search", {}) == page()
    assert len(session.calls) == 2
    assert failed.closed and succeeded.closed


@pytest.mark.parametrize("failure", [
    requests.exceptions.SSLError, requests.exceptions.InvalidURL,
    requests.exceptions.InvalidSchema, requests.exceptions.InvalidHeader, OSError,
])
def test_tls_and_request_configuration_failures_are_sanitized_without_retry(api, config, failure):
    session = FakeSession(failure(PASSWORD + AUTHORIZATION + RAW))
    client = api.client.OpenSearchClient(config, session=session)
    with pytest.raises(api.client.OpenSearchClientError) as error:
        client.post_json("/synthetic/_search", {})
    assert len(session.calls) == 1
    assert_redacted(error.value)


@pytest.mark.parametrize("host_count", [1, 2])
@pytest.mark.parametrize("http_failure", [False, True])
def test_retry_budget_is_bounded_and_exhaustion_is_sanitized(api, config, host_count, http_failure):
    config = replace(config, hosts=config.hosts[:host_count])
    failures = [
        FakeResponse({"secret": RAW}, status=503) if http_failure else
        requests.exceptions.ConnectionError(PASSWORD + AUTHORIZATION + RAW)
        for _ in range(host_count * 2)
    ]
    session = FakeSession(*failures)
    client = api.client.OpenSearchClient(config, session=session)
    with pytest.raises(api.client.OpenSearchClientError) as error:
        client.post_json("/synthetic/_search", {})
    assert len(session.calls) == host_count * 2
    assert_redacted(error.value)
    if http_failure:
        assert all(response.closed for response in failures)


@pytest.mark.parametrize("payload", [None, [], "synthetic-text", 17, False])
def test_http_success_requires_json_object_without_retry(api, config, payload):
    response = FakeResponse(payload)
    session = FakeSession(response)
    client = api.client.OpenSearchClient(config, session=session)
    with pytest.raises(api.client.OpenSearchClientError, match="JSON object"):
        client.post_json("/synthetic/_search", {})
    assert len(session.calls) == 1 and response.closed


def test_json_decode_error_does_not_leak_body_or_exception_chain(api, config):
    response = FakeResponse(error=ValueError(PASSWORD + AUTHORIZATION + RAW))
    session = FakeSession(response)
    client = api.client.OpenSearchClient(config, session=session)
    with pytest.raises(api.client.OpenSearchClientError) as error:
        client.post_json("/synthetic/_search", {})
    assert len(session.calls) == 1 and response.closed
    assert_redacted(error.value)


@pytest.mark.parametrize("use_ssl,verify,ca_bundle", [
    (False, False, None), (True, False, None), (True, True, "synthetic-ca.pem"),
])
def test_tls_settings_are_explicitly_applied(api, config, use_ssl, verify, ca_bundle):
    config = replace(config, hosts=("node.invalid:9200",), use_ssl=use_ssl,
                     verify_certs=verify, ca_bundle=ca_bundle)
    source, session = source_for(api, config, page())
    source.read_page(**READ)
    assert session.calls[0][0].startswith("https://" if use_ssl else "http://")
    assert session.calls[0][1]["verify"] == (ca_bundle or verify)


def test_index_expression_is_used_explicitly_without_expansion(api, config):
    source, session = source_for(api, replace(config, index_expression="custom-a,custom-b*"), page())
    source.read_page(**READ)
    assert session.calls[0][0].endswith("/custom-a,custom-b*/_search")
    assert len(session.calls) == 1


def test_client_closes_only_owned_session(api, config, monkeypatch):
    owned = FakeSession()
    monkeypatch.setattr(requests, "Session", lambda: owned)
    with api.client.OpenSearchClient(config) as client:
        assert client.config is config
    assert owned.closed
    injected = FakeSession()
    with api.client.OpenSearchClient(config, session=injected):
        pass
    assert not injected.closed


def test_construction_and_read_have_no_file_environment_or_processing_side_effects(
    api, config, monkeypatch, isolated_backend,
):
    import os

    session = FakeSession(FakeResponse(page(hit())))
    with monkeypatch.context() as guard:
        guard.setattr(builtins, "open", isolated_backend)
        guard.setattr(io, "open", isolated_backend)
        guard.setattr(os, "getenv", isolated_backend)
        guard.setattr(type(os.environ), "__getitem__", isolated_backend)
        guard.setattr(requests, "Session", lambda: session)
        client = api.client.OpenSearchClient(config)
        source = api.source.OpenSearchSource(client)
        assert session.calls == []
        record = source.read_page(**READ).records[0]
        assert record.raw_text == RAW
        assert not hasattr(record, "event_id")
    # The autouse fixture also forbids every processing-layer import and SQLite.
    assert len(session.calls) == 1
