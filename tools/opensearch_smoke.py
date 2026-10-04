#!/usr/bin/env python3
"""MANUAL, read-only OpenSearch acquisition smoke test. Never scheduled by CI.

Run from Linux/Citrix after supplying environment variables:
    python3 -B tools/opensearch_smoke.py

Required environment variables (no credentials or hosts are stored by this tool):
    OPENSEARCH_HOSTS                 Comma-separated origins, without credentials
    OPENSEARCH_USERNAME
    OPENSEARCH_PASSWORD
    OPENSEARCH_USE_SSL               true or false; must match host schemes
    OPENSEARCH_VERIFY_CERTS          true or false
    OPENSEARCH_SOURCE_SCOPE          Stable source/profile identifier
    OPENSEARCH_INDEX                 Explicit index expression

Optional environment variables:
    OPENSEARCH_CA_BUNDLE               CA file; requires SSL and verification
    OPENSEARCH_CONNECT_TIMEOUT_SECONDS 5
    OPENSEARCH_REQUEST_TIMEOUT_SECONDS 15
    OPENSEARCH_SMOKE_LOOKBACK_MINUTES   15 (positive integer)
    OPENSEARCH_SMOKE_END                UTC now, sampled once; or an aware ISO time
    OPENSEARCH_SMOKE_NAMESPACE          ai-voice
    OPENSEARCH_SMOKE_WORKLOAD           aihub-foya-stt-apis-http
    OPENSEARCH_SMOKE_CONTAINER          Unset/empty: no container filter
    OPENSEARCH_SMOKE_PAGE_SIZE          3 (positive integer)

Namespace/workload defaults belong only to this manual tool, not production.
For a historical index, set OPENSEARCH_SMOKE_END to a time covering known records.
The fixed interval [end - lookback, end) is reused for page 1, page 2 and the replay.
If page 1 is short, rerun with a larger lookback and the printed end timestamp.
Exit 0 means the sampled checks passed; exit 1 means failure/insufficient data.
This does not establish snapshot completeness or exactly-once delivery.
"""

import argparse
from datetime import datetime, timedelta, timezone
import json
import logging
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "backend"))

from ingestion_layer.contracts import Framing
from ingestion_layer.opensearch_client import OpenSearchClient, OpenSearchClientError
from ingestion_layer.opensearch_config import OpenSearchConfig, OpenSearchFieldMapping
from ingestion_layer.opensearch_source import OpenSearchSource, OpenSearchSourceError


WIDEN = (
    "The bounded interval may need widening: increase OPENSEARCH_SMOKE_LOOKBACK_MINUTES "
    "and rerun with OPENSEARCH_SMOKE_END set to the end printed for this run."
)


class SmokeFailure(Exception):
    """Only fixed diagnostic text, never source or credential values."""


def require(condition, message):
    # Keep validation active even when Python runs with optimization enabled.
    if not condition:
        raise SmokeFailure(message)


def required_env(name):
    value = os.environ.get(name)
    require(value is not None and bool(value.strip()), "Missing environment variable: " + name)
    return value


def boolean_env(name):
    value = required_env(name).strip().lower()
    require(value in ("true", "false"), name + " must be true or false")
    return value == "true"


def emit(metadata):
    # Explicit allowlist below; JSON escaping also prevents terminal control text.
    print(json.dumps(metadata, ensure_ascii=True, allow_nan=False), flush=True)


def identity(record):
    ref = record.source_reference
    # Version changes must not hide overlap of the same source document.
    return (ref.source_scope, ref.source_partition, ref.record_id, ref.generation)


def validate_page(page, selection, label):
    require(len(page.records) <= selection["page_size"], label + ": page size exceeded")
    refs = [record.source_reference for record in page.records]
    keys = [identity(record) for record in page.records]
    require(len(set(refs)) == len(refs), label + ": duplicate SourceReferences")
    require(len(set(keys)) == len(keys), label + ": duplicate document identities")
    orders = []
    for record in page.records:
        stream = record.stream_identity
        require(stream is not None, label + ": missing StreamIdentity")
        for name in ("namespace", "workload", "container"):
            if selection[name] is not None:
                require(getattr(stream, name) == selection[name], label + ": " + name + " mismatch")
        for name in ("pod", "pod_instance", "container", "container_instance"):
            value = getattr(stream, name)
            require(type(value) is str and bool(value.strip()), label + ": missing " + name)
        require(stream.channel in ("stdout", "stderr"), label + ": invalid channel")
        require(record.framing is Framing.PHYSICAL_LINE, label + ": framing mismatch")
        require(type(record.raw_text) is str, label + ": raw_text must be a string")
        order = record.retrieval_order
        require(type(order) is tuple and len(order) == 2, label + ": invalid retrieval_order")
        orders.append(order)
    require(all(a < b for a, b in zip(orders, orders[1:])),
            label + ": retrieval_order is not strictly increasing")


def print_page(label, page):
    emit({
        "page": label, "record_count": len(page.records),
        "interval_exhausted": page.interval_exhausted,
        "page_limit_reached": page.page_limit_reached,
        "next_cursor_exists": page.next_cursor is not None,
    })
    for record in page.records:
        ref, stream = record.source_reference, record.stream_identity
        emit({
            "page": label, "source_partition": ref.source_partition,
            "record_id": ref.record_id, "version": ref.version,
            "namespace": stream.namespace, "workload": stream.workload,
            "pod": stream.pod, "pod_instance_present": bool(stream.pod_instance),
            "container": stream.container,
            "container_instance_present": bool(stream.container_instance),
            "channel": stream.channel, "retrieval_order": record.retrieval_order,
            "framing": record.framing.value, "raw_text_length": len(record.raw_text),
        })


def load_configuration():
    """Share manual smoke environment/TLS handling without opening a client."""
    # Fix both endpoints before any requests. No subsequent call samples now.
    end_text = os.environ.get("OPENSEARCH_SMOKE_END")
    if end_text:
        end = datetime.fromisoformat(
            end_text[:-1] + "+00:00" if end_text.endswith("Z") else end_text
        )
        require(end.utcoffset() is not None, "OPENSEARCH_SMOKE_END must include a timezone")
        end = end.astimezone(timezone.utc)
    else:
        end = datetime.now(timezone.utc)
    minutes = int(os.environ.get("OPENSEARCH_SMOKE_LOOKBACK_MINUTES", "15"))
    require(minutes > 0, "OPENSEARCH_SMOKE_LOOKBACK_MINUTES must be positive")
    start = end - timedelta(minutes=minutes)
    size = int(os.environ.get("OPENSEARCH_SMOKE_PAGE_SIZE", "3"))
    config = OpenSearchConfig(
        hosts=tuple(part.strip() for part in required_env("OPENSEARCH_HOSTS").split(",")),
        username=required_env("OPENSEARCH_USERNAME"),
        password=required_env("OPENSEARCH_PASSWORD"),
        use_ssl=boolean_env("OPENSEARCH_USE_SSL"),
        verify_certs=boolean_env("OPENSEARCH_VERIFY_CERTS"),
        index_expression=required_env("OPENSEARCH_INDEX"),
        connect_timeout=float(os.environ.get("OPENSEARCH_CONNECT_TIMEOUT_SECONDS", "5")),
        request_timeout=float(os.environ.get("OPENSEARCH_REQUEST_TIMEOUT_SECONDS", "15")),
        source_scope=required_env("OPENSEARCH_SOURCE_SCOPE"),
        field_mapping=OpenSearchFieldMapping(), page_size_limit=size,
        ca_bundle=os.environ.get("OPENSEARCH_CA_BUNDLE") or None,
    )
    selection = dict(
        start=start, end=end, page_size=size,
        namespace=os.environ.get("OPENSEARCH_SMOKE_NAMESPACE", "ai-voice"),
        workload=os.environ.get("OPENSEARCH_SMOKE_WORKLOAD", "aihub-foya-stt-apis-http"),
        container=os.environ.get("OPENSEARCH_SMOKE_CONTAINER") or None,
    )
    return config, selection


def run_smoke():
    config, selection = load_configuration()
    start, end, size = selection["start"], selection["end"], selection["page_size"]
    emit({"index": config.index_expression, "start": start.isoformat(),
          "end": end.isoformat(), "page_size": size})
    with OpenSearchClient(config) as client:
        source = OpenSearchSource(client)
        first = source.read_page(**selection, cursor=None)
        validate_page(first, selection, "page1")
        print_page("page1", first)
        require(bool(first.records), "page1 is empty. " + WIDEN)
        if len(first.records) < size:
            emit({"notice": "page1 has fewer records than the configured page size. " + WIDEN})

        second = None
        if first.next_cursor is not None:
            second = source.read_page(**selection, cursor=first.next_cursor)
            validate_page(second, selection, "page2")
            print_page("page2", second)
            require({r.source_reference for r in first.records}.isdisjoint(
                    r.source_reference for r in second.records), "Pages overlap by SourceReference")
            require({identity(r) for r in first.records}.isdisjoint(
                    identity(r) for r in second.records), "Pages overlap by document identity")
            if second.records:
                require(second.records[0].retrieval_order > first.records[-1].retrieval_order,
                        "page2 does not advance beyond page1")

        repeated = source.read_page(**selection, cursor=None)
        validate_page(repeated, selection, "page1_repeat")
        print_page("page1_repeat", repeated)
        require([(r.source_reference, r.retrieval_order) for r in first.records] ==
                [(r.source_reference, r.retrieval_order) for r in repeated.records],
                "Repeated page1 identities/order changed within the fixed interval")
        require(len(first.records) == size, "Insufficient page1 records. " + WIDEN)

    emit({"result": "PASS", "page1_repeat_stable": True,
          "page2_read": second is not None,
          "page2_nonempty": second is not None and bool(second.records)})


def main(argv=None):
    argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
    ).parse_args(argv)
    try:
        run_smoke()
        return 0
    except (SmokeFailure, OpenSearchClientError, OpenSearchSourceError) as exc:
        # These classes contain fixed checks or sanitized production diagnostics.
        print("FAIL: " + str(exc), file=sys.stderr)
    except Exception:
        # Never print arbitrary exception values or tracebacks with source data.
        print("FAIL: configuration or unexpected validation error; details suppressed.", file=sys.stderr)
    return 1


if __name__ == "__main__":
    logging.disable(logging.CRITICAL)
    raise SystemExit(main())
