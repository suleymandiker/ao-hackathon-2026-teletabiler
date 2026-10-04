#!/usr/bin/env python3
"""MANUAL, read-only OpenSearch -> SegmentationSession smoke; never run by CI.

Run from Linux/Citrix at the repository root:
    python3 -B tools/segmentation_session_smoke.py

Reuse tools/opensearch_smoke.py's environment interface, including required:
    OPENSEARCH_HOSTS, OPENSEARCH_USERNAME, OPENSEARCH_PASSWORD,
    OPENSEARCH_USE_SSL, OPENSEARCH_VERIFY_CERTS, OPENSEARCH_SOURCE_SCOPE,
    OPENSEARCH_INDEX
and optional CA bundle, timeouts and OPENSEARCH_SMOKE_* selection variables.
OPENSEARCH_SMOKE_PAGE_SIZE defaults to 3; LOOKBACK_MINUTES defaults to 15.
OPENSEARCH_SMOKE_END is an aware ISO time or UTC now, sampled once.
Namespace/workload defaults are inherited from the acquisition smoke; set
OPENSEARCH_SMOKE_NAMESPACE and OPENSEARCH_SMOKE_WORKLOAD for your selection.

Also REQUIRED (caller-attested prevalidated policy; no discovery/admission):
    SEGMENTATION_SMOKE_REGEX
    SEGMENTATION_SMOKE_POLICY_ID
    SEGMENTATION_SMOKE_VALIDATION_REFERENCE
The same immutable snapshot applies to every complete selected stream key.
Regex compilation checks syntax only, not safety or applicability.

Exactly two nonempty pages are acquired over one fixed [start, end) interval.
Only this finite manual sample and its outputs are retained for offline replay:
at most twice the configured page size in records (not a byte-size guarantee).
No parser, registry, LLM, persistence, checkpoint, watermark or worker is used.
No snapshot completeness or exactly-once semantics are asserted.

Output is JSON Lines: fixed diagnostics, counts, lengths, booleans and normalized
requested timestamps only. Source/config identifiers, policies, raw text, cursor
values and exception details are never printed. The last row is the sole result.
Logging and warnings are suppressed only during this manual CLI invocation;
their previous settings are restored, and TLS verification is still enforced
according to OPENSEARCH_VERIFY_CERTS / OPENSEARCH_CA_BUNDLE.
Exit 0 is PASS; exit 1 is FAIL (including insufficient data). A PASS with
cross_page_multiline_observed=false validates structural page neutrality but
does not claim a real spanning event was observed. Change page size/lookback/
query to obtain one; reuse the printed end via OPENSEARCH_SMOKE_END if desired.
"""

import argparse
from collections import defaultdict, deque
import logging
from pathlib import Path
import re
import sys
import warnings

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "backend"))

from opensearch_smoke import emit, load_configuration, required_env
from ingestion_layer.contracts import Framing
from ingestion_layer.opensearch_client import OpenSearchClient, OpenSearchClientError
from ingestion_layer.opensearch_source import OpenSearchSource, OpenSearchSourceError
from segmentation_layer.contracts import AssembledEvent, SegmentationPolicy, StreamKey, UnassembledRecord
from segmentation_layer.segmentation_session import SegmentationSession


FAILURE_CODES = frozenset({
    "configuration_error", "invalid_arguments", "acquisition_error", "runtime_error",
    "insufficient_pages", "page_size_exceeded", "record_not_accepted",
    "unexpected_emission_reason", "stream_contamination", "provenance_mismatch",
    "page_boundary_state_changed", "page_boundary_not_equivalent",
    "close_did_not_release_state",
})


class SmokeFailure(Exception):
    """An internal validation failure; only allowlisted codes reach stdout."""


def require(condition, code):
    if not condition:
        raise SmokeFailure(code)


class SafeArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        # argparse's default error includes caller-supplied argument values.
        raise SmokeFailure("invalid_arguments")


class ProvenanceAudit:
    """Audit original input occurrences in per-stream FIFO order, without I/O.

    Page membership attaches to an occurrence, not a SourceReference value:
    equal references (or even repeated input objects) must not imply spanning.
    The audit retains at most the two configured pages, just like the replay.
    """

    def __init__(self, policy):
        self.policy = policy
        self.pending = defaultdict(deque)
        self.nonblank_keys = set()
        self.expect_header = set()
        self.cross_page = False
        self.blank_outputs = 0
        self.records_read = 0

    def add_page(self, page, number, page_size):
        require(bool(page.records), "insufficient_pages")
        require(len(page.records) <= page_size, "page_size_exceeded")
        for record in page.records:
            key = StreamKey.from_identity(record.stream_identity)
            require(key is not None and record.framing is Framing.PHYSICAL_LINE,
                    "record_not_accepted")
            self.pending[key].append((record, number))
            if record.raw_text and not record.raw_text.isspace():
                self.nonblank_keys.add(key)
            self.records_read += 1

    def consume(self, outputs, reason):
        for output in outputs:
            if isinstance(output, AssembledEvent):
                require(output.emission_reason == reason, "unexpected_emission_reason")
                records, evidence = output.records, output.evidence
            else:
                require(reason == "next_header" and isinstance(output, UnassembledRecord),
                        "unexpected_emission_reason")
                require(output.reason == "blank_context" and output.evidence is not None,
                        "record_not_accepted")
                records, evidence = (output.record,), (output.evidence,)
                require(not output.evidence.included, "provenance_mismatch")
                self.blank_outputs += 1

            key = output.stream_key
            require(key is not None and all(
                StreamKey.from_identity(record.stream_identity) == key for record in records
            ), "stream_contamination")
            require(output.policy == self.policy, "provenance_mismatch")
            require(bool(records) and len(records) == len(evidence), "provenance_mismatch")
            if key in self.expect_header:
                require(isinstance(output, AssembledEvent) and
                        evidence[0].decision.start_new_event, "unexpected_emission_reason")
                self.expect_header.remove(key)

            membership = set()
            queue = self.pending.get(key)
            for record, item in zip(records, evidence):
                require(bool(queue) and queue[0][0] is record, "provenance_mismatch")
                membership.add(queue.popleft()[1])
                nonblank = bool(record.raw_text) and not record.raw_text.isspace()
                require(item.included == nonblank, "provenance_mismatch")

            if isinstance(output, AssembledEvent):
                require(output.text == "\n".join(
                    record.raw_text for record, item in zip(records, evidence) if item.included
                ), "provenance_mismatch")
                if reason == "next_header":
                    # The boundary header must already have arrived in this
                    # page/prefix and belong to this same stream's next event.
                    require(bool(queue), "unexpected_emission_reason")
                    self.expect_header.add(key)
                self.cross_page |= membership == {1, 2}
                emit({"emission_reason": reason, "assembled_event_length": len(output.text),
                      "contributor_count": len(records)})

    def check_open(self, session):
        remaining = sum(len(queue) for queue in self.pending.values())
        require(not session.closed and session.active_stream_count == len(self.pending)
                and session.pending_record_count == remaining, "page_boundary_state_changed")
        # Every stream that has seen content must still have a tail: a page
        # end cannot consume it, even if an erroneous flush is mislabeled.
        require(all(self.pending[key] for key in self.nonblank_keys), "page_boundary_state_changed")

    def check_complete(self):
        require(not any(self.pending.values()) and not self.expect_header, "provenance_mismatch")


def state_snapshot(session):
    return session.closed, session.active_stream_count, session.pending_record_count


def close_checked(session):
    outputs = tuple(session.close())
    require(state_snapshot(session) == (True, 0, 0), "close_did_not_release_state")
    require(all(isinstance(output, AssembledEvent) and output.emission_reason == "analysis_end"
                for output in outputs), "unexpected_emission_reason")
    require(not tuple(session.close()) and state_snapshot(session) == (True, 0, 0),
            "close_did_not_release_state")
    return outputs


def run_smoke():
    try:
        config, selection = load_configuration()
        policy = SegmentationPolicy(
            policy_id=required_env("SEGMENTATION_SMOKE_POLICY_ID"),
            regex_pattern=required_env("SEGMENTATION_SMOKE_REGEX"),
            validation_reference=required_env("SEGMENTATION_SMOKE_VALIDATION_REFERENCE"),
        )
        re.compile(policy.regex_pattern)  # Syntax only; caller owns prevalidation.
    except Exception:
        raise SmokeFailure("configuration_error") from None

    emit({"start": selection["start"].isoformat(), "end": selection["end"].isoformat(),
          "page_size": selection["page_size"], "verify_certs": config.verify_certs})

    def provider(key, first_record):
        return policy

    paged = SegmentationSession(provider)
    audit = ProvenanceAudit(policy)
    pages, paged_outputs = [], []
    cursor = None
    with OpenSearchClient(config) as client:
        source = OpenSearchSource(client)
        for number in (1, 2):
            before_read = state_snapshot(paged)
            page = source.read_page(**selection, cursor=cursor)
            require(state_snapshot(paged) == before_read, "page_boundary_state_changed")
            audit.add_page(page, number, selection["page_size"])
            if number == 1:
                require(page.next_cursor is not None, "insufficient_pages")
            pages.append(page)
            outputs = tuple(paged.feed_page(page))
            audit.consume(outputs, "next_header")
            audit.check_open(paged)
            paged_outputs.extend(outputs)
            emit({"page": number, "record_count": len(page.records),
                  "raw_text_total_length": sum(len(record.raw_text) for record in page.records),
                  "next_header_outputs": sum(isinstance(output, AssembledEvent) for output in outputs),
                  "active_stream_count": paged.active_stream_count,
                  "pending_record_count": paged.pending_record_count})
            cursor = page.next_cursor

    tails = close_checked(paged)
    audit.consume(tails, "analysis_end")
    audit.check_complete()
    paged_outputs.extend(tails)

    continuous = SegmentationSession(provider)
    continuous_outputs = []
    for page in pages:
        for record in page.records:
            continuous_outputs.extend(continuous.feed(record))
    continuous_outputs.extend(close_checked(continuous))
    # Immutable contract equality includes text, ordered original contributors
    # (and SourceReferences), keys, reasons, counts, evidence and policy. Never
    # print either side, even on mismatch. No session state is shared.
    require(paged_outputs == continuous_outputs, "page_boundary_not_equivalent")

    if not audit.cross_page:
        emit({"notice": "Cross-page multiline NOT OBSERVED. Retry with another "
              "OPENSEARCH_SMOKE_PAGE_SIZE, OPENSEARCH_SMOKE_LOOKBACK_MINUTES or query selection."})
    keys = audit.pending.keys()
    emit({
        "result": "PASS", "pages_read": len(pages), "records_read": audit.records_read,
        "stream_count": len(keys), "channel_count": len({key.channel for key in keys}),
        "pod_instance_count": len({key.pod_instance for key in keys}),
        "container_instance_count": len({key.container_instance for key in keys}),
        "page_boundary_structurally_valid": True, "page_boundary_equivalent": True,
        "cross_page_multiline_observed": audit.cross_page, "stream_isolation_valid": True,
        "close_released_all_state": True, "analysis_end_outputs": len(tails),
        "blank_context_outputs": audit.blank_outputs,
        "active_stream_count": paged.active_stream_count,
        "pending_record_count": paged.pending_record_count,
    })


def main(argv=None):
    previous_logging = logging.root.manager.disable
    try:
        # Scope diagnostic suppression to this manual entry point and restore
        # both settings on success/failure. Production TLS behavior is unchanged.
        logging.disable(sys.maxsize)
        with warnings.catch_warnings():
            # Warning messages can contain hosts, policy text or source data.
            # Only this CLI's explicit diagnostic allowlist may reach output.
            warnings.simplefilter("ignore")
            SafeArgumentParser(
                prog="segmentation_session_smoke.py", description=__doc__,
                formatter_class=argparse.RawDescriptionHelpFormatter,
            ).parse_args(argv)
            run_smoke()
        return 0
    except SmokeFailure as exc:
        code = exc.args[0] if exc.args else None
        reason = code if type(code) is str and code in FAILURE_CODES else "runtime_error"
    except (OpenSearchClientError, OpenSearchSourceError):
        reason = "acquisition_error"
    except Exception:
        reason = "runtime_error"
    finally:
        logging.disable(previous_logging)
    emit({"result": "FAIL", "reason": reason})
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
