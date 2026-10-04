"""Golden file compatibility and source-neutral single-stream transitions."""

import builtins
from dataclasses import FrozenInstanceError
import importlib
from pathlib import Path
import socket
import sqlite3

import pytest


HEADER = "2026-01-01T00:00:00Z operation started  "
NEXT = "2026-01-01T00:00:01Z operation completed"
REGEX = r"^\d{4}-\d{2}-\d{2}T"


@pytest.fixture(autouse=True)
def isolated_backend(monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[2] / "src" / "backend"))

    def forbidden(*args, **kwargs):
        pytest.fail("Boundary assembly must not perform external or persistent I/O")

    for owner, names in (
        (sqlite3, ("connect",)), (sqlite3.dbapi2, ("connect",)),
        (socket.socket, ("connect", "connect_ex")),
        (socket, ("create_connection", "getaddrinfo")),
    ):
        for name in names:
            monkeypatch.setattr(owner, name, forbidden)
    original_import = builtins.__import__
    blocked = {
        "ai_engine", "parser_layer", "template_layer", "downstream_pipeline",
        "full_pipeline_v2", "requests", "httpx", "opensearchpy", "streamlit",
        "segmentation_layer.segmentation_pipeline",
        "segmentation_layer.header_discovery", "segmentation_layer.regex_validator",
        "segmentation_layer.policy_registry",
    }

    def guarded_import(name, *args, **kwargs):
        if name in blocked or name.split(".")[0] in blocked:
            pytest.fail(f"Boundary assembly must not import {name}")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded_import)


def decision(line_no, classification, start, regex_match, reason, confidence):
    return dict(line_no=line_no, classification=classification, start_new_event=start,
                regex_match=regex_match, reason=reason, confidence=confidence)


@pytest.mark.parametrize("terminator", ["\n", "\r\n", "\r"])
@pytest.mark.parametrize("final_terminator", [False, True])
def test_file_golden_text_positions_and_decisions(tmp_path, terminator, final_terminator):
    from segmentation_layer.multiline_assembler import MultilineAssembler

    lines = ["", "orphan prefix  ", "\t", HEADER, "\t  detail  ", "  ",
             "Traceback (most recent call last):", NEXT, ""]
    payload = terminator.join(lines) + (terminator if final_terminator else "")
    path = tmp_path / "golden.log"
    path.write_bytes(payload.encode("utf-8"))
    expected = [
        dict(event=lines[1], line_count=1, start_line=2, end_line=2,
             first_line_is_header=False, source="continuation",
             decisions=[decision(2, "CONTINUATION", False, False,
                                 "continuation_default", 0.55)]),
        dict(event="\n".join([lines[3], lines[4], lines[6]]), line_count=3,
             start_line=4, end_line=7, first_line_is_header=True, source="strong_header",
             decisions=[
                 decision(4, "STRONG_HEADER", True, True, "deterministic_strong_header", 1.0),
                 decision(5, "CONTINUATION", False, False, "known_multiline_continuation", 0.99),
                 decision(7, "CONTINUATION", False, False, "known_multiline_continuation", 0.99),
             ]),
        dict(event=NEXT, line_count=1, start_line=8, end_line=8,
             first_line_is_header=True, source="strong_header",
             decisions=[decision(8, "STRONG_HEADER", True, True,
                                 "deterministic_strong_header", 1.0)]),
    ]
    assembler = MultilineAssembler()
    assert list(assembler.iter_event_records(str(path), REGEX)) == expected
    assert list(assembler.iter_events(str(path), REGEX)) == [item["event"] for item in expected]


@pytest.mark.parametrize("suffix", ["", "\n", "\r\n"])
def test_file_final_content_and_invalid_utf8_compatibility(tmp_path, suffix):
    from segmentation_layer.multiline_assembler import MultilineAssembler

    path = tmp_path / "final.log"
    path.write_bytes(HEADER.encode() + b"\xff" + suffix.encode())
    assert list(MultilineAssembler().iter_events(str(path), REGEX)) == [HEADER]


def test_file_classifier_injection_and_separator_remain_compatible(tmp_path):
    from segmentation_layer.header_classifier import HeaderClassifier
    from segmentation_layer.multiline_assembler import MultilineAssembler

    class RecordingClassifier:
        def __init__(self):
            self.calls = []

        def classify(self, line, regex, *, has_current_event, after_blank=False):
            self.calls.append((line, has_current_event, after_blank))
            return HeaderClassifier().classify(
                line, regex, has_current_event=has_current_event, after_blank=after_blank,
            )

    path = tmp_path / "injection.log"
    path.write_text(HEADER + "\n  continuation\n\n" + NEXT, encoding="utf-8")
    classifier = RecordingClassifier()
    assembler = MultilineAssembler(separator="\n", classifier=classifier)
    assert list(assembler.iter_events(str(path), REGEX)) == [HEADER + "\n  continuation", NEXT]
    assert classifier.calls == [
        (HEADER, False, True), ("  continuation", True, False),
        (NEXT, True, True), (NEXT, False, True),
    ]


def new_state(regex=REGEX):
    module = importlib.import_module("segmentation_layer.multiline_assembler")
    return module.MultilineAssemblyState(regex)


def test_incremental_flush_is_explicit_idempotent_and_reusable():
    state = new_state()
    assert state.feed(HEADER).completed is None
    assert state.feed("\tcontinuation  ").completed is None
    event = state.flush()
    assert event.text == HEADER + "\n\tcontinuation  "
    assert [item.ordinal for item in event.evidence] == [1, 2]
    assert state.has_pending is False
    assert state.flush() is None
    assert state.feed(NEXT).completed is None
    assert state.flush().text == NEXT
    with pytest.raises(FrozenInstanceError):
        event.text = "changed"


@pytest.mark.parametrize("continuation", [
    "    nested header", "\tERROR: indented", "Traceback (most recent call last):",
    'File "worker.py", line 4', "ValueError: failed", "Caused by: failure",
    "Suppressed: failure", "... 3 more", "at service.call(x)",
    "detail: error", "recommendation: inspect service",
])
def test_known_continuations_veto_even_broad_regex(continuation):
    state = new_state(r"^.*")
    state.feed(HEADER)
    step = state.feed(continuation)
    assert step.completed is None
    assert step.evidence.decision.regex_match is True
    assert step.evidence.decision.reason == "known_multiline_continuation"
    assert state.flush().text == HEADER + "\n" + continuation


def test_orphan_prefix_and_chained_exception_blank_context():
    state = new_state()
    state.feed("orphan prefix")
    orphan = state.feed(HEADER).completed
    assert orphan.text == "orphan prefix"
    assert orphan.evidence[0].decision.start_new_event is False
    parts = ["Traceback (most recent call last):", "  stack frame", "ValueError: first",
             "", "During handling of the above exception, another exception occurred:",
             " \t", "Traceback (most recent call last):", "ServiceError: second"]
    for part in parts:
        assert state.feed(part).completed is None
    event = state.feed(NEXT).completed
    assert event.text == "\n".join([HEADER] + [part for part in parts if part and not part.isspace()])
    assert [item.ordinal for item in event.evidence if not item.included] == [6, 8]
    assert state.flush().text == NEXT


def test_json_context_and_regex_precedence():
    state = new_state(r"^EVENT ")
    state.feed('{"first": 1}')
    assert state.feed('{"nested": 2}').completed is None
    state.feed(" \t")
    event = state.feed('{"next": 3}').completed
    assert event.text == '{"first": 1}\n{"nested": 2}'
    assert event.evidence[-1].included is False
    state = new_state(r'^\{"')
    state.feed('{"first": 1}')
    assert state.feed('{"second": 2}').completed.text == '{"first": 1}'


def test_opaque_input_never_uses_text_normalization():
    class OpaqueText(str):
        def strip(self, *args):
            pytest.fail("Opaque input must not be stripped")

        rstrip = strip
        lstrip = strip
        splitlines = strip
        encode = strip

    state = new_state()
    raw = OpaqueText(HEADER + "\r\n  embedded continuation\r\n")
    state.feed(raw)
    state.feed("\tsecond unit  \r\n")
    assert state.flush().text == raw + "\n\tsecond unit  \r\n"


def test_raw_regex_evidence_sees_crlf_while_legacy_classifier_keeps_its_api():
    import re
    from segmentation_layer.header_classifier import HeaderClassifier

    classifier = HeaderClassifier()
    regex = re.compile(r"^CUSTOM$")
    assert classifier.classify("CUSTOM\r\n", regex, has_current_event=True).start_new_event is True
    raw = classifier.classify_raw("CUSTOM\r\n", regex, has_current_event=True)
    assert raw.regex_match is False
    state = new_state(r"^CUSTOM$")
    state.feed(HEADER)
    assert state.feed("CUSTOM\r\n").completed is None
    assert state.flush().text == HEADER + "\nCUSTOM\r\n"


@pytest.mark.parametrize("opening", ["{", "{  \t", "{\u2003"])
def test_json_opening_whitespace_has_same_context_without_trimming(opening):
    state = new_state(r"^EVENT ")
    first = state.feed(opening)
    assert first.evidence.decision.reason == "json_context"
    assert first.evidence.decision.start_new_event is True
    assert state.feed(opening).completed is None
    assert state.flush().text == opening + "\n" + opening


@pytest.mark.parametrize("text", ["", " ", "\t\r\n", "\u2003"])
def test_blank_without_active_event_has_evidence_but_no_event(text):
    state = new_state()
    step = state.feed(text)
    assert step.evidence.included is False
    assert step.evidence.decision.reason == "blank"
    assert step.completed is None
    assert state.has_pending is False
    assert state.flush() is None


def test_file_generators_and_factory_states_are_independent(tmp_path):
    from segmentation_layer.multiline_assembler import MultilineAssembler

    path = tmp_path / "independent.log"
    path.write_text(HEADER + "\n" + NEXT, encoding="utf-8")
    assembler = MultilineAssembler()
    first = assembler.iter_events(str(path), REGEX)
    second = assembler.iter_events(str(path), REGEX)
    assert next(first) == next(second) == HEADER
    assert list(first) == list(second) == [NEXT]
    first_state = assembler.new_state(REGEX)
    second_state = assembler.new_state(REGEX)
    first_state.feed(HEADER)
    second_state.feed(NEXT)
    assert first_state.flush().text == HEADER
    assert second_state.flush().text == NEXT
