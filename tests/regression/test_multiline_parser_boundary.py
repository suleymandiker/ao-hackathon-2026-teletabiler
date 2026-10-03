"""Regressions at the physical-line, logical-event, and parser boundary."""

from pathlib import Path
import socket
import sqlite3

import pytest


HEADER = "2026-01-01T00:00:00Z operation started"
CONTINUATION = "    ERROR detail from continuation"
LOGICAL_EVENT = HEADER + "\n" + CONTINUATION
HEADER_REGEX = r"^\d{4}-\d{2}-\d{2}T"


@pytest.fixture(autouse=True)
def isolated_backend(monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[2] / "src" / "backend"))

    def forbidden(*args, **kwargs):
        pytest.fail("Network and SQLite access are forbidden in this regression")

    monkeypatch.setattr(sqlite3, "connect", forbidden)
    monkeypatch.setattr(sqlite3.dbapi2, "connect", forbidden)
    monkeypatch.setattr(socket.socket, "connect", forbidden)
    monkeypatch.setattr(socket.socket, "connect_ex", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(socket, "getaddrinfo", forbidden)


@pytest.fixture
def assembled_record(tmp_path):
    from segmentation_layer.multiline_assembler import MultilineAssembler

    log_path = tmp_path / "multiline.log"
    log_path.write_text(LOGICAL_EVENT + "\n", encoding="utf-8")
    records = list(MultilineAssembler().iter_event_records(str(log_path), HEADER_REGEX))
    assert len(records) == 1
    return records[0]


def test_assembler_preserves_internal_newline(assembled_record):
    assert assembled_record["event"] == LOGICAL_EVENT


def test_assembler_preserves_continuation_structure(assembled_record):
    assert assembled_record["line_count"] == 2
    assert assembled_record["start_line"] == 1
    assert assembled_record["end_line"] == 2
    continuation = assembled_record["decisions"][1]
    assert continuation["classification"] == "CONTINUATION"
    assert continuation["start_new_event"] is False
    assert len(assembled_record["event"].splitlines()) == 2


def test_assembler_preserves_continuation_indentation(assembled_record):
    # Raw-event fidelity includes the indentation used to classify continuation.
    assert CONTINUATION in assembled_record["event"]


@pytest.mark.parametrize("assembled", [False, True], ids=["physical-newlines", "assembled"])
def test_continuation_error_does_not_become_header_severity(assembled_record, assembled):
    from parser_layer.parsers.structured_text_parser import StructuredTextParser

    event = assembled_record["event"] if assembled else LOGICAL_EVENT
    parsed = StructuredTextParser().parse(event)
    assert parsed is not None
    assert parsed["severity"] is None


@pytest.mark.parametrize("assembled", [False, True], ids=["physical-newlines", "assembled"])
def test_continuation_severity_does_not_discard_header_message(assembled_record, assembled):
    from parser_layer.parsers.structured_text_parser import StructuredTextParser

    event = assembled_record["event"] if assembled else LOGICAL_EVENT
    parsed = StructuredTextParser().parse(event)
    assert parsed is not None
    assert parsed["message"] == "operation started\n" + CONTINUATION


@pytest.mark.parametrize("terminator", ["\n", "\r\n", ""], ids=["lf", "crlf", "no-terminator"])
def test_single_line_event_remains_unchanged(tmp_path, terminator):
    from segmentation_layer.multiline_assembler import MultilineAssembler

    log_path = tmp_path / "single-line.log"
    log_path.write_bytes((HEADER + terminator).encode("utf-8"))
    records = list(MultilineAssembler().iter_event_records(str(log_path), HEADER_REGEX))
    assert len(records) == 1
    assert records[0]["event"] == HEADER
    assert records[0]["line_count"] == 1
    assert records[0]["first_line_is_header"] is True
    assert records[0]["decisions"][0]["classification"] == "STRONG_HEADER"
