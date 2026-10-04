"""Additive parser diagnostics; the canonical event contract is unchanged."""

from dataclasses import dataclass
from enum import Enum
from typing import Any


class Recognition(str, Enum):
    BUILTIN_RECOGNIZED = "BUILTIN_RECOGNIZED"
    PLAIN_TEXT = "PLAIN_TEXT"
    UNSUPPORTED_STRUCTURE = "UNSUPPORTED_STRUCTURE"
    MALFORMED_INPUT = "MALFORMED_INPUT"
    PARSER_ERROR = "PARSER_ERROR"
    EMPTY_INPUT = "EMPTY_INPUT"


class Delivery(str, Enum):
    PARSED = "PARSED"
    FALLBACK = "FALLBACK"
    FAILED = "FAILED"
    IGNORED = "IGNORED"


@dataclass(frozen=True)
class ParseOutcome:
    """Frozen diagnostics around the original, still-mutable event dictionary.

    Recognition may be None when the existing parser proves no listed category.
    PLAIN_TEXT identifies the current plain-text route, not proof that the input
    lacks structure. UNSUPPORTED_STRUCTURE and MALFORMED_INPUT are reserved for
    future evidence; Phase 1 does not infer them from rejection or fallback.
    FAILED is reserved too: errors that previously escaped still raise.
    parser_id identifies the delivering parser (or safe_fallback), not a policy.
    """

    event: dict[str, Any] | None
    recognition: Recognition | None
    delivery: Delivery
    parser_id: str | None
    reason_code: str | None
