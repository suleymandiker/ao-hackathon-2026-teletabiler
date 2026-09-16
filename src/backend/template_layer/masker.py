# -*- coding: utf-8 -*-
import re


class MessageMasker:
    """Deterministically remove common high-cardinality values from messages.

    The masker is deliberately conservative: semantic literals are preserved,
    while values that have the *shape* of runtime identities are normalized.
    """

    # Only identity fields belong here. Business state such as status, route,
    # action, error and reason must remain literal because it carries meaning.
    _IDENTITY_KEYS = (
        "request_id",
        "upstream_request_id",
        "trace_id",
        "span_id",
        "correlation_id",
        "transaction_id",
        "event_id",
        "session_id",
        "order_id",
        "checkout_id",
        "user_id",
        "pod",
        "pod_name",
        "pod_uid",
        "container_id",
        "process_id",
        "task_id",
        "execution_id",
    )
    _KEYS = "|".join(_IDENTITY_KEYS)
    _JSON_ID = re.compile(
        rf'(?i)(["\'](?:{_KEYS})["\']\s*:\s*)["\'][^"\']+["\']'
    )
    _KV_ID = re.compile(
        rf"(?i)\b({_KEYS})(\s*=\s*)(?:\"[^\"]*\"|'[^']*'|[^\s,|}}\]]+)"
    )
    _COLON_ID = re.compile(
        rf"(?i)\b({_KEYS})(\s*:\s*)(?:\"[^\"]*\"|'[^']*'|[^\s,|}}\]]+)"
    )

    # Generic runtime-ID shapes. These are intentionally structural rather
    # than product/vendor specific. Examples covered:
    #   core.20015                       -> core.<ID>
    #   blk_7864850143895703065          -> blk_<ID>
    #   rdd_26_1                         -> rdd_<ID>
    #   30546174_101902835               -> <ID>
    #   attempt_1445_0020_m_000009_0     -> attempt_<ID>
    #
    # We do NOT mask arbitrary words, enum/state values, class names, routes,
    # or short single-number suffixes such as TLSv1 / phase-2.
    _PREFIXED_COMPOUND_ID = re.compile(
        r"(?<![\w])([A-Za-z][A-Za-z0-9-]*[_-])"
        r"(?=[A-Za-z0-9_-]*\d)"
        r"(?=[A-Za-z0-9_-]*[_-])"
        r"[A-Za-z0-9]+(?:[_-][A-Za-z0-9]+)+\b"
    )
    _PREFIXED_LONG_NUM_ID = re.compile(
        r"(?<![\w])([A-Za-z][A-Za-z0-9-]*[._-])-?\d{4,}\b"
    )
    _NUMERIC_COMPOUND_ID = re.compile(
        r"(?<![\w.])-?\d{2,}(?:[_-]\d{2,})+(?![\w.])"
    )

    _PATTERNS = (
        # UUID shape is intentionally not restricted to RFC version/variant
        # bits. Operational logs often contain UUID-shaped opaque IDs that do
        # not satisfy those semantic constraints.
        (re.compile(r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b"), "<UUID>"),
        (re.compile(r"(?<![\w:])(?:[0-9a-fA-F]{1,4}:){2,7}[0-9a-fA-F]{1,4}(?![\w:])"), "<IP>"),
        (re.compile(r"(?<![\w.])(?:25[0-5]|2[0-4]\d|1?\d?\d)(?:\.(?:25[0-5]|2[0-4]\d|1?\d?\d)){3}(?![\w.])"), "<IP>"),
        (re.compile(r"\bhttps?://[^\s\]\[(){}<>\"']+", re.IGNORECASE), "<URL>"),
        (re.compile(r"\b0x[0-9a-fA-F]+\b"), "<HEX>"),
        (re.compile(r"\b[0-9a-fA-F]{16,}\b"), "<HEX>"),
    )

    _NUMBER = re.compile(r"(?<![\w.])-?\d+(?:\.\d+)?(?![\w.])")

    def mask(self, message):
        value = " ".join(str(message or "").split())

        # Explicit identity fields have highest priority.
        value = self._JSON_ID.sub(r'\1"<ID>"', value)
        value = self._KV_ID.sub(r"\1\2<ID>", value)
        value = self._COLON_ID.sub(r"\1\2<ID>", value)

        # Strong primitive types must be recognized before generic compound
        # identities so IPs/UUIDs/URLs retain their more useful placeholders.
        for pattern, replacement in self._PATTERNS:
            value = pattern.sub(replacement, value)

        # Normalize embedded/compound runtime identities that ordinary numeric
        # masking cannot see because digits are attached to letters or '_'.
        value = self._PREFIXED_COMPOUND_ID.sub(r"\1<ID>", value)
        value = self._PREFIXED_LONG_NUM_ID.sub(r"\1<ID>", value)
        value = self._NUMERIC_COMPOUND_ID.sub("<ID>", value)

        # Finally normalize standalone numeric values.
        value = self._NUMBER.sub("<NUM>", value)
        return value
