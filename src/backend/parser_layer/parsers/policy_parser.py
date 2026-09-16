# -*- coding: utf-8 -*-
import re

from parser_layer.timestamp.timestamp_normalizer import TimestampNormalizer


class PolicyParser:
    """Deterministic runtime parser for a verified learned ParserPolicy."""

    SEVERITY_MAP = {
        "V": "DEBUG",
        "VERBOSE": "DEBUG",
        "D": "DEBUG",
        "DEBUG": "DEBUG",
        "I": "INFO",
        "INFO": "INFO",
        "INFORMATION": "INFO",
        "W": "WARNING",
        "WARN": "WARNING",
        "WARNING": "WARNING",
        "E": "ERROR",
        "ERR": "ERROR",
        "ERROR": "ERROR",
        "C": "CRITICAL",
        "CRIT": "CRITICAL",
        "CRITICAL": "CRITICAL",
        "F": "FATAL",
        "FATAL": "FATAL",
    }

    EXPLICIT_KV_RE = re.compile(
        r'(?<![\w.-])(?P<key>[A-Za-z_][A-Za-z0-9_.-]*)='
        r'(?P<value>"[^"\r\n]*"|\'[^\'\r\n]*\'|[^\s,()\[\]{}]+)'
    )

    INTEGER_ATTRIBUTES = {
        "pid",
        "tid",
        "port",
        "http_status",
        "status_code",
        "partition",
    }

    def __init__(self, policy):
        self.policy = dict(policy)
        self.pattern = re.compile(self.policy["regex"])
        self.ts = TimestampNormalizer()

    def _value(self, match, policy_key):
        group = self.policy.get(policy_key)
        return self._group_value(match, group)

    @staticmethod
    def _group_value(match, group):
        if group is None or group == "":
            return None
        try:
            if isinstance(group, int):
                value = match.group(group)
            elif isinstance(group, str) and group.isdigit():
                value = match.group(int(group))
            else:
                value = match.group(group)
        except (IndexError, KeyError, ValueError):
            return None
        return value.strip() if isinstance(value, str) else value

    @classmethod
    def _normalize_severity(cls, value):
        if value is None:
            return None
        text = str(value).strip().upper()
        return cls.SEVERITY_MAP.get(text, text)

    @classmethod
    def _attribute_value(cls, name, value):
        if value is None:
            return None
        if name in cls.INTEGER_ATTRIBUTES:
            try:
                return int(value)
            except (TypeError, ValueError):
                pass
        return value


    @staticmethod
    def _explicit_scalar_value(value):
        text = value
        if (
            len(text) >= 2
            and text[0] == text[-1]
            and text[0] in ('"', "'")
        ):
            text = text[1:-1]
        if text.isdigit():
            if len(text) > 1 and text.startswith("0"):
                return text
            return int(text)
        try:
            return float(text)
        except ValueError:
            return text

    @classmethod
    def _extract_explicit_attributes(cls, text):
        attributes = {}
        for match in cls.EXPLICIT_KV_RE.finditer(text or ""):
            key = match.group("key")
            if key not in attributes:
                attributes[key] = cls._explicit_scalar_value(match.group("value"))
        return attributes

    def parse(self, log):
        # Learned policies are required to start with ^. match() therefore expresses
        # the contract directly and avoids a general search. partition() obtains the
        # header and multiline continuation in one pass instead of splitlines() twice.
        if not log:
            return None
        first_line, separator, continuation = log.partition("\n")
        first_line = first_line.rstrip("\r")
        match = self.pattern.match(first_line)
        if not match:
            return None

        raw_ts = self._value(match, "timestamp_group")
        message = self._value(match, "message_group")

        # If no message group was learned, preserve the whole logical event.
        # Otherwise append the exact continuation text for multiline events.
        if message:
            if separator:
                message = message + "\n" + continuation
        else:
            message = log

        # Preserve safe explicit scalar key=value observations even when the
        # learned policy is responsible only for the structural header.
        attributes = self._extract_explicit_attributes(message)
        for attr_name, group_ref in (self.policy.get("attribute_groups") or {}).items():
            value = self._group_value(match, group_ref)
            if value is not None and value != "" and attr_name not in attributes:
                # Explicit message key=value evidence is more specific than a
                # learned generic/header attribute with the same name. Preserve
                # the first explicit observation instead of overwriting it.
                attributes[attr_name] = self._attribute_value(attr_name, value)

        # Only an explicitly learned timestamp_group may populate canonical
        # timestamp. Numeric fields stored as attributes (for example a field
        # named "epoch") are preserved as observed source data, but are not
        # promoted implicitly: their time basis/semantics may be format-specific.
        timestamp = self.ts.normalize(raw_ts) if raw_ts else None

        return {
            "timestamp": timestamp,
            "severity": self._normalize_severity(self._value(match, "severity_group")),
            "message": message,
            "host": self._value(match, "host_group"),
            "service": self._value(match, "service_group"),
            "component": self._value(match, "component_group"),
            "trace_id": self._value(match, "trace_id_group"),
            "span_id": self._value(match, "span_id_group"),
            "attributes": attributes,
        }
