# -*- coding: utf-8 -*-
from datetime import datetime, timezone
import xxhash
from parser_layer.timestamp.timestamp_evidence import unresolved_timestamp_evidence


class CanonicalEventBuilder:
    """Build the stable CanonicalEvent v2 contract.

    The builder only promotes fields that have stable cross-format semantics.
    Format-specific/event-specific values remain under ``attributes``.  Context
    that is not present in the log is intentionally left for later enrichment.
    """

    SCHEMA_VERSION = "2.0"

    def __init__(self):
        self._counter = 0

    def build(self, fields, raw, source=None):
        fields = dict(fields or {})
        attributes = dict(fields.get("attributes") or {})

        timestamp = fields.get("timestamp")
        if timestamp is None:
            evidence = unresolved_timestamp_evidence(raw, fields.get("raw_timestamp"))
            # Preserve source attributes with the same names without allowing
            # them to masquerade as parser-generated diagnostic evidence.
            for key, value in evidence.items():
                if key in attributes and attributes[key] != value:
                    backup = "source_" + key
                    while backup in attributes:
                        backup = "source_" + backup
                    attributes[backup] = attributes[key]
                attributes[key] = value
        observed_timestamp = datetime.now(timezone.utc)
        severity = self._normalize_severity(fields.get("severity"))
        message = fields.get("message") or raw

        service = self._take_first(
            fields,
            attributes,
            "service",
            "service_name",
            "app",
            "application",
        )
        host = self._take_first(fields, attributes, "host", "hostname")
        component = self._take_first(fields, attributes, "component", "logger")

        trace_id = self._take_first(fields, attributes, "trace_id", "traceId")
        span_id = self._take_first(fields, attributes, "span_id", "spanId")

        # These values are useful but are event-specific rather than canonical
        # resource identity, so they live in attributes in v2.
        self._move_to_attributes(fields, attributes, "client_ip")
        self._move_to_attributes(fields, attributes, "http_status")
        self._move_to_attributes(fields, attributes, "network_error_code")
        self._move_to_attributes(fields, attributes, "function")
        self._move_to_attributes(fields, attributes, "line")

        event_id = self._generate_unique_event_id(raw, timestamp, source)

        return {
            "schema_version": self.SCHEMA_VERSION,
            "event_id": event_id,
            "timestamp": timestamp,
            "observed_timestamp": observed_timestamp,
            "severity": severity,
            "message": str(message),
            "resource": {
                "service": service,
                "host": host,
                "component": component,
            },
            "trace": {
                "trace_id": trace_id,
                "span_id": span_id,
            },
            "attributes": attributes,
            "raw": raw,
        }

    @staticmethod
    def _normalize_severity(value):
        if value is None or not str(value).strip():
            return None
        severity = str(value).upper().strip('"\' ')
        return "WARNING" if severity == "WARN" else severity

    @staticmethod
    def _take_first(fields, attributes, *keys):
        for key in keys:
            value = fields.get(key)
            if value is not None and value != "":
                return value
        for key in keys:
            value = attributes.pop(key, None)
            if value is not None and value != "":
                return value
        return None

    @staticmethod
    def _move_to_attributes(fields, attributes, key):
        value = fields.get(key)
        if value is not None and key not in attributes:
            attributes[key] = value

    def _generate_unique_event_id(self, raw, event_time, source):
        self._counter += 1
        seed = f"{self._counter}|{event_time}|{source}|{raw}"
        return xxhash.xxh64(seed.encode("utf-8")).hexdigest()[:13]
