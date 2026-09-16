# -*- coding: utf-8 -*-
import logging
import time
from parser_layer.format_detector.format_detector import FormatDetector
from parser_layer.parsers.json_parser import JsonParser
from parser_layer.parsers.syslog_parser import SyslogParser
from parser_layer.parsers.kv_parser import KVParser
from parser_layer.parsers.structured_text_parser import StructuredTextParser
from parser_layer.parsers.positional_structured_parser import PositionalStructuredParser
from parser_layer.parsers.plain_text_parser import PlainTextParser
from parser_layer.parsers.policy_parser import PolicyParser
from parser_layer.canonical_event_builder import CanonicalEventBuilder
from parser_layer.policy.policy_registry import ParserPolicyRegistry
from parser_layer.policy.policy_discovery import ParserPolicyDiscovery

logger = logging.getLogger(__name__)


class ParserPipeline:
    """Deterministic hot-path parser with bounded unknown-format discovery.

    prepare() is control-plane and should be called once with a small representative
    sample (typically <=150 logical events). process() never calculates signatures,
    queries SQLite or calls AI.
    """

    def __init__(self, registry_path=None, ai_enabled=True):
        self.detector = FormatDetector()
        self.parsers = {
            "json": JsonParser(),
            "syslog": SyslogParser(),
            "kv": KVParser(),
            "structured_text": StructuredTextParser(),
            "positional_structured": PositionalStructuredParser(),
            "plain_text": PlainTextParser(),
        }
        self.builder = CanonicalEventBuilder()
        self.registry = ParserPolicyRegistry(registry_path)
        self.discovery = ParserPolicyDiscovery()
        self.ai_enabled = ai_enabled
        self.custom_parser = None
        self.policy_signature = None
        self.policy_source = None
        self.registry_source = None

    def prepare(self, logical_event_samples):
        """Resolve one custom policy from a bounded sample; no per-event registry work."""
        # A ParserPipeline instance is reused across files by the QA/runtime. Never let
        # one file's custom parser leak into the next file when prepare() finds no policy.
        self.custom_parser = None
        self.policy_signature = None
        self.policy_source = None
        self.registry_source = None

        samples = [x for x in logical_event_samples[:150] if x and x.strip()]
        unknown = [x for x in samples if self.detector.detect(x) == "plain_text"]
        if not unknown:
            return {"status": "built-in", "ai_calls": 0}

        signature = self.discovery.signature(unknown)
        self.policy_signature = signature
        required_version = self.discovery.POLICY_SCHEMA_VERSION
        policy, registry_source = self.registry.get(
            signature,
            required_version=required_version,
        )

        if policy:
            # Revalidate against the current bounded sample. This is control-plane
            # work only and prevents a structurally valid but wrong message boundary
            # from remaining authoritative forever.
            accepted, success = self.discovery.validate(policy, unknown)
            if accepted:
                self.custom_parser = PolicyParser(policy)
                self.policy_source = policy.get("source", "registry")
                self.registry_source = registry_source
                return {
                    "status": "policy-found",
                    "signature": signature,
                    "registry": registry_source,
                    "validation_success": success,
                    "policy_version": required_version,
                    "ai_calls": 0,
                }

            self.registry.invalidate(signature)
            registry_source = "invalidated"

        if not self.ai_enabled:
            return {
                "status": "safe-fallback",
                "signature": signature,
                "registry": registry_source,
                "policy_version": required_version,
                "ai_calls": 0,
            }

        policy = self.discovery.discover(unknown)
        ai_calls = 1 if self.discovery.enabled() else 0
        accepted, success = self.discovery.validate(policy, unknown)
        if not accepted:
            return {
                "status": "safe-fallback",
                "signature": signature,
                "registry": registry_source,
                "validation_success": success,
                "policy_version": required_version,
                "ai_calls": ai_calls,
            }

        policy = dict(policy)
        policy["source"] = "ai-discovery"
        policy["_policy_schema_version"] = required_version
        self.registry.save(signature, policy, "ai-discovery", success)
        self.custom_parser = PolicyParser(policy)
        self.policy_source = "ai-discovery"
        self.registry_source = "miss"
        return {
            "status": "policy-stored",
            "signature": signature,
            "validation_success": success,
            "policy_version": required_version,
            "ai_calls": ai_calls,
        }

    def process(self, event):
        if not event or not event.strip():
            return None

        t0 = time.perf_counter()
        format_type = "safe_fallback"
        parser_fallback = False
        try:
            format_type = self.detector.detect(event)
            parser = self.parsers.get(format_type, self.parsers["plain_text"])

            # A learned policy is consulted only for unknown/plain-text events and is
            # already resident in RAM. No hash/SQLite/AI operation occurs here.
            if format_type == "plain_text" and self.custom_parser is not None:
                fields = self.custom_parser.parse(event)
                if not fields:
                    fields = self.parsers["plain_text"].parse(event)
                    parser_fallback = True
            else:
                fields = parser.parse(event)
                if not fields and format_type != "plain_text":
                    fields = self.parsers["plain_text"].parse(event)
                    parser_fallback = True

            # Zero-loss safe fallback: a meaningful logical event must remain
            # representable without inventing timestamp or severity.
            if not fields:
                fields = {
                    "timestamp": None,
                    "severity": None,
                    "message": event,
                    "attributes": {},
                }
                parser_fallback = True

            result = self.builder.build(fields, event, format_type)

            total_ms = (time.perf_counter() - t0) * 1000
            if total_ms > 20:
                logger.warning(
                    "[PARSER] slow event %.1fms format=%s fallback=%s",
                    total_ms,
                    format_type,
                    parser_fallback,
                )
            return result
        except Exception as exc:
            logger.exception(
                "[PARSER] deterministic parse failed; preserving raw event: %s", exc
            )
            return self.builder.build(
                {
                    "timestamp": None,
                    "severity": None,
                    "message": event,
                    "attributes": {},
                },
                event,
                "safe_fallback",
            )
