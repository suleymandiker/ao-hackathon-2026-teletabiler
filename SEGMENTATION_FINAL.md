# Segmentation — Simple Final Architecture

Goal: split very large raw log streams into correct logical events without loading the full file into RAM and without sending the full log to an LLM.

```text
LOG STREAM
    -> Small bounded sample
    -> Structural format signature
    -> Policy Registry (RAM + SQLite)
         FOUND -> reuse verified policy, AI = 0
         NEW   -> AI discovery -> deterministic validation -> store verified policy
    -> Streaming Header Classifier
    -> Multiline Assembly
    -> Logical Events
    -> Parser
    -> Canonical Events
```

## Persistence

There is one segmentation source of truth: `data/.segmentation_policy_registry.sqlite3`.
The registry keeps only compact verified policies. It does not store raw logs, prompts, or raw AI responses.
A small in-process RAM dictionary avoids repeated SQLite lookups.

Removed from the design:
- legacy `segmentation_cache.json` / `SegmentationCache`
- global `.aiops_ai_cache.sqlite3`
- segmentation refinement/critic stages
- segmentation drift/revalidation state machine

## Runtime behavior

Known format: sample -> signature -> registry hit -> stream segmentation. No AI call and no full-file validation pass.

Unknown format: sample -> signature -> registry miss -> one AI discovery call -> deterministic full-stream validation -> store only if verified -> stream segmentation. If the AI candidate fails, the deterministic fallback is validated and may be stored instead.

The data plane remains deterministic and streaming. Regex is candidate evidence; `HeaderClassifier` protects multiline continuations such as Python tracebacks, Java stack traces, indented blocks, and attached JSON.
