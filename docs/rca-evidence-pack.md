# RCA evidence pack (Phase 7)

“More evidence is not automatically better evidence.” The expert receives bounded,
non-redundant deterministic evidence, not the complete incident graph. Selection
changes only the prompt representation. Deterministic pipeline decisions remain
authoritative, including when the expert fails.

## Previous flow and changed files

Previously `ExpertRCAEngine` computed deterministic RCA, took the first five
incidents, collected their signal IDs in a set, sent up to 20 compact signals from
that unordered set and the first 30 local edges, and included **all** deterministic
RCA results. Incident membership, root hints and edge endpoints repeated long IDs.
Templates were cut at 220 characters, but total input and output fields were not
bounded. Any parsed object with one expected field could be accepted. Truncated
JSON was not rejected by finish reason; rejected response text was logged.

Changed production files: `src/backend/rca_layer/rca_engine.py`,
`src/backend/ai_engine.py`, `src/backend/prompts/rca_expert.md`. New modules:
`src/backend/rca_layer/evidence.py` and `expert_output.py`. Regression coverage is in
`tests/regression/test_rca_evidence.py` and `test_rca_expert.py`. This document is
the only new documentation file. The working tree was clean at task start.

## Selection and traceability

The flow is deterministic RCA → `RCAEvidenceSelector` → `RCAEvidencePack` → gateway
→ `RCAExpertOutputValidator` → existing Turkish expert fields. One expert
interpretation per case remains, with at most one HTTP 400 format compatibility retry.

- Incident context selection is stable by severity, start time and ID. Only
  qualified signals belonging to those incidents are eligible.
- Grouping uses exact template, service, component, namespace, cluster, scope and
  alarm-type identity, excluding the time bucket. Distinct IDs are not rewritten.
  Equivalent windows contribute occurrence sums, window counts, minimum severity
  and maximum qualification score. First/last time is included only when every
  contributing signal has resolved timestamps.
- Existing deterministic candidate order reserves the first meaningful distinct
  root groups. Duplicate candidates in one group do not consume multiple hint
  slots. Remaining slots prefer probable-root membership and new components and
  patterns, then severity, existing graph relevance, score, time and stable identity
  tie-breaks. Public deterministic RCA is never re-ranked.
- `S1…` aliases identify group representatives; the internal immutable pack retains
  both exact representative IDs and every member ID. `I1…` maps to incident IDs.
  Mappings are not sent, logged or persisted. Accepted incident references map back
  into `etkilenen_olaylar`; `kanit_sinyal_idleri` contains representative real signal
  IDs. Representative event IDs are never included.
- Edges must have selected endpoints sharing an incident. Self-edges within one
  aggregate disappear. One strongest deterministic edge represents each directed
  group pair; bounded selection prioritizes root relationships, declared dependency
  paths, new evidence types and component pairs, then score/time/stable identity.
- Incident summaries omit membership lists. Context is allowlisted and non-empty;
  dependency paths come only from existing context/edges. Unknown placeholders,
  raw events, arbitrary metadata and raw provenance are excluded. Pattern text
  comes only from existing templates; multiline traceback frames are replaced by
  an available normalized exception line or the first bounded template line.

Selection scans existing aggregated signals/edges, then performs bounded candidate
selection. It never visits source events or computes new all-pairs correlations.

## Configuration, size and compaction

| Environment variable | Default |
| --- | ---: |
| `RCA_EVIDENCE_MAX_SIGNALS` | 8 groups |
| `RCA_EVIDENCE_MAX_CORRELATIONS` | 10 edges |
| `RCA_EVIDENCE_MAX_ROOT_CANDIDATES` | 3 distinct groups |
| `RCA_EVIDENCE_MAX_PATTERN_CHARS` | 240 |
| `RCA_EVIDENCE_MAX_INPUT_CHARS` | 12,000 |
| `RCA_EVIDENCE_MAX_INPUT_BYTES` | 24,000 |

Both input limits include the loaded system prompt, compact response schema and
serialized evidence JSON. They measure content, not HTTP framing or a tokenizer.
Serialization uses sorted keys, compact separators and Unicode JSON. The pack is
always complete JSON. `approximate_tokens = ceil(input_chars / 4)` is explicitly a
rough character heuristic. No tokenizer dependency was added.

Over budget, reduction proceeds through optional context, lower-priority edges
(keeping the strongest), non-root signals outside the protected edge, and shorter
pattern detail. Only then are weaker root hints/supporting groups removed. Finally
the remaining edge/non-root support can be removed. The strongest root and required
incident summaries survive; if these still cannot fit, the expert is skipped.
Diagnostics state whether grouping, selection, truncation or budget reduction
compacted the evidence.

A synthetic 18-signal/153-edge case selects 6 groups, 10 edges and 3 root hints:
3,689 evidence characters, 6,933 total input characters / 7,021 UTF-8 bytes including
prompt/schema. These are measured serialized sizes, **not gateway token counts**.
Typical operational targets remain approximately 2,000–3,500 prompt tokens and
500–900 output tokens; only a real gateway response can measure those.

## Exact expert output contract

All fields below are required; additional fields are rejected. Output is one JSON
object, at most 6,000 characters. JSON fences, malformed JSON, duplicate object keys,
non-finite numbers, empty required text and input-echo structures are rejected.

| Field | Type and bound |
| --- | --- |
| `durum_ozeti` | Nonblank string, ≤400 characters |
| `kok_neden_hipotezi` | Nonblank string, ≤400 characters |
| `guven` | Finite number in [0,1]; booleans rejected; expert judgment, not calibrated probability |
| `nedensellik_durumu` | `destekleniyor`, `belirsiz`, or `yetersiz` |
| `etkilenen_olaylar` | 1–5 distinct existing I aliases, ≤16 characters each |
| `kanit_referanslari` | 1–5 distinct selected S aliases, ≤16 characters each; must belong to a referenced incident |
| `karar_gerekcesi` | 0–3 nonblank strings, ≤160 characters each |
| `alternatif_hipotezler` | 0–2 nonblank strings, ≤160 characters each |
| `onerilen_incelemeler` | 0–4 nonblank strings, ≤160 characters each; advisory checks only |
| `eksik_kanitlar` | 0–4 nonblank strings, ≤160 characters each |

`response_format()` defines strict `json_schema` for RCA only. An HTTP 400 that
explicitly identifies unsupported `response_format`/schema can retry once without
`response_format`. Unrelated, opaque and invalid-schema errors do not trigger that
retry. Local validation remains mandatory in every mode. Only
`finish_reason=stop` is accepted. `length`, `max_tokens`, missing/unknown reasons or
any validation failure preserve deterministic RCA without partially merging output.
The existing `qwen_destekli` availability annotation remains on accepted results;
candidate IDs, order, scores, evidence and confirmed-root fields do not change.

## Security, telemetry and cache behavior

`rca_expert.md` now includes the unchanged `common_system.md` untrusted-data
contract. Templates remain in the user message, never interpolated into system
instructions. The prompt prohibits input echoes, raw logs, graph lists and automatic
actions. Secret-like assignments, bearer tokens and JWT-like strings are redacted
from bounded evidence text.

`[RCA CONTEXT]` reports counts, serialized sizes and explicitly approximate tokens.
`[RCA AI]` reports allowlisted gateway token usage, finish reason and duration.
Missing gateway counts remain unknown. Accepted expert results carry a small
`expert_diagnostics` object; rejected-call diagnostics are available on the engine
and console. Neither includes prompts or alias maps. RCA gateway errors omit
response bodies, endpoints, exception details and credentials.

Inspection found **no response cache or conversation-body logger in this checkout's
`ai_engine.py`**. None was introduced. Repeated calls therefore reach the gateway;
truncated/invalid/API-error results are never cached or reused as accepted RCA.
Equivalent evidence serializes identically; changed selected content changes model
input. Other agents retain their existing response-format behavior.

No `.env` file or `QWEN_RCA_MAX_TOKENS` setting was changed. The pre-existing
`max(256, min(1600, configured_value))` call cap remains: configured 2200 still means
an effective 1600 in this checkout. This remaining configuration drift is unchanged.
Thinking remains controlled by the existing false flag. The configured RCA
deployment does not support `include_reasoning`, so that field is omitted entirely,
regardless of its environment flag (see the compatibility fix below).

## Phase 7 validation baseline and Citrix smoke

| Windows runtime | Focused RCA tests | Full regression suite |
| --- | --- | --- |
| Python 3.14.8 / Streamlit 1.65.0 | 68 passed (1.34 s) | 646 passed (28.64 s) |
| Python 3.10.0 / Streamlit 1.63.0 | 68 passed (1.52 s) | 646 passed (59.41 s) |

Focused command: `python -B -m pytest -q -p no:cacheprovider tests/regression/test_rca_expert.py tests/regression/test_rca_evidence.py`.
Full command: `python -B -m pytest -q -p no:cacheprovider tests/regression`.
The full result adds 68 tests to the 578-test baseline. Initial tests
reproduced acceptance of truncated/partial output and rejected-response leakage,
then passed after the changes. The deterministic RCA class is AST-identical to HEAD.
Fixtures compare expert-disabled/enabled events, templates, signal IDs, qualified
sets, correlation pairs, incidents and candidate order/scores, with fixed identity
assertions. Tests block network, LLM and production SQLite/learning state.

`git diff --check` passed. Additional whitespace and Python syntax checks passed
for all eight changed/new files. The complete diff and new modules/tests were
reviewed. Nothing was staged, committed or pushed.

Tracked `git diff --stat` (untracked additions are excluded by Git):

```text
 src/backend/ai_engine.py           |  43 ++++++---
 src/backend/prompts/rca_expert.md   |  40 ++++----
 src/backend/rca_layer/rca_engine.py | 179 +++++++++---------------------------
 3 files changed, 92 insertions(+), 170 deletions(-)
```

New source/test additions: `evidence.py` 316 lines, `expert_output.py` 85 lines,
`test_rca_evidence.py` 208 lines and `test_rca_expert.py` 322 lines (931 total), plus
this document. No core pipeline, frontend, registry or environment file changed.

For the real Citrix smoke, run the same finite analysis through the existing UI or
entry point with current credentials/configuration. Keep output-token and reasoning
settings unchanged. Compare deterministic counts and IDs against the prior run;
compare `[RCA CONTEXT]` counts/sizes and `[RCA AI]` measured tokens/finish reason.
Success means a compact schema-valid expert object, valid mapped references and
`finish_reason=stop`; a rejected expert must leave deterministic RCA available.
Keep only aggregate diagnostics for review. Do not print or persist the prompt,
raw logs, credentials or alias map. No live gateway smoke was performed here.

## Citrix request-capability fix

The user's live compatibility matrix confirms the root cause: the configured
`saka__glm-53-flash-dynamo-saka` gateway rejects the **presence** of
`include_reasoning`, even false, with this message:

> Validation: Unsupported parameter(s): `include_reasoning`

Phase 7 began sending this field through the existing Qwen-family branch. The
first schema request and the no-format retry both retained it and both returned
400. The same deployment accepted the current strict schema, JSON object mode,
and `chat_template_kwargs.enable_thinking=false`; structured calls returned
`finish_reason=stop` and `reasoning_length=0`.

Before this fix, the working tree contained only the interrupted gateway task's
changes in `ai_engine.py`, `test_rca_expert.py` and this document. Its three-attempt
fallback and opaque-400 retries have been replaced by one conditional no-format
retry. No unrelated user changes were found or discarded.

| RCA request field | Previous Phase 7 request | Corrected first request |
| --- | --- | --- |
| Model, endpoint, messages | Existing deployment and bounded evidence | Unchanged |
| `temperature` | 0.0 | 0.0 |
| `max_tokens` | 1600 when configured 2200 | Unchanged: 1600 |
| `chat_template_kwargs.enable_thinking` | false | false |
| `include_reasoning` | false | **Key absent** |
| `response_format` | Current strict `json_schema` | Same complete schema |

`MODELS_CONFIG['Ajan_2_RCA_Expert']['capabilities']` explicitly records
`supports_enable_thinking=True`, `supports_include_reasoning=False`,
`supports_json_object=True`, and `supports_json_schema=True`. These are
application configuration for this deployment, not inferred Qwen/GLM capabilities.
When configuring another deployment, review these four flags against its observed
support. No model-name heuristics, capability probes or persisted capability cache
were added. Unrelated agents retain their existing request behavior.

Capability filtering runs after `extra_body` so an unsupported `include_reasoning`
cannot be reintroduced there. Both false and true environment values are omitted
when support is false. Caller-owned option dictionaries and schema are preserved.
The configured deployment keeps the schema on its first request. An explicitly
configured deployment without schema support can use its supported JSON object
mode, or omit the format if neither is supported.

RCA retries once without `response_format` only for HTTP 400 with a recognized
unsupported-format code, or an unsupported-parameter/value code naming
`response_format`. The observed gateway message pattern is also recognized by
exact match for known field names. An unrelated/opaque 400, invalid-schema error,
other HTTP status, or transport error stops immediately. A 200 response also stops
retries even if local validation subsequently rejects it. Neither request contains
`include_reasoning` for the configured deployment. No behavior is learned across
analyses.

`ATTEMPT`, `HTTP_<status>` and `RETRY` identify attempt number and `response_mode`.
Allowlisted error type/code, field and schema keyword are safe diagnostics;
unrecognized values stay `unknown`. Even recognized messages are never printed.
Headers, credentials, payloads, evidence, raw error paths and alias maps are not
logged. Gateway usage and duration telemetry remain authoritative and unchanged.

The production change is limited to `src/backend/ai_engine.py`; regression tests
and this document are the only other changed files. Evidence selection, grouping,
aliases, budgets, prompt/schema, strict validator, deterministic RCA and persisted
state remain unchanged. Tests check the exact request schema, key omission,
capability isolation, unchanged evidence and deterministic identities, strict
validation after fallback, and secret-free diagnostics. Network, LLM and persistent
state access are blocked by fixtures.

Validation on Windows Python 3.14.8 / Streamlit 1.65.0:

- Before the fix: 34 failures / 88 passes reproduced unsupported-field
  serialization and the interrupted implementation's fallback behavior.
- Focused RCA tests: **122 passed** (1.19 s).
- Full `tests/regression`: **700 passed** (17.52 s), versus the 646-test baseline.
- `git diff --check`, modified-file whitespace checks and Python syntax checks
  passed; the complete diff was reviewed. No files staged, committed or pushed.

Re-smoke in the existing Citrix Python 3.14.8 / Streamlit 1.65.0 environment:

1. Restart Streamlit from the repository root to load the changed gateway:

   ```powershell
   python -B -m streamlit run src/frontend/streamlit_app.py
   ```

2. Keep current credentials, model, thinking flag, token setting and analysis
   options. Upload the same 16 MB file and run the same finite analysis.
3. Compare the deterministic results: 30,394 events, 8,089 patterns, 18 qualified
   signals, 153 correlations and 1 incident. Expect 8 selected signal groups,
   10 selected edges, 3 root hints, 6,155 serialized characters and approximately
   2,350 pre-call tokens for that same input.
4. Expect attempt 1 with `response_mode=json_schema`, HTTP 200 and no retry.
   Record only aggregate gateway prompt/completion/total tokens, finish reason and
   duration. Request-key omission is covered by the request-capture regressions;
   do not print the live payload or headers to verify it.
5. Success requires `finish_reason=stop` **and** accepted expert analysis in the UI
   (`case_analysis` populated with valid mapped references). HTTP 200 alone is not
   proof of semantic acceptance. Rejected output still preserves deterministic RCA.

No post-fix live gateway smoke was run locally; the supplied live matrix is the
compatibility evidence, and this re-smoke confirms the integrated production path.
