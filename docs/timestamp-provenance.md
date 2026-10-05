# Event timestamp authority and source timezone policy

This report supersedes the earlier timestamp investigation with the explicit
source-time decision and authority rules. The working tree was clean when this
implementation started; the prior 810-test timestamp/determinism baseline is
preserved except for deliberately updated expectations described below.

## Three distinct kinds of time

- **Event time** is the occurrence time identified by an authoritative parser
  header/field. Only this, or the authorized source fallback, drives correlation.
- **Source-record time** is acquisition evidence, such as OpenSearch `@timestamp`.
  It remains separately identified even when used as fallback event time.
- **Business time** is payload data: expiry, start, activation, invoice or payment
  dates. It must never silently become occurrence time.

All resolved occurrence times are timezone-aware UTC instants internally.
`19:32Z == 22:32+03:00`. OpenShift displaying three hours behind Türkiye is not
itself a timestamp defect. There is no manual +3-hour adjustment; display timezone
is a presentation concern. Original header text, raw messages and raw acquisition
timestamp evidence are retained separately from normalized values.

## Confirmed promotion defects

`TimestampNormalizer.extract()` previously used `pattern.search(first_line)` and
tried ISO `T` patterns before space-separated patterns. A line beginning with
`2026-09-28 19:30:53,252` could therefore select an embedded
`activationDate=2026-08-26T20:54:51.000+03:00`. A headerless payload date could also
be selected. Both structured and plain parsing used that result as event time.

KV extraction scanned the whole logical event and accepted matches inside nested
containers, dotted payload keys and continuation lines. Five deterministic tests
reproduced these promotion defects before their production fixes.

Event-time extraction now matches only the recognized header start (optionally
bracketed or severity-prefixed), or the parser's explicitly defined timestamp
field. KV time keys must be exact top-level assignments on the first line, outside
quoted payloads and containers. Pattern ordering cannot choose a later business
date in preference to a naive header.

The structured parser also used its historical timestamp span for **message
slicing**. Changing that slicing would change actual template identity, which is
outside this task's authorization. `legacy_message_span()` retains that exact
formatting behavior, with no normalization or occurrence-time authority. The
independent authoritative capture supplies event time. A regression preserves
the old message for the embedded-date example, and source-policy integration
checks preserve template IDs. The legacy message-boundary defect is not repaired
in this task; it cannot supply event time anymore.

The provided real-file counts (1,104 resolved records and approximately 1,104
lines containing embedded offsets) are consistent with this defect, but no local
raw-file audit establishes that every one of those 1,104 records followed it.
The source file itself was not supplied locally.

## Authority and normalization contract

`resolve_event_time()` implements this precedence:

1. Authoritative message/header timestamp with an explicit timezone/offset, or
   an explicitly supported epoch value: `message_explicit`.
2. Authoritative naive message/header timestamp plus the source's explicit
   timezone policy: `message_source_timezone`.
3. An absolute timestamp on the source record associated with this event:
   `source_record`.
4. Otherwise `None`: `untimed`.

A diagnostic timestamp search never supplies occurrence time. A missing year,
missing date or date without a clock is not filled in. Ambiguous/nonexistent DST
local clocks stay unresolved rather than selecting a fold or shifting the time.
Invalid source timezone names fail explicitly. Programmatic naive `datetime`
values also require source policy; the old implicit UTC assignment is removed.

| Parser | Authorized location |
| --- | --- |
| Plain/structured generic text | Header start, optionally bracketed or severity-prefixed. Other producer-prefix layouts require an explicit parser/policy contract. |
| Structured format-specific paths | Existing Windows/Zookeeper/HealthApp header fields or access-log timestamp brackets. |
| Syslog | Existing RFC5424 timestamp field; RFC3164 yearless clocks remain unresolved without another absolute source fallback. |
| Positional | Existing validated positional timestamp column, with date consistency checks unchanged. |
| JSON | Existing top-level `timestamp`, `time`, `@timestamp` keys in existing priority order. No recursive value scan. |
| KV | Exact top-level `timestamp`, `time`, `@timestamp` assignments on the header line. Nested/dotted/continuation occurrences are not event fields. |
| Policy parser | The explicitly declared `timestamp_group` in its existing first-line policy contract. Arbitrary attributes are not promoted. |
| Structured alarms | The existing canonical `timestamp` source field; no text parser is introduced. |

The previous ISO suffix and epoch-zero fixes remain. Explicit `Z`, `+03:00`,
`+0300`, negative offsets, fractional seconds and access-log offsets retain their
absolute meaning. Source policy resolves supported complete naive dates, including
the real `YYYY-MM-DD HH:mm:ss,fff` header. Slash-date locale interpretation is
unchanged; no new day/month convention is invented. Yearless syslog/clock-only
values remain unresolved even when the source timezone is known.

## Source-scoped configuration and provenance

`TimestampSourcePolicy(source_timezone=None)` and `TimestampContext` are frozen
value objects under `parser_layer/timestamp/source_policy.py`. They express a
source-provided normalization contract, independent of ingestion technology.
The full-pipeline entry point creates them per invocation and passes them through
the existing parser/outcome contract into canonical construction. Neither the
cached pipeline nor parser stores a mutable/current timezone.

Backend file invocation for this source:

```python
result = pipeline.process_file(path, source_timezone="UTC")
```

Other sources can explicitly use `source_timezone="Europe/Istanbul"`. Omitting
it leaves naive timestamps unresolved. File, structured-alarm/package and finite
page entry points accept the optional keyword. A page invocation's policy applies
to the explicitly supplied source scope; callers must separate sources requiring
different timezone policies. No policy is inferred from environment, machine,
user, browser, upload time or file modification time.

For this file, the user explicitly established UTC as its source timezone:

```text
2026-09-28 19:32:11,408 + source_timezone=UTC
    -> 2026-09-28T19:32:11.408+00:00
2026-09-28 19:32:11,408 + source_timezone=Europe/Istanbul
    -> 2026-09-28T16:32:11.408+00:00
```

Windows/Citrix uses stdlib `zoneinfo` with `tzdata` now declared in requirements.
Use equivalent IANA timezone data versions when comparing runs across machines.

Canonical events have additive `timestamp_provenance` metadata:

```json
{"basis":"message_source_timezone","message_timestamp_raw":"2026-09-28 19:32:11,408","source_timezone":"UTC","source_record_time":null,"source_record_timestamp_raw":null,"source_record_field":null}
```

All four bases are explicit, including untimed events. Provenance accompanies the
canonical event into the templated downstream row; signals retain bounded
`timestamp_basis_counts`. This metadata is not used for scores/ranking. Legacy
standalone aggregate inputs without provenance have an empty basis-count mapping.

## Source-record fallback and OpenSearch

OpenSearch acquisition continues to preserve its configured timestamp field as
`IngestedLogRecord.source_timestamp_raw`, without performing parsing in the
adapter. The additive `source_timestamp_field` records the actual configured
field name (normally `@timestamp`). Acquisition still has no parser dependency.

For an assembled event, the **first included record** owns the source fallback.
Omitted blanks and continuation records cannot lend their timestamps to an event.
Use that record's normalized `source_timestamp` if it is a valid absolute instant;
otherwise attempt its `source_timestamp_raw`. Never use `first_observed_at`,
retrieval order or the machine clock. Source timezone policy resolves message
clocks only; it does not guess a timezone for naive acquisition timestamps.

If message time resolves, it wins even when source-record time differs. Both the
normalized source-record instant and original source evidence remain available
in provenance. If message time cannot resolve and source time is absolute, event
time uses the source instant with `basis=source_record`. No skew rejection,
correlation adjustment or automatic time shift is introduced.

## Time-quality diagnostics

`AIOPS_TIME_DEBUG` remains opt-in, false by default, accepting `1`, `true`, `yes`
and `on` case-insensitively. `RCA_DEBUG` is independent and need not be enabled.
All new output remains counts only; no raw headers, business values, source field
values, payloads, credentials, HTTP headers or general environment contents print.

Added counters:

- `timestamp_basis_message_explicit`
- `timestamp_basis_message_source_timezone`
- `timestamp_basis_source_record`
- `timestamp_basis_untimed`
- `naive_header_resolved_by_source_policy` (line/header parser routes; JSON/KV and
  structured-alarm fields are counted in the broader message-policy basis)

Existing event/source-presence, parsed/downstream resolved/unresolved,
not-delivered, lost/changed, candidate/qualified timed/untimed and parser/status
counters remain. Presence means detected timestamp evidence or a resolved instant;
for pages this can include source-record fallback. A configured but unresolved
local clock is labeled `source_timezone_unresolved`; missing policy remains
`timezone_missing`. Status labels are diagnostic, not authority or scoring rules.

All collectors are invocation-local, with fixed labels and bounded storage. The
four basis counts sum to delivered canonical events on the real parser paths.
`parsed_to_downstream_timestamp_lost` and `..._changed` should both remain zero.
No diagnostic fields are added to the top-level pipeline result.

## Compatibility and validation

Segmentation boundaries, raw event text, message/template generation and actual
template IDs are preserved. Persistent template/Drain state and policy signatures
are unchanged: no reset, invalidation or migration. Corrected occurrence time can
change canonical event IDs (timestamp is part of their seed), factual windows,
signal/incident identities and the resulting decisions naturally.

Canonical schema version and the frozen `ParseOutcome` envelope remain compatible;
provenance and source-field metadata are additive. Old callers can omit all new
keywords. Parser overrides receiving source context should forward the optional
`timestamp_context` keyword. Exact canonical snapshots now assert the added
metadata. The old page determinism test explicitly expecting an untimed event
despite valid source-record time was updated to assert the authorized fallback,
its basis and the unchanged prohibition on observation-time substitution.

No qualification weights, correlation scores/evidence, incident grouping,
deterministic RCA ranking or equal-time ordering/precedence rule changed.
RCA prompts, evidence grouping, budgets, schemas, debug mode and fallback are
untouched. No wall-clock occurrence-time fallback is restored.

Local tests exercise embedded business values, header/field authority, both source
timezones, DST ambiguity, explicit offset equivalence, source-record precedence,
provenance through aggregation, real ProgrammingError/soap/BALANCE header shapes,
repeated analyses and a fresh pipeline using the same temporary learned state.
The ProgrammingError fixture uses repeated occurrences to qualify under the
existing threshold; no production signal count is hardcoded. New tests block
network/LLM/SQLite access and use temporary learning state. UI tests use the real
Streamlit AppTest runner with fake analysis/acquisition boundaries.

## Exact Citrix three-run smoke

1. Keep the same policies, template/Drain learning state, topology, window size and
   16 MB file. Install the updated requirements in the normal Citrix environment
   if needed; do not reset learning state. Stop Streamlit.
2. From the repository root in that environment:

   ```powershell
   $env:AIOPS_TIME_DEBUG = 'true'
   $env:RCA_DEBUG = 'false'
   python -B -m streamlit run src/frontend/streamlit_app.py
   ```

3. Open **Dosya / Paket Analizi**, upload
   `aicc-mcp-gateway-http-7fb879fc5f-9tpkw-aicc-mcp-gateway-http.log`, then select
   **Timestamp timezone: UTC**. Select UTC after uploading: a new upload resets
   the selector to Unknown. Leave the expert setting as before so aggregate
   `[RCA CONTEXT]` remains available. Analyze and label the result Run 1.
4. Start another file investigation in the same Streamlit process. Use the same
   file and explicitly confirm **UTC** again. Analyze as Run 2; the pipeline
   instance and intentional learning remain cached.
5. Stop Streamlit with Ctrl+C and relaunch the same command in the same terminal.
   Upload the same file, select **UTC** again, and analyze as Run 3. The selector
   safely defaults to Unknown after restart.
6. Retain both `[PIPELINE]` summaries, the complete `[TIME QUALITY]` integer block
   and `[RCA CONTEXT]` from every run. Compare all counts with the same source
   policy. Expected interpretation, conditional on parser support for all headers:

   ```text
   events_total=30394
   source_timestamp_present approximately 30391
   parsed_timestamp_resolved approaches 30391
   timestamp_basis_message_source_timezone approaches 30391
   parsed_timestamp_unresolved approaches 3
   timestamp_basis_untimed approaches 3
   timestamp_basis_source_record=0          (this is a file upload)
   downstream_timestamp_resolved=parsed_timestamp_resolved
   downstream_timestamp_unresolved=parsed_timestamp_unresolved
   parsed_to_downstream_timestamp_lost=0
   parsed_to_downstream_timestamp_changed=0
   ```

   These are smoke expectations, never production assertions. The three headerless
   startup events must not acquire business or observation timestamps. The prior
   1,104 resolved values are not trusted targets: without source policy, rejecting
   embedded dates can reduce that number; with UTC policy, authoritative headers
   should instead account for almost all resolved events.
7. Qualified real errors should become timed when their header parser succeeds.
   If they remain untimed, inspect `parser_<route>_<status>`, `timezone_missing`,
   `source_timezone_unresolved` and retained header evidence. Do not tune downstream
   rules. Real windows may change qualification counts, correlations and incidents.
   Do not target 105 correlations, one incident, or the previous 0/15 timed split.
8. Require matching deterministic counts/time-quality distributions across the
   three runs under the same configuration and relevant learning state. Record
   any learning change separately. RCA context selection should be deterministic;
   expert wording is not required to match, and previous prompt tokens are not a
   target. After collecting results, stop Streamlit, set
   `$env:AIOPS_TIME_DEBUG = 'false'`, and restart.

The real Citrix smoke remains an operator step. Local tests establish the source
policy and authority semantics, not a measured distribution for the unavailable
real file.

## Validation results

- New authority/source-timezone suite: **41 passed**; existing timestamp/provenance
  suite: **47 passed** (combined **88**).
- Existing determinism suite: **27 passed**, including the deliberate source-record
  fallback expectation update described above.
- Parser outcomes, multiline/context boundaries, finite ingestion and OpenSearch
  acquisition: **253 passed**.
- Focused RCA expert/evidence/density: **158 passed**.
- Authority plus Streamlit source/UI checks: **85 passed** (41 authority + 44 UI).
- Full `python -B -m pytest -q -p no:cacheprovider tests/regression`:
  **852 passed** in 30.62 seconds, versus the 810-test baseline.
- Complete diff reviewed. Tracked and new-file whitespace checks pass. No real
  network/LLM calls, production SQLite or production learning state used in tests.
- Nothing staged, committed or pushed.

Changed files: `requirements.txt`; `full_pipeline_v2.py`; ingestion contracts and
OpenSearch source mapping; canonical builder and parser pipeline; KV/structured/
syslog parsers and timestamp normalizer; new `timestamp/source_policy.py`;
`time_quality.py`; aggregation's additive basis counters; frontend upload runtime
and Streamlit selector; new `test_timestamp_authority.py`; updated parser outcome,
ingestion, determinism and Streamlit source regressions; this report.
