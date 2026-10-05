# Timestamp provenance and Citrix time-quality investigation

The reported three Citrix runs of the same 16 MB file were deterministic:
30,394 segmented/parsed/templated events, 8,089 candidates, 15 qualified signals,
0 correlations, 15 incidents and 15 RCA results. Selected RCA evidence had zero
known signal windows. The raw file and its timestamp examples were not supplied
for this investigation. Local fixtures confirm the defects below, but do not
establish that those defects occur in that file.

## Provenance through the current implementation

| Stage | Authoritative field/path | Finding |
| --- | --- | --- |
| Physical lines | `MultilineAssembler.iter_event_records()` | The file adapter removes line terminators, omits blank lines with boundary evidence, and joins retained lines with `\n`. The timestamp-bearing first line and continuation indentation survive. No segmentation change was needed. |
| Logical event | `ParserPipeline.process_with_outcome(raw)` | Format detection chooses JSON, syslog, structured text, positional text, KV, plain text or a resident custom policy. `process()` delegates to this same outcome method. Outcome recognition/delivery describe parsing, not timestamp resolution. |
| Extracted fields | `fields['timestamp']`, optional `fields['raw_timestamp']` | `TimestampNormalizer` is the existing timestamp authority. Explicit source values/header captures supply time; no second parser or clock is added. Parsers now pass their existing raw capture into the builder's already-supported diagnostic field. |
| Canonical event | `event['timestamp']` | `CanonicalEventBuilder` copies the parsed value. For unresolved time, `attributes.raw_timestamp` and `attributes.timestamp_status` retain diagnostic evidence. `observed_timestamp` remains observation metadata only. |
| Template result | `TemplatePipeline.process(event)` | Reads the message and returns `TemplateResult`; it does not replace the event or mutate its timestamp. |
| Downstream input | `FullAIOpsPipelineV2`: `row = dict(event)` | Adds template fields and invocation-local `source_order`. Timestamp survives unchanged. Tests compare canonical and templated values and simulate a loss to verify diagnostics detect it. |
| Aggregation | `SignalAggregator`, `analysis_time.source_time_ms()` | Consumes only `timestamp`. Valid values produce factual millisecond buckets; unresolved values enter an explicit `None` bucket. Observation/upload/retrieval clocks are ignored. |
| Signal | `timestamp_resolved`, `first_seen_ms`, `last_seen_ms`, `window_start_ms`, `window_end_ms` | Timed signals have real bounds. Untimed bounds remain `None`, and the signal ID ends in `:untimed`. Qualification thresholds are unchanged. |
| Correlation | `SignalCorrelator.correlate()` | Existing temporal predicates require valid first/last bounds. Untimed signals cannot enter pair comparisons. Resolved time makes comparison possible but does not guarantee an edge. |
| RCA evidence | `RCAEvidenceSelector` | `signal_windows` counts distinct numeric windows of resolved member signals. Zero means no known window in that selected group; it does not establish that every raw event in the file lacks time. |

Structured alarm packages bypass text parsing and copy the source `timestamp`
field into their canonical/template representation. Their diagnostics label this
route `structured_alarm`. In page ingestion, `source_timestamp_raw` and
`source_timestamp` on acquisition records remain separate `source_provenance`;
they are not automatically promoted into canonical event time. Time-quality
presence counts concern parsed logical-event evidence (or the structured alarm's
timestamp field), not acquisition/observation metadata.

## Confirmed defects and supported formats

Before production edits, four tests showed that `TimestampNormalizer.normalize()`
could resolve a timestamp while `extract()` truncated its explicit timezone:

- `2026-10-05 09:10:11Z`
- `2026-10-05 12:10:11+03:00`
- `2026-10-05 12:10:11+0300`
- `2026-10-05T12:10:11 +03:00`

The exact loss stage was **header extraction before canonical construction**.
The existing ISO search patterns now retain the complete suffix. All four yield
`2026-10-05T09:10:11+00:00`. No timezone is inferred. Three additional review
regressions ensure a following word beginning with `Z`, or an offset-like prefix
inside a longer token, cannot be mistaken for a complete timezone suffix.

Three other failing tests showed that JSON's truthiness checks discarded epoch
zero under `timestamp`, `time`, and `@timestamp`. Zero now survives with the
existing key priority/fallback rules. It represents the actual Unix epoch.

Seven tests reproduced incomplete unresolved-time diagnostics: severity-prefixed
ISO, day-first slash dates, Apache brackets, a non-leading KV time, severity-prefixed
month/day clocks, positional clocks and custom policy timestamp groups. Their
existing raw captures now reach the builder, and diagnostic-only validation
recognizes missing timezone/year evidence. These changes do not resolve naive time.

| Tested source shape | Parser route | Result |
| --- | --- | --- |
| `YYYY-MM-DDTHH:mm:ssZ`, `...+03:00`, `...-0400` | Structured text; also explicit JSON/KV fields and RFC5424 | UTC instant with the source offset respected. |
| `YYYY-MM-DD HH:mm:ssZ`, `...+03:00`, `...+0300`, `... +03:00` | Structured text | UTC instant; adjacent suffix loss fixed. |
| `YYYY-MM-DDTHH:mm:ss +03:00` | Structured text | UTC instant; spaced suffix loss fixed. |
| `YYYY-MM-DD HH:mm:ss,fff+03:00` | Structured text | UTC instant with fractional seconds retained. |
| `ERROR YYYY-MM-DDTHH:mm:ss+03:00 ...` | Structured text | UTC instant; prefix does not remove the time. Severity extraction itself is unchanged. |
| `YYYY-MM-DD HH:mm:ss`, `YYYY-MM-DDTHH:mm:ss` | Structured text | `None`, `timezone_missing`. |
| `DD/MM/YYYY HH:mm:ss` (including day > 12 and ambiguous day/month) | Structured text | Header span extracted, but `None`. Existing normalizer format list is month-first and naive; diagnostics also recognize day-first shape without choosing a locale or timezone. Offset-bearing slash formats are not newly supported. |
| `Oct  5 HH:mm:ss host app: ...` | RFC3164 syslog | `None`, `year_missing`; timezone is missing too. |
| `ERROR MM-DD HH:mm:ss [worker] ...` | Plain-text fallback from structured detection | `None`, `year_missing`; segmentation recognizing this header does not make it absolute time. |
| `[Sun Dec 04 04:47:44 2005] ...` | Structured text | `None`, `timezone_missing`; raw diagnostic capture preserved. |
| `DD/Mon/YYYY:HH:mm:ss +0300` in an access-log bracket | Structured text | UTC instant. |
| JSON epoch milliseconds and zero; explicit KV/JSON ISO timestamp | JSON/KV | Factual UTC instant. |
| Positional `YYYY-MM-DD-HH.mm.ss.fff`; custom captured `YYYYMMDD-H:M:S:fff` | Positional/custom | `None`, `timezone_missing`, with original capture retained. |
| Missing time or malformed calendar date | Existing parser/fallback | `None`, respectively `missing` or `invalid`. |

Repository documentation (`docs/mimari.md`, `docs/fazlar.md`) specifies unresolved
time when year/timezone evidence is absent. No source-local timezone convention
was found for these text logs. The existing normalizer's legacy handling of
programmatic naive `datetime` objects is unchanged; this task does not extend that
behavior to naive strings.

## Opt-in diagnostics

Set `AIOPS_TIME_DEBUG=true`; default is false. `1`, `true`, `yes`, and `on` are
accepted case-insensitively with surrounding whitespace ignored. The flag is read
at each full-pipeline invocation and is independent of `RCA_DEBUG`.

The collector lives only within that invocation and retains fixed counters, not
events, raw values or unbounded per-format labels. It prints one `[TIME QUALITY]`
block after downstream processing. Disabled mode creates no collector and prints
no additional lines. No diagnostic fields are added to the pipeline result.
Standalone `DownstreamAIOpsPipeline.process()` has no parser-stage diagnostics;
instrumentation belongs to the full file/page/package orchestration path.

| Fields | Meaning |
| --- | --- |
| `events_total`, `parsed_events_total`, `parser_no_event` | Logical inputs, delivered canonical records and inputs with no canonical record. For structured alarms, the first two count source records. |
| `source_timestamp_present`, `source_timestamp_not_detected` | Delivered events with resolved time or a nonempty raw timestamp capture, versus events with no detected timestamp. Presence includes malformed and source-local values; nondetection is not proof that an unsupported format contains no time. |
| `parsed_timestamp_resolved`, `parsed_timestamp_unresolved` | Canonical values convertible by the same authority aggregation uses. |
| `downstream_events_total`, `downstream_timestamp_resolved`, `downstream_timestamp_unresolved` | Exact templated rows supplied to downstream and their time quality, before aggregate filtering. |
| `parsed_events_not_delivered`, `parsed_resolved_not_delivered` | Canonical records rejected by templating, including those carrying resolved time. |
| `parsed_to_downstream_timestamp_lost`, `parsed_to_downstream_timestamp_changed` | Delivered rows whose resolved time disappeared, or whose normalized time changed (including loss/gain). Both should be zero. |
| `signal_candidates_timed`, `signal_candidates_untimed` | Candidate signals with/without usable temporal bounds. |
| `qualified_signals_timed`, `qualified_signals_untimed` | The subset eligible/ineligible for existing temporal pair comparisons. |
| `timestamp_status_<status>` | Delivered-event counts for `resolved`, `missing`, `timezone_missing`, `year_missing`, `date_missing`, `unparsed`, `invalid`, `unknown`. `unparsed` means resolvable diagnostic evidence was not promoted by the selected parser. `invalid` also covers unsupported timestamp syntax. |
| `parser_<parser_id>_<status>` | The same counts by delivering parser, using fixed labels from the existing outcome contract. Older injected parsers without outcomes are labeled `unavailable`. |

Only code-owned labels and integer counts are printed. No examples, full logs,
timestamp values, API keys, Authorization, headers, credentials, policy contents
or general environment contents are printed. Existing RCA prompt debugging is
unchanged and is not needed for these diagnostics.

## Interpretation and compatibility

No downstream propagation defect was reproduced. A synthetic eight-event analysis
has six detected timestamps: four resolved, two timezone-missing, plus two events
with no time. All four resolved timestamps reach aggregation; no value changes.
It yields two timed and two untimed qualified signals. The timed pair shares a
service and differs by one second, naturally producing one existing `.58` edge.
The two untimed groups retain `signal_windows=0`; each timed group has one window.

For the real file, parsing/extraction failure and insufficient source time both
remain possible. The observed zeros alone cannot distinguish them. No claim is
made that the real file has any of the reproduced suffix/epoch defects. If all
qualified signals remain untimed, zero temporal correlations is correct under the
current predicates. If some become timed but there are still no edges, inspect
the unchanged service/topology/gap predicates; time alone is insufficient.

There is no change to segmentation boundaries, correlation algorithms/scores,
qualification thresholds, incident rules, RCA grouping/prompts/budgets/schema,
equal-time tie-breaking or the rule that equal time is not temporal precedence.
No machine/upload/observation-time fallback is restored. Missing factual time
remains `None`.

The canonical schema, parser outcome signature, template algorithm, policy
signatures and persisted state formats are unchanged. Raw timestamp captures use
an existing builder field and can improve unresolved diagnostic attributes.
Correctly resolved timestamps change affected event IDs (time is part of their
seed), window/incident identities and natural downstream decisions. Where a
truncated timezone suffix previously leaked into a message with no following
severity, consuming the complete timestamp can also change normalized message
text and thus template IDs. Existing learning remains readable and reusable for
unchanged messages; old entries are retained. No state is reset or migrated.

## Citrix three-run procedure

1. Use the same repository revision, 16 MB input, topology, window size and UI
   settings as the completed smoke. Keep the verified policy and template/Drain
   state; do not delete/reset them. Stop Streamlit before enabling the flag.
2. In the Citrix PowerShell terminal, from the repository root:

   ```powershell
   $env:AIOPS_TIME_DEBUG = 'true'
   $env:RCA_DEBUG = 'false'
   python -B -m streamlit run src/frontend/streamlit_app.py
   ```

   Use the same Python environment normally used for that deployment. Leave the
   existing expert setting enabled to produce `[RCA CONTEXT]`; that aggregate
   line does not require printing prompts with `RCA_DEBUG`.
3. Analyze the same file twice through the same cached Streamlit pipeline. Label
   the terminal results Run 1 and Run 2. For each run retain both `[PIPELINE]`
   summary lines, the complete `[TIME QUALITY]` integer block, and `[RCA CONTEXT]`.
4. Stop Streamlit with Ctrl+C. Run the same launch command again in the same
   terminal (the flags remain set), and analyze the same file once as Run 3.
5. Compare all three runs. With unchanged boundaries the expected first summary
   is `Segmentasyon=30394 | Ayrıştırma=30394 | Şablonlama=30394` and
   `events_total=parsed_events_total=downstream_events_total=30394`.
   Require these accounting checks:

   ```text
   source_timestamp_present + source_timestamp_not_detected = parsed_events_total
   parsed_timestamp_resolved + parsed_timestamp_unresolved = parsed_events_total
   downstream_timestamp_resolved + downstream_timestamp_unresolved = downstream_events_total
   parsed_to_downstream_timestamp_lost = 0
   parsed_to_downstream_timestamp_changed = 0
   parsed_events_not_delivered = 0
   qualified_signals_timed + qualified_signals_untimed = [PIPELINE] Nitelikli
   ```

   All diagnostic counts and deterministic pipeline/RCA context selection counts
   should match across the three runs when learning/configuration are equivalent.
   Record any policy/template learning change separately. Expert wording need not
   match. The prior 1,825 prompt tokens and 4,461 evidence characters are comparison
   data, not targets after newly recovered time changes selected evidence.
6. Interpret the actual counts, without presupposing their values:
   - `source_timestamp_present=30394`, `timestamp_status_timezone_missing=30394`,
     `downstream_timestamp_resolved=0` means detected calendar clocks without an
     absolute timezone, not propagation loss. With the same 15 qualified signals,
     timed/untimed would be `0/15`, and no known signal windows are expected.
   - Large `timestamp_status_missing`/nondetection counts mean no timestamp was
     detected by the chosen paths. Use parser breakdowns to request a tiny locally
     sanitized header example if the source owner believes time exists.
   - `timestamp_status_unparsed>0` identifies resolvable evidence that its selected
     parser did not promote. `invalid>0` needs source-format validation.
   - Resolved parser and downstream counts matching above zero, with zero
     lost/changed counts, rule out a timestamp drop at templating for those rows.
   - `qualified_signals_timed>0` enables temporal comparisons. Correlation/incident
     counts must emerge from factual time and existing predicates. Do not target
     the historical 105 correlations or one incident.
7. Disable diagnostics and restart after collecting results:

   ```powershell
   $env:AIOPS_TIME_DEBUG = 'false'
   python -B -m streamlit run src/frontend/streamlit_app.py
   ```

The real three-run time-quality smoke remains to be performed by the Citrix
operator; no real-file timestamp distribution or post-fix count is claimed here.

## Local validation

- Timestamp/provenance suite: **47 passed**. Defect reproductions failed before
  their production fixes (14 cases total), then passed.
- Three additional suffix-token regressions failed during implementation review
  and passed after requiring a complete timezone token.
- Existing downstream determinism suite: **27 passed**.
- RCA expert/evidence/density suites: **158 passed**.
- Parser outcomes, multiline boundary, context isolation and ingested pipeline:
  **71 passed**.
- Full `python -B -m pytest -q -p no:cacheprovider tests/regression`:
  **810 passed** (30.92 s), preserving the 763-test baseline plus 47 new cases.
- New tests block sockets, HTTP, LLM calls and SQLite before constructing parser
  dependencies; real template persistence uses temporary paths only.
- Changed files: `.env.example`, `full_pipeline_v2.py`, new `time_quality.py`,
  JSON/KV/plain/structured/positional/policy parsers, `timestamp_normalizer.py`,
  `timestamp_evidence.py`, new `test_timestamp_provenance.py`, and this report.
