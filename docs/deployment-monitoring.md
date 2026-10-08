# Deployment monitoring

## Pipeline Trace / Evidence Explorer

**Pipeline Trace is observational. It does not alter pipeline decisions.**
For newly executed Deployment Monitor runs, the existing stage selector keeps
its summaries and adds persisted Records / Evidence, selected input/output
details, related evidence, and Event Lineage. Search covers stored text, IDs,
severity, service/component, pod/container, pattern and boundary status. Signal
decisions and pattern identities also have dedicated filters. Tables page through
100 items; a 240-character table preview explicitly indicates shortening. The
selected item shows its stored content, including continuation indentation.
Technical JSON is secondary to content and recorded field/value tables.

The current evidence inventory and capture additions are:

| Stage | Existing authoritative evidence | Observation added |
| --- | --- | --- |
| Acquisition | `IngestedLogRecord`, `SourceReference`, stream identity, raw source time/order | Sanitized record content, ordinal, duplicate flag, inside-window/overlap flags, and source-reference hash before deduplication |
| Segmentation | `AssembledEvent` contributors, `LineEvidence`, pinned policy, emission reason, `BoundaryStatus`; `UnassembledRecord` disposition | Full bounded output list, assembled text, contributor links, classification/confidence, policy identity/validation reference, admitted flag, owned analysis ordinal |
| Parsing | `ParseOutcome` and canonical event fields; old samples capped at 200 | Outcome from the same parser call, canonical fields and logical input link; no extraction heuristics or reparse |
| Patterns | Authoritative template result and `last_decision` | Every bounded occurrence linked to its canonical event, template ID/text, reliability/source/reason; existing aggregate pattern counts stay intact |
| Signal | All candidates, score, qualified flag, reason and qualification evidence; only eight representative event IDs | Exact aggregation membership callback before the representative cap; typed `QualificationEvidence` records the existing threshold/eligibility rule at the gate |
| Correlation | Accepted edges with source/target signal IDs, scores, time gaps, relationship evidence | Persisted copies and member links; rejected proposals do not exist in the domain and are not invented |
| Incident | IDs, times, severity, services, signals, templates, probable root and confidence | Persisted copies with actual signal membership and links to edges whose endpoints belong to the incident; these links do not claim every edge caused construction |
| RCA | Deterministic ranked candidates/scores/evidence and optional validated case interpretation | Separate deterministic results, actual submitted expert Evidence Pack and alias/member maps, and optional interpretation/status |

### Read model and lineage

`monitoring/trace.py` defines `PipelineTrace`, `TraceItem` and `TraceLimits`.
The version-1 envelope is stored under `detailed_pipeline_trace` alongside the
existing result. It contains `run_id`, `schema_version`, `limits`, eight ordered
`stages`, exact per-stage `totals`, `omitted_items`, and `trace_complete`.
Each item contains `ref`, `ordinal`, `parents`, sanitized `data`,
`preview_truncated`, `omitted_fields`, and `omitted_links`.

`pipeline_observation.PipelineObserver` is an optional synchronous callback over
borrowed outputs. Core processing imports only this source-neutral protocol;
it does not depend on monitoring DTOs. The executor constructs one collector per
run. No collector is cached on a pipeline, parser, template miner or user session.
Parsing uses the existing `process_with_outcome` implementation (the same method
called by `process`), exactly once. Aggregation observes its actual group members;
the frontend does not reproduce grouping, scoring or membership logic.

Source references use the same SHA-256 scope/partition/record-ID identity as
monitor receipts and boundary diagnostics, with the existing intentional version
exclusion. Canonical event IDs, template IDs, signal IDs and incident IDs remain
unchanged in processing. Presentation references are deterministic within a run:
`record:N`, `logical:N`, `canonical:N`, `occurrence:N`, `signal:<domain-id>`,
`incident:<domain-id>` and `rca:<incident-id>`. Correlation objects have no domain
ID, so `correlation:N` follows their authoritative output order. Expert evidence
uses `expert:input`, `expert:result` or `expert:status`. Unsafe or excessively long
display IDs become an explicit `redacted-id:<sha256>` consistently in links;
this never replaces a domain identity. The run ID supplies the namespace.

Logical ordinals include excluded context/dispositions; `analysis_event_ordinal`
identifies an admitted event in the original analysis. Acquisition includes
duplicate retrieval records, while only the retained record links to assembly.
Consequently trace-stage totals need not equal the owned-event summary counts.
Event Lineage traverses stored parent links and their reverse links. It can follow
a physical record through assembly, canonical output, pattern occurrence, signal,
correlation, incident and RCA. Suppression is a factual terminal state. Missing
details under a budget are marked as limited coverage, never as a failed stage.
Reference design leaves pattern occurrences addressable for a later baseline UI;
no baseline facts or anomaly decisions are implemented here.

### Persistence, limits and security

Trace construction uses already acquired records and actual stage outputs; it
does not query OpenSearch again. Opening any stage, RCA or lineage performs no
segmentation, parsing, templating, scoring, correlation, incident construction,
RCA, network or LLM call. The frontend reads only the selected result whose trace
`run_id` matches that completed MonitorRun.

The existing `SQLiteMonitorRepository.succeed` transaction publishes the whole
result (including trace), successful status, receipts and watermark atomically.
Acquisition/pipeline failures publish no result. Serialization or transaction
failure cannot leave a completed partial trace. SQLite schema version remains 1;
no table migration, historical rewrite, watermark reset, monitor recreation,
template/Drain reset, or policy invalidation occurs. Historical results without
the new field remain readable and show:

> Detailed pipeline trace was not stored for this historical run.

Default trace configuration preserves up to **2,000 items per stage**, **16 MiB
for the entire serialized trace**, **8,192 characters per textual field**,
**16,000 field nodes/links per item**, and nesting depth **12**. These are
`TraceLimits` application configuration, injectable through
`OpenSearchMonitorExecutor(trace_limits=...)`, not monitor scheduling settings.
The standard acquisition bound is 100 records × 20 pages = 2,000; existing
configuration permits at most 500 × 20 = 10,000. Trace limits do not change either
acquisition bound. The shared byte budget reserves 4 KiB for its envelope.

Each stage retains an ordered prefix until its item/byte allowance is exhausted;
there is no random sampling. Counters continue for every omitted item. Text/field
shortening and omitted links set item flags and `trace_complete=false`; the UI
displays both global and item-level coverage notices. IDs and source hashes are
preserved independently of text-preview limits where possible. Read-time paging
does not discard stored records. The collector's temporary identity/membership
maps are bounded by the existing finite acquisition, and it does not retain raw
source objects. This adds bounded evidence per run, not an unbounded raw-log
archive; existing history retention is unchanged.

A compact representative fixture with three acquisition records, two logical
events, two canonical outputs, two pattern occurrences and one suppressed signal
serializes to approximately **6 KiB**. Full-contract regression fixtures also
exercise larger content, the legacy 200-sample boundary, and item/byte/field/link
limits. Retained-memory probes cover 2,000 and 10,000 input items after exhaustion.

All trace text and nested canonical attributes cross `evidence_redaction` before
retention/persistence. It covers configured OpenSearch credentials/endpoints and
the configured AI key, Authorization/Basic/Bearer, JWTs, API keys, passwords,
secrets, credentials, cookies and sensitive headers, private-key blocks, and
credential-bearing URLs. `user_id`/`userId` values are conservatively redacted
because this workload can embed credentials there. Arbitrary acquisition metadata
and cursors are not captured. The UI redacts again, including technical JSON,
and displays log content through text/code/dataframe controls, never raw HTML.
No unredacted trace copy or trace content is written to worker logs. Redaction
does not modify events used by learning/processing or the expert prompt.

`BoundaryStatus` and `emission_reason` are copied from the actual assembly result.
No completion/truncation inference is added, and
`boundary_allows_baseline_evidence()` remains unchanged. Deterministic RCA ranking
is preserved even when the optional expert is unavailable. The expert trace
shows the actual submitted pack (sanitized), with any further trace shortening
explicit; it never invokes the selector or gateway from the UI. Existing
`qwen_destekli` annotation does not transfer authority to the expert.

### Trace smoke on the existing Citrix deployment

1. Keep the existing checkout, monitor definitions, `AIOPS_MONITOR_DB`, learning
   directory and environment. Reload/restart its Streamlit process with this code
   if required. Do not create a replacement monitor or edit SQLite to force a run.
2. When the existing monitor is enabled and due, from that checkout run exactly
   `.\.venv\Scripts\python.exe -B tools\monitor_worker.py --once --max-runs 1`.
   Require START → ACQUIRED → PIPELINE → SUCCESS and record the exact run ID/window.
3. In Deployment Monitors, open that same new successful window. Preserve the
   summaries and inspect all eight stage selections. For a normal workload,
   zero qualified signals/correlations/incidents/RCA are valid factual empty states.
4. Select a logical event and compare contributing physical records, indentation,
   assembled text, stream, source times, policy, emission reason and boundary
   status. Follow its canonical event, authoritative pattern occurrence and actual
   signal decision/evidence through Event Lineage. Check any coverage notices.
5. Inspect content and technical expanders for credential redaction, including
   `user_id`. Open an older run and verify its explicit historical-trace message.
   Confirm the new watermark equals the successful logical end without replay.

The local implementation environment has no configured monitoring database at
`data/monitoring/monitors.sqlite3`, no available browser session, and its native
computer-use pipe is unavailable. The live procedure therefore remains pending
access to the existing Citrix deployment; no successful live smoke is claimed and
no empty replacement monitoring database is created.

Trace validation: **162 focused tests** passed across trace, real ingestion,
monitoring persistence and Streamlit suites. The full offline regression suite
passed **1,018 tests** (968 baseline + 50 additions). Tests use temporary databases
and learning state, block external network/LLM calls, and cover identical pipeline
outputs with/without observation, atomic rollback, full membership, redaction,
historical reads, actual expert input capture, and explicit resource limits.
Tracked and untracked whitespace checks passed. Nothing was staged, committed or
pushed; live Citrix validation remains pending as described above.

## Architecture and configuration finding

Streamlit is the **control plane**: monitor definitions, enable/pause, persisted
status, history, and the existing Investigation Workbench. It never schedules
monitor execution or polls OpenSearch. `tools/monitor_worker.py` is the independent
**execution plane**. It acquires bounded windows and calls
`FullAIOpsPipelineV2.process_ingested_pages`; parsing, templates, qualification,
correlation, incidents, deterministic RCA, expert RCA and planning remain there.

The local configuration investigation found **no `OPENSEARCH_*` keys** in either
the process environment or repository `.env`. No values or credentials were
printed. `load_connection()` requires the explicit connection settings below and
the old UI collapsed all configuration errors into a generic unavailable warning.
This is the locally observed cause; Citrix configuration was not inspected or
contacted. There is no evidence to justify inventing aliases or weakening TLS.

Previously `.env` loading was an indirect side effect of importing `ai_engine`.
Both entrypoints now explicitly load the repository `.env`, with existing process
variables taking precedence. Restart both processes after changing configuration.
Monitors/history render with missing source configuration. "Configuration loaded"
does **not** claim that connectivity/authentication has been checked: the worker
checks that on acquisition. Source unavailable, monitor paused, and run failed
are separate states.

Required application environment settings (values are never stored in monitors):

```text
OPENSEARCH_HOSTS
OPENSEARCH_USERNAME
OPENSEARCH_PASSWORD
OPENSEARCH_USE_SSL           true or false
OPENSEARCH_VERIFY_CERTS      true or false
OPENSEARCH_SOURCE_SCOPE      stable source profile identifier
OPENSEARCH_INDEX             explicit bounded index expression/profile selection
```

Optional existing settings: `OPENSEARCH_CA_BUNDLE`,
`OPENSEARCH_CONNECT_TIMEOUT_SECONDS` (5),
`OPENSEARCH_REQUEST_TIMEOUT_SECONDS` (15), and `AIOPS_POLICY_REGISTRY_PATH`.
Use the existing verified segmentation policy registry. Policy discovery is not
performed by the worker. The existing resolver still requires unique positive
evidence, including at least two supported headers in its bounded sample. Sparse
nonempty windows without that evidence fail safely with `POLICY`; empty windows
succeed without policy resolution. Provision/validate the source policy before
enabling a monitor. Namespace/workload discovery is not implemented by the
existing adapter, so the form uses exact administrator-supplied text fields.

One configured source profile is supported in this phase. The monitor's profile
must equal `OPENSEARCH_SOURCE_SCOPE`; the configured index belongs to that profile.
Do not repoint a profile to a different cluster/index generation and reuse its
monitor watermark. Create a new monitor/profile for a different source history.

## UTC daily-index profiles

A subsequent Citrix smoke established that `gocpbmgpup1*` includes historical
indices without an `@timestamp` mapping, while the relevant concrete daily index
passes pagination/replay checks. To avoid querying those unrelated indices,
configure this source profile once in the untracked repository `.env`:

```dotenv
OPENSEARCH_INDEX=gocpbmgpup1*
OPENSEARCH_INDEX_STRATEGY=daily_utc
```

Restart UI/worker after changing the profile settings. The default strategy is
`literal`, preserving existing aliases, literal indices and arbitrary wildcard
expressions for sources that have not declared a daily-index convention.
`daily_utc` explicitly declares `<base>*` → `<base>-YYYY.MM.DD`; it rejects other
pattern shapes rather than guessing. To manually target a literal historical
index instead, use `OPENSEARCH_INDEX_STRATEGY=literal` with that exact index.

The pure resolver is
`src/backend/ingestion_layer/opensearch_indices.py::resolve_index_expression`.
It returns a comma-separated list of concrete indices in ascending UTC date order.
It uses only the supplied acquisition bounds, never the machine timezone, clock
or monitor's message timestamp timezone. No index discovery request is made.

| Actual acquisition interval `[start,end)` | Suffixes selected |
| --- | --- |
| Oct 5 10:00Z–10:15Z | `2026.10.05` |
| Oct 5 23:55Z–Oct 6 00:10Z | `2026.10.05,2026.10.06` |
| Oct 5 23:45Z–Oct 6 00:00Z | `2026.10.05` only |
| Oct 5 23:59Z–Oct 6 00:16Z | `2026.10.05,2026.10.06` |

Monitoring supplies both its existing lookback and lookahead to the resolver.
Thus a logical Oct 6 00:00Z–00:15Z run with 60-second overlap retrieves
Oct 5 23:59Z–Oct 6 00:16Z and selects both days. Lookahead near midnight can also
require the next day even when the logical end is exactly midnight. Multi-day
bounded smoke intervals enumerate all touched dates; monitor catch-up still uses
the existing separate bounded windows, with no widened backlog query.

`OpenSearchSource.read_page` uses the resolved target for the HTTP search path
and cursor binding. Sort, `search_after`, page limits, timeouts, shard validation,
actual hit `_index`/`_id` source references, overlap/dedupe and at-least-once behavior
are unchanged. No `unmapped_type`, ignored shard failures or unavailable-index
override was added: a missing/broken relevant daily index still fails acquisition
and leaves the watermark unchanged.

Monitors/runs continue persisting their source profile, not today's date. Neither
SQLite schema nor existing monitor definitions changed for daily-index resolution.
Each successful result's `source_summary` includes `index_expression`, `index_strategy`, `resolved_index`,
`namespace`, `workload`, `container`, and the existing exact retrieval bounds.
The profile wildcard stays configured; tomorrow's target resolves automatically.

The manual smoke CLI calls `application_environment.load_environment()` before
configuration, using the same repository `.env` bootstrap as UI/worker. Connection
parsing is shared through `opensearch_connection.load_opensearch_connection`;
process variables retain precedence. No `runpy` or external `load_dotenv` wrapper
is needed. Smoke output includes the configured and resolved index targets, and
continues excluding credentials and raw messages.

For the real Citrix check, configure the two profile settings above, keep the
existing credentials in the secure environment/untracked `.env`, and run:

```powershell
Set-Location D:\Dev\hackathon\ao-hackathon-2026-teletabiler
$env:OPENSEARCH_INDEX = 'gocpbmgpup1*'
$env:OPENSEARCH_INDEX_STRATEGY = 'daily_utc'
$env:OPENSEARCH_SMOKE_END = '2026-10-05T21:30:00Z'
$env:OPENSEARCH_SMOKE_LOOKBACK_MINUTES = '15'
$env:OPENSEARCH_SMOKE_PAGE_SIZE = '3'
.venv\Scripts\python.exe -B tools\opensearch_smoke.py
```

Use the already validated exact `OPENSEARCH_SMOKE_NAMESPACE`,
`OPENSEARCH_SMOKE_WORKLOAD` and optional `OPENSEARCH_SMOKE_CONTAINER` settings;
adjust the explicit historical end to a known populated interval if needed.
Verify `resolved_index=gocpbmgpup1-2026.10.05`, PASS, stable page-1 replay and a
nonempty page 2. No unrelated July indices should appear in the target.

Then follow the one-disabled-monitor smoke procedure below, with the daily profile
strategy configured in both processes. Enable only that monitor and run:

```powershell
.venv\Scripts\python.exe -B tools\monitor_worker.py --once --max-runs 1
```

In its persisted investigation, open **Window, source policy and boundary
diagnostics**. Verify `resolved_index` against `retrieval_start`/`retrieval_end`,
including both overlaps, and verify namespace/workload/container against the
monitor definition. Check SUCCESS and the exact watermark advancement, then pause
the monitor. A failed relevant-index query must remain FAILED without advancement.
The automated regressions verify the actual HTTP path and all exact query filters
through fake transport; a successful live run's diagnostics use that same resolver.

Daily-index correction validation: 309 focused OpenSearch/monitoring/smoke/UI
tests passed, 273 timestamp/determinism/RCA tests passed, and the full regression
suite passed **930 tests** (896 baseline + 34 new). `git diff --check` and whitespace
checks for new files passed. No live OpenSearch calls were made during this
correction; the commands above are the Citrix validation procedure.

## Source identity and document cluster filtering

The real logical window `[2026-10-05T21:38:00Z,2026-10-05T21:53:00Z)` exposed a
separate acquisition bug after daily-index resolution was correct. The monitor's
logical alias `gocpbmgpup1` was passed from `MonitorDefinition.cluster_id` through
`OpenSearchMonitorExecutor.execute()` into `OpenSearchSource.read_page(cluster_id=...)`.
That argument emits a term on `openshift.cluster_id.keyword`, whose documents
contain a UUID, not the alias. The executor also compared returned stream UUIDs
to that same alias, so removing only the query filter would still reject records.

The supplied live diagnostic found 66 records with time, namespace, workload and
container filters, and zero after adding the alias as a document cluster filter.
Those counts describe that diagnostic only; no count is assumed by application
logic. A correctly scoped, complete empty query remains a valid SUCCESS.

Identity now has explicit meanings:

| Value | Meaning and behavior |
| --- | --- |
| `source_profile` / configured `source_scope` | Must match `OPENSEARCH_SOURCE_SCOPE`; identifies the configured connection/index family. Returned `SourceReference.source_scope` is checked against it. |
| `cluster_alias` | Logical cluster name. The legacy constructor/JSON field `cluster_id` is retained, exposed as this property and labeled **Logical cluster alias** in the form. It never creates a document filter. |
| Optional `document_cluster_id` | Explicit actual `openshift.cluster_id` value. Defaults to `None`; never inferred from the profile, alias, index or environment name. Only this field supplies `read_page(cluster_id=...)` and optional returned-UUID validation. |

The low-level source's existing `cluster_id` keyword remains compatible and has
document-level semantics. `StreamIdentity.source_scope` continues carrying the
document UUID, preserving pod/container/channel segmentation identity. No parser,
canonical event, template identity, timestamp, downstream or RCA contract changes.
The manual OpenSearch application and smoke tool do not pass the logical alias
as a document filter; their source-reference handling remains unchanged.

For the reported example with the default 60-second overlap, the exact selection is:

```text
source_scope: gocpbmgpup1
index_expression: gocpbmgpup1*
index_strategy: daily_utc
resolved_index: gocpbmgpup1-2026.10.05
@timestamp: gte 2026-10-05T21:37:00+00:00, lt 2026-10-05T21:54:00+00:00
kubernetes.namespace_name.keyword: ai-voice
kubernetes.labels.app.keyword: aihub-foya-stt-apis-http
kubernetes.container_name.keyword: aihub-foya-stt-apis-http
document_cluster_id: null (no openshift.cluster_id term)
```

With zero overlap, the same query uses the exact logical bounds 21:38–21:53Z.
The time range remains half-open. An explicitly configured document UUID adds
only `term openshift.cluster_id.keyword = <configured UUID>`; namespace, workload,
container, sorting, cursor binding, daily UTC dates and shard validation are unchanged.

Successful results persist `source_scope`, `cluster_alias`, `source_profile`,
`index_expression`, `resolved_index`, retrieval bounds, namespace, workload,
container and `document_cluster_id` (null when absent). The worker's ACQUIRED log
includes the effective scope from the redacted result through an explicit field
allowlist. It never logs connection objects, headers or raw queries. The workbench
shows **İndeks** as the resolved index when available, **Kaynak indeks deseni** as
the configured pattern, and separate retrieval bounds and document UUID status.
Older results lacking a resolved index continue showing their recorded pattern;
the UI does not invent a historical query target.

`MonitorDefinition` gains only the optional `document_cluster_id` JSON field.
Old definitions without that field read as `None`; their stored `cluster_id`
remains a logical alias. SQLite schema version stays 1; no database migration,
automatic history rewrite, watermark reset or learning-state reset occurs.
The new filter is part of the immutable scope after the first run, just like
namespace/workload. Changing it then requires a new monitor. Result persistence
still atomically commits receipts, SUCCESS and `last_successful_end=window.end`;
query, pipeline or persistence failure does not advance the watermark.

**The prior smoke SUCCESS for 21:38–21:53Z used an invalid filter and is not proof
of acquisition correctness.** Preserve it as history and use a **new disabled
smoke monitor** after this patch. Do not reuse its watermark or delete history.

## Domain and persistence

`monitoring/domain.py` defines immutable typed objects:

| Object | Fields |
| --- | --- |
| `MonitorDefinition` | name, source_profile, legacy cluster_id (logical cluster_alias), namespace, workload, optional container, initial_start, interval_seconds, window_seconds, ingestion_delay_seconds, overlap_seconds, source_timezone, page_size, max_pages, optional document_cluster_id |
| `DeploymentMonitor` | id, definition, enabled, status, last_successful_end, next_run_at, safe last error category/summary, created_at, updated_at, revision |
| `MonitorRun` | id, monitor_id, exact window, immutable definition snapshot, actual start/finish, created_at, status, claim token, attempt count, counts, result reference, safe error category/summary |
| `RunCounts` | retrieved/unique records, logical/parsed events, templated-event count, candidate/qualified signals, correlations, incidents, RCA count |

`template_count` currently counts successfully templated events, matching the
pipeline's `stats.templated`; it is not a count of newly learned patterns.
Runtime monitor states are `ACTIVE`, `PAUSED`, `RUNNING`, `ERROR`. Run states are
`RUNNING`, `SUCCESS`, `FAILED`. `enabled` is scheduling intent, distinct from
runtime status. Disabling preserves all configuration, history, results and
watermark. An already claimed run may finish; no further run is claimed while
paused. Re-enabling resumes persisted progress, including any failed window.

SQLite implements the `MonitorRepository` protocol (including the run/result
repository methods). Worker and UI do not issue SQL. Default storage:

```text
data/monitoring/monitors.sqlite3
```

Override with the same absolute `AIOPS_MONITOR_DB` in UI and worker. Mutable SQLite
files under `data/` are git-ignored. Tables are `monitors`, `monitor_runs`,
`monitor_results`, and `monitor_receipts`. Definition/count/result payloads are versioned by schema
version 1; scheduling/claim/watermark fields are indexed relational columns.
Initialization is transactional and additive. Unknown versions and databases
containing unrelated tables are rejected. Existing policy/template databases
are never migrated or reset. SQLite foreign keys and `BEGIN IMMEDIATE` protect
transactions; connections are operation-scoped, not cached Streamlit resources.

The database enforces `UNIQUE(monitor_id, window_start, window_end)` and one
result per run. Success atomically stores presentation result, reference
receipts, run counts/status, and the new watermark. A failed transaction stores
none of them. Retries reuse the same run/result identity and increment attempts;
the latest attempt's timing/error replaces its previous attempt metadata.
History retains windows, not a separate audit row for every failed attempt.

Optimistic revisions reject stale definition edits. After the first run, only
name and scheduler interval are editable; changing source identity, timezone,
window or acquisition policy requires a new monitor. This prevents reinterpreting
old windows or mixing evidence from different scopes under one watermark.

## Scheduling, recovery and idempotency

All instants are timezone-aware and persisted in UTC. Scheduler clocks are
injected into `MonitorWorker`; no scheduling clock enters event occurrence time.

1. A new monitor has explicit `initial_start` and no successful watermark.
2. Next start is `last_successful_end`, or `initial_start` before first success.
3. Next end is start plus `window_seconds`. Every logical window is **[start,end)**.
4. A run is eligible only when enabled, `next_run_at <= now`, unclaimed, and its
   entire retrieval end is stable under ingestion delay.
5. Watermark advances only in the success transaction, to the exact logical end.
6. A ready backlog is processed promptly as multiple sequential bounded runs.
   A tick defaults to one run; `--max-runs` bounds each tick to at most 100.
7. With no ready backlog, the next due time is the later of the next safe boundary
   and completion plus `interval_seconds`. Interval controls scheduler cadence;
   it does not widen windows, create gaps or replace the acquisition watermark.
8. Failure retries after the interval. It never moves the watermark to now.

For a 900-second window, start 12:00, 60-second delay and 60-second overlap:

```text
logical window:   [12:00,12:15)
retrieval window: [11:59,12:16)
earliest run:     12:17
next logical:    [12:15,12:30)
```

The added lookahead also respects `retrieval_end <= now - ingestion_delay`.
Restart/re-enable at 12:47 with watermark 12:00 processes 12:00–12:15,
12:15–12:30, and 12:30–12:45 sequentially. It never substitutes "last 15 minutes".

Claims are durable and transactional, with attempt tokens fencing stale
completion writes. The CLI holds an OS file lock for its lifetime, on the same
monitoring storage, before recovering `RUNNING` rows as interrupted attempts.
The OS releases the lock after crashes. Do not delete the lock file: all
contenders must lock the same file. Direct callers of `recover_running()` must
establish exclusive worker ownership first; the UI never calls it.

**Current supported mode: one worker instance.** This is not an HA/distributed
lease design. SQLite and advisory locks need a filesystem with correct locking;
multiple hosts/pods, network filesystems, or alternative entrypoints can defeat
these assumptions. PostgreSQL transactional claims, expiring leases/heartbeats
and fencing belong at the repository/worker ownership boundary in a future phase.

Acquisition is **at least once**. Durable success/result/watermark publication is
idempotent per window, but a crash after pipeline execution and before commit can
repeat computation and expert AI calls. Template learning is not rolled back by
the monitoring transaction. No exactly-once delivery or exactly-once external
side-effect claim is made.

## Multiline ownership, overlap and limits

The worker queries a configurable overlap on **both** sides. Lookback reconstructs
events that started before a window; lookahead can complete events that started
near its end. The existing `SegmentationSession` assembles original records,
preserving pod instance/container instance/iostream separation across all pages.
No session or raw assembly buffer survives a run.

Within retrieval, `(source scope, concrete _index, _id)` is hashed to deduplicate
physical records before assembly; document version changes do not create a new
line. Record order remains the original `@timestamp ASC`,
`openshift.sequence ASC` order. Only an explicitly configured document cluster ID
adds a filter on `openshift.cluster_id.keyword`; a logical source alias never
does. Namespace/workload/container filters keep their existing mappings.
Pod names never define a monitor.

The optional source-neutral `assembled_event_filter` in the authoritative pipeline
applies **after assembly and before parsing/template learning/downstream**. Without
it the existing finite-page behavior is unchanged. Monitoring assigns each event
to the logical window containing the absolute source timestamp of its first
**included** contributor, regardless of the event's message timestamp. Context
events outside the window are not analyzed. Consumed contributor hashes are
committed with success and checked before analysis, preventing duplicate overlap
events/signals. Receipts older than the next overlap horizon are pruned; window
history and results remain durable.

There is no universal guarantee of complete assembly for arbitrary-length events.
An owned `analysis_end` tail retains the current finite-pipeline flush semantics
and increments `boundary_tail_events`: the cut is **not proof of producer EOF**.
A leading continuation with no header is excluded and counted in `orphan_events`.
An event containing previously consumed contributors is not analyzed again; any
new contributors in that reconstruction increment `overlap_conflicts`. Very long
traces spanning more than the overlap can therefore have incomplete tail evidence.
All three conditions are persisted; run inspection distinguishes possible
incompleteness from excluded orphans and overlap conflicts.
Use a suitable overlap for the source; durable assembler checkpoints or explicit
late-event correction would require a later design, not silent re-counting.

At most 500 records/page, 20 pages/run, and 24 hours including both overlaps are
accepted. The worker retains at most that finite acquisition batch before pipeline
execution, so a partial acquisition never trains/templates or publishes a partial
successful window. A full final page without exhaustion fails `ACQUISITION_LIMIT`
(even if a subsequent empty page would have proved completion). Missing/repeated
cursors, timeout, shard failure and malformed pages fail safely. The adapter's
cursor validation, transport timeouts and query ordering remain in force.

These are **record/window bounds**, not a byte bound on individual messages or an
unbounded streaming redesign. Database history/results need an operational retention
policy as volume grows. OpenSearch still has no PIT/snapshot or late-arrival
guarantee. Records indexed after the delay/horizon are not retrospectively added to
successful windows. Index-generation reuse and nonunique cross-index sort keys
remain source-profile constraints from the existing adapter.

## Boundary completeness and future eligibility

The former broad warning came from `monitoring/execution.py::WindowOwnership`:
owned `analysis_end` events incremented `window_assembly.boundary_tail_events`.
`monitoring_views.py` displayed the same warning whenever that counter,
`orphan_events`, or `overlap_conflicts` was nonzero. None of those counters alone
proved truncation. The real run with 378 physical records / 198 logical events
and completed requests before the 06:39 retrieval boundary provides no positive
evidence of a truncated event; this patch does not classify it as confirmed loss.

`segmentation_layer/contracts.py` adds the typed `BoundaryStatus` literal and an
immutable derived `AssembledEvent.boundary_status` property. Existing emission
reasons remain unchanged:

| Existing emission reason | Boundary status | Evidence |
| --- | --- | --- |
| `next_header` | `complete` | The next valid header in the same stream closes this event. |
| `analysis_end` | `possible_incomplete` | The bounded analysis ended; producer completion is unproven. |
| `explicit_stream_close` | `possible_incomplete` | The existing contract defines a lifecycle cut, not authoritative producer EOF. |

`confirmed_truncated` is reserved for positive continuation evidence beyond a
boundary. No current emission produces it; cross-run continuation evidence is
not available. No completion is inferred from an HTTP success message or elapsed
time. No truncation is inferred from search exhaustion or an analysis-end tail.

**A page boundary is not an event boundary.** `feed_page()` continues pending
state across `search_after` pages, including empty pages and all page/cycle limit
flags. Only existing header/explicit-close/analysis-end behavior emits events.
Repeated close remains empty. Overlap can reduce boundary risk but cannot prove
completion, and increasing it only moves the final retrieval cut. The existing
60-second default overlap, logical windows, scheduling, dedupe, watermark rules
and OpenSearch queries are unchanged.

Propagation is additive:

```text
AssembledEvent.boundary_status
  -> FullAIOpsPipelineV2._assembled_provenance
  -> canonical attributes.source_provenance + result.event_provenance
  -> monitoring.boundary_quality.build_boundary_quality
  -> redacted source_summary.boundary_quality
  -> persisted MonitorRun Investigation
```

Status attaches after the canonical builder assigns event identity. The monitor
projection covers only selected/owned logical events, including a parser-None
output if one occurs. Context-only, duplicate and excluded orphan events do not
inflate the denominator. The envelope retains:

- exact `total_events`, counts for all three statuses, and `analysis_end_events`;
- compact `event_statuses` for **every** owned event, including complete events;
- up to **200** `affected_events`, matching the existing diagnostic sample budget.

Each compact entry has a one-based `event_ordinal`, canonical `event_id` (possibly
None), and status. Use run ID plus ordinal to identify an occurrence: event ID
alone need not be unique. The detail sample must never substitute for the full
status contract when making future eligibility decisions.

The metadata-only table shows ordinal/ID, status, emission reason, a stream hash,
pod/container **instance IDs**, channel, first included and last source-record
times, physical contributor count (including retained blank provenance), and
hashed first/last source references. It does not copy raw text, arbitrary metadata,
raw timestamp captures, headers, cursors, or sensitive request payloads. Source
times use the existing resolver for display without changing timestamp authority.
The existing OpenSearch credential/text redaction and `SAKA_API_KEY` redaction run
before persistence; display redacts strings again. The general JSON view excludes
the per-event envelope so it cannot bypass the 200-row table limit. Detailed rows
are not added to worker logs. Per-event status storage remains proportional to
the existing finite, bounded monitoring batch; no cross-run assembly state is added.

UI text now reports actual counts, for example:

```text
Boundary completeness: X of 198 logical events are marked possible_incomplete
(X emitted at analysis end). Confirmed truncation: 0 recorded.
```

`X` comes from event metadata. Possible incompleteness uses a caution; positive
confirmed counts use an error-style diagnostic. Zero possible/confirmed events
produce a neutral caption, not a warning. Orphans and overlap conflicts have a
separate informational message and are not called confirmed truncation.

`boundary_allows_baseline_evidence(status)` in the segmentation contract is a
**future boundary-only guard**: only `complete` returns true, meaning potentially
eligible if every other future condition is satisfied. `possible_incomplete`,
`confirmed_truncated`, missing and unknown status return false for future
deployment baseline learning and NEW/RARE evidence. No baseline, NEW_PATTERN,
RARE_PATTERN or FREQUENCY_ANOMALY feature is implemented. The helper is not wired
into current template learning, qualification, correlation, incidents or RCA;
possibly incomplete events retain their current analysis and Investigation visibility.

Existing stored runs are not rewritten. Reopening an older run uses its recorded
`boundary_tail_events` count as the count of possible analysis-end tails and shows
zero **recorded** confirmed truncations. A caption explains that event-level
details were not stored. An old result without even that counter shows quality as
unrecorded. No event IDs/status lists are invented for historical results, and
missing historical status must not pass the future eligibility guard. SQLite
schema, learned templates, parser policies and policy signatures need no migration.

To inspect the reported run in Citrix:

1. Restart the Streamlit process with this patch and the same `AIOPS_MONITOR_DB`.
   No worker execution is needed merely to reopen history.
2. Open **Deployment Monitors**, select the existing aihub-foya-stt monitor, and
   choose the successful run around `[2026-10-06T06:23:00Z,06:38:00Z)` under
   **Open investigation**. Confirm the stored 198 logical / 378 physical counts.
3. Read the quantified boundary message. Its possible count must match the
   recorded `boundary_tail_events` under **Window, source policy and boundary
   diagnostics**. Expect zero recorded confirmed truncations and the historical
   metadata limitation caption; do not infer an actual truncated request.
4. For event-level details, allow the same monitor's next normal bounded run with
   the patched worker (`.venv\Scripts\python.exe -B tools\monitor_worker.py --once --max-runs 1`
   when enabled and due). Keep the existing overlap/window/filter settings.
   Refresh, select the new SUCCESS, and inspect **Boundary-affected events
   (metadata only)**. Verify counts against the run, safe stream/event identifiers,
   source times and the sample limit. If no events are affected, expect no caution.
5. Confirm the prior run/history is unchanged and the new run follows the existing
   watermark. Do not reset or replay the old window just to populate diagnostics.

## Timestamp and learning state compatibility

Every monitor has its own `source_timezone`, default **Unknown / None**. UTC must
be configured explicitly for a validated UTC source. The pipeline creates the
existing immutable `TimestampSourcePolicy` for each invocation. Authority remains:

```text
explicit message event time
  > naive message event time resolved by source policy
  > absolute OpenSearch source-record timestamp
  > untimed
```

Raw OpenSearch `@timestamp` is resolved using the existing timestamp resolver
without a timezone assumption or manual +3-hour shift. The record itself is
unchanged. Parser fallback still belongs to the first included contributor of
**each event**, not the first page/query record. No parser schema, policy signature,
template identity algorithm, signal threshold, correlation score, incident grouping,
RCA rank, expert schema or Evidence Pack is changed.

The UI keeps its existing cached manual pipeline and learning lifecycle. The worker
has its own persistent template registry and Drain files under
`<AIOPS_DATA_DIR>/monitoring/learning/` (override `AIOPS_MONITOR_LEARNING_DIR`). It keeps
one pipeline instance across runs. This new execution plane starts a distinct
learning history; it does not erase or silently overwrite the manual pipeline's
learned state. Learned IDs/matches depend on the relevant persisted history, as
before. An operator who needs the existing learned history can seed **both** files
from a consistent stopped snapshot into the worker-owned directory before its first
run. Do not point the worker and cached manual pipeline at the same mutable files.
There is no automatic migration/invalidation of prior learning.

## UI, errors and observability

Overview introduces a primary Deployment Monitors entry; navigation includes the
monitor list. Create/edit, disabled-by-default creation, pause/enable, exact
watermark and timezone, run history, and successful-run drill-down are available.
The workbench uses `InvestigationPresenter` and existing investigation/signal/
incident/RCA views. Refresh reads persisted state; no browser session is required
for execution. The existing upload and ad-hoc OpenShift flows remain under
**Manual Investigation / Test / Smoke** for compatibility.

Worker output is allowlisted structured `[MONITOR]` JSON: START, ACQUIRED, PIPELINE,
SUCCESS, FAILED, with opaque IDs, exact windows, redacted effective acquisition
scope, integer counts, duration and safe categories. Error summaries are fixed
text, never exception strings. Categories include configuration, auth, timeout,
query, acquisition limit, policy, pipeline,
persistence and interrupted/unknown failures.

Legacy pipeline stdout/stderr/logging (including opt-in RCA prompt debug output)
is suppressed in the unattended worker. The manual pipeline's logging behavior is
unchanged. Results reuse the existing presentation projection: raw traces, arbitrary
metadata, raw provenance and cursor values are excluded; known OpenSearch secrets,
credential-shaped text and configured `SAKA_API_KEY` are redacted. No connection
objects, headers or credentials are stored in SQLite. Results still contain
operational evidence: protect the monitoring database with normal access controls.
The schema is not encrypted and this phase adds no multi-user authorization layer.

## Local execution and Citrix smoke

Run from the repository root. Use two separate PowerShell terminals. Configure
connection secrets through the existing secure environment or untracked `.env`;
do not paste credentials into terminal transcripts, monitor names or source profile
fields. Confirm the existing verified segmentation registry covers the source.

In **both terminals**, choose the same durable monitoring database:

```powershell
Set-Location D:\Dev\hackathon\ao-hackathon-2026-teletabiler
$env:AIOPS_MONITOR_DB = Join-Path $PWD 'data\monitoring\monitors.sqlite3'
```

Terminal 1, UI:

```powershell
.venv\Scripts\python.exe -B -m streamlit run src/frontend/streamlit_app.py
```

1. Open **Deployment Monitors**. Verify "Source configuration loaded". This only
   checks configuration; a later worker run checks live access.
2. Pause other smoke monitors and create **one NEW disabled monitor**. For the
   reported source, set **Source profile / scope** and **Logical cluster alias**
   to `gocpbmgpup1`, namespace `ai-voice`, and both workload and container to
   `aihub-foya-stt-apis-http`. Leave **Document OpenShift cluster UUID** blank unless
   the actual document UUID is independently configured. Use interval/window
   **900 seconds**, delay **60 seconds**, overlap **60 seconds**, page size **100**,
   maximum pages **20**. Leave source timezone blank (**Unknown**) unless this
   workload's message timezone is independently verified. Configure
   `OPENSEARCH_SOURCE_SCOPE=gocpbmgpup1`, `OPENSEARCH_INDEX=gocpbmgpup1*` and
   `OPENSEARCH_INDEX_STRATEGY=daily_utc` in both processes.
3. Choose an explicit initial start covering one known safe 15-minute test period.
   Its end plus overlap must be at least the delay behind current time. The form
   proposes a recent timestamp, but verify it against actual data availability.
   Record this exact start/end and configured timezone. Leave Enabled unchecked.

Terminal 2, one bounded worker tick:

```powershell
.venv\Scripts\python.exe -B tools/monitor_worker.py --once --max-runs 1
```

4. Refresh UI: the monitor is PAUSED, no run exists, watermark is None. Disabled
   monitors do not even construct the pipeline/acquisition client.
5. Enable the monitor in UI. Run the **same one-tick command** again. Do not start
   an unlimited loop for this smoke. Observe one START and one SUCCESS, or a safe
   FAILED category. A missing policy/limit/auth failure must leave the watermark
   unchanged; correct configuration before retrying the same window.
6. Refresh. Inspect MonitorRun logical `[start,end)`, actual times, counts, attempt,
   status, and persisted investigation. Open **Window, source policy and boundary
   diagnostics**: confirm retrieval overlap, explicit timezone (including None if
   intended), verified policy selection, and any boundary diagnostics. For the
   known active interval, require **ACQUIRED records > 0**, pipeline execution and
   MonitorRun SUCCESS. Check `source_scope=gocpbmgpup1`, the correct resolved daily
   index, namespace/workload/container above, and `document_cluster_id=null`.
   There must be no alias-based `openshift.cluster_id=gocpbmgpup1` filter. Technical
   details must show the resolved index alongside the base pattern. Confirm
   `last_successful_end` equals exactly the successful logical end.
7. Restart by invoking the one-tick command in a fresh worker process. If the next
   window is not safe/due, expect no new run. Once it is safe/due, another one-tick
   invocation must start at the persisted end, never at a fresh relative lookback.
   If a known historical backlog was chosen, the restarted process processes only
   one subsequent 15-minute window. Verify the first success/result was not duplicated.
8. Pause the monitor. Invoke the one-tick command again and refresh: no new runs;
   watermark/history/results remain intact. Leave the smoke monitor paused.

After operational approval, continuous mode is the same separate process:

```powershell
.venv\Scripts\python.exe -B tools/monitor_worker.py --poll-seconds 10 --max-runs 1
```

For a later OpenShift deployment: run the worker command as a separate deployment
with **replicas=1**, process environment supplied by ConfigMaps/Secrets, durable
monitoring/worker-learning storage, and the verified-policy store. UI and worker
must see the same monitoring database. Validate filesystem locking semantics first;
do not scale SQLite/advisory-lock workers across pods and call it HA. PostgreSQL
repository/leases are the intended migration path.

## Compact monitoring product

The normal application opens at **Deployment Monitors**. Its list uses one compact
monitor-summary SELECT and holds the result in the Streamlit session until an
explicit Refresh or monitor edit. Opening a monitor loads its latest ten compact
runs and, when present, one latest finding. Older runs load only on request.
The ordinary detail view does not load Investigation or Pipeline Trace JSON.

The monitoring database schema is version 5. Normal worker execution stores a
durable claim before acquisition. After successful processing, one short
transaction publishes compact run counters, the monitor's latest projection and
watermark, a bounded baseline ring, up to ten actionable findings, and only the
source-reference receipts still relevant to retrieval overlap. The baseline
ring preserves the last 30 successful metric windows and five pattern windows;
failed windows do not enter it. New compact runs do not write full result JSON,
per-window metric/pattern rows, or successful shard records. Existing version 4
result and trace rows remain readable until retention removes them.

Reference deduplication within a run is exact. It starts in memory with a
100,000-reference ceiling and spills to a temporary run-local SQLite file under
`<AIOPS_DATA_DIR>/monitoring/tmp` if needed. The shared `monitors.sqlite3` does
not receive per-record run-dedupe writes. Terminal runs remove the temporary
file; the worker removes stale crash leftovers during maintenance. Recent
overlap receipts remain in the shared database and publish atomically with the
successful watermark. Failed windows replay from their beginning.

**Debug Pipeline** is an explicit action within a monitor. Representative Debug
reads at most two pages of up to 200 source records each and displays at most ten
deterministic same-stream cases with up to 20 neighboring records in each
direction. Exact Window Replay processes the selected logical window using the
normal retrieval overlap and exact-window metrics. Both use a disposable
read-only learning snapshot, make no monitoring repository writes, and retain
their trace only in the Streamlit session. Source retention or changed learning
state can make replay differ from an old production result.

The worker runs bounded terminal-history cleanup about hourly between ticks and
low-frequency `PRAGMA optimize` and WAL `PASSIVE` checkpoint about daily. Default
retention is seven days for compact run rows, 30 days for findings, and three
days for detailed failure/shard diagnostics. Definitions, watermarks, baseline
state, and receipts still needed by overlap are protected. No full automatic
`VACUUM` is run. Database size reporting includes the database, WAL, and SHM
files, excluding separate policy and template learning stores.

SQLite uses a single worker and short writer transactions. UI reads use
`query_only` connections without `BEGIN IMMEDIATE`; normal UI repository
construction performs no DDL. WAL should live on a validated local or RWO
filesystem available to one worker and the UI, preferably in one pod. A shared
RWX/NFS SQLite file with multiple writer pods is unsupported; independent pods
or horizontal writers require a transactional shared database such as PostgreSQL.

At a five-minute interval, one monitor creates 288 compact run rows per day,
bounded to roughly 2,016 rows by seven-day retention, plus zero to ten finding
rows per run and a single updated baseline row. Normal success has three fixed
state writes (run, monitor, baseline), optional bounded finding/receipt inserts,
and an overlap-receipt prune. A synthetic no-finding/no-receipt success is
measured at four SQLite write statements and one final transaction. A separate durable claim transaction remains
necessary for restart recovery and owner fencing.

Transient OpenSearch page/count failures use at most three attempts with short
bounded backoff. Repeated search timeout, shard failure, HTTP 429, and HTTP 5xx
pressure can split a shard into exact half-open children. Invalid cursors,
malformed responses, incompatible mappings, bad timestamps/sequence values,
and authentication failures fail immediately with safe reason codes. Three
repeated non-retryable failures of the same window/reason mark the monitor
**BLOCKED** and delay its next attempt by at least one hour. The same window
remains pending and its watermark does not advance. The UI displays only the
stable reason, attempts, and blocked window. A successful retry clears BLOCKED.

## Validation and next phase

Boundary-completeness validation: five regression cases first failed for missing
classification/projection and the broad warning. After the additive patch,
**255 focused boundary/session/pipeline/monitoring/UI tests** and **273
timestamp/determinism/RCA tests** passed. The full offline regression suite passed
**968 tests** (951 baseline + 17 new cases). `git diff --check` and the separate
no-index whitespace check for the new boundary projection module passed. No real
OpenSearch/LLM calls or live Citrix smoke were performed for this patch; use the
history inspection procedure above. Nothing was staged, committed or pushed.

Source-identity correction validation: the alias filter, returned-UUID rejection
and incorrect technical index display were first reproduced as three failing
regressions. After the correction, **393 focused acquisition/monitoring/UI tests**,
**273 timestamp/determinism/RCA tests**, and **951 full regression tests** passed
(930 baseline + 21 added cases). Automated tests use no real OpenSearch/LLM calls.
`git diff --check` passed. The real Citrix smoke remains an operator validation
step using the new-monitor procedure above; no live result is claimed here.

Offline tests use temporary SQLite, injected clocks, fake OpenSearch transport and
pipeline dependencies. Tests block network/LLM and production learning state.
Coverage includes atomic rollback, interrupted owner fencing, retry idempotency,
backlog/delay/window boundaries, source-timezone propagation, per-event fallback,
overlap/multistream dedupe, real downstream signal counts, bounded pages/receipts,
sanitized logging/results, OS worker lock, CLI once mode, and real Streamlit controls.
Existing upload, timestamp, determinism, RCA and ingestion regressions also run.

Initial deployment-monitoring validation (before the daily-index correction):

| Selection | Result |
| --- | --- |
| New monitoring backend and real Streamlit monitoring tests | 44 passed |
| Existing timestamp, determinism, RCA, ingestion/OpenSearch, UI and boundary selection | 548 passed |
| Full `python -B -m pytest -q -p no:cacheprovider tests/regression` | 896 passed (852 baseline + 44 new) |
| `git diff --check`, plus no-index whitespace checks for new files | Passed |

The compatibility selection includes all 27 downstream determinism tests, both
timestamp authority/provenance suites and all three focused RCA suites. Full-suite
validation exposed an existing import test that globally reloaded ingestion
contracts and left old enum references in earlier imports. That test now executes
an isolated module, preserving its no-I/O assertions without replacing production
type checks. The existing navigation assertion now includes Deployment Monitors.
No real OpenSearch/LLM smoke was executed locally; use the controlled Citrix steps
above. Nothing was staged, committed or pushed.

Next-phase hooks are stable monitor identity, immutable run configuration, exact
windows, result references, pattern/signal counts and durable success publication.
A future versioned deployment baseline can consume only committed successful runs
and record its own watermark/version to support NEW PATTERN, RARE PATTERN and
FREQUENCY ANOMALY. None of those detectors, ML, embeddings or remediation are
implemented here.
