# Deployment monitoring

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

## Domain and persistence

`monitoring/domain.py` defines immutable typed objects:

| Object | Fields |
| --- | --- |
| `MonitorDefinition` | name, source_profile, cluster_id, namespace, workload, optional container, initial_start, interval_seconds, window_seconds, ingestion_delay_seconds, overlap_seconds, source_timezone, page_size, max_pages |
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
src/data/monitoring/monitors.sqlite3
```

Override with the same absolute `AIOPS_MONITOR_DB` in UI and worker. The directory
is git-ignored. Tables are `monitors`, `monitor_runs`, `monitor_results`, and
`monitor_receipts`. Definition/count/result payloads are versioned by schema
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
`openshift.sequence ASC` order. The same exact mapping gains only an optional
cluster filter (`openshift.cluster_id.keyword`); namespace/workload/container
filters keep their existing mappings. Pod names never define a monitor.

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
All three conditions are persisted and prominently warned about in run inspection.
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
`<monitor-db-directory>/learning/` (override `AIOPS_MONITOR_LEARNING_DIR`). It keeps
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
SUCCESS, FAILED, with opaque IDs, exact windows, integer counts, duration and safe
categories. Error summaries are fixed text, never exception strings. Categories
include configuration, auth, timeout, query, acquisition limit, policy, pipeline,
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
$env:AIOPS_MONITOR_DB = Join-Path $PWD 'src\data\monitoring\monitors.sqlite3'
```

Terminal 1, UI:

```powershell
.venv\Scripts\python.exe -B -m streamlit run src/frontend/streamlit_app.py
```

1. Open **Deployment Monitors**. Verify "Source configuration loaded". This only
   checks configuration; a later worker run checks live access.
2. Create **one disabled monitor**. Use the configured source profile, validated
   cluster, exact namespace and workload (e.g. the validated gateway workload),
   and optional container. Use interval/window **900 seconds**, delay **60 seconds**,
   overlap **60 seconds**, page size **100**, maximum pages **20**. Set **UTC** only
   if this source's naive message timestamps were validated as UTC.
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
   intended), verified policy selection, and any boundary diagnostics. Confirm
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

## Validation and next phase

Offline tests use temporary SQLite, injected clocks, fake OpenSearch transport and
pipeline dependencies. Tests block network/LLM and production learning state.
Coverage includes atomic rollback, interrupted owner fencing, retry idempotency,
backlog/delay/window boundaries, source-timezone propagation, per-event fallback,
overlap/multistream dedupe, real downstream signal counts, bounded pages/receipts,
sanitized logging/results, OS worker lock, CLI once mode, and real Streamlit controls.
Existing upload, timestamp, determinism, RCA and ingestion regressions also run.

Local validation for this implementation:

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
