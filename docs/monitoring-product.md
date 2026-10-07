# Deployment monitoring product

Creation asks for Namespace and Workload, discovered from the default source
over a bounded 24-hour window. Workload options depend on Namespace. Deployment
identity uses `kubernetes.namespace_name` and `kubernetes.labels.app`; pod and
container instance identifiers never define a monitor target.

After selection, a lightweight 15-minute search shows the exact count, latest
source timestamp and one redacted application message. No parser, learning,
analysis or LLM executes. A discovered target with zero recent logs is quiet and
can be monitored. Query, mapping and shard failures block creation with an
explicit validation error. Discovery lists at most 200 values at each level,
and makes truncation explicit; targets absent for more than 24 hours may not
appear. Samples are redacted before their 2,000-character display bound.

Defaults are an automatically generated workload name, active monitoring,
15-minute scheduling and analysis windows, start from now and all containers.
Run every offers 5/15/30 minutes and 1 hour. Advanced settings contain start
mode, historical UTC date/time and an optional container selector. Selecting a
container immediately refreshes validation. The current runtime supports one
configured source, so there is no source-profile selector. Acquisition limits,
overlap, ingestion delay and timezone defaults remain backend responsibilities.

Cards separate lifecycle, execution and successful analysis outcome. HEALTHY
means the last successful analysis had no qualified signals or incidents;
ATTENTION means it did. Failure remains FAILED, and log flow is unknown after
failure. Flow indicators describe the last acquisition window, including
bounded overlap, rather than a live check.

Details use Overview, Findings, Runs and Settings. Findings summarize persisted
qualified signals and incidents. Investigate opens the relevant persisted run.
Runs are newest first; Investigation and Pipeline Trace live inside selected-run
details. Opening results never acquires logs or executes analysis.

Settings lock targets after any run history exists. Name and scheduling interval
remain editable under existing repository rules; changing the interval after
history preserves the established analysis window and watermark. Cloning creates
a new monitor identity with a newly validated target and no inherited history.
Pause and archive preserve audit evidence.

## Exact acquisition fields and diagnostics

The old default mapping assumes `.keyword` subfields. On indices where the base
Kubernetes fields are already keywords and those subfields do not exist, exact
term queries silently return zero. A deterministic regression reproduces this
with the supplied aida target, document timestamp and logical window.

Monitoring now resolves field capabilities over the acquisition's UTC-resolved
indices before querying. A valid configured keyword field is preserved; otherwise
only a verified keyword base field is used. Missing, analyzed, conflicting or
nonsearchable mappings fail explicitly. Namespace/workload filters stay exact,
container remains optional, and document cluster filtering requires an explicit
document UUID. Timestamp/sequence ordering and acquisition bounds are unchanged.
This fixture establishes the mapping defect; confirming that it is the current
live source's root cause requires access to that source's mapping.

Successful results persist source, resolved indices, logical/acquisition windows,
effective filters, cluster-filter activation, mapping verification, sort fields,
record/page counts and acquisition-limit status. Values use existing redaction;
credentials, transport headers and cursors are omitted. Selected-run technical
details display this evidence, including Pipeline Trace.

SQLite schema v3 adds nullable `monitor_runs.acquisition_diagnostics`. Existing
records default to NULL, and existing results and audit evidence are untouched.
New failed attempts persist the same bounded redacted acquisition snapshot in
the failure transaction, without creating a successful result or advancing the
watermark. This column describes the latest failed attempt of the logical run;
it resets when that same logical run is retried, like the existing error fields.
Historical runs without the snapshot explicitly state that acquisition details
were not stored. No production-like database is manually reset.
