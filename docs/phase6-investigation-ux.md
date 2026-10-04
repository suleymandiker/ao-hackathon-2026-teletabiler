# Phase 6 — AI-IN-AI Investigation UX

The frontend now follows Overview → Investigation result → Pipeline details →
Patterns/evidence → Incident/RCA. The implementation uses the existing finite
analysis pipeline. It does not introduce new backend decisions or services.

## A. Changed files

| File | Responsibility |
| --- | --- |
| `.streamlit/config.toml` | Light investigation canvas and dark navigation; existing server/upload settings retained. |
| `src/frontend/streamlit_app.py` | Navigation, forms, synchronous execution, source ownership and safe session result. |
| `src/frontend/analysis_runtime.py` (new) | Existing upload conversions, temporary-file cleanup, cached pipeline and shared analysis lock. |
| `src/frontend/presentation.py` (new) | Pure deterministic presenter and immutable, allowlisted view models. |
| `src/frontend/investigation_views.py` (new) | Result, stage, pattern, incident, topology and evidence rendering. |
| `src/frontend/theme.py` | Centralized restrained CSS and HTML-escaping primitives. |
| `src/backend/opensearch_application.py` | Optional automatic verified-policy selection and safe source diagnostics. |
| `src/backend/verified_policy_resolver.py` (new) | Bounded, deterministic compatibility evaluation of existing verified snapshots. |
| `tests/regression/test_streamlit_sources.py` | AppTest navigation, source isolation, rendering and security regressions. |
| `tests/regression/test_investigation_presentation.py` (new) | Truthful counts, identities, timestamps, lineage, redaction and bounded evidence. |
| `tests/regression/test_verified_policy_resolver.py` (new) | Compatibility/failure guards, unchanged page replay and read-only registry behavior. |
| `docs/phase6-investigation-ux.md` (new) | Design, compatibility and validation report. |

The supplied `docs/ui-reference/aiops-investigation-mockup.png` and
`src/frontend/assets/ai-in-ai-logo.png` were already untracked at the start.
Their bytes are preserved. No other pre-existing working-tree edits were present.
No files were staged, committed or pushed.

## B–F. Information architecture and mockup interpretation

The mockup supplies hierarchy, density, spacing and navigation direction. Backend
contracts supply every operational value. The design uses a compact dark sidebar,
wide light canvas, blue primary action, restrained borders, and an eight-stage
horizontal explanation. The exact logo appears at 161 px width with its original
background and aspect ratio. No replacement artwork or large hero was created.

Classification: **A** directly supported; **B** safely derived or adapted in
presentation; **C** unavailable and omitted/deferred.

| Mockup element | Class | Implementation or adaptation |
| --- | --- | --- |
| Brand/logo | A | Exact supplied AI-IN-AI PNG; separate Operations label. |
| Compact sidebar and page context | B | Overview, Investigations, Log Patterns, Incidents; no engine-stage navigation. |
| Overview task cards and CTAs | A | OpenShift investigation and existing file/package analysis. |
| Recent analyses/history table, time, “view all” | C | No persisted history. Only the current session's last successful result can be reopened; no invented timestamp. |
| “OpenShift connected” indicator | B | Configuration readiness only, explicitly worded as configured/unconfigured; no connection-health claim. |
| Avatar, Settings, Help placeholders | C | Omitted without supported account/settings/help workflows. |
| Source, namespace, workload, time interval | A/B | Existing request fields; namespace/workload use text because inventory options are unavailable; 15-minute default. |
| Advanced container, end time and limits | A | Existing optional filters and bounded acquisition, expressed as a record limit. |
| Automatic format handling | B | Verified-policy resolver at the application boundary; no manual selector or regex in normal UI. |
| Processing state | A/B | Generic synchronous processing status, then actual returned results. |
| Live stage checkmarks, counters and waiting states | C | No progress callbacks; no simulated stage completion. |
| Stop/cancel action | C | No cancellation contract; omitted. |
| Result summary and count chain | A/B | Existing counts, derived unique patterns, actual incident candidates and qualification outcomes. |
| Horizontal pipeline and semantic colors | B | Eight inspectable stages derived from counts; suppression is attention and zero output is not failure. |
| Main indicators / most frequent pattern | B | Existing totals and occurrence sums grouped by unchanged template ID. |
| “Why no incident?” reason breakdown | A/B | Existing suppression reasons only. No invented threshold, decay or severity cause distribution. |
| Analysis timestamp / share button | C | No fabricated completion time or share workflow; the actual fixed source interval is in technical details. |
| Pattern table, count, severity, result | A/B | Existing template text and IDs, summed counts, backend qualification/incident membership. |
| Pattern trend arrows / historical comparison | C | No trend data; omitted. |
| Pattern selector and detail panel | B | Supported Streamlit selector and bordered detail with progressive disclosure. |
| First/last seen | A/B | Source-derived aggregation timestamps, only when every grouped signal has resolved time. UTC is explicit. |
| Pods/streams in pattern detail | A/C | Actual services and hosts when supplied; no inferred pod inventory or stream lineage. |
| Pattern pipeline lineage and “why suppressed?” | B | Actual signal outcomes/reasons; no invented lineage. |
| Raw example logs | C | Not shown; bounded safe event references replace raw messages. |
| Incident ID, severity, title, root and impact | A/B | Existing incident candidate contract; fallback title derives from probable root when no title exists. |
| Incident duration/creation time | C | No authoritative lifecycle timestamps; omitted. |
| Confidence percentage/bar | C | No calibrated confidence shown. Existing heuristic scores stay technical. |
| RCA candidates and supporting evidence | A | Deterministic RCA hypotheses and their existing evidence; optional allowlisted expert interpretation. |
| Topology graph | A/B | Declared dependency paths only, conditional on incident context. |
| Healthy/normal topology nodes | C | No health assertion. Colors distinguish probable root, incident services and additional context. |
| Timeline | A/B | Real pattern first/last observations only; no signal/correlation/incident/RCA creation timestamps. |
| Incident tabs / evidence drilldown | B | Detail sections and expanders for actual RCA, dependencies, evidence and technical fields. |

## G–H. Overview and New Investigation

Overview presents two working tasks and an honest empty state. A successful result
is retained only in the current Streamlit session. There is no investigation
database or generated sample history.

The form exposes source, namespace, workload and lookback. Advanced controls
contain container, timezone-aware explicit end time and maximum records. Default
acquisition remains 100 records per page × 3 pages. Supported limits map to the
existing maximum 20 pages × 500 records and 24-hour interval. The interval is fixed
once the request is created. The result states when the record budget was reached.

Execution uses one synchronous `st.status`. A cached lock serializes both upload
and OpenShift analyses using the shared cached pipeline. The lock is always
released. Failure clears stale results and renders only known safe error messages.

The durable `active_source` session value is distinct from the form widget's
`analysis_source` value. This preserves ownership when Streamlit cleans up hidden
widgets: file results remain explorable, and switching source cannot revive a
result from the previous source. Two navigation regressions were reproduced before
the fix and passed afterward.

## I–J. Automatic verified-policy resolution

The boundary loads the existing verified segmentation catalog read-only, then
acquires the first bounded page. The resolver evaluates at most its first 200
original records and rejects a sample exceeding 256,000 aggregate raw characters.
It evaluates at most 100 verified policies; a larger catalog fails safely instead
of silently excluding competing candidates. These are compatibility-evaluation
bounds, not continuous ingestion or hard wall-clock regex deadlines.

The existing `RegexValidator` safety admission and `HeaderClassifier.classify_raw`
semantics are reused. The file validator is not invoked because it normalizes
file content and emits source diagnostics. Record content, continuation indentation,
blank records and embedded newlines are not rewritten or split by selection.

A compatible candidate requires at least two effective matching headers overall,
positive evidence in every eligible nonblank sampled stream, and no independent
strong header missed by its regex. The classifier's continuation veto remains
authoritative. Candidate support is the effective matched-header count. A runner-up
within 10% of the best support, including identical boundary results, blocks
selection as ambiguous. Catalog order does not decide a tie.

No match, insufficient evidence, oversized input, an oversized catalog or no verified
policy produces the safe no-policy failure. Ambiguity produces its own safe message.
The pipeline is not constructed on these failures. An empty acquired interval keeps
the existing no-records message. Failures do not trigger discovery or guessing.

On success the immutable original `SegmentationPolicy` snapshot is supplied by a
local provider to unchanged `process_ingested_pages(...)`. The original first page
and remaining original page generator are passed exactly once. No extra network
sampling, re-fetch, record mutation, registry write, hit update, LLM call or
`HeaderDiscovery` is introduced. Selection details appear only in technical details;
the regex is never rendered.

Compatibility evidence is limited to the initial sample. A sparse stream with only
continuations may fail selection conservatively. Later records or streams can
contain formats absent from that sample. This is a finite investigation selection,
not proof of future-format coverage. The existing explicit-policy application API
remains compatible; `None` without `automatic=True` still fails safely.

## K–M. Result, pipeline and stage details

`backend result → InvestigationPresenter → InvestigationViewModel → renderers`
keeps nested backend extraction out of the navigation app. Frozen dataclasses and
tuples hold allowlisted values. Presentation does not qualify signals, establish
correlations, create incidents or rank backend RCA candidates.

The main summary answers whether an incident candidate exists, then shows the
actual scope and count chain. Incident results emphasize severity, probable root,
affected services and evidence. Otherwise the summary explains actual suppression
or qualified-signal results. It does not interpret a finite no-incident result as
global service health.

Acquisition, Segmentation, Parsing, Patterns, Signal, Correlation, Incident and RCA
each show a meaningful result and semantic status. Green means output exists;
amber marks suppression, unreliable templates, unassembled input or a reached
budget; gray marks absent output or structured-alarm bypass. Execution failures
use the error flow rather than displaying a successful partial pipeline. Zero
output alone never becomes red/failure.

Stage selection reveals only available counts: pages/records/streams/budget,
logical events and dispositions, parsed events and optional parser outcomes,
unique patterns/occurrences, signal candidates/qualified/suppressed, correlation
relationships, incident candidates and RCA results. Missing parser detail counters
are omitted. Backend stages are not promoted to top-level product pages.

## N–S. Patterns, signals, incidents, topology, timeline and evidence

Patterns group actual signal rows by existing `template_id`. Counts sum existing
occurrences; severity, services, hosts and real first/last observation times come
from those rows. The most frequent pattern is sorted by count. Outcome reflects
incident membership, qualification or suppression, with individual signal detail
available when a group contains multiple rows. Pattern selection reveals normalized
template text and bounded evidence, never raw logs by default.

Signal reason counts group only actual suppressed-signal `qualification_reason`
values. The current generic suppression reason cannot establish separate threshold,
decay or severity failure counts. Technical scores and reliability ratios remain
in technical details. Missing reasons stay missing.

Incident views preserve actual candidate IDs, membership, severity, event/signal
counts and template membership. They expose probable root and deterministic RCA
hypotheses as evidence requiring investigation, without asserting confirmed
causality. Existing expert commentary is optional, allowlisted, bounded and
redacted; its recommended investigations are advisory. Model/usage metadata and
uncalibrated confidence percentages are not rendered. Expert failure leaves
deterministic RCA visible.

Topology is rendered only for actual declared dependency paths in incident context.
Probable root, incident services and additional context have distinct colors;
unaffected/healthy status is not invented. Correlations are shown as relationships,
not causal proof. No topology inference or cross-analysis context reuse was added.

Timeline contains only actual pattern first/last observations. Every contributing
signal must have `timestamp_resolved is True`, preventing the aggregator's fallback
time from becoming a claimed source timestamp. No processing-stage, incident or RCA
creation times are inferred.

The view model retains at most 200 pattern/signal/incident/correlation rows, 20 event
references per evidence section, 40 timeline observations and 500 characters per
source text field before bounded evidence composition. Main totals use the complete returned result. Topology and expert
lists also have explicit display bounds. Evidence uses representative event IDs;
raw provenance and SourceReference payloads are not copied into UI session state.

Existing OpenSearch redaction is preserved and reused for file display. Configured
credentials/hosts, secret assignments, bearer strings and JWT-shaped text are
redacted. Explicit field allowlists exclude cursors, HTTP bodies, arbitrary metadata
and raw traces. Unexpected metadata dictionaries are not stringified. All dynamic
HTML is escaped in centralized theme helpers. Exceptions never render arbitrary
exception text. This is bounded operational evidence, not a raw payload browser.

## T–V. Compatibility, state and identities

The existing LOG/TXT/MD, JSON and CSV conversion behavior remains, including alarm
severity normalization. ZIP uploads still call `process_package`; other supported
uploads call `process_file`. Temporary files are removed on completion/failure.
Structured alarms keep the existing fast path and display explicit segmentation/
parsing bypass instead of simulated text processing.

**Event identity and template identity are unchanged.** No canonical schema,
event-ID function, template-ID generation, registry schema, parser semantics,
SegmentationSession invariant, acquisition provenance, downstream decision rule or
RCA input contract was changed. Tests assert unchanged input objects/IDs and original
page replay; existing core regressions remain part of full validation.

Automatic selection changes how the application chooses an existing policy. For
the same snapshot and records, processing semantics remain unchanged. Choosing a
different compatible existing policy can naturally affect boundaries; no new
identity algorithm or policy signature is introduced. Existing learning is reused
without invalidation, migration, reset or deletion.

Application configuration, cached persistent template learning, and session-scoped
analysis presentation remain distinct. No client, cursor, SegmentationSession,
provider or request-specific topology is stored in shared UI state. The entry-point
import convention is retained. Stable public Streamlit APIs and mapping-style
session access are used; no private testing/runtime state interface was added.

## W–Z. Validation and review

Both runtime checks completed on Windows:

| Runtime | Focused suites | Complete regression suite |
| --- | --- | --- |
| Python 3.14.8 / Streamlit 1.65.0 / SQLite 3.50.4 | **122 passed**, 22.72 s | **578 passed**, 20.85 s |
| Python 3.10.0 / Streamlit 1.63.0 / SQLite 3.35.5 | **122 passed**, 37.96 s | **578 passed**, 35.88 s |

The full suite contains 39 more cases than the 539-test baseline. Focused counts:
14 resolver, 46 application boundary, 19 presentation and 43 Streamlit cases.
The new navigation regression and extended isolation regression first failed for
the intended hidden-widget state cleanup, then passed after the application fix.

Focused command (run with each interpreter):

```text
python -B -m pytest -q -p no:cacheprovider tests/regression/test_verified_policy_resolver.py tests/regression/test_opensearch_application.py tests/regression/test_investigation_presentation.py tests/regression/test_streamlit_sources.py
```

Full command:

```text
python -B -m pytest -q -p no:cacheprovider tests/regression
```

`git diff --check` passed with exit code 0. Additional whitespace and Python syntax
validation covered all modified and new text files, including the untracked files.
Git reports normal LF-to-CRLF working-copy notices; no whitespace errors remain.
The complete tracked diff and all new source/test files were reviewed.

`git diff --stat`:

```text
 .streamlit/config.toml                     |  25 +-
 src/backend/opensearch_application.py      |  29 +-
 src/frontend/streamlit_app.py              | 689 +++++++----------------------
 src/frontend/theme.py                      | 433 ++++--------------
 tests/regression/test_streamlit_sources.py | 237 ++++++----
 5 files changed, 427 insertions(+), 986 deletions(-)
```

Because nothing was staged, that stat excludes new files. Supplemental additions:

| New source/test file | Lines |
| --- | ---: |
| `src/backend/verified_policy_resolver.py` | 100 |
| `src/frontend/analysis_runtime.py` | 88 |
| `src/frontend/investigation_views.py` | 203 |
| `src/frontend/presentation.py` | 346 |
| `tests/regression/test_investigation_presentation.py` | 183 |
| `tests/regression/test_verified_policy_resolver.py` | 117 |
| Total new source/test content | 1,037 |

This report is an additional new documentation file. The two supplied PNGs are
unchanged pre-existing untracked assets, not generated implementation changes.

Security regressions retain their external-socket, application HTTP (including
localhost), real OpenSearch, LLM, SQLite and production-learning guards. Windows
asyncio's literal loopback socketpair allowance remains intact. Registry fixtures
use isolated repository-local temporary catalogs and preserve URI/read-only SQLite
compatibility. Tests do not require real OpenSearch credentials or production state.

The Streamlit server started successfully for a visual check. The enabled computer
surface reported no browsers/apps, so no rendered-browser comparison or visual
refinement pass was possible. AppTest validates elements and interactions on
Streamlit 1.65; it does not establish pixel-level layout quality. A desktop browser
comparison against the supplied mockup remains the visual follow-up. The temporary
Streamlit validation server was stopped after testing.

## AA. Deferred production-hardening work

Continuous polling/workers, asynchronous services, checkpoints, watermarks,
late-arrival handling, exactly-once semantics, index rollover recovery, continuous
policy discovery, LLM discovery, policy editing and structured-event canonical
convergence remain out of scope. Persistent investigation/incident management,
acknowledgment, sharing, real progress/cancellation, historical trends and calibrated
confidence also need explicit backend contracts. Automatic remediation and human
action execution were not introduced.
