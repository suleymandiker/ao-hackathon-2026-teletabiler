# Downstream determinism and RCA evidence hardening

The previous blocker is resolved by the explicitly authorized missing-time and
equal-time rules. At continuation start, this report and
`test_downstream_determinism.py` were the only existing changes, both untracked.
They were reconciled. Nothing was staged, committed or pushed.

## Confirmed cause and source-time behavior

Previously `CanonicalEventBuilder.build()` assigned the machine clock to
`observed_timestamp`. The parser deliberately left source-local timestamps without
timezone evidence unresolved. Aggregation used `timestamp or observed_timestamp`,
and `_ts_ms()` had another `datetime.now()` fallback. Execution timing became the
minute-bucket coordinate for historical untimed events. Counts crossed
qualification thresholds as a run crossed a minute boundary. This was not decay,
randomness or a retained downstream counter.

The four-event fixture previously changed from 2 candidates / 2 qualified /
1 correlation / 1 incident to 3 / 1 / 0 / 0 at a minute boundary. A real canonical
builder plus temporary persistent template pipeline also reproduced changing
downstream counts with unchanged segmentation/parsing/templating counts.

The new contract is:

- `analysis_time.source_time_ms()` reuses the parser's `TimestampNormalizer`.
  Canonical source fields and timezone rules are unchanged. Invalid, nonfinite,
  boolean and unresolved inputs return `None`; epoch zero is preserved.
  Observation/retrieval clocks never supply source time.
- Timed signals keep numeric minute buckets and the existing ID formula. Untimed
  events use one explicit `None` bucket per existing template/identity key in that
  invocation. The ID ends in `:untimed`; first/last/window start/end are `None`,
  and `timestamp_resolved` is false. There is no artificial calendar timestamp.
- Untimed counts cover the supplied finite analysis, not an invented 60-second
  interval. Existing severity, failure, repetition and reliability weights and
  thresholds are unchanged. Qualification has no recency/decay feature, so no
  substitute contribution or new weight was introduced.
- Every existing correlation rule requires a temporal bound. Missing required
  source time cannot satisfy those predicates: no edge, gap, proximity or
  precedence is emitted. Signals remain eligible for non-temporal qualification.
  No new non-temporal correlation algorithm or changed timed score was introduced.
- Incident root-merging rules also require their existing temporal bounds.
  Untimed signals remain eligible under existing singleton/root incident rules.
  Unknown spans are `None`; incident IDs use `inc:untimed:<ordinal>`.

## Finite reference, ordering and live compatibility

The finite downstream invocation computes the maximum valid source timestamp in
one pass over supplied templated events, before aggregation filtering. Its immutable
local `AnalysisTimeContext` is returned as additive diagnostics:

```json
{"analysis_time":{"analysis_reference_time_ms":1791158400000,"timestamp_basis":"source"}}
```

With no valid source timestamp, including an empty analysis, the reference is
`null` and the basis is `unresolved`. No current scoring algorithm needs this
reference. It never fills event timestamps or persists on a shared pipeline.
The observer consumes the input once without another materialized event list;
the existing batch aggregation memory architecture is otherwise unchanged.

Full file/package processing carries invocation-local logical delivery order into
templated events and aggregates. Page processing preserves the session's existing
completion order. `SourceRecord.retrieval_order` is explicitly opaque in its
contract; metadata is retained as provenance, not reinterpreted to reorder pages.
No provider-specific processing was added downstream.

Aggregation orders windows, then earliest known time, source order and signal ID.
Representatives preserve explicit source order, using event ID for standalone
inputs without an ordinal. Correlation orders by source time, source order and
signal ID. Incident inputs use the same stable order; structured-root exact ties
use source order/ID. Topology traversal and database-entity ties use lexical
identity. RCA keeps its existing severity/time/outgoing-count/qualification-score
ranking, adding source order and ID only for ties. Unknown times sort after known
times; sorting sentinels never become public timestamps.

Equal valid timestamps retain zero gap and applicable service/topology evidence.
They do not emit `nedensel_zaman_sırası`: that label requires a strictly earlier
topology-root source timestamp. The existing topology tolerance and numeric scores
remain unchanged, including when a root timestamp is slightly later.

The page regression preserves parsed source time and retrieval metadata, and proves
acquisition time is not silently substituted into missing canonical time. This
page API is already a caller-bounded finite analysis, not continuous monitoring.
No ingestion timestamps, cursors, watermarks or acquisition semantics changed.

Identity compatibility is precise:

- Canonical event IDs, parser semantics, template IDs, persistent learning and
  stored-state formats are unchanged. No learning-state reset/migration occurs.
- Resolved, non-tied inputs retain their signal-ID formula and edge scores;
  existing fixed-identity integrated regressions pass.
- Fabricated untimed bucket/incident IDs necessarily change to `untimed` IDs.
  Edges based on those fabricated times disappear. Equal-time direction/order
  and tied incident numbering can change as authorized.
- Expert-only changes below never mutate signals, edges, incidents or deterministic
  RCA candidates/scores/order.
- The canonical event-ID counter is unchanged. Representative event samples are
  not promised byte-identical across reused parser invocations; tests compare the
  requested downstream decision identities directly.

## Expert grouping and traceability

Grouping reuses exact existing `template_id`, service, component, namespace,
cluster, scope, alarm type and exact selected-incident membership. Missing template
identity falls back to signal ID. Similar text never supplies a family. Different
components/incidents remain separate. Counts sum, severity takes the minimum and
qualification takes the maximum, as before. Time ranges require resolved evidence.
`signal_windows` counts distinct known windows; `represented_signal_count` is added
when it differs. Untimed evidence has zero known windows.

The immutable pack retains representatives and exact member mappings. After
unchanged strict output validation, `kanit_sinyal_idleri` now resolves accepted
aliases to every underlying member ID in stable order without duplicates.
Singleton behavior is unchanged. No long member-ID list is sent to the LLM.

S1/S2: actual debug IDs were not supplied, and the local validated registry has no
`chatbot_param_list` template. Equivalence is not proven. Similar ProgrammingError
text stays separate unless existing identity proves equivalence. Tests cover both.

S4-S8: actual IDs were not supplied, and the local registry has no `[BALANCE]`
template. No duration regex, second template engine or invented family field was
added. Distinct template IDs remain distinct; when authoritative identity is shared,
all five form one group with exact traceability. No duration range is guessed.

## Correlation diversity and measured examples

Edges retain one strongest representative per directed group pair. Equivalent
families then share directed service/component/scope/incident coordinates, the full
existing evidence-type set, exact dependency path and source/target root roles.
Alias changes or different gaps alone do not require another slot.

Within a family, selection prefers score, available path/more evidence, shorter
numeric gap, then stable serialized-edge/identity ties. Across remaining families,
it prefers dependency paths, root involvement, new evidence types and component
pairs, then score/gap/identity. The maximum is a cap. No graph object or evidence
type is changed or invented.

These are synthetic measurements with identical prompt/schema overhead (3,244
characters). Approximate tokens mean `ceil(input_chars / 4)`, not measured tokens:

| Fixture | Groups before/after | Edges before/after | Evidence chars before/after | Total input chars before/after | Approx. tokens before/after |
| --- | --- | --- | --- | --- | --- |
| Eight distinct IDs | 8 / 8 | 10 / 5 | 4,167 / 3,647 | 7,411 / 6,891 | 1,853 / 1,723 |
| Shared authoritative BALANCE ID | 4 / 4 | 6 / 4 | 2,389 / 2,210 | 5,633 / 5,454 | 1,409 / 1,364 |

Shared-ID grouping already existed. This change makes incident membership, window
counts and returned traceability precise and removes redundant edge families.
It does not claim to discover a family for the unavailable real example. Adding
distinct topology evidence retains it as a sixth useful edge in the first fixture.

Example shared-ID group after the change:

```json
{"ref":"S4","component":"service_executor","signal_windows":1,"represented_signal_count":5,"total_occurrences":5,"pattern":"[BALANCE] FAILED AssetReadException duration=<NUM>ms"}
```

That normalized pattern is supplied authoritative fixture data, not a new masking
rule. Root hints preserve deterministic order/scores. Pattern redaction/truncation,
prompt content, budgets and response schema remain unchanged.

## Validation and changed files

- Original reproductions: 3 failures / 2 passes before production edits.
- Extended determinism suite: **27 passed**, before evidence work.
- Determinism plus ingestion compatibility: **65 passed**.
- Focused RCA expert/evidence/density suite: **158 passed**.
- Full `python -B -m pytest -q -p no:cacheprovider tests/regression`:
  **763 passed** (17.74 s), versus 726 originally and 728 passed / 3 failed previously.
- No expected failures remain. New tests block network/LLM and production SQLite;
  real template persistence is tested only with temporary paths.
- Complete production/test diff reviewed. `git diff --check`, untracked-file
  whitespace checks and Python syntax checks pass. Nothing staged or committed.

Production: `analysis_time.py`, `aggregation_layer/aggregator.py`,
`correlation_layer/correlator.py`, `downstream_pipeline.py`, `full_pipeline_v2.py`,
`incident_candidate_layer/builder.py`, `input_package_layer/topology.py`,
`rca_layer/rca_engine.py`, `rca_layer/evidence.py`, `rca_layer/expert_output.py`.
Tests: reconciled `test_downstream_determinism.py`, new
`test_rca_evidence_density.py`, additive contract assertion in
`test_ingested_pipeline.py`. Docs: this report and `rca-evidence-pack.md`.

`git diff --stat`: 11 tracked files changed, 120 insertions and 55 deletions.
Git excludes the four untracked files listed above from that statistic; two
(the determinism harness and this report) were already present at task start.

GLM compatibility: strict json_schema remains first; include_reasoning remains
absent; enable_thinking retains its existing false default; max-token clamping is
unchanged. Strict validation, length/max_tokens rejection and deterministic fallback
pass existing tests. RCA_DEBUG remains safe, opt-in and functional. No real GLM
call was made during this task.

## Citrix three-run smoke

The 16 MB file was not supplied. Its reported 8,119/8,122/8,089 variation is **not
proven** to have this cause. No real-file counts are claimed. Under fixed source
time, template-learning, configuration and topology inputs, the corrected path is
expected to be stable. This remains a smoke procedure, not a completed result.

1. Stop Streamlit. Use the actual log, validated-template/Drain state and active
   policy database paths below. Originals are only read. If the app uses separate
   segmentation/parser databases, snapshot each and pass its copy explicitly to
   the corresponding constructor instead of the shared environment setting.
2. From the repo root, run this with the same window size/configuration as the UI.
   Each invocation gets an independent copy of the same learning snapshot and
   fresh parser/downstream state. No expert/network discovery runs. Only counts
   and ordered-decision digests are printed, not raw logs or credentials.

```powershell
$env:AIOPS_SMOKE_LOG = 'C:\operator\actual-16mb.log'
$env:AIOPS_SMOKE_POLICY = 'C:\operator\active-policy.sqlite3'
$env:AIOPS_SMOKE_TEMPLATE = 'C:\operator\template_state_v4f.json'
$env:AIOPS_SMOKE_DRAIN = 'C:\operator\template_drain_v4f.bin'
@'
import hashlib, json, os, shutil, sqlite3, sys, tempfile
from contextlib import closing
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0, str(Path('src/backend').resolve()))
from full_pipeline_v2 import FullAIOpsPipelineV2
from segmentation_layer.policy_registry import SegmentationPolicyRegistry
source = Path(os.environ['AIOPS_SMOKE_LOG']).resolve(strict=True)
keys = ('segmented', 'parsed', 'templated', 'signal_candidates',
        'qualified_signals', 'correlations', 'incidents')
with tempfile.TemporaryDirectory() as directory:
    base = Path(directory) / 'snapshot'
    base.mkdir()
    for variable, filename in [('AIOPS_SMOKE_TEMPLATE', 'templates.json'),
                               ('AIOPS_SMOKE_DRAIN', 'drain.bin')]:
        shutil.copy2(Path(os.environ[variable]), base / filename)
    policy = Path(os.environ['AIOPS_SMOKE_POLICY']).resolve(strict=True)
    with closing(sqlite3.connect(policy.as_uri() + '?mode=ro', uri=True)) as src:
        with closing(sqlite3.connect(base / 'policies.sqlite3')) as dst:
            src.backup(dst)
    expected = None
    for index in range(1, 4):
        work = Path(directory) / str(index)
        shutil.copytree(base, work)
        with patch.dict(os.environ, {'AIOPS_POLICY_REGISTRY_PATH': str(work / 'policies.sqlite3'),
                                     'RCA_DEBUG': 'false'}), \
             patch.object(SegmentationPolicyRegistry, '_memory', {}), \
             patch('requests.sessions.Session.request', side_effect=RuntimeError('Offline smoke')) as network:
            pipeline = FullAIOpsPipelineV2(template_state=str(work / 'templates.json'),
                drain_state=str(work / 'drain.bin'), window_seconds=60, use_ai_rca=False)
            result = pipeline.process_file(str(source))
            assert network.call_count == 0, 'Prepare verified policies before this offline smoke'
        selected = {
            'counts': {key: result['stats'][key] for key in keys},
            'candidates': [(row['signal_id'], row['count'], row['qualified'], row['qualification_score'])
                           for row in result['signals']],
            'qualified': [row['signal_id'] for row in result['qualified_signals']],
            'correlations': result['correlations'], 'incidents': result['incidents'],
            'rca': result['rca'], 'analysis_time': result['analysis_time'],
        }
        encoded = json.dumps(selected, ensure_ascii=False, sort_keys=True,
                             separators=(',', ':'), allow_nan=False).encode('utf-8')
        digest = hashlib.sha256(encoded).hexdigest()
        print(json.dumps({'run': index, **selected['counts'], 'decision_sha256': digest}))
        if expected is None:
            expected = encoded
        assert encoded == expected, 'Ordered deterministic decisions changed'
'@ | python -B -
```

3. Require all three counts and digests to match. Dictionary keys are serialized
   canonically; decision arrays are not sorted by the checker. If discovery is
   attempted, prepare verified policies first: fallback is not equivalent to the
   configured real investigation.
4. Restart normally with `$env:RCA_DEBUG = 'true'` and
   `python -B -m streamlit run src/frontend/streamlit_app.py`. Analyze the same file
   three times through the cached instance, keeping intentional learning enabled.
   Compare downstream counts, explicitly accounting for any differing learning
   snapshot. Do not expect the old fabricated-time correlation counts.
5. Confirm first-attempt json_schema, HTTP 200, include_reasoning=absent,
   enable_thinking=False and finish_reason=stop **plus strict local acceptance**.
   Inspect group/edge diversity and gateway-reported tokens. Expert wording is not
   promised byte-stable. Disable RCA_DEBUG and restart after local debugging.
