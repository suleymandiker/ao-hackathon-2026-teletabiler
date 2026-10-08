# High volume deployment monitoring

The worker keeps OpenSearch as the raw log source. Each run first obtains an
exact `[window_start, window_end)` count and minute histogram with `size: 0`.
Pod and container top counts are requested only when field capabilities confirm
an aggregatable exact field. Their exactness flag follows OpenSearch's terms
error bound; distribution baseline signals require exact counts. Content
acquisition retains the configured overlap,
uses the existing timestamp/sequence `search_after` order, and compares unique
in-window references with the exact count before allowing success.

`page_size` stays at its persisted value (maximum 500). The persisted
`max_pages` now limits each acquisition shard. The histogram helps choose time
shards; every shard also receives an exact count preflight. A shard too large for
its page budget or raw buffer is bisected into exact half-open UTC children before
its records are sent to segmentation. This continues across the full logical
window. An irreducible dense timestamp or exhausted safety budget fails the run
with `ACQUISITION_LIMIT`; no result or watermark is committed.

Safety bounds are 65,536 characters per monitored source message, 10,000 buffered
records or 8,000,000 message characters per
shard, 24 split levels, 4,096 shards, and 100,000 content request pages per run.
The incremental segmentation session permits at most 10,000 active streams,
20,000 pending physical records, and 8,000,000 pending characters. Compact
analytical state permits 1,000 patterns, 1,000 signal groups, and 200 qualified
signals entering correlation. These are
resource safety controls, not a fixed record ceiling for a logical window.
Exceeding one fails closed.

Verified policy selection samples at most 200 initial records and 256,000
characters, buffering no more than 20 nonempty content pages before replay.

SQLite monitoring schema version 4 adds run metrics, pattern metrics, shard
diagnostics, and a disk-backed run reference ledger. Raw source bodies and
OpenSearch cursors are not stored. Successful result, receipts, metrics, and
watermark commit in one transaction. An interrupted attempt restarts the exact
window and its shards. The local worker still holds its OS-owned advisory lock;
the repository protocol is the boundary for a future shared-database scheduler.
SQLite does not provide distributed claims. PostgreSQL is not enabled.

Baseline signals require five successful metric-bearing windows and use up to
30 previous windows. They record current values, median, median absolute
deviation, ratio/delta, and threshold. Cold-start windows collect baseline
without a volume, rate, pattern, or parsed-severity anomaly claim. Signal and
trace evidence is bounded and redacted before monitoring persistence.
