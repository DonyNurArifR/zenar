# Benchmarks

Measured on: Apple arm64 laptop, macOS, `cargo run --release`. Dimension 384, 4-bit TurboVec, top-10 results,
200 random queries per row of the table. Each run uses a fresh database directory.

Reproduce:

```sh
cargo run --release -p zenar-core --example bench -- 1000000 384
cargo run --release -p zenar-core --example filter_probe -- 1000000 384
cargo run -p zenar-core --example stoolap_probe -- writer /path/probe.db   # and reader, in parallel
```

## Ingest (default `MetadataSync::Normal`, index synced every 1000 adds, then `flush()`)

| Rows | Total | Per row |
|---|---|---|
| 10,000 | 0.18 s | 0.018 ms |
| 100,000 | 1.63 s | 0.016 ms |
| 1,000,000 | 12.9 s | 0.013 ms |

Earlier policies, for the record: `Full` metadata sync with a per-add index sync took 199 s for 10K rows
(19.9 ms/row). `Full` with batched index sync took 159 s (15.9 ms/row). The cost was Stoolap's per-commit fsync.
An UPDATE per row (insert PENDING, then mark READY) cost about 12 ms more per filtered query at 1M rows, from
MVCC versions. The current add writes each row once.

## Search latency, same process as ingest (p50 / p95 / p99)

Measured right after ingest, in the same process. The database is fresh, so the metadata table has no reopen.

| Rows | No filter | `tenant = A` (10%) | Compound filter (~4%) |
|---|---|---|---|
| 10,000 | 0.07 / 0.08 / 0.08 ms | 0.20 / 0.22 / 0.22 ms | 0.21 / 0.23 / 0.24 ms |
| 100,000 | 0.22 / 0.30 / 0.36 ms | 2.46 / 3.28 / 3.62 ms | 2.15 / 3.10 / 3.55 ms |
| 1,000,000 | 1.15 / 1.26 / 1.37 ms | 46.1 / 46.8 / 47.3 ms | 51.9 / 52.6 / 53.0 ms |

The compound filter is `tenant = A AND category IN (faq, product) AND price >= 100`.

## Reopen: metadata lookups after reopen (fixed in part)

After `VectorDB.open()` on an existing database, search latency was dominated by the metadata fetch for the top-K
results. The cause, measured in this round:

- Stoolap 0.4 does not use the primary-key index for an `id IN (...)` list (parameterized or literal). It scans
  the table. Latency grows linearly with rows: 1.5 ms at 10K, 7.6 ms at 100K, 46 ms at 1M for 10 ids.
- Stoolap does use the index for `id = $1`. Ten single-id queries took ~10 ms at 1M, where one IN query took ~47 ms.
- In a process that ingested the data, the same IN lookup is fast. The scan problem appears only on a reopened table.

Fix: `MetadataStore::fetch_documents` runs one prepared `id = $1` query per id for up to 32 ids, and chunked
IN lists (256 ids per query) above that. A scan costs about the same whatever the list length, so past a few dozen
ids a scan per chunk is cheaper than one lookup per id.

Reopened database, fresh process, 1M rows, dim 384, top-10, 200 queries (`examples/reopen_bench.rs`):

| Case | Before (IN list) | After (per-id) |
|---|---|---|
| Unfiltered search | 57 ms (p50, measured earlier) | 12.2 / 16.4 / 20.2 ms |
| `tenant = A` (10%) | not measured on reopen | 32.5 / 33.4 / 34.3 ms |

Open with reconciliation: 0.34 s for 1M rows.

Remaining cost: each reopened `id = $1` lookup still takes ~1 ms. An in-process table takes ~0.005 ms. The cause is
not confirmed. It may be per-query overhead on reopened tables. Reproduce with `bench 1000000 384`, then
`reopen_bench target/bench-data/1000000-384 1000000 384`.

## Filtered-search gate (design spec section 90): **not passed**

"Filtered search must not be dominated by the Stoolap step at 1M+ rows." At 1M rows and 10% selectivity,
the Stoolap candidate query is ~95% of filtered search time, and filtered search is ~40x slower than unfiltered.

Measured split at 1M rows (`examples/filter_probe.rs`):

| Component | Time |
|---|---|
| TurboVec search, no allowlist | 1.07 ms |
| TurboVec `search_with_allowlist`, 10% of ids | 1.94 ms |
| TurboVec `search_with_allowlist`, 100% of ids | 13.9 ms |
| Stoolap candidate query, 100K ids back, no index | 15.3 ms |
| Stoolap candidate query, with index on `tenant` | 14.7 ms (no gain) |
| Stoolap candidate query after insert-then-UPDATE of status | 27.4 ms (MVCC versions) |

Inside `Collection::search`, the Stoolap candidate query is the main cost, and it is slower there than in the probe
(about 42 ms against 15 ms). That gap is unexplained.

What has been ruled out or established:

- Indexing the filter column does not help at 10% selectivity. The cost is materializing ~100K ids.
- Plan caching is in place and is not the bottleneck.
- The file-backed store is not slower than in-memory for this query when the process ingested the data.
- TurboVec's allowlist path is not the bottleneck (about 2 ms at 10%).

Remaining ideas, ordered by cost: measure the candidate query at 0.1% and 1% selectivity (where an index should help);
test a path that avoids materializing large candidate sets; re-test after the reopen issue is resolved.

Filtered search is correct at every size tested. The gate is a performance target, and it is not yet met.

## Crash behavior

Covered by tests, not by benchmark: see [STORAGE.md](STORAGE.md#crash-tests).
