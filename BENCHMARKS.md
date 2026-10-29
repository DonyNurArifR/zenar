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

## Reopen blocker: metadata lookups slow down after the database is reopened

**This blocks production use on large collections.** Search latency after `VectorDB.open()` on an existing
database is dominated by the metadata fetch for the top-K results (`WHERE status = 'READY' AND id IN (...)`).

Measured with `examples/fetch_probe.rs`: a `MetadataStore` fetch of 10 random ids, on an existing database
in a fresh process, with no other work:

| Rows | Fetch, 10 random ids | Fetch, 10 ids spaced 1000 apart |
|---|---|---|
| 10,000 | 1.5 ms | — |
| 100,000 | 7.6 ms | — |
| 1,000,000 | 46 ms | 1.4 ms |

Latency grows linearly with row count, which means a full scan per fetch. Spaced ids were fast only because
their matches sit near the start of the table, so the scan stopped early.

- The same random lookup on a table created in the same process is about 0 ms at 1M rows.
- A reopened `VectorDB` made 1M-row unfiltered search take ~57 ms per query in a fresh process
  (compare 1.2 ms in the process that ingested).
- Adding `CREATE INDEX ... (id)` on the reopened table changed 100K lookups from 7.8 to 5.4 ms. That is not a fix.
- Writing a Stoolap snapshot before close did not change the reopen cost.
- The `PkIndex` is a virtual index that Stoolap registers when a table loads (`src/storage/mvcc/engine.rs`).
  The cause is probably in how that index is populated on reload. This was not confirmed.

Reproduce: `cargo run --release -p zenar-core --example bench -- 1000000 384` (creates the data), then
`cargo run --release -p zenar-core --example fetch_probe -- target/bench-data/1000000-384 1000000`.

Options, for a decision:

1. Report to Stoolap with this reproduction, and wait for a fix. Zenar's design does not change.
2. Keep the documents and metadata in a Zenar-owned store (for example a log file plus an in-memory id index
   rebuilt on open). Stoolap would then serve only collection config. This changes the design in
   `ZENAR_TECHNICAL_DESIGN_SPECIFICATION.md` (sections 4.2 and 19) and the recovery story.
3. Switch the metadata engine to SQLite (rusqlite). This is spec section 91's fallback. It changes the dependency and the license set.

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
