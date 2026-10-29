# Benchmarks

Reference machine: Apple arm64 laptop, macOS. Release build. Dimension 384, 4-bit TurboVec, top-10 results.
Each search row is the p50 / p95 / p99 of 200 random queries. Each run uses a fresh database directory.

Reproduce:

```sh
cargo run --release -p zenar-core --example bench -- 1000000 384           # ingest, then searches in-process
cargo run --release -p zenar-core --example reopen_bench -- target/bench-data/1000000-384 1000000 384
```

## Current backend: SQLite (rusqlite, WAL, `synchronous=NORMAL`)

### Ingest

| Path | Rows | Total | Per row |
|---|---|---|---|
| `add_many`, batches of 1000 (benchmark) | 1,000,000 | 29.9 s | 0.030 ms |
| `add`, one row at a time (benchmark, before batching) | 1,000,000 | 52.9 s | 0.053 ms |

Each row has three indexed metadata fields and one 384-dim vector. A batch validates every record, adds the vectors,
syncs the index once, and commits all rows in one SQLite transaction. The old Stoolap backend ingested at 0.013 ms/row
without these per-row costs, but it had the reopen problem (see History).

Measured breakdown with the Python API at 100K rows, one row at a time: no metadata 0.017 ms, three indexed fields
0.035 ms. The indexes cost about half of the per-row time.

### Search, in-process (same process as ingest)

| Case | p50 | p95 | p99 |
|---|---|---|---|
| No filter | 1.18 ms | 1.35 ms | 7.9 ms |
| `tenant = A` (10%) | 5.7 ms | 9.0 ms | 12.4 ms |
| Compound: `tenant = A AND category IN (faq, product) AND price >= 100` (~4%) | 25.4 ms | 26.2 ms | 31.5 ms |
| `price = 5` (0.1%) | 0.34 ms | 0.45 ms | 0.51 ms |

### Search, after reopen (fresh process, existing database)

| Case | p50 | p95 | p99 |
|---|---|---|---|
| No filter | 1.18 ms | 1.45 ms | 8.9 ms |
| `tenant = A` (10%) | 5.8 ms | 7.5 ms | 13.5 ms |

Open with reconciliation: 0.29 s for 1M rows.

### What the numbers say

- Unfiltered and selective (0.1%) search are close to pure TurboVec cost.
- A filter's cost comes from how many rows it matches. Each match is one index entry, and each candidate needs one
  table-row read to check the rest of the filter. Compound filters made of non-selective predicates are the slow case.
  SQLite can use one index per table access, so it cannot intersect two indexes.
- The `status` predicate was removed from the candidate query. With it, SQLite reads every candidate row from the table,
  which took 24 ms for the 10% filter instead of 5.7 ms. The fetch of final results still filters on `READY`.

### Known gaps against the design spec's section 90 gate

"Filtered search must not be dominated by the metadata step at 1M+ rows." Met for selective filters. Not met for
broad or compound filters, where the metadata step is most of the cost. Options, if the gate must hold for those:
composite indexes tuned to the common filter shapes, or a Zenar-owned metadata store (the planned doc store).

## History

Measured while the metadata engine was Stoolap 0.4 (the first branch). Kept for the record, because the reopen
behavior drove the switch.

| Measurement | Stoolap 0.4 |
|---|---|
| Ingest, 1M rows | 0.013 ms/row (12.9 s) |
| In-process filtered 10% | 46 ms |
| In-process unfiltered | 1.2 ms |
| Reopen unfiltered (IN-list hydration) | 57 ms |
| Reopen unfiltered (per-id hydration) | 12 ms |
| Reopen, 10 random ids, fetch only | 46 ms at 1M, 7.6 ms at 100K, 1.5 ms at 10K |

Stoolap's `IN` list on the primary key scanned the table after a reopen (cost linear in rows). Equality on the key used
its index, but each lookup still cost ~1 ms. These numbers were the reason for the backend switch.

## Crash behavior

Covered by tests, not by benchmark: see [STORAGE.md](STORAGE.md#crash-tests).
