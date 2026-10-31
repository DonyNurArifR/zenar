# Benchmarks

Reference machine: Apple arm64 laptop, macOS. Release build. Dimension 384, 4-bit TurboVec, top-10 results.
Each search row is the p50 / p95 / p99 of 200 random queries. Each run uses a fresh database directory.

Reproduce:

```sh
cargo run --release -p zenar-core --example bench -- 1000000 384           # ingest, then searches in-process
cargo run --release -p zenar-core --example reopen_bench -- target/bench-data/1000000-384 1000000 384
```

## Current backend: Zenar document store

Reference machine: Apple arm64 laptop, macOS. Release build. Dimension 384, 4-bit TurboVec, top-10 results.
Search rows are p50 / p95 / p99 over 200 random queries. Each run uses a fresh database directory.

Reproduce:

```sh
cargo run --release -p zenar-core --example bench -- 1000000 384
cargo run --release -p zenar-core --example reopen_bench -- target/bench-data/1000000-384 1000000 384
```

### Ingest (`add_many`, batches of 1000)

| Rows | Total | Per row |
|---|---|---|
| 10,000 | 0.12 s | 0.012 ms |
| 100,000 | 1.20 s | 0.012 ms |
| 1,000,000 | 11.9 s | 0.012 ms |

Each row has three metadata fields and one 384-dim vector. A batch writes its log frames in one write, adds its vectors,
syncs the index and the log once, then makes the batch visible.

One row at a time (`add`), Python API, 20K rows, default durability: 0.014 ms/row.
With `durable=True` (every add syncs the index and the log): **7.2 ms/row**. Use `add_many` batches when you need
durability per batch. That costs one sync per 1000 rows instead of one per row.

### Delete

One `delete` costs about 8 ms, at 10K and at 100K rows alike. The cost is the two fsyncs (index file and log), not the
collection size. `delete_many` pays them once per batch. Use it for bulk removal.

### Search, in-process (same process as ingest), 1M rows

| Case | p50 | p95 | p99 |
|---|---|---|---|
| No filter | 1.17 ms | 1.46 ms | 2.85 ms |
| `tenant = A` (10%) | 3.65 ms | 4.24 ms | 6.22 ms |
| Compound: `tenant = A AND category IN (faq, product) AND price >= 100` (~4%) | 15.2 ms | 16.1 ms | 25.2 ms |
| `price = 5` (0.1%) | 0.32 ms | 0.41 ms | 0.43 ms |

### Search, in-process, smaller collections

| Rows | No filter (p50) | 10% filter (p50) | Compound (p50) | 0.1% (p50) |
|---|---|---|---|---|
| 10,000 | 0.10 ms | 0.10 ms | 0.15 ms | 0.02 ms |
| 100,000 | 0.24 ms | 0.39 ms | 1.22 ms | 0.08 ms |

### Search, after reopen (fresh process, existing 1M-row database)

| Case | p50 | p95 | p99 |
|---|---|---|---|
| Open with reconciliation | 0.79 s | | |
| No filter | 1.16 ms | 1.34 ms | 1.57 ms |
| `tenant = A` (10%) | 3.61 ms | 4.22 ms | 4.88 ms |

Open time is the replay of the log into the in-memory indexes. It grows linearly with rows.

### What the numbers say

- Unfiltered and selective (0.1%) search are close to pure TurboVec cost. The 0.1% filter is faster than the unfiltered
  search at 10K rows, because it returns few candidates.
- A broad filter costs what its matching rows cost: one posting per match, merged and intersected in memory. The compound
  filter combines three predicates of which two are broad. It is the slowest case, at about 13x the unfiltered search.
- The gate in the design spec (section 90) asks that the metadata step not dominate filtered search at 1M rows. Selective
  filters pass it. Broad and compound filters pass it at 10%, and compound filters are the remaining case.

### Known gaps

- Compound filters are not tuned. The merge of many postings sorts the matches. A k-way merge would be cheaper.
- Open time for large logs is linear. A persisted index would avoid the replay. It would also need its own recovery.
- Memory is not measured here. The indexes hold one list entry per (row, field) and one entry per distinct value.

## History

Earlier engines, kept for the record. The reopen behavior of Stoolap drove the switch to SQLite. SQLite's filter cost
drove the switch to the document store.

SQLite (rusqlite, WAL) at 1M rows, same workload, before batched ingest: ingest 0.053 ms/row (per-row inserts), 0.030
ms/row with `add_many`. Reopened unfiltered 1.2 ms. Tenant 10%: 5.5 ms. Compound: 24 ms. 0.1%: 0.34 ms. Open with
reconciliation: 0.28 s.

Stoolap 0.4:

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
