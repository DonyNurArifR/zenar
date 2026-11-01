# Benchmarks

Reference machine: Apple arm64 laptop, macOS. Release build. Dimension 384, 4-bit TurboVec, top-10 results.
Each search row is the p50 / p95 / p99 of 200 random queries. Each run uses a fresh database directory.

The sections below give the current measurements. The other examples are `filter_profile` (filter steps alone) and
`compound_breakdown` (compound filter split into its steps). Reproduction commands are in each section.

## Current backend: Zenar document store

Reference machine: Apple arm64 laptop, macOS. Release build. Dimension 384, 4-bit TurboVec, top-10 results.
Search rows are p50 / p95 / p99 over 200 random queries. Each run uses a fresh database directory. The runs were made on an
idle machine, one after another.

Reproduce (the second command reopens the database that `bench` keeps in the directory it is given):

```sh
D=$(mktemp -d)
cargo run --release -p zenar-core --example bench -- 1000000 384 "$D"
cargo run --release -p zenar-core --example reopen_bench -- "$D" 1000000 384
```

### Ingest (`add_many`, batches of 1000)

| Rows | Total | Per row |
|---|---|---|
| 10,000 | 0.13 s | 0.013 ms |
| 100,000 | 1.32 s | 0.013 ms |
| 1,000,000 | 11.2 s | 0.011 ms |

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
| No filter | 0.38 ms | 0.53 ms | 1.02 ms |
| `tenant = A` (10%) | 2.87 ms | 3.34 ms | 3.56 ms |
| Compound: `tenant = A AND category IN (faq, product) AND price >= 100` (~4%) | 3.84 ms | 4.20 ms | 4.50 ms |
| `price = 5` (0.1%) | 0.29 ms | 0.31 ms | 0.33 ms |

Measured on the sparse-code build (fields store codes only for rows that have them). The compound row was 2.96 ms
before that change, see the note on compound filters below.

Measured with TurboVec 1.1.2. The 1.0.0 numbers, from the same day, are in the comparison below.

### Search, in-process, smaller collections

| Rows | No filter (p50) | 10% filter (p50) | Compound (p50) | 0.1% (p50) |
|---|---|---|---|---|
| 10,000 | 0.09 ms | 0.10 ms | 0.09 ms | 0.02 ms |
| 100,000 | 0.24 ms | 0.40 ms | 0.39 ms | 0.08 ms |

### Search, after reopen (fresh process, existing 1M-row database)

| Case | p50 | p95 | p99 |
|---|---|---|---|
| Open with reconciliation | 0.44 s | | |
| No filter | 0.36 ms | 0.41 ms | 0.44 ms |
| `tenant = A` (10%) | 2.86 ms | 3.41 ms | 3.54 ms |

Open time is the replay of the log into the in-memory indexes, plus loading the vector index. Replay is about 0.3 s of
the 0.45 s at 1M rows. It grows linearly with the log.

### TurboVec 1.1.2 against 1.0.0 (same machine, same day)

Both builds run the same `bench` example, one after the other. Ingest was 0.020 ms/row for both on this day. The machine
was slower than on the earlier days, which is why ingest differs from the 0.011 ms/row above.

| Case | TurboVec 1.0.0 | TurboVec 1.1.2 |
|---|---|---|
| No filter, p50 | 1.18 ms | 0.38 ms |
| `tenant = A` (10%), p50 | 3.52 ms | 2.90 ms |
| Compound, p50 | 3.51 ms | 2.96 ms |
| `price = 5` (0.1%), p50 | 0.31 ms | 0.30 ms |
| Peak RSS, 100K rows | 52.6 MB | 61.8 MB |
| Peak RSS, 1M rows | 410 MB | 415 MB |

The on-disk format is unchanged for this fixture: a database written by 1.0.0 opens under 1.1.2 and returns the same
top results (`tests/test_fixture_compat.py`). The memory rise is largest at small sizes and about 2% at 1M rows.

### Metadata memory after open (1M documents, 3 fields, tiny vectors)

Current resident memory in a fresh process: 225 MB before the presence list was removed, 206 MB after. The on-disk log
was 257 MB, about 270 bytes per document. The largest remaining in-memory item is a second id map: Zenar keeps one from
id to row, and TurboVec keeps its own. Removing either needs a change to the library or the layout.

### Memory, 1M rows, dim 384

Measured with `/usr/bin/time -l` around the `bench` example (ingest, then all searches, same process). Peak resident
set size was 415 MB with TurboVec 1.1.2 (410 MB with 1.0.0). The vectors are 4-bit TurboVec codes, about 192 MB at this size (1M × 384 × 4 bits). Each indexed
field keeps 4 bytes per document in its value postings, and 8 bytes per document that has the field in its presence
list (ARCHITECTURE.md, "Metadata indexes"). For three fields at 1M rows, that is about 36 MB. The 36 MB is computed from
the struct sizes, not measured. The rest is the document log index, the id map and the per-row bookkeeping. Raw float32 vectors would need about 1.5 GB for the same rows.
The breakdown of the remainder has not been measured by component.

### Where the compound filter's time goes (`compound_breakdown`, 1M rows, TurboVec 1.0.0)

| Step | Time |
|---|---|
| Document store: compound filter (90,000 candidates) | 0.37 ms |
| Liveness check of the candidates against the vector index | 0.45 ms |
| Vector search over the candidates (allowlist) | 1.95 ms |
| Vector search, no filter, for comparison | 1.09 ms |

The document-store step was 3.6 ms before the candidate-driven evaluation (see below). The vector search over the
candidate allowlist is TurboVec's, and it costs more than the same search with no filter.

### Document store, filter steps only (`filter_profile`, 1M rows)

| Step | Time | Matches |
|---|---|---|
| `tenant = A` | 0.28 ms | 100,000 |
| `category IN (faq, product)` | 1.20 ms | 400,000 |
| `price >= 100` | 2.62 ms | 900,000 |
| Compound (all three) | 0.51 ms | 90,000 |

The compound row is the candidate-driven evaluation: the smallest leaf lists its rows, and the other leaves are checked per
row through their value codes. The standalone leaves still materialize their full result. Before this change the compound
was 3.6 ms.

### What the numbers say

- Field codes are stored only for rows that have the field. A collection with thousands of fields no longer costs
  fields times documents: 877 MB became 54 MB for 5,000 fields and 20,000 documents. The price is about 0.9 ms on the
  compound filter at 1M rows, from 2.96 ms to 3.84 ms.

- Unfiltered and selective (0.1%) search are close to pure TurboVec cost. The 0.1% filter is faster than the unfiltered
  search at 10K rows, because it returns few candidates.
- A broad filter costs what its matching rows cost in the vector search. The compound filter is now about 3x the unfiltered
  search at 1M rows, down from about 6x. Most of what remains is the allowlist search.
- The gate in the design spec (section 90) asks that the metadata step not dominate filtered search at 1M rows. The
  document-store step of the compound filter is now a small part of its cost.

### Known gaps

- The per-component memory breakdown is not measured.
- Open time is linear in the log. A persisted index would avoid the replay. It would also need its own recovery, and it is
  not built.
- The allowlist search in TurboVec costs more per candidate than an unfiltered search. Changing that would mean changing
  TurboVec.

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
