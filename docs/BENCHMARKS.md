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

One `delete` costs about 25 ms on the reference machine (`write_cost`, 10,000 rows): 24.8 ms measured. Its cost is
the index sync and the log sync (about 7.5 ms together), plus two writes of the catalog (about 7.7 ms each, measured with
`catalog_cost`). Each catalog write is a temporary file, a file sync, a rename and a directory sync. The earlier figure of
8 ms predates those two catalog writes. `delete_many` pays the index and log syncs and the catalog writes once per batch: 23 to 31 ms per 1,000 ids.
Use it for bulk removal.

A delete also holds the write lock while it compacts. Compaction runs when dead rows exceed live rows and 4096. See the
"Concurrency and write cost" section for the measured stall.

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

## Concurrency and write cost

Reader and writer behaviour on one collection, and the cost of each write step. Three builds are compared:

- **original**: commit d9ffc5e, one writer at a time through the collection lock. The lock covered both syncs.
- **Phase 1**: commit 4dc3d67, the log sync outside the lock, writes serialized by a writer gate.
- **group commit**: this change. Writes queue, and a leader syncs the queued writes as one group.

Reference machine, release build, dimension 384, 100,000 rows, batches of 50 rows, 8 reader threads, 5 s per case.
Every number is the median of three runs for its column. Search latency is per query. Batch latency is per `add_many`
of 50 rows, including its wait for the state lock. Reproduce with `concurrent_bench -- 100000 384 8 5 50 <writers>`.

### Readers and writers (continuous writer)

| Writers | Build | Search/s | Search p50 | p95 | p99 | Writer batches/s | Batch p50 | p95 | p99 |
|---|---|---|---|---|---|---|---|---|---|
| 1 | original | 699 | 11.1 ms | 20.7 ms | 29.6 ms | 92.2 | 10.9 ms | 12.9 ms | 15.9 ms |
| 1 | Phase 1 | 6,165 | 0.62 ms | 5.04 ms | 5.96 ms | 118.5 | 8.2 ms | 9.7 ms | 14.9 ms |
| 1 | group commit | 6,330 | 0.63 ms | 5.03 ms | 5.86 ms | 116.1 | 8.2 ms | 10.7 ms | 15.4 ms |
| 2 | original | 508 | 14.0 ms | 31.5 ms | 45.2 ms | 101.8 | 18.0 ms | 36.0 ms | 50.9 ms |
| 2 | Phase 1 | 5,934 | 0.64 ms | 5.05 ms | 5.99 ms | 119.9 | 16.1 ms | 20.3 ms | 25.7 ms |
| 2 | group commit | 4,323 | 1.00 ms | 5.82 ms | 6.60 ms | 225.9 | 8.6 ms | 10.4 ms | 14.5 ms |
| 4 | original | 315 | 25.8 ms | 42.0 ms | 50.5 ms | 108.6 | 32.0 ms | 71.9 ms | 92.8 ms |
| 4 | Phase 1 | 5,919 | 0.65 ms | 5.07 ms | 6.06 ms | 118.8 | 32.7 ms | 39.9 ms | 43.4 ms |
| 4 | group commit | 2,626 | 2.03 ms | 7.13 ms | 8.41 ms | 400.0 | 9.5 ms | 13.7 ms | 16.4 ms |

With no writer, search with 8 readers is unchanged across the builds within run-to-run noise: about 31,000 to 34,000
per second. Reader throughput without a writer was not repeated for the group-commit build; the code on that path is
unchanged.

### Writer throughput without readers

| Writers | Original | Phase 1 | Group commit |
|---|---|---|---|
| 1 | 116.2 batches/s | 132.6 batches/s | 135.0 batches/s |
| 2 | 126.7 batches/s | 133.5 batches/s | 236.8 batches/s |
| 4 | 128.7 batches/s | 133.7 batches/s | 421.4 batches/s |

Batch p50 without readers: 8.8 ms, 7.1 ms and 7.0 ms at one writer; 15.1, 14.8 and 8.0 ms at two; 30.9, 29.1 and 9.0 ms at four.
Group commit at four writers is 3.2 times Phase 1 and 3.3 times the original.

### What the groups did (group commit build, counters from `Collection::write_stats`)

Each group is one index sync and one log sync. The counters are cumulative for one run, and the run covers the readers
case above.

| Writers | Groups per 5 s run | Writes per group | Group sizes seen | Lock hold per hold, mean | Reader wait, mean |
|---|---|---|---|---|---|
| 1 | about 585 | 1.00 | all 1 | 1.9 ms | 0.7 ms |
| 2 | about 570 | 2.00 | 2 (all but one group) | 2.0 ms | 0.9 ms |
| 4 | about 507 | 3.99 | 4 (about 99% of groups) | 2.1 ms | 1.5 ms |

Each group takes the state lock twice: once to stage and sync the index, and once to commit. So a group holds the lock for
two sections, and each is about 2 ms on average. Without readers the mean hold is 2.0 ms at one writer, 2.5 ms at two,
and 2.6 to 3.0 ms at four. The largest single hold since the collection was opened was about 90 ms, from the seeding batches. The
counters' maxima are cumulative, so they do not describe one case.

### Write costs through the public API (`write_cost`, 100,000 rows)

| Operation | Original | Phase 1 | Group commit (1 writer) |
|---|---|---|---|
| `add`, `durable=True` | 7.9 ms | 7.8 ms | 8.3 ms |
| `add_many`, 50 rows | 7.8 ms | 7.7 ms | 8.3 ms |

Five alternating runs of Phase 1 and the group-commit build give `add_many` means of 7.7 and 8.0 ms, with runs from 6.5 to
8.8 ms on both. The single-writer path is about 4% slower in the mean. That is within the variation between runs.

The delete path is unchanged: one delete is about 24 ms, and a compacting `delete_many` of 1,000 ids is 115 ms at
200,000 rows and 261 ms at 1,000,000 (compaction holds the lock; see ROADMAP.md).

### Trade-off: group commit against reader throughput

Group commit trades reader throughput for writer throughput, and this is measured, not estimated. A group stages every
batch in it while holding the state lock, so the lock is held longer per group as the group grows. At four writers each
group holds the lock for about 2 ms per section, twice per group. Readers then wait longer on average (1.5 ms against
0.7 ms at one writer), and search throughput falls from about 6,300 per second at one writer to about 2,600 at four. The
writers' gain is larger: 400 batches per second at four writers, against 116 at one. Work done per second rises, and
each search waits longer. Moving the encoding of each batch out of the lock is the next thing to measure. A bounded
number of writers, or a cap on group size, would limit the reader cost if it matters for a workload.

### Write steps (`write_breakdown`, 50-row batch, sequential)

| Step | Time per batch |
|---|---|
| Encode and write the log bytes | 0.09 ms |
| Add 50 vectors in memory | 0.80 ms |
| Index sync (write and fsync) | 3.93 ms |
| Log sync (fsync) | 3.71 ms |
| Commit (make visible, in memory) | 0.03 ms |
| Total | 8.57 ms |

Both syncs are about one fsync each (3.7 ms, measured with `write_breakdown`'s log sync on this machine). They are 89% of a
batch.

### Write cost through the public API (`write_cost`)

| Operation | Time |
|---|---|
| `add`, default durability | 0.01 ms |
| `add`, `durable=True` | 7.3 to 8.0 ms |
| `add_many`, 50 rows | 7.3 to 9.0 ms (7.3 to 8.5 ms before Phase 1) |
| `delete`, one id (10,000 rows) | 24.8 ms |
| `delete_many`, 1,000 ids, no compaction | 23 to 31 ms |
| `delete_many`, 1,000 ids, the batch that compacts (200,000 rows, 100,000 live after) | 115 ms before the sequential rewrite |
| `delete_many`, 1,000 ids, the batch that compacts (1,000,000 rows, 500,000 live after) | 261 ms before, 127 to 132 ms after the sequential rewrite |

Compaction holds the write lock for the whole rewrite and the reopen that follows it. Its cost grows with the live rows
left. The rewrite reads the log once, from the front (it used to read each live row with its own `pread`). The reopen
replays the new log, which is most of what is left. `docs/COMPACTION.md` has the measurements and the design for moving the
rewrite out of the lock.

### Compaction and catalog costs (`compaction_probe`, `catalog_cost`)

Probe: 1,000,000 documents with three metadata fields, half of them deleted (500,000 live).

| Step | Time |
|---|---|
| `compact()`, before the change (per-row reads) | 366 to 375 ms |
| `compact()`, after the change (one sequential read) | 187 ms |
| Sequential rewrite alone (read, filter, write, no sync) | 47 ms |
| Reopen with replay of the 500,000 live rows | 151 to 159 ms |
| Sync of the rewritten file | 3.5 to 5 ms |
| One catalog write (atomic replace of `catalog.json`) | 7.6 to 7.8 ms |

A delete makes two catalog writes under the lock, so it pays about 15 ms of catalog time. That is measured, and it accounts
for most of the difference between a delete and the two syncs it also makes.

### Syncs per group (`group_cost`, no threads)

Four batches of 50 rows, written in one pass with one index sync and one log sync per batch, against one pair of syncs
for the whole group. This is the saving that group commit makes on the sync path.

| Group size | One sync pair per batch | One sync pair per group | Speed-up |
|---|---|---|---|
| 1 | 9.7 ms per batch | 9.2 ms per batch | 1.05x |
| 2 | 9.1 ms per batch | 5.1 ms per batch | 1.78x |
| 4 | 8.9 ms per batch | 2.9 ms per batch | 3.07x |

### Memory of dead rows and of the vector index (`memory_probe`, `vector_memory_probe`)

Heap in use, counted with a global allocator (resident size is not used, because allocators keep freed pages).
`memory_probe` runs a sliding window: each round adds 100,000 documents and deletes the oldest 90,000. Reproduce with
`memory_probe -- 6 retain` and `memory_probe -- 6 compact`.

| Round | Live | Dead | Heap, no compaction | Heap, compaction with reopen |
|---|---|---|---|---|
| 1 | 10,000 | 90,000 | 12.8 MB | 4.3 MB |
| 3 | 30,000 | 270,000 | 36.5 MB | 6.1 MB |
| 6 | 60,000 | 540,000 | 65.7 MB | 9.0 MB |

A dead row costs about 105 bytes of heap. Without compaction, the store grows by about 9.5 MB per round. `vector_memory_probe`
(200,000 vectors, dimension 384): 46.7 MB after the adds, 46.7 MB after removing 90%, and 50.9 MB after adding 180,000 new
vectors. The vector index reuses its freed capacity, so only the document store accumulates dead rows. `docs/COMPACTION.md`
discusses what this means for the compaction design.

### Delta scan cost (`delta_scan`, dimension 384, naive scalar loops)

| Vectors scanned per query | Time per query |
|---|---|
| 1,024 | 0.17 ms |
| 4,096 | 0.47 ms |
| 16,384 | 1.86 ms |

Compare with a single unfiltered search of 0.08 ms (1 reader, above). A scan of recent vectors would cost each reader
this much on every query, so it is the wrong way to avoid the write lock (docs/CONCURRENCY.md, option G).

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
