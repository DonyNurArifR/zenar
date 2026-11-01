# Concurrency and write latency: design proposal

Status: **proposal**. No locking or durability code has changed. The measurements are from the reference machine
(Apple arm64 laptop, macOS, release build). The raw numbers and reproduction commands are in BENCHMARKS.md, "Concurrency
and write cost".

## Summary

Under a continuous writer, search throughput on one collection falls from about 33,000 to about 680 per second with eight
readers. The cause is the write lock. Each batch holds it for about 8.6 ms, and about 89% of that is two fsyncs made while
the lock is held. A continuous writer therefore keeps the lock held about 99% of the time, and readers get a turn only in
the gaps between batches.

The readers are not starved in the strict sense. Their p99 is 28 ms, about three batch times, so no reader waits
indefinitely. What collapses is throughput: the readers' share of the lock falls to about 2%.

The proposal, in order:

1. **Phase 1 (locking change, no file-format change).** Move the log fsync out of the collection lock. A per-collection
   writer gate serializes writers, and documents become visible only after their log bytes are synced. Estimated lock
   hold for a 50-row batch falls from 8.6 ms to about 4.9 ms, and reader share under a continuous writer rises from about
   2% to about 45%. These are model estimates until a prototype is measured.
2. **Delete path (behaviour change; needs a decision).** A delete writes the catalog twice under the lock, about 17 ms of
   its 25 ms. Option J2 below removes most of that cost and weakens one guarantee in a documented way.
3. **Compaction (lock change, later).** Compaction rewrites the whole log under the lock: 115 ms at 100,000 live rows and
   261 ms at 500,000. It should move out of the critical section.
4. **Group commit.** Only worth building if several writers are measured to matter. For one writer it gives nothing.

Nothing here is implemented yet. Phase 1 changes the lock protocol, so it waits for the decision recorded in ROADMAP.md.

## Current architecture

### The lock

Each `Collection` has one `RwLock<State>`. `State` holds the `VectorStore` (TurboVec), the `DocStore` (log and indexes),
the count of unsynced adds, the poison reason, and the closed flag. Two small `Mutex` caches hold field statistics and
scoped discovery results. Every write takes the write lock and clears both caches before it changes anything, so a reader
can never keep statistics from before a write.

The database has its own `RwLock` over the collection map, a `Mutex` inside the catalog, and an advisory `flock` on
`LOCK` for the life of the process. None of these is on the search path.

### Who takes which lock

| Mode | Operations |
|---|---|
| Shared (read) | `dimension`, `bit_width`, `get`, `get_many`, `count`, `search` (through `search_counted`), `describe`, `find_values`, `retrieve`, `fields_prompt`, `tool_spec` |
| Exclusive (write) | `add`, `add_many`, `delete`, `delete_many`, `flush`, `calibrate`, `reconcile` (on open), `persist` (on create) |

Reads never write to disk, and searches never trigger a lazy repack of the vector layout. TurboVec patches its blocked
layout on each add, so the first search after a write costs the same as any other. Readers that run in parallel share the
index without copying it.

### What the write lock covers

| Operation | Under the write lock | Disk I/O under the lock |
|---|---|---|
| `add` (sync boundary) | validate, stage log bytes, add vector, index sync, log sync, commit | log write, index write and fsync, log fsync |
| `add` (no sync) | validate, stage log bytes, add vector, commit | log write |
| `add_many` | validate, stage log bytes, add vectors, index sync, log sync, commit | log write, index write and fsync, log fsync |
| `delete` | catalog write (lower lengths), remove vector, index sync, stage tombstone, log sync, compaction if due, catalog write (record lengths) | two catalog writes (each with a file sync and a directory sync), index write and fsync, tombstone write, log fsync, and compaction when due |
| `delete_many` | the same, once per batch | the same as `delete`, once per batch |
| `flush` | index sync, log sync, catalog write | index write and fsync, log fsync, catalog write |

## Measurements

Reference machine, release build, dimension 384. Full tables in BENCHMARKS.md.

- **One fsync** (`write_breakdown`'s log sync, a 50-row append): 3.7 ms.
- **One 50-row batch** (`write_breakdown`, sequential): 8.6 ms. Encode 0.09, vector add 0.80, index sync 3.93, log sync
  3.71, commit 0.03.
- **Readers with a continuous writer** (`concurrent_bench`, 100,000 rows, 8 readers, median of three runs): search
  679 per second (p50 11.1 ms, p99 27.6 ms), against 33,455 per second without a writer (p50 0.24 ms). The writer runs at
  90 batches per second, against 116 without readers.
- **Readers with a writer at 1 or 10 batches per second:** search at 95% and 85% of the no-writer rate. The write lock is
  free most of the time, so readers are barely affected.
- **Delete** (`write_cost`): 24.8 ms per id. The two fsyncs of the index and the log account for about 7.5 ms, measured
  above. The remaining 17 ms is the two catalog writes, which each sync a temporary file and a directory. That figure is
  by subtraction; the catalog writes were not timed on their own.
- **Compaction** (`write_cost`, the `delete_many` that compacts): 115 ms at 100,000 live rows, 261 ms at 500,000.

### Reading the numbers

- The writer's batch time includes its wait for the write lock. With readers present, batches take about 11 ms
  against 8.6 ms alone. Readers slow the writer too, but less than they are slowed.
- The write lock is fair enough. With a continuous writer, the reader p50 is about one batch time and p99 about two to three.
  Readers do not wait for an unbounded time. They lose throughput because the lock is held for most of each cycle.
- Search cost does not rise after a write. With 8 readers the p50 is 0.238 ms with a 1-batch-per-second writer, against
  0.236 ms without one.

## Engine constraints (TurboVec 1.1.2)

These decide which options are possible without changing TurboVec.

1. **Mutation needs exclusive access.** `add`, `remove` and `sync` take `&mut self`. `search` takes `&self`. Readers can
   share the index, but no write can overlap a search.
2. **`sync` is one call.** It writes the new units to the index file and fsyncs them. The engine offers no split into
   "prepare the bytes" and "fsync them later". So the index fsync cannot leave the lock without a change to TurboVec.
3. **`sync` reads the live state.** It reads the in-memory index through closures and checks the file's identity through
   a cursor. Its output must therefore match the state at the moment it runs, which makes it a write.
4. **Searches see a consistent index.** A search that starts after an add sees the new vector. The index has no
   snapshot or versioning.
5. **Removal is in place.** `remove` changes the index at once, so there is no older version for readers to keep.

Constraints 1 to 3 mean the index fsync stays under the lock in any design that does not change TurboVec. The log fsync is
different: it depends on the log bytes, not on the in-memory state. It can run outside the lock.

## Options

The estimates for the write path use the measured steps. "Model" marks an estimate from those steps, not a benchmark.
Reader throughput is the no-writer rate (33,000/s) times the readers' share of the lock.

| Option | Reader throughput with continuous writer | Writer throughput | Tail latency | Memory | Lock contention | Crash consistency | Durability | Complexity | Corruption risk |
|---|---|---|---|---|---|---|---|---|---|
| **A. Status quo** | 680/s (measured) | 116 batches/s | p99 28 ms | none | 99% of time | unchanged | unchanged | none | none |
| **B. Log fsync outside the lock** | about 15,000/s (model: 45% share) | about the same (model) | p99 about 10 ms (model: about 2 holds of 4.9 ms) | none; one extra fd per collection | about 57% of time (model: 4.9 of 8.6 ms) | same states as today; see below | unchanged | medium | medium: new interleavings, covered by the writer gate and tests |
| **C. Group commit (N writers)** | scales with the gain in B for N writers; no gain for one writer | up to about N× until CPU-bound | each write waits up to one group | a queue of pending batches | fewer, longer holds | same states as today | unchanged; each waiter is acked after the shared fsync | high | medium: the group's failure must fail every member |
| **D. Fair or write-preferring RwLock** | no change in the duty cycle | no change | shapes tails only; the tail is already about 3 batches | none | none | unchanged | unchanged | low | none |
| **E. Snapshot reads (copy the index per write)** | unchanged for writers | writer copies the index each batch | no lock wait for readers | a full copy of the vector index per write: 192 MB at 1M rows (4-bit) | none | unchanged | unchanged | high; needs a persistent index, which TurboVec does not have | low |
| **F. Versioned metadata snapshots** | unchanged | unchanged | unchanged | copies of the field indexes | none | unchanged | unchanged | medium | low |
| **G. Delta index (recent vectors in a brute-force buffer, plus a vector log)** | reads cost 2 to 5 times more per query (measured scan: 0.17 ms per 1,024 vectors, 0.47 ms per 4,096) | high | low | the buffer until merged | low | a new file, and a merge step that holds the lock | needs a vector log with its own fsync | high | high: a new file format and new recovery rules |
| **H. Acknowledge before the fsync** | high | high | low | none | low | a crash can lose acknowledged writes | **changes the contract** | low | high |
| **I. Plain `fsync` instead of the full sync on macOS** | not measured | not measured | not measured | none | none | weaker: power loss may not reach the platter | **weakens the contract** | low | high |
| **J2. Catalog writes budgeted (see below)** | delete path only | deletes about 3 times faster (model: about 8 ms instead of 25 ms) | deletes hold the lock about 65% less (model) | none | none | the shortened-file check becomes approximate by up to the budget | changes the refusal bound | low | medium |
| **K1. Compaction outside the lock** | removes the 115 to 261 ms stalls | unchanged | removes the compaction tail | a second log during the rewrite | low | new states during compaction; needs a plan for deletes that happen during the rewrite | unchanged | high | medium |

### Why the options are ranked this way

- **B** removes the largest cost (the log fsync, 3.7 of 8.6 ms) with no change to the files. It needs a new locking
  protocol, and that is what the design is for.
- **C** helps only with several writers. The benchmark has one. It is worth doing if agents writing at the same time are a
  real workload.
- **D** does not help. The duty cycle, not the order of waiters, limits the readers.
- **E and F** do not help. The vector index cannot be snapshotted without copying it, and the metadata commit takes 0.03 ms.
- **G** has the wrong cost curve. A reader pays the scan on every query, which is 2 to 5 times a search. It is a rewrite of
  the vector storage, not a lock change.
- **H and I** trade durability for speed. They need an explicit decision and a documented change to the contract in
  STORAGE.md, and I have not proposed either.
- **J2** is the cheapest fix for the delete path. It trades an exact refusal bound for speed. That is a decision for you.
- **K1** removes the largest tail. It is more work than B and should come after it.

## Proposed design (Phase 1: option B)

### Locking protocol

Two locks per collection, taken in this order only:

1. `writers`: a `Mutex<()>` that serializes all writes to the collection. Readers never take it.
2. `state`: the existing `RwLock<State>`.

Readers take `state` for reading, as now. Nothing else changes for them.

An add or `add_many`:

1. Take `writers`.
2. Take `state` for writing. Validate, stage the log bytes, add the vectors, and sync the index (as now). Release `state`.
3. Sync the log without any collection lock. The sync uses a duplicated file handle, so it cannot disturb the append
   handle.
4. Take `state` for writing again. Check the closed flag and the poison flag. Mark the unsynced count, commit the
   documents, and record the lengths if this is a flush. Release `state`, then release `writers`.

Deletes follow the same pattern, with the tombstones written in step 2 and committed in step 4. Flush takes `writers` for
its whole duration.

### Visibility during the window

Between steps 2 and 4 the vectors are in the index but the documents are not committed. Consequences:

- **Filtered search** is unaffected. Candidates come from the document indexes, which change only at commit.
- **Unfiltered search** can see a vector whose document is not yet committed. It then returns fewer than `top_k` results
  when the top hit is such a vector. This is the one visible change, and it must be documented in API.md.
- **`get` and `count`** do not show the document until commit. This matches the current behaviour: they reflect committed
  state only.
- **Deletes** remove the vector at step 2, but the document stays visible until step 4. A search cannot return the
  document during that window, and `get` still can. This is also documented.

### Failure handling

- **Failure before step 2 finishes:** unchanged. The existing undo path runs under the lock.
- **Index sync succeeds, log sync fails in step 3:** take `state` and run the existing undo: remove the vectors, sync the
  index again (best effort), truncate the log. If the truncation fails, poison the collection, as now. This is the same
  code path as today's log-sync failure, moved to step 4.
- **Collection closed during step 3** (for example, `drop_collection` ran): step 4 returns `CollectionClosedError`. The
  log bytes were synced. The files are about to be deleted, so the outcome is the same as a drop after the write.
- **Poisoned during step 3** (another failure path): step 4 returns the poison error and does the undo.

### Recovery implications

None on disk. The files, the catalog, the recorded lengths and the reopen rules do not change. A crash in step 3 leaves
the same state as a crash in the middle of today's `add_many`: vectors may be durable without their documents, or
documents without their vectors. Reconciliation on open already removes both.

### Benchmark methodology

- **Baseline:** the `concurrent_bench` cases as recorded, with the same machine, rows, dimension and batch size.
- **Acceptance (Phase 1):**
  - Reader throughput with a continuous writer at least 3 times the baseline (about 2,000/s). Target: the model's
    estimate of about 15,000/s.
  - Writer throughput no more than 10% lower than the baseline.
  - Readers-only throughput within run-to-run noise (about 15%).
  - Batch p50 with a writer alone within 10% of the baseline, since the second fsync now runs off the lock.
- **Correctness:** the existing tests must pass unchanged: the model-based random test, the fault injections, the kill -9
  tests and the concurrent read/write test. Add: a test that a search during a blocked log sync never returns a
  document before its commit, and a test that a log-sync failure after the index sync leaves the database as today.
- **Additional cases to add:** deletes under readers (`delete_many` batches with readers running), and multi-writer runs
  with two and four writers. The multi-writer runs are the baseline for option C.
- **Compaction stall:** `write_cost` at 1 million rows, as recorded. A Phase 3 change should move it below 10 ms for the
  locked part.

### Expected tradeoffs

- Phase 1 adds one fd per collection and one mutex. Memory is unchanged.
- Readers see a vector before its document for about one fsync. Unfiltered search can return fewer results during that
  window.
- The protocol has more states, so the risk of corruption rises. The writer gate limits that risk: only one writer is in
  the window at a time.
- Phase 1 does not reduce the index fsync, which is 3.9 of the remaining 4.9 ms. Once the log fsync is off the lock, that
  index fsync becomes the dominant hold. The next gain would need an engine change or option G.

## Decisions needed

1. Approve Phase 1 (option B) and its visibility rule, or ask for a different one.
2. Choose the delete path behaviour: keep exact refusal (status quo, 25 ms per delete), or accept a documented bound on
   the shortened-file check in exchange for faster deletes (J2). J2 would persist the recorded lengths lower than the
   truth, by a budget of deletes, and rewrite them every budget deletes.
3. Confirm that multiple concurrent writers are not a target workload. If they are, option C moves up.
4. Confirm that durability stays as it is. Options H and I are not proposed, and both need a decision if they come up
   later.
