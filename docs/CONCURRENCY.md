# Concurrency and write latency

Status: **Phase 1 and group commit implemented.** Log fsync runs outside the collection lock. Writes queue, and a leader
syncs the writes that have queued as one group. The open items are listed at the end. Measurements are from the reference machine (Apple arm64 laptop, macOS,
release build). Raw numbers and reproduction commands are in BENCHMARKS.md, "Concurrency and write cost".

## Summary

Before Phase 1, a continuous writer held the collection's write lock for about 8.6 ms per 50-row batch, and about 89% of
that was two fsyncs. With 8 readers, search fell from about 31,000 per second to about 700 per second. Readers were not
starved indefinitely (their p99 was about three batch times). Their throughput collapsed because they could hold the
lock only in the gaps between batches.

Phase 1 moves the log fsync out of the lock. A write now holds the lock for its index sync and in-memory work, about
4.8 ms per batch. Results with a continuous writer, against the same build before the change. Each cell is the median of three runs:

| Measure | Before | Phase 1 |
|---|---|---|
| Search throughput, 8 readers | 699 / s | 6,541 / s |
| Search p50 / p95 / p99 | 11.1 / 20.7 / 29.6 ms | 0.63 / 5.1 / 5.4 ms |
| Writer batches / s | 92 | 117 |
| Writer batch p50 | 10.9 ms | 8.4 ms |

Without a writer, search with 8 readers is unchanged within run-to-run noise (30.9k / s before, 32.1k / s after). A
single writer's `add_many` of 50 rows costs the same within noise: 7.3 to 8.5 ms before, 7.3 to 9.0 ms after.

The model in the earlier proposal estimated about 15,000 search per second. The measured figure is 6,500. The
difference is the cost of readers waiting for the writer's index sync and for the writer to take the lock back. The
model left those out.

## Group commit

Writes from several threads share one index sync and one log sync. A write queues itself and returns only after its own
group's sync, or after the group's rollback. The queue replaces the writer gate of Phase 1.

- **Queue and leader.** Each write is a request in the collection's queue, with its own result slot. A writer that finds
  no leader becomes the leader and takes the queue as its group. Other writers wait on a condition variable until their
  result is set. Groups run one at a time, so a group's sync and rollback never overlap another group's.
- **Linger.** A leader that finds fewer writes queued than there were writer threads seen in the last 20 ms waits up to
  2 ms for the rest. Writers that arrive join the group. A lone writer never waits. The linger is needed. Without it,
  when a group ends, the waiting writer takes leadership at once, before the writer that just finished has queued its
  next write, so every group had one write and nothing was shared.
- **One group, in order.** Adds in the group run together: each is validated against the collection and against the
  adds before it, staged into the log, and added to the index. Then one index sync, one log sync (with the lock
  released), and one commit of every batch. Deletes, flushes and calibrations run alone, after the adds queued before
  them.
- **Per-write results.** A write that fails its own validation (a duplicate id, a bad vector) gets its own error. The
  others in the group are unaffected. An `add` that does not reach a sync boundary is committed without a sync, as before.

Measured with eight readers and a continuous writer, median of three runs (BENCHMARKS.md, "Readers and writers"): writer
throughput 116, 226 and 400 batches/s with one, two and four writers. The Phase 1 build stays near 119 at every count,
because it has one write in progress at a time. Writers alone reach 135, 237 and 421 batches/s. Search throughput with readers
falls as writers are added: 6,330, 4,323 and 2,626 per second. The leader holds the index lock while it stages the
whole group, so readers wait longer. This is the trade-off, stated plainly: group commit trades reader throughput for
writer throughput. At four writers against Phase 1, writes gain 3.4 times and searches lose 2.3 times.

The group sizes match the design. At four writers, 499 of 503 groups in a 5 second run had four writes. The groups hold
the lock for about 2 ms per section, twice per group.

## Current architecture

### Locks

- **Writer gate** (`writers: Mutex<()>`): one per collection. Taken first by every write (add, add_many, delete,
  delete_many, flush, calibrate). Never taken by readers. Always taken before the state lock.
- **State lock** (`state: RwLock<State>`): one per collection. Readers take it shared. A write takes it exclusive in
  three short sections: before the index sync, after the log sync, and for the commit.
- **Database**: a lock over the collection map, a catalog mutex, and the `LOCK` file for the process.
- **Writer admission** (`Admission` in `collection.rs`): a count of writers that have announced they want the state lock,
  under its own mutex, and a condition variable. A writer announces before it calls `write()` on the state lock, and
  releases the announcement as soon as `write()` returns, on success, on error and on unwinding (an RAII guard). A reader
  calls `admit_reader` before it calls `read()`: it waits while any writer is announced, and does not wait otherwise.
  The gate closes at the announcement, not when the writer starts to block on the lock. Every acquisition of the state
  lock goes through it: `read_state`, `write_state_op` (stage, commit, delete, flush, calibrate, reconcile, swap, persist),
  the poison write in a group that stopped part way, and `close`.

  **Lock order.** The admission mutex is taken alone and released before the state lock is taken. It is never held while
  a guard is waited for, so the two cannot form a cycle. Search, document fetch, and the other read work run under the
  state read guard, not under the admission mutex.

  **Guarded code cannot reach the lock.** The code that runs while a state guard is held is written as methods on
  `Core`, the part of a `Collection` that holds the data and no reference to the collection or its lock. Each such
  method receives the guard or the state explicitly. The lock is taken only by `with_read` and `with_write`, which pass
  `Core` and the guard to a function pointer. A function pointer cannot capture `self`, so a call that would take the
  lock again cannot be written inside guarded code; the compiler rejects it. `lock_architecture_tests` in
  `collection.rs` also checks the structure in the source.

  **Recursive reads are not allowed.** A thread that holds a `StateRead` must not take another guard of the same lock. A
  second read waits at the gate for a writer that waits for the first guard: a deadlock. A second write waits for the
  thread's own guard. Debug builds assert the read rule (`READS_HELD`). Release builds do not check it at run time; the
  structure above is what keeps guarded code from taking a second guard. No code path takes a second guard today;
  `retrieve` releases its guard before it searches, and `describe` passes its guard down rather than taking another.

  **What is guaranteed.** Once a writer is announced, readers that have not yet passed the gate wait, so an announced
  writer gets the state lock after the readers that were already past the gate release it. A reader that finishes a
  search re-enters the gate and cannot reacquire ahead of an announced writer. Writers that announce while others are
  announced share the gate. The guarantee is not first-come-first-served among writers, and it is not a fairness bound
  for readers: while writers keep announcing, readers wait. Here the writers that reach the state lock are the group
  leader, a swap, and the other short operations, so the announced count stays small. The measured effect is in
  `docs/BENCHMARKS.md`, "Writer admission".

### Who takes which lock

| Mode | Operations |
|---|---|
| Writer gate | `add`, `add_many`, `delete`, `delete_many`, `flush`, `calibrate` |
| State, shared | `dimension`, `bit_width`, `get`, `get_many`, `count`, `search` (through `search_counted`), `describe`, `find_values`, `retrieve`, `fields_prompt`, `tool_spec` |
| State, exclusive | `add`, `add_many`, `delete`, `delete_many` (three sections each), `flush` (whole run), `calibrate`, `reconcile` (on open), `persist` (on create) |

### What a write does now (before group commit)

| Step | Lock | Disk I/O |
|---|---|---|
| Validate, stage log bytes, add vectors in memory | writer gate, state (exclusive) | log write (page cache) |
| Index sync | state (exclusive) | index write and fsync |
| Duplicate the log file handle | state (exclusive) | none |
| Log sync | writer gate only | log fsync |
| Check closed and poisoned, commit documents | writer gate, state (exclusive) | none |

Deletes follow the same shape. The catalog writes that record lengths, and compaction, still run under the state lock in
the first and third sections.

### Visibility

Between the index sync and the commit, a write's vectors are in the index and its documents are not committed.

- **Filtered search** is unaffected. Candidates come from the document indexes, which change only at commit.
- **Unfiltered search** ranks over the index and drops the hits that have no committed document. It asks TurboVec for
  `top_k` plus at most 64 extra (`min(pending, 64)`, the bounded over-fetch). If the first pass returns fewer than
  `top_k` committed documents, and more than 64 vectors are pending, the search retries once with the full
  `top_k + pending`. Both passes run under one read guard, so they see the same index and the same pending count. The
  retry is the only case that asks for more than 64 extra, and it runs only when the first pass cannot supply `top_k`
  committed results. The result returns `top_k` committed documents whenever that many exist, and all committed
  documents that the index holds otherwise. Arithmetic saturates, so a very large `top_k` cannot overflow.
  `SEARCH_OVERFETCH_CAP` in `collection.rs` holds the cap of 64.
- **`get` and `count`** reflect committed state only, as before.
- **A delete** removes its vector in the first section, so search stops returning the document at once. `get` still
  returns it until the commit. Search skips a removed vector without shrinking the result, since the removed vector
  is never in the index.

## Failure handling

Group commit adds these cases. The earlier cases still apply to each group.

- **A group's log sync fails:** every write in the group gets the error, and every write is rolled back, as one batch.
  The vectors are removed, the index is synced again, and the log is truncated to the start of the group. The next group
  proceeds normally. Test: `group_that_fails_its_log_sync_rolls_back_every_member`.
- **The rollback cannot re-sync the index, or cannot truncate the log:** the collection is poisoned. Its disk and memory
  may disagree, so it refuses every operation until it is reopened, and the reopen reconciles. A test covers the
  re-sync case: `failed_add_with_failing_index_poisons_and_reopen_leaves_no_trace`.
- **A write is rejected while its group is staged:** it gets its own error, and the other writes commit. Test:
  `grouped_writes_fail_individually_and_the_rest_commit`.
- **A leader that panics part way through a group:** the group's staged writes may be in the index and the log but not in
  memory. The guard poisons the collection, finishes every write the group left without a result with an error, and
  releases the leader role, so no writer waits for ever. A reopen then reconciles the files. Test:
  `leader_panic_poisons_releases_the_queue_and_reopen_recovers`.

- **Failure before the index sync:** unchanged. The batch is undone under the lock.
- **Index sync succeeds, log sync fails:** the lock is taken again and the batch is rolled back, as before. The rollback
  removes the vectors, syncs the index again, and truncates the log. A failed re-sync or truncation poisons the
  collection (see the group case above).
- **Collection closed during the log sync:** the commit returns `CollectionClosedError`. The log bytes were synced, so the
  write may be applied on reopen. This is the rule that already applies to errors (STORAGE.md).
- **Delete whose log sync fails:** the collection is poisoned, as before. Reopening completes the delete, because the
  index was synced without the vector.
- **Flush:** holds the lock throughout, so no write is between its syncs.

Tests that cover these paths are listed in STORAGE.md, "Crash tests". The window tests use a barrier to stop a write
between its index sync and its commit, and check what a search or a delete sees there. The failure tests inject a log
sync error in that window.

## Recovery implications

None on disk. The files, the catalog, the recorded lengths and the reopen rules are unchanged. A crash in the log-sync
window leaves the state of a crash in the middle of an older `add_many`: vectors may be durable without their documents,
and reconciliation removes them.

## Benchmark method

`concurrent_bench` (100,000 rows, dimension 384, batches of 50, 5 seconds per case, median of three runs). The same
binary was run on the build before Phase 1 and on the build after. Each case reports throughput and p50, p95 and p99.
`write_cost` gives the public-API write costs, including the compaction batch. The full tables are in BENCHMARKS.md.

## Staging outside the state lock (investigated, not implemented)

The question was whether batch preparation can run outside the state lock, so that readers wait less. The leader is the
only writer, and groups run one at a time, so no other write changes the schema, the id sets or the log while a leader
prepares its group. That makes preparation safe to move. The measurement decides whether it is worth moving.

Per 50-row batch, under four writers, from temporary timers around each step (`stage_request`, 400 samples):

| Step | Time | Lock | Movable |
|---|---|---|---|
| Validate (ids, vectors, metadata, duplicates) | 0.055 ms | state, exclusive | yes: read-only, given a read lock |
| Extend the schema (catalog write, only when a batch adds a field) | 0.000 ms in these runs | state, exclusive | no: writes the catalog |
| Encode and write the log bytes (`stage_puts`) | 0.054 ms | state, exclusive | the encoding is pure; the write is not |
| Add the vectors to the index (`store.add`) | 0.225 ms | state, exclusive | no: TurboVec needs `&mut` |
| Index sync per group | about 3.9 ms | state, exclusive | no: TurboVec syncs from its in-memory state |

What could move is the validation and the encoding, about 0.1 ms per batch. At four writers that is about 0.4 ms of a group's
exclusive time, which is roughly 8% of its estimated 5 ms (four staging steps and one index sync). The index sync is most of
the hold, and it cannot move without a TurboVec change. Moving the validation and encoding would need a read-lock preparation
phase, a split of `stage_puts` into encode and write, and a check that the schema and the id sets have not changed. That is
about 100 lines and two more lock sections, for a gain that is inside the run-to-run noise.

**Decision: not implemented.** The design is sound, but the measured gain is below noise, and the added lock sections raise
the risk of an ordering mistake. The next lock-hold target is the index sync, which is an engine constraint.

Ordering and visibility, for the record. Nothing becomes visible before the log sync: documents are committed only after the
log sync in `commit_synced`, whatever happens during preparation.

## Audit

This is the check made before the group-commit change was committed. Each item names where it is enforced and the test
that covers it. The tests are in `crates/zenar-core/src/collection.rs` unless noted.

| Requirement | Where it is enforced | Test |
|---|---|---|
| Pending vectors are never exposed as committed documents | `get` and `count` read only committed documents. Search keeps only hits with a committed document. | `pending_documents_are_invisible_to_get_count_and_search`, `search_in_window_returns_full_top_k_from_committed_documents`, `filtered_search_in_window_never_returns_uncommitted_documents` |
| `top_k` correct, unfiltered and filtered | Unfiltered search over-fetches `min(pending, 64)` and retries with the full `top_k + pending` only when the first pass has fewer than `top_k` committed results. Filtered search ranks only committed candidates, so it needs no extra. | `search_overfetch_tests` in `collection.rs` (no pending, pending at and above the cap, a crowded first pass, fewer committed than `top_k`, no committed documents, rollback, deletes, concurrent writes, and an overflowing `top_k`); `acknowledged_writes_are_seen_and_acknowledged_deletes_are_not` (top_k 10,000 across concurrent writes) |
| Distance metrics | Zenar has one metric: the inner product, which TurboVec computes. No other metric is offered, so there is none to check. The scores are approximate, because the vectors are quantized to 4 bits, so the order is approximate too. | `non_multiple_of_8_dimension_is_padded_and_ranks_correctly` in `storage.rs`. The audit checked which results may appear, not that they match exact scores. |
| Concurrent insert, update and delete do not expose inconsistent results | There is no update operation. Insert and delete are queued and grouped. A search never returns a delete acknowledged before it starts, and never misses an insert acknowledged before it starts. | `acknowledged_writes_are_seen_and_acknowledged_deletes_are_not`, `concurrent_inserts_and_searches_return_committed_full_results`, `tests/test_concurrency.py` |
| A failed log sync rolls back the affected writes | `rollback_batch` removes the vectors, re-syncs the index, and truncates the log to the group's start. | `failed_log_sync_in_window_rolls_back_and_stays_usable`, `group_that_fails_its_log_sync_rolls_back_every_member` |
| A rollback failure poisons the collection | A failed re-sync or a failed truncation sets the poison flag. A panic in the leader poisons too. | `failed_add_with_failing_index_poisons_and_reopen_leaves_no_trace`, `failed_add_many_with_failing_index_poisons_and_reopen_is_all_or_nothing`, `failed_undo_poisons_and_reopen_removes_the_orphan_frames`, `leader_panic_poisons_releases_the_queue_and_reopen_recovers` |
| Group commit keeps the durability guarantees | A write returns only after its group's log sync (or its rollback). The `add` with no sync, which is not a sync boundary, is unchanged. | `acknowledgement_comes_after_the_log_sync`, the kill -9 tests in `tests/test_robustness.py` |
| A rejected group member fails on its own | Validation happens per write, against the collection and the group's earlier writes. | `grouped_writes_fail_individually_and_the_rest_commit` |
| Crash recovery of a partly completed group | After a crash between the index sync and the log sync, the whole group is present. After a power loss inside the log write, a prefix of the batch is present and the stray vectors are removed. | `crash_after_group_index_sync_keeps_the_whole_group`, `torn_group_log_keeps_a_prefix_and_removes_stray_vectors` |
| No write is acknowledged before its required sync | The result is set only after the log sync in `commit_synced`. | `acknowledgement_comes_after_the_log_sync` |

What the audit did not cover, and the remaining risks:

- The audit found one gap, which is fixed: a rollback whose index re-sync failed was ignored, so the index could hold
  vectors the log did not. It now poisons the collection. Two existing tests that assumed a usable collection after that
  failure were changed to the poisoned state.
- Power loss is not tested on hardware. Crashes are simulated by copying the directory, and by cutting the copied log.
- Ranking is approximate. The vectors are 4-bit quantized, so the top-k order can differ from exact inner products. The
  audit checked membership and visibility, not exact order.
- Stage failures are not injected. A failed `stage_puts`, which poisons the collection, has no test.
- Groups do not span collections. Each collection has its own queue.
- Single process only. The `LOCK` file keeps a second process out.

## Open items, in priority order

0. **Staging under the lock.** The leader stages every batch of its group while it holds the index lock. Each batch's
   encoding and vector add could run before the lock is taken. This is the open item most likely to recover reader
   throughput at several writers. It is not measured yet: the add itself is about 0.8 ms per batch.

1. **Compaction holds the lock only for its swap.** The build runs without the lock. The swap holds it for about 20 ms
   at 200,000 live documents, down from 86 to 100 ms for the reopen-based compaction. Of the 22 to 25 ms swap hold, 23 ms
   is filesystem time: the tail sync, the catalog writes, the rename and the directory sync (`docs/BENCHMARKS.md`,
   "Write-lock holds by operation"). The design is in docs/R3.md, and the argument for deletes during the build is in
   docs/COMPACTION.md.
2. **Deletes write the catalog twice under the lock.** Measured per delete group: the catalog write before the log is
   10.8 ms of filesystem time in a 11.1 ms hold (`delete_stage`), and the write after the commit is 7.2 ms of a 7.0 ms
   hold (`delete_commit`). You chose to keep exact refusal. Its cost stays unless the recorded-length scheme changes, and
   this document does not propose a change that keeps the bound exact.
3. **The first index persist of a new database holds the lock for 80 ms.** `create_collection` syncs the new index
   under the collection lock. It is one-time, and the collection is not visible to other threads then, so it blocks no
   reader. The next create takes 7 ms. Noted so that the figure is not mistaken for steady-state behaviour.
3. **Index sync holds the lock.** About 3.9 ms per batch. TurboVec's sync takes `&mut self` and syncs from its in-memory
   state, so the fsync cannot leave the lock without a change to TurboVec. This is now the dominant hold.
4. ~~Group commit~~. Built. See "Group commit" above.
5. **Flush holds the lock during its two fsyncs.** Rare in a write loop, but a long flush stalls readers.

## Decisions

- Phase 1 (log sync outside the lock, writer gate, visibility as above): approved and implemented.
- Delete path: exact refusal kept. No change to the shortened-file check.
- Multiple concurrent writers: a supported workload. Group commit is to be evaluated and built where it gives a measured
  gain, without weakening durability.
- Durability: unchanged. No write is acknowledged before its required sync, and macOS full sync is kept.
