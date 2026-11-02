# Compaction: investigation and design proposal

Status: **investigation. The design is now in R3.md**, which supersedes the swap described in option C here.

Status of this page: **proposal, revised after the memory and failure analysis below**. The only change made is a sequential rewrite of
the log, which keeps the same format and the same lock protocol. The background swap is not implemented, and the revision
shows the swap as written cannot reclaim memory, so it is not ready to implement. It changes when the collection lock is held
and what happens on disk, so it waits for review.

## What compaction does today

A delete that leaves more dead rows than live rows (and more than 4096 dead) calls `DocStore::compact`, under the collection
lock, from inside the delete. `compact` does four things:

1. Rewrites the live documents to a temporary log (`*.log.compact`), syncs it, and drops the old handle.
2. Renames the temporary log over the log, and syncs the directory.
3. Reopens the new log, which replays every frame into fresh indexes.
4. The delete records the new lengths in the catalog.

The lock is held for all of it. Readers and writers wait.

## Measurements

Reference machine, release build. Probe: `compaction_probe` (1,000,000 documents, three metadata fields, 500,000 live).
Stall: `write_cost` (1,000,000 rows, no metadata, the batch of 1,000 deletes that compacts). Each row is one measurement.

| Quantity | Before the sequential rewrite | After |
|---|---|---|
| `compact()` total | 366 to 375 ms | 187 ms |
| rewrite part (`compact()` total minus the replay and the sync) | about 210 ms | about 25 ms |
| sequential rewrite alone (read, filter, write; no sync) | not applicable | 47 ms |
| reopen with replay, 500,000 live rows | 151 to 159 ms | unchanged |
| `sync_all` of the rewritten file | 3.5 to 5 ms | unchanged |
| `rename` over the log | 0.2 ms | unchanged |
| one catalog write (atomic replace of `catalog.json`) | 7.6 to 7.8 ms | unchanged |
| per-row `fetch` of 500,000 live rows (one `pread` each) | 280 ms | unchanged |
| in-memory index rebuild, 500,000 rows, three fields (a lower bound: no disk, no decode) | 80 ms | not used |
| compacting `delete_many` batch, `write_cost` 1,000,000 rows | 254 to 265 ms | 127 to 132 ms |

The rewrite used to read each live row with its own `pread`, which is what the per-row `fetch` shows. It now reads the log
once from the front and copies the frames whose offset is a live row's offset. Offsets are matched, not ids, because an id
can appear twice in a log: once live and once deleted. A test covers that case (`compaction_keeps_the_live_copy_of_a_re_added_id`).

The rest of the in-lock cost is the replay (156 ms at 500,000 live rows) and the catalog writes.

## Options

| Option | Lock held (1M rows, 500k live) | Consistency | Risk | Status |
|---|---|---|---|---|
| A. In-lock, per-row reads (the old code) | 366 ms | exact | none | replaced |
| B. In-lock, one sequential read | 187 ms in `compact`; 127 to 132 ms as a delete batch | exact | low: format unchanged | **implemented** |
| C. Background rewrite, then a swap that copies the tail and remaps offsets, without a reopen | not measured | the protocol is modelled | medium | rejected: it leaks dead-row memory (see Memory) |
| R3. Background build of the new rows and indexes, a tail apply, then the swap | not measured | the protocol is modelled; the Rust tests are planned | medium | **candidate**, pending review |
| D. In-lock rebuild of the indexes in the swap | 80 ms or more for 500k rows, plus the swap | exact | medium | rejected: it holds the lock about as long as the reopen (R1) |
| E. Background rewrite, then reopen to replay | 156 ms or more | exact | medium | rejected: the replay is the largest cost |

Option B is an independent improvement with no change to the lock protocol or the format, and it is implemented. Option C
is the design that removes the stall.

## Option C: the design (superseded by R3 in the Memory section)

### Steps

1. **Snapshot** (under the collection lock, leader op): record the log length `S`, and each live row's payload offset and
   length. This is O(rows) and takes about a millisecond at a million rows (not measured yet).
2. **Rewrite** (no lock): read the old log from the front up to `S`, and write the frames of the snapshot's live rows to
   `*.log.compact`. Writes keep appending to the old log past `S`, and the rewrite does not read those bytes.
3. **Swap** (leader op, under the lock, between write groups, so no group is in its window):
   a. Copy the bytes of the old log from `S` to its end onto the temporary log. Call the new length `N`.
   b. Sync the temporary log.
   c. Record `durable_log_len = N` in the catalog. One write. Recording `N` before the rename is safe: a crash before the
      rename leaves the old log, which is longer than `N`, and the shortened-file check accepts it.
   d. Rename the temporary log over the log. Sync the directory.
   e. Remap the row offsets. Let `R` be the length of the rewrite. A live row whose offset `p` is below `S` gets its offset
      from the rewrite. A row at or past `S` gets `R + (p - S)`, because the tail is copied verbatim. Dead rows keep their
      stale offsets: they are never read, and the next compaction skips them.
   f. Release the lock.
4. **Cleanup:** on open, remove a leftover `*.log.compact`.

The indexes, the row numbers and the postings do not change. So the swap needs no reopen and no rebuild.

### Consistency with deletes and writes during the rewrite

Writes during the rewrite append to the old log, and deletes append tombstones. Those bytes are after `S`, so they are in the
tail that step 3a copies. Their effect on memory (a row marked dead, a row added) happened as they ran, and the remap in step
3e keeps the row offsets right. So the new log holds the same logical history as the old one, from `S` onward.

The claim, stated precisely: at a swap, replaying the new log gives the same live documents as replaying the old log, with
the same values.

**Proof sketch.** Let the old log be `L`, the snapshot length `S`, and `K` the set of PUT frames live at `S`. The new log is
`L'` = (the frames of `K`, in log order) followed by `L[S:]`. A PUT in `K` is the only live PUT of its id at `S`, and every
other PUT at or before `S` is either dead at `S` or superseded. So replaying `L[:S]` gives exactly the documents of `K`. Each
frame after `S` is replayed after the state of `K`, in the same order it was replayed in `L`. Replay is a fold over frames
where a PUT sets an id and a DEL removes it. Replaying `L'` therefore computes the same fold as `L`, provided the state after
`K` equals the state after `L[:S]`, and that holds by the first paragraph. The frames of `K` are kept verbatim, so their
checksums are still valid.

**Model check.** `tests/test_compaction_model.py` takes random interleavings of puts, deletes, snapshots and swaps. After every
swap it checks that the live documents of the new log equal those of the old log, tag for tag. A run of 20,000 interleavings
held at each of 332,771 swaps. The model is of the protocol, not of the Rust code. The Rust code needs its own test before
it is built (see the test plan).

### Failure handling

- **Rewrite fails:** delete the temporary log. Nothing changed.
- **Swap fails before the catalog write:** delete the temporary log. Nothing changed.
- **Catalog write fails:** the catalog reverts its in-memory copy (already the behaviour of `Catalog::update`). Delete the
  temporary log.
- **Rename fails:** the catalog records `N`, which is at most the length of the old log, so reopening the old log is safe.
  Delete the temporary log. Nothing in memory has changed, so the collection stays usable.
- **Directory sync fails after the rename:** the file is new and the memory has not been remapped. Poison the collection.
  Reopening replays the new log.

### Recovery

Crash points, with the state a reopen finds:

- Before the catalog write: the old log, the old catalog. Nothing to do.
- After the catalog write, before the rename: the old log, with `durable_log_len = N` (at most the old length). The check passes.
  The stale temporary log is removed.
- After the rename, before the remap: the new log, which holds the replayed state. Reopen replays it.

### Lock hold

Not measured. An earlier estimate of 25 to 35 ms is withdrawn. The swap's hold will be measured from `WriteStats` once an
implementation exists (see the measurements section).

## Memory: what dead rows cost, and what a swap without a reopen leaves behind

`memory_probe` (heap counted with a global allocator, not resident size, which allocators do not return) runs a sliding-window
workload: each round adds 100,000 documents and deletes the oldest 90,000.

| Round | Live | Dead | Heap, no compaction | Heap, current compaction (reopen) |
|---|---|---|---|---|
| 1 | 10,000 | 90,000 | 12.8 MB | 4.3 MB |
| 3 | 30,000 | 270,000 | 36.5 MB | 6.1 MB |
| 6 | 60,000 | 540,000 | 65.7 MB | 9.0 MB |

- Each dead row costs about **105 bytes** of heap (from round 6: (65.7 − 9.0) MB over 540,000 dead rows). It is the row
  entry (24 bytes), its postings (4 bytes per field), the id and key storage, and the table's spare capacity.
- Without compaction, memory grows linearly with the total number of deletes, about 9.5 MB per round here, and never falls.
- With the current compaction, memory follows the live set, because the reopen rebuilds everything.

**The vector index is different.** `vector_memory_probe` (200,000 vectors, dimension 384): 46.7 MB after the adds, still
46.7 MB after removing 90% (the allocation is kept), and 50.9 MB after adding 180,000 new vectors. TurboVec reuses the freed
capacity, so the vector side is bounded by its peak live size. Only the document store accumulates dead rows.

**What this means for the swap.** The swap in option C changes the row offsets and keeps the row table and the postings, so
the dead rows stay in memory. Repeated swaps then grow memory exactly as no compaction does. So a swap alone cannot be the
design: memory has to be reclaimed too, and reclaiming it means rebuilding the rows and the postings without the dead ones.

Ways to reclaim, with the measured costs:

| Way | Lock held for the reclaiming part | Reclaims memory | Status |
|---|---|---|---|
| R1. Reopen (current, via the replay) | 156 ms at 500,000 live rows, plus the rewrite | yes | the baseline |
| R2. Rebuild the rows and indexes in the swap, under the lock | 80 ms or more at 500,000 rows (in-memory lower bound), plus the swap | yes | rejected: it holds the lock about as long as R1 |
| R3. Build the new rows and indexes in the background from the snapshot (double buffering), then apply only the tail under the lock, and swap | the tail (writes since the snapshot, a few hundred frames), plus the swap: two syncs, one catalog write, a rename | yes | **the candidate**; not measured |
| R4. Swap and keep dead rows until a reopen | the swap only | no: dead rows accumulate | rejected |

R3 is the design to pursue. It needs the tail application to be an exact replay of the tail frames into the new structures,
which is the same logic the reopen already uses, applied to a bounded number of frames.

## Analysis of each stage of the swap

The stages, in order. The lock column says whether the stage needs the collection lock.

| Stage | Lock | Failure | Effect on disk | Effect in memory | Required action |
|---|---|---|---|---|---|
| S1 snapshot the live rows and the length `S` | leader op | none expected | none | none | none |
| S2 build the new rows and indexes in the background (R3) | none | out of memory, or I/O error | none (the temporary log is written separately) | the new structures are discarded | drop them, delete the temporary log |
| S3 rewrite the live PUT frames to the temporary log | none | I/O error | temporary log is partial | none | delete the temporary log |
| S4 copy the tail (bytes after `S`) to the temporary log | leader op | I/O error | temporary log is partial | none | delete the temporary log |
| S5 sync the temporary log | leader op | I/O error | none beyond S4 | none | delete the temporary log |
| S6 record `durable_log_len = N` in the catalog | leader op | I/O error | `catalog.json` unchanged (atomic replace) | the in-memory catalog reverts | delete the temporary log |
| S7 rename the temporary log over the log | leader op | I/O error (rename is atomic: old or new) | old log in place | none | delete the temporary log. The catalog's `N` is at most the old length, so reopening the old log passes the check |
| S8 sync the directory | leader op | I/O error | the rename may or may not be durable | the file is new, the memory still matches the old file | **poison** the collection; reopen |
| S9 apply the tail to the new structures, then swap them in | leader op | cannot fail once S8 has passed (CPU only) | new log in place | new structures in place | none beyond the swap itself |

Every failure before S7 leaves the old log and the old catalog in a state that replays to the same documents. A failure at
S8 leaves a new file that the memory does not match, which is why it poisons the collection. A reopen then replays the new
log, which holds the same documents.

## Crash and power-loss recovery

Crash points (a process crash, and a power loss, which also loses unsynced data):

- **Before S6:** the old log and the old catalog. The temporary log is removed on open. Nothing to do.
- **After S6, before S7:** the old log, longer than `N`, with `durable_log_len = N`. The shortened-file check passes. The
  temporary log is removed on open.
- **After S7, before S8:** either the old log (length at least `N`) or the new log (length `N`), depending on whether the
  rename reached the disk. Both pass the check, and both replay to the same documents.
- **After S8:** the new log, length `N`. Reopen replays it. This is the same as any log of length `N`.
- **Power loss during S5:** the temporary log is synced before the rename, so the rename never points at unsynced bytes.
- **Power loss during S6:** the catalog's atomic replace syncs the file and the directory, which the catalog already does.

The one ordering requirement is that `N` is recorded in the catalog before the rename, and that the temporary log is synced
before the catalog write. With both, every crash point replays to the same documents.

## Concurrent inserts and deletes during the rewrite

- Writes during S2 and S3 append to the old log beyond `S`, and deletes append tombstones there too. Those bytes are the tail
  copied in S4, so the new log holds the same history as the old one from `S` onward.
- The rewrite reads only bytes below `S`, which never change, so it needs no lock.
- A row deleted during the rewrite is still copied in S3, if it was live at `S`. Its tombstone is in the tail, so the replay
  of the new log removes it, as the old log would.
- A row added during the rewrite is not in the snapshot. It is in the tail, and R3 applies it to the new structures.
- The model (`tests/test_compaction_model.py`) checks the logical claim over random interleavings: after every swap, the live
  documents of the new log equal those of the old log, tag for tag. It held at 332,771 swaps. The model is of the protocol.

## Tests in Rust (required before implementation)

The Python model does not exercise the Rust code. The implementation needs these tests, using `cfg(test)` hooks in the same
way as the existing window hooks:

1. **Model-based test** extending `random_operations_agree_with_a_reference_model_across_reopens` with compactions forced at
   random points, including while writes are in flight. After each swap and each reopen, the live documents match the
   reference.
2. **Writes during the rewrite:** pause the rewrite with a window hook, run adds, deletes and searches, then release it. The
   results must equal a run without the background rewrite.
3. **Crash points:** a copy of the directory taken at each stage (S6 done, S7 done, S8 done), then reopened. Each must match
   the reference.
4. **Failure injection:** one test per failing stage (S3, S4, S5, S6, S7, S8), each checking the state the stage leaves
   (usable, or poisoned).
5. **Memory:** after a swap with reclamation (R3), the heap in use must be close to the live set's size. Use the counting
   allocator as in `memory_probe`, with a tolerance.
6. **Lock hold:** `WriteStats` already records the duration of each exclusive hold. The swap's hold is measured from the
   counters, not estimated.

## Measurements still to make (once R3 exists)

- The swap's lock hold, from `WriteStats`, at a million rows, with and without writes during the rewrite.
- The reclamation: heap before and after a swap with R3, against the live set's size.
- The rewrite's duration, which runs off the lock: it must not affect reader latency (measure with `concurrent_bench`).

## Decisions for review

1. Approve R3 (background build of the new structures, a tail apply and a swap) as the design, or ask for R1 (reopen) as
   the baseline to keep.
2. Accept the one-catalog-write ordering (record `N` before the rename).
3. Accept that the swap's lock hold is a measured target (below 40 ms at a million rows), not an estimate, and that the
   withdrawn estimate of 25 to 35 ms is not a target.
