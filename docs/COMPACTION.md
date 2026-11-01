# Compaction: investigation and design proposal

Status: **proposal**. The only change made is a sequential rewrite of the log (below), which keeps the same format and the
same lock protocol. The background swap described here is not implemented. It changes when the collection lock is held
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
| C. Background rewrite, then a swap under the lock: the tail is copied, offsets are remapped, no reopen | estimated 25 to 35 ms: two syncs, one catalog write, a rename, and an O(rows) remap. The remap cost is not measured yet. | proven at the protocol level (below) | medium: a new interleaving of writes and the swap | **proposed** |
| D. Background rewrite, then the swap rebuilds the indexes from the rows | 80 ms or more for 500k rows, plus the swap | as C | medium | rejected: C avoids the rebuild |
| E. Background rewrite, then reopen to replay | 156 ms or more | as C | medium | rejected: the replay is the largest cost |

Option B is an independent improvement with no change to the lock protocol or the format, and it is implemented. Option C
is the design that removes the stall.

## Option C: the design

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

### Lock hold, estimated

Two syncs (about 4 ms each), one catalog write (about 8 ms), a rename (0.2 ms), the tail copy (the bytes written during the
rewrite: about 0.1 ms per 100 writes), and the remap. Estimated 25 to 35 ms in total at a million rows, against 127 to 132 ms
now. The remap is O(rows): a loop over the row table. It has not been measured.

## Test plan (before implementation is accepted)

1. **Model-based test in Rust**, extending `random_operations_agree_with_a_reference_model_across_reopens`: random adds,
   deletes and compactions, with the compaction moved into the background (a window hook can hold it between the rewrite and
   the swap). Check the live documents against the reference after every swap, and across reopens.
2. **Deletes during the rewrite:** a window test that deletes, adds and searches while the rewrite is paused. The results
   must match a run without the background rewrite.
3. **Crash points:** the copy-based crash tests, one per crash point above. Each reopen must match the reference.
4. **Failure injection:** a failing catalog write, a failing rename, and a failing directory sync, each with the expected state.
5. **Benchmarks:** `write_cost` at one million rows, the compacting batch; `concurrent_bench` with deletes under readers.
   Acceptance: the swap's lock hold below 40 ms at a million rows, and no change in the ordinary delete path.

## Decisions for review

1. Approve option C, and its one-catalog-write ordering (record `N` before the rename, not after).
2. Accept that dead rows stay in memory and in the postings until the next reopen. Compaction would then reclaim disk only.
   The alternative is a background index rebuild, which is more work.
3. Accept a temporary log of up to the live size on disk during a rewrite.
