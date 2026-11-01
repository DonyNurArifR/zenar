# Storage and durability

## Layout

```text
<root>/LOCK                  advisory lock, held while the database is open
<root>/catalog.json          format version, collections: dimension, bit width, metadata schema
<root>/docs/<name>.zdocs     document store: text and metadata, append-only
<root>/vectors/<name>.zvec   vector index (TurboVec) for each collection
```

The document log is the source of truth for documents. TurboVec holds the vectors. The log holds no vectors, so
**a lost index file cannot be rebuilt by Zenar**. Its vectors must be re-added from your source data.

Indexes (per metadata field) live in memory. They are rebuilt from the log on open.

## Locking

1. **`LOCK`** (exclusive `flock`) is held for the lifetime of a `VectorDB` handle. A second open, in this process
   or another, raises `DatabaseLockedError`.
2. The catalog and logs are only written by the process that holds `LOCK`.

## The document log

Each record is a frame: `length (u32 LE)`, `crc32 (u32 LE)`, then the payload. The CRC is IEEE CRC-32, the zlib one.

- **PUT**: id, text, and the metadata fields the document carries. Missing fields are not stored (they are NULL).
- **DEL**: a tombstone for an id. A later PUT of the same id is a new document.

On open, the log is replayed from the start:

| What is found at the end of the log or in the middle | Result |
|---|---|
| A frame cut off by the end of the file (header or payload) | Torn tail. Truncated to the last valid frame. Earlier records are kept. |
| Zeros from the last valid frame to the end | Torn tail. Truncated. (A file system may extend a file before writing data.) |
| A bad CRC or undecodable frame followed by more data | `RecoveryError`. The database does not open. Later records are never dropped silently. |

Compaction runs when dead records outnumber live ones and there are more than 4096 dead. It writes the live records to a
temporary file, syncs it, releases the log's file handle (Windows refuses to rename over an open file), renames the
temporary file over the log, syncs the directory, and reopens the log. If the rename fails, the old log is reopened and
nothing changes. Any other error in compaction poisons the collection (see below). The safe course is a reopen, which
replays whichever log is on disk.

`drop_collection` deletes the collection's files. On Windows that fails while a Python object still holds the collection
open. Drop the handle first, then drop the collection.

## Ingest order

### `add`

1. Validate the record. A field name the schema lacks extends the schema, with the type of its first value. The catalog
   is written first, so a record that uses a new field never reaches the log before the catalog knows the field.
2. Write the PUT frame to the log (not yet visible to readers).
3. Add the vector to the in-memory index.
4. At a sync boundary (every 1000 adds, or every add with `durable=True`), sync the index file, then the log. If that
   fails, the vector is removed and the log is truncated back. If the truncation fails too, the collection is poisoned.
5. Make the document visible.

### `add_many`

A batch is all-or-nothing:

1. Validate every record: ids, vectors, metadata, duplicates within the batch and against stored documents.
   If the batch introduces fields, the new schema is written to the catalog. No document is written yet.
2. Write every PUT frame to the log, in one write.
3. Add every vector to the index.
4. Sync the index file, then the log.
5. Make the documents visible.

If step 2, 3 or 4 fails, the vectors added so far are removed, the index is synced again (best effort), and the log is
truncated to its length before the batch. If the truncation fails, the collection is poisoned: the batch's bytes would
otherwise stay in the log in front of later writes.

## Deletes

`delete_many` does the same steps for a batch, with one index sync and one log sync. Every id must exist and be unique,
or nothing changes.

`delete` (one id):

1. Remove the vector from the in-memory index, and sync the index file. A deleted vector is gone from disk before the
   tombstone is written.
2. Write a DEL frame to the log.
3. Sync the log.
4. Make the deletion visible.

If step 1's sync, step 2 or step 3 fails, the collection is **poisoned**. Its in-memory state is ahead of its durable state,
and it cannot be fixed safely in place. Every later operation on that collection returns `PersistenceError` until the
database is reopened. Reopening replays the durable state.

## Reconciliation on open

The index file and the log can disagree after a crash. On open, each collection is reconciled:

| Found | Action | Counted as |
|---|---|---|
| A live document with no vector in the index | Write a tombstone for it. Its vector is gone. | `orphaned_rows_removed` |
| A vector in the index with no live document | Remove it from the index. | `orphaned_vectors_removed` |

Both kinds of repair are durable after reconciliation. `db.recovery_report()` returns the counts. A non-zero count after a
crash is expected when adds were not flushed. It means the unsynced window was discarded, not that data was corrupted.

Reconciliation only repairs the unsynced tail. The catalog also records the log length and the vector count at each flush and close, and at each delete.
If a file is found shorter than that, something outside Zenar truncated or removed it. Opening then fails with
`RecoveryError` and changes nothing, instead of dropping the documents that were already synced. This covers:

- a log shorter than its recorded length, checked before the log is read, so a torn tail is not cut back first;
- a log whose synced part is damaged at its end (the bytes are kept, the open is refused);
- a missing log, which is refused and not recreated empty, while the catalog says it held synced bytes;
- a vector index shorter than its recorded count, or missing.

A file damaged in its middle fails the same way, with its own named error. Databases created before this rule have no
recorded lengths, so the check does not apply to them.

A delete lowers the recorded lengths before it changes anything on disk. The index loses one vector per deleted document
when it is synced, and the log shrinks only when compaction runs. So the vector count is lowered by the number of
documents deleted, and the log length is zeroed only when compaction is due. A crash in the middle of a delete can then
leave each file no shorter than its recorded length less that amount, and the check stays on. The lengths are recorded
again once the delete is synced.

## Consistency across the two files

A write changes two files: the vector index and the document log. Zenar does not make them one atomic transaction. No
journal covers both files, and the operating system does not offer a transaction across two files. Each operation
instead follows a fixed order of durable steps, and the rules on open decide what a crash can leave behind.

### Order of durable steps

| Operation | Steps, in order |
|---|---|
| `add`, `add_many` | Write the log bytes. Add the vectors in memory. At a sync boundary: sync the index file, then the log. Make the documents visible. |
| `delete`, `delete_many` | Lower the recorded lengths. Remove the vectors in memory. Sync the index file. Write the tombstones. Sync the log. Compact if due. Record the lengths. |
| `flush()` | Sync the index file, then the log, for each collection. Record the lengths. |

Adds sync the index first. So once a log sync has returned, every document it covers has its vector on disk too.
Deletes sync the index before they write tombstones. So once the index sync returns, the vector is gone for good. The
document is then removed on the next open, whether or not its tombstone reached the disk.

### What a failure leaves behind

The error column is what the call returns. The last column is what the next open produces, at the level of one
document. "Poisoned" is defined in the section above.

| Failure | Error returned | Collection afterwards | After reopen |
|---|---|---|---|
| `add` or `add_many` is rejected by the index before the log is synced (for example, `store.add` fails) | Yes | Undone in memory and in the log. Usable. | Absent. |
| `add` or `add_many`: writing the log bytes fails | Yes | Poisoned. The write is not trusted to have been truncated. | The leftover bytes are unsynced documents. Those without a vector are removed. |
| `add` or `add_many`: index sync succeeds, log sync fails | Yes | Undone. Usable. | Can be present. The vector is on disk, so reconciliation keeps the document if its bytes survived. An error can still have been applied. |
| `add` or `add_many`: truncating the log back fails | Yes | Poisoned. | Leftover bytes are unsynced documents. Those without a vector are removed. |
| `delete` fails at the index sync | Yes | Poisoned. | Present, with its vector. The delete was not applied. |
| `delete` fails after the index sync (tombstone or log sync) | Yes | Poisoned. | Absent. The vector is gone, so the document is removed. |
| Compaction fails after the rename | Yes | Poisoned. | The log on disk is replayed. Its live documents are the same either way. |
| `flush()` fails | Yes | Usable. Nothing already synced is undone. | Whatever reached the disk. |
| Power loss during `add_many` | No error (the process did not return) | Not applicable | A prefix of the batch can survive. The index is synced first, and the log bytes may be partly on disk. A process crash gives all or nothing. |

After an error, check with `get()` or `count()` before retrying. An error means the write may or may not have been
applied. Retrying an add that was applied returns `DuplicateId`.

### What reopen guarantees

- Synced data is never dropped silently. A file shorter than its last sync, a missing file, or a synced record damaged
  at its end fails the open with `RecoveryError`, and the file is left as it was (tested).
- A document without a vector, and a vector without a document, are both removed. The counts are in
  `db.recovery_report()`. A count above zero after a crash is expected for adds that were not synced.
- A poisoned collection refuses every operation until the database is reopened.
- A reopen is only accepted when the index and the log agree after reconciliation.

### What users can expect

- After `flush()` returns, or after an add with `durable=True` or an `add_many` returns, the data survives a process
  crash and a power loss, as far as the platform's `fsync` holds. Power loss has not been tested on hardware.
- After `delete` returns `Ok`, the deletion survives, with the same caveat.
- `add_many` is all or nothing across a process crash and across errors Zenar reports. A power loss during it can leave
  a prefix of the batch.
- Each collection is separate. `db.flush()` syncs the collections one after another. There is no transaction across
  collections, and none across several calls.

### Limits

- Adds that were not synced are lost on a crash, by design. The default `Normal` syncs every 1000 adds.
- A vector index that is lost cannot be rebuilt from the log, because the log holds no vectors. Opening refuses, and the
  vectors have to be added again from the source data.
- Each collection's index and log are checked against each other only on open. A file changed by another process while
  Zenar is open is not detected until the next open.

## Durability policy

| Operation | Durability |
|---|---|
| `add` | Synced every `SYNC_INTERVAL` (1000) adds. Adds since the last sync are lost on crash, and reconciliation removes what is left of them. |
| `add` with `durable=True` | Every add is synced before it returns. |
| `add_many` | Synced before it returns. |
| `delete` | The index sync and the log sync happen before it returns. |
| `flush()` | Syncs the index file and the log of every collection. Every add made before `flush()` returns survives a process crash **and** a power loss. |
| Clean close | Unsynced adds are synced when the collection handle is dropped. |

`durable=True` makes every add power-safe on return. The default, `Normal`, gives the same guarantee after `flush()`.

## Crash tests

- `test_kill_minus_9_mid_write_leaves_consistent_database` (Python): SIGKILL during a write stream. Every live document
  must be searchable after reopen.
- `test_flush_makes_adds_survive_kill_minus_9` (Python): adds covered by a `flush()` survive SIGKILL.
- `torn_tail_is_truncated_and_earlier_records_survive`, `zero_filled_tail_is_treated_as_torn`,
  `corruption_in_the_middle_is_an_error_not_a_silent_drop` (Rust, log level).
- `document_without_vector_is_removed_on_reopen`, `vector_without_document_is_removed_on_reopen` (Rust, collection level).
- `poisoned_collection_refuses_every_operation` (Rust).
- `failed_delete_poisons_and_reopen_keeps_the_document`, `failed_add_leaves_no_trace_and_the_collection_stays_usable`,
  `failed_add_many_is_all_or_nothing_across_reopen` (Rust): real index-write failures, injected by making the index file
  refuse writes (Unix only; the tests refuse to run as root).
- `failed_undo_poisons_and_reopen_removes_the_orphan_frames` (Rust): a failing truncation poisons the collection, and the
  orphan frames do not produce a duplicate id on the next open.
- `missing_vector_index_is_refused_without_changes`, `missing_document_log_is_refused_and_not_recreated`,
  `log_cut_short_after_sync_is_refused_without_changing_the_file`, `damaged_synced_record_is_refused_without_truncation`
  (Rust): refusals on open.
- `crash_during_delete_does_not_hide_a_missing_index` (Rust): a crash inside a delete's window still refuses a missing index.

Power loss is not tested here. The `fsync` guarantees come from the platform, and they have not been checked on real
hardware.

## Index file format

TurboVec `IdMapIndex` v7 snapshot, written with `sync()` (temp file, sync, atomic rename). The file stores the padded
dimension. Zenar keeps the logical dimension in the catalog.
