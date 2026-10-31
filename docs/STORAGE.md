# Storage and durability

## Layout

```text
<root>/LOCK                  advisory lock, held while the database is open
<root>/catalog.json          format version, collections: dimension, bit width, metadata schema
<root>/docs/<name>.log       document log: text and metadata, append-only
<root>/vectors/<name>.tvim   TurboVec index for each collection
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
4. At a sync boundary (every 1000 adds, or every add with `durable=True`), sync the index file and the log. If that
   fails, the vector is removed and the log is truncated back.
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
truncated to its length before the batch.

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
If the log or the index is found shorter than that, something outside Zenar truncated or removed it. Opening then fails
with `RecoveryError` and changes nothing, instead of dropping the documents that were already synced. A file that is
damaged in its middle fails the same way, with its own named error. Databases created before this rule have no recorded
lengths, so the check does not apply to them. A delete clears the recorded lengths before it changes anything on disk, and records them again once it is
synced, so a crash in the middle of a delete cannot leave a recorded length larger than the file.

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

Power loss is not tested here. The `fsync` guarantees come from the platform, and they have not been checked on real
hardware.

## Index file format

TurboVec `IdMapIndex` v7 snapshot, written with `sync()` (temp file, sync, atomic rename). The file stores the padded
dimension. Zenar keeps the logical dimension in the catalog.
