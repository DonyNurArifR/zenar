# Storage and durability

## Layout

```text
<root>/LOCK                 advisory lock, held while the database is open
<root>/metadata.db          SQLite (WAL mode): collection config, document text, metadata
<root>/metadata.db-wal      SQLite write-ahead log (exists while the database is open)
<root>/vectors/<name>.tvim  TurboVec index for each collection
```

SQLite is the source of truth for documents and for recovery. TurboVec holds the vectors. SQLite stores
no vectors, so **a lost index file cannot be rebuilt by Zenar**. Its vectors must be re-added from your
source data.

## Locking

Two layers:

1. **Zenar `LOCK`** (exclusive `flock`) is held for the lifetime of a `VectorDB` handle. A second open,
   in this process or another, raises `DatabaseLockedError`. This covers the vector files, which SQLite does not
   protect.
2. **SQLite's own locking** on `metadata.db`. It serializes writers, and readers in WAL mode do not block the writer.

## Ingest order

For each `add`:

1. Add the vector to the in-memory index.
2. At each sync boundary (every 1000 adds), sync the index file. If the sync fails, the vector is removed again.
3. Insert the document row with status `READY`.

The row is written once, after the vector. The order matters for crash recovery. The consequences of a crash
at each point:

- Before step 2's sync: the vector is lost with the process. There is no row. Nothing to repair.
- After the sync, before step 3: a vector with no row. Recovery removes it.
- After step 3, before the next sync: a `READY` row with no vector on disk. Recovery removes the row.

`PENDING` rows can still appear in an older database. Recovery rolls them back.

## Deletes

For each `delete`:

1. Mark the row `DELETING`.
2. Remove the vector from the index and **sync the index file before returning**.
3. Delete the row.

A deleted vector is therefore never on disk once `delete` returns. This is the compliance guarantee.

## Durability policy

| Operation | Durability |
|---|---|
| `add` | Index synced every `SYNC_INTERVAL` (1000) adds. Adds since the last sync can be lost on crash. |
| `flush()` | Syncs every index file and runs `PRAGMA wal_checkpoint(FULL)`. Every add made before `flush()` returns survives a process crash **and** a power loss. |
| `delete` | Index synced before return. Durable against process crash. Power loss: see below. |
| Metadata commits | `MetadataSync::Normal` (default): `synchronous=NORMAL`. A WAL checkpoint fsyncs. `Full` / `durable=True`: `synchronous=FULL`, fsync per commit. |

With `Normal`, commits after the last checkpoint can be lost on power failure. Reconciliation still gives a
consistent state (see below). Acknowledged adds are power-safe after `flush()`. Without `flush()`, use
`durable=True` if power-loss durability of every add matters.

A delete with `Normal` can also come back after power loss. The index file was synced, so the vector is gone. On
reopen, the row that survived is a `READY` row with no vector, and recovery removes it. The document does not come
back.

Measured cost (dim 384, 1M rows, `examples/bench.rs`): `Normal` ingests at 0.053 ms/row on SQLite. Stoolap's per-commit fsync
made `Full` roughly 600-1100x slower on that backend. See BENCHMARKS.md.

## Recovery on open

Each collection is reconciled against its metadata:

| Found | Action | Counted as |
|---|---|---|
| `PENDING` row | Remove row and vector | `rolled_back_pending` |
| `DELETING` row | Finish the delete | `finished_deletes` |
| `READY` row, no vector in index | Remove row (its vector is gone) | `orphaned_rows_removed` |
| Vector in index, no row | Remove vector | `orphaned_vectors_removed` |

`db.recovery_report()` returns these counts. A non-zero `orphaned_rows_removed` after a crash is expected
when adds were not flushed. It means the window of unsynced adds was discarded, not that data was corrupted.

## Crash tests

- `test_kill_minus_9_mid_write_leaves_consistent_database` (Python): SIGKILL during a write stream. Every
  `READY` row must be searchable after reopen.
- `test_flush_makes_adds_survive_kill_minus_9` (Python): adds covered by a `flush()` survive SIGKILL.
- Rust unit tests in `collection.rs`: crash before sync, crash mid-delete, READY without vector.

## Index file format

TurboVec `IdMapIndex` v7 snapshot, written with `sync()` (temp file, `fsync`, atomic rename). The file stores
the padded dimension. Zenar keeps the logical dimension in the collection record.
