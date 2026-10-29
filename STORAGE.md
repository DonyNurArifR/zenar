# Storage and durability

## Layout

```text
<root>/LOCK                 advisory lock, held while the database is open
<root>/metadata.db          Stoolap: collection config, document text, metadata
<root>/vectors/<name>.tvim  TurboVec index for each collection
```

Stoolap is the source of truth for documents and for recovery. TurboVec holds the vectors. Stoolap stores
no vectors, so **a lost index file cannot be rebuilt by Zenar**. Its vectors must be re-added from your
source data.

## Locking

Two layers:

1. **Zenar `LOCK`** (exclusive `flock`) is held for the lifetime of a `VectorDB` handle. A second open,
   in this process or another, raises `DatabaseLockedError`. This protects the vector files, which sit outside
   Stoolap's lock.
2. **Stoolap's own file lock** on `metadata.db`. Measured with `examples/stoolap_probe.rs`: a second process
   opening the same file gets `database is locked by another process`.

## Ingest order

For each `add`:

1. Add the vector to the in-memory index.
2. At each sync boundary (every 1000 adds), sync the index file. If the sync fails, the vector is removed again.
3. Insert the document row with status `READY`.

The row is written after the vector, so no UPDATE is needed per add. Stoolap is MVCC, and an UPDATE per row
measurably slows every later scan (see BENCHMARKS.md). The consequences of a crash at each point:

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
| `flush()` | Syncs the index. Every add made before `flush()` returns survives a process crash (kill -9, panic, OOM). It survives a power loss only with `durable=True` (see below). |
| `delete` | Index synced before return. Durable. |
| Metadata commits | `MetadataSync::Normal` (default): WAL is not fsynced per commit. `MetadataSync::Full` / `durable=True`: fsync per commit. |

With `Normal`, recent metadata commits can be lost on power failure. Stoolap 0.4 has no durability barrier
that `flush()` could call. `create_snapshot` writes backup files and does not commit the WAL. Reconciliation still
produces a consistent state, because every combination of a lost recent commit is one recovery already repairs
(see below). So:

- Process crash: adds covered by `flush()` survive, with either setting.
- Power loss: use `durable=True`. Otherwise acknowledged adds may be dropped, though the database stays consistent.

Measured cost (dim 384, `examples/bench.rs`): `Normal` ingests at 0.013 ms/row at 1M rows. `Full` took
15.9 ms/row at 10K rows with batched index sync, and 19.9 ms/row with a sync per add. Both are roughly 600-1100x slower than `Normal`.

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
