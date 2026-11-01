# Changelog

All notable changes to Zenar. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the
project follows [semantic versioning](https://semver.org/) while it is pre-1.0.

## Unreleased

### Changed
- Unfiltered search returns `top_k` committed results when that many exist, even while a write is between its index sync
  and its commit. Documents of a write that has not returned yet are not returned.
- A failed write to the document log poisons the collection, and so does a failed truncation after one. Before, the
  leftover bytes stayed in the log, and a later add of the same id could make the database refuse to open.
- A delete no longer turns off the shortened-file check while it runs. A crash inside a delete cannot hide a missing
  vector index.
- `RecoveryError` on open is raised before any file is truncated, and a missing document log is refused instead of
  recreated empty while the catalog says it held synced bytes.

### Testing
- Fault injection for staging: a failed `stage_puts` poisons the collection, and a reopen drops the staged documents.
- A stress test: rejected group members (repeated ids) among valid writes, under concurrent readers. Valid writes commit,
  rejected ones fail alone, and no search returns a repeated or uncommitted id.

### Documentation
- COMPACTION.md: memory of dead rows (about 105 bytes each), the reclamation options, the analysis of every stage of a swap
  and of crashes and power loss, and the Rust test plan. The background swap is not implemented.
- CONCURRENCY.md: the staging breakdown and the decision not to move validation and encoding out of the lock.
- ARCHITECTURE.md describes the metadata indexes: structures, lookup and update costs, memory, the alternatives that
  were measured and those that were not.
- STORAGE.md states that the vector index and the document log are not one transaction. It covers the order of durable
  steps, the outcome of each failure, the poisoning rule, and reconciliation on open.
- ROADMAP.md lists the open production-readiness items in priority order.
- CONCURRENCY.md describes the write lock, the measured costs, and the locking change in Phase 1.
- BENCHMARKS.md corrects the delete cost (about 25 ms, not 8 ms) and adds the concurrency and write-cost measurements.

### Performance
- Compaction reads the log once, from the front, instead of one read per live row. A compacting delete batch at a million
  rows falls from about 260 ms to about 130 ms, and the compaction in `DocStore::compact` from about 370 ms to about 190 ms
  (the reopen replay is most of the rest). The file format is unchanged.
- A write syncs its document log with the collection lock released. The index is still synced under the lock. With
  8 readers and one continuous writer, search throughput rises from about 700 to about 6,200 per second, and p99 falls
  from about 30 ms to about 6 ms. Single-writer write costs are unchanged within noise.
- Writes from several threads to one collection share syncs (group commit). Writes queue; a leader syncs the queued
  writes as one group, with one index sync and one log sync. Writes alone, four writers reach about 420 batches per second,
  against about 130 before, and about 3.2 times the Phase 1 build. With 8 readers, search throughput at four writers
  falls to about 2,600 per second, against about 5,900 for Phase 1: group commit trades reader throughput for writer
  throughput. See BENCHMARKS.md, "Readers and writers".
- A group's failure rolls back the whole group, and every write in it gets the error. A write the group rejects on its own
  (a duplicate id, a bad vector) gets its own error, and the others commit.
- Writes to one collection are queued (no per-collection writer gate). Readers do not take the queue.
- A failed re-sync of the index during a rollback poisons the collection, as a failed truncation already did. Before, the
  disk could hold vectors that the log did not have, and the collection stayed usable.
- `Collection::write_stats` (Rust API) returns counters for group sizes, lock holds, and readers' waits for the state lock.
  It is a diagnostic and is not in the Python API.


## 0.2.0

First release since 0.1.1. It changes the on-disk format, so databases written by 0.1.x cannot be opened and must
be recreated. Breaking changes are listed first.

### Breaking
- Database files have new names: `docs/<name>.zdocs` and `vectors/<name>.zvec`. The catalog format is version 3.
  Databases from earlier releases are refused with a message that says so.
- A collection handle raises `CollectionClosedError` after its database is closed or its collection is dropped.
  It used to write, and the write was lost on reopen.
- A metadata value of `None` is an absent field, not an error.
- `top_k` is no longer capped at 1000.

### Highlights
- Metadata fields are unlimited, and a new field name extends the schema.
- Unfiltered search is about 3x faster, with TurboVec 1.1.2.
- Field codes are stored only for rows that have the field, so wide schemas use far less memory.
- Batches write the catalog once, not per sync.
- Agent tool schemas are checked against the parser, and nested filters are typed.

Release 0.1.2 was tagged but never released; its changes are included here.

### Changed
- Database files have Zenar's own extensions, so they are not mistaken for logs: `docs/<name>.zdocs` (document store)
  and `vectors/<name>.zvec` (vector index). The catalog format is version 3. A database with the old names is refused
  with the usual message; create a new one.
- A batch writes the catalog once, at flush or close, not at every sync. A 50-document `add_many` took 16 ms; it now takes
  9 ms on macOS, where a full flush is the main cost. Deletes still record their lengths at once.
- Less memory per document. The per-field presence list held the same rows as the codes list, so it is gone. A million
  documents with three metadata fields open in 206 MB, down from 225 MB. The file format is unchanged.
- The agent `where` schema is the one the parser checks, at every level of `and`, `or` and `not` (they referred only to
  "an object" before). It is shared under `$defs`, so nested filters cost no extra size. A tool name must be 1 to 64
  letters, digits, `_` or `-`.
- The catalog format is version 2. Version 2 also marks the record format with 32-bit field counts. A database from an
  earlier release is refused with a message that says so, instead of being misread. No database has been released to
  PyPI. Releases 0.1.1 and 0.1.2 on GitHub wrote version 1, and they cannot be read by this version.
- A metadata field name the schema lacks extends the schema, with the type of its first value. Before, it was
  rejected after the first insert. A field keeps its type; a value of another type is still rejected.
- `VectorDB()` with no path uses `./zenar`, created if missing. An explicit path is used as given.
- Metadata fields per collection are not limited. The per-document field count is stored as 32 bits, not 16. Records
  written before this change cannot be read; no database has been released to PyPI, so none exist outside development.
- TurboVec is 1.1.2 (was 1.0.0). Unfiltered search is about 3x faster at 1M rows. The vector index file format is the same as 1.0.0, and it gives the same answers.
- `top_k` is no longer capped at 1000. A result is at most the number of matching documents.

### Fixed
- numpy scalars (`int64`, `bool_`) are accepted as metadata values. A `None` metadata value is treated as an
  absent field, not rejected.
- The per-document field count is 32 bits (it was 16, and truncated silently above 65,535 fields).
- Each indexed field stores a code only for the rows that have it. A collection with thousands of fields used to cost
  fields times documents times 4 bytes: 877 MB for 5,000 fields and 20,000 documents. It is now 54 MB. A typical
  three-field collection at 1M rows is unchanged in peak memory, and compound search there is about 0.9 ms slower.
- A collection handle can no longer write after its database is closed or its collection is dropped. Such a write
  looked successful and was lost on reopen. It now raises `CollectionClosedError`.
- A filter that compares with NaN is refused with `InvalidFilterError`. `field < NaN` used to match every document.
- A two-dimensional numpy array is refused as a vector. It used to be flattened and accepted.
- A non-object `scope` names the scope argument in its error, not `where`.

## 0.1.2

Tagged only. Never released; see 0.2.0.

### Fixed
- A database whose document log or vector index was emptied, deleted or truncated outside Zenar now fails to open
  with `RecoveryError`. Before, it opened with fewer documents and no error.
- Deletes no longer leave a database that refuses to open after a crash in the middle of the delete.

### Added
- Tests for each kind of shortened file, for named errors on bad inputs, and for concurrent writers and readers.

## 0.1.1

Released on GitHub. Not yet on PyPI.

### Fixed
- An out-of-range dimension argument raises `InvalidArgumentError`. It raised `DimensionMismatchError`, which is for
  vectors whose length differs from the collection's dimension.

### Changed
- CI does not run for changes that touch only documentation or Markdown.

## 0.1.0

First release. Released on GitHub. Not yet on PyPI.

- Collections with vector search (TurboVec) and a document log for typed metadata.
- Filters on typed fields with SQL NULL and three-valued logic, tenant isolation, and scoped agent tools
  (`describe`, `find_values`, `retrieve`, `tool_spec`).
- Atomic `add_many` and `delete_many`, `flush()`, `durable=True`, crash recovery and reconciliation on open.
- Python bindings for 3.10 and later, with wheels for Linux, macOS and Windows, and an sdist.
