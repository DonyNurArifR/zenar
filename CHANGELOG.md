# Changelog

All notable changes to Zenar. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the
project follows [semantic versioning](https://semver.org/) while it is pre-1.0.

## Unreleased

### Changed
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

Tagged. The release workflow has not run for this tag, so there is no GitHub release yet.

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
