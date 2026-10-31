# Changelog

All notable changes to Zenar. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the
project follows [semantic versioning](https://semver.org/) while it is pre-1.0.

## Unreleased

### Changed
- TurboVec is 1.1.2 (was 1.0.0). Unfiltered search is about 3x faster at 1M rows. A database written by 1.0.0 opens
  unchanged and gives the same answers.
- `top_k` is no longer capped at 1000. A result is at most the number of matching documents.

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
