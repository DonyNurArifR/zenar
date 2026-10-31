# Architecture

## Crates

- **zenar-core** (pure Rust, no PyO3): `VectorDB`, `Collection`, the document store, the catalog, the filter AST and
  its validation, and the vector-index wrapper.
- **zenar-python** (PyO3, extension `zenar._zenar`): Python classes, exception mapping, GIL release, panic guard.
  Built by maturin.
- **python/zenar**: thin package that re-exports the extension.

## Components

| Component | Module | Holds | Persistence |
|---|---|---|---|
| Vector index | `storage.rs` (TurboVec) | one vector per id | `vectors/<name>.tvim`, synced at boundaries |
| Document store | `docstore.rs` | text and metadata per id; per-field indexes | `docs/<name>.log`, append-only |
| Catalog | `catalog.rs` | collection config and schema | `catalog.json`, replaced atomically |
| Collection | `collection.rs` | the write and read protocol across the three | none (orchestration) |

TurboVec is a compressed flat vector index with fixed inner-product scoring, true O(1) removal and a TQ+ calibration step.

## Why a document store, not a database

The metadata engine went through three versions:

1. **Stoolap 0.4.** Its primary-key index was not used by `IN` lists after a reopen, so every search after a restart
   scanned the table. Ingest was fast, but `Full` durability cost 600x in ingest speed.
2. **SQLite (rusqlite).** Reopen behavior was fine, and durability could be checkpointed for `flush()`. Filtered search
   still read one table row per candidate. Ingest with indexed fields was 0.030 ms/row.
3. **Zenar document store (current).** The filter evaluates against per-field indexes held in memory. Documents are read
   from the log by offset. Ingest is 0.012 ms/row. Selective and broad filters are both faster than on SQLite (BENCHMARKS.md).

The costs of the current design are real and are listed in the tradeoffs below. The sections that follow describe what it does.

### Tradeoffs against a general database

- The indexes are in memory. Open time grows with the number of rows (0.8 s for 1M rows, dominated by the log replay).
  Memory grows with the number of distinct metadata values and rows.
- There is no query language. Only the `Filter` AST is supported, and it is evaluated by Zenar's own code. That code
  is checked against an independent reference (`indexed_evaluation_matches_reference_three_valued_logic`).
- Durability and recovery are Zenar's own (STORAGE.md). They are tested with torn tails, orphans, and `kill -9`.
  They have not been checked on real power loss.

## The filter engine

A filter evaluates to two sorted row lists, TRUE and FALSE. The rest are NULL. This is SQL's three-valued logic:

| Filter | TRUE rows | FALSE rows |
|---|---|---|
| `field == v` | postings for `v` | rows with the field present, minus TRUE |
| `field != v` | rows with the field present, minus postings for `v` | postings for `v` |
| `field > v` (and the other orders) | postings in range | rows with the field present, minus TRUE |
| `field IN (...)` | union of postings | present, minus TRUE |
| `NOT f` | FALSE of `f` | TRUE of `f` |
| `f AND g` | intersection | union of FALSE |
| `f OR g` | union | intersection of FALSE |

A row where the field is missing is NULL for every comparison. NULL is never TRUE, and under `NOT` it stays NULL.

The FALSE set costs a pass over every row of the field. It is built only when a parent needs it (`NOT`, or an `AND`/`OR`
under one). A top-level comparison does not build it. That is why a 0.1% filter takes about 0.3 ms.

## Query path

```text
search(vector, top_k, filter)
  no filter  -> TurboVec top-K over all vectors              (the document store is not read for ranking)
  filter     -> document store: TRUE rows for the filter     (per-field indexes, in memory)
                -> keep ids still present in TurboVec        (stale ids are dropped, not errors)
                -> empty? return []                          (TurboVec not touched)
                -> TurboVec top-K over the allowlist
                -> document store: text and metadata for top-K only (one pread per document)
```

## Concurrency

- One `RwLock` per collection. Searches run concurrently. Writes (add, add_many, delete, flush, calibrate) are serialized.
- The document log is read with positioned reads (`pread`), so readers do not share a file cursor.
- The GIL is released around every core call (`py.detach`).
- No panic crosses into Python. Each core call is wrapped in `catch_unwind` and reported as `VectorStoreError`.
- A collection whose in-memory state cannot be made durable or undone is poisoned. Every operation then fails until the
  database is reopened (STORAGE.md).

## Deviations from the design specification

Each deviation was chosen to satisfy another requirement in the spec, or because the spec's design did not hold up in
measurement. They are listed here so they are not rediscovered later.

| Spec | Implemented | Reason |
|---|---|---|
| Section 4.2: one `documents` table in Stoolap | One log per collection (`docs/<name>.log`), with indexes in memory | Filtered search and ingest were limited by the metadata engine (BENCHMARKS.md). |
| Section 9/10: filter compiled to SQL | Filter AST evaluated by the document store | No SQL is built from user input at all. Injection is impossible by construction, not only by parameterization. |
| Section 13: index on frequently filtered fields | An index per metadata field, held in memory | Same result as a database index, with no per-row table reads. |
| Section 25/65: a separate lock | `LOCK` file, held for the life of the handle | Same as spec. |
| Section 69: dimension must be a multiple of 8 | Any dimension up to 16384, zero-padded internally | Phase 5 requires the section 39 example (`dimension=3`) to work. Zero padding leaves dot products and norms unchanged, so scores are exact. |
| Section 73: names `[a-zA-Z0-9_-]` | `[a-z0-9_]`, starting with a letter, max 64 | Names become file names. Rejected, not sanitized. |
| Section 67/86: sync on every write | Sync every 1000 adds, every delete, and on `flush()`. `durable=True` syncs every add. | Per-add sync made ingest quadratic. See STORAGE.md. |
| Section 74: `rebuild(name)` | Re-runs reconciliation on the collection | A true rebuild needs the source vectors, which Zenar does not keep. |
| Section 18: `search` results | Same shape, plus `metadata` as a dict | |
| Section 47/74 error model | Every variant mapped to a Python class under `ZenarError` | |
| Section 16/72: MSRV 1.89 | MSRV 1.89 | The dependency graph resolves on 1.89. The full build is verified by CI, not on the reference machine. |

## Metadata schema rules

- A field name joins the schema the first time a record uses it. Its type is that first value's type.
- Later adds may use any subset of those fields. Missing fields are NULL.
- A new field name, or a value of a different type, raises `InvalidMetadataError`. Integers widen to floats. Mixed numeric
  comparisons in filters are allowed, and evaluated by value.
- Field and collection names: lowercase letters, digits and `_`, starting with a letter, at most 64 characters.

A field keeps its type for the life of the collection. A value of another type for that field is rejected.
