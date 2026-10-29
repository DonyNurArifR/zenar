# Architecture

## Crates

- **zenar-core** (pure Rust, no PyO3): `VectorDB`, `Collection`, filter AST and compiler, storage wrappers.
- **zenar-python** (PyO3, extension `zenar._zenar`): Python classes, exception mapping, GIL release,
  panic guard. Compiled by maturin.
- **python/zenar**: thin package that re-exports the extension.

## Why TurboVec and SQLite

TurboVec is a compressed flat vector index with fixed inner-product scoring, true O(1) removal and a
TQ+ calibration step. SQLite (through rusqlite, bundled) is the metadata store. It is embedded, public
domain, and its WAL mode gives one writer with concurrent readers. Its primary-key lookups stay fast after a
reopen, which the previous engine (Stoolap 0.4) did not (see BENCHMARKS.md).

The metadata engine was first Stoolap 0.4 and was replaced by SQLite. The reasons are in BENCHMARKS.md. The Zenar-owned document store is the planned next step, for the broad-filter cases
where SQLite's per-row lookups dominate.

### The alternative considered: one engine

Stoolap 0.4 has `VECTOR(N)` columns and HNSW, so one engine could in principle hold everything. We kept the
vector index separate:

1. TurboVec stores 4-bit codes. Its unfiltered search is about 1 ms at 1M rows (BENCHMARKS.md). An HNSW
   index at the same size was not benchmarked here.
2. HNSW is approximate and has its own metric and tuning surface. TurboVec's exact allowlist search gives the
   filter guarantee directly.
3. The vector index sits behind a small wrapper, so it can be swapped (design spec, section 91).

The costs: two stores, a consistency protocol (see [STORAGE.md](STORAGE.md)), and the recovery code that
reconciles them.

## Query path

```text
search(vector, top_k, filter)
  no filter  -> TurboVec top-K over all vectors            (SQLite not touched)
  filter     -> SQLite: candidate ids for the filter        (index scan, no table reads for ids alone)
                -> keep ids present in TurboVec            (stale ids are dropped, not errors)
                -> empty? return []                        (TurboVec not touched)
                -> TurboVec top-K over the allowlist
                -> SQLite: text and metadata for top-K only (READY rows, by primary key)
```

Filter SQL is cached per connection by its text (`prepare_cached`). Filters with the same shape but different
values reuse one prepared statement.

## Concurrency

- One `RwLock` per collection. Searches run concurrently. Writes (add, delete, flush) are serialized.
- SQLite: one writer connection behind a mutex. A pool of four read-only connections in WAL mode, so reads run
  alongside writes. The metadata store is shared through an `Arc`.
- The GIL is released around every core call (`py.detach`).
- No panic crosses into Python: each core call is wrapped in `catch_unwind` and reported as `VectorStoreError`.

## Deviations from the design specification

Each deviation was chosen to satisfy another requirement in the spec. They are listed here so they are not
rediscovered later.

| Spec | Implemented | Reason |
|---|---|---|
| Section 69: dimension must be a multiple of 8 | Any dimension up to 16384, zero-padded internally | Phase 5 requires the section 39 example (`dimension=3`) to work. Zero padding leaves dot products and norms unchanged, so scores are exact. |
| Section 4.2: one `documents` table | One table per collection (`zdocs_<name>`) plus `zenar_collections` | Typed metadata columns per collection; the schema is locked by the first add. Collection names are validated, so they are safe as identifiers. |
| Section 10: `Filter` AST | Same AST, compiled to parameterized SQLite SQL | Booleans stored as 0/1 and timestamps as UTC microseconds, so ordering holds. |
| Section 69 + 13: indexes on frequent fields | An index on every metadata field, created when the field is added | Filtered search needs them (BENCHMARKS.md). The cost is ingest speed. |
| Section 73: names `[a-zA-Z0-9_-]` | `[a-z0-9_]`, starting with a letter, max 64 | Identifiers are interpolated into SQL, so the allowlist stays narrow. Rejected, not sanitized. |
| Section 67/86: sync on every write | Sync every 1000 adds, every delete, and on `flush()` | Per-add sync made ingest quadratic. See STORAGE.md. |
| Section 85: per-commit fsync | `synchronous=NORMAL` by default, `FULL` with `durable=True` | Per-commit fsync was ~600x slower on Stoolap. `flush()` checkpoints the WAL, so flushed adds are power-safe. |
| Section 74: `rebuild(name)` | Re-runs reconciliation and rewrites the index file | The metadata store does not hold vectors, so a true rebuild needs the source vectors. |
| Section 18: `search` results | Same shape, plus `metadata` as a dict | |
| Section 47/74 error model | Every variant mapped to a Python class under `ZenarError` | |
| Section 16/72: MSRV 1.89 | MSRV 1.89 | Dependency graph resolves on 1.89 once Stoolap is gone. The full build on 1.89 is verified by CI, not on this machine. |

## Metadata schema rules

- The first `add` that carries metadata defines the schema: each field's name and type.
- Later adds may use any subset of those fields. Missing fields are stored as NULL.
- A new field name, or a value of a different type, raises `InvalidMetadataError`. Integers widen to floats.
- Field names: lowercase letters, digits and `_`, starting with a letter. The names `id`, `text`, `status` are reserved.

Schema changes after the first add are not supported. Adding a field later means a new collection.
