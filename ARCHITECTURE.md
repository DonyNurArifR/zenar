# Architecture

## Crates

- **zenar-core** (pure Rust, no PyO3): `VectorDB`, `Collection`, filter AST and compiler, storage wrappers.
- **zenar-python** (PyO3, extension `zenar._zenar`): Python classes, exception mapping, GIL release,
  panic guard. Compiled by maturin.
- **python/zenar**: thin package that re-exports the extension.

## Why TurboVec and Stoolap

TurboVec is a compressed flat vector index with fixed inner-product scoring, true O(1) removal and a
TQ+ calibration step. Stoolap is an embedded SQL engine with MVCC, prepared plans and typed columns, which
fits metadata filtering and recovery state.

### The alternative considered: Stoolap only

Stoolap 0.4 also has `VECTOR(N)` columns and HNSW indexes, so a single engine could hold everything.
We kept the two-engine design for three reasons:

1. TurboVec stores 4-bit codes, and its unfiltered search is about 1 ms at 1M rows (BENCHMARKS.md). We did not
   benchmark Stoolap's HNSW at the same size, so the comparison is not complete.
2. Stoolap's HNSW is an approximate index with its own metric and tuning surface. TurboVec's exact
   allowlist search gives the filter guarantee directly.
3. Keeping the vector index behind a small wrapper leaves the option to swap it (see the escape hatches in the
   design spec, section 91).

The costs are real: two stores, a consistency protocol (see [STORAGE.md](STORAGE.md)), and a
durability gap between them. Revisit this decision if the consistency protocol becomes the bottleneck or
a bug source.

## Query path

```text
search(vector, top_k, filter)
  no filter  -> TurboVec top-K over all vectors            (Stoolap not touched)
  filter     -> Stoolap: candidate ids (READY only)
                -> keep ids present in TurboVec            (stale ids are dropped, not errors)
                -> empty? return []                        (TurboVec not touched)
                -> TurboVec top-K over the allowlist
                -> Stoolap: text and metadata for top-K only
```

Filter SQL is cached by its text (`MetadataStore::prepared`). Filters with the same shape but different values
reuse one prepared plan.

## Concurrency

- One `RwLock` per collection. Searches run concurrently. Writes (add, delete, flush) are serialized.
- Stoolap `Database` is `Send + Sync`. The metadata store is shared through an `Arc`.
- The GIL is released around every core call (`py.detach`).
- No panic crosses into Python: each core call is wrapped in `catch_unwind` and reported as `VectorStoreError`.

## Deviations from the design specification

Each deviation was chosen to satisfy another requirement in the spec. They are listed here so they are not
rediscovered later.

| Spec | Implemented | Reason |
|---|---|---|
| Section 69: dimension must be a multiple of 8 | Any dimension up to 16384, zero-padded internally | Phase 5 requires the section 39 example (`dimension=3`) to work. Zero padding leaves dot products and norms unchanged, so scores are exact. |
| Section 4.2: one `documents` table | One table per collection (`zdocs_<name>`) plus `zenar_collections` | Typed metadata columns per collection; the schema is locked by the first add. Collection names are validated, so they are safe as identifiers. |
| Section 73: names `[a-zA-Z0-9_-]` | `[a-z0-9_]`, starting with a letter, max 64 | Stoolap identifiers are case-insensitive; hyphens would need mangling. Rejected, not sanitized. |
| Section 67/86: sync on every write | Sync every 1000 adds, every delete, and on `flush()` | Per-add sync made ingest quadratic. See STORAGE.md. |
| Section 85: Stoolap fsync | `sync_mode=normal` by default | Per-commit fsync was ~600x slower. Recovery covers the lost-commit case. `durable=True` restores fsync. |
| Section 74: `rebuild(name)` | Re-runs reconciliation and rewrites the index file | Stoolap does not store vectors, so a true rebuild needs the source vectors. |
| Section 18: `search` results | Same shape, plus `metadata` as a dict | |
| Section 47/74 error model | Every variant mapped to a Python class under `ZenarError` | |
| Section 16/72: MSRV 1.89 | MSRV 1.90 | `roaring 0.11.5` and `ordered-float 5.5.0` (pulled in by stoolap/turbovec) require rustc 1.90. |

## Metadata schema rules

- The first `add` that carries metadata defines the schema: each field's name and type.
- Later adds may use any subset of those fields. Missing fields are stored as NULL.
- A new field name, or a value of a different type, raises `InvalidMetadataError`. Integers widen to floats.
- Field names: lowercase letters, digits and `_`, starting with a letter. The names `id`, `text`, `status` are reserved.

Schema changes after the first add are not supported. Adding a field later means a new collection.
