# Zenar

Embedded, lightweight, high-performance vector database for Python — a Rust
core built on [TurboVec](https://github.com/RyanCodrai/turbovec) for vector
search and [SQLite](https://sqlite.org) (via rusqlite) for metadata/SQL
filtering.

**Status: 0.1.0, not yet published.** The core, Python API, persistence and
crash recovery are implemented and tested. Filtered search at 1M rows does not
yet meet its performance target. See [Project Status](#project-status).

## Why Zenar

- **Embedded, serverless** — no Docker, no external service, just a Python import.
- **Fast and light** — TurboVec's quantized vector index compresses roughly
  16x versus raw float32 (e.g. ~31GB → ~4GB at 10M vectors), with
  SIMD-accelerated search.
- **Correct metadata filtering** — filters resolve to an exact candidate-ID
  set *before* vector search runs, not a post-hoc pass over results. A
  tenant-scoped filter can't leak results from another tenant.
- **Rust core, Python API** — the performance-critical path is Rust; Python
  stays the ergonomic surface.

## Architecture

```
Python
  |
  v
Zenar (Rust, PyO3)
  |
  +----------------------+
  |                       |
  v                       v
TurboVec               SQLite
vectors / ANN          metadata / SQL
```

Zenar doesn't reimplement a vector index or a SQL engine — it's a thin
integration layer over two purpose-built Rust crates, plus the coordination
logic (ID mapping, consistency, query planning) needed to make them behave
like one database.

## Usage

Build the extension from a checkout (PyPI wheels come with the first release):

```sh
python -m venv .venv && . .venv/bin/activate
pip install maturin pytest numpy
maturin develop --release
```

```python
from zenar import VectorDB, field as f

with VectorDB("./my_vectors") as db:
    docs = db.create_collection("documents", dimension=3)

    docs.add(id=1, vector=[1.0, 0.0, 0.0], text="Hello world",
             metadata={"tenant": "A", "category": "faq", "price": 150.0})

    docs.search([1.0, 0.0, 0.0], top_k=5, tenant="A")                  # keyword equality
    docs.search([1.0, 0.0, 0.0], top_k=5,
                filter=(f.price >= 100) & ((f.category == "faq") | (f.category == "product")))
```

- `dimension` is optional. Omit it and Zenar takes it from the first `add()`.
- Dimensions that are not multiples of 8 are accepted. Zenar pads them internally, and scores are unchanged.
- Every comparison in a `filter=` expression must be parenthesized when combined with `&`, `|`, `~`.

Full reference: [API.md](API.md). Filter semantics: [FILTERING.md](FILTERING.md).
Durability and recovery: [STORAGE.md](STORAGE.md). Design: [ARCHITECTURE.md](ARCHITECTURE.md).
Measurements: [BENCHMARKS.md](BENCHMARKS.md).

## Project Status

Implemented and tested (Rust: 41 unit and 11 integration tests, including a random-filter fuzz test and
a concurrent read/write test; Python: 29 tests, including `kill -9` crash tests):

- Collections, add, get, delete, search, with the query planner and stale-ID handling.
- Typed metadata, the full filter operator set, NULL and type rules, tenant isolation.
- Persistence, crash reconciliation, process locking, `flush()`.
- `bit_width` and `calibrate()`.
- Python bindings with a complete exception hierarchy, GIL release, numpy input, and a panic guard.

Measured on the reference machine at 1M rows, dim 384 (BENCHMARKS.md):

- Selective filters (0.1%) search in 0.34 ms. Unfiltered search is 1.2 ms. Reopen costs nothing extra.
- Broad filters are the weak case. `tenant = A` (10%) takes 5.7 ms. A compound filter of non-selective
  predicates takes 25 ms, because each candidate row is read from the table. This does not fully meet the design
  spec's section 90 gate.
- Ingest is 0.030 ms per row with `add_many` batches of 1000 (`add` one row at a time: 0.053 ms).

Not yet done:

- **Cross-platform wheels and CI** are written (`.github/workflows/`) but have not run on GitHub or on every target.
  Verified here: macOS arm64 only.
- **Zenar-owned document store** (planned) for broad-filter performance and ingest speed.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

Zenar is dual-licensed under the MIT license
([LICENSE-MIT](LICENSE-MIT)) and the Apache License, Version 2.0
([LICENSE-APACHE](LICENSE-APACHE)). Anyone using Zenar may choose to comply
with either one.

Zenar's compiled distribution statically links [TurboVec](https://github.com/RyanCodrai/turbovec)
(MIT), [rusqlite](https://github.com/rusqlite/rusqlite) (MIT) and the bundled
[SQLite](https://sqlite.org) (public domain). Their license texts are reproduced in
[THIRD_PARTY_NOTICES](THIRD_PARTY_NOTICES), as required by their terms. Zenar is not affiliated with
any of these projects.
