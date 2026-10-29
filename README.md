# Zenar

Embedded, lightweight, high-performance vector database for Python — a Rust
core built on [TurboVec](https://github.com/RyanCodrai/turbovec) for vector
search and [Stoolap](https://github.com/stoolap/stoolap) for metadata/SQL
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
TurboVec               Stoolap
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

Not yet passed:

- **Reopen blocker (production).** After a reopen, metadata lookups for search results scan the table in
  Stoolap 0.4. At 1M rows, that is ~46 ms per query. The reproduction and options are in BENCHMARKS.md.
  This needs a decision before Zenar can be called production-ready on large collections.
- **Filtered-search performance gate.** At 1M rows and 10% selectivity, filtered search takes ~46 ms,
  against ~1.2 ms unfiltered. Stoolap's candidate query is ~95% of that. See BENCHMARKS.md.
- **Cross-platform wheels and CI** are written (`.github/workflows/`) but have not been run on
  every target. The MSRV is 1.90, not 1.89 as first planned.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

Zenar is dual-licensed under the MIT license
([LICENSE-MIT](LICENSE-MIT)) and the Apache License, Version 2.0
([LICENSE-APACHE](LICENSE-APACHE)). Anyone using Zenar may choose to comply
with either one.

Zenar's compiled distribution statically links [TurboVec](https://github.com/RyanCodrai/turbovec)
(MIT) and [Stoolap](https://github.com/stoolap/stoolap) (Apache-2.0). Their
full license texts are reproduced in [THIRD_PARTY_NOTICES](THIRD_PARTY_NOTICES),
as required by their own license terms. Zenar is not affiliated with either
project.
