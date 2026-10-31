# Zenar

Embedded, lightweight, high-performance vector database for Python — a Rust
core built on [TurboVec](https://github.com/RyanCodrai/turbovec) for vector
search and a document log with in-memory indexes for metadata filtering.

**Status: 0.1.1 is the latest release (GitHub, wheels attached). Not yet on PyPI.** The core, Python API, persistence and
crash recovery are implemented and tested. At 1M rows, filtered search takes
0.3 to 3.0 ms against 0.4 ms unfiltered, depending on the filter. The design
spec makes the filter path a pass/fail gate but sets no numeric threshold, so
this is measured, not yet judged. See [Project Status](#project-status).

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
TurboVec          doc log + indexes
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

Full reference: [docs/API.md](docs/API.md). Filter semantics: [docs/FILTERING.md](docs/FILTERING.md).
Durability and recovery: [docs/STORAGE.md](docs/STORAGE.md). Design: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).
Measurements: [docs/BENCHMARKS.md](docs/BENCHMARKS.md).

## Project Status

Implemented and tested (Rust: 58 unit and 27 integration tests, including a reference-checked three-valued filter
oracle, a model-based test with reopens, a random-filter fuzz test, a concurrent read/write test, and a test that cuts
or flips every byte of the last log record; Python: 84 tests, including repeated `kill -9` crash cycles):

- Collections, add, add_many (atomic), delete, delete_many (atomic), get, search, with the query planner and stale-ID handling.
- Typed metadata, the full filter operator set, NULL and type rules, tenant isolation.
- A Zenar-owned document log with in-memory indexes; torn-tail and corruption handling; reconciliation on open.
- Process locking, `flush()`, `durable=True`, and a poisoning rule for collections that cannot be made consistent.
- `bit_width` and `calibrate()`.
- Python bindings with a complete exception hierarchy, GIL release, numpy input, and a panic guard.
- Agent tools: `describe` (fields, values and counts), `find_values`, `retrieve` with a JSON `where`
  filter that suggests field names and explains empty results, and `tool_spec` for tool-calling models.
  See docs/API.md, "Agent tools".

Measured on the reference machine at 1M rows, dim 384 (docs/BENCHMARKS.md):

- Ingest with `add_many`: 0.011 ms per row. One row at a time: 0.014 ms.
- Unfiltered search: 0.4 ms. Reopened unfiltered search: 0.4 ms. Open (log replay and index load): 0.44 s.
- Selective filter (0.1%): 0.3 ms. Filter on 10% of rows: 2.9 ms. Compound filter (~4%): 3.0 ms.
- Peak memory at this size: 418 MB (4-bit vectors, about 192 MB of it).

Not done:

- **Cross-platform CI** runs on GitHub Actions for Ubuntu, macOS and Windows: Rust tests on Rust 1.89 and stable,
  Python wheel tests in a fresh venv on all three OS, plus a dependency audit and license check. Each release builds
  five wheels and an sdist, and smoke-tests them on three OS. The Intel macOS wheel is built but has not been run.
  No PyPI release yet.
- **Power-loss behavior** is tested at the process level (`kill -9`) and at the byte level (every cut point in a torn
  record). Real power-loss testing on hardware is not done.
- **Compound filters** cost about 8x an unfiltered search at 1M rows. Most of that is TurboVec's allowlist search, not
  Zenar's (docs/BENCHMARKS.md).
- **Open time** grows with the log: 0.45 s at 1M rows. There is no persisted index yet (docs/BENCHMARKS.md, known gaps).
- **Agent tools** depend on the model. With local models, filters are sometimes sent as text (Zenar decodes them) or
  drop conditions. Accuracy against hosted models is not measured (docs/API.md, Agent tools).

## Contributing

See [docs/CONTRIBUTING.md](docs/CONTRIBUTING.md).

## License

Zenar is dual-licensed under the MIT license
([LICENSE-MIT](LICENSE-MIT)) and the Apache License, Version 2.0
([LICENSE-APACHE](LICENSE-APACHE)). Anyone using Zenar may choose to comply
with either one.

Zenar's compiled distribution statically links [TurboVec](https://github.com/RyanCodrai/turbovec)
(MIT). Its license text, and the license texts of every other shipped crate, are
reproduced in [THIRD_PARTY_NOTICES](THIRD_PARTY_NOTICES), as required by their terms. Zenar is not affiliated
with any of these projects.
