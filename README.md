# Zenar

Embedded, lightweight, high-performance vector database for Python — a Rust
core built on [TurboVec](https://github.com/RyanCodrai/turbovec) for vector
search and [Stoolap](https://github.com/stoolap/stoolap) for metadata/SQL
filtering.

**Status: pre-alpha.** Not yet installable, not yet functional. See
[Project Status](#project-status).

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

## Target API

This is the API Zenar is being built toward — shown so it's clear what shape
the project is aiming for. **Not runnable yet.**

```python
from zenar import VectorDB

db = VectorDB("./my_vectors")

docs = db.create_collection("documents", dimension=1536)

docs.add(
    id=1,
    vector=embedding,
    text="Hello world",
    metadata={"tenant": "A", "category": "faq"},
)

results = docs.search(
    query_embedding,
    top_k=5,
    filter={"tenant": "A", "category": {"$in": ["faq", "product"]}},
)
```

## Project Status

Zenar is in early development. Nothing is published to PyPI or crates.io
yet, and the core add/search path isn't wired up. Not ready for use.

## License

Licensed under either of:

- MIT license ([LICENSE-MIT](LICENSE-MIT))
- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))

at your option.

TurboVec is MIT-licensed; Stoolap is Apache-2.0-licensed. Zenar is not
affiliated with either project.

## Contributing

Not yet set up for external contributions — this will change as the core
API stabilizes.
