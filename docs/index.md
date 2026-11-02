# Zenar

Zenar is an embedded vector database for Python. There is no server to run: you import it, point it at a folder, and search.

It pairs a fast vector index ([TurboVec](https://github.com/RyanCodrai/turbovec)) with a document log that holds your metadata. Filters resolve to an exact set of candidate documents before the vector search runs, so a tenant-scoped query cannot return another tenant's documents.

`VectorDB()` with no argument keeps the database in `./zenar`, created if it does not exist. Pass a path to choose the
folder yourself.

## Install

Build from a checkout for now. Prebuilt wheels are attached to each GitHub release.

```sh
python -m venv .venv && . .venv/bin/activate
pip install maturin pytest numpy
maturin develop --release
```

## A first search

```python
from zenar import VectorDB, field as f

with VectorDB("./my_vectors") as db:
    docs = db.create_collection("documents", dimension=3)

    docs.add(id=1, vector=[1.0, 0.0, 0.0], text="Hello world",
             metadata={"tenant": "A", "category": "faq", "price": 150.0})

    docs.search([1.0, 0.0, 0.0], top_k=5, tenant="A")
    docs.search([1.0, 0.0, 0.0], top_k=5,
                filter=(f.price >= 100) & ((f.category == "faq") | (f.category == "product")))
```

## What it gives you

- **Embedded:** no Docker, no external service.
- **Correct filters:** tenant isolation holds by construction, and NULL and type rules are fixed and tested.
- **Durable:** an append-only log with checksums, torn-write recovery, and process-level locking.
- **Agent tools:** `describe`, `find_values`, `retrieve` and `tool_spec`, so a model can search with scoped, validated filters. See the [API reference](API.md).

## Where to go next

- [API reference](API.md): every call, parameter and error.
- [Filtering](FILTERING.md): operators, NULL semantics and tenant isolation.
- [Storage and recovery](STORAGE.md): what survives a crash, and what does not.
- [Benchmarks](BENCHMARKS.md): measured numbers at 10K, 100K and 1M rows.
- [Contributing](CONTRIBUTING.md): setup, tests and the rules for changes.

```{toctree}
:maxdepth: 1
:hidden:

API
FILTERING
STORAGE
ARCHITECTURE
CONCURRENCY
COMPACTION
R3
BENCHMARKS
ROADMAP
CONTRIBUTING
```
