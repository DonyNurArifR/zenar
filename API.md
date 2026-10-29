# Zenar Python API

```python
from zenar import VectorDB, field as f

with VectorDB("./data") as db:                       # flushes and releases the lock on exit
    docs = db.create_collection("docs", dimension=3)  # dimension optional, bit_width=4 default
    docs.add(id=1, vector=[1.0, 0.0, 0.0], text="hello", metadata={"tenant": "A"})
    docs.search([1.0, 0.0, 0.0], top_k=1, tenant="A")
    docs.search([1.0, 0.0, 0.0], top_k=10, filter=f.tenant == "A")
```

## VectorDB

| Method | Notes |
|---|---|
| `VectorDB(path, durable=False)` | Opens or creates a database directory. Raises `DatabaseLockedError` if already open. `durable=True` syncs the index and the document log on every add. Power-safe per add, at about 7 ms per row. Use `add_many` batches instead when you can. |
| `create_collection(name, dimension=None, bit_width=4)` | `dimension=None` is inferred from the first `add`. `bit_width` is 2, 3 or 4. |
| `get_collection(name)` / `list_collections()` | |
| `drop_collection(name)` | Deletes documents and the vector file. |
| `rebuild(name)` | Re-runs crash reconciliation for one collection and rewrites its index file. Returns the repair counts. |
| `recovery_report()` | Repairs made when the database was opened. |
| `stats()` | Per-collection row counts. |
| `flush()` | Syncs the index and the document log of every collection. Every add made before it returns survives a process crash and a power loss. |
| `close()` / `with` | Flushes and releases the lock. |

## Collection

| Method | Notes |
|---|---|
| `add(id, vector, text, metadata=None)` | `id` is a non-negative integer up to 2**63-1. Duplicate ids raise `DuplicateIdError`. |
| `add_many(records)` | Each record is a dict with `id`, `vector`, `text`, optional `metadata`. **Atomic:** every record is validated first, then all are written in one transaction. If any record fails, none is added. Returns the count. Use this for ingest: it is about 1.8x faster than a loop of `add`. |
| `get(id)` / `get_many(ids)` | Return dicts `{id, text, metadata}`, or `None` / skipped ids. Vectors are not returned. |
| `delete(id)` | Removes the vector from the index and syncs before returning. Raises `VectorNotFoundError` if absent. |
| `delete_many(ids)` | Deletes many ids atomically with one index sync and one log sync: all of them are deleted, or none is. Use this for bulk deletes. |
| `search(vector, top_k=10, filter=None, **equality)` | Returns `[{id, score, text, metadata}]`, best first. |
| `count()`, `stats()`, `dimension`, `bit_width` | |
| `calibrate(sample)` | Fits the quantizer from a flat sample of `n * dimension` floats. See below. |

`vector` may be a list of floats or a numpy array (float32 or float64).

### Search arguments

Keyword arguments are equality conditions ANDed together:

```python
docs.search(q, top_k=5, tenant="A", category="faq")
```

For anything else, pass `filter=` built from `field` comparisons. Combine them with `&`, `|` and `~`:

```python
docs.search(q, filter=(f.tenant == "A") & (f.price >= 50) & ~(f.language == "fr"))
docs.search(q, filter=f.category.isin(["faq", "product"]))
```

**Every comparison must be parenthesized** when combined with `&`, `|`, `~`. Without parentheses,
`f.price >= 50 & f.price <= 500` raises `TypeError`. The expression fails loudly rather than silently
changing meaning. `filter=` and keyword conditions together raise `InvalidFilterError`.

Operators: `==`, `!=`, `>`, `>=`, `<`, `<=`, `.isin(...)`, `&`, `|`, `~`. See [FILTERING.md](FILTERING.md)
for NULL and type rules.

### Calibration and bit width

`bit_width=4` (default) gives the best recall at the largest memory cost; 2 is the smallest. The index
is usable without calibration. Calibrating (`calibrate(sample)`) improves recall, and calibrating after a
large uncalibrated ingest costs recall compared with calibrating first. Use a uniform random sample of
your vectors, at least 256 of them (`n * dimension` floats).

## Metadata values

| Python type | Stored as | Notes |
|---|---|---|
| `str` | text | |
| `int` | integer | Must fit in 64 bits. |
| `float` | float | Must be finite. An `int` value on a float field is widened. |
| `bool` | boolean | Ordering comparisons are rejected. |
| `datetime` | timestamp (UTC) | Must be timezone-aware. Naive datetimes raise `InvalidMetadataError`. Returned as aware UTC. |

## Errors

Every error inherits from `zenar.ZenarError`:

`CollectionNotFoundError`, `CollectionAlreadyExistsError`, `InvalidCollectionNameError`,
`DimensionMismatchError`, `DuplicateIdError`, `VectorNotFoundError`, `InvalidVectorError`,
`InvalidMetadataError`, `InvalidFilterError`, `InvalidArgumentError`, `DatabaseLockedError`,
`VectorStoreError`, `MetadataStoreError`, `PersistenceError`, `RecoveryError`.

Catch `ZenarError` to handle any library error.
