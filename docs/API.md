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
| `VectorDB(path=None, durable=False)` | Opens or creates a database directory. With no `path`, the database lives in `./zenar` in the current directory, created if missing. An explicit `path` is used exactly as given, and created if missing. Raises `DatabaseLockedError` if already open. `durable=True` syncs the index and the document log on every add. Power-safe per add, at about 7 ms per row. Use `add_many` batches instead when you can. |
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
| `add_many(records)` | Each record is a dict with `id`, `vector`, `text`, optional `metadata`. **Atomic:** every record is validated first, then all are written in one transaction. If any record fails, none is added. Returns the count. Use this for ingest: it is faster per row than a loop of `add`, and it syncs once per batch (BENCHMARKS.md). |
| `get(id)` / `get_many(ids)` | Return dicts `{id, text, metadata}`, or `None` / skipped ids. Vectors are not returned. |
| `delete(id)` | Removes the vector from the index and syncs before returning. Raises `VectorNotFoundError` if absent. |
| `delete_many(ids)` | Deletes many ids atomically with one index sync and one log sync: all of them are deleted, or none is. Use this for bulk deletes. |
| `search(vector, top_k=10, filter=None, **equality)` | Returns `[{id, score, text, metadata}]`, best first. Returns `top_k` results when that many committed documents match. Documents added by a write that has not returned yet are not returned. The score is the inner product of the query and the stored vector, the only metric Zenar supports. |
| `name`, `count()`, `stats()`, `dimension`, `bit_width` | `name` is the collection's name. |
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

`CollectionNotFoundError`, `CollectionClosedError`, `CollectionAlreadyExistsError`, `InvalidCollectionNameError`,
`DimensionMismatchError`, `DuplicateIdError`, `VectorNotFoundError`, `InvalidVectorError`,
`InvalidMetadataError`, `InvalidFilterError`, `InvalidArgumentError`, `DatabaseLockedError`,
`VectorStoreError`, `MetadataStoreError`, `PersistenceError`, `RecoveryError`.

`DimensionMismatchError` means a vector's length differs from the collection's dimension. A dimension argument that is out of range (zero, or above the limit) raises `InvalidArgumentError`.

`PersistenceError` after a failed write means the collection is poisoned. Every later call on it raises the same error
until the database is reopened. A failed write that was cleanly undone leaves the collection usable, but the write may
still have been applied. Check with `get()` before retrying (STORAGE.md, "What a failure leaves behind").

`RecoveryError` on open means a file was removed, truncated or damaged outside Zenar. The open is refused and the
files are left as they were.

Concurrent writes to one collection can share a sync. A write returns only after its own sync, so an `add` or `add_many`
that returns without error is durable under the same rules as before. An error from a write means it was not applied,
except as STORAGE.md describes for errors after a sync.

`CollectionClosedError` means the handle outlived its database, or its collection was dropped. Keep a reference to the
`VectorDB` for as long as you use its collections. A collection handle cannot write after its database is closed, since
such a write would look successful and then be lost.

Catch `ZenarError` to handle any library error.

## Agent tools

`search` is pure vector search. Agents use `retrieve` and the discovery methods below. They are built for a model
that writes the filter itself, so they add a scope the model cannot change.

```python
c = db.get_collection("products")
scope = {"tenant": tenant_of(current_user)}   # fixed by your application, never by the model

c.describe(scope=scope)                              # fields: type, documents, values with counts
c.find_values("category", "elec", scope=scope)       # values containing "elec", most frequent first
spec = c.tool_spec("search_products", scope=scope)   # tool definition, a dict with create_prompt()
spec.create_prompt(instructions="You help customers find products.")
c.tool_spec("search_products", prompt_only=True, scope=scope)   # just the fields and values, as text

r = c.retrieve(embed(user_query), top_k=5, where=model_filter, scope=scope)
r["hits"]      # the results
r["matched"]   # documents that matched the filter, before top_k cut the list
r["warnings"]  # for example: no live document has tenant = "Z"; values that exist: "A", "B"
```

The caller embeds the text. `retrieve` takes the vector.

### `scope` is required

Every agent method takes `scope` as a keyword argument, and it has no default. Leaving it out is a `TypeError`.
Passing `scope=None` means the whole collection. Use it only from trusted code, never for a model-facing tool.

The scope is combined with the model's `where` on the server, with AND. The model cannot widen it with `or`, and it
cannot exclude it with `not`. Discovery (`describe`, `find_values`, `tool_spec`, the prompt, and the warnings) is
computed over the documents inside the scope, so it names no value from outside.

An unknown field in the scope is an error, not an empty scope.

### The `where` format

A JSON object. Each key is a field name. A plain value means equality. An object gives operators:

| Form | Meaning |
|---|---|
| `{"tenant": "A"}` | `tenant` equals `"A"` |
| `{"price": {"gte": 10, "lt": 50}}` | both conditions hold |
| `{"category": {"in": ["tools", "garden"]}}` | one of the values |
| `{"or": [{...}, {...}]}` | at least one object matches |
| `{"and": [{...}, {...}]}` | all objects match |
| `{"not": {...}}` | the object does not match |

Operators: `eq`, `ne`, `gt`, `gte`, `lt`, `lte`, `in`, `nin`. Timestamps are RFC 3339 strings. Field names are
case-sensitive. An empty object matches everything. `and`, `or` and `not` cannot be field names. A `where` larger than
16 KB is refused. A `where` sent as a JSON-encoded string is decoded first, so `'{"tenant": "A"}'` works the same as
`{"tenant": "A"}`; a string that does not decode to an object is refused.

### Errors that help a model fix the filter

- An unknown field names the valid fields, and suggests a close one: `unknown field "catgory", did you mean "category"?`
- A wrong type names the expected type: `field "price" is float; expected a number, got the string "cheap"`.
- An unknown operator lists the valid ones.
- A value that no document in scope has is not an error. It returns zero hits, and `warnings` lists the values that
  exist in scope.

### What the model sees

`tool_spec` describes the collection in plain language: each field with its type and its values, or its range for a
field with many values. The `where` schema uses `enum` for short value lists, so the model is steered to values that
exist. Range bounds (`gt`, `gte`, `lt`, `lte`) are not restricted to existing values.

The schema is the same one the parser checks, at every level of `and`, `or` and `not`. Every `where` the schema accepts,
the parser accepts. The reverse is not true: a value outside the `enum` is accepted by the parser, with a warning, so
a call that matches nothing still returns zero hits, not an error.

The tool's `parameters` carry the shared `where` definition under `$defs`, and nested filters refer to it with `$ref`.
Some function-calling APIs do not support `$ref`. Check this with the provider you use before relying on nested filters.

The tool name must be 1 to 64 letters, digits, `_` or `-`. Any other name is refused by `tool_spec`.

### Limits and things your application must do

- Metadata values: `str`, `int` (64-bit), `float` (finite), `bool`, timezone-aware `datetime`, and numpy scalars (converted
  to their Python value). A `None` value means the field is absent. Other types are rejected.
- Metadata: a field name is added to the schema the first time a record uses it, with the type of that first value.
  A field keeps its type: a later value of another type is rejected. The number of fields is not limited. A document's
  field count is stored as 32 bits. Each field costs memory only for the documents that have it.

- Document text is data. If it can contain instructions written by someone else, do not present it to the model as
  instructions. Keep it inside a clearly marked data section.
- `retrieve` does not embed text. Zenar does not cap `top_k`: it returns at most the number of matching documents.
  A very large result is still expensive to send to a model, so cap `top_k` in your application when you forward the
  model's arguments.
- A scope is only as strong as the value you pass. Derive it from the authenticated user, not from the model's output.
- Discovery over a large scope costs more than retrieval. `describe` on a 5% scope of 1M documents takes about 14 ms,
  and it is not cached. Cache the spec per session if the data changes slowly.

### Discovery for many values

For a field with more than 200 distinct values, `describe` reports the range and the most frequent values, and does not
list them all. `find_values(field, text, scope=...)` searches the values instead. Use it when the user names something
specific.
