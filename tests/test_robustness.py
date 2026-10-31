"""Bad inputs get a named error, and concurrent use keeps the collection consistent."""

import math
import pathlib
import random
import tempfile
import threading

import pytest

import zenar


@pytest.fixture
def coll():
    db = zenar.VectorDB(tempfile.mkdtemp())
    c = db.create_collection("docs", dimension=3)
    c.add(id=1, vector=[1.0, 0.0, 0.0], text="ok", metadata={"tenant": "A"})
    yield db, c
    db.close()


@pytest.mark.parametrize(
    "call, error",
    [
        (lambda c: c.add(id=2, vector=[math.nan, 0.0, 0.0], text="x"), zenar.InvalidVectorError),
        (lambda c: c.add(id=3, vector=[math.inf, 0.0, 0.0], text="x"), zenar.InvalidVectorError),
        (lambda c: c.add(id=4, vector=[], text="x"), zenar.InvalidVectorError),
        (lambda c: c.add(id=5, vector=[1.0, 2.0], text="x"), zenar.DimensionMismatchError),
        (lambda c: c.add(id=-1, vector=[1.0, 0.0, 0.0], text="x"), zenar.InvalidArgumentError),
        (lambda c: c.add(id=2**64, vector=[1.0, 0.0, 0.0], text="x"), zenar.InvalidArgumentError),
        (lambda c: c.add(id=6, vector=[1.0, 0.0, 0.0], text="x", metadata={"tenant": math.nan}), zenar.InvalidMetadataError),
        (lambda c: c.add(id=7, vector=[1.0, 0.0, 0.0], text="x", metadata={"tenant": {"a": 1}}), zenar.InvalidMetadataError),
        (lambda c: c.search([1.0, 0.0, 0.0], top_k=0), zenar.InvalidArgumentError),
        (lambda c: c.search([1.0, 0.0, 0.0], top_k=-5), zenar.InvalidArgumentError),
        (lambda c: c.search([math.nan, 0.0, 0.0], top_k=3), zenar.InvalidVectorError),
        (lambda c: c.search("not a vector", top_k=3), TypeError),
        (lambda c: c.delete(999999), zenar.VectorNotFoundError),
    ],
)
def test_bad_input_raises_the_named_error(coll, call, error):
    _, c = coll
    with pytest.raises(error):
        call(c)


@pytest.mark.parametrize(
    "call, error",
    [
        (lambda db: db.create_collection("bad name!", dimension=3), zenar.InvalidCollectionNameError),
        (lambda db: db.create_collection("docs", dimension=3), zenar.CollectionAlreadyExistsError),
        (lambda db: db.create_collection("b9", dimension=3, bit_width=9), zenar.InvalidArgumentError),
        (lambda db: db.create_collection("dh", dimension=10**9), zenar.InvalidArgumentError),
        (lambda db: db.get_collection("missing"), zenar.CollectionNotFoundError),
    ],
)
def test_bad_database_call_raises_the_named_error(coll, call, error):
    db, _ = coll
    with pytest.raises(error):
        call(db)


def test_concurrent_writers_and_readers_keep_the_collection_consistent():
    root = tempfile.mkdtemp()
    db = zenar.VectorDB(root)
    c = db.create_collection("docs", dimension=8)
    errors, lock = [], threading.Lock()
    added, deleted = set(), set()

    def writer(t):
        rng = random.Random(t)
        base = t * 100_000
        try:
            for i in range(150):
                doc = base + i
                c.add(id=doc, vector=[rng.random() for _ in range(8)], text=f"d{doc}",
                      metadata={"tenant": "A" if i % 2 else "B"})
                with lock:
                    added.add(doc)
                if i % 5 == 4:
                    c.delete(doc)
                    with lock:
                        deleted.add(doc)
        except zenar.ZenarError as e:
            with lock:
                errors.append(repr(e))

    def reader(t):
        rng = random.Random(1000 + t)
        try:
            for _ in range(200):
                hits = c.search([rng.random() for _ in range(8)], top_k=5, tenant="A")
                assert all(h["metadata"]["tenant"] == "A" for h in hits)
        except zenar.ZenarError as e:
            with lock:
                errors.append(repr(e))

    threads = [threading.Thread(target=writer, args=(t,)) for t in range(6)]
    threads += [threading.Thread(target=reader, args=(t,)) for t in range(3)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()

    live = added - deleted
    assert errors == []
    assert c.count() == len(live)
    assert all(c.get(d) is not None for d in list(live)[:100])
    assert all(c.get(d) is None for d in list(deleted)[:100])
    db.close()
    reopened = zenar.VectorDB(root)
    assert reopened.get_collection("docs").count() == len(live)
    reopened.close()


def _saved_collection(root):
    db = zenar.VectorDB(root)
    c = db.create_collection("docs", dimension=8)
    c.add_many([
        {"id": i, "vector": [float(i % 7 + 1)] * 8, "text": f"t{i}", "metadata": {"tenant": "A"}}
        for i in range(200)
    ])
    db.close()


@pytest.mark.parametrize("damage", ["log_deleted", "log_emptied", "log_cut_in_half", "vectors_deleted"])
def test_a_file_shortened_outside_zenar_is_refused_not_silently_dropped(damage):
    root = tempfile.mkdtemp()
    _saved_collection(root)
    log = pathlib.Path(root, "docs", "docs.log")
    vectors = pathlib.Path(root, "vectors", "docs.tvim")
    if damage == "log_deleted":
        log.unlink()
    elif damage == "log_emptied":
        log.write_bytes(b"")
    elif damage == "log_cut_in_half":
        data = log.read_bytes()
        log.write_bytes(data[: len(data) // 2])
    else:
        vectors.unlink()
    with pytest.raises(zenar.ZenarError):
        zenar.VectorDB(root)


def test_a_clean_reopen_still_keeps_every_document():
    root = tempfile.mkdtemp()
    _saved_collection(root)
    db = zenar.VectorDB(root)
    assert db.get_collection("docs").count() == 200
    db.close()
