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


def test_a_stale_index_from_an_interrupted_drop_is_not_loaded_by_a_new_collection():
    root = tempfile.mkdtemp()
    db = zenar.VectorDB(root)
    c = db.create_collection("a", dimension=8)
    c.add_many([{"id": i, "vector": [1.0] * 8, "text": "t", "metadata": {}} for i in range(20)])
    db.close()
    index = pathlib.Path(root, "vectors", "a.tvim")
    stale = index.read_bytes()
    db = zenar.VectorDB(root)
    db.drop_collection("a")
    db.close()
    index.write_bytes(stale)  # the leftover an interrupted drop could leave behind
    db = zenar.VectorDB(root)
    db.create_collection("a")  # no dimension: nothing is written to disk until the first add
    db.close()
    db = zenar.VectorDB(root)
    assert db.get_collection("a").count() == 0
    assert db.recovery_report()["orphaned_vectors_removed"] == 0
    db.close()


def test_a_handle_cannot_write_after_its_database_is_closed():
    root = tempfile.mkdtemp()
    db = zenar.VectorDB(root)
    c = db.create_collection("docs", dimension=3)
    c.add(id=1, vector=[1.0, 0.0, 0.0], text="kept")
    db.close()
    with pytest.raises(zenar.CollectionClosedError):
        c.add(id=2, vector=[1.0, 0.0, 0.0], text="lost")
    with pytest.raises(zenar.CollectionClosedError):
        c.count()
    reopened = zenar.VectorDB(root)
    assert reopened.get_collection("docs").count() == 1
    reopened.close()


def test_a_handle_cannot_write_after_its_collection_is_dropped():
    db = zenar.VectorDB(tempfile.mkdtemp())
    c = db.create_collection("docs", dimension=3)
    db.drop_collection("docs")
    with pytest.raises(zenar.CollectionClosedError):
        c.add(id=1, vector=[1.0, 0.0, 0.0], text="lost")
    assert "docs" not in db.list_collections()
    db.close()


@pytest.mark.parametrize("make_filter", [
    lambda f: f.p == float("nan"),
    lambda f: f.p < float("nan"),
    lambda f: f.p >= float("nan"),
    lambda f: f.p.isin([float("nan")]),
])
def test_a_nan_filter_value_is_refused(make_filter):
    db = zenar.VectorDB(tempfile.mkdtemp())
    c = db.create_collection("docs", dimension=3)
    c.add(id=1, vector=[1.0, 0.0, 0.0], text="a", metadata={"p": 1.0})
    with pytest.raises(zenar.InvalidFilterError):
        c.search([1.0, 0.0, 0.0], top_k=5, filter=make_filter(zenar.field))
    db.close()


def test_only_one_dimensional_arrays_are_vectors():
    np = pytest.importorskip("numpy")
    db = zenar.VectorDB(tempfile.mkdtemp())
    c = db.create_collection("docs", dimension=3)
    c.add(id=1, vector=np.array([1.0, 0.0, 0.0]), text="ok")
    for shape_vector in (np.array([[1.0, 0.0, 0.0]]), np.array([[1.0], [0.0], [0.0]])):
        with pytest.raises(zenar.InvalidVectorError):
            c.add(id=2, vector=shape_vector, text="flattened")
    assert c.count() == 1
    db.close()


def test_a_non_object_scope_names_the_scope_argument():
    db = zenar.VectorDB(tempfile.mkdtemp())
    c = db.create_collection("docs", dimension=3)
    c.add(id=1, vector=[1.0, 0.0, 0.0], text="a", metadata={"tenant": "A"})
    with pytest.raises(zenar.InvalidFilterError, match="scope must be a JSON object"):
        c.describe(scope="A")
    db.close()


def test_calling_without_a_path_uses_a_default_folder_in_the_current_directory(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    db = zenar.VectorDB()
    db.create_collection("docs", dimension=3).add(id=1, vector=[1.0, 0.0, 0.0], text="a")
    db.close()
    assert (tmp_path / "zenar" / "catalog.json").is_file()
    reopened = zenar.VectorDB()
    assert reopened.get_collection("docs").count() == 1
    reopened.close()


def test_a_document_may_have_more_fields_than_a_16_bit_count_holds():
    db = zenar.VectorDB(tempfile.mkdtemp())
    c = db.create_collection("wide", dimension=3)
    fields = {f"f{i}": i for i in range(70_000)}
    c.add(id=1, vector=[1.0, 0.0, 0.0], text="wide", metadata=fields)
    assert c.get(1)["metadata"]["f69999"] == 69_999
    hits = c.search([1.0, 0.0, 0.0], top_k=1, filter=zenar.field.f69999 == 69_999)
    assert [h["id"] for h in hits] == [1]
    db.close()
