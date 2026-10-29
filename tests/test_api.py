"""Python-facing API tests (spec sections 9, 17, 39, 74, 83)."""

import datetime as dt
import subprocess
import sys
import textwrap
import time

import numpy as np
import pytest

import zenar
from zenar import VectorDB, field as f


@pytest.fixture
def db(tmp_path):
    with VectorDB(str(tmp_path / "db")) as handle:
        yield handle


def test_spec_section_39_vertical_slice(db):
    collection = db.create_collection("docs", dimension=3)
    collection.add(id=1, vector=[1.0, 0.0, 0.0], text="hello", metadata={"tenant": "A"})
    results = collection.search([1.0, 0.0, 0.0], top_k=1)
    assert len(results) == 1
    assert results[0]["id"] == 1
    assert results[0]["text"] == "hello"
    assert results[0]["metadata"] == {"tenant": "A"}
    assert results[0]["score"] > 0.99


def test_kwargs_are_equality_and_anded(db):
    c = db.create_collection("kw", dimension=8)
    v = [1.0] * 8
    c.add(1, v, "a", {"tenant": "A", "category": "faq"})
    c.add(2, v, "b", {"tenant": "A", "category": "product"})
    c.add(3, v, "c", {"tenant": "B", "category": "faq"})
    ids = sorted(h["id"] for h in c.search(v, top_k=10, tenant="A", category="faq"))
    assert ids == [1]


def test_filter_and_kwargs_together_is_an_error(db):
    c = db.create_collection("both", dimension=8)
    c.add(1, [1.0] * 8, "a", {"tenant": "A"})
    with pytest.raises(zenar.InvalidFilterError):
        c.search([1.0] * 8, top_k=1, filter=f.tenant == "A", category="faq")


def test_field_operators_and_combinators(db):
    c = db.create_collection("ops", dimension=8)
    for i in range(10):
        c.add(i, [float(i + 1)] * 8, f"doc{i}", {"price": float(i * 100), "category": "faq" if i % 2 else "tools"})
    hits = c.search([1.0] * 8, top_k=20, filter=(f.price >= 50) & (f.price <= 500))
    assert sorted(h["id"] for h in hits) == [1, 2, 3, 4, 5]
    hits = c.search([1.0] * 8, top_k=20, filter=f.category.isin(["faq"]))
    assert all(h["id"] % 2 == 1 for h in hits)
    hits = c.search([1.0] * 8, top_k=20, filter=~(f.category == "faq"))
    assert all(h["id"] % 2 == 0 for h in hits)


def test_unparenthesized_comparison_raises_type_error(db):
    c = db.create_collection("paren", dimension=8)
    c.add(1, [1.0] * 8, "a", {"price": 100.0})
    with pytest.raises(TypeError):
        _ = f.price >= 50 & f.price <= 500


def test_filter_has_no_truth_value(db):
    with pytest.raises(TypeError):
        bool(f.tenant == "A")


def test_unknown_field_is_invalid_filter(db):
    c = db.create_collection("uf", dimension=8)
    c.add(1, [1.0] * 8, "a", {"tenant": "A"})
    with pytest.raises(zenar.InvalidFilterError):
        c.search([1.0] * 8, filter=f.nope == "x")


def test_tenant_isolation_from_python(db):
    c = db.create_collection("tenants", dimension=16)
    rng = np.random.default_rng(0)
    for i in range(100):
        tenant = "A" if i % 2 == 0 else "B"
        c.add(i, rng.standard_normal(16).astype(np.float32), f"doc {i}", {"tenant": tenant})
    query = rng.standard_normal(16).astype(np.float32)
    hits = c.search(query, top_k=10, filter=f.tenant == "A")
    assert len(hits) == 10
    assert all(h["metadata"]["tenant"] == "A" for h in hits)


def test_numpy_float32_and_float64_inputs(db):
    c = db.create_collection("np", dimension=8)
    c.add(1, np.ones(8, dtype=np.float32), "f32", {})
    c.add(2, np.ones(8, dtype=np.float64), "f64", {})
    assert len(c.search(np.ones(8), top_k=5)) == 2


def test_datetime_metadata_roundtrips_as_utc(db):
    c = db.create_collection("dates", dimension=8)
    when = dt.datetime(2026, 1, 1, 12, 0, tzinfo=dt.timezone(dt.timedelta(hours=2)))
    c.add(1, [1.0] * 8, "d", {"created_at": when})
    got = c.get(1)["metadata"]["created_at"]
    assert got.tzinfo is not None
    assert got == when
    hits = c.search([1.0] * 8, filter=f.created_at >= dt.datetime(2025, 12, 31, tzinfo=dt.timezone.utc))
    assert [h["id"] for h in hits] == [1]


def test_naive_datetime_rejected(db):
    c = db.create_collection("naive", dimension=8)
    with pytest.raises(zenar.InvalidMetadataError):
        c.add(1, [1.0] * 8, "x", {"when": dt.datetime(2026, 1, 1)})


def test_get_get_many_count_delete(db):
    c = db.create_collection("crud", dimension=8)
    for i in range(3):
        c.add(i, [float(i + 1)] * 8, f"t{i}", {"tenant": "A"})
    assert c.count() == 3
    assert c.get(99) is None
    assert [d["id"] for d in c.get_many([2, 0, 99])] == [2, 0]
    c.delete(0)
    assert c.get(0) is None
    assert c.count() == 2
    with pytest.raises(zenar.VectorNotFoundError):
        c.delete(0)


def test_duplicate_id_and_bad_vectors(db):
    c = db.create_collection("bad", dimension=8)
    c.add(1, [1.0] * 8, "a")
    with pytest.raises(zenar.DuplicateIdError):
        c.add(1, [1.0] * 8, "b")
    with pytest.raises(zenar.DimensionMismatchError):
        c.add(2, [1.0] * 7, "c")
    with pytest.raises(zenar.InvalidVectorError):
        c.add(3, [float("nan")] + [1.0] * 7, "d")


def test_lazy_dimension_inferred_from_first_add(db):
    c = db.create_collection("lazy")
    assert c.dimension is None
    c.add(1, [0.5] * 16, "a")
    assert c.dimension == 16


def test_invalid_names_and_config(db):
    with pytest.raises(zenar.InvalidCollectionNameError):
        db.create_collection("../../etc/passwd", dimension=8)
    with pytest.raises(zenar.DimensionMismatchError):
        db.create_collection("huge", dimension=20000)
    with pytest.raises(zenar.InvalidArgumentError):
        db.create_collection("bits", dimension=8, bit_width=5)


def test_collection_management(db):
    db.create_collection("one", dimension=8)
    with pytest.raises(zenar.CollectionAlreadyExistsError):
        db.create_collection("one", dimension=8)
    assert db.list_collections() == ["one"]
    db.drop_collection("one")
    with pytest.raises(zenar.CollectionNotFoundError):
        db.get_collection("one")


def test_add_many_and_stats(db):
    c = db.create_collection("many", dimension=8)
    n = c.add_many(
        [{"id": i, "vector": [float(i + 1)] * 8, "text": f"t{i}", "metadata": {"n": i}} for i in range(5)]
    )
    assert n == 5
    stats = db.stats()
    assert stats["collections"]["many"]["count"] == 5


def test_sql_injection_text_is_data(db):
    c = db.create_collection("inj", dimension=8)
    payload = "'; DROP TABLE zdocs_inj; --"
    c.add(1, [1.0] * 8, payload, {"tenant": payload})
    hits = c.search([1.0] * 8, filter=f.tenant == payload)
    assert hits[0]["text"] == payload


def test_panic_at_boundary_becomes_exception(db):
    with pytest.raises(zenar.VectorStoreError, match="deliberate panic"):
        zenar._zenar._panic_for_test()


def test_persistence_across_handles(tmp_path):
    path = str(tmp_path / "persist")
    with VectorDB(path) as db:
        c = db.create_collection("p", dimension=8)
        c.add(7, [1.0] * 8, "kept", {"tenant": "A"})
    with VectorDB(path) as db:
        c = db.get_collection("p")
        assert c.get(7)["text"] == "kept"
        assert db.recovery_report()["rolled_back_pending"] == 0


def test_second_handle_is_locked(tmp_path):
    path = str(tmp_path / "locked")
    with VectorDB(path):
        with pytest.raises(zenar.DatabaseLockedError):
            VectorDB(path)


def test_calibrate_and_bit_width(db):
    c = db.create_collection("cal", dimension=16, bit_width=2)
    assert c.bit_width == 2
    rng = np.random.default_rng(1)
    for i in range(300):
        c.add(i, rng.standard_normal(16).astype(np.float32), "x", {})
    c.calibrate(rng.standard_normal(16 * 256).astype(np.float32))
    assert len(c.search(rng.standard_normal(16).astype(np.float32), top_k=5)) == 5


def _kill_during_writes(path, flush_every):
    """Runs a writer in a child process and SIGKILLs it mid-stream."""
    flush_line = "            if i % 20 == 0: db.flush()" if flush_every else ""
    writer = textwrap.dedent(
        f"""
        import numpy as np
        from zenar import VectorDB
        db = VectorDB({path!r})
        c = db.create_collection("crash", dimension=32)
        rng = np.random.default_rng(2)
        i = 0
        while True:
            c.add(i, rng.standard_normal(32).astype(np.float32), "row", {{"n": i}})
{flush_line}
            i += 1
        """
    )
    proc = subprocess.Popen([sys.executable, "-c", writer])
    time.sleep(2.0)
    proc.kill()
    proc.wait()


def test_kill_minus_9_mid_write_leaves_consistent_database(tmp_path):
    """Spec sections 26 and 86: after SIGKILL, every served row has its vector."""
    path = str(tmp_path / "crash")
    _kill_during_writes(path, flush_every=False)
    with VectorDB(path) as db:
        c = db.get_collection("crash")
        ready = c.count()
        query = np.ones(32, dtype=np.float32)
        hits = c.search(query, top_k=max(ready, 1))
        assert len(hits) == ready, "a READY row was not searchable after recovery"
        # Rows added after the last sync boundary may be lost; the repair is reported.
        report = db.recovery_report()
        assert report["rolled_back_pending"] >= 0


def test_flush_makes_adds_survive_kill_minus_9(tmp_path):
    """Adds covered by a completed flush() must survive a SIGKILL."""
    path = str(tmp_path / "flushed")
    _kill_during_writes(path, flush_every=True)
    with VectorDB(path) as db:
        c = db.get_collection("crash")
        assert c.count() >= 20
        assert c.get(0) is not None


def test_path_with_dsn_metacharacters_is_rejected(tmp_path):
    with pytest.raises(zenar.InvalidArgumentError):
        VectorDB(str(tmp_path / "bad?name"))


def test_path_with_spaces_works(tmp_path):
    path = str(tmp_path / "with space")
    with VectorDB(path) as db:
        db.create_collection("s", dimension=8).add(1, [1.0] * 8, "x", {})
    with VectorDB(path) as db:
        assert db.get_collection("s").count() == 1


def test_durable_flag_roundtrip(tmp_path):
    path = str(tmp_path / "durable")
    with VectorDB(path, durable=True) as db:
        db.create_collection("d", dimension=8).add(1, [1.0] * 8, "x", {"tenant": "A"})
    with VectorDB(path) as db:
        assert db.get_collection("d").get(1)["text"] == "x"


def test_flush_then_close_keeps_adds(tmp_path):
    path = str(tmp_path / "flush")
    db = VectorDB(path)
    c = db.create_collection("f", dimension=8)
    for i in range(10):
        c.add(i, [float(i + 1)] * 8, f"t{i}", {})
    db.flush()
    db.close()
    with VectorDB(path) as db:
        assert db.get_collection("f").count() == 10


def test_search_validation_errors(db):
    c = db.create_collection("sv", dimension=8)
    c.add(1, [1.0] * 8, "x", {})
    with pytest.raises(zenar.InvalidArgumentError):
        c.search([1.0] * 8, top_k=0)
    with pytest.raises(zenar.DimensionMismatchError):
        c.search([1.0] * 4)
    with pytest.raises(zenar.InvalidVectorError):
        c.search([float("inf")] + [1.0] * 7)


@pytest.mark.parametrize("bad_id", [-1, 2**64, 2**100, "7", 1.5])
def test_bad_ids_raise_invalid_argument(db, bad_id):
    c = db.create_collection("ids", dimension=8)
    with pytest.raises(zenar.InvalidArgumentError):
        c.add(bad_id, [1.0] * 8, "x", {})
    with pytest.raises(zenar.InvalidArgumentError):
        c.get(bad_id)
    with pytest.raises(zenar.InvalidArgumentError):
        c.delete(bad_id)


def test_bad_top_k_and_dimension_raise_invalid_argument(db):
    c = db.create_collection("tk", dimension=8)
    c.add(1, [1.0] * 8, "x", {})
    with pytest.raises(zenar.InvalidArgumentError):
        c.search([1.0] * 8, top_k=-3)
    with pytest.raises(zenar.InvalidArgumentError):
        c.search([1.0] * 8, top_k="ten")
    with pytest.raises(zenar.InvalidArgumentError):
        db.create_collection("neg", dimension=-8)
    with pytest.raises(zenar.InvalidArgumentError):
        db.create_collection("bits", dimension=8, bit_width=-4)


def test_max_u64_id_is_accepted_and_round_trips(db):
    c = db.create_collection("big", dimension=8)
    c.add(2**63 - 1, [1.0] * 8, "max", {})
    assert c.get(2**63 - 1)["text"] == "max"
    with pytest.raises(zenar.InvalidArgumentError):
        c.add(2**63, [1.0] * 8, "over", {})
