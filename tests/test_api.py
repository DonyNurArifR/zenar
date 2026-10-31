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
    with pytest.raises(zenar.InvalidArgumentError):
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
        assert db.recovery_report()["orphaned_rows_removed"] == 0


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
    with open_after_crash(path) as db:
        c = db.get_collection("crash")
        ready = c.count()
        query = np.ones(32, dtype=np.float32)
        hits = c.search(query, top_k=max(ready, 1))
        assert len(hits) == ready, "a READY row was not searchable after recovery"
        # Rows added after the last sync boundary may be lost; the repair is reported.
        report = db.recovery_report()
        assert report["orphaned_rows_removed"] >= 0


def test_flush_makes_adds_survive_kill_minus_9(tmp_path):
    """Adds covered by a completed flush() must survive a SIGKILL."""
    path = str(tmp_path / "flushed")
    _kill_during_writes(path, flush_every=True)
    with open_after_crash(path) as db:
        c = db.get_collection("crash")
        assert c.count() >= 20
        assert c.get(0) is not None


@pytest.mark.skipif(sys.platform == "win32", reason="Windows does not allow '?' in file names")
def test_path_with_url_metacharacters_works(tmp_path):
    path = str(tmp_path / "odd?#&name")
    with VectorDB(path) as db:
        db.create_collection("m", dimension=8).add(1, [1.0] * 8, "x", {})
    with VectorDB(path) as db:
        assert db.get_collection("m").count() == 1


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


def test_add_many_is_atomic(db):
    c = db.create_collection("atomic", dimension=8)
    c.add(1, [1.0] * 8, "existing", {})
    good = [{"id": i, "vector": [float(i + 1)] * 8, "text": f"t{i}", "metadata": {}} for i in range(2, 30)]
    bad = good + [{"id": 40, "vector": [1.0] * 7, "text": "wrong dim", "metadata": {}}]
    with pytest.raises(zenar.DimensionMismatchError):
        c.add_many(bad)
    assert c.count() == 1
    dup = good + [{"id": 1, "vector": [1.0] * 8, "text": "dup", "metadata": {}}]
    with pytest.raises(zenar.DuplicateIdError):
        c.add_many(dup)
    assert c.count() == 1


def test_add_many_returns_count_and_is_searchable(db):
    c = db.create_collection("many2", dimension=8)
    n = c.add_many([{"id": i, "vector": [float(i + 1)] * 8, "text": f"t{i}", "metadata": {"tenant": "A"}} for i in range(300)])
    assert n == 300
    assert c.count() == 300
    hit = c.search([1.0] * 8, top_k=1, filter=f.tenant == "A")[0]
    assert hit["text"] == f"t{hit['id']}"  # all vectors share a direction, so the top hit is any of them


def test_add_many_accepts_numpy_vectors(db):
    c = db.create_collection("npmany", dimension=8)
    rows = [{"id": i, "vector": np.full(8, i + 1, dtype=np.float32), "text": "x"} for i in range(5)]
    assert c.add_many(rows) == 5


def test_delete_many_is_atomic(db):
    c = db.create_collection("dm", dimension=8)
    c.add_many([{"id": i, "vector": [float(i + 1)] * 8, "text": f"t{i}"} for i in range(10)])
    with pytest.raises(zenar.VectorNotFoundError):
        c.delete_many([1, 2, 77])
    assert c.count() == 10
    assert c.delete_many([1, 2, 3]) == 3
    assert c.count() == 7
    assert c.get(2) is None


def open_after_crash(path, timeout=20.0):
    """Opens a database whose writer was just killed. The lock must clear once the process is gone.
    On Windows that can take a moment after `wait()` returns, so retry for a bounded time."""
    deadline = time.monotonic() + timeout
    started = time.monotonic()
    while True:
        try:
            db = VectorDB(path)
            waited = time.monotonic() - started
            if waited > 1.0:
                print(f"lock cleared after {waited:.2f}s")
            return db
        except zenar.DatabaseLockedError:
            if time.monotonic() > deadline:
                raise
            time.sleep(0.1)


def test_repeated_kill_minus_9_cycles_stay_consistent(tmp_path):
    """Several crash-and-restart cycles on one database. After each restart, the live count
    must match the searchable documents, and flushed batches must still be there."""
    path = str(tmp_path / "cycles")
    flushed_total = 0
    for cycle in range(3):
        writer = textwrap.dedent(
            f"""
            import numpy as np
            from zenar import VectorDB
            db = VectorDB({path!r})
            c = db.get_collection("cy") if "cy" in db.list_collections() else db.create_collection("cy", dimension=32)
            rng = np.random.default_rng({cycle})
            base = c.count()
            batch = 0
            while True:
                rows = [{{"id": base + batch * 50 + j + {cycle} * 1000000, "vector": rng.standard_normal(32).astype(np.float32), "text": "row", "metadata": {{"cycle": {cycle}}}}} for j in range(50)]
                c.add_many(rows)
                db.flush()
                batch += 1
            """
        )
        proc = subprocess.Popen([sys.executable, "-c", writer])
        time.sleep(1.5)
        proc.kill()
        proc.wait()

        with open_after_crash(path) as db:
            c = db.get_collection("cy")
            live = c.count()
            assert live >= flushed_total, "a flushed batch disappeared"
            hits = c.search(np.ones(32, dtype=np.float32), top_k=max(live, 1))
            assert len(hits) == live, f"cycle {cycle}: {live} live but {len(hits)} searchable"
            flushed_total = live


def _shop(db):
    c = db.create_collection("shop", dimension=8)
    c.add_many(
        [
            {"id": 1, "vector": [1.0] * 8, "text": "wrench", "metadata": {"tenant": "A", "category": "tools", "price": 12.5}},
            {"id": 2, "vector": [1.0] * 8, "text": "kettle", "metadata": {"tenant": "A", "category": "appliances", "price": 40.0}},
            {"id": 3, "vector": [1.0] * 8, "text": "radio", "metadata": {"tenant": "B", "category": "electronics", "price": 25.0}},
        ]
    )
    return c


def test_describe_lists_fields_types_values_and_counts(db):
    c = _shop(db)
    info = c.describe(scope=None)
    assert info["documents"] == 3
    fields = {f["name"]: f for f in info["fields"]}
    assert fields["tenant"]["type"] == "text"
    assert {v["value"]: v["count"] for v in fields["tenant"]["values"]} == {"A": 2, "B": 1}
    assert fields["price"]["min"] == 12.5 and fields["price"]["max"] == 40.0


def test_describe_is_updated_by_writes(db):
    c = _shop(db)
    assert {"value": "C", "count": 1} not in c.describe(scope=None)["fields"][0]["values"]
    c.add(4, [1.0] * 8, "pen", {"tenant": "C", "category": "office", "price": 2.0})
    tenant = next(f for f in c.describe(scope=None)["fields"] if f["name"] == "tenant")
    assert {"value": "C", "count": 1} in tenant["values"]


def test_retrieve_with_where_operators_and_counts(db):
    c = _shop(db)
    r = c.retrieve([1.0] * 8, top_k=1, where={"tenant": "A", "price": {"gte": 10, "lt": 30}}, scope=None)
    assert r["matched"] == 1
    assert [h["text"] for h in r["hits"]] == ["wrench"]
    r = c.retrieve([1.0] * 8, top_k=10, where={"or": [{"category": "tools"}, {"category": "electronics"}]}, scope=None)
    assert r["matched"] == 2
    r = c.retrieve([1.0] * 8, top_k=10, where={"not": {"tenant": "A"}, "category": {"in": ["electronics", "tools"]}}, scope=None)
    assert r["matched"] == 1 and r["hits"][0]["text"] == "radio"
    assert r["warnings"] == []


def test_retrieve_decodes_a_where_sent_as_a_json_string(db):
    c = _shop(db)
    as_object = c.retrieve([1.0] * 8, top_k=10, where={"tenant": "A", "price": {"gte": 10}}, scope=None)
    as_string = c.retrieve([1.0] * 8, top_k=10, where='{"tenant": "A", "price": {"gte": 10}}', scope=None)
    assert as_string["hits"] == as_object["hits"]
    assert as_string["matched"] == as_object["matched"]
    with pytest.raises(zenar.InvalidFilterError):
        c.retrieve([1.0] * 8, top_k=10, where='"tenant"', scope=None)


def test_retrieve_warns_when_a_value_matches_nothing(db):
    c = _shop(db)
    r = c.retrieve([1.0] * 8, top_k=5, where={"tenant": "Z"}, scope=None)
    assert r["matched"] == 0 and r["hits"] == []
    assert len(r["warnings"]) == 1
    assert '"A"' in r["warnings"][0] and '"B"' in r["warnings"][0]


def test_retrieve_suggests_the_field_name_for_a_typo(db):
    c = _shop(db)
    with pytest.raises(zenar.InvalidFilterError, match='did you mean "category"'):
        c.retrieve([1.0] * 8, where={"catgory": "tools"}, scope=None)


def test_retrieve_rejects_wrong_types_with_the_expected_type(db):
    c = _shop(db)
    with pytest.raises(zenar.InvalidFilterError, match="expected a number"):
        c.retrieve([1.0] * 8, where={"price": "cheap"}, scope=None)
    with pytest.raises(zenar.InvalidFilterError, match="JSON-compatible"):
        c.retrieve([1.0] * 8, where={"tenant": object()}, scope=None)


def test_retrieve_without_where_matches_everything(db):
    c = _shop(db)
    assert c.retrieve([1.0] * 8, top_k=10, scope=None)["matched"] == 3
    assert c.retrieve([1.0] * 8, top_k=10, where={}, scope=None)["matched"] == 3


def test_find_values_and_tool_spec(db):
    c = _shop(db)
    assert [v["value"] for v in c.find_values("category", "TOOL", scope=None)] == ["tools"]
    spec = c.tool_spec("search_shop", scope=None)
    assert spec["name"] == "search_shop"
    assert "category (text)" in spec["description"]
    assert set(spec["parameters"]["required"]) == {"query"}
    where = spec["parameters"]["properties"]["where"]
    assert set(where["properties"]) >= {"tenant", "category", "price", "and", "or", "not"}
    assert set(where["properties"]["tenant"]["anyOf"][0]["enum"]) == {"A", "B"}
    assert spec["input_schema"] == spec["parameters"]


def test_tool_spec_prompt_only_lists_fields_and_values(db):
    c = _shop(db)
    text = c.tool_spec("search_shop", prompt_only=True, scope=None)
    assert isinstance(text, str)
    assert "- tenant (text): " in text and '"A"' in text
    assert "- price (float): " in text
    assert "description" not in text and "parameters" not in text


def test_tool_spec_is_a_dict_that_creates_a_prompt(db):
    from zenar.agent import ToolSpec

    c = _shop(db)
    spec = c.tool_spec("search_shop", scope=None)
    assert isinstance(spec, ToolSpec) and isinstance(spec, dict)
    assert spec["name"] == "search_shop"
    prompt = spec.create_prompt(instructions="You are a shop assistant.")
    assert "- tenant (text): values \"A\", \"B\"" in prompt
    assert "Filterable fields:" in prompt
    assert "Call this tool with `query`" in prompt
    assert prompt.endswith("You are a shop assistant.")
    assert "You are a shop assistant." not in spec.create_prompt()



def test_range_bounds_are_not_restricted_to_existing_values(db):
    c = _shop(db)
    where = c.tool_spec("search_shop", scope=None)["parameters"]["properties"]["where"]
    price = where["properties"]["price"]["anyOf"][1]["properties"]
    assert "enum" not in price["gte"] and "enum" not in price["lt"], "a range bound may be any number"
    tenant = where["properties"]["tenant"]["anyOf"][1]["properties"]
    assert set(tenant["eq"]["enum"]) == {"A", "B"}, "equality should steer to values that exist"


def _two_tenants(db):
    c = db.create_collection("tenanted", dimension=8)
    rows = []
    for i in range(12):
        tenant = "acme" if i % 3 == 0 else "globex"
        rows.append({"id": i, "vector": [float(i + 1)] * 8, "text": f"item {i}",
                     "metadata": {"tenant": tenant, "category": "tools" if i % 2 else "garden"}})
    c.add_many(rows)
    return c


def test_scope_is_required_on_every_agent_call(db):
    c = _two_tenants(db)
    with pytest.raises(TypeError):
        c.describe()
    with pytest.raises(TypeError):
        c.retrieve([1.0] * 8)
    with pytest.raises(TypeError):
        c.tool_spec("search")
    with pytest.raises(TypeError):
        c.find_values("tenant", "a")
    # Passing None is an explicit, visible choice.
    assert c.describe(scope=None)["documents"] == 12


def test_model_cannot_reach_another_tenant_through_where(db):
    c = _two_tenants(db)
    scope = {"tenant": "acme"}
    # Asking for the other tenant directly.
    assert c.retrieve([1.0] * 8, 50, {"tenant": "globex"}, scope=scope)["matched"] == 0
    # Widening with "or".
    widened = {"or": [{"tenant": "acme"}, {"tenant": "globex"}]}
    assert c.retrieve([1.0] * 8, 50, widened, scope=scope)["matched"] == 4
    # Excluding the scope with "not".
    assert c.retrieve([1.0] * 8, 50, {"not": {"tenant": "acme"}}, scope=scope)["matched"] == 0
    # Combining scope with the model's own conditions still works.
    assert c.retrieve([1.0] * 8, 50, {"category": "tools"}, scope=scope)["matched"] == 2


def test_discovery_inside_a_scope_mentions_no_other_tenant(db):
    c = _two_tenants(db)
    scope = {"tenant": "acme"}
    info = c.describe(scope=scope)
    assert info["documents"] == 4
    assert "globex" not in str(info)
    assert c.find_values("tenant", "glob", scope=scope) == []
    spec = c.tool_spec("search", scope=scope)
    assert "globex" not in str(spec)
    assert "globex" not in c.tool_spec("search", prompt_only=True, scope=scope)


def test_scope_with_an_unknown_field_is_an_error_not_an_empty_scope(db):
    c = _two_tenants(db)
    with pytest.raises(zenar.InvalidFilterError, match='did you mean "tenant"'):
        c.describe(scope={"tennant": "acme"})


def test_warnings_inside_a_scope_name_no_other_tenant(db):
    c = _two_tenants(db)
    r = c.retrieve([1.0] * 8, 5, {"tenant": "zzz"}, scope={"tenant": "acme"})
    assert len(r["warnings"]) == 1
    assert '"acme"' in r["warnings"][0]
    assert "globex" not in r["warnings"][0]


def test_retrieve_caps_top_k_for_model_calls(db):
    c = _two_tenants(db)
    with pytest.raises(zenar.InvalidArgumentError, match="limit for retrieve is 1000"):
        c.retrieve([1.0] * 8, top_k=5000, scope=None)


def test_retrieve_reports_the_applied_filter_in_words(db):
    c = _two_tenants(db)
    r = c.retrieve([1.0] * 8, 5, {"category": "tools", "tenant": {"in": ["acme"]}}, scope=None)
    assert r["applied"].startswith("(") and "category = \"tools\"" in r["applied"]
    assert "tenant in [\"acme\"]" in r["applied"]
    assert c.retrieve([1.0] * 8, 5, None, scope=None)["applied"] == "no filter (all documents in scope)"
