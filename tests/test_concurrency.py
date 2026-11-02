"""Concurrent writers and readers through the Python API. Writers queue and share syncs; these tests
check that every acknowledged write is searchable, that nothing is lost or repeated, and that a
reopen returns the same documents."""

import threading

import zenar

DIM = 4


def vector(i):
    return [float(i % 7) / 7.0 + 0.1, float(i % 5) / 5.0, 0.0, 1.0]


def test_concurrent_writers_all_commit_and_survive_reopen(tmp_path):
    path = str(tmp_path / "db")
    db = zenar.VectorDB(path, durable=True)
    c = db.create_collection("c", dimension=DIM)
    errors = []

    def writer(w):
        try:
            for b in range(15):
                ids = [w * 10_000 + b * 10 + i for i in range(10)]
                if b % 3 == 0:
                    for i in ids:
                        c.add(i, vector(i), f"doc {i}", {"w": w})
                else:
                    records = [
                        {"id": i, "vector": vector(i), "text": f"doc {i}", "metadata": {"w": w}}
                        for i in ids
                    ]
                    assert c.add_many(records) == 10
        except Exception as e:  # collected for the main thread
            errors.append(e)

    threads = [threading.Thread(target=writer, args=(w,)) for w in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors, errors
    expected = 4 * 15 * 10
    assert c.count() == expected

    hits = c.search(vector(0), top_k=expected)
    assert len(hits) == expected
    assert len({h["id"] for h in hits}) == expected
    db.close()

    db = zenar.VectorDB(path)
    c = db.get_collection("c")
    assert c.count() == expected
    assert c.get(3 * 10_000 + 14 * 10 + 9) is not None
    db.close()


def test_search_during_concurrent_writes_returns_committed_results(tmp_path):
    db = zenar.VectorDB(str(tmp_path / "db"))
    c = db.create_collection("c", dimension=DIM)
    c.add_many([
        {"id": i, "vector": vector(i), "text": "seed", "metadata": {}} for i in range(100)
    ])
    stop = threading.Event()
    errors = []

    def writer(w):
        try:
            b = 0
            while not stop.is_set() and b < 40:
                ids = [10_000 * (w + 1) + b * 5 + i for i in range(5)]
                c.add_many([
                    {"id": i, "vector": vector(i), "text": "w", "metadata": {}} for i in ids
                ])
                b += 1
        except Exception as e:
            errors.append(e)

    def reader():
        try:
            for _ in range(200):
                before = c.count()
                hits = c.search(vector(3), top_k=10)
                assert len(hits) >= min(10, before), (len(hits), before)
                assert len({h["id"] for h in hits}) == len(hits)
                for h in hits:
                    assert c.get(h["id"]) is not None
        except Exception as e:
            errors.append(e)

    writers = [threading.Thread(target=writer, args=(w,)) for w in range(3)]
    readers = [threading.Thread(target=reader) for _ in range(3)]
    for t in writers + readers:
        t.start()
    for t in readers:
        t.join()
    stop.set()
    for t in writers:
        t.join()
    assert not errors, errors
    db.close()
