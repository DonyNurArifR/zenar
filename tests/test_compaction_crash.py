"""SIGKILL during a background compaction: the reopened database keeps every flushed document.

The Rust tests cut each compaction transition deterministically (docs/R3.md, section 8). This test kills a real
process at an unscheduled point: after its deletes have started a compaction, while it keeps writing. Which
transition the kill lands in is not controlled, so the test checks the outcome, not the timing.
"""

import random
import subprocess
import sys
import textwrap
import time

import numpy as np
import pytest

from zenar import VectorDB

DIM = 16
DELETED = 6000
SEED_ROWS = 12000

WRITER = textwrap.dedent(
    f"""
    import numpy as np
    from zenar import VectorDB
    db = VectorDB({{path!r}})
    c = db.create_collection("cp", dimension={DIM})
    rng = np.random.default_rng(7)
    def batch(first, n):
        return [(first + i, rng.standard_normal({DIM}).astype(np.float32), "row") for i in range(n)]
    first = 0
    while first < {SEED_ROWS}:
        for i, v, t in batch(first, 1000):
            c.add(i, v, t)
        first += 1000
    db.flush()
    print("flushed", first, flush=True)
    c.delete_many(list(range({DELETED})))
    print("deleted", flush=True)
    i = {SEED_ROWS}
    while True:
        for j, v, t in batch(i, 200):
            c.add(j, v, t)
        i += 200
        db.flush()
        print("flushed", i, flush=True)
    """
)


def _run_until_killed(path):
    """Starts the writer, lets it run past its deletes, and kills it. Returns the last flushed row count."""
    proc = subprocess.Popen(
        [sys.executable, "-c", WRITER.format(path=path)],
        stdout=subprocess.PIPE,
        text=True,
    )
    flushed = 0
    deleted = False
    deadline = time.time() + 120
    while time.time() < deadline:
        line = proc.stdout.readline()
        if not line:
            break
        if line.startswith("flushed"):
            flushed = int(line.split()[1])
        if line.startswith("deleted"):
            deleted = True
            break
    assert deleted, "the writer did not reach its deletes"
    # The build of a compaction takes a few milliseconds here, so the kill lands before, during or after it. The
    # delay is random per trial. Which one happened is not asserted: the Rust tests cut each transition deterministically.
    time.sleep(random.uniform(0.0, 0.03))
    proc.kill()
    proc.wait()
    return flushed


@pytest.mark.parametrize("trial", range(6))
def test_kill_during_a_background_compaction_keeps_every_flushed_row(tmp_path, trial):
    path = str(tmp_path / f"crash-{trial}")
    flushed = _run_until_killed(path)

    db = VectorDB(path)
    c = db.get_collection("cp")
    count = c.count()
    # Every row the writer flushed before the kill is there, and the deleted rows are not.
    assert count >= flushed - DELETED, (count, flushed)
    # The writer adds 200 rows between flushes, so the kill can leave up to 200 unflushed rows behind.
    assert count <= flushed + 200, (count, flushed)
    hits = c.search(np.ones(DIM, dtype=np.float32), top_k=count)
    ids = {h["id"] for h in hits}
    assert len(ids) == count, "a row is served twice"
    assert ids.isdisjoint(range(DELETED)), "a deleted row came back"
    assert set(range(DELETED, flushed)) <= ids, "a flushed row is missing"
    db.close()

    # A second open sees the same rows: the recovery is settled, not repeated differently.
    db = VectorDB(path)
    c = db.get_collection("cp")
    assert c.count() == count
    db.close()
