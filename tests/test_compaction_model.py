"""A model of the compaction swap protocol (docs/COMPACTION.md, option C), not of the Rust code.

A compaction snapshots the log length S and the live PUT frames at S, writes those frames to a new
log, and later swaps: the new log is the rewrite followed by every byte appended after S. The model
checks that, after every swap, the new log holds the same live documents, with the same values, as the
old log. It runs random interleavings of puts, deletes, snapshots and swaps.
"""

import random


def live_puts(log):
    """id -> index of the live PUT in `log`, as the document log's replay computes it."""
    live = {}
    for idx, op in enumerate(log):
        if op[0] == "put":
            live[op[1]] = idx
        else:
            live.pop(op[1], None)
    return live


def values(log):
    """id -> the tag of the live PUT: the value a reader would see."""
    return {i: log[idx][2] for i, idx in live_puts(log).items()}


def run_trial(rng, steps):
    log = []
    snapshot = None  # (log length at the snapshot, indices of the live PUTs at the snapshot)
    swaps = 0
    for _ in range(steps):
        r = rng.random()
        if r < 0.45:
            log.append(("put", rng.randint(0, 40), len(log)))
        elif r < 0.7:
            log.append(("del", rng.randint(0, 40)))
        elif snapshot is None:
            snapshot = (len(log), sorted(live_puts(log).values()))
        elif rng.random() < 0.5:
            snap_len, keep = snapshot
            # The rewrite holds the snapshot's live PUTs, in log order. The tail is every byte after the snapshot.
            new_log = [log[i] for i in keep] + log[snap_len:]
            assert values(new_log) == values(log), "the swap changed the live documents"
            log = new_log
            snapshot = None
            swaps += 1
    return swaps


def test_swap_preserves_live_documents_over_random_interleavings():
    rng = random.Random(7)
    swaps = sum(run_trial(rng, rng.randint(5, 300)) for _ in range(5000))
    assert swaps > 10000
