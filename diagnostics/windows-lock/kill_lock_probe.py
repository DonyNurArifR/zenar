#!/usr/bin/env python3
"""Temporary diagnostic for the intermittent Windows DatabaseLockedError in tests/test_compaction_crash.py.

It repeats the test's scenario: a writer child process runs past its deletes, is killed at a random point, and the
database is reopened. For each trial it writes one JSON line with the child's PID and exit code, the process status
after the kill, the kill and wait timestamps, every open attempt with its time and outcome, the time until the LOCK
file became available, and the same data-integrity checks the test makes.

Only the harness retries the open, and only to measure how long the lock takes to release. The first attempt is
recorded separately, so the result is the same as the test's first open. A lock that never becomes available within
the bound is recorded as such. Nothing here changes product behaviour, and no test imports this file.

Usage: python kill_lock_probe.py --trials 60 --out lock-probe.jsonl
"""

import argparse
import datetime
import json
import os
import random
import shutil
import subprocess
import sys
import tempfile
import time

DIM = 16
DELETED = 6000
SEED_ROWS = 12000
ATTEMPT_LOG_HEAD = 50   # attempts recorded from the start of a trial
ATTEMPT_LOG_TAIL = 10   # and from the end, so a long wait is still visible
HANDLE_SNAPSHOTS = 5    # handle-owner snapshots for the first trials whose first open failed
CHILD_TIMEOUT_S = 120

# The writer from tests/test_compaction_crash.py, unchanged except for the placeholders.
WRITER = """
import numpy as np
from zenar import VectorDB
db = VectorDB(__PATH__)
c = db.create_collection("cp", dimension=__DIM__)
rng = np.random.default_rng(7)
def batch(first, n):
    return [(first + i, rng.standard_normal(__DIM__).astype(np.float32), "row") for i in range(n)]
first = 0
while first < __SEED__:
    for i, v, t in batch(first, 1000):
        c.add(i, v, t)
    first += 1000
db.flush()
print("flushed", first, flush=True)
c.delete_many(list(range(__DELETED__)))
print("deleted", flush=True)
i = __SEED__
while True:
    for j, v, t in batch(i, 200):
        c.add(j, v, t)
    i += 200
    db.flush()
    print("flushed", i, flush=True)
"""


def writer_code(path):
    return (WRITER.replace("__PATH__", repr(path)).replace("__DIM__", str(DIM))
            .replace("__SEED__", str(SEED_ROWS)).replace("__DELETED__", str(DELETED)))


def stamp():
    return {"mono": time.monotonic(), "wall": datetime.datetime.now(datetime.timezone.utc).isoformat()}


def process_status(pid):
    """Whether the process is still running. On Windows it asks tasklist; elsewhere it sends signal 0."""
    if sys.platform == "win32":
        out = subprocess.run(["tasklist", "/FI", f"PID eq {pid}", "/FO", "CSV", "/NH"],
                             capture_output=True, text=True, timeout=60).stdout.strip()
        return {"alive": f'"{pid}"' in out, "tasklist": out[:300]}
    try:
        os.kill(pid, 0)
        return {"alive": True, "tasklist": None}
    except ProcessLookupError:
        return {"alive": False, "tasklist": None}


def stderr_tail(path, limit=1500):
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            return f.read()[-limit:]
    except OSError:
        return None


def handle_owner(lock_path):
    """Which process holds the LOCK file, if the Sysinternals handle tool is on PATH. Recorded, never required."""
    tool = shutil.which("handle64.exe") or shutil.which("handle.exe") or shutil.which("handle")
    if not tool:
        return {"tool": None}
    out = subprocess.run([tool, "-accepteula", "-nobanner", lock_path],
                         capture_output=True, text=True, timeout=120).stdout
    return {"tool": os.path.basename(tool), "output": out[:2000]}


def lock_file_info(path):
    lock = os.path.join(path, "LOCK")
    if not os.path.exists(lock):
        return {"exists": False}
    return {"exists": True, "size": os.path.getsize(lock)}


def try_open(path):
    """One attempt to open the database. Returns (db or None, outcome record)."""
    from zenar import VectorDB
    t0 = stamp()
    try:
        db = VectorDB(path)
        return db, {"at": t0, "outcome": "open"}
    except Exception as e:  # the outcome is the data here; nothing is hidden
        return None, {"at": t0, "outcome": type(e).__name__, "message": str(e)[:300]}


def integrity_checks(db, path, flushed):
    """The data-integrity assertions of tests/test_compaction_crash.py, recorded instead of raised.

    The database is closed here, as the test closes it, and then opened again for the second-open check.
    """
    import numpy as np
    from zenar import VectorDB
    checks = {}
    try:
        c = db.get_collection("cp")
        count = c.count()
        checks["count"] = count
        checks["count_ge_flushed_minus_deleted"] = count >= flushed - DELETED
        checks["count_le_flushed_plus_200"] = count <= flushed + 200
        hits = c.search(np.ones(DIM, dtype=np.float32), top_k=count)
        ids = {h["id"] for h in hits}
        checks["no_row_served_twice"] = len(ids) == count
        checks["deleted_rows_absent"] = ids.isdisjoint(range(DELETED))
        checks["flushed_rows_present"] = set(range(DELETED, flushed)) <= ids
        db.close()
        db2 = VectorDB(path)
        checks["second_open_count_equal"] = db2.get_collection("cp").count() == count
        db2.close()
    except Exception as e:
        checks["error"] = f"{type(e).__name__}: {str(e)[:300]}"
    booleans = [v for k, v in checks.items() if isinstance(v, bool)]
    checks["all_passed"] = "error" not in checks and all(booleans)
    return checks


rec_counter = {"snapshots": 0}


def run_trial(n, work, bound_s):
    from zenar import VectorDB  # noqa: F401  (import errors surface here, not per trial)
    path = os.path.join(work, f"trial-{n:03d}")
    stderr_log = os.path.join(work, f"writer-{n:03d}.stderr")
    rec = {"type": "trial", "trial": n, "path": path, "platform": sys.platform}
    rec["spawn"] = stamp()
    with open(stderr_log, "w", encoding="utf-8") as err:
        proc = subprocess.Popen([sys.executable, "-c", writer_code(path)], stdout=subprocess.PIPE,
                                stderr=err, text=True)
    rec["child_pid"] = proc.pid
    flushed = 0
    deleted = False
    deadline = time.time() + CHILD_TIMEOUT_S
    while time.time() < deadline:
        line = proc.stdout.readline()
        if not line:
            break
        if line.startswith("flushed"):
            flushed = int(line.split()[1])
        if line.startswith("deleted"):
            deleted = True
            break
    rec["reached_deletes"] = deleted
    rec["flushed_before_deletes_seen"] = flushed
    if not deleted:
        proc.kill()
        proc.wait()
        rec["error"] = "writer did not reach its deletes"
        return rec
    # Same random delay as the test, so the kill lands at an unscheduled point.
    time.sleep(random.uniform(0.0, 0.03))
    rec["kill_called"] = stamp()
    proc.kill()
    rec["kill_returned"] = stamp()
    rc = proc.wait()
    rec["wait_returned"] = stamp()
    rec["child_exit_code"] = rc
    rec["child_status_after_wait"] = process_status(proc.pid)
    rec["lock_file_after_wait"] = lock_file_info(path)

    base = rec["wait_returned"]["mono"]
    first_db, first = try_open(path)
    first["since_wait_ms"] = round((first["at"]["mono"] - base) * 1000, 3)
    rec["first_open_attempt"] = first
    attempts = [first]
    db = first_db
    bound_end = base + bound_s
    while db is None and time.monotonic() < bound_end:
        time.sleep(0.005)
        db, a = try_open(path)
        a["since_wait_ms"] = round((a["at"]["mono"] - base) * 1000, 3)
        attempts.append(a)
    rec["attempt_count"] = len(attempts)
    rec["child_stderr_tail"] = stderr_tail(stderr_log)
    if not first["outcome"] == "open" and rec_counter["snapshots"] < HANDLE_SNAPSHOTS:
        rec_counter["snapshots"] += 1
        rec["handle_owner_at_first_failure"] = handle_owner(os.path.join(path, "LOCK"))
    rec["attempts_head"] = attempts[:ATTEMPT_LOG_HEAD]
    rec["attempts_tail"] = attempts[-ATTEMPT_LOG_TAIL:]
    rec["first_attempt_opened"] = first["outcome"] == "open"

    if db is None:
        rec["lock_available"] = False
        rec["outcome"] = f"lock not available within {bound_s:g} s"
        rec["handle_owner"] = handle_owner(os.path.join(path, "LOCK")) if os.path.exists(path) else None
        rec["child_status_at_bound"] = process_status(proc.pid)
        return rec

    available = attempts[-1]["at"]["mono"]
    rec["lock_available"] = True
    rec["time_to_lock_available_ms"] = round((available - base) * 1000, 3)
    rec["integrity"] = integrity_checks(db, path, flushed)
    rec["outcome"] = ("open with all integrity checks passed" if rec["integrity"]["all_passed"]
                      else "open with an integrity check failing")
    return rec


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--trials", type=int, default=60)
    ap.add_argument("--out", required=True)
    ap.add_argument("--bound-seconds", type=float, default=30.0)
    args = ap.parse_args()
    work = tempfile.mkdtemp(prefix="lock-probe-")
    started = stamp()
    with open(args.out, "w", encoding="utf-8") as out:
        out.write(json.dumps({"type": "run", "started": started, "python": sys.version, "platform": sys.platform,
                              "trials": args.trials, "bound_seconds": args.bound_seconds}) + "\n")
        for n in range(args.trials):
            try:
                rec = run_trial(n, work, args.bound_seconds)
            except Exception as e:
                rec = {"type": "trial", "trial": n, "harness_error": f"{type(e).__name__}: {str(e)[:300]}"}
            out.write(json.dumps(rec, default=str) + "\n")
            out.flush()
            print(json.dumps({k: rec.get(k) for k in ("trial", "outcome", "child_exit_code", "time_to_lock_available_ms",
                                                      "first_attempt_opened", "error", "harness_error")}, default=str), flush=True)
        trials = []
        with open(args.out, encoding="utf-8") as f:
            trials = [json.loads(l) for l in f if json.loads(l).get("type") == "trial"]
        summary = {
            "type": "summary",
            "trials": len(trials),
            "first_attempt_opened": sum(1 for t in trials if t.get("first_attempt_opened")),
            "opened_after_retry": sum(1 for t in trials if t.get("lock_available") and not t.get("first_attempt_opened")),
            "lock_not_available_within_bound": sum(1 for t in trials if t.get("lock_available") is False),
            "integrity_failures": sum(1 for t in trials if t.get("lock_available") and not t["integrity"]["all_passed"]),
            "harness_errors": sum(1 for t in trials if "harness_error" in t or "error" in t),
            "child_exit_codes": sorted({t.get("child_exit_code") for t in trials if "child_exit_code" in t}, key=str),
            "child_alive_after_wait": sum(1 for t in trials if t.get("child_status_after_wait", {}).get("alive")),
        }
        times = sorted(t["time_to_lock_available_ms"] for t in trials if t.get("lock_available"))
        if times:
            summary["time_to_lock_available_ms"] = {"min": times[0], "median": times[len(times) // 2], "max": times[-1]}
        out.write(json.dumps(summary, default=str) + "\n")
    print(json.dumps(summary, default=str), flush=True)
    shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    main()
