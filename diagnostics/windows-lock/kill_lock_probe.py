#!/usr/bin/env python3
"""Temporary diagnostic for the intermittent Windows DatabaseLockedError in tests/test_compaction_crash.py.

Each trial repeats the test's scenario: a writer child runs past its deletes, is killed at a random point, and the
database is reopened. The ordering matches the test:

    proc.kill()  -> kill returned  -> proc.wait()  -> wait returned  -> VectorDB(path)   (first open)

Between wait returned and the first open there is no subprocess call, no sleep, and no logging. Only a monotonic
clock reading is taken, into memory. External diagnostics (tasklist, the Sysinternals handle tool) run only after a
first open has failed, and the bounded retry polls the open every millisecond until it succeeds or 30 s pass.

Every trial writes one JSON line. A trial counts as ok only when the database opened and every integrity check
passed. A failed open, a persistent lock, a failed integrity check, or a harness exception is recorded as such,
never as a success.

Usage: python kill_lock_probe.py --trials 100 --out lock-probe.jsonl
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

from zenar import VectorDB

DIM = 16
DELETED = 6000
SEED_ROWS = 12000
CHILD_TIMEOUT_S = 120        # the writer's own phase; the test allows the same
RETRY_POLL_S = 0.001         # polling interval for the bounded retry
ATTEMPT_LOG_LIMIT = 30000   # at 1 ms polling, this is the full 30 s bound
HANDLE_SNAPSHOTS = 5         # handle-owner snapshots, for the first trials whose first open failed

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

_ATTEMPT_LOG = []   # filled only by the retry loop, after the first open


def writer_code(path):
    return (WRITER.replace("__PATH__", repr(path)).replace("__DIM__", str(DIM))
            .replace("__SEED__", str(SEED_ROWS)).replace("__DELETED__", str(DELETED)))


def ms(ns):
    return round(ns / 1e6, 3)


def try_open(path):
    """One attempt. Returns (database or None, error type name or None, error message or None)."""
    try:
        return VectorDB(path), None, None
    except Exception as e:
        return None, type(e).__name__, str(e)[:300]


def process_status(pid):
    """External check, run only after a failed first open. On Windows it asks tasklist."""
    if sys.platform == "win32":
        out = subprocess.run(["tasklist", "/FI", f"PID eq {pid}", "/FO", "CSV", "/NH"],
                             capture_output=True, text=True, timeout=60).stdout.strip()
        return {"alive": f'"{pid}"' in out, "tasklist": out[:300]}
    try:
        os.kill(pid, 0)
        return {"alive": True, "tasklist": None}
    except ProcessLookupError:
        return {"alive": False, "tasklist": None}


def handle_tool():
    return os.environ.get("HANDLE_EXE") or shutil.which("handle64.exe") or shutil.which("handle.exe") or shutil.which("handle")


def handle_owner(lock_path):
    """Which processes hold the LOCK file, via the Sysinternals handle tool when it is available."""
    tool = handle_tool()
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


def stderr_tail(path, limit=1500):
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            return f.read()[-limit:]
    except OSError:
        return None


def integrity_checks(db, path, flushed):
    """The data-integrity assertions of tests/test_compaction_crash.py, recorded instead of raised.

    The database is closed here, as the test closes it, and then opened again for the second-open check.
    """
    import numpy as np
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
    booleans = [v for v in checks.values() if isinstance(v, bool)]
    checks["all_passed"] = "error" not in checks and all(booleans)
    return checks


def run_trial(n, work, bound_s, snapshots_taken):
    path = os.path.join(work, f"trial-{n:03d}")
    stderr_log = os.path.join(work, f"writer-{n:03d}.stderr")
    rec = {"type": "trial", "trial": n, "path": path,
           "started_wall": datetime.datetime.now(datetime.timezone.utc).isoformat()}
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
    if not deleted:
        proc.kill()
        proc.wait()
        rec["trial_status"] = "writer_did_not_reach_deletes"
        rec["stderr_tail"] = stderr_tail(stderr_log)
        return rec
    rec["flushed_rows_before_kill_seen"] = flushed

    # Same random delay as the test, so the kill lands at an unscheduled point.
    time.sleep(random.uniform(0.0, 0.03))
    rec["running_before_kill"] = proc.poll() is None
    t_kill_call = time.perf_counter_ns()
    proc.kill()
    t_kill_returned = time.perf_counter_ns()
    rc = proc.wait()
    t_wait_returned = time.perf_counter_ns()
    # ---- Critical window: wait() has returned. Nothing but the first open runs until t_open_start. ----
    t_open_start = time.perf_counter_ns()
    db, err_type, err_msg = try_open(path)
    t_open_end = time.perf_counter_ns()
    # ---- End of critical window. Everything below is recorded from values already in memory. ----
    rec["child_exit_code"] = rc
    rec["kill_call_to_return_ms"] = ms(t_kill_returned - t_kill_call)
    rec["wait_return_after_kill_return_ms"] = ms(t_wait_returned - t_kill_returned)
    rec["wait_return_to_first_open_start_ms"] = ms(t_open_start - t_wait_returned)
    rec["first_open_duration_ms"] = ms(t_open_end - t_open_start)
    rec["first_open_result"] = "open" if db is not None else err_type
    if db is None:
        rec["first_open_message"] = err_msg

    if db is not None:
        rec["first_open_ok"] = True
        rec["time_to_successful_open_ms"] = rec["wait_return_to_first_open_start_ms"]
        rec["retries"] = 0
    else:
        rec["first_open_ok"] = False
        # External diagnostics: only now, after the first open has failed.
        rec["process_status_at_first_failure"] = process_status(proc.pid)
        rec["lock_file_at_first_failure"] = lock_file_info(path)
        if snapshots_taken[0] < HANDLE_SNAPSHOTS:
            snapshots_taken[0] += 1
            rec["handle_owner_at_first_failure"] = handle_owner(os.path.join(path, "LOCK"))
        # Bounded retry, polling every RETRY_POLL_S. Each attempt is recorded in memory with its offset from wait().
        attempts = []
        bound_ns = t_wait_returned + int(bound_s * 1e9)
        while db is None and time.perf_counter_ns() < bound_ns and len(attempts) < ATTEMPT_LOG_LIMIT:
            time.sleep(RETRY_POLL_S)
            t_a = time.perf_counter_ns()
            db, a_type, a_msg = try_open(path)
            attempts.append((ms(t_a - t_wait_returned), "open" if db is not None else a_type))
        rec["retry_attempts"] = len(attempts)
        rec["retry_attempts_log"] = attempts[:200] + attempts[-50:] if len(attempts) > 250 else attempts
        rec["retries"] = len(attempts)
        if db is not None:
            rec["time_to_successful_open_ms"] = attempts[-1][0]
        else:
            rec["time_to_successful_open_ms"] = None
            rec["lock_available"] = False
            rec["process_status_at_bound"] = process_status(proc.pid)
            rec["trial_status"] = f"lock_not_available_within_{bound_s:g}s"
            return rec

    if rec["first_open_ok"]:
        rec["lock_available"] = True
    integrity = integrity_checks(db, path, flushed)
    rec["integrity"] = integrity
    rec["trial_status"] = "ok" if integrity["all_passed"] else "integrity_failed"
    return rec


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--trials", type=int, default=100)
    ap.add_argument("--out", required=True)
    ap.add_argument("--bound-seconds", type=float, default=30.0)
    args = ap.parse_args()
    work = tempfile.mkdtemp(prefix="lock-probe-")
    snapshots_taken = [0]
    with open(args.out, "w", encoding="utf-8") as out:
        header = {"type": "run", "python": sys.version, "platform": sys.platform, "trials": args.trials,
                  "bound_seconds": args.bound_seconds, "retry_poll_seconds": RETRY_POLL_S,
                  "handle_tool": handle_tool()}
        out.write(json.dumps(header) + "\n")
        out.flush()
        for n in range(args.trials):
            try:
                rec = run_trial(n, work, args.bound_seconds, snapshots_taken)
            except Exception as e:
                rec = {"type": "trial", "trial": n, "trial_status": "harness_error",
                       "harness_error": f"{type(e).__name__}: {str(e)[:300]}"}
            out.write(json.dumps(rec, default=str) + "\n")
            out.flush()
            print(json.dumps({k: rec.get(k) for k in ("trial", "trial_status", "first_open_result",
                                                      "child_exit_code", "time_to_successful_open_ms")},
                             default=str), flush=True)
        # Summary from the file, so it reflects what was recorded.
        with open(args.out, encoding="utf-8") as f:
            trials = [j for j in map(json.loads, f) if j.get("type") == "trial"]
        statuses = {}
        for t in trials:
            statuses[t.get("trial_status")] = statuses.get(t.get("trial_status"), 0) + 1
        firsts = sorted(t["wait_return_to_first_open_start_ms"] for t in trials if "wait_return_to_first_open_start_ms" in t)
        times = sorted(t["time_to_successful_open_ms"] for t in trials if t.get("time_to_successful_open_ms") is not None)
        summary = {
            "type": "summary", "trials": len(trials), "trial_status_counts": statuses,
            "first_open_ok": sum(1 for t in trials if t.get("first_open_ok")),
            "retried": sum(1 for t in trials if t.get("retries", 0) > 0),
            "lock_not_available": sum(1 for t in trials if t.get("lock_available") is False),
            "child_exit_codes": {str(k): v for k, v in sorted(
                {c: sum(1 for t in trials if t.get("child_exit_code") == c) for c in {t.get("child_exit_code") for t in trials}}.items(),
                key=lambda kv: str(kv[0]))},
        }
        if firsts:
            summary["wait_to_first_open_ms"] = {"min": firsts[0], "median": firsts[len(firsts) // 2], "max": firsts[-1]}
        if times:
            summary["time_to_successful_open_ms"] = {"min": times[0], "median": times[len(times) // 2], "max": times[-1]}
        out.write(json.dumps(summary, default=str) + "\n")
    print(json.dumps(summary, default=str), flush=True)
    shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    main()
