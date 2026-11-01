# Roadmap

Open work toward a production release, in priority order. Each item says what is known from measurement and what is
not. Items marked done are verified in the code and the test suite; the tests are named in STORAGE.md.

## Done in the consistency review

- Metadata indexes documented against the code, with the alternatives that were measured and the ones that were not
  (ARCHITECTURE.md, "Metadata indexes").
- Cross-file consistency documented: operation order, failure outcomes, reopen guarantees (STORAGE.md,
  "Consistency across the two files").
- Failure paths tested with real index-write failures, not only by setting the poison flag.
- Measured the write lock: which operations hold it, for how long, and which of its steps is I/O (BENCHMARKS.md,
  "Concurrency and write cost"; docs/CONCURRENCY.md). Added the `concurrent_bench`, `write_breakdown`, `write_cost` and
  `delta_scan` examples.
- Phase 1 of the concurrency proposal: log sync moved outside the collection lock, behind a per-collection writer gate.
  Continuous-writer search throughput with 8 readers rose from about 700 to about 6,200 per second (median of three
  runs). p99 fell from about 30 ms to about 6 ms. Search returns `top_k` committed results when that many exist (over-fetch for uncommitted
  vectors). Regression tests cover window searches, injected log-sync failures, and concurrent inserts and searches
  (docs/CONCURRENCY.md, BENCHMARKS.md).
- Audit of the write path (CONCURRENCY.md, "Audit"): a failed index re-sync during rollback now poisons the collection. New
  regression tests cover pending documents, acknowledgement after the log sync, a crash between a group's index sync and
  log sync, a torn group log, and concurrent acknowledged writes and deletes.
- Group commit for concurrent writers. Writes queue; a leader syncs a group with one index sync and one log sync. Writes alone, two
  and four writers reach 1.8 and 3.2 times the Phase 1 rate. A bounded
  linger lets writers share a group. A failed group rolls back every member (docs/CONCURRENCY.md, "Group commit").
- Fixed: a failed truncation was ignored, so the failed write's bytes stayed in the log, and a later add of the same id
  made the database refuse to open ("duplicate live id"). A failed log write, or a failed truncation after one, now poisons the collection.
- Fixed: a log that was shorter than its last sync was truncated before the shortened-file check, so a refused open
  changed the file. The check now runs before any truncation.
- Fixed: a missing document log was recreated empty on open. It is now refused while the catalog says it held synced
  bytes.
- Fixed: a crash inside a delete zeroed the recorded lengths, which turned off the shortened-file check. A delete now
  lowers the recorded lengths by what it can remove, so a missing vector index is still refused after such a crash.

## Open, in priority order

1. **Compaction holds the write lock for its whole rewrite and reopen.** The rewrite now reads the log once: a compacting
   delete batch at a million rows fell from 261 ms to 127 to 132 ms. The reopen replay (156 ms at 500,000 live rows) is
   now most of the rest. The design that moves the rewrite out of the lock (snapshot, background rewrite, a swap that copies
   the tail and remaps offsets) is in docs/COMPACTION.md. Its consistency claim is proven at the protocol level by
   `tests/test_compaction_model.py`. It is not implemented, pending review of the design.
0. **Staging under the index lock: investigated, not implemented.** Validation and encoding are about 0.1 ms per batch of
   about 5 ms of a four-writer group's exclusive time. Moving them out would gain less than the run-to-run noise
   (docs/CONCURRENCY.md, "Staging outside the state lock"). The index sync is most of the hold, and it needs a TurboVec change.

1b. **Index sync holds the write lock.** About 3.9 ms per batch. TurboVec syncs from its in-memory state with `&mut self`,
   so this needs an engine change to move out (docs/CONCURRENCY.md, open item 3).
1c. **Deletes write the catalog twice under the lock.** About 17 ms of a 24 ms delete. Exact refusal is kept by decision,
   so this stays as it is (docs/CONCURRENCY.md, open item 2).
2. **Open time grows with the log.** 0.44 s at 1M rows, and linear in the log. A persisted index or checkpoint would avoid
   the replay, and it would need its own recovery rules. Not started.
3. **Power-loss behaviour is not tested on hardware.** The process-level crash tests and the byte-level torn-record tests
   pass. Real power loss needs a test rig.
4. **Two id maps.** Zenar's `by_id` and TurboVec's id map together cost about 70 MB per 1M documents. Removing one needs a
   TurboVec change.
5. **Single process only.** A second process gets `DatabaseLockedError`. A server process would remove that limit.
6. **Intel macOS wheel: cross-built, not run.** The x86_64 wheel builds here (tag `macosx_10_12_x86_64`, the native module is
   an x86_64 Mach-O). This Mac has no Rosetta, so the wheel cannot be run. Running it needs Rosetta installed.
7. **Agent tools have not been run against a hosted model.** Local-model behaviour is documented in API.md.
8. **PyPI trusted publisher is not configured.** That is a repository setting, not a code change.
9. **Per-component memory breakdown** is not measured. The index split in ARCHITECTURE.md is computed from struct sizes.
10. **No in-place metadata update.** A changed document is deleted and added again. Adding an update would need its own
    consistency rules.
