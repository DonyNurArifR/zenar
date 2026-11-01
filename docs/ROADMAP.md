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
- Fixed: a failed truncation was ignored, so the failed write's bytes stayed in the log, and a later add of the same id
  made the database refuse to open ("duplicate live id"). A failed log write, or a failed truncation after one, now poisons the collection.
- Fixed: a log that was shorter than its last sync was truncated before the shortened-file check, so a refused open
  changed the file. The check now runs before any truncation.
- Fixed: a missing document log was recreated empty on open. It is now refused while the catalog says it held synced
  bytes.
- Fixed: a crash inside a delete zeroed the recorded lengths, which turned off the shortened-file check. A delete now
  lowers the recorded lengths by what it can remove, so a missing vector index is still refused after such a crash.

## Open, in priority order

1. **Reader throughput collapses under a continuous writer.** With 8 readers, search falls from about 33,000/s to about
   680/s while one writer runs batches of 50. The write lock is held 8.6 ms per batch, and 89% of that is two fsyncs. Readers
   are not starved indefinitely: p99 is about three batch times. The cost is throughput. The proposal (docs/CONCURRENCY.md)
   moves the log fsync out of the lock, behind a per-collection writer gate. That changes the locking protocol, so it
   waits for your decision. Its estimates are a model until a prototype is measured.
1a. **Deletes hold the write lock for about 25 ms.** Two catalog writes account for about 17 ms of it (by subtraction;
   not timed alone). Each catalog write syncs a temporary file and a directory. Keeping exact refusal costs that time.
   Budgeting the recorded lengths (docs/CONCURRENCY.md, option J2) would bring it to about 8 ms, but it changes the bound
   on the shortened-file check. That is a decision for you.
1b. **Compaction holds the write lock for the whole rewrite.** 115 ms at 100,000 live rows and 261 ms at 500,000. A
   long delete can stall every reader for that long. Moving the rewrite out of the lock is option K1 in the proposal.
2. **Open time grows with the log.** 0.44 s at 1M rows, and linear in the log. A persisted index or checkpoint would avoid
   the replay, and it would need its own recovery rules. Not started.
3. **Power-loss behaviour is not tested on hardware.** The process-level crash tests and the byte-level torn-record tests
   pass. Real power loss needs a test rig.
4. **Two id maps.** Zenar's `by_id` and TurboVec's id map together cost about 70 MB per 1M documents. Removing one needs a
   TurboVec change.
5. **Single process only.** A second process gets `DatabaseLockedError`. A server process would remove that limit.
6. **Intel macOS wheel is built but not run.**
7. **Agent tools have not been run against a hosted model.** Local-model behaviour is documented in API.md.
8. **PyPI trusted publisher is not configured.** That is a repository setting, not a code change.
9. **Per-component memory breakdown** is not measured. The index split in ARCHITECTURE.md is computed from struct sizes.
10. **No in-place metadata update.** A changed document is deleted and added again. Adding an update would need its own
    consistency rules.
