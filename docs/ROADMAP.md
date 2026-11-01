# Roadmap

Open work toward a production release, in priority order. Each item says what is known from measurement and what is
not. Items marked done are verified in the code and the test suite; the tests are named in STORAGE.md.

## Done in the consistency review

- Metadata indexes documented against the code, with the alternatives that were measured and the ones that were not
  (ARCHITECTURE.md, "Metadata indexes").
- Cross-file consistency documented: operation order, failure outcomes, reopen guarantees (STORAGE.md,
  "Consistency across the two files").
- Failure paths tested with real index-write failures, not only by setting the poison flag.
- Fixed: a failed truncation was ignored, so the failed write's bytes stayed in the log, and a later add of the same id
  made the database refuse to open ("duplicate live id"). A failed log write, or a failed truncation after one, now poisons the collection.
- Fixed: a log that was shorter than its last sync was truncated before the shortened-file check, so a refused open
  changed the file. The check now runs before any truncation.
- Fixed: a missing document log was recreated empty on open. It is now refused while the catalog says it held synced
  bytes.
- Fixed: a crash inside a delete zeroed the recorded lengths, which turned off the shortened-file check. A delete now
  lowers the recorded lengths by what it can remove, so a missing vector index is still refused after such a crash.

## Open, in priority order

1. **Readers stall during heavy writes.** The writer holds the collection lock across the index sync and the fsyncs. With
   a continuous writer, 8 readers run about 760 searches/s, against about 6,800/s with no writer (p50 11 ms). Group commit,
   or moving the fsyncs out of the lock, would fix it. Both change the lock protocol, so they need a design decision.
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
