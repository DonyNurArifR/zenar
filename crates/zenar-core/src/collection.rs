use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::thread::{self, ThreadId};
use std::time::{Duration, Instant};

use crate::catalog::{Catalog, Entry};
use crate::docstore::{DocRef, DocStore, DocumentRow, FieldStats, Staged};
use crate::error::ZenarError;
use crate::filter::{Filter, validate};
use crate::names::is_valid_field_name;
use crate::storage::{VectorStore, validate_dimension, validate_vector};
use crate::value::{FieldKind, MetaValue};

/// Largest id the store accepts. Ids are stored as i64 in the index files.
pub const MAX_ID: u64 = i64::MAX as u64;

/// Scopes whose statistics are kept between calls.
const SCOPE_CACHE_ENTRIES: usize = 16;

/// The most uncommitted vectors an unfiltered search over-fetches for on its first pass. Above it, a search
/// retries with the full over-fetch, and only when the first pass returned fewer than `top_k` committed results.
const SEARCH_OVERFETCH_CAP: usize = 64;

/// Statistics for one scope, as cached by `Collection::stats_for_scope`.
struct ScopedEntry {
    key: String,
    stats: Arc<Vec<FieldStats>>,
    documents: usize,
}

/// Adds between index syncs (spec section 86). Syncing on every add made ingest quadratic.
/// Adds since the last sync can be lost on a crash. Recovery removes their documents or
/// vectors, and `flush()` makes them durable on demand.
pub const SYNC_INTERVAL: usize = 1000;

/// One search result (spec section 18).
#[derive(Debug, Clone, PartialEq)]
pub struct Hit {
    pub id: u64,
    pub score: f32,
    pub text: String,
    pub metadata: Vec<(String, MetaValue)>,
}

/// Result of `Collection::retrieve`.
#[derive(Debug, Clone, PartialEq)]
pub struct Retrieval {
    pub hits: Vec<Hit>,
    /// The model's filter in plain language, or "no filter". Lets the model check what was applied.
    pub applied: String,
    /// Documents that matched the filter, before `top_k` cut the list.
    pub matched: usize,
    /// Notes for the caller, such as a value no document has.
    pub warnings: Vec<String>,
}

/// One document for `Collection::add_many`.
#[derive(Debug, Clone, PartialEq)]
pub struct Record {
    pub id: u64,
    pub vector: Vec<f32>,
    pub text: String,
    pub metadata: Vec<(String, MetaValue)>,
}

/// Repairs made when a collection was reconciled on open (spec section 26).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RecoveryReport {
    /// Documents with no vector in the index. Removed, since the vector is gone.
    pub orphaned_rows_removed: usize,
    /// Vectors in the index with no live document. Removed.
    pub orphaned_vectors_removed: usize,
}

impl RecoveryReport {
    pub fn is_clean(&self) -> bool {
        *self == RecoveryReport::default()
    }
}

struct State {
    store: VectorStore,
    docs: DocStore,
    /// Vectors added since the index was last synced.
    unsynced: usize,
    /// Length of the log up to the end of the last committed write. Bytes below it are final: a rollback
    /// truncates only to the start of a group, which is at or after it. Compaction copies up to it.
    committed_len: u64,
    /// Vectors added to the index whose documents are not committed yet. Non-zero only between
    /// the index sync and the commit of a write. Unfiltered search asks for this many extra results
    /// and drops the uncommitted ones.
    pending: usize,
    /// Set when an in-memory change could not be made durable or undone. Every
    /// operation then fails until the database is reopened, which replays the
    /// durable state.
    poisoned: Option<String>,
    /// Set when the database is closed or the collection is dropped. A handle that
    /// outlives either must not write: the write would look successful and then be lost.
    closed: bool,
}

/// Catch-up stops when fewer bytes than this are committed after the last copied position, or after this many
/// rounds. The swap copies whatever is left.
const CATCH_UP_BYTES: u64 = 64 * 1024;
const CATCH_UP_ROUNDS: usize = 8;

/// A compacted document structure built in the background, waiting for its swap.
struct Built {
    docs: DocStore,
    /// The temporary log that `docs` writes to.
    tmp: PathBuf,
    /// The old log's length up to which `docs` has the tail.
    tail_from: u64,
}

/// The state of the background compaction of one collection.
#[derive(Default)]
struct CompactionSlot {
    running: bool,
    built: Option<Built>,
    thread: Option<thread::JoinHandle<()>>,
}

/// A test observer of the compaction transitions.
/// Test hook at each compaction transition (see `compaction_point`). It runs under the write guard, so it must not call
/// into the collection: a call would take the state lock again. It receives only the transition's name. The type is
/// compiled only in unit tests, so no release build or Python wheel contains it.
#[cfg(test)]
type CompactionHook = Arc<dyn Fn(&'static str) + Send + Sync>;

/// Largest group size counted separately in `WriteStats`. Bigger groups share the last bucket.
pub const GROUP_BUCKETS: usize = 16;

/// Counters for the write path, read through `Collection::write_stats`. Every counter is cumulative
/// since the collection was opened.
#[derive(Default)]
struct WriteCounters {
    /// Syncs of a batch, one per group that synced: the number of `(index sync, log sync)` pairs.
    sync_groups: AtomicU64,
    /// Histogram of the number of writes per synced group, bucket `i` for `i + 1` writes.
    group_writes: [AtomicU64; GROUP_BUCKETS],
    /// Writes staged for a sync, summed over all groups.
    synced_writes: AtomicU64,
    /// Exclusive holds of the state lock by writes, and their total and largest duration in ns.
    holds: AtomicU64,
    hold_ns: AtomicU64,
    max_hold_ns: AtomicU64,
    /// Shared acquisitions of the state lock by readers, and the time each spent waiting for it.
    read_waits: AtomicU64,
    read_wait_ns: AtomicU64,
    max_read_wait_ns: AtomicU64,
    /// Whether each exclusive hold is recorded in `trace`. Off by default; see `Collection::set_lock_trace`.
    tracing: AtomicBool,
    trace: Mutex<Vec<LockEvent>>,
}

/// One exclusive hold of the collection lock, recorded while `Collection::set_lock_trace` is on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LockEvent {
    /// The operation that took the lock, for example `add_stage`, `delete_commit` or `swap`.
    pub op: &'static str,
    /// Time from the request for the lock to its acquisition.
    pub wait_ns: u64,
    /// Time from the acquisition to the release.
    pub hold_ns: u64,
    /// Time spent in the filesystem calls that were timed while the lock was held (`WriteGuard::add_fs`).
    pub fs_ns: u64,
}

impl WriteCounters {
    fn record_hold(&self, held: Duration) {
        let ns = u64::try_from(held.as_nanos()).unwrap_or(u64::MAX);
        self.holds.fetch_add(1, Ordering::Relaxed);
        self.hold_ns.fetch_add(ns, Ordering::Relaxed);
        self.max_hold_ns.fetch_max(ns, Ordering::Relaxed);
    }

    fn record_read_wait(&self, waited: Duration) {
        let ns = u64::try_from(waited.as_nanos()).unwrap_or(u64::MAX);
        self.read_waits.fetch_add(1, Ordering::Relaxed);
        self.read_wait_ns.fetch_add(ns, Ordering::Relaxed);
        self.max_read_wait_ns.fetch_max(ns, Ordering::Relaxed);
    }

    fn record_sync_group(&self, writes: usize) {
        self.sync_groups.fetch_add(1, Ordering::Relaxed);
        self.synced_writes
            .fetch_add(writes as u64, Ordering::Relaxed);
        let bucket = writes.clamp(1, GROUP_BUCKETS) - 1;
        self.group_writes[bucket].fetch_add(1, Ordering::Relaxed);
    }
}

/// A snapshot of the write path's counters. See `Collection::write_stats`.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct WriteStats {
    /// Groups that synced: each is one index sync and one log sync.
    pub sync_groups: u64,
    /// `group_writes[i]` counts the synced groups with `i + 1` writes. The last bucket also counts
    /// every larger group.
    pub group_writes: [u64; GROUP_BUCKETS],
    /// Writes that went through a sync group, summed over groups.
    pub synced_writes: u64,
    /// Exclusive holds of the state lock by writes.
    pub write_holds: u64,
    pub write_hold_total_ns: u64,
    pub write_hold_max_ns: u64,
    /// Shared acquisitions of the state lock by readers, and the time spent waiting for them.
    pub read_waits: u64,
    pub read_wait_total_ns: u64,
    pub read_wait_max_ns: u64,
}

/// Writer-preference admission for the state lock.
///
/// A writer announces itself before it waits for the state lock, and a reader waits here while any writer is
/// announced. The gate closes to new readers at the announcement, not when the writer starts waiting on the lock.
/// Readers that are already past the gate keep their place, so the lock's holders drain and the announced writers
/// get it. A reader that finishes a search releases the lock and then queues at the gate again, so it cannot
/// reacquire ahead of a waiting writer.
///
/// Lock order: the admission mutex is taken alone and released before the state lock is taken. It is never held
/// while a read or write guard is waited for, so the two cannot form a cycle.
///
/// Guarantees, and what they are not: an announced writer gets the state lock once the readers already past the gate
/// have released it. This does not give first-come-first-served order among writers, and it is not a fairness bound
/// for readers: while writers keep announcing, readers wait. In this crate the writers that reach the state lock
/// are the group leader, the swap, and the other short operations, so the announced count stays small.
struct Admission {
    /// Writers announced and not yet past their `write()` call.
    announced: Mutex<usize>,
    /// Signalled when `announced` falls to zero.
    open: Condvar,
    /// Readers that waited at the gate, counted for tests and diagnostics.
    gate_waits: AtomicU64,
    /// Writer announcements, counted for tests.
    total_announcements: AtomicU64,
}

impl Admission {
    fn new() -> Self {
        Self {
            announced: Mutex::new(0),
            open: Condvar::new(),
            gate_waits: AtomicU64::new(0),
            total_announcements: AtomicU64::new(0),
        }
    }

    /// Blocks while any writer is announced. Returns at once when none is.
    fn admit_reader(&self) {
        let mut announced = self.announced.lock().unwrap_or_else(|p| p.into_inner());
        if *announced == 0 {
            return;
        }
        self.gate_waits.fetch_add(1, Ordering::Relaxed);
        #[cfg(test)]
        let deadline = Instant::now() + Duration::from_secs(60);
        while *announced > 0 {
            #[cfg(test)]
            {
                // A test that leaves an announcement behind fails here, rather than waiting for ever.
                let left = deadline.saturating_duration_since(Instant::now());
                assert!(
                    !left.is_zero(),
                    "a reader waited 60 s at the admission gate: an announcement was never released"
                );
                announced = self
                    .open
                    .wait_timeout(announced, left)
                    .unwrap_or_else(|p| p.into_inner())
                    .0;
            }
            #[cfg(not(test))]
            {
                announced = self.open.wait(announced).unwrap_or_else(|p| p.into_inner());
            }
        }
    }

    /// Announces a writer. The gate closes now. The announcement is released when the returned value is
    /// dropped, which the write path does right after `write()` returns, on success and on error alike, and
    /// which unwinding also does.
    fn announce(&self) -> Announcement<'_> {
        {
            let mut announced = self.announced.lock().unwrap_or_else(|p| p.into_inner());
            *announced += 1;
        }
        self.total_announcements.fetch_add(1, Ordering::Relaxed);
        Announcement { admission: self }
    }

    fn retire(&self) {
        let mut announced = self.announced.lock().unwrap_or_else(|p| p.into_inner());
        debug_assert!(*announced > 0, "an announcement was retired twice");
        *announced -= 1;
        if *announced == 0 {
            self.open.notify_all();
        }
    }

    #[cfg(test)]
    fn waiting_writers(&self) -> usize {
        *self.announced.lock().unwrap_or_else(|p| p.into_inner())
    }
}

/// One announced writer, released on drop.
struct Announcement<'a> {
    admission: &'a Admission,
}

impl Drop for Announcement<'_> {
    fn drop(&mut self) {
        self.admission.retire();
    }
}

thread_local! {
    /// How many `StateRead` guards this thread holds. A read entry point that runs while another one is held would
    /// wait at the gate for a writer that is waiting for that very guard, so debug builds refuse it.
    static READS_HELD: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Shared access to the state. Entry points take one of these; it must not be held while another is taken.
struct StateRead<'a> {
    guard: RwLockReadGuard<'a, State>,
}

impl std::ops::Deref for StateRead<'_> {
    type Target = State;
    fn deref(&self) -> &State {
        &self.guard
    }
}

impl Drop for StateRead<'_> {
    fn drop(&mut self) {
        READS_HELD.with(|n| n.set(n.get() - 1));
    }
}

/// Exclusive access to the state. Dropping it records how long the lock was held.
struct WriteGuard<'a> {
    guard: RwLockWriteGuard<'a, State>,
    since: Instant,
    counters: &'a WriteCounters,
    op: &'static str,
    waited_ns: u64,
    fs_ns: u64,
}

impl WriteGuard<'_> {
    /// Adds the time since `start` to the filesystem time of this hold.
    fn add_fs(&mut self, start: Instant) {
        self.fs_ns += u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX);
    }

    /// Runs `f` on the state, and adds its time to the filesystem time of this hold.
    fn timed<T>(&mut self, f: impl FnOnce(&mut State) -> T) -> T {
        let start = Instant::now();
        let out = f(&mut self.guard);
        self.fs_ns += u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX);
        out
    }
}

impl std::ops::Deref for WriteGuard<'_> {
    type Target = State;
    fn deref(&self) -> &State {
        &self.guard
    }
}

impl std::ops::DerefMut for WriteGuard<'_> {
    fn deref_mut(&mut self) -> &mut State {
        &mut self.guard
    }
}

impl Drop for WriteGuard<'_> {
    fn drop(&mut self) {
        let held = self.since.elapsed();
        self.counters.record_hold(held);
        if self.counters.tracing.load(Ordering::Relaxed) {
            let event = LockEvent {
                op: self.op,
                wait_ns: self.waited_ns,
                hold_ns: u64::try_from(held.as_nanos()).unwrap_or(u64::MAX),
                fs_ns: self.fs_ns,
            };
            if let Ok(mut trace) = self.counters.trace.lock() {
                trace.push(event);
            }
        }
    }
}

/// One write queued for the writer. Its result is set by the leader, and taken by the writer that queued it.
struct Request {
    op: Op,
    outcome: Mutex<Option<Result<usize, ZenarError>>>,
    /// Set once the leader has a result for the write. A taken result leaves `outcome` empty, so this
    /// flag tells a finished write from an abandoned one.
    done: AtomicBool,
}

impl Request {
    fn new(op: Op) -> Self {
        Self {
            op,
            outcome: Mutex::new(None),
            done: AtomicBool::new(false),
        }
    }

    fn finish(&self, result: Result<usize, ZenarError>) {
        let mut outcome = self.outcome.lock().unwrap_or_else(|p| p.into_inner());
        *outcome = Some(result);
        self.done.store(true, Ordering::SeqCst);
    }
}

/// A write operation. Adds are grouped; the others run alone.
enum Op {
    /// `batch` is false for `add`, which may be committed without a sync, and true for `add_many`,
    /// which always syncs.
    Add {
        records: Vec<Record>,
        batch: bool,
    },
    Delete(Vec<u64>),
    Flush,
    Calibrate(Vec<f32>),
    /// Swap in the structure a background compaction built.
    Compact,
}

/// The queue of writes waiting for a leader.
#[derive(Default)]
struct Queue {
    pending: Vec<Arc<Request>>,
    leading: bool,
    /// The last time each writer thread queued a write. The leader counts the recent ones to decide
    /// how long to wait for the others before it syncs.
    seen: HashMap<ThreadId, Instant>,
}

/// How long a leader waits for the other recent writers before it starts a group. A lone writer
/// never waits. A group only grows when a writer arrives within this time.
const GROUP_LINGER: Duration = Duration::from_millis(2);

/// A writer thread counts as recent for this long after its last write.
const RECENT_WRITER: Duration = Duration::from_millis(20);

/// Holds the leader role for one group. Dropping it finishes any write the group left without a
/// result, and releases the role, so a panic in the group cannot leave writers waiting for ever.
struct Lead<'a> {
    collection: &'a Collection,
    group: Vec<Arc<Request>>,
}

impl Drop for Lead<'_> {
    fn drop(&mut self) {
        if self
            .group
            .iter()
            .any(|req| !req.done.load(Ordering::SeqCst))
        {
            // The group stopped part way, so its staged writes may be in the index and the log but not in
            // memory. Only a reopen, which reconciles the two files, can make the state consistent again.
            let announced = self.collection.admission.announce();
            let mut state = self
                .collection
                .state
                .write()
                .unwrap_or_else(|p| p.into_inner());
            drop(announced);
            state.poisoned.get_or_insert_with(|| {
                "a write group was aborted part way; reopen the database to recover".into()
            });
            drop(state);
            for req in &self.group {
                req.finish(Err(ZenarError::VectorStoreError(
                    "the write group was aborted".into(),
                )));
            }
        }
        let mut queue = self
            .collection
            .queue
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        queue.leading = false;
        self.collection.ready.notify_all();
    }
}

/// The writes of one group's adds, staged in order and waiting for their sync.
struct Batch {
    /// The requests whose writes are staged, with their record counts and whether they are `add_many`.
    members: Vec<(Arc<Request>, usize, bool)>,
    /// The ids of the vectors added to the index, in order.
    added: Vec<u64>,
    /// The staged log writes, in order. The first one's start is the rollback point.
    staged: Vec<Staged>,
    /// Whether the index was empty when the batch started, so that the batch may fix the dimension.
    was_empty: bool,
}

impl Batch {
    fn new(was_empty: bool) -> Self {
        Self {
            members: Vec::new(),
            added: Vec::new(),
            staged: Vec::new(),
            was_empty,
        }
    }
}

/// Why a request could not be staged. A request error changes nothing. A batch error may leave writes
/// behind, so the batch is rolled back.
enum StageError {
    Request(ZenarError),
    Batch(ZenarError),
}

fn queue_poisoned() -> ZenarError {
    ZenarError::VectorStoreError("collection write queue poisoned".into())
}

fn finish_all(reqs: &[Arc<Request>], e: &ZenarError) {
    for req in reqs {
        req.finish(Err(e.clone()));
    }
}

fn finish_members(batch: &Batch, e: &ZenarError) {
    for (req, _, _) in &batch.members {
        req.finish(Err(e.clone()));
    }
}

/// The values to store for each record of a batch, and the metadata fields the batch introduces.
type PreparedBatch = (Vec<Vec<(String, MetaValue)>>, Vec<(String, FieldKind)>);

/// Checks a batch against the collection. Returns the values to store and the metadata fields the
/// batch introduces. Nothing is written. The vectors of earlier requests in the same group are
/// already in the index, so they are checked for duplicates as stored documents are.
fn prepare_batch(state: &State, records: &[Record]) -> Result<PreparedBatch, ZenarError> {
    if state.store.dim().is_none() {
        validate_dimension(records[0].vector.len())?;
    }
    let expected_dim = state.store.dim().or(Some(records[0].vector.len()));
    let mut seen = HashSet::with_capacity(records.len());
    let mut schema: Vec<(String, FieldKind)> = state.docs.fields().to_vec();
    let mut new_fields: Vec<(String, FieldKind)> = Vec::new();
    let mut prepared: Vec<Vec<(String, MetaValue)>> = Vec::with_capacity(records.len());
    for record in records {
        check_id(record.id)?;
        if !seen.insert(record.id)
            || state.store.contains(record.id)
            || state.docs.contains(record.id)
        {
            return Err(ZenarError::DuplicateId(record.id));
        }
        validate_vector(&record.vector, expected_dim)?;
        let (introduced, values) = resolve_metadata(&schema, record.metadata.clone())?;
        schema.extend(introduced.iter().cloned());
        new_fields.extend(introduced);
        prepared.push(values);
    }
    Ok((prepared, new_fields))
}

/// The compaction's starting point under the read lock: the log path, the fields, and the committed length.
type CompactionSnapshot = (PathBuf, Vec<(String, FieldKind)>, u64);

/// The retrieval's parsed filters under the read lock: the model's, the scope's, and the warnings.
type ParsedRetrieval = (Option<Filter>, Option<Filter>, Vec<String>);
/// The parts of a collection that code running under a state guard may use. `Core` has no field that refers to
/// `Collection` or to the state lock, so code that takes `&Core` has no route to the lock and cannot take it again.
/// Guard-holding code is written as methods on `Core`, which receive the guard explicitly. Only `Collection` takes
/// the lock, through `with_read` and `with_write`, and those pass `Core` and the guard to a function pointer. A
/// function pointer cannot capture `Collection`.
struct Core {
    name: String,
    /// Field statistics for discovery. Cleared by every write, see `write_state_op`.
    stats: Mutex<Option<Arc<Vec<FieldStats>>>>,
    /// Statistics for scoped calls, keyed by the scope, most recent last. Cleared by every write.
    scoped: Mutex<Vec<ScopedEntry>>,
    index_path: PathBuf,
    catalog: Arc<Catalog>,
    /// Sync the document log on every commit, not only at sync boundaries.
    durable: bool,
    /// Counters for `write_stats`, and the lock trace.
    counters: WriteCounters,
    /// Test hook at each compaction transition: an observer, and one point that fails.
    #[cfg(test)]
    compaction_hook: Mutex<Option<CompactionHook>>,
    #[cfg(test)]
    fail_compaction_at: Mutex<Option<&'static str>>,
    /// Test hook: the next staging of a batch fails with an injected error, as a failed log write would.
    #[cfg(test)]
    fail_stage: AtomicBool,
    /// Test only: how many unfiltered searches retried with the full over-fetch.
    #[cfg(test)]
    search_retries: AtomicU64,
}

/// What a group's add stage leaves to do. A batch that needs a log sync is handed back, with the lock released,
/// so the log can sync while readers run. The batch is then committed under the lock again.
enum AddStep {
    /// Nothing is left: the batch was committed, or rolled back, or it was empty.
    Finished,
    /// The index is synced. The log syncs through `log` next, and the batch commits after that.
    Sync { batch: Batch, log: std::fs::File },
}

/// A delete staged under the lock. Its log syncs with the lock released, then it commits under the lock again.
struct DeleteStep {
    staged: Staged,
    log: std::fs::File,
}

/// A named set of vectors plus their documents and metadata.
///
/// Writes are serialized behind one lock. Reads run concurrently with each other.
///
/// The lock is taken only in this impl block, in `read_state`, `write_state_op` and `close`. Code that runs under
/// a guard lives on `Core`, see `with_read` and `with_write`.
pub struct Collection {
    /// Everything code under a state guard may use. See `Core`.
    core: Core,
    /// Writes waiting for a leader. See `submit`.
    queue: Mutex<Queue>,
    /// The background compaction: whether one is running, and the structure it built for the swap.
    compaction: Mutex<CompactionSlot>,
    /// This collection, as a `Weak` so that a compaction thread can reach it without keeping it alive.
    self_ref: std::sync::OnceLock<std::sync::Weak<Collection>>,
    /// Signalled when a leader finishes, so that a waiting writer can become the leader.
    ready: Condvar,
    state: RwLock<State>,
    /// Test hook that runs while a write has released `state` and is syncing its log.
    #[cfg(test)]
    window_hook: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    /// Test hook: the next log sync fails with an injected error.
    #[cfg(test)]
    fail_log_sync: AtomicBool,
    /// Writer-preference admission for `state`: see `Admission`.
    admission: Admission,
}

impl Core {
    /// Stages a group of adds under the lock, and finishes the batch it leaves: see `finish_batch`.
    fn stage_adds(&self, state: &mut WriteGuard<'_>, reqs: Vec<Arc<Request>>) -> AddStep {
        let mut batch = Batch::new(state.store.dim().is_none());
        for req in reqs {
            let Op::Add {
                records,
                batch: is_batch,
            } = &req.op
            else {
                unreachable!("run_adds takes only adds");
            };
            let count = records.len();
            let is_batch = *is_batch;
            match self.stage_request(state, &mut batch, records) {
                Ok(()) => batch.members.push((req, count, is_batch)),
                Err(StageError::Request(e)) => req.finish(Err(e)),
                Err(StageError::Batch(e)) => {
                    let cause = self.rollback_batch(state, batch, e);
                    req.finish(Err(cause));
                    batch = Batch::new(state.store.dim().is_none());
                }
            }
        }
        self.finish_batch(state, batch)
    }

    /// Decides how a staged batch finishes. A batch that needs no log sync is committed here. Otherwise the index is
    /// synced under the lock, and the log is handed back for the sync that follows the release of the lock. A batch
    /// whose index or log cannot be synced is rolled back here.
    fn finish_batch(&self, state: &mut WriteGuard<'_>, batch: Batch) -> AddStep {
        if batch.members.is_empty() {
            return AddStep::Finished;
        }
        let needs_sync = self.durable
            || batch.members.iter().any(|(_, _, is_batch)| *is_batch)
            || state.unsynced + batch.added.len() >= SYNC_INTERVAL;
        if !needs_sync {
            state.unsynced += batch.added.len();
            self.commit_batch(state, batch);
            return AddStep::Finished;
        }

        state.pending = batch.added.len();
        if let Err(e) = state.timed(|s| s.store.sync(&self.index_path)) {
            self.rollback_batch(state, batch, e);
            return AddStep::Finished;
        }
        match state.timed(|s| s.docs.sync_handle()) {
            Ok(log) => AddStep::Sync { batch, log },
            Err(e) => {
                self.rollback_batch(state, batch, e);
                AddStep::Finished
            }
        }
    }

    /// Commits a batch after its log sync. A failed sync rolls it back instead.
    fn commit_synced(
        &self,
        state: &mut WriteGuard<'_>,
        (batch, synced): (Batch, Result<(), ZenarError>),
    ) {
        if let Err(e) = synced {
            self.rollback_batch(state, batch, e);
            return;
        }
        state.pending = 0;
        state.unsynced = 0;
        self.counters.record_sync_group(batch.members.len());
        self.commit_batch(state, batch);
    }

    /// Validates one add request and stages it into the batch. A request error leaves the batch as
    /// it was. A batch error means the batch's writes may be partly done, and the caller rolls it back.
    fn stage_request(
        &self,
        state: &mut WriteGuard<'_>,
        batch: &mut Batch,
        records: &[Record],
    ) -> Result<(), StageError> {
        healthy(state).map_err(StageError::Request)?;
        let (values, new_fields) = prepare_batch(state, records).map_err(StageError::Request)?;
        self.extend_schema(state, &new_fields)
            .map_err(StageError::Request)?;
        let refs: Vec<DocRef<'_>> = records
            .iter()
            .zip(values.iter())
            .map(|(r, values)| DocRef {
                id: r.id,
                text: r.text.as_str(),
                metadata: values.as_slice(),
            })
            .collect();
        #[cfg(test)]
        if self
            .fail_stage
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            let e = ZenarError::PersistenceError("injected staging failure".into());
            return Err(StageError::Batch(poison(state, e)));
        }
        let staged = state
            .timed(|s| s.docs.stage_puts(&refs))
            .map_err(|e| StageError::Batch(poison(state, e)))?;
        batch.staged.push(staged);
        for record in records {
            batch.added.push(record.id);
            state
                .store
                .add(&record.vector, record.id)
                .map_err(StageError::Batch)?;
        }
        Ok(())
    }

    /// Undoes a batch that could not be committed: removes its vectors, syncs the index to match, and
    /// truncates the log to the start of the batch. Finishes every member with the cause and returns it.
    /// If the index cannot be synced to match, or the truncation fails, the collection is poisoned: the
    /// disk would then disagree with memory, and only a reopen, which reconciles, may continue.
    fn rollback_batch(&self, state: &mut State, batch: Batch, cause: ZenarError) -> ZenarError {
        state.pending = 0;
        for id in &batch.added {
            state.store.remove(*id);
        }
        if !batch.added.is_empty()
            && let Err(e) = state.store.sync(&self.index_path)
        {
            poison(state, e);
        }
        let cause = match batch.staged.into_iter().next() {
            Some(first) => undo_or_poison(state, first, cause),
            None => cause,
        };
        for (req, _, _) in &batch.members {
            req.finish(Err(cause.clone()));
        }
        cause
    }

    /// Makes a batch visible and finishes its members. The first vector of a collection fixes its
    /// dimension, which is recorded in the catalog. If that write fails, the batch is already visible,
    /// so every member gets the error and may still be applied (docs/STORAGE.md).
    fn commit_batch(&self, state: &mut State, batch: Batch) {
        for staged in batch.staged {
            state.docs.commit(staged);
            state.committed_len = state.docs.log_len();
        }
        let recorded = if batch.was_empty && state.store.dim().is_some() {
            self.record_dimension(state)
        } else {
            Ok(())
        };
        for (req, count, _) in &batch.members {
            req.finish(match &recorded {
                Ok(()) => Ok(*count),
                Err(e) => Err(e.clone()),
            });
        }
    }

    /// Persists new schema fields in the catalog, then extends the in-memory schema.
    /// The order matters. A record that uses a new field must not reach the log before the
    /// catalog knows the field, or reopening would fail.
    fn extend_schema(
        &self,
        state: &mut State,
        new_fields: &[(String, FieldKind)],
    ) -> Result<(), ZenarError> {
        if new_fields.is_empty() {
            return Ok(());
        }
        let added: Vec<(String, String)> = new_fields
            .iter()
            .map(|(n, k)| (n.clone(), k.as_str().to_string()))
            .collect();
        self.catalog
            .update(&self.name, |entry| entry.fields.extend(added))?;
        state.docs.extend_fields(new_fields);
        Ok(())
    }

    fn record_dimension(&self, state: &State) -> Result<(), ZenarError> {
        if let Some(dim) = state.store.dim() {
            self.catalog
                .update(&self.name, |entry| entry.dimension = Some(dim))?;
        }
        Ok(())
    }

    /// Syncs the vector index and the document log. Both must succeed.
    fn sync_both(&self, state: &mut WriteGuard<'_>) -> Result<(), ZenarError> {
        state.timed(|s| s.store.sync(&self.index_path))?;
        state.docs.sync()?;
        state.unsynced = 0;
        Ok(())
    }

    /// Lowers the recorded lengths before a delete changes anything on disk. The index can lose
    /// `deleting` vectors when it is synced, and the log shrinks only when compaction runs, so
    /// before the delete is recorded again a crash can leave each file between its recorded length
    /// and that length less `deleting`. Lowering by exactly that keeps the shortened-file check
    /// on, so a file that is missing or cut short is still refused. Zeroing the log length is
    /// needed only when compaction runs.
    fn lower_recorded_lengths(&self, deleting: usize, compacting: bool) -> Result<(), ZenarError> {
        self.catalog.update(&self.name, |entry| {
            entry.durable_vectors = entry.durable_vectors.saturating_sub(deleting);
            if compacting {
                entry.durable_log_len = 0;
            }
        })
    }

    /// Records how far the log and the vector index are synced. Call it right after a successful
    /// sync, and after compaction, since both change what is durable.
    fn record_durable(&self, state: &State) -> Result<(), ZenarError> {
        let log_len = state.docs.log_len();
        let vectors = state.store.len();
        self.catalog.update(&self.name, |entry| {
            entry.durable_log_len = log_len;
            entry.durable_vectors = vectors;
            // Recording the lengths also ends any compaction that was recorded as pending.
            entry.compaction_pending = None;
        })
    }

    /// Puts the catalog entry back to its state before the compaction. If that write fails, the catalog may
    /// still hold the pending record, which a reopen settles, so the collection is poisoned.
    fn undo_before_rename(
        &self,
        state: &mut WriteGuard<'_>,
        prior: &Entry,
        cause: ZenarError,
    ) -> Result<(), ZenarError> {
        let (len, pending) = (prior.durable_log_len, prior.compaction_pending);
        match self.catalog.update(&self.name, |e| {
            e.durable_log_len = len;
            e.compaction_pending = pending;
        }) {
            Ok(()) => Err(cause),
            Err(e) => Err(poison(state, e)),
        }
    }

    /// Checks a delete's ids under the lock, then removes them from the index, syncs the index and stages the
    /// tombstones. The index change is synced under the write lock before the tombstones are logged, so a deleted
    /// vector leaves the disk first. Any failure from the index sync or the staging poisons the collection.
    fn stage_deletes(
        &self,
        state: &mut WriteGuard<'_>,
        ids: &[u64],
    ) -> Result<DeleteStep, ZenarError> {
        healthy(state)?;
        if ids.len() > 1 {
            let mut seen = HashSet::with_capacity(ids.len());
            for &id in ids {
                if !seen.insert(id) {
                    return Err(ZenarError::InvalidArgument(format!(
                        "id {id} appears twice in the batch"
                    )));
                }
            }
        }
        for &id in ids {
            if !state.docs.contains(id) {
                return Err(ZenarError::VectorNotFound(id));
            }
        }
        // The log shrinks only at a swap, which records its own lengths (see `swap_locked`).
        let lowering = Instant::now();
        let lowered = self.lower_recorded_lengths(ids.len(), false);
        state.add_fs(lowering);
        lowered?;
        for &id in ids {
            state.store.remove(id);
        }
        if let Err(e) = state.timed(|s| s.store.sync(&self.index_path)) {
            return Err(poison(state, e));
        }
        let staged = match state.timed(|s| s.docs.stage_deletes(ids)) {
            Ok(staged) => staged,
            Err(e) => return Err(poison(state, e)),
        };
        let log = match state.timed(|s| s.docs.sync_handle()) {
            Ok(log) => log,
            Err(e) => return Err(poison(state, e)),
        };
        Ok(DeleteStep { staged, log })
    }

    /// Commits a delete after its log sync, and records the durable lengths. Returns whether the log now passes the
    /// compaction threshold, so that the caller starts a compaction once the lock is released.
    fn commit_deletes(
        &self,
        state: &mut WriteGuard<'_>,
        (staged, synced): (Staged, Result<(), ZenarError>),
    ) -> (bool, Result<(), ZenarError>) {
        if let Err(e) = synced {
            return (false, Err(poison(state, e)));
        }
        state.docs.commit(staged);
        state.committed_len = state.docs.log_len();
        state.unsynced = 0;
        let compact_due = state.docs.should_compact();
        let recording = Instant::now();
        let recorded = self.record_durable(state);
        state.add_fs(recording);
        match recorded {
            Ok(()) => (compact_due, Ok(())),
            Err(e) => (compact_due, Err(poison(state, e))),
        }
    }

    fn flush_state(&self, state: &mut WriteGuard<'_>, _: ()) -> Result<(), ZenarError> {
        healthy(state)?;
        if state.unsynced > 0 || self.durable {
            self.sync_both(state)?;
        }
        // Adds only grow the files, so their recorded lengths can lag until the next flush. Recording
        // here keeps the shortened-file check exact at every flush and close. Deletes record on their own,
        // since they are the operations that shrink a file.
        self.record_durable(state)?;
        Ok(())
    }

    fn calibrate_state(
        &self,
        state: &mut WriteGuard<'_>,
        sample: &[f32],
    ) -> Result<(), ZenarError> {
        healthy(state)?;
        state.store.calibrate(sample)?;
        state.timed(|s| s.store.sync(&self.index_path))?;
        state.unsynced = 0;
        Ok(())
    }

    /// Reconciles the document log and the vector index after a restart (spec section 26).
    fn reconcile_state(
        &self,
        state: &mut WriteGuard<'_>,
        _: (),
    ) -> Result<RecoveryReport, ZenarError> {
        healthy(state)?;
        let mut report = RecoveryReport::default();

        let live: HashSet<u64> = state.docs.live_ids().into_iter().collect();
        let mut orphan_docs: Vec<u64> = live
            .iter()
            .copied()
            .filter(|id| !state.store.contains(*id))
            .collect();
        orphan_docs.sort_unstable();
        let mut dirty = false;
        for id in orphan_docs {
            let staged = state.docs.stage_delete(id)?;
            state.docs.commit(staged);
            state.committed_len = state.docs.log_len();
            report.orphaned_rows_removed += 1;
            dirty = true;
        }

        let stray: Vec<u64> = state.store.ids().filter(|id| !live.contains(id)).collect();
        for id in stray {
            state.store.remove(id);
            report.orphaned_vectors_removed += 1;
            dirty = true;
        }

        if dirty {
            self.sync_both(state)?;
        }
        Ok(report)
    }

    /// Writes the current index file (used when a new collection is created with a dimension).
    fn persist_state(&self, state: &mut WriteGuard<'_>, _: ()) -> Result<(), ZenarError> {
        state.timed(|s| s.store.sync(&self.index_path))?;
        state.unsynced = 0;
        Ok(())
    }

    /// The swap (docs/R3.md, section 4). Failures before the rename undo themselves, and leave the collection
    /// usable. A failure after the rename poisons it, and a reopen settles the catalog and the log. On success
    /// the old structure is returned, for the caller to drop once the lock is released.
    fn swap_locked(
        &self,
        state: &mut WriteGuard<'_>,
        built: Built,
    ) -> Result<Option<DocStore>, ZenarError> {
        let Built {
            mut docs,
            tmp,
            tail_from,
        } = built;
        let path = state.docs.log_path().to_path_buf();
        let prior = self
            .catalog
            .get(&self.name)?
            .ok_or_else(|| ZenarError::CollectionNotFound(self.name.clone()))?;
        // Before the rename: copy the final tail, sync the temporary log, and record the pending lengths.
        let fs_start = Instant::now();
        let before_rename = (|| -> Result<(), ZenarError> {
            healthy(state)?;
            let bytes = DocStore::read_log_range(&path, tail_from, state.committed_len)?;
            docs.append_raw(&bytes)?;
            self.compaction_point("swap tail")?;
            docs.sync()?;
            self.compaction_point("swap tail synced")?;
            let new_len = docs.log_len();
            let old_len = state.docs.log_len();
            self.catalog.update(&self.name, |e| {
                e.durable_log_len = new_len;
                e.compaction_pending = Some(old_len);
            })?;
            self.compaction_point("pending recorded")?;
            Ok(())
        })();
        state.add_fs(fs_start);
        if let Err(e) = before_rename {
            drop(docs);
            let _ = fs::remove_file(&tmp);
            return self.undo_before_rename(state, &prior, e).map(|()| None);
        }
        // The rename. The old log's handle is released first, as Windows requires.
        let rename_start = Instant::now();
        state.docs.release_handle();
        if let Err(e) = fs::rename(&tmp, &path) {
            let _ = fs::remove_file(&tmp);
            if let Err(reattach) = state.docs.reattach() {
                return Err(poison(state, reattach));
            }
            return self
                .undo_before_rename(state, &prior, crate::docstore::io_err(e))
                .map(|()| None);
        }
        // From here the file on disk is the new log, and the memory must follow it.
        if let Err(e) = self.compaction_point("renamed") {
            return Err(poison(state, e));
        }
        let synced_parent = crate::docstore::sync_parent(&path);
        state.add_fs(rename_start);
        if let Err(e) = synced_parent {
            return Err(poison(state, e));
        }
        if let Err(e) = self.compaction_point("directory synced") {
            return Err(poison(state, e));
        }
        // The new structure already has an open handle on the renamed file, so no reopen is needed.
        docs.retarget(&path);
        let old = std::mem::replace(&mut state.docs, docs);
        if let Err(e) = self.compaction_point("swapped") {
            return Err(poison(state, e));
        }
        let recording = Instant::now();
        let recorded = self.record_durable(state);
        state.add_fs(recording);
        if let Err(e) = recorded {
            return Err(poison(state, e));
        }
        if let Err(e) = self.compaction_point("pending cleared") {
            return Err(poison(state, e));
        }
        state.committed_len = state.docs.log_len();
        state.unsynced = 0;
        Ok(Some(old))
    }

    /// Test hook at each transition of a compaction. A hook can observe the directory (to copy it, as a
    /// crash would leave it), and a configured point fails with an injected error.
    #[cfg(test)]
    fn compaction_point(&self, point: &'static str) -> Result<(), ZenarError> {
        let hook = self.compaction_hook.lock().ok().and_then(|h| h.clone());
        if let Some(hook) = hook {
            hook(point);
        }
        let mut fail = self
            .fail_compaction_at
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        if *fail == Some(point) {
            *fail = None;
            return Err(ZenarError::PersistenceError(format!(
                "injected failure at {point}"
            )));
        }
        Ok(())
    }

    #[cfg(not(test))]
    fn compaction_point(&self, _point: &'static str) -> Result<(), ZenarError> {
        Ok(())
    }

    /// Statistics and document count for a scope. `None` is the whole collection. A scope is
    /// computed once and reused until the next write, since discovery repeats the same scope.
    fn stats_for_scope(
        &self,
        state: &State,
        scope: Option<&serde_json::Value>,
    ) -> Result<(Arc<Vec<FieldStats>>, usize), ZenarError> {
        let Some(value) = scope else {
            return Ok((self.cached_stats(state)?, state.docs.live_count()));
        };
        let key = value.to_string();
        {
            let scoped = self
                .scoped
                .lock()
                .map_err(|_| ZenarError::VectorStoreError("scope cache poisoned".into()))?;
            if let Some(entry) = scoped.iter().find(|e| e.key == key) {
                return Ok((Arc::clone(&entry.stats), entry.documents));
            }
        }
        let rows = self.scope_rows(state, Some(value))?.unwrap_or_default();
        let stats = Arc::new(state.docs.field_stats_in(&rows));
        let documents = rows.len();
        let mut scoped = self
            .scoped
            .lock()
            .map_err(|_| ZenarError::VectorStoreError("scope cache poisoned".into()))?;
        if scoped.len() >= SCOPE_CACHE_ENTRIES {
            scoped.remove(0);
        }
        scoped.push(ScopedEntry {
            key,
            stats: Arc::clone(&stats),
            documents,
        });
        Ok((stats, documents))
    }

    /// The live rows that `scope` allows. `None` for no scope, meaning the whole collection.
    /// An empty scope object allows every live document.
    fn scope_rows(
        &self,
        state: &State,
        scope: Option<&serde_json::Value>,
    ) -> Result<Option<Vec<u32>>, ZenarError> {
        let Some(value) = scope else {
            return Ok(None);
        };
        // The scope is parsed by the same code as `where`, so the message must name the argument.
        if !value.is_object() {
            return Err(ZenarError::InvalidFilter(
                "scope must be a JSON object, for example {\"tenant\": \"A\"}".into(),
            ));
        }
        let stats = self.cached_stats(state)?;
        let filter = crate::agent::parse(value, state.docs.fields(), &stats)?
            .filter
            .unwrap_or(Filter::And(Vec::new()));
        validate(&filter, state.docs.fields())?;
        Ok(Some(state.docs.matching_rows(&filter)?))
    }

    /// Field statistics, cached until the next write.
    fn cached_stats(&self, state: &State) -> Result<Arc<Vec<FieldStats>>, ZenarError> {
        let mut cache = self
            .stats
            .lock()
            .map_err(|_| ZenarError::VectorStoreError("stats cache poisoned".into()))?;
        if let Some(stats) = cache.as_ref() {
            return Ok(Arc::clone(stats));
        }
        let stats = Arc::new(state.docs.field_stats());
        *cache = Some(Arc::clone(&stats));
        Ok(stats)
    }

    /// Candidates for an unfiltered search, best first, as `(score, id)` pairs of the index: the first pass, or the
    /// full over-fetch when the first pass lacks committed results.
    ///
    /// Vectors whose documents are not committed yet can rank in the top results (`pending` of them at most).
    /// The first pass over-fetches only `min(pending, SEARCH_OVERFETCH_CAP)` for them. If it returns fewer than
    /// `top_k` committed candidates while more than the cap is pending, a retry asks for the full
    /// `top_k + pending`, which always holds at least `min(top_k, committed)` committed candidates. Both passes
    /// run under the caller's read guard, so they see the same index and the same `pending`. The caller drops
    /// the uncommitted candidates. Arithmetic saturates, so a very large `top_k` cannot wrap.
    fn unfiltered_candidates(
        &self,
        state: &State,
        query: &[f32],
        top_k: usize,
    ) -> Result<(Vec<f32>, Vec<u64>), ZenarError> {
        let pending = state.pending;
        let full = top_k.saturating_add(pending).min(state.store.len());
        let first = top_k
            .saturating_add(pending.min(SEARCH_OVERFETCH_CAP))
            .min(state.store.len());
        let (scores, ids) = state.store.search(query, first, None)?;
        if pending <= SEARCH_OVERFETCH_CAP || first >= full {
            return Ok((scores, ids));
        }
        let committed = ids
            .iter()
            .filter(|id| state.docs.contains(**id))
            .take(top_k)
            .count();
        if committed >= top_k {
            return Ok((scores, ids));
        }
        #[cfg(test)]
        self.search_retries.fetch_add(1, Ordering::Relaxed);
        state.store.search(query, full, None)
    }

    fn dimension_locked(&self, state: &State, _: ()) -> Option<usize> {
        state.store.dim()
    }

    fn bit_width_locked(&self, state: &State, _: ()) -> usize {
        state.store.bit_width()
    }

    fn get_locked(&self, state: &State, id: u64) -> Result<Option<DocumentRow>, ZenarError> {
        healthy(state)?;
        Ok(state.docs.fetch(&[id])?.into_iter().next())
    }

    fn get_many_locked(&self, state: &State, ids: &[u64]) -> Result<Vec<DocumentRow>, ZenarError> {
        healthy(state)?;
        state.docs.fetch(ids)
    }

    fn count_locked(&self, state: &State, _: ()) -> Result<usize, ZenarError> {
        healthy(state)?;
        Ok(state.docs.live_count())
    }

    fn compaction_snapshot(&self, state: &State, _: ()) -> Result<CompactionSnapshot, ZenarError> {
        healthy(state)?;
        Ok((
            state.docs.log_path().to_path_buf(),
            state.docs.fields().to_vec(),
            state.committed_len,
        ))
    }

    /// Like `search`, and also returns how many documents matched the filter before `top_k`
    /// cut the list. A caller can tell "no match" apart from "more matches than requested".
    fn search_locked(
        &self,
        state: &State,
        (query, top_k, filter): (&[f32], usize, Option<&Filter>),
    ) -> Result<(Vec<Hit>, usize), ZenarError> {
        healthy(state)?;
        if state.store.dim().is_none() {
            return Ok((Vec::new(), 0));
        }
        validate_vector(query, state.store.dim())?;

        let (scores, ids, matched) = match filter {
            None => {
                let matched = state.docs.live_count();
                if state.store.is_empty() {
                    return Ok((Vec::new(), 0));
                }
                let (scores, ids) = self.unfiltered_candidates(state, query, top_k)?;
                (scores, ids, matched)
            }
            Some(filter) => {
                validate(filter, state.docs.fields())?;
                let candidates = state.docs.matching_ids(filter)?;
                let live: Vec<u64> = candidates
                    .into_iter()
                    .filter(|id| state.store.contains(*id))
                    .collect();
                let matched = live.len();
                if live.is_empty() {
                    return Ok((Vec::new(), 0));
                }
                let k = top_k.min(live.len());
                let (scores, ids) = state.store.search(query, k, Some(&live))?;
                (scores, ids, matched)
            }
        };
        if ids.is_empty() {
            return Ok((Vec::new(), matched));
        }
        let mut docs: HashMap<u64, DocumentRow> = state
            .docs
            .fetch(&ids)?
            .into_iter()
            .map(|d| (d.id, d))
            .collect();
        let hits = ids
            .into_iter()
            .zip(scores)
            .filter_map(|(id, score)| {
                docs.remove(&id).map(|doc| Hit {
                    id,
                    score,
                    text: doc.text,
                    metadata: doc.metadata,
                })
            })
            .take(top_k)
            .collect();
        Ok((hits, matched))
    }

    /// Discovery: every field with its type, the documents that carry it, and its values with
    /// counts. `scope` limits both the documents counted and the values listed. `None` means
    /// the whole collection, and should only be used by trusted code.
    fn describe_locked(
        &self,
        state: &State,
        scope: Option<&serde_json::Value>,
    ) -> Result<serde_json::Value, ZenarError> {
        healthy(state)?;
        let (fields, documents) = self.stats_for_scope(state, scope)?;
        let fields: Vec<serde_json::Value> = fields
            .iter()
            .map(|stat| crate::agent::field_json(stat, documents))
            .collect();
        Ok(serde_json::json!({
            "name": self.name,
            "dimension": state.store.dim(),
            "bit_width": state.store.bit_width(),
            "documents": documents,
            "fields": fields,
        }))
    }

    /// Finds values of one field that contain `text` (case-insensitive), most frequent first,
    /// within `scope`. For fields with many distinct values, where listing them is not useful.
    fn find_values_locked(
        &self,
        state: &State,
        (field, text, limit, scope): (&str, &str, usize, Option<&serde_json::Value>),
    ) -> Result<Vec<serde_json::Value>, ZenarError> {
        healthy(state)?;
        let (stats, _) = self.stats_for_scope(state, scope)?;
        let stat = stats.iter().find(|s| s.name == field).ok_or_else(|| {
            let names: Vec<&str> = stats.iter().map(|s| s.name.as_str()).collect();
            ZenarError::InvalidFilter(format!(
                "unknown field {field:?}; filterable fields: {}",
                names.join(", ")
            ))
        })?;
        let needle = text.to_lowercase();
        Ok(stat
            .values
            .iter()
            .filter(|(value, _)| crate::agent::value_text(value).to_lowercase().contains(&needle))
            .take(limit)
            .map(|(value, count)| {
                serde_json::json!({"value": crate::agent::value_json(value), "count": count})
            })
            .collect())
    }

    /// Fields and their values as plain text, for a prompt, within `scope`.
    fn fields_prompt_locked(
        &self,
        state: &State,
        scope: Option<&serde_json::Value>,
    ) -> Result<String, ZenarError> {
        healthy(state)?;
        let (stats, _) = self.stats_for_scope(state, scope)?;
        Ok(crate::agent::fields_prompt(&stats))
    }

    /// Tool definition for this collection, within `scope` (see `agent::tool_spec`).
    fn tool_spec_locked(
        &self,
        state: &State,
        (tool_name, scope): (&str, Option<&serde_json::Value>),
    ) -> Result<serde_json::Value, ZenarError> {
        healthy(state)?;
        let (stats, documents) = self.stats_for_scope(state, scope)?;
        Ok(crate::agent::tool_spec(
            tool_name, &self.name, documents, &stats,
        ))
    }

    /// The retrieval's parsing under the read lock: the model's `where` and the caller's `scope`, with the warnings
    /// about the model's values. The model's values and the warnings about them must come from inside the scope, or a
    /// warning could name another tenant's values. Statistics are only needed for the model's `where`, so they are
    /// computed only then.
    fn parse_retrieval(
        &self,
        state: &State,
        (where_, scope): (Option<&serde_json::Value>, Option<&serde_json::Value>),
    ) -> Result<ParsedRetrieval, ZenarError> {
        healthy(state)?;
        let fields = state.docs.fields();
        let (where_filter, warnings) = match where_ {
            None => (None, Vec::new()),
            Some(value) => {
                let (stats, _) = self.stats_for_scope(state, scope)?;
                let parsed = crate::agent::parse(value, fields, &stats)?;
                (parsed.filter, parsed.warnings)
            }
        };
        // The scope's own warnings are not reported, so its statistics are not needed.
        let scope_filter = match scope {
            None => None,
            Some(value) => crate::agent::parse(value, fields, &[])?.filter,
        };
        Ok((where_filter, scope_filter, warnings))
    }
}

impl Collection {
    pub(crate) fn new(
        name: &str,
        index_path: PathBuf,
        catalog: Arc<Catalog>,
        store: VectorStore,
        docs: DocStore,
        durable: bool,
    ) -> Self {
        let docs_len = docs.log_len();
        Self {
            core: Core {
                name: name.to_string(),
                stats: Mutex::new(None),
                scoped: Mutex::new(Vec::new()),
                index_path,
                catalog,
                durable,
                counters: WriteCounters::default(),
                #[cfg(test)]
                compaction_hook: Mutex::new(None),
                #[cfg(test)]
                fail_compaction_at: Mutex::new(None),
                #[cfg(test)]
                fail_stage: AtomicBool::new(false),
                #[cfg(test)]
                search_retries: AtomicU64::new(0),
            },
            queue: Mutex::new(Queue::default()),
            compaction: Mutex::new(CompactionSlot::default()),
            self_ref: std::sync::OnceLock::new(),
            ready: Condvar::new(),
            state: RwLock::new(State {
                closed: false,
                store,
                docs,
                unsynced: 0,
                committed_len: docs_len,
                pending: 0,
                poisoned: None,
            }),
            #[cfg(test)]
            window_hook: Mutex::new(None),
            #[cfg(test)]
            fail_log_sync: AtomicBool::new(false),
            admission: Admission::new(),
        }
    }
    pub fn name(&self) -> &str {
        &self.core.name
    }

    pub fn dimension(&self) -> Option<usize> {
        self.with_read((), Core::dimension_locked).unwrap_or(None)
    }
    pub fn bit_width(&self) -> usize {
        self.with_read((), Core::bit_width_locked).unwrap_or(0)
    }
    /// Adds one document. Concurrent writes to the collection are grouped: see `submit`.
    ///
    /// The document becomes visible only after the index and the log are synced, or, for an add that
    /// does not reach a sync boundary, after it is added in memory (docs/STORAGE.md).
    pub fn add(
        &self,
        id: u64,
        vector: &[f32],
        text: &str,
        metadata: Vec<(String, MetaValue)>,
    ) -> Result<(), ZenarError> {
        check_id(id)?;
        let record = Record {
            id,
            vector: vector.to_vec(),
            text: text.to_string(),
            metadata,
        };
        self.submit(Op::Add {
            records: vec![record],
            batch: false,
        })
        .map(|_| ())
    }

    /// Adds many documents atomically. Either every record is added or none is, for errors Zenar
    /// reports and for a process crash. After a power loss a prefix of the batch can survive
    /// (STORAGE.md, "What a failure leaves behind").
    ///
    /// The batch is validated before anything is written: ids, vectors, metadata, duplicates within
    /// the batch and against stored documents. It is then written, synced and committed with the
    /// other writes in its group. Any failure in the group rolls back every batch in it.
    pub fn add_many(&self, records: &[Record]) -> Result<usize, ZenarError> {
        if records.is_empty() {
            return Ok(0);
        }
        self.submit(Op::Add {
            records: records.to_vec(),
            batch: true,
        })
    }

    /// Deletes one document. The vector leaves the index and the index is synced before the delete
    /// is logged, so a deleted vector is gone from disk once `delete` returns (spec section 67).
    pub fn delete(&self, id: u64) -> Result<(), ZenarError> {
        self.submit(Op::Delete(vec![id])).map(|_| ())
    }

    /// Deletes many documents with one index sync and one log sync. Either all of them are
    /// deleted or none is: every id must exist and be unique, or nothing changes.
    pub fn delete_many(&self, ids: &[u64]) -> Result<usize, ZenarError> {
        if ids.is_empty() {
            return Ok(0);
        }
        self.submit(Op::Delete(ids.to_vec()))
    }

    /// Makes every add so far durable: the index and the document log are synced. Holds the write
    /// lock throughout, so readers wait for these two fsyncs.
    pub fn flush(&self) -> Result<(), ZenarError> {
        self.submit(Op::Flush).map(|_| ())
    }

    /// Fits the TQ+ calibration from a sample (spec section 88).
    pub fn calibrate(&self, sample: &[f32]) -> Result<(), ZenarError> {
        self.submit(Op::Calibrate(sample.to_vec())).map(|_| ())
    }

    /// Queues a write and returns its result. Writes are grouped: whichever writer finds no leader
    /// takes every queued write and runs them as one group (`lead`). Other writers wait until their
    /// own result is ready. A group runs one at a time, so its sync and rollback never overlap
    /// another group's. A write returns only after the sync that makes it durable, or after its
    /// rollback.
    fn submit(&self, op: Op) -> Result<usize, ZenarError> {
        let req = Arc::new(Request::new(op));
        let mut queue = self.queue.lock().map_err(|_| queue_poisoned())?;
        queue.seen.insert(thread::current().id(), Instant::now());
        queue.pending.push(Arc::clone(&req));
        // A leader may be lingering for writers. This one is what it waits for.
        self.ready.notify_all();
        loop {
            if let Some(result) = req.outcome.lock().map_err(|_| queue_poisoned())?.take() {
                return result;
            }
            if queue.leading {
                queue = self.ready.wait(queue).map_err(|_| queue_poisoned())?;
                continue;
            }
            queue.leading = true;
            let group = self.gather(queue)?;
            self.lead(group);
            queue = self.queue.lock().map_err(|_| queue_poisoned())?;
        }
    }

    /// Takes the queued writes for the next group. While the group holds fewer writes than there are
    /// recent writer threads, waits up to `GROUP_LINGER` for the rest. Returns with the queue
    /// unlocked, and the leader role held by the caller.
    fn gather(
        &self,
        mut queue: std::sync::MutexGuard<'_, Queue>,
    ) -> Result<Vec<Arc<Request>>, ZenarError> {
        let deadline = Instant::now() + GROUP_LINGER;
        let mut group = Vec::new();
        loop {
            group.append(&mut queue.pending);
            let now = Instant::now();
            queue
                .seen
                .retain(|_, last| now.duration_since(*last) < RECENT_WRITER);
            if group.len() >= queue.seen.len() || now >= deadline {
                drop(queue);
                return Ok(group);
            }
            let (guard, _) = self
                .ready
                .wait_timeout(queue, deadline - now)
                .map_err(|_| queue_poisoned())?;
            queue = guard;
        }
    }

    /// Runs one group in the order its writes were queued. Adds run together, one index sync and one
    /// log sync for all of them. Deletes, flushes and calibrations run alone, so that they see every
    /// add queued before them. The guard finishes any write left without a result and releases the
    /// leader role, even if this panics.
    fn lead(&self, group: Vec<Arc<Request>>) {
        let guard = Lead {
            collection: self,
            group: group.clone(),
        };
        let mut adds = Vec::new();
        for req in group {
            if matches!(req.op, Op::Add { .. }) {
                adds.push(req);
            } else {
                self.run_adds(std::mem::take(&mut adds));
                let result = self.run_other(&req.op);
                req.finish(result);
            }
        }
        self.run_adds(adds);
        drop(guard);
    }

    /// Runs a group of adds: validates and stages each write, then syncs and commits them together.
    fn run_adds(&self, reqs: Vec<Arc<Request>>) {
        if reqs.is_empty() {
            return;
        }
        let step = match self.with_write("add_stage", reqs, Core::stage_adds) {
            Ok(step) => step,
            Err((reqs, e)) => return finish_all(&reqs, &e),
        };
        let AddStep::Sync { batch, log } = step else {
            return;
        };
        // The lock is released while the log syncs, so readers run during its fsync.
        #[cfg(test)]
        self.run_window_hook();
        let synced = self.sync_log(&log);
        if let Err(((batch, _), e)) =
            self.with_write("add_commit", (batch, synced), Core::commit_synced)
        {
            finish_members(&batch, &e);
        }
    }
    /// Runs a delete, flush or calibration. Each takes the write lock for its own work.
    fn run_other(&self, op: &Op) -> Result<usize, ZenarError> {
        match op {
            Op::Delete(ids) => self.run_delete(ids),
            Op::Flush => self.flush_locked().map(|()| 0),
            Op::Calibrate(sample) => self.calibrate_locked(sample).map(|()| 0),
            Op::Compact => self.swap_queued().map(|()| 0),
            Op::Add { .. } => unreachable!("adds run in run_adds"),
        }
    }

    /// Checks the ids of a delete and deletes them. A missing id, or an id repeated in the batch,
    /// changes nothing.
    fn run_delete(&self, ids: &[u64]) -> Result<usize, ZenarError> {
        let step = self
            .with_write("delete_stage", ids, Core::stage_deletes)
            .map_err(|(_, e)| e)??;
        // The lock is released while the log syncs, as for adds.
        #[cfg(test)]
        self.run_window_hook();
        let synced = self.sync_log(&step.log);
        let (compact_due, committed) = self
            .with_write("delete_commit", (step.staged, synced), Core::commit_deletes)
            .map_err(|(_, e)| e)?;
        // The threshold is checked under the lock, and the compaction starts once the lock is released.
        if compact_due {
            self.start_compaction();
        }
        committed?;
        Ok(ids.len())
    }
    fn flush_locked(&self) -> Result<(), ZenarError> {
        self.with_write("flush", (), Core::flush_state)
            .map_err(|(_, e)| e)?
    }
    fn calibrate_locked(&self, sample: &[f32]) -> Result<(), ZenarError> {
        self.with_write("calibrate", sample, Core::calibrate_state)
            .map_err(|(_, e)| e)?
    }
    /// Syncs the log through a handle taken under the lock. Test builds can inject a failure here.
    fn sync_log(&self, log: &std::fs::File) -> Result<(), ZenarError> {
        #[cfg(test)]
        if self
            .fail_log_sync
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(ZenarError::PersistenceError(
                "injected log sync failure".into(),
            ));
        }
        log.sync_data().map_err(crate::docstore::io_err)
    }

    /// Test hook: runs in the window where the write lock is released and the log is syncing.
    #[cfg(test)]
    fn run_window_hook(&self) {
        let hook = self.window_hook.lock().ok().and_then(|h| h.clone());
        if let Some(hook) = hook {
            hook();
        }
    }

    /// Turns the lock trace on or off. While on, every exclusive hold of the state lock is recorded, with the
    /// operation that took it, its wait, its hold and the filesystem time timed inside it. Turning it on
    /// clears the trace. A diagnostic: it costs a lock and a push per hold.
    pub fn set_lock_trace(&self, on: bool) {
        if let Ok(mut trace) = self.core.counters.trace.lock() {
            trace.clear();
        }
        self.core.counters.tracing.store(on, Ordering::Relaxed);
    }

    /// Takes the events recorded since the trace was turned on, and empties the trace.
    pub fn take_lock_trace(&self) -> Vec<LockEvent> {
        match self.core.counters.trace.lock() {
            Ok(mut trace) => std::mem::take(&mut *trace),
            Err(_) => Vec::new(),
        }
    }

    /// Counters for the write path and the readers' waits for the state lock, cumulative since the
    /// collection was opened. A diagnostic: see BENCHMARKS.md, "Several writers".
    pub fn write_stats(&self) -> WriteStats {
        let c = &self.core.counters;
        let load = |a: &AtomicU64| a.load(Ordering::Relaxed);
        let mut group_writes = [0u64; GROUP_BUCKETS];
        for (out, bucket) in group_writes.iter_mut().zip(c.group_writes.iter()) {
            *out = load(bucket);
        }
        WriteStats {
            sync_groups: load(&c.sync_groups),
            group_writes,
            synced_writes: load(&c.synced_writes),
            write_holds: load(&c.holds),
            write_hold_total_ns: load(&c.hold_ns),
            write_hold_max_ns: load(&c.max_hold_ns),
            read_waits: load(&c.read_waits),
            read_wait_total_ns: load(&c.read_wait_ns),
            read_wait_max_ns: load(&c.max_read_wait_ns),
        }
    }

    pub fn get(&self, id: u64) -> Result<Option<DocumentRow>, ZenarError> {
        self.with_read(id, Core::get_locked)?
    }
    /// Returns found documents in the order of `ids`. Missing ids are skipped.
    pub fn get_many(&self, ids: &[u64]) -> Result<Vec<DocumentRow>, ZenarError> {
        self.with_read(ids, Core::get_many_locked)?
    }
    pub fn count(&self) -> Result<usize, ZenarError> {
        self.with_read((), Core::count_locked)?
    }
    /// Top-`top_k` search, optionally filtered (spec sections 18-22).
    ///
    /// * No filter: TurboVec only. Documents are read for the top-K alone.
    /// * Filter: the document log's indexes give the matching ids. Those are narrowed to ids
    ///   still in the index (spec section 89), and TurboVec searches only those.
    /// * Empty candidate set: returns without touching TurboVec (spec section 21).
    pub fn search(
        &self,
        query: &[f32],
        top_k: usize,
        filter: Option<&Filter>,
    ) -> Result<Vec<Hit>, ZenarError> {
        Ok(self.search_counted(query, top_k, filter)?.0)
    }

    /// Like `search`, and also returns how many documents matched the filter before `top_k`
    /// cut the list. A caller can tell "no match" apart from "more matches than requested".
    pub fn search_counted(
        &self,
        query: &[f32],
        top_k: usize,
        filter: Option<&Filter>,
    ) -> Result<(Vec<Hit>, usize), ZenarError> {
        if top_k == 0 {
            return Err(ZenarError::InvalidArgument(
                "top_k must be greater than 0".into(),
            ));
        }
        self.with_read((query, top_k, filter), Core::search_locked)?
    }

    pub fn describe(
        &self,
        scope: Option<&serde_json::Value>,
    ) -> Result<serde_json::Value, ZenarError> {
        self.with_read(scope, Core::describe_locked)?
    }
    /// Finds values of one field that contain `text` (case-insensitive), most frequent first,
    /// within `scope`. For fields with many distinct values, where listing them is not useful.
    pub fn find_values(
        &self,
        field: &str,
        text: &str,
        limit: usize,
        scope: Option<&serde_json::Value>,
    ) -> Result<Vec<serde_json::Value>, ZenarError> {
        self.with_read((field, text, limit, scope), Core::find_values_locked)?
    }
    /// Retrieval for an agent.
    ///
    /// `where_` is the model's filter. `scope` is the caller's fixed filter, such as the tenant.
    /// The two are ANDed on the server, so the model cannot widen the scope with `or` or `not`.
    /// Returns the hits, how many documents matched before `top_k`, and warnings about the model's
    /// values. Warnings about `scope` are not reported.
    pub fn retrieve(
        &self,
        query: &[f32],
        top_k: usize,
        where_: Option<&serde_json::Value>,
        scope: Option<&serde_json::Value>,
    ) -> Result<Retrieval, ZenarError> {
        // One read lock for parsing. It is released before the search takes its own.
        let (where_filter, scope_filter, warnings) =
            self.with_read((where_, scope), Core::parse_retrieval)??;
        let applied = match &where_filter {
            Some(f) => crate::agent::describe_filter(f),
            None => "no filter (all documents in scope)".to_string(),
        };
        let filter = match (scope_filter, where_filter) {
            (None, None) => None,
            (Some(scope), None) => Some(scope),
            (None, Some(model)) => Some(model),
            (Some(scope), Some(model)) => Some(Filter::And(vec![scope, model])),
        };
        let (hits, matched) = self.search_counted(query, top_k, filter.as_ref())?;
        Ok(Retrieval {
            hits,
            applied,
            matched,
            warnings,
        })
    }
    /// Fields and their values as plain text, for a prompt, within `scope`.
    pub fn fields_prompt(&self, scope: Option<&serde_json::Value>) -> Result<String, ZenarError> {
        self.with_read(scope, Core::fields_prompt_locked)?
    }
    /// Tool definition for this collection, within `scope` (see `agent::tool_spec`).
    pub fn tool_spec(
        &self,
        tool_name: &str,
        scope: Option<&serde_json::Value>,
    ) -> Result<serde_json::Value, ZenarError> {
        // Function-calling APIs accept only these names. Refusing a bad one here gives a clear error,
        // instead of a rejection from the model provider.
        let valid_name = !tool_name.is_empty()
            && tool_name.len() <= 64
            && tool_name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-');
        if !valid_name {
            return Err(ZenarError::InvalidArgument(format!(
                "tool name {tool_name:?} must be 1 to 64 letters, digits, '_' or '-'"
            )));
        }
        self.with_read((tool_name, scope), Core::tool_spec_locked)?
    }

    pub fn reconcile(&self) -> Result<RecoveryReport, ZenarError> {
        self.with_write("reconcile", (), Core::reconcile_state)
            .map_err(|(_, e)| e)?
    }
    /// Writes the current index file (used when a new collection is created with a dimension).
    pub(crate) fn persist(&self) -> Result<(), ZenarError> {
        self.with_write("persist", (), Core::persist_state)
            .map_err(|(_, e)| e)?
    }
    /// Starts a background build of the compacted structure, unless one is running. The build holds no lock
    /// while it works. Its result is swapped in by a queued `Op::Compact`, so no write group is in its window
    /// during the swap (docs/R3.md, section 4).
    fn start_compaction(&self) {
        let Some(me) = self.self_ref.get().and_then(std::sync::Weak::upgrade) else {
            return;
        };
        {
            let mut slot = me.compaction.lock().unwrap_or_else(|p| p.into_inner());
            if slot.running {
                return;
            }
            slot.running = true;
        }
        let runner = Arc::clone(&me);
        let handle = thread::spawn(move || {
            let _ = runner.run_compaction();
        });
        me.compaction
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .thread = Some(handle);
    }

    /// Builds the compacted structure, then queues its swap. Clears `running` when done.
    fn run_compaction(&self) -> Result<(), ZenarError> {
        let result = match self.build_compaction() {
            Ok(built) => {
                self.compaction
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .built = Some(built);
                self.submit(Op::Compact).map(|_| ())
            }
            Err(e) => Err(e),
        };
        let leftover = {
            let mut slot = self.compaction.lock().unwrap_or_else(|p| p.into_inner());
            slot.running = false;
            slot.built.take()
        };
        if let Some(built) = leftover {
            // The swap did not take the build: remove its temporary log.
            let tmp = built.tmp.clone();
            drop(built);
            let _ = fs::remove_file(tmp);
        }
        // Deletes made during a successful build may already have passed the threshold. Their delete found a build
        // running and did not start one, so start the next compaction here. A failed one is not retried here: it
        // would fail the same way and start again, without end. The next delete starts the next attempt.
        let due = result.is_ok()
            && self
                .with_read((), |_, state, ()| state.docs.should_compact())
                .is_ok_and(|due| due);
        if due {
            self.start_compaction();
        }
        result
    }

    /// Builds the compacted document structure without the collection lock (docs/R3.md, section 3):
    ///
    /// 1. snapshot the committed length `S` under the read lock;
    /// 2. write the live PUT frames below `S` to a temporary log, and replay it into a new structure;
    /// 3. catch up: copy the committed bytes after the last copied position into the temporary log, and apply
    ///    them to the new structure, in rounds, until the lag is small.
    ///
    /// Writes continue meanwhile. They append to the old log, and the swap copies whatever is left.
    fn build_compaction(&self) -> Result<Built, ZenarError> {
        let (path, fields, snapshot) = self.with_read((), Core::compaction_snapshot)??;
        let tmp = path.with_extension("log.compact");
        let _ = fs::remove_file(&tmp);
        match self.build_into(&path, &tmp, fields, snapshot) {
            Ok((docs, tail_from)) => Ok(Built {
                docs,
                tmp,
                tail_from,
            }),
            Err(e) => {
                // A failed build leaves nothing behind: the temporary log is never the log.
                let _ = fs::remove_file(&tmp);
                Err(e)
            }
        }
    }

    /// The steps of `build_compaction` that write the temporary log.
    fn build_into(
        &self,
        path: &Path,
        tmp: &Path,
        fields: Vec<(String, FieldKind)>,
        snapshot: u64,
    ) -> Result<(DocStore, u64), ZenarError> {
        self.core.compaction_point("build snapshot")?;
        DocStore::write_snapshot(path, snapshot, tmp)?;
        self.core.compaction_point("build temp synced")?;
        let mut docs = DocStore::open(tmp, fields)?;
        self.core.compaction_point("build replayed")?;
        let mut tail_from = snapshot;
        for _ in 0..CATCH_UP_ROUNDS {
            let watermark = self.with_read((), |_, state, ()| state.committed_len)?;
            if watermark.saturating_sub(tail_from) < CATCH_UP_BYTES {
                break;
            }
            let bytes = DocStore::read_log_range(path, tail_from, watermark)?;
            docs.append_raw(&bytes)?;
            tail_from = watermark;
            self.core.compaction_point("catch-up round")?;
        }
        self.core.compaction_point("build done")?;
        Ok((docs, tail_from))
    }

    /// Takes the built structure and swaps it in, as a queued operation. Nothing is built if none is waiting.
    fn swap_queued(&self) -> Result<(), ZenarError> {
        let built = self
            .compaction
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .built
            .take();
        let Some(built) = built else {
            return Ok(());
        };
        let old = self
            .with_write("swap", built, Core::swap_locked)
            .map_err(|(_, e)| e)??;
        // The old structure comes back out of the swap and is dropped here, after the lock is released, so
        // freeing its rows and postings does not add to the writers' wait.
        if let Some(old) = old {
            drop(old);
        }
        Ok(())
    }
    /// Starts a compaction and waits for it, on this thread. Test use only.
    #[cfg(test)]
    pub(crate) fn compact_now(&self) -> Result<(), ZenarError> {
        {
            let mut slot = self.compaction.lock().unwrap_or_else(|p| p.into_inner());
            if slot.running {
                return Err(ZenarError::InvalidArgument(
                    "a compaction is running".into(),
                ));
            }
            slot.running = true;
        }
        self.run_compaction()
    }

    /// Test only: writers announced and not yet past their `write()` call.
    #[cfg(test)]
    pub(crate) fn waiting_writers(&self) -> usize {
        self.admission.waiting_writers()
    }

    /// Test only: readers that waited at the admission gate.
    #[cfg(test)]
    pub(crate) fn gate_waits(&self) -> u64 {
        self.admission.gate_waits.load(Ordering::Relaxed)
    }

    /// Test only: writer announcements so far.
    #[cfg(test)]
    pub(crate) fn writer_announcements(&self) -> u64 {
        self.admission.total_announcements.load(Ordering::Relaxed)
    }

    /// Test only: how many unfiltered searches retried with the full over-fetch.
    #[cfg(test)]
    pub(crate) fn search_retry_count(&self) -> u64 {
        self.core.search_retries.load(Ordering::Relaxed)
    }

    /// Waits for a background compaction to finish. Test use only.
    #[cfg(test)]
    pub(crate) fn wait_for_compaction(&self) {
        let handle = self
            .compaction
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .thread
            .take();
        if let Some(handle) = handle {
            let _ = handle.join();
        }
    }

    /// Lets the database give this collection the reference a compaction thread needs.
    pub(crate) fn set_self_ref(self: &Arc<Self>) {
        let _ = self.self_ref.set(Arc::downgrade(self));
    }

    fn read_state(&self) -> Result<StateRead<'_>, ZenarError> {
        debug_assert_eq!(
            READS_HELD.with(|n| n.get()),
            0,
            "a state read was taken while this thread holds one; that deadlocks once a writer is waiting"
        );
        let waiting = Instant::now();
        self.admission.admit_reader();
        let guard = self
            .state
            .read()
            .map_err(|_| ZenarError::VectorStoreError("collection lock poisoned".into()))?;
        READS_HELD.with(|n| n.set(n.get() + 1));
        let guard = StateRead { guard };
        self.core.counters.record_read_wait(waiting.elapsed());
        self.not_closed(&guard)?;
        Ok(guard)
    }

    fn not_closed(&self, state: &State) -> Result<(), ZenarError> {
        if state.closed {
            return Err(ZenarError::CollectionClosed(self.core.name.clone()));
        }
        Ok(())
    }

    /// Marks the collection closed, so that no handle to it can read or write it any more.
    pub(crate) fn close(&self) {
        let announced = self.admission.announce();
        let mut state = match self.state.write() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        drop(announced);
        state.closed = true;
    }

    #[cfg(test)]
    fn write_state(&self) -> Result<WriteGuard<'_>, ZenarError> {
        self.write_state_op("other")
    }

    /// Takes the write lock for the operation `op`, and records the wait for it in the lock trace.
    fn write_state_op(&self, op: &'static str) -> Result<WriteGuard<'_>, ZenarError> {
        let waiting = Instant::now();
        let announced = self.admission.announce();
        let guard = self.state.write();
        drop(announced);
        let guard =
            guard.map_err(|_| ZenarError::VectorStoreError("collection lock poisoned".into()))?;
        let since = Instant::now();
        let waited_ns = u64::try_from((since - waiting).as_nanos()).unwrap_or(u64::MAX);
        self.not_closed(&guard)?;
        // Every mutation takes the write lock first, so clearing here means no reader can store
        // statistics computed before the mutation after it has been cleared.
        if let Ok(mut cache) = self.core.stats.lock() {
            *cache = None;
        }
        if let Ok(mut scoped) = self.core.scoped.lock() {
            scoped.clear();
        }
        Ok(WriteGuard {
            guard,
            since,
            counters: &self.core.counters,
            op,
            waited_ns,
            fs_ns: 0,
        })
    }

    /// Runs `f` on the state under a read guard. `f` is a function pointer, not a closure, so it gets only `Core`
    /// and the state. It cannot take the state lock again, and it cannot reach `Collection`.
    fn with_read<A, R>(&self, arg: A, f: fn(&Core, &State, A) -> R) -> Result<R, ZenarError> {
        let state = self.read_state()?;
        Ok(f(&self.core, &state, arg))
    }

    /// Runs `f` on the state under the write guard for `op`. A function pointer, as for `with_read`. When the
    /// lock cannot be taken, `arg` comes back with the error, so that the caller can finish what it holds.
    fn with_write<A, R>(
        &self,
        op: &'static str,
        arg: A,
        f: fn(&Core, &mut WriteGuard<'_>, A) -> R,
    ) -> Result<R, (A, ZenarError)> {
        let mut state = match self.write_state_op(op) {
            Ok(state) => state,
            Err(e) => return Err((arg, e)),
        };
        Ok(f(&self.core, &mut state, arg))
    }
}

impl Drop for Collection {
    /// A clean close syncs adds that are still unsynced. Errors cannot be reported from
    /// drop. If the sync fails, reconciliation on the next open repairs the state.
    fn drop(&mut self) {
        if let Ok(state) = self.state.get_mut()
            && state.poisoned.is_none()
            && state.unsynced > 0
        {
            let _ = state.store.sync(&self.core.index_path);
            let _ = state.docs.sync();
            state.unsynced = 0;
        }
    }
}

fn healthy(state: &State) -> Result<(), ZenarError> {
    match &state.poisoned {
        None => Ok(()),
        Some(reason) => Err(ZenarError::PersistenceError(format!(
            "collection is inconsistent after a failed write ({reason}); reopen the database to recover"
        ))),
    }
}

fn poison(state: &mut State, e: ZenarError) -> ZenarError {
    state.poisoned.get_or_insert_with(|| e.to_string());
    e
}

/// Truncates a staged write after a failure and returns `cause`. If the log cannot be truncated,
/// the staged bytes stay in it, and the next write would land after them. The collection is then
/// poisoned, so that only a reopen (which treats those bytes as unsynced documents) can continue.
fn undo_or_poison(
    state: &mut State,
    staged: crate::docstore::Staged,
    cause: ZenarError,
) -> ZenarError {
    if let Err(e) = state.docs.undo(staged) {
        poison(state, e);
    }
    cause
}

pub fn check_id(id: u64) -> Result<(), ZenarError> {
    if id > MAX_ID {
        return Err(ZenarError::InvalidArgument(format!(
            "id must be at most {MAX_ID}"
        )));
    }
    Ok(())
}

/// Validates metadata against the schema. An unlocked schema (no fields yet) takes the fields
/// of this document. Returns the newly introduced fields and the values to store.
fn resolve_metadata(
    schema: &[(String, FieldKind)],
    metadata: Vec<(String, MetaValue)>,
) -> Result<Resolved, ZenarError> {
    let mut new_fields: Vec<(String, FieldKind)> = Vec::new();
    let mut values: Vec<(String, MetaValue)> = Vec::with_capacity(metadata.len());
    let mut seen = HashSet::new();

    for (name, value) in metadata {
        if !is_valid_field_name(&name) {
            return Err(ZenarError::InvalidMetadata(format!(
                "invalid field name {name:?}"
            )));
        }
        if !seen.insert(name.clone()) {
            return Err(ZenarError::InvalidMetadata(format!(
                "duplicate field {name:?}"
            )));
        }
        if let MetaValue::Float(f) = value
            && !f.is_finite()
        {
            return Err(ZenarError::InvalidMetadata(format!(
                "field {name:?} must be finite"
            )));
        }

        let declared = schema
            .iter()
            .chain(new_fields.iter())
            .find(|(n, _)| *n == name)
            .map(|(_, k)| *k);
        let value = match declared {
            Some(kind) => coerce(&name, kind, value)?,
            // A new field name extends the schema, with the type of its first value. A field keeps
            // that type for the life of the collection (see `coerce`).
            None => {
                new_fields.push((name.clone(), FieldKind::of(&value)));
                value
            }
        };
        values.push((name, value));
    }
    Ok((new_fields, values))
}

/// Fields a document introduces, and its metadata as stored.
type Resolved = (Vec<(String, FieldKind)>, Vec<(String, MetaValue)>);

/// Coerces a value to a field's kind. Integers widen to floats. Nothing else converts.
fn coerce(name: &str, kind: FieldKind, value: MetaValue) -> Result<MetaValue, ZenarError> {
    match (kind, value) {
        (FieldKind::Float, MetaValue::Integer(i)) => Ok(MetaValue::Float(i as f64)),
        (kind, value) if FieldKind::of(&value) == kind => Ok(value),
        (kind, value) => Err(ZenarError::InvalidMetadata(format!(
            "field {name:?} is {}, got {}",
            kind.as_str(),
            FieldKind::of(&value).as_str()
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_metadata_defines_schema() {
        let (new, values) =
            resolve_metadata(&[], vec![("tenant".into(), MetaValue::Text("A".into()))]).unwrap();
        assert_eq!(new, vec![("tenant".to_string(), FieldKind::Text)]);
        assert_eq!(values.len(), 1);
    }

    #[test]
    fn unknown_field_extends_the_schema() {
        let schema = vec![("tenant".to_string(), FieldKind::Text)];
        let (new_fields, values) = resolve_metadata(
            &schema,
            vec![("color".into(), MetaValue::Text("red".into()))],
        )
        .unwrap();
        assert_eq!(new_fields, vec![("color".to_string(), FieldKind::Text)]);
        assert_eq!(values[0].0, "color");
    }

    #[test]
    fn a_new_field_keeps_the_type_of_its_first_value() {
        let schema = vec![("tenant".to_string(), FieldKind::Text)];
        let (new_fields, _) =
            resolve_metadata(&schema, vec![("price".into(), MetaValue::Float(9.5))]).unwrap();
        assert_eq!(new_fields, vec![("price".to_string(), FieldKind::Float)]);
    }

    #[test]
    fn integer_widens_to_float_field() {
        let schema = vec![("price".to_string(), FieldKind::Float)];
        let (_, values) =
            resolve_metadata(&schema, vec![("price".into(), MetaValue::Integer(150))]).unwrap();
        assert_eq!(values[0].1, MetaValue::Float(150.0));
    }

    #[test]
    fn wrong_type_rejected() {
        let schema = vec![("tenant".to_string(), FieldKind::Text)];
        let r = resolve_metadata(&schema, vec![("tenant".into(), MetaValue::Integer(1))]);
        assert!(matches!(r, Err(ZenarError::InvalidMetadata(_))));
    }

    #[test]
    fn nan_metadata_rejected() {
        let r = resolve_metadata(&[], vec![("score".into(), MetaValue::Float(f64::NAN))]);
        assert!(matches!(r, Err(ZenarError::InvalidMetadata(_))));
    }

    #[test]
    fn id_bounds() {
        assert!(check_id(MAX_ID).is_ok());
        assert!(check_id(MAX_ID + 1).is_err());
    }
}

#[cfg(test)]
mod recovery_tests {
    use super::*;
    use crate::database::VectorDB;
    use std::fs;
    use std::path::Path;
    use std::thread;

    fn temp_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("zenar-rec-{tag}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    fn v(seed: f32) -> Vec<f32> {
        (0..8).map(|i| seed + i as f32).collect()
    }

    /// A document made durable without its vector (a crash after the log sync, before the index
    /// sync) is removed on reopen.
    #[test]
    fn document_without_vector_is_removed_on_reopen() {
        let dir = temp_dir("orphan-doc");
        {
            let db = VectorDB::open(&dir).unwrap();
            let c = db.create_collection("od", Some(8), 4).unwrap();
            c.add(1, &v(1.0), "kept", vec![]).unwrap();
            db.flush().unwrap();
            // Simulate the crash window: the document is durable, the vector is not.
            let mut state = c.write_state().unwrap();
            let staged = state
                .docs
                .stage_puts(&[DocRef {
                    id: 2,
                    text: "orphan",
                    metadata: &[],
                }])
                .unwrap();
            state.docs.commit(staged);
            state.docs.sync().unwrap();
        }
        let db = VectorDB::open(&dir).unwrap();
        assert_eq!(db.recovery_report().orphaned_rows_removed, 1);
        let c = db.get_collection("od").unwrap();
        assert!(c.get(2).unwrap().is_none());
        assert!(c.get(1).unwrap().is_some());
        assert_eq!(c.count().unwrap(), 1);
        fs::remove_dir_all(&dir).ok();
    }

    /// A vector made durable without its document is removed on reopen.
    #[test]
    fn vector_without_document_is_removed_on_reopen() {
        let dir = temp_dir("stray-vec");
        {
            let db = VectorDB::open(&dir).unwrap();
            let c = db.create_collection("sv", Some(8), 4).unwrap();
            c.add(1, &v(1.0), "kept", vec![]).unwrap();
            db.flush().unwrap();
            let mut state = c.write_state().unwrap();
            state.store.add(&v(2.0), 2).unwrap();
            state.store.sync(&c.core.index_path).unwrap();
        }
        let db = VectorDB::open(&dir).unwrap();
        assert_eq!(db.recovery_report().orphaned_vectors_removed, 1);
        let c = db.get_collection("sv").unwrap();
        let hits = c.search(&v(2.0), 5, None).unwrap();
        assert!(hits.iter().all(|h| h.id != 2));
        fs::remove_dir_all(&dir).ok();
    }

    /// Once an in-memory state cannot be made durable or undone, every operation fails until reopen.
    #[test]
    fn poisoned_collection_refuses_every_operation() {
        let dir = temp_dir("poison");
        let db = VectorDB::open(&dir).unwrap();
        let c = db.create_collection("p", Some(8), 4).unwrap();
        c.add(1, &v(1.0), "x", vec![]).unwrap();
        db.flush().unwrap();
        c.write_state().unwrap().poisoned = Some("test".into());
        assert!(matches!(
            c.add(2, &v(2.0), "y", vec![]),
            Err(ZenarError::PersistenceError(_))
        ));
        assert!(matches!(c.get(1), Err(ZenarError::PersistenceError(_))));
        assert!(matches!(c.delete(1), Err(ZenarError::PersistenceError(_))));
        assert!(matches!(
            c.search(&v(1.0), 1, None),
            Err(ZenarError::PersistenceError(_))
        ));
        drop(c);
        drop(db);
        // Reopening replays the durable state and clears the poison.
        let db = VectorDB::open(&dir).unwrap();
        assert_eq!(db.get_collection("p").unwrap().count().unwrap(), 1);
        fs::remove_dir_all(&dir).ok();
    }

    /// Makes the index file and its directory refuse writes. An incremental sync appends to the
    /// file, and a full rewrite creates a temporary file next to it, so both fail.
    #[cfg(unix)]
    fn fail_index_syncs(index: &Path, failing: bool) {
        use std::os::unix::fs::PermissionsExt;
        let (file_mode, dir_mode) = if failing {
            (0o444, 0o555)
        } else {
            (0o644, 0o755)
        };
        fs::set_permissions(index, fs::Permissions::from_mode(file_mode)).unwrap();
        fs::set_permissions(
            index.parent().unwrap(),
            fs::Permissions::from_mode(dir_mode),
        )
        .unwrap();
    }

    /// The fault tests need a file that refuses writes. Root is not refused, so fail loudly
    /// instead of passing without injecting a fault.
    #[cfg(unix)]
    fn assert_index_refuses_writes(index: &Path) {
        assert!(
            fs::OpenOptions::new().append(true).open(index).is_err(),
            "the fault tests need an index file that refuses writes (is this running as root?)"
        );
    }

    /// A delete whose index sync fails poisons the collection. The tombstone was never written,
    /// so reopening keeps the document and its vector.
    #[cfg(unix)]
    #[test]
    fn failed_delete_poisons_and_reopen_keeps_the_document() {
        let dir = temp_dir("fail-delete");
        let index = dir.join("vectors").join("fd.zvec");
        {
            let db = VectorDB::open(&dir).unwrap();
            let c = db.create_collection("fd", Some(8), 4).unwrap();
            c.add(1, &v(1.0), "one", vec![]).unwrap();
            c.add(2, &v(2.0), "two", vec![]).unwrap();
            db.flush().unwrap();

            fail_index_syncs(&index, true);
            assert_index_refuses_writes(&index);
            assert!(c.delete(1).is_err());
            fail_index_syncs(&index, false);

            assert!(matches!(c.get(2), Err(ZenarError::PersistenceError(_))));
            assert!(matches!(c.delete(2), Err(ZenarError::PersistenceError(_))));
        }
        let db = VectorDB::open(&dir).unwrap();
        assert!(db.recovery_report().is_clean());
        let c = db.get_collection("fd").unwrap();
        assert_eq!(c.count().unwrap(), 2);
        assert!(c.get(1).unwrap().is_some());
        fs::remove_dir_all(&dir).ok();
    }

    /// A durable add whose index sync fails is rolled back. The rollback cannot sync the index either,
    /// so the collection is poisoned: it refuses reads and writes until it is reopened. The reopen
    /// finds the failed document nowhere.
    #[cfg(unix)]
    #[test]
    fn failed_add_with_failing_index_poisons_and_reopen_leaves_no_trace() {
        let dir = temp_dir("fail-add");
        let index = dir.join("vectors").join("fa.zvec");
        {
            let db = VectorDB::open_with(&dir, crate::database::MetadataSync::Full).unwrap();
            let c = db.create_collection("fa", Some(8), 4).unwrap();
            c.add(1, &v(1.0), "one", vec![]).unwrap();

            fail_index_syncs(&index, true);
            assert_index_refuses_writes(&index);
            assert!(c.add(2, &v(2.0), "two", vec![]).is_err());
            assert!(matches!(c.get(1), Err(ZenarError::PersistenceError(_))));
            assert!(matches!(c.count(), Err(ZenarError::PersistenceError(_))));
            fail_index_syncs(&index, false);
        }
        let db = VectorDB::open(&dir).unwrap();
        let c = db.get_collection("fa").unwrap();
        assert_eq!(c.count().unwrap(), 1);
        assert!(c.get(2).unwrap().is_none());
        c.add(3, &v(3.0), "three", vec![]).unwrap();
        drop(c);
        drop(db);
        let db = VectorDB::open(&dir).unwrap();
        assert_eq!(db.get_collection("fa").unwrap().count().unwrap(), 2);
        fs::remove_dir_all(&dir).ok();
    }

    /// A batch whose index sync fails is rolled back, and the rollback cannot sync the index. The
    /// collection is poisoned. After a reopen none of the batch's ids is stored, and they can be added.
    #[cfg(unix)]
    #[test]
    fn failed_add_many_with_failing_index_poisons_and_reopen_is_all_or_nothing() {
        let dir = temp_dir("fail-batch");
        let index = dir.join("vectors").join("fb.zvec");
        let batch = || {
            vec![
                Record {
                    id: 2,
                    vector: v(2.0),
                    text: "two".into(),
                    metadata: vec![],
                },
                Record {
                    id: 3,
                    vector: v(3.0),
                    text: "three".into(),
                    metadata: vec![],
                },
            ]
        };
        {
            let db = VectorDB::open(&dir).unwrap();
            let c = db.create_collection("fb", Some(8), 4).unwrap();
            c.add(1, &v(1.0), "one", vec![]).unwrap();
            db.flush().unwrap();

            fail_index_syncs(&index, true);
            assert_index_refuses_writes(&index);
            assert!(c.add_many(&batch()).is_err());
            assert!(matches!(c.count(), Err(ZenarError::PersistenceError(_))));
            fail_index_syncs(&index, false);
        }
        let db = VectorDB::open(&dir).unwrap();
        assert_eq!(db.get_collection("fb").unwrap().count().unwrap(), 1);
        let c = db.get_collection("fb").unwrap();
        assert_eq!(c.add_many(&batch()).unwrap(), 2);
        drop(c);
        drop(db);
        let db = VectorDB::open(&dir).unwrap();
        assert_eq!(db.get_collection("fb").unwrap().count().unwrap(), 3);
        fs::remove_dir_all(&dir).ok();
    }

    /// A vector index that has disappeared after a sync is refused on open. Recreating it empty
    /// would make every document an orphan and remove it.
    #[test]
    fn missing_vector_index_is_refused_without_changes() {
        let dir = temp_dir("missing-index");
        {
            let db = VectorDB::open(&dir).unwrap();
            let c = db.create_collection("mi", Some(8), 4).unwrap();
            c.add(1, &v(1.0), "one", vec![]).unwrap();
            c.add(2, &v(2.0), "two", vec![]).unwrap();
            db.flush().unwrap();
        }
        let docs = dir.join("docs").join("mi.zdocs");
        let before = fs::read(&docs).unwrap();
        fs::remove_file(dir.join("vectors").join("mi.zvec")).unwrap();
        assert!(matches!(
            VectorDB::open(&dir).err(),
            Some(ZenarError::RecoveryError(_))
        ));
        assert_eq!(fs::read(&docs).unwrap(), before);
        fs::remove_dir_all(&dir).ok();
    }

    /// A document log that has disappeared after a sync is refused, and not recreated empty.
    #[test]
    fn missing_document_log_is_refused_and_not_recreated() {
        let dir = temp_dir("missing-log");
        {
            let db = VectorDB::open(&dir).unwrap();
            let c = db.create_collection("ml", Some(8), 4).unwrap();
            c.add(1, &v(1.0), "one", vec![]).unwrap();
            db.flush().unwrap();
        }
        let docs = dir.join("docs").join("ml.zdocs");
        fs::remove_file(&docs).unwrap();
        assert!(matches!(
            VectorDB::open(&dir).err(),
            Some(ZenarError::RecoveryError(_))
        ));
        assert!(!docs.exists());
        fs::remove_dir_all(&dir).ok();
    }

    /// A synced log cut short is refused before the open truncates anything, so the file is left
    /// as it was found.
    #[test]
    fn log_cut_short_after_sync_is_refused_without_changing_the_file() {
        let dir = temp_dir("cut-log");
        {
            let db = VectorDB::open(&dir).unwrap();
            let c = db.create_collection("cl", Some(8), 4).unwrap();
            c.add(1, &v(1.0), "one", vec![]).unwrap();
            c.add(2, &v(2.0), "two", vec![]).unwrap();
            db.flush().unwrap();
        }
        let docs = dir.join("docs").join("cl.zdocs");
        let len = fs::metadata(&docs).unwrap().len();
        fs::OpenOptions::new()
            .write(true)
            .open(&docs)
            .unwrap()
            .set_len(len - 5)
            .unwrap();
        let cut = fs::read(&docs).unwrap();
        assert!(matches!(
            VectorDB::open(&dir).err(),
            Some(ZenarError::RecoveryError(_))
        ));
        assert_eq!(fs::read(&docs).unwrap(), cut);
        fs::remove_dir_all(&dir).ok();
    }

    /// A synced record damaged at the end of the log is not mistaken for a torn tail: the open
    /// is refused and the damaged bytes are kept.
    #[test]
    fn damaged_synced_record_is_refused_without_truncation() {
        let dir = temp_dir("damaged-synced");
        {
            let db = VectorDB::open(&dir).unwrap();
            let c = db.create_collection("ds", Some(8), 4).unwrap();
            c.add(1, &v(1.0), "one", vec![]).unwrap();
            c.add(2, &v(2.0), "two", vec![]).unwrap();
            db.flush().unwrap();
        }
        let docs = dir.join("docs").join("ds.zdocs");
        let mut bytes = fs::read(&docs).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        fs::write(&docs, &bytes).unwrap();
        assert!(matches!(
            VectorDB::open(&dir).err(),
            Some(ZenarError::RecoveryError(_))
        ));
        assert_eq!(fs::read(&docs).unwrap(), bytes);
        fs::remove_dir_all(&dir).ok();
    }

    /// If a failed add cannot be truncated out of the log, its bytes stay there. The collection is
    /// poisoned, so no later add can land after them. On reopen they are unsynced documents and
    /// are removed, and the same id can be added again without the log holding two live copies.
    #[cfg(unix)]
    #[test]
    fn failed_undo_poisons_and_reopen_removes_the_orphan_frames() {
        let dir = temp_dir("failed-undo");
        let index = dir.join("vectors").join("fu.zvec");
        {
            let db = VectorDB::open_with(&dir, crate::database::MetadataSync::Full).unwrap();
            let c = db.create_collection("fu", Some(8), 4).unwrap();
            c.add(1, &v(1.0), "one", vec![]).unwrap();

            fail_index_syncs(&index, true);
            assert_index_refuses_writes(&index);
            c.write_state().unwrap().docs.fail_undo = true;
            assert!(c.add(2, &v(2.0), "two", vec![]).is_err());
            fail_index_syncs(&index, false);

            assert!(matches!(c.get(1), Err(ZenarError::PersistenceError(_))));
        }
        let db = VectorDB::open(&dir).unwrap();
        assert_eq!(db.recovery_report().orphaned_rows_removed, 1);
        let c = db.get_collection("fu").unwrap();
        assert_eq!(c.count().unwrap(), 1);
        c.add(2, &v(2.0), "two again", vec![]).unwrap();
        drop(c);
        drop(db);
        let db = VectorDB::open(&dir).unwrap();
        assert_eq!(db.get_collection("fu").unwrap().count().unwrap(), 2);
        fs::remove_dir_all(&dir).ok();
    }

    /// A crash after a delete clears the recorded lengths and before it records them again must
    /// not hide a missing index. The open is refused, and the documents are not removed.
    #[test]
    fn crash_during_delete_does_not_hide_a_missing_index() {
        let dir = temp_dir("crash-in-delete");
        {
            let db = VectorDB::open(&dir).unwrap();
            let c = db.create_collection("cd", Some(8), 4).unwrap();
            c.add(1, &v(1.0), "one", vec![]).unwrap();
            c.add(2, &v(2.0), "two", vec![]).unwrap();
            db.flush().unwrap();
            // The window a delete opens, then a crash: poisoning keeps the drop from recording again.
            c.core.lower_recorded_lengths(1, false).unwrap();
            c.write_state().unwrap().poisoned = Some("simulated crash".into());
        }
        fs::remove_file(dir.join("vectors").join("cd.zvec")).unwrap();
        assert!(matches!(
            VectorDB::open(&dir).err(),
            Some(ZenarError::RecoveryError(_))
        ));
        fs::remove_dir_all(&dir).ok();
    }

    /// Releases a writer parked in the window when dropped. A failing assertion unwinds through
    /// it, so the writer is never left waiting on a barrier that the test will not reach.
    struct Release<'a>(&'a std::sync::Barrier);

    impl Drop for Release<'_> {
        fn drop(&mut self) {
            self.0.wait();
        }
    }

    /// Sets the hook that runs while a write is between its index sync and its commit.
    fn set_window_hook(c: &Collection, hook: impl Fn() + Send + Sync + 'static) {
        *c.window_hook.lock().unwrap() = Some(Arc::new(hook));
    }

    /// A vector with `x` in the first coordinate, so that a query along that axis ranks by `x`.
    fn along(x: f32, i: u64) -> Vec<f32> {
        let mut out = vec![0.0f32; 8];
        out[0] = x + 0.001 * i as f32;
        out
    }

    /// Ten committed documents, then a batch of twenty added in the window. The batch ranks first for
    /// the query. A search during the window must still return five committed results.
    #[test]
    fn search_in_window_returns_full_top_k_from_committed_documents() {
        let dir = temp_dir("window-topk");
        let db = VectorDB::open(&dir).unwrap();
        let c = db.create_collection("wt", Some(8), 4).unwrap();
        let committed: Vec<Record> = (0..10)
            .map(|i| Record {
                id: i,
                vector: along(0.1, i),
                text: format!("committed {i}"),
                metadata: vec![],
            })
            .collect();
        c.add_many(&committed).unwrap();

        let pending: Vec<Record> = (100..120)
            .map(|i| Record {
                id: i,
                vector: along(1.0, i),
                text: format!("pending {i}"),
                metadata: vec![],
            })
            .collect();
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let inside = Arc::clone(&barrier);
        set_window_hook(&c, move || {
            inside.wait();
            inside.wait();
        });
        let writer = {
            let c = Arc::clone(&c);
            thread::spawn(move || c.add_many(&pending).unwrap())
        };
        barrier.wait();
        let release = Release(&barrier);
        let query = along(1.0, 0);
        let hits = c.search(&query, 5, None).unwrap();
        assert_eq!(
            hits.len(),
            5,
            "uncommitted vectors must not shrink the result"
        );
        assert!(hits.iter().all(|h| h.id < 10), "{hits:?}");
        assert!(hits.iter().all(|h| c.get(h.id).unwrap().is_some()));
        drop(release);
        assert_eq!(writer.join().unwrap(), 20);

        *c.window_hook.lock().unwrap() = None;
        let after = c.search(&query, 5, None).unwrap();
        assert!(after.iter().all(|h| h.id >= 100), "{after:?}");
        assert_eq!(c.count().unwrap(), 30);
        fs::remove_dir_all(&dir).ok();
    }

    /// A filtered search in the window only sees committed documents, whatever the filter matches.
    #[test]
    fn filtered_search_in_window_never_returns_uncommitted_documents() {
        use crate::filter::Filter;
        use crate::value::MetaValue;
        let dir = temp_dir("window-filter");
        let db = VectorDB::open(&dir).unwrap();
        let c = db.create_collection("wf", Some(8), 4).unwrap();
        let committed: Vec<Record> = (0..4)
            .map(|i| Record {
                id: i,
                vector: along(0.1, i),
                text: "a".into(),
                metadata: vec![("tenant".into(), MetaValue::Text("A".into()))],
            })
            .collect();
        c.add_many(&committed).unwrap();
        let pending: Vec<Record> = (100..110)
            .map(|i| Record {
                id: i,
                vector: along(1.0, i),
                text: "b".into(),
                metadata: vec![("tenant".into(), MetaValue::Text("A".into()))],
            })
            .collect();
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let inside = Arc::clone(&barrier);
        set_window_hook(&c, move || {
            inside.wait();
            inside.wait();
        });
        let writer = {
            let c = Arc::clone(&c);
            thread::spawn(move || c.add_many(&pending).unwrap())
        };
        barrier.wait();
        let release = Release(&barrier);
        let filter = Filter::Eq {
            field: "tenant".into(),
            value: MetaValue::Text("A".into()),
        };
        let hits = c.search(&along(1.0, 0), 10, Some(&filter)).unwrap();
        assert_eq!(hits.len(), 4);
        assert!(hits.iter().all(|h| h.id < 4), "{hits:?}");
        drop(release);
        writer.join().unwrap();
        fs::remove_dir_all(&dir).ok();
    }

    /// A delete that has removed its vector and is syncing its log: the removed vector is not
    /// returned, and the top results are still full.
    #[test]
    fn search_in_delete_window_skips_the_removed_vector_and_stays_full() {
        let dir = temp_dir("window-delete");
        let db = VectorDB::open(&dir).unwrap();
        let c = db.create_collection("wd", Some(8), 4).unwrap();
        let committed: Vec<Record> = (0..10)
            .map(|i| Record {
                id: i,
                vector: along(0.1 * (10 - i) as f32, i),
                text: "x".into(),
                metadata: vec![],
            })
            .collect();
        c.add_many(&committed).unwrap();
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let inside = Arc::clone(&barrier);
        set_window_hook(&c, move || {
            inside.wait();
            inside.wait();
        });
        let writer = {
            let c = Arc::clone(&c);
            thread::spawn(move || c.delete(0).unwrap())
        };
        barrier.wait();
        let release = Release(&barrier);
        let hits = c.search(&along(1.0, 0), 5, None).unwrap();
        assert_eq!(hits.len(), 5);
        assert!(hits.iter().all(|h| h.id != 0), "{hits:?}");
        drop(release);
        writer.join().unwrap();
        assert!(c.get(0).unwrap().is_none());
        fs::remove_dir_all(&dir).ok();
    }

    /// A log sync that fails in the window rolls the batch back: nothing is visible, the collection
    /// stays usable, and a reopen does not see the batch.
    #[cfg(unix)]
    #[test]
    fn failed_log_sync_in_window_rolls_back_and_stays_usable() {
        let dir = temp_dir("window-fail-add");
        {
            let db = VectorDB::open_with(&dir, crate::database::MetadataSync::Full).unwrap();
            let c = db.create_collection("fw", Some(8), 4).unwrap();
            c.add(1, &along(0.5, 1), "one", vec![]).unwrap();
            c.fail_log_sync
                .store(true, std::sync::atomic::Ordering::SeqCst);
            assert!(c.add(2, &along(0.9, 2), "two", vec![]).is_err());
            assert!(c.get(2).unwrap().is_none());
            assert_eq!(c.count().unwrap(), 1);
            let hits = c.search(&along(1.0, 0), 5, None).unwrap();
            assert!(hits.iter().all(|h| h.id != 2), "{hits:?}");
            c.add(3, &along(0.7, 3), "three", vec![]).unwrap();
        }
        let db = VectorDB::open(&dir).unwrap();
        let c = db.get_collection("fw").unwrap();
        assert_eq!(c.count().unwrap(), 2);
        assert!(c.get(2).unwrap().is_none());
        assert!(c.get(3).unwrap().is_some());
        fs::remove_dir_all(&dir).ok();
    }

    /// A log sync that fails after a delete has removed the vector poisons the collection, as before.
    /// Reopening then finds the document without its vector and removes it.
    #[cfg(unix)]
    #[test]
    fn failed_log_sync_in_delete_window_poisons_and_reopen_completes_the_delete() {
        let dir = temp_dir("window-fail-delete");
        {
            let db = VectorDB::open_with(&dir, crate::database::MetadataSync::Full).unwrap();
            let c = db.create_collection("fd2", Some(8), 4).unwrap();
            c.add(1, &along(0.5, 1), "one", vec![]).unwrap();
            c.add(2, &along(0.6, 2), "two", vec![]).unwrap();
            c.fail_log_sync
                .store(true, std::sync::atomic::Ordering::SeqCst);
            assert!(c.delete(1).is_err());
            assert!(matches!(c.get(2), Err(ZenarError::PersistenceError(_))));
        }
        let db = VectorDB::open(&dir).unwrap();
        let c = db.get_collection("fd2").unwrap();
        assert_eq!(c.count().unwrap(), 1);
        assert!(c.get(1).unwrap().is_none());
        fs::remove_dir_all(&dir).ok();
    }

    /// Concurrent writers and readers on one collection. Every committed document stays findable,
    /// no result repeats an id, and every search returns as many results as the committed documents
    /// it could rank, up to `top_k`.
    #[test]
    fn concurrent_inserts_and_searches_return_committed_full_results() {
        use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
        let dir = temp_dir("concurrent");
        let db = VectorDB::open(&dir).unwrap();
        let c = db.create_collection("cc", Some(8), 4).unwrap();
        c.add_many(
            &(0..50)
                .map(|i| Record {
                    id: 1_000 + i,
                    vector: along(0.2, i),
                    text: "seed".into(),
                    metadata: vec![],
                })
                .collect::<Vec<_>>(),
        )
        .unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let next_id = Arc::new(AtomicU64::new(0));
        let mut writers = Vec::new();
        for _ in 0..3 {
            let (c, next_id) = (Arc::clone(&c), Arc::clone(&next_id));
            writers.push(thread::spawn(move || {
                for _ in 0..60 {
                    let first = next_id.fetch_add(5, Ordering::SeqCst);
                    let batch: Vec<Record> = (first..first + 5)
                        .map(|i| Record {
                            id: i,
                            vector: along(1.0 - (i % 7) as f32 * 0.05, i),
                            text: "w".into(),
                            metadata: vec![],
                        })
                        .collect();
                    c.add_many(&batch).unwrap();
                }
            }));
        }
        let mut readers = Vec::new();
        for r in 0..3 {
            let (c, stop) = (Arc::clone(&c), Arc::clone(&stop));
            readers.push(thread::spawn(move || {
                let mut searches = 0usize;
                while !stop.load(Ordering::SeqCst) {
                    let before = c.count().unwrap();
                    let query = along(1.0, r as u64);
                    let hits = c.search(&query, 10, None).unwrap();
                    let expected = 10.min(before);
                    assert!(
                        hits.len() >= expected,
                        "{} results, expected at least {expected}",
                        hits.len()
                    );
                    let mut ids: Vec<u64> = hits.iter().map(|h| h.id).collect();
                    ids.sort_unstable();
                    ids.dedup();
                    assert_eq!(ids.len(), hits.len(), "repeated id in results");
                    for h in &hits {
                        assert!(
                            c.get(h.id).unwrap().is_some(),
                            "result {} is not committed",
                            h.id
                        );
                    }
                    searches += 1;
                }
                searches
            }));
        }
        for w in writers {
            w.join().unwrap();
        }
        stop.store(true, Ordering::SeqCst);
        let searches: usize = readers.into_iter().map(|h| h.join().unwrap()).sum();
        assert!(searches > 0);
        assert_eq!(c.count().unwrap(), 50 + 3 * 60 * 5);
        fs::remove_dir_all(&dir).ok();
    }

    /// Writes queued behind a leader run as one group. Here two `add_many` calls queue while the
    /// first group is in its window, then run together, and a log-sync failure rolls back both.
    /// The first group, already committed, is kept, and the collection stays usable.
    #[cfg(unix)]
    #[test]
    fn group_that_fails_its_log_sync_rolls_back_every_member() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let dir = temp_dir("group-fail");
        let db = VectorDB::open_with(&dir, crate::database::MetadataSync::Full).unwrap();
        let c = db.create_collection("gf", Some(8), 4).unwrap();
        c.add(0, &along(0.5, 0), "seed", vec![]).unwrap();

        let batch = |first: u64| -> Vec<Record> {
            (first..first + 3)
                .map(|i| Record {
                    id: i,
                    vector: along(0.8, i),
                    text: "g".into(),
                    metadata: vec![],
                })
                .collect()
        };
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let inside = Arc::clone(&barrier);
        let calls = AtomicUsize::new(0);
        // Call 0 parks the first group in its window. Call 1 is the window of the grouped
        // writes, and makes their log sync fail. Holding a handle to the collection in the hook
        // leaks it at the end of the test, which is harmless here.
        let handle = Arc::clone(&c);
        set_window_hook(&c, move || match calls.fetch_add(1, Ordering::SeqCst) {
            0 => {
                inside.wait();
                inside.wait();
            }
            1 => handle.fail_log_sync.store(true, Ordering::SeqCst),
            _ => {}
        });

        let first = {
            let c = Arc::clone(&c);
            thread::spawn(move || c.add_many(&batch(100)))
        };
        barrier.wait();
        let release = Release(&barrier);
        let second = {
            let c = Arc::clone(&c);
            thread::spawn(move || c.add_many(&batch(200)))
        };
        let third = {
            let c = Arc::clone(&c);
            thread::spawn(move || c.add_many(&batch(300)))
        };
        while c.queue.lock().unwrap().pending.len() < 2 {
            thread::yield_now();
        }
        drop(release);

        assert_eq!(first.join().unwrap().unwrap(), 3);
        assert!(second.join().unwrap().is_err());
        assert!(third.join().unwrap().is_err());
        assert_eq!(c.count().unwrap(), 4, "only the first group is committed");
        assert!(c.get(200).unwrap().is_none());
        assert!(c.get(300).unwrap().is_none());

        c.add_many(&batch(400)).unwrap();
        assert_eq!(c.count().unwrap(), 7);
        drop(c);
        drop(db);
        let db = VectorDB::open(&dir).unwrap();
        assert_eq!(db.get_collection("gf").unwrap().count().unwrap(), 7);
        fs::remove_dir_all(&dir).ok();
    }

    /// In one group, a rejected member gets its own error and the others commit. Here the second
    /// batch repeats an id the first batch of the group added.
    #[test]
    fn grouped_writes_fail_individually_and_the_rest_commit() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let dir = temp_dir("group-reject");
        let db = VectorDB::open(&dir).unwrap();
        let c = db.create_collection("gr", Some(8), 4).unwrap();
        c.add(0, &along(0.5, 0), "seed", vec![]).unwrap();

        let batch = |ids: &[u64]| -> Vec<Record> {
            ids.iter()
                .map(|&i| Record {
                    id: i,
                    vector: along(0.8, i),
                    text: "g".into(),
                    metadata: vec![],
                })
                .collect()
        };
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let inside = Arc::clone(&barrier);
        let calls = AtomicUsize::new(0);
        set_window_hook(&c, move || {
            if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                inside.wait();
                inside.wait();
            }
        });
        let first = {
            let c = Arc::clone(&c);
            thread::spawn(move || c.add_many(&batch(&[100])))
        };
        barrier.wait();
        let release = Release(&barrier);
        let second = {
            let c = Arc::clone(&c);
            thread::spawn(move || c.add_many(&batch(&[200, 201])))
        };
        let third = {
            let c = Arc::clone(&c);
            thread::spawn(move || c.add_many(&batch(&[201, 202])))
        };
        while c.queue.lock().unwrap().pending.len() < 2 {
            thread::yield_now();
        }
        drop(release);

        assert_eq!(first.join().unwrap().unwrap(), 1);
        let second = second.join().unwrap();
        let third = third.join().unwrap();
        // Both queued batches are in one group. Whichever is staged second repeats id 201.
        assert!(
            matches!(second, Ok(2)) ^ matches!(third, Ok(2)),
            "exactly one of the two may commit: {second:?} {third:?}"
        );
        let failed = if second.is_err() { second } else { third };
        assert!(
            matches!(failed, Err(ZenarError::DuplicateId(201))),
            "{failed:?}"
        );
        assert_eq!(c.count().unwrap(), 4);
        fs::remove_dir_all(&dir).ok();
    }

    /// Copies a directory tree, as a crash would leave it on disk.
    fn copy_tree(src: &Path, dst: &Path) {
        fs::create_dir_all(dst).unwrap();
        for entry in fs::read_dir(src).unwrap() {
            let entry = entry.unwrap();
            let target = dst.join(entry.file_name());
            if entry.file_type().unwrap().is_dir() {
                copy_tree(&entry.path(), &target);
            } else {
                fs::copy(entry.path(), target).unwrap();
            }
        }
    }

    /// A pending document, added in the window, is invisible to `get`, `count` and every kind of search.
    #[test]
    fn pending_documents_are_invisible_to_get_count_and_search() {
        let dir = temp_dir("pending-invisible");
        let db = VectorDB::open(&dir).unwrap();
        let c = db.create_collection("pi", Some(8), 4).unwrap();
        c.add_many(
            &(0..5)
                .map(|i| Record {
                    id: i,
                    vector: along(0.1, i),
                    text: "c".into(),
                    metadata: vec![],
                })
                .collect::<Vec<_>>(),
        )
        .unwrap();
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let inside = Arc::clone(&barrier);
        set_window_hook(&c, move || {
            inside.wait();
            inside.wait();
        });
        let writer = {
            let c = Arc::clone(&c);
            thread::spawn(move || {
                c.add_many(&[Record {
                    id: 100,
                    vector: along(1.0, 100),
                    text: "p".into(),
                    metadata: vec![],
                }])
            })
        };
        barrier.wait();
        let release = Release(&barrier);
        let got = c.get(100).unwrap();
        let count = c.count().unwrap();
        let hits = c.search(&along(1.0, 0), 10, None).unwrap();
        drop(release);
        assert!(got.is_none());
        assert_eq!(count, 5);
        assert!(hits.iter().all(|h| h.id != 100), "{hits:?}");
        assert_eq!(writer.join().unwrap().unwrap(), 1);
        assert_eq!(c.count().unwrap(), 6);
        fs::remove_dir_all(&dir).ok();
    }

    /// `add_many` returns only after its log sync. While the batch is in the window, its caller has not
    /// returned, so an acknowledgement cannot precede the sync.
    #[test]
    fn acknowledgement_comes_after_the_log_sync() {
        use std::sync::atomic::{AtomicBool, Ordering};
        let dir = temp_dir("ack-after-sync");
        let db = VectorDB::open_with(&dir, crate::database::MetadataSync::Full).unwrap();
        let c = db.create_collection("ak", Some(8), 4).unwrap();
        c.add(0, &along(0.1, 0), "seed", vec![]).unwrap();
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let inside = Arc::clone(&barrier);
        set_window_hook(&c, move || {
            inside.wait();
            inside.wait();
        });
        let returned = Arc::new(AtomicBool::new(false));
        let writer = {
            let (c, returned) = (Arc::clone(&c), Arc::clone(&returned));
            thread::spawn(move || {
                let result = c.add_many(&[Record {
                    id: 1,
                    vector: along(0.5, 1),
                    text: "a".into(),
                    metadata: vec![],
                }]);
                returned.store(true, Ordering::SeqCst);
                result
            })
        };
        barrier.wait();
        let release = Release(&barrier);
        let acknowledged_early = returned.load(Ordering::SeqCst);
        drop(release);
        assert!(!acknowledged_early, "add_many returned before its log sync");
        assert_eq!(writer.join().unwrap().unwrap(), 1);
        assert!(returned.load(Ordering::SeqCst));
        fs::remove_dir_all(&dir).ok();
    }

    /// A process crash between a group's index sync and its log sync keeps the whole group. The crash
    /// is simulated by copying the database directory from inside the window: the index is synced,
    /// the log bytes are written but not synced, and the copy is reopened.
    #[test]
    fn crash_after_group_index_sync_keeps_the_whole_group() {
        let dir = temp_dir("crash-in-group");
        let snapshot = temp_dir("crash-in-group-snapshot");
        let db = VectorDB::open(&dir).unwrap();
        let c = db.create_collection("cg", Some(8), 4).unwrap();
        c.add(0, &along(0.1, 0), "seed", vec![]).unwrap();
        let (src, dst) = (dir.clone(), snapshot.clone());
        set_window_hook(&c, move || {
            let _ = fs::remove_dir_all(&dst);
            copy_tree(&src, &dst);
        });
        let batch: Vec<Record> = (1..=5)
            .map(|i| Record {
                id: i,
                vector: along(0.2 * i as f32, i),
                text: "g".into(),
                metadata: vec![],
            })
            .collect();
        assert_eq!(c.add_many(&batch).unwrap(), 5);
        drop(c);
        drop(db);

        let recovered = VectorDB::open(&snapshot).unwrap();
        assert!(
            recovered.recovery_report().is_clean(),
            "{:?}",
            recovered.recovery_report()
        );
        let rc = recovered.get_collection("cg").unwrap();
        assert_eq!(rc.count().unwrap(), 6);
        for i in 1..=5 {
            assert!(rc.get(i).unwrap().is_some());
        }
        fs::remove_dir_all(&dir).ok();
        fs::remove_dir_all(&snapshot).ok();
    }

    /// Writes and reads from several threads on one collection: an acknowledged add is returned by a
    /// later filtered search, and an acknowledged delete is not returned by a later search.
    #[test]
    fn acknowledged_writes_are_seen_and_acknowledged_deletes_are_not() {
        use crate::filter::Filter;
        use crate::value::MetaValue;
        use std::collections::HashSet;
        use std::sync::Mutex;
        let dir = temp_dir("linearizable");
        let db = VectorDB::open(&dir).unwrap();
        let c = db.create_collection("lz", Some(8), 4).unwrap();
        c.add_many(
            &(0..200)
                .map(|i| Record {
                    id: i,
                    vector: along(0.3, i),
                    text: "s".into(),
                    metadata: vec![("tag".into(), MetaValue::Text("s".into()))],
                })
                .collect::<Vec<_>>(),
        )
        .unwrap();
        let acked = Arc::new(Mutex::new(Vec::<u64>::new()));
        let deleted = Arc::new(Mutex::new(Vec::<u64>::new()));
        let mut threads = Vec::new();
        for w in 0..2u64 {
            let (c, acked) = (Arc::clone(&c), Arc::clone(&acked));
            threads.push(thread::spawn(move || {
                for b in 0..20u64 {
                    let ids: Vec<u64> = (0..5).map(|i| 1_000 + w * 1_000 + b * 5 + i).collect();
                    let records: Vec<Record> = ids
                        .iter()
                        .map(|&id| Record {
                            id,
                            vector: along(0.6, id),
                            text: "w".into(),
                            metadata: vec![("tag".into(), MetaValue::Text("w".into()))],
                        })
                        .collect();
                    c.add_many(&records).unwrap();
                    acked.lock().unwrap().extend(ids);
                }
            }));
        }
        {
            let (c, deleted) = (Arc::clone(&c), Arc::clone(&deleted));
            threads.push(thread::spawn(move || {
                for id in 0..200u64 {
                    c.delete(id).unwrap();
                    deleted.lock().unwrap().push(id);
                }
            }));
        }
        for _ in 0..2 {
            let (c, acked, deleted) = (Arc::clone(&c), Arc::clone(&acked), Arc::clone(&deleted));
            threads.push(thread::spawn(move || {
                let query = along(1.0, 0);
                let w_filter = Filter::Eq {
                    field: "tag".into(),
                    value: MetaValue::Text("w".into()),
                };
                for _ in 0..60 {
                    // Snapshot the acknowledgements first: each one was made before its search starts.
                    let acked_now: Vec<u64> = acked.lock().unwrap().clone();
                    let deleted_now: Vec<u64> = deleted.lock().unwrap().clone();
                    let all: HashSet<u64> = c
                        .search(&query, 10_000, None)
                        .unwrap()
                        .iter()
                        .map(|h| h.id)
                        .collect();
                    for id in &deleted_now {
                        assert!(!all.contains(id), "deleted id {id} returned");
                    }
                    let tagged: HashSet<u64> = c
                        .search(&query, 10_000, Some(&w_filter))
                        .unwrap()
                        .iter()
                        .map(|h| h.id)
                        .collect();
                    for id in &acked_now {
                        assert!(tagged.contains(id), "acknowledged id {id} missing");
                    }
                }
            }));
        }
        for t in threads {
            t.join().unwrap();
        }
        assert_eq!(c.count().unwrap(), 2 * 20 * 5);
        fs::remove_dir_all(&dir).ok();
    }

    /// A power loss in the middle of a group's log write leaves a prefix of the batch in the log, while
    /// the index was synced in full. Reopening keeps the prefix and removes the stray vectors, as
    /// STORAGE.md describes for power loss. The copy is cut inside the batch's frames.
    #[test]
    fn torn_group_log_keeps_a_prefix_and_removes_stray_vectors() {
        let dir = temp_dir("torn-group");
        let snapshot = temp_dir("torn-group-snapshot");
        let db = VectorDB::open(&dir).unwrap();
        let c = db.create_collection("tg", Some(8), 4).unwrap();
        c.add(0, &along(0.1, 0), "seed", vec![]).unwrap();
        let (src, dst) = (dir.clone(), snapshot.clone());
        set_window_hook(&c, move || {
            let _ = fs::remove_dir_all(&dst);
            copy_tree(&src, &dst);
        });
        let batch: Vec<Record> = (1..=5)
            .map(|i| Record {
                id: i,
                vector: along(0.2 * i as f32, i),
                text: "g".into(),
                metadata: vec![],
            })
            .collect();
        c.add_many(&batch).unwrap();
        drop(c);
        drop(db);

        // Cut the copy's log a few bytes into the batch's frames: the seed document and the first
        // document of the batch survive, the rest is torn.
        let log = snapshot.join("docs").join("tg.zdocs");
        let full = fs::metadata(&log).unwrap().len();
        let seed_frame_len = fs::metadata(&log).unwrap().len() / 6;
        let cut = seed_frame_len * 2 + 9;
        assert!(cut < full);
        fs::OpenOptions::new()
            .write(true)
            .open(&log)
            .unwrap()
            .set_len(cut)
            .unwrap();

        let recovered = VectorDB::open(&snapshot).unwrap();
        let report = recovered.recovery_report();
        let rc = recovered.get_collection("tg").unwrap();
        assert_eq!(
            rc.count().unwrap(),
            2,
            "the seed and the first document of the batch"
        );
        assert!(rc.get(1).unwrap().is_some());
        assert!(rc.get(2).unwrap().is_none());
        assert_eq!(report.orphaned_vectors_removed, 4, "{report:?}");
        fs::remove_dir_all(&dir).ok();
        fs::remove_dir_all(&snapshot).ok();
    }

    /// A panic in the leader part way through a group poisons the collection and releases the queue. The
    /// write that panicked gets an error. A write queued behind it gets an error rather than waiting. A
    /// reopen then reconciles the files: the panicked write is applied, since its index sync and its log
    /// bytes were both written before the panic, and the documented rule for an error after a sync allows that.
    #[test]
    fn leader_panic_poisons_releases_the_queue_and_reopen_recovers() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let dir = temp_dir("leader-panic");
        {
            let db = VectorDB::open_with(&dir, crate::database::MetadataSync::Full).unwrap();
            let c = db.create_collection("lp", Some(8), 4).unwrap();
            c.add(0, &along(0.5, 0), "seed", vec![]).unwrap();
            let calls = AtomicUsize::new(0);
            set_window_hook(&c, move || {
                if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    panic!("injected panic in the leader");
                }
            });
            let victim = {
                let c = Arc::clone(&c);
                thread::spawn(move || c.add(1, &along(0.6, 1), "one", vec![]))
            };
            assert!(victim.join().is_err());
            assert!(matches!(
                c.add(2, &along(0.7, 2), "two", vec![]),
                Err(ZenarError::PersistenceError(_))
            ));
            assert!(matches!(c.get(0), Err(ZenarError::PersistenceError(_))));
        }
        let db = VectorDB::open(&dir).unwrap();
        let c = db.get_collection("lp").unwrap();
        assert_eq!(c.count().unwrap(), 2);
        assert!(c.get(1).unwrap().is_some());
        c.add(2, &along(0.7, 2), "two", vec![]).unwrap();
        fs::remove_dir_all(&dir).ok();
    }

    /// A failed staging poisons the collection, as a failed log write does, and nothing it staged is
    /// visible. A reopen drops the staged documents, which have no vectors.
    #[test]
    fn failed_staging_poisons_and_reopen_drops_the_staged_documents() {
        use std::sync::atomic::Ordering;
        let dir = temp_dir("fail-stage");
        {
            let db = VectorDB::open(&dir).unwrap();
            let c = db.create_collection("fs", Some(8), 4).unwrap();
            c.add_many(
                &(0..3)
                    .map(|i| Record {
                        id: i,
                        vector: along(0.2, i),
                        text: "kept".into(),
                        metadata: vec![],
                    })
                    .collect::<Vec<_>>(),
            )
            .unwrap();
            c.core.fail_stage.store(true, Ordering::SeqCst);
            let batch: Vec<Record> = (10..14)
                .map(|i| Record {
                    id: i,
                    vector: along(0.5, i),
                    text: "staged".into(),
                    metadata: vec![],
                })
                .collect();
            assert!(matches!(
                c.add_many(&batch),
                Err(ZenarError::PersistenceError(_))
            ));
            assert!(matches!(c.count(), Err(ZenarError::PersistenceError(_))));
            assert!(matches!(
                c.add(20, &along(0.1, 20), "x", vec![]),
                Err(ZenarError::PersistenceError(_))
            ));
        }
        let db = VectorDB::open(&dir).unwrap();
        let c = db.get_collection("fs").unwrap();
        assert_eq!(c.count().unwrap(), 3);
        for i in 10..14 {
            assert!(c.get(i).unwrap().is_none());
        }
        c.add_many(&[Record {
            id: 10,
            vector: along(0.5, 10),
            text: "again".into(),
            metadata: vec![],
        }])
        .unwrap();
        assert_eq!(c.count().unwrap(), 4);
        fs::remove_dir_all(&dir).ok();
    }

    /// Writers send valid batches and rejected ones (a repeated id) to the same groups, while readers search.
    /// Every valid batch commits, every rejected batch fails alone, and no search returns a repeated or
    /// uncommitted id.
    #[test]
    fn rejected_group_members_under_concurrent_readers_do_not_disturb_the_rest() {
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        let dir = temp_dir("rejected-readers");
        let db = VectorDB::open(&dir).unwrap();
        let c = db.create_collection("rr", Some(8), 4).unwrap();
        c.add(0, &along(0.5, 0), "seed", vec![]).unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let committed = Arc::new(AtomicUsize::new(0));
        let mut readers = Vec::new();
        for _ in 0..2 {
            let (c, stop) = (Arc::clone(&c), Arc::clone(&stop));
            readers.push(thread::spawn(move || {
                while !stop.load(Ordering::SeqCst) {
                    let hits = c.search(&along(1.0, 0), 1000, None).unwrap();
                    let mut ids: Vec<u64> = hits.iter().map(|h| h.id).collect();
                    ids.sort_unstable();
                    ids.dedup();
                    assert_eq!(ids.len(), hits.len(), "repeated id in results");
                }
            }));
        }
        let mut writers = Vec::new();
        for w in 0..3u64 {
            let (c, committed) = (Arc::clone(&c), Arc::clone(&committed));
            writers.push(thread::spawn(move || {
                for b in 0..30u64 {
                    let base = 1_000 + w * 1_000 + b * 10;
                    let ids: Vec<u64> = if b % 3 == 0 {
                        vec![base, base, base + 1] // a repeated id: rejected on its own
                    } else {
                        vec![base, base + 1, base + 2]
                    };
                    let records: Vec<Record> = ids
                        .iter()
                        .map(|&id| Record {
                            id,
                            vector: along(0.6, id),
                            text: "w".into(),
                            metadata: vec![],
                        })
                        .collect();
                    match c.add_many(&records) {
                        Ok(n) => {
                            assert_ne!(b % 3, 0, "a rejected batch committed");
                            committed.fetch_add(n, Ordering::SeqCst);
                        }
                        Err(e) => {
                            assert_eq!(b % 3, 0, "a valid batch failed: {e:?}");
                            assert!(matches!(e, ZenarError::DuplicateId(_)));
                        }
                    }
                }
            }));
        }
        for w in writers {
            w.join().unwrap();
        }
        stop.store(true, Ordering::SeqCst);
        for r in readers {
            r.join().unwrap();
        }
        // Each writer sends 30 batches: 10 rejected, 20 valid of 3 records each.
        assert_eq!(committed.load(Ordering::SeqCst), 3 * 20 * 3);
        assert_eq!(c.count().unwrap(), 1 + 3 * 20 * 3);
        fs::remove_dir_all(&dir).ok();
    }

    /// The transitions of a compaction, in order (see `build_compaction` and `swap_locked`). This fixture has
    /// no writes during the build, so no catch-up round happens.
    const COMPACTION_POINTS: [&str; 11] = [
        "build snapshot",
        "build temp synced",
        "build replayed",
        "build done",
        "swap tail",
        "swap tail synced",
        "pending recorded",
        "renamed",
        "directory synced",
        "swapped",
        "pending cleared",
    ];

    /// A collection with 10,000 committed documents and 6,000 of them deleted, which is the state in which the
    /// delete compacts. Returns the database and the collection.
    fn compacting_fixture(dir: &Path) -> (VectorDB, Arc<Collection>) {
        let db = VectorDB::open(dir).unwrap();
        let c = db.create_collection("cp", Some(8), 4).unwrap();
        for chunk in (0..10_000u64).collect::<Vec<_>>().chunks(1000) {
            let records: Vec<Record> = chunk
                .iter()
                .map(|&i| Record {
                    id: i,
                    vector: along(0.1 + (i % 50) as f32 * 0.01, i),
                    text: format!("doc {i}"),
                    metadata: vec![],
                })
                .collect();
            c.add_many(&records).unwrap();
        }
        db.flush().unwrap();
        (db, c)
    }

    /// The documents a reopened compaction fixture must hold: ids 6000..10000, and nothing deleted.
    fn check_compacting_fixture(db: &VectorDB) {
        let c = db.get_collection("cp").unwrap();
        assert_eq!(c.count().unwrap(), 4000);
        assert!(c.get(5999).unwrap().is_none());
        assert!(c.get(6000).unwrap().is_some());
        assert!(c.get(9999).unwrap().is_some());
    }

    /// The catalog file of a database, as text.
    fn catalog_text(dir: &Path) -> String {
        fs::read_to_string(dir.join("catalog.json")).unwrap()
    }

    /// A catalog written before the pending record existed reads as before, and is written as before.
    #[test]
    fn catalog_without_the_pending_field_reads_and_writes_as_before() {
        let dir = temp_dir("catalog-compat");
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("catalog.json");
        fs::write(
            &path,
            r#"{"version":3,"collections":[{"name":"x","dimension":8,"bit_width":4,"fields":[],"durable_log_len":5,"durable_vectors":2}]}"#,
        )
        .unwrap();
        let catalog = Catalog::open(&path).unwrap();
        let entry = catalog.get("x").unwrap().unwrap();
        assert_eq!(entry.compaction_pending, None);
        assert_eq!(entry.durable_log_len, 5);
        catalog.update("x", |e| e.durable_vectors = 3).unwrap();
        assert!(!catalog_text(&dir).contains("compaction_pending"));
        fs::remove_dir_all(&dir).ok();
    }

    /// A pending record round-trips through the catalog file.
    #[test]
    fn pending_record_round_trips() {
        let dir = temp_dir("catalog-pending");
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("catalog.json");
        fs::write(
            &path,
            r#"{"version":3,"collections":[{"name":"x","dimension":8,"bit_width":4,"fields":[],"durable_log_len":5,"durable_vectors":2}]}"#,
        )
        .unwrap();
        let catalog = Catalog::open(&path).unwrap();
        catalog
            .update("x", |e| e.compaction_pending = Some(9))
            .unwrap();
        drop(catalog);
        let reopened = Catalog::open(&path).unwrap();
        assert_eq!(
            reopened.get("x").unwrap().unwrap().compaction_pending,
            Some(9)
        );
        fs::remove_dir_all(&dir).ok();
    }

    /// A catalog write that fails before its rename leaves the file and the memory as they were.
    #[test]
    fn catalog_failure_before_rename_reverts_memory_and_keeps_the_file() {
        use std::sync::atomic::Ordering;
        let dir = temp_dir("catalog-fail-before");
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("catalog.json");
        fs::write(
            &path,
            r#"{"version":3,"collections":[{"name":"x","dimension":8,"bit_width":4,"fields":[],"durable_log_len":5,"durable_vectors":2}]}"#,
        )
        .unwrap();
        let catalog = Catalog::open(&path).unwrap();
        let before = catalog_text(&dir);
        catalog
            .fail_persist_before_rename
            .store(true, Ordering::SeqCst);
        assert!(catalog.update("x", |e| e.durable_log_len = 99).is_err());
        assert_eq!(catalog.get("x").unwrap().unwrap().durable_log_len, 5);
        assert_eq!(catalog_text(&dir), before);
        assert!(!path.with_extension("json.tmp").exists());
        fs::remove_dir_all(&dir).ok();
    }

    /// A catalog write that fails after its rename keeps the new value in memory, and reports the error,
    /// because the file holds the new value.
    #[test]
    fn catalog_failure_after_rename_keeps_the_new_value() {
        use std::sync::atomic::Ordering;
        let dir = temp_dir("catalog-fail-after");
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("catalog.json");
        fs::write(
            &path,
            r#"{"version":3,"collections":[{"name":"x","dimension":8,"bit_width":4,"fields":[],"durable_log_len":5,"durable_vectors":2}]}"#,
        )
        .unwrap();
        let catalog = Catalog::open(&path).unwrap();
        catalog
            .fail_persist_after_rename
            .store(true, Ordering::SeqCst);
        assert!(catalog.update("x", |e| e.durable_log_len = 99).is_err());
        assert_eq!(catalog.get("x").unwrap().unwrap().durable_log_len, 99);
        drop(catalog);
        assert_eq!(
            Catalog::open(&path)
                .unwrap()
                .get("x")
                .unwrap()
                .unwrap()
                .durable_log_len,
            99
        );
        fs::remove_dir_all(&dir).ok();
    }

    /// Every transition of a compaction, copied as a crash would leave it, reopens to the committed documents.
    /// This is the 17-state table of docs/R3.md, section 5, for the synchronous compaction: each copy is a
    /// state the process could be killed in, and each must reopen without loss or refusal.
    #[test]
    fn crash_at_every_compaction_transition_reopens_to_the_committed_documents() {
        let dir = temp_dir("crash-transitions");
        let copies = temp_dir("crash-transitions-copies");
        let (db, c) = compacting_fixture(&dir);
        let (src, dst) = (dir.clone(), copies.clone());
        *c.core.compaction_hook.lock().unwrap() = Some(Arc::new(move |point: &'static str| {
            copy_tree(&src, &dst.join(point.replace(' ', "_")));
        }));
        assert_eq!(
            c.delete_many(&(0..6000).collect::<Vec<u64>>()).unwrap(),
            6000
        );
        c.wait_for_compaction();
        *c.core.compaction_hook.lock().unwrap() = None;
        drop(c);
        drop(db);
        // The original reopens too.
        check_compacting_fixture(&VectorDB::open(&dir).unwrap());
        for point in COMPACTION_POINTS {
            let copy = copies.join(point.replace(' ', "_"));
            assert!(copy.exists(), "no copy at {point}");
            let recovered = VectorDB::open(&copy).unwrap_or_else(|e| panic!("{point}: {e:?}"));
            check_compacting_fixture(&recovered);
        }
        fs::remove_dir_all(&dir).ok();
        fs::remove_dir_all(&copies).ok();
    }

    /// A failure at each transition of the compaction. Before the rename, the compaction undoes itself and the
    /// collection stays usable. From the rename on, the file and the memory can disagree until a reopen, so the
    /// collection is poisoned. A reopen recovers the committed documents in every case.
    #[test]
    fn failure_at_each_compaction_transition_is_safe_and_reopen_recovers() {
        let before_rename = [
            "build snapshot",
            "build temp synced",
            "build replayed",
            "build done",
            "swap tail",
            "swap tail synced",
            "pending recorded",
        ];
        for point in COMPACTION_POINTS {
            let dir = temp_dir(&format!("fail-{}", point.replace(' ', "-")));
            let (db, c) = compacting_fixture(&dir);
            *c.core.fail_compaction_at.lock().unwrap() = Some(point);
            assert!(
                c.delete_many(&(0..6000).collect::<Vec<u64>>()).is_ok(),
                "{point}"
            );
            c.wait_for_compaction();
            if before_rename.contains(&point) {
                check_compacting_fixture(&db);
                c.add(20_000, &along(0.3, 20_000), "after", vec![]).unwrap();
                assert_eq!(c.count().unwrap(), 4001, "{point}");
            } else {
                assert!(
                    matches!(c.count(), Err(ZenarError::PersistenceError(_))),
                    "{point}"
                );
            }
            drop(c);
            drop(db);
            let db = VectorDB::open(&dir).unwrap_or_else(|e| panic!("{point}: {e:?}"));
            let c = db.get_collection("cp").unwrap();
            let expected = if before_rename.contains(&point) {
                4001
            } else {
                4000
            };
            assert_eq!(c.count().unwrap(), expected, "{point}");
            assert!(c.get(6000).unwrap().is_some(), "{point}");
            assert!(c.get(5999).unwrap().is_none(), "{point}");
            fs::remove_dir_all(&dir).ok();
        }
    }

    /// A catalog write that fails while the pending record is recorded poisons the collection. The pending
    /// A pending record whose log is neither the old nor the new length refuses to open: the log changed
    /// outside Zenar, and recovery cannot tell which state it is in.
    #[test]
    fn pending_compaction_with_an_unexpected_log_length_refuses_to_open() {
        let dir = temp_dir("pending-unexpected");
        let copies = temp_dir("pending-unexpected-copy");
        let (db, c) = compacting_fixture(&dir);
        let (src, dst) = (dir.clone(), copies.clone());
        *c.core.compaction_hook.lock().unwrap() = Some(Arc::new(move |point: &'static str| {
            if point == "pending recorded" {
                copy_tree(&src, &dst);
            }
        }));
        c.delete_many(&(0..6000).collect::<Vec<u64>>()).unwrap();
        c.wait_for_compaction();
        *c.core.compaction_hook.lock().unwrap() = None;
        drop(c);
        drop(db);
        // The copy holds pending = old length, with the old log. Truncate the log to a third length.
        let log = copies.join("docs").join("cp.zdocs");
        let len = fs::metadata(&log).unwrap().len();
        fs::OpenOptions::new()
            .write(true)
            .open(&log)
            .unwrap()
            .set_len(len - 7)
            .unwrap();
        let before = fs::read(&log).unwrap();
        assert!(matches!(
            VectorDB::open(&copies),
            Err(ZenarError::RecoveryError(_))
        ));
        assert_eq!(
            fs::read(&log).unwrap(),
            before,
            "a refused open must not change the log"
        );
        fs::remove_dir_all(&dir).ok();
        fs::remove_dir_all(&copies).ok();
    }

    /// The power-loss rows of the crash table depend on the order of the transitions: the temporary log is
    /// synced before the pending record, the pending record is written before the rename, and the lengths are
    /// recorded after the directory sync. This test pins that order.
    #[test]
    fn compaction_transitions_run_in_the_documented_order() {
        let dir = temp_dir("compaction-order");
        let (db, c) = compacting_fixture(&dir);
        let seen = Arc::new(Mutex::new(Vec::<&'static str>::new()));
        let log = Arc::clone(&seen);
        *c.core.compaction_hook.lock().unwrap() = Some(Arc::new(move |point: &'static str| {
            log.lock().unwrap().push(point);
        }));
        c.delete_many(&(0..6000).collect::<Vec<u64>>()).unwrap();
        c.wait_for_compaction();
        *c.core.compaction_hook.lock().unwrap() = None;
        assert_eq!(*seen.lock().unwrap(), COMPACTION_POINTS.to_vec());
        drop(c);
        drop(db);
        fs::remove_dir_all(&dir).ok();
    }

    /// Writes during the build are carried by the tail. Here 2,000 documents are added and 500 deleted while the
    /// build has replayed its snapshot, which is enough committed bytes for a catch-up round.
    #[test]
    fn writes_during_the_build_are_carried_by_the_tail() {
        let dir = temp_dir("writes-during-build");
        let (db, c) = compacting_fixture(&dir);
        let seen = Arc::new(Mutex::new(Vec::<&'static str>::new()));
        let log = Arc::clone(&seen);
        let writer = Arc::clone(&c);
        let fired = std::sync::atomic::AtomicBool::new(false);
        *c.core.compaction_hook.lock().unwrap() = Some(Arc::new(move |point: &'static str| {
            log.lock().unwrap().push(point);
            if point == "build replayed" && !fired.swap(true, std::sync::atomic::Ordering::SeqCst) {
                let records: Vec<Record> = (20_000..22_000)
                    .map(|i| Record {
                        id: i,
                        vector: along(0.2, i),
                        text: "tail".into(),
                        metadata: vec![],
                    })
                    .collect();
                writer.add_many(&records).unwrap();
                writer
                    .delete_many(&(6000..6500).collect::<Vec<u64>>())
                    .unwrap();
            }
        }));
        c.delete_many(&(0..6000).collect::<Vec<u64>>()).unwrap();
        c.wait_for_compaction();
        *c.core.compaction_hook.lock().unwrap() = None;
        assert!(
            seen.lock().unwrap().contains(&"catch-up round"),
            "{:?}",
            seen.lock().unwrap()
        );
        assert_eq!(c.count().unwrap(), 4000 + 2000 - 500);
        assert!(c.get(6000).unwrap().is_none() || c.get(6000).unwrap().is_some());
        assert!(c.get(6499).unwrap().is_none());
        assert!(c.get(6500).unwrap().is_some());
        assert!(c.get(21_999).unwrap().is_some());
        // The swap keeps the live rows. The 500 deletes made during the build are applied to the new structure
        // as dead rows, which the next compaction reclaims (docs/R3.md, section 10).
        assert_eq!(
            c.state.read().unwrap().docs.row_count(),
            5500 + 500,
            "after the swap: live rows and the dead rows of the build's deletes"
        );
        c.compact_now().unwrap();
        assert_eq!(
            c.state.read().unwrap().docs.row_count(),
            5500,
            "the next compaction reclaims them"
        );
        drop(c);
        drop(db);
        let db = VectorDB::open(&dir).unwrap();
        let c = db.get_collection("cp").unwrap();
        assert_eq!(c.count().unwrap(), 5500);
        assert!(c.get(6499).unwrap().is_none());
        assert!(c.get(21_999).unwrap().is_some());
        fs::remove_dir_all(&dir).ok();
    }

    /// Search results during the build and after the swap equal those before it: the same ids, in the same
    /// order, for several values of top_k.
    #[test]
    fn search_is_the_same_before_during_and_after_the_build() {
        let dir = temp_dir("topk-during-build");
        let (_db, c) = compacting_fixture(&dir);
        let query = along(0.3, 0);
        let reference: Vec<Vec<u64>> = [1usize, 10, 50, 5000]
            .iter()
            .map(|&k| {
                c.search(&query, k, None)
                    .unwrap()
                    .iter()
                    .map(|h| h.id)
                    .collect()
            })
            .collect();
        let during = Arc::new(Mutex::new(Vec::<Vec<u64>>::new()));
        let sink = Arc::clone(&during);
        let reader = Arc::clone(&c);
        *c.core.compaction_hook.lock().unwrap() = Some(Arc::new(move |point: &'static str| {
            // Only the build runs without the collection lock. A swap point holds it, so a search there would wait.
            if point == "build done" {
                for k in [1usize, 10, 50, 5000] {
                    let ids: Vec<u64> = reader
                        .search(&along(0.3, 0), k, None)
                        .unwrap()
                        .iter()
                        .map(|h| h.id)
                        .collect();
                    sink.lock().unwrap().push(ids);
                }
            }
        }));
        c.delete_many(&(0..6000).collect::<Vec<u64>>()).unwrap();
        c.wait_for_compaction();
        *c.core.compaction_hook.lock().unwrap() = None;
        // The reference was taken before the delete, so it includes deleted ids. Compare against the
        // results after the delete, taken with the same query and k.
        let after: Vec<Vec<u64>> = [1usize, 10, 50, 5000]
            .iter()
            .map(|&k| {
                c.search(&query, k, None)
                    .unwrap()
                    .iter()
                    .map(|h| h.id)
                    .collect()
            })
            .collect();
        let during = during.lock().unwrap().clone();
        assert_eq!(during.len(), 4);
        for (i, ids) in during.iter().enumerate() {
            assert_eq!(ids, &after[i], "result {i} during the build");
        }
        for ids in &after {
            assert!(
                ids.iter().all(|&id| id >= 6000),
                "a deleted id was returned: {ids:?}"
            );
        }
        assert!(reference.iter().all(|ids| !ids.is_empty()));
        fs::remove_dir_all(&dir).ok();
    }

    /// Dead rows are reclaimed without a reopen. After the swap the row table holds only live documents, and
    /// the process is the same one that built them.
    #[test]
    fn dead_rows_are_reclaimed_by_the_swap_without_reopening() {
        let dir = temp_dir("reclaim");
        let (db, c) = compacting_fixture(&dir);
        assert_eq!(
            c.state.read().unwrap().docs.row_count(),
            10_000,
            "before: every row is in memory"
        );
        c.delete_many(&(0..6000).collect::<Vec<u64>>()).unwrap();
        c.wait_for_compaction();
        assert_eq!(
            c.state.read().unwrap().docs.row_count(),
            4000,
            "after: only live rows"
        );
        assert_eq!(c.count().unwrap(), 4000);
        drop(c);
        drop(db);
        fs::remove_dir_all(&dir).ok();
    }

    /// Repeated compactions do not accumulate rows: each round adds 6,000 documents, deletes the 6,000 oldest,
    /// compacts, and leaves the row table at the live size, with a log of about the live size.
    #[test]
    fn repeated_compactions_keep_memory_and_the_log_at_the_live_size() {
        let dir = temp_dir("repeated");
        let (db, c) = compacting_fixture(&dir);
        let log = dir.join("docs").join("cp.zdocs");
        // The fixture holds ids 0..10000. Delete the first 6,000 so the live window is ids 6000..10000: 4,000
        // documents. Each round adds 6,000 and deletes the 6,000 oldest, so the dead rows pass both the live
        // count and 4,096, and a compaction runs every round.
        c.delete_many(&(0..6000).collect::<Vec<u64>>()).unwrap();
        c.wait_for_compaction();
        assert_eq!(c.count().unwrap(), 4000, "setup");
        let mut window: std::collections::VecDeque<u64> = (6000..10_000).collect();
        let mut next = 10_000u64;
        for round in 0..5 {
            let ids: Vec<u64> = (next..next + 6000).collect();
            next += 6000;
            for chunk in ids.chunks(1000) {
                let records: Vec<Record> = chunk
                    .iter()
                    .map(|&i| Record {
                        id: i,
                        vector: along(0.1 + (i % 50) as f32 * 0.01, i),
                        text: format!("doc {i}"),
                        metadata: vec![],
                    })
                    .collect();
                c.add_many(&records).unwrap();
            }
            window.extend(ids);
            let victims: Vec<u64> = (0..6000).map(|_| window.pop_front().unwrap()).collect();
            c.delete_many(&victims).unwrap();
            c.wait_for_compaction();
            assert_eq!(c.count().unwrap(), 4000, "round {round}");
            assert_eq!(
                c.state.read().unwrap().docs.row_count(),
                4000,
                "round {round}: rows reclaimed"
            );
            let len = fs::metadata(&log).unwrap().len();
            assert!(len < 4000 * 400, "round {round}: the log is {len} bytes");
        }
        drop(c);
        drop(db);
        let db = VectorDB::open(&dir).unwrap();
        assert_eq!(db.get_collection("cp").unwrap().count().unwrap(), 4000);
        fs::remove_dir_all(&dir).ok();
    }

    /// A build that fails removes its temporary log, and the collection is usable.
    #[test]
    fn failed_build_leaves_no_temporary_log() {
        let dir = temp_dir("failed-build-tmp");
        let (db, c) = compacting_fixture(&dir);
        *c.core.fail_compaction_at.lock().unwrap() = Some("build replayed");
        c.delete_many(&(0..6000).collect::<Vec<u64>>()).unwrap();
        c.wait_for_compaction();
        assert!(!dir.join("docs").join("cp.log.compact").exists());
        assert_eq!(c.count().unwrap(), 4000);
        drop(c);
        drop(db);
        fs::remove_dir_all(&dir).ok();
    }

    /// Waits until no compaction is running, including one that a finished compaction started.
    fn wait_until_idle(c: &Collection) {
        loop {
            c.wait_for_compaction();
            if !c.compaction.lock().unwrap().running {
                return;
            }
        }
    }

    /// Deletes made while a build runs leave dead rows in the swapped structure. When those rows make the structure
    /// due again, the next compaction starts after the swap, with no further write from the user.
    #[test]
    fn deletes_during_a_build_that_make_it_due_are_compacted_without_another_write() {
        let dir = temp_dir("due-after-swap");
        let db = VectorDB::open(&dir).unwrap();
        let c = db.create_collection("cp", Some(8), 4).unwrap();
        for chunk in (0..20_000u64).collect::<Vec<_>>().chunks(1000) {
            let records: Vec<Record> = chunk
                .iter()
                .map(|&i| Record {
                    id: i,
                    vector: along(0.1 + (i % 50) as f32 * 0.01, i),
                    text: format!("doc {i}"),
                    metadata: vec![],
                })
                .collect();
            c.add_many(&records).unwrap();
        }
        // The first delete leaves 8,000 live and 12,000 dead, so it starts a build. The build's first pause deletes
        // 5,000 more: after the swap that is 3,000 live and 5,000 dead, which is due again.
        let fired = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let reader = Arc::clone(&c);
        *c.core.compaction_hook.lock().unwrap() = Some(Arc::new(move |point: &'static str| {
            if point == "build done" && !fired.swap(true, Ordering::SeqCst) {
                reader
                    .delete_many(&(12_000..17_000).collect::<Vec<u64>>())
                    .unwrap();
            }
        }));
        c.delete_many(&(0..12_000).collect::<Vec<u64>>()).unwrap();
        wait_until_idle(&c);
        assert_eq!(c.count().unwrap(), 3000);
        assert_eq!(
            c.state.read().unwrap().docs.row_count(),
            3000,
            "the second compaction reclaimed the rows deleted during the first build"
        );
        drop(c);
        drop(db);
        fs::remove_dir_all(&dir).ok();
    }

    /// A swap runs after the group in flight commits. The group's window holds the swap back, and the swap's
    /// "swapped" point is not reached until the group is released.
    #[test]
    fn swap_waits_for_an_in_flight_group() {
        let dir = temp_dir("swap-waits-group");
        let (db, c) = compacting_fixture(&dir);
        let (entered_tx, entered_rx) = std::sync::mpsc::channel::<()>();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let release_rx = Mutex::new(release_rx);
        let paused = AtomicBool::new(false);
        *c.window_hook.lock().unwrap() = Some(Arc::new(move || {
            if !paused.swap(true, Ordering::SeqCst) {
                entered_tx.send(()).unwrap();
                release_rx.lock().unwrap().recv().unwrap();
            }
        }));
        let delete = {
            let c = Arc::clone(&c);
            thread::spawn(move || c.delete_many(&(0..6000).collect::<Vec<u64>>()).unwrap())
        };
        entered_rx.recv().unwrap();
        let swapped = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&swapped);
        *c.core.compaction_hook.lock().unwrap() = Some(Arc::new(move |point: &'static str| {
            if point == "swapped" {
                flag.store(true, Ordering::SeqCst);
            }
        }));
        let compaction = {
            let c = Arc::clone(&c);
            thread::spawn(move || c.compact_now().unwrap())
        };
        thread::sleep(Duration::from_millis(300));
        assert!(
            !swapped.load(Ordering::SeqCst),
            "the swap ran while a group was in its window"
        );
        release_tx.send(()).unwrap();
        assert_eq!(delete.join().unwrap(), 6000);
        compaction.join().unwrap();
        assert!(swapped.load(Ordering::SeqCst));
        assert_eq!(c.count().unwrap(), 4000);
        drop(c);
        drop(db);
        fs::remove_dir_all(&dir).ok();
    }

    /// A row deleted while the build runs is absent after the swap, and stays absent after a reopen.
    #[test]
    fn a_row_deleted_during_the_build_is_absent_after_the_swap_and_reopen() {
        let dir = temp_dir("delete-in-build");
        let (db, c) = compacting_fixture(&dir);
        let reader = Arc::clone(&c);
        let fired = AtomicBool::new(false);
        *c.core.compaction_hook.lock().unwrap() = Some(Arc::new(move |point: &'static str| {
            if point == "build done" && !fired.swap(true, Ordering::SeqCst) {
                reader.delete_many(&[7000, 7001, 7002]).unwrap();
            }
        }));
        c.delete_many(&(0..6000).collect::<Vec<u64>>()).unwrap();
        wait_until_idle(&c);
        assert_eq!(c.count().unwrap(), 3997);
        assert!(c.get(7000).unwrap().is_none());
        drop(c);
        drop(db);
        let db = VectorDB::open(&dir).unwrap();
        let c = db.get_collection("cp").unwrap();
        assert_eq!(c.count().unwrap(), 3997);
        assert!(c.get(7001).unwrap().is_none());
        assert!(c.get(7003).unwrap().is_some());
        drop(c);
        drop(db);
        fs::remove_dir_all(&dir).ok();
    }

    /// An id deleted and added again while the build runs keeps its new copy through the swap and a reopen.
    #[test]
    fn a_re_added_id_during_the_build_keeps_its_new_copy() {
        let dir = temp_dir("re-add-in-build");
        let (db, c) = compacting_fixture(&dir);
        let reader = Arc::clone(&c);
        let fired = AtomicBool::new(false);
        *c.core.compaction_hook.lock().unwrap() = Some(Arc::new(move |point: &'static str| {
            if point == "build done" && !fired.swap(true, Ordering::SeqCst) {
                reader.delete_many(&[7000]).unwrap();
                reader
                    .add_many(&[Record {
                        id: 7000,
                        vector: along(0.5, 7000),
                        text: "new copy".into(),
                        metadata: vec![],
                    }])
                    .unwrap();
            }
        }));
        c.delete_many(&(0..6000).collect::<Vec<u64>>()).unwrap();
        wait_until_idle(&c);
        assert_eq!(c.get(7000).unwrap().unwrap().text, "new copy");
        drop(c);
        drop(db);
        let db = VectorDB::open(&dir).unwrap();
        let c = db.get_collection("cp").unwrap();
        assert_eq!(c.count().unwrap(), 4000);
        assert_eq!(c.get(7000).unwrap().unwrap().text, "new copy");
        drop(c);
        drop(db);
        fs::remove_dir_all(&dir).ok();
    }

    /// The committed length moves only when a group commits: it is the same inside a delete's window and inside an
    /// add's window, and it advances after each.
    #[test]
    fn committed_length_moves_only_at_a_commit() {
        let dir = temp_dir("watermark");
        let (db, c) = compacting_fixture(&dir);
        let seen = Arc::new(Mutex::new(Vec::new()));
        let (sink, reader) = (Arc::clone(&seen), Arc::clone(&c));
        *c.window_hook.lock().unwrap() = Some(Arc::new(move || {
            sink.lock()
                .unwrap()
                .push(reader.state.read().unwrap().committed_len);
        }));
        let before = c.state.read().unwrap().committed_len;
        c.delete_many(&[1]).unwrap();
        let after_delete = c.state.read().unwrap().committed_len;
        c.add_many(&[Record {
            id: 20_000,
            vector: along(0.2, 20_000),
            text: "w".into(),
            metadata: vec![],
        }])
        .unwrap();
        let after_add = c.state.read().unwrap().committed_len;
        *c.window_hook.lock().unwrap() = None;
        let inside = seen.lock().unwrap().clone();
        assert_eq!(inside.len(), 2, "one window per group: {inside:?}");
        assert_eq!(inside[0], before, "inside the delete's window");
        assert_eq!(inside[1], after_delete, "inside the add's window");
        assert!(after_delete > before && after_add > after_delete);
        drop(c);
        drop(db);
        fs::remove_dir_all(&dir).ok();
    }

    /// A group staged while a build takes its snapshot, then rolled back, is not in the rewrite: after the swap and a
    /// reopen its documents are absent, and the committed ones are all there.
    #[test]
    fn a_rolled_back_group_in_a_build_window_is_absent_after_the_swap() {
        let dir = temp_dir("rollback-window");
        let (db, c) = compacting_fixture(&dir);
        c.fail_log_sync.store(true, Ordering::SeqCst);
        let builder = Arc::clone(&c);
        let started = Arc::new(Mutex::new(None::<thread::JoinHandle<()>>));
        let slot = Arc::clone(&started);
        let fired = AtomicBool::new(false);
        *c.window_hook.lock().unwrap() = Some(Arc::new(move || {
            if !fired.swap(true, Ordering::SeqCst) {
                // The build's snapshot is taken while this group is staged and not committed. The group is not
                // joined here: its leader is this thread, and the swap waits for it.
                let b = Arc::clone(&builder);
                *slot.lock().unwrap() = Some(thread::spawn(move || b.compact_now().unwrap()));
                thread::sleep(Duration::from_millis(100));
            }
        }));
        let records: Vec<Record> = (20_000..20_050u64)
            .map(|i| Record {
                id: i,
                vector: along(0.3, i),
                text: "staged".into(),
                metadata: vec![],
            })
            .collect();
        assert!(c.add_many(&records).is_err(), "the group's log sync fails");
        *c.window_hook.lock().unwrap() = None;
        let handle = started
            .lock()
            .unwrap()
            .take()
            .expect("the build was started");
        handle.join().unwrap();
        assert_eq!(c.count().unwrap(), 10_000);
        assert!(c.get(20_000).unwrap().is_none());
        drop(c);
        drop(db);
        let db = VectorDB::open(&dir).unwrap();
        let c = db.get_collection("cp").unwrap();
        assert_eq!(c.count().unwrap(), 10_000);
        assert!(c.get(20_000).unwrap().is_none());
        drop(c);
        drop(db);
        fs::remove_dir_all(&dir).ok();
    }

    /// Opening a copy taken with the pending record and the old log reverts the record: the old log is kept and the
    /// committed documents are the ones it held.
    #[test]
    fn pending_record_with_old_length_is_reverted_on_open() {
        let dir = temp_dir("pending-old");
        let copies = temp_dir("pending-old-copies");
        let (db, c) = compacting_fixture(&dir);
        let (src, dst) = (dir.clone(), copies.clone());
        *c.core.compaction_hook.lock().unwrap() = Some(Arc::new(move |point: &'static str| {
            if point == "pending recorded" {
                copy_tree(&src, &dst);
            }
        }));
        c.delete_many(&(0..6000).collect::<Vec<u64>>()).unwrap();
        wait_until_idle(&c);
        drop(c);
        drop(db);
        assert!(
            fs::read_to_string(copies.join("catalog.json"))
                .unwrap()
                .contains("compaction_pending")
        );
        let recovered = VectorDB::open(&copies).unwrap();
        check_compacting_fixture(&recovered);
        assert!(
            !fs::read_to_string(copies.join("catalog.json"))
                .unwrap()
                .contains("compaction_pending"),
            "the reopen cleared the pending record"
        );
        drop(recovered);
        fs::remove_dir_all(&dir).ok();
        fs::remove_dir_all(&copies).ok();
    }

    /// Opening a copy taken after the rename, with the pending record and the new log, completes the record: the
    /// new log is kept and the committed documents are the same.
    #[test]
    fn pending_record_with_new_length_is_completed_on_open() {
        let dir = temp_dir("pending-new");
        let copies = temp_dir("pending-new-copies");
        let (db, c) = compacting_fixture(&dir);
        let (src, dst) = (dir.clone(), copies.clone());
        *c.core.compaction_hook.lock().unwrap() = Some(Arc::new(move |point: &'static str| {
            if point == "renamed" {
                copy_tree(&src, &dst);
            }
        }));
        c.delete_many(&(0..6000).collect::<Vec<u64>>()).unwrap();
        wait_until_idle(&c);
        drop(c);
        drop(db);
        let log_before = fs::metadata(copies.join("docs").join("cp.zdocs"))
            .unwrap()
            .len();
        let recovered = VectorDB::open(&copies).unwrap();
        check_compacting_fixture(&recovered);
        assert_eq!(
            fs::metadata(copies.join("docs").join("cp.zdocs"))
                .unwrap()
                .len(),
            log_before,
            "the new log was kept"
        );
        assert!(
            !fs::read_to_string(copies.join("catalog.json"))
                .unwrap()
                .contains("compaction_pending")
        );
        drop(recovered);
        fs::remove_dir_all(&dir).ok();
        fs::remove_dir_all(&copies).ok();
    }

    /// A temporary log left by an interrupted build is removed on open, and the documents are the committed ones.
    #[test]
    fn stale_temporary_log_is_removed_on_open() {
        let dir = temp_dir("stale-temp");
        let copies = temp_dir("stale-temp-copies");
        let (db, c) = compacting_fixture(&dir);
        let (src, dst) = (dir.clone(), copies.clone());
        *c.core.compaction_hook.lock().unwrap() = Some(Arc::new(move |point: &'static str| {
            if point == "build replayed" {
                copy_tree(&src, &dst);
            }
        }));
        c.delete_many(&(0..6000).collect::<Vec<u64>>()).unwrap();
        wait_until_idle(&c);
        drop(c);
        drop(db);
        let temp = copies.join("docs").join("cp.log.compact");
        assert!(temp.exists(), "the copy holds the temporary log");
        let recovered = VectorDB::open(&copies).unwrap();
        assert!(!temp.exists(), "the reopen removed the temporary log");
        check_compacting_fixture(&recovered);
        drop(recovered);
        fs::remove_dir_all(&dir).ok();
        fs::remove_dir_all(&copies).ok();
    }

    /// The swapped structure is the one a reopen would build: the same live ids in the same search order, before and
    /// after a reopen, with writes during the build and enough tail for several catch-up rounds.
    #[test]
    fn tail_replay_equals_full_replay_across_catch_up_rounds() {
        let dir = temp_dir("tail-equals-replay");
        let (db, c) = compacting_fixture(&dir);
        let rounds = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (count, writer) = (Arc::clone(&rounds), Arc::clone(&c));
        let fired = AtomicBool::new(false);
        *c.core.compaction_hook.lock().unwrap() = Some(Arc::new(move |point: &'static str| {
            if point == "catch-up round" {
                count.fetch_add(1, Ordering::SeqCst);
            }
            // 3,000 documents after the snapshot: about 100 KB of tail, more than a catch-up round copies (64 KiB).
            if point == "build replayed" && !fired.swap(true, Ordering::SeqCst) {
                let records: Vec<Record> = (30_000..33_000u64)
                    .map(|i| Record {
                        id: i,
                        vector: along(0.4, i),
                        text: format!("tail {i}"),
                        metadata: vec![],
                    })
                    .collect();
                writer.add_many(&records).unwrap();
            }
        }));
        c.delete_many(&(0..6000).collect::<Vec<u64>>()).unwrap();
        wait_until_idle(&c);
        *c.core.compaction_hook.lock().unwrap() = None;
        assert!(rounds.load(Ordering::SeqCst) >= 1, "no catch-up round ran");
        let query = along(0.4, 0);
        let before: Vec<u64> = c
            .search(&query, 10_000, None)
            .unwrap()
            .iter()
            .map(|h| h.id)
            .collect();
        assert_eq!(before.len(), 4000 + 3000);
        drop(c);
        drop(db);
        let db = VectorDB::open(&dir).unwrap();
        let c = db.get_collection("cp").unwrap();
        let after: Vec<u64> = c
            .search(&query, 10_000, None)
            .unwrap()
            .iter()
            .map(|h| h.id)
            .collect();
        assert_eq!(
            after, before,
            "the reopened search differs from the swapped one"
        );
        drop(c);
        drop(db);
        fs::remove_dir_all(&dir).ok();
    }

    /// A filtered search returns the same ids during the build and after the swap as it did before the build.
    #[test]
    fn filtered_search_is_the_same_during_the_build_and_after_the_swap() {
        let dir = temp_dir("filtered-during-build");
        let db = VectorDB::open(&dir).unwrap();
        let c = db.create_collection("cp", Some(8), 4).unwrap();
        for chunk in (0..10_000u64).collect::<Vec<_>>().chunks(1000) {
            let records: Vec<Record> = chunk
                .iter()
                .map(|&i| Record {
                    id: i,
                    vector: along(0.1 + (i % 50) as f32 * 0.01, i),
                    text: format!("doc {i}"),
                    metadata: vec![(
                        "tenant".into(),
                        MetaValue::Text(if i % 2 == 0 { "A" } else { "B" }.into()),
                    )],
                })
                .collect();
            c.add_many(&records).unwrap();
        }
        let filter = Filter::Eq {
            field: "tenant".into(),
            value: MetaValue::Text("A".into()),
        };
        let query = along(0.3, 0);
        let ids = |c: &Collection| -> Vec<u64> {
            c.search(&query, 50, Some(&filter))
                .unwrap()
                .iter()
                .map(|h| h.id)
                .collect()
        };
        let reference = ids(&c);
        assert_eq!(reference.len(), 50);
        let during = Arc::new(Mutex::new(Vec::new()));
        let (sink, reader) = (Arc::clone(&during), Arc::clone(&c));
        *c.core.compaction_hook.lock().unwrap() = Some(Arc::new(move |point: &'static str| {
            // Only the build runs without the collection lock, so a search is taken at "build done".
            if point == "build done" {
                let query = along(0.3, 0);
                let hits: Vec<u64> = reader
                    .search(
                        &query,
                        50,
                        Some(&Filter::Eq {
                            field: "tenant".into(),
                            value: MetaValue::Text("A".into()),
                        }),
                    )
                    .unwrap()
                    .iter()
                    .map(|h| h.id)
                    .collect();
                sink.lock().unwrap().push(hits);
            }
        }));
        c.delete_many(&(0..6000).collect::<Vec<u64>>()).unwrap();
        wait_until_idle(&c);
        *c.core.compaction_hook.lock().unwrap() = None;
        let during = during.lock().unwrap().clone();
        assert_eq!(during.len(), 1);
        let after = ids(&c);
        assert_eq!(
            during[0], after,
            "the search during the build differs from the one after the swap"
        );
        assert!(after.iter().all(|id| *id >= 6000 && id % 2 == 0));
        assert_eq!(after.len(), 50);
        drop(c);
        drop(db);
        fs::remove_dir_all(&dir).ok();
    }

    /// Random adds, deletes, reopens and compactions agree with a map of the live documents after every step that
    /// is checked.
    #[test]
    fn random_operations_with_compactions_agree_with_a_reference_model() {
        let dir = temp_dir("random-compact-model");
        let mut db = VectorDB::open(&dir).unwrap();
        let mut c = db.create_collection("cp", Some(8), 4).unwrap();
        let mut model: std::collections::BTreeMap<u64, String> = Default::default();
        let mut seed = 0x2545_F491_4F6C_DD1Du64;
        let mut rand = move |n: u64| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed % n
        };
        for step in 0..1200u64 {
            match rand(10) {
                0..=4 => {
                    let mut ids = Vec::new();
                    for _ in 0..40 {
                        let id = rand(3000);
                        if !model.contains_key(&id) && !ids.contains(&id) {
                            ids.push(id);
                        }
                    }
                    let records: Vec<Record> = ids
                        .iter()
                        .map(|&i| Record {
                            id: i,
                            vector: along(0.1 + (i % 50) as f32 * 0.01, i),
                            text: format!("v{step}-{i}"),
                            metadata: vec![],
                        })
                        .collect();
                    if !records.is_empty() {
                        c.add_many(&records).unwrap();
                        for r in &records {
                            model.insert(r.id, r.text.clone());
                        }
                    }
                }
                5..=7 => {
                    let victims: Vec<u64> = model
                        .keys()
                        .copied()
                        .filter(|id| id % 7 == step % 7)
                        .take(60)
                        .collect();
                    if !victims.is_empty() {
                        c.delete_many(&victims).unwrap();
                        for id in &victims {
                            model.remove(id);
                        }
                    }
                }
                8 => {
                    c.compact_now().unwrap();
                }
                _ => {
                    drop(c);
                    drop(db);
                    db = VectorDB::open(&dir).unwrap();
                    c = db.get_collection("cp").unwrap();
                }
            }
            if step % 100 == 99 {
                assert_eq!(c.count().unwrap(), model.len(), "step {step}");
                for (id, text) in &model {
                    assert_eq!(
                        c.get(*id).unwrap().unwrap().text,
                        *text,
                        "step {step}, id {id}"
                    );
                }
            }
        }
        drop(c);
        drop(db);
        fs::remove_dir_all(&dir).ok();
    }

    /// The old document structure is dropped by the swap, once, before the compaction returns.
    #[test]
    fn old_structure_is_dropped_at_the_swap() {
        let dir = temp_dir("old-dropped");
        let (db, c) = compacting_fixture(&dir);
        c.delete_many(&(0..6000).collect::<Vec<u64>>()).unwrap();
        wait_until_idle(&c);
        let before = crate::docstore::DROPPED.with(|d| d.get());
        c.compact_now().unwrap();
        let after = crate::docstore::DROPPED.with(|d| d.get());
        assert_eq!(
            after - before,
            1,
            "the old structure is dropped once, by the swap"
        );
        drop(c);
        drop(db);
        fs::remove_dir_all(&dir).ok();
    }

    /// A swap that fails before its rename leaves the threshold crossed. The failed compaction does not start
    /// another build by itself: the next delete does.
    #[test]
    fn failed_swap_does_not_restart_a_compaction_by_itself() {
        let dir = temp_dir("failed-swap-no-loop");
        let (db, c) = compacting_fixture(&dir);
        let builds = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = Arc::clone(&builds);
        *c.core.compaction_hook.lock().unwrap() = Some(Arc::new(move |point: &'static str| {
            if point == "build done" {
                counter.fetch_add(1, Ordering::SeqCst);
            }
        }));
        *c.core.fail_compaction_at.lock().unwrap() = Some("swap tail");
        c.delete_many(&(0..6000).collect::<Vec<u64>>()).unwrap();
        wait_until_idle(&c);
        thread::sleep(Duration::from_millis(200));
        assert_eq!(
            builds.load(Ordering::SeqCst),
            1,
            "a failed swap must not start another build by itself"
        );
        assert_eq!(c.count().unwrap(), 4000);
        drop(c);
        drop(db);
        fs::remove_dir_all(&dir).ok();
    }

    /// Writers, deleters and readers run during background compactions. No search returns a repeated id or an
    /// id deleted before it started, every acknowledged add is counted, and the reopened state matches.
    #[test]
    fn concurrent_writers_deleters_and_readers_during_compaction() {
        use std::collections::HashSet;
        use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
        let dir = temp_dir("concurrent-compaction");
        let db = VectorDB::open(&dir).unwrap();
        let c = db.create_collection("cc", Some(8), 4).unwrap();
        c.add_many(
            &(0..10_000)
                .map(|i| Record {
                    id: i,
                    vector: along(0.1 + (i % 40) as f32 * 0.01, i),
                    text: "seed".into(),
                    metadata: vec![],
                })
                .collect::<Vec<_>>(),
        )
        .unwrap();
        let deleted = Arc::new(Mutex::new(Vec::<u64>::new()));
        let added = Arc::new(AtomicU64::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let mut threads = Vec::new();
        {
            let (c, deleted) = (Arc::clone(&c), Arc::clone(&deleted));
            threads.push(thread::spawn(move || {
                // Batches: one delete call makes two catalog writes, so single deletes would be slow here.
                for chunk in (0..6000u64).collect::<Vec<_>>().chunks(500) {
                    c.delete_many(chunk).unwrap();
                    deleted.lock().unwrap().extend_from_slice(chunk);
                }
            }));
        }
        for w in 0..2u64 {
            let (c, added) = (Arc::clone(&c), Arc::clone(&added));
            threads.push(thread::spawn(move || {
                for b in 0..40u64 {
                    let records: Vec<Record> = (0..50)
                        .map(|i| Record {
                            id: 100_000 + w * 100_000 + b * 50 + i,
                            vector: along(0.5, b * 50 + i),
                            text: "w".into(),
                            metadata: vec![],
                        })
                        .collect();
                    c.add_many(&records).unwrap();
                    added.fetch_add(50, Ordering::SeqCst);
                }
            }));
        }
        let mut readers = Vec::new();
        for _ in 0..2 {
            let (c, deleted, stop) = (Arc::clone(&c), Arc::clone(&deleted), Arc::clone(&stop));
            readers.push(thread::spawn(move || {
                while !stop.load(Ordering::SeqCst) {
                    let gone: HashSet<u64> = deleted.lock().unwrap().iter().copied().collect();
                    let hits = c.search(&along(0.5, 0), 500, None).unwrap();
                    let ids: HashSet<u64> = hits.iter().map(|h| h.id).collect();
                    assert_eq!(ids.len(), hits.len(), "repeated id in results");
                    for id in &gone {
                        assert!(!ids.contains(id), "deleted id {id} returned");
                    }
                }
            }));
        }
        for t in threads {
            t.join().unwrap();
        }
        c.wait_for_compaction();
        stop.store(true, Ordering::SeqCst);
        for r in readers {
            r.join().unwrap();
        }
        let expected = 10_000 - 6000 + added.load(Ordering::SeqCst) as usize;
        assert_eq!(c.count().unwrap(), expected);
        drop(c);
        drop(db);
        let db = VectorDB::open(&dir).unwrap();
        assert_eq!(db.get_collection("cc").unwrap().count().unwrap(), expected);
        fs::remove_dir_all(&dir).ok();
    }
}

#[cfg(test)]
mod search_overfetch_tests {
    use super::*;
    use crate::database::VectorDB;

    const DIM: usize = 8;

    fn temp_dir(name: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("zenar-overfetch-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    /// A document whose vector scores `score` against `query()`. The second coordinate only tells documents apart.
    fn doc(id: u64, score: f32) -> Record {
        let mut vector = vec![0.0f32; DIM];
        vector[0] = score;
        vector[1] = 0.001 * (id % 97) as f32;
        Record {
            id,
            vector,
            text: format!("doc {id}"),
            metadata: vec![],
        }
    }

    fn query() -> Vec<f32> {
        let mut q = vec![0.0f32; DIM];
        q[0] = 1.0;
        q
    }

    fn ids(hits: &[Hit]) -> Vec<u64> {
        hits.iter().map(|h| h.id).collect()
    }

    /// A collection with `committed` documents of score `committed_score` (ids 0..committed), and their
    /// documents only: no pending write.
    fn committed_collection(
        dir: &Path,
        committed: u64,
        committed_score: f32,
    ) -> (VectorDB, Arc<Collection>) {
        let db = VectorDB::open(dir).unwrap();
        let c = db.create_collection("c", Some(DIM), 4).unwrap();
        let records: Vec<Record> = (0..committed).map(|i| doc(i, committed_score)).collect();
        if !records.is_empty() {
            c.add_many(&records).unwrap();
        }
        (db, c)
    }

    /// Runs `probe` once, while `records` are pending: after their index sync and before their commit. Returns what
    /// `probe` returned, and the add's result.
    fn probe_while_pending<T: Send + 'static>(
        c: &Arc<Collection>,
        records: Vec<Record>,
        probe: impl Fn(&Collection) -> T + Send + Sync + 'static,
    ) -> (Option<T>, Result<usize, ZenarError>) {
        let slot: Arc<Mutex<Option<T>>> = Arc::new(Mutex::new(None));
        let (sink, reader) = (Arc::clone(&slot), Arc::clone(c));
        let fired = AtomicBool::new(false);
        *c.window_hook.lock().unwrap() = Some(Arc::new(move || {
            if !fired.swap(true, Ordering::SeqCst) {
                *sink.lock().unwrap() = Some(probe(&reader));
            }
        }));
        let added = c.add_many(&records);
        *c.window_hook.lock().unwrap() = None;
        let out = slot.lock().unwrap().take();
        (out, added)
    }

    fn assert_committed_only(hits: &[Hit], committed: u64) {
        let mut seen = HashSet::new();
        for h in hits {
            assert!(
                h.id < committed,
                "an uncommitted document was returned: {}",
                h.id
            );
            assert!(seen.insert(h.id), "document {} returned twice", h.id);
        }
    }

    #[test]
    fn no_pending_writes_search_once_and_never_retry() {
        let dir = temp_dir("none");
        let (db, c) = committed_collection(&dir, 20, 0.5);
        let hits = c.search(&query(), 5, None).unwrap();
        assert_eq!(hits.len(), 5);
        assert_committed_only(&hits, 20);
        assert_eq!(c.search_retry_count(), 0);
        drop(c);
        drop(db);
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn pending_at_the_cap_does_not_retry() {
        let dir = temp_dir("at-cap");
        let (db, c) = committed_collection(&dir, 100, 0.1);
        let pending: Vec<Record> = (1000..1064).map(|i| doc(i, 0.9)).collect();
        let (hits, added) =
            probe_while_pending(&c, pending, |c| c.search(&query(), 5, None).unwrap());
        assert_eq!(added.unwrap(), 64);
        let hits = hits.unwrap();
        assert_eq!(
            hits.len(),
            5,
            "committed results are returned from the first pass"
        );
        assert_committed_only(&hits, 100);
        assert_eq!(c.search_retry_count(), 0, "pending == cap must not retry");
        drop(c);
        drop(db);
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn pending_above_the_cap_with_enough_committed_does_not_retry() {
        let dir = temp_dir("enough");
        let (db, c) = committed_collection(&dir, 200, 0.5);
        let pending: Vec<Record> = (1000..1100).map(|i| doc(i, 0.1)).collect();
        let (hits, added) =
            probe_while_pending(&c, pending, |c| c.search(&query(), 5, None).unwrap());
        assert_eq!(added.unwrap(), 100);
        let hits = hits.unwrap();
        assert_eq!(hits.len(), 5);
        assert_committed_only(&hits, 200);
        assert_eq!(
            c.search_retry_count(),
            0,
            "the first pass had enough committed results"
        );
        drop(c);
        drop(db);
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn pending_above_the_cap_that_crowds_out_the_committed_retries() {
        let dir = temp_dir("crowded");
        let (db, c) = committed_collection(&dir, 300, 0.1);
        // The 100 pending documents score highest, so the first pass of top_k + 64 holds only uncommitted ones.
        let pending: Vec<Record> = (1000..1100).map(|i| doc(i, 0.9)).collect();
        let (hits, added) =
            probe_while_pending(&c, pending, |c| c.search(&query(), 5, None).unwrap());
        assert_eq!(added.unwrap(), 100);
        let hits = hits.unwrap();
        assert_eq!(hits.len(), 5, "the retry returns top_k committed results");
        assert_committed_only(&hits, 300);
        assert_eq!(c.search_retry_count(), 1);
        drop(c);
        drop(db);
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn fewer_committed_documents_than_top_k_returns_all_of_them() {
        let dir = temp_dir("few");
        let (db, c) = committed_collection(&dir, 3, 0.1);
        let pending: Vec<Record> = (1000..1100).map(|i| doc(i, 0.9)).collect();
        let (hits, added) =
            probe_while_pending(&c, pending, |c| c.search(&query(), 10, None).unwrap());
        assert_eq!(added.unwrap(), 100);
        let hits = hits.unwrap();
        assert_eq!(hits.len(), 3, "every committed document is returned");
        assert_committed_only(&hits, 3);
        assert_eq!(c.search_retry_count(), 1);
        drop(c);
        drop(db);
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn no_committed_documents_returns_nothing_while_pending() {
        let dir = temp_dir("no-committed");
        let (db, c) = committed_collection(&dir, 0, 0.0);
        let pending: Vec<Record> = (1000..1100).map(|i| doc(i, 0.9)).collect();
        let (hits, added) =
            probe_while_pending(&c, pending, |c| c.search(&query(), 5, None).unwrap());
        assert_eq!(added.unwrap(), 100);
        assert!(hits.unwrap().is_empty(), "no committed document exists");
        drop(c);
        drop(db);
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn top_k_larger_than_the_population_returns_every_committed_document() {
        let dir = temp_dir("big-k");
        let (db, c) = committed_collection(&dir, 20, 0.5);
        assert_eq!(c.search(&query(), 100, None).unwrap().len(), 20);
        let pending: Vec<Record> = (1000..1100).map(|i| doc(i, 0.9)).collect();
        let (hits, added) =
            probe_while_pending(&c, pending, |c| c.search(&query(), 100, None).unwrap());
        assert_eq!(added.unwrap(), 100);
        let hits = hits.unwrap();
        assert_eq!(hits.len(), 20);
        assert_committed_only(&hits, 20);
        drop(c);
        drop(db);
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn huge_top_k_does_not_overflow_the_over_fetch() {
        let dir = temp_dir("overflow");
        let (db, c) = committed_collection(&dir, 20, 0.5);
        let pending: Vec<Record> = (1000..1100).map(|i| doc(i, 0.9)).collect();
        let (hits, added) =
            probe_while_pending(&c, pending, |c| c.search(&query(), usize::MAX - 2, None));
        assert_eq!(added.unwrap(), 100);
        let hits = hits.unwrap().unwrap();
        assert_eq!(hits.len(), 20);
        assert_committed_only(&hits, 20);
        drop(c);
        drop(db);
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_rolled_back_write_leaves_no_pending_state() {
        let dir = temp_dir("rollback");
        let (db, c) = committed_collection(&dir, 300, 0.1);
        let pending: Vec<Record> = (1000..1100).map(|i| doc(i, 0.9)).collect();
        let (hits, added) = probe_while_pending(&c, pending, |c| {
            // Make the write's log sync fail when it resumes: the write is rolled back.
            c.fail_log_sync.store(true, Ordering::SeqCst);
            c.search(&query(), 5, None).unwrap()
        });
        assert!(added.is_err(), "the write failed");
        let hits = hits.unwrap();
        assert_eq!(hits.len(), 5);
        assert_committed_only(&hits, 300);
        assert_eq!(
            c.state.read().unwrap().pending,
            0,
            "rollback cleared the pending count"
        );
        let retries = c.search_retry_count();
        let after = c.search(&query(), 5, None).unwrap();
        assert_eq!(after.len(), 5);
        assert_committed_only(&after, 300);
        assert_eq!(
            c.search_retry_count(),
            retries,
            "no retry once nothing is pending"
        );
        drop(c);
        drop(db);
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn deletes_during_searches_are_not_returned() {
        let dir = temp_dir("deletes");
        let (db, c) = committed_collection(&dir, 200, 0.5);
        // The deleted documents score highest, so without the delete they would fill the results.
        let high: Vec<Record> = (1000..1040).map(|i| doc(i, 0.9)).collect();
        c.add_many(&high).unwrap();
        let deleted: Vec<u64> = (1000..1040).collect();
        let slot: Arc<Mutex<Option<Vec<u64>>>> = Arc::new(Mutex::new(None));
        let (sink, reader) = (Arc::clone(&slot), Arc::clone(&c));
        let fired = AtomicBool::new(false);
        *c.window_hook.lock().unwrap() = Some(Arc::new(move || {
            if !fired.swap(true, Ordering::SeqCst) {
                let hits = reader.search(&query(), 10, None).unwrap();
                *sink.lock().unwrap() = Some(ids(&hits));
            }
        }));
        assert_eq!(c.delete_many(&deleted).unwrap(), 40);
        *c.window_hook.lock().unwrap() = None;
        let during = slot.lock().unwrap().take().unwrap();
        assert_eq!(during.len(), 10);
        assert!(
            during.iter().all(|id| *id >= 40),
            "a deleted document was returned: {during:?}"
        );
        let after = c.search(&query(), 10, None).unwrap();
        assert_eq!(after.len(), 10);
        assert!(after.iter().all(|h| h.id < 200));
        drop(c);
        drop(db);
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn concurrent_writes_and_searches_return_committed_results() {
        let dir = temp_dir("concurrent");
        let (db, c) = committed_collection(&dir, 50, 0.5);
        let acked = Arc::new(AtomicU64::new(50));
        let stop = Arc::new(AtomicBool::new(false));
        let writer = {
            let (c, acked, stop) = (Arc::clone(&c), Arc::clone(&acked), Arc::clone(&stop));
            thread::spawn(move || {
                let mut next = 50u64;
                for _ in 0..40 {
                    // Each batch scores above the committed documents, so pending writes compete for the top.
                    let records: Vec<Record> = (next..next + 150).map(|i| doc(i, 0.8)).collect();
                    c.add_many(&records).unwrap();
                    next += 150;
                    acked.store(next, Ordering::SeqCst);
                }
                stop.store(true, Ordering::SeqCst);
            })
        };
        let readers: Vec<_> = (0..4)
            .map(|_| {
                let (c, acked, stop) = (Arc::clone(&c), Arc::clone(&acked), Arc::clone(&stop));
                thread::spawn(move || {
                    let mut searches = 0u64;
                    while !stop.load(Ordering::SeqCst) {
                        // Documents acknowledged before the search are committed, so they must be available.
                        let committed_before = acked.load(Ordering::SeqCst);
                        let hits = c.search(&query(), 10, None).unwrap();
                        let issued = acked.load(Ordering::SeqCst) + 150;
                        assert!(
                            hits.len() >= 10.min(committed_before as usize),
                            "{} hits",
                            hits.len()
                        );
                        assert!(hits.len() <= 10);
                        let mut seen = HashSet::new();
                        for h in &hits {
                            assert!(h.id < issued, "an id that was never written: {}", h.id);
                            assert!(seen.insert(h.id), "document {} returned twice", h.id);
                        }
                        searches += 1;
                    }
                    searches
                })
            })
            .collect();
        writer.join().unwrap();
        let searched: u64 = readers.into_iter().map(|r| r.join().unwrap()).sum();
        assert!(searched > 0);
        assert_eq!(c.count().unwrap(), 50 + 40 * 150);
        drop(c);
        drop(db);
        fs::remove_dir_all(&dir).ok();
    }
}

#[cfg(test)]
mod admission_tests {
    use super::*;
    use crate::database::VectorDB;
    use std::sync::mpsc;

    const DIM: usize = 8;

    fn temp_dir(name: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("zenar-admission-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    fn doc(id: u64) -> Record {
        let mut vector = vec![0.0f32; DIM];
        vector[0] = 1.0;
        vector[1] = 0.001 * id as f32;
        Record {
            id,
            vector,
            text: format!("doc {id}"),
            metadata: vec![],
        }
    }

    fn collection(dir: &Path, docs: u64) -> (VectorDB, Arc<Collection>) {
        let db = VectorDB::open(dir).unwrap();
        let c = db.create_collection("c", Some(DIM), 4).unwrap();
        if docs > 0 {
            let records: Vec<Record> = (0..docs).map(doc).collect();
            c.add_many(&records).unwrap();
        }
        (db, c)
    }

    /// Waits for a condition that another thread will make true. Fails after ten seconds, so a broken gate
    /// fails the test rather than hanging it.
    fn until(cond: impl Fn() -> bool, what: &str) {
        let start = Instant::now();
        while !cond() {
            assert!(
                start.elapsed() < Duration::from_secs(10),
                "timed out waiting for {what}"
            );
            thread::yield_now();
        }
    }

    /// Takes a read guard on another thread and waits for it, for at most ten seconds, so a gate that never opens
    /// fails the test instead of hanging it. Returns whether the read was granted.
    fn read_within_ten_seconds(c: &Arc<Collection>) -> bool {
        let (tx, rx) = mpsc::channel();
        let cr = Arc::clone(c);
        thread::spawn(move || {
            let granted = cr.read_state().is_ok();
            let _ = tx.send(granted);
        });
        rx.recv_timeout(Duration::from_secs(10)).unwrap_or(false)
    }

    /// Case 1: Readers stay concurrent while no writer is announced: two readers hold the lock at the same time.
    #[test]
    fn readers_stay_concurrent_when_no_writer_waits() {
        let dir = temp_dir("concurrent-readers");
        let (db, c) = collection(&dir, 10);
        let (a_in, a_in_rx) = mpsc::channel::<()>();
        let (b_in, b_in_rx) = mpsc::channel::<()>();
        let c1 = Arc::clone(&c);
        let first = thread::spawn(move || {
            let guard = c1.read_state().unwrap();
            a_in.send(()).unwrap();
            b_in_rx
                .recv_timeout(Duration::from_secs(10))
                .expect("the second reader was blocked by the first");
            drop(guard);
        });
        let c2 = Arc::clone(&c);
        let second = thread::spawn(move || {
            let guard = c2.read_state().unwrap();
            b_in.send(()).unwrap();
            a_in_rx
                .recv_timeout(Duration::from_secs(10))
                .expect("the first reader was blocked by the second");
            drop(guard);
        });
        first.join().unwrap();
        second.join().unwrap();
        assert_eq!(c.gate_waits(), 0, "no reader waited at the gate");
        drop(c);
        drop(db);
        fs::remove_dir_all(&dir).ok();
    }

    /// Cases 2 and 3: A writer that announces itself closes the gate: a reader arriving after it waits, and runs only after
    /// the writer has held the lock. The reader that already held the lock finishes first.
    #[test]
    fn a_waiting_writer_stops_later_readers_and_existing_readers_finish() {
        let dir = temp_dir("writer-gate");
        let (db, c) = collection(&dir, 10);
        let order = Arc::new(Mutex::new(Vec::<&'static str>::new()));
        let held = c.read_state().unwrap();

        let (cw, ow) = (Arc::clone(&c), Arc::clone(&order));
        let writer = thread::spawn(move || {
            let guard = cw.write_state_op("test").unwrap();
            ow.lock().unwrap().push("writer");
            drop(guard);
        });
        until(|| c.waiting_writers() == 1, "the writer to announce itself");

        let (cr, or) = (Arc::clone(&c), Arc::clone(&order));
        let reader = thread::spawn(move || {
            let guard = cr.read_state().unwrap();
            or.lock().unwrap().push("reader");
            drop(guard);
        });
        until(
            || c.gate_waits() == 1,
            "the new reader to queue behind the writer",
        );
        assert!(
            order.lock().unwrap().is_empty(),
            "the writer or the reader ran while the first reader held the lock"
        );

        drop(held);
        writer.join().unwrap();
        // Bounded: a reader that never gets in fails here rather than blocking the join.
        until(
            || order.lock().unwrap().len() == 2,
            "the reader to run after the writer",
        );
        reader.join().unwrap();
        assert_eq!(*order.lock().unwrap(), vec!["writer", "reader"]);
        assert_eq!(c.waiting_writers(), 0);
        drop(c);
        drop(db);
        fs::remove_dir_all(&dir).ok();
    }

    /// Case 4: Several announced writers are counted, the gate stays closed until the last one has its lock, and readers
    /// resume after all of them finish.
    #[test]
    fn multiple_waiting_writers_are_counted_and_readers_resume_after_them() {
        let dir = temp_dir("many-writers");
        let (db, c) = collection(&dir, 10);
        let held = c.read_state().unwrap();
        let mut writers = Vec::new();
        for _ in 0..2 {
            let cw = Arc::clone(&c);
            writers.push(thread::spawn(move || {
                let guard = cw.write_state_op("test").unwrap();
                drop(guard);
            }));
        }
        until(
            || c.waiting_writers() == 2,
            "both writers to announce themselves",
        );
        drop(held);
        for w in writers {
            w.join().unwrap();
        }
        assert_eq!(c.waiting_writers(), 0, "every announcement was released");
        let waits = c.gate_waits();
        assert!(
            read_within_ten_seconds(&c),
            "a reader after the writers was blocked"
        );
        assert_eq!(
            c.gate_waits(),
            waits,
            "a reader after the writers did not wait"
        );
        drop(c);
        drop(db);
        fs::remove_dir_all(&dir).ok();
    }

    /// Case 5: Accounting is restored when a write fails, and when an announcement is unwound by a panic.
    #[test]
    fn accounting_is_restored_after_an_error_and_after_a_panic() {
        let dir = temp_dir("accounting");
        let (db, c) = collection(&dir, 10);
        c.close();
        assert!(
            c.write_state_op("test").is_err(),
            "a closed collection refuses writes"
        );
        assert_eq!(
            c.waiting_writers(),
            0,
            "the failed write released its announcement"
        );
        assert!(
            !read_within_ten_seconds(&c),
            "a closed collection refuses reads"
        );
        drop(c);
        drop(db);
        fs::remove_dir_all(&dir).ok();

        let gate = Admission::new();
        let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _announced = gate.announce();
            panic!("unwinding through an announcement");
        }));
        assert!(unwound.is_err());
        assert_eq!(
            gate.waiting_writers(),
            0,
            "unwinding released the announcement"
        );
        gate.admit_reader();
    }

    /// Case 6: Readers resume once the waiting writers have finished, so admission never stays closed.
    #[test]
    fn readers_resume_after_all_waiting_writers_finish() {
        let dir = temp_dir("resume");
        let (db, c) = collection(&dir, 10);
        for _ in 0..3 {
            let guard = c.write_state_op("test").unwrap();
            drop(guard);
            let (tx, rx) = mpsc::channel();
            let cr = Arc::clone(&c);
            let reader = thread::spawn(move || {
                let guard = cr.read_state().unwrap();
                tx.send(guard.committed_len).unwrap();
            });
            rx.recv_timeout(Duration::from_secs(10))
                .expect("a reader blocked after the writers finished");
            reader.join().unwrap();
        }
        drop(c);
        drop(db);
        fs::remove_dir_all(&dir).ok();
    }

    /// Case 7: A reader that re-acquires the lock as soon as it can does not starve a writer: the writer completes its
    /// operations while the reader keeps running.
    #[test]
    fn a_continuous_reader_cannot_starve_a_writer() {
        let dir = temp_dir("starvation");
        let (db, c) = collection(&dir, 10);
        let stop = Arc::new(AtomicBool::new(false));
        let (cr, sr) = (Arc::clone(&c), Arc::clone(&stop));
        let (reads_tx, reads_rx) = mpsc::channel();
        let reader = thread::spawn(move || {
            let mut reads = 0u64;
            while !sr.load(Ordering::SeqCst) {
                let guard = cr.read_state().unwrap();
                std::hint::black_box(guard.docs.live_count());
                drop(guard);
                reads += 1;
            }
            let _ = reads_tx.send(reads);
        });
        let (tx, rx) = mpsc::channel();
        let cw = Arc::clone(&c);
        let writer = thread::spawn(move || {
            for _ in 0..100 {
                let guard = cw.write_state_op("test").unwrap();
                drop(guard);
            }
            tx.send(()).unwrap();
        });
        rx.recv_timeout(Duration::from_secs(60))
            .expect("the writer was starved by a continuous reader");
        writer.join().unwrap();
        stop.store(true, Ordering::SeqCst);
        let reads = reads_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("the reader did not finish once the writer was done");
        assert!(reads > 0);
        reader.join().unwrap();
        drop(c);
        drop(db);
        fs::remove_dir_all(&dir).ok();
    }

    /// Case 8: Concurrent writers keep group commit correct: every acknowledged document is committed and findable, and
    /// writes are grouped.
    #[test]
    fn concurrent_writes_keep_group_commit_correct() {
        let dir = temp_dir("group-commit");
        let (db, c) = collection(&dir, 50);
        let stop = Arc::new(AtomicBool::new(false));
        let readers: Vec<_> = (0..2)
            .map(|_| {
                let (cr, sr) = (Arc::clone(&c), Arc::clone(&stop));
                thread::spawn(move || {
                    while !sr.load(Ordering::SeqCst) {
                        let hits = cr
                            .search(&[1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], 10, None)
                            .unwrap();
                        assert!(hits.len() <= 10);
                    }
                })
            })
            .collect();
        let writers: Vec<_> = (0..4u64)
            .map(|w| {
                let cw = Arc::clone(&c);
                thread::spawn(move || {
                    for b in 0..20u64 {
                        let first = 1000 + (w * 20 + b) * 25;
                        let records: Vec<Record> = (first..first + 25).map(doc).collect();
                        cw.add_many(&records).unwrap();
                    }
                })
            })
            .collect();
        for w in writers {
            w.join().unwrap();
        }
        stop.store(true, Ordering::SeqCst);
        for r in readers {
            r.join().unwrap();
        }
        assert_eq!(c.count().unwrap(), 50 + 4 * 20 * 25);
        for id in [1000u64, 1499, 1975] {
            assert!(
                c.get(id).unwrap().is_some(),
                "acknowledged document {id} is missing"
            );
        }
        let stats = c.write_stats();
        assert!(stats.sync_groups >= 1);
        assert!(
            stats.synced_writes >= 80,
            "every add was grouped and synced: {stats:?}"
        );
        drop(c);
        drop(db);
        fs::remove_dir_all(&dir).ok();
    }

    /// Case 9: Delete and the compaction swap take the same admission as the other writes.
    #[test]
    fn delete_and_compaction_swap_use_the_same_admission() {
        let dir = temp_dir("delete-swap");
        let (db, c) = collection(&dir, 20);
        let before = c.writer_announcements();
        assert_eq!(c.delete_many(&[0, 1]).unwrap(), 2);
        assert!(
            c.writer_announcements() > before,
            "delete did not announce itself"
        );
        let before = c.writer_announcements();
        c.compact_now().unwrap();
        assert!(
            c.writer_announcements() > before,
            "the compaction swap did not announce itself"
        );
        assert_eq!(c.count().unwrap(), 18);
        drop(c);
        drop(db);
        fs::remove_dir_all(&dir).ok();
    }
}

/// Checks the lock architecture on the source of this file. A textual check cannot replace the compiler, which already
/// stops `Core` from naming `Collection`, and the function-pointer signatures of `with_read` and `with_write`. This
/// check guards the rules the types do not state: no lock token in `Core`, and lock acquisition only in the helpers.
#[cfg(test)]
mod lock_architecture_tests {
    /// The production code of this file: everything before the test module.
    fn production_source() -> &'static str {
        let src = include_str!("collection.rs");
        let end = src
            .find("#[cfg(test)]\nmod tests {")
            .expect("the test module starts after the production code");
        &src[..end]
    }

    /// Whether `line` names the type `word`, as a whole word.
    fn names(line: &str, word: &str) -> bool {
        line.match_indices(word).any(|(i, _)| {
            let before = line[..i].chars().next_back();
            let after = line[i + word.len()..].chars().next();
            !before.is_some_and(|c| c.is_alphanumeric() || c == '_')
                && !after.is_some_and(|c| c.is_alphanumeric() || c == '_')
        })
    }

    /// The lines of `text` that are code, not comments.
    fn code_lines(text: &str) -> impl Iterator<Item = &str> {
        text.lines().filter(|l| {
            let t = l.trim_start();
            !t.starts_with("//")
        })
    }

    /// The text of the block that starts with `header` and ends at the next line that is exactly `}`.
    fn block<'a>(src: &'a str, header: &str) -> Option<&'a str> {
        let start = src.find(header)?;
        let rest = &src[start..];
        let end = rest.find("\n}\n")?;
        Some(&rest[..end])
    }

    /// Tokens that a `Core` method must not contain: each one reaches the state lock or `Collection`.
    const FORBIDDEN_IN_CORE: &[&str] = &[
        "read_state",
        "write_state",
        "with_read",
        "with_write",
        "self.state",
        "Collection",
        "self_ref",
        "submit(",
        "start_compaction",
        "Weak",
    ];

    /// The methods that may take the state lock directly, or call a method that does. Every other method reaches the
    /// lock through `with_read` or `with_write`, which hand the guard to a function pointer on `Core`.
    const MAY_LOCK: &[&str] = &[
        "read_state",
        "write_state",
        "write_state_op",
        "with_read",
        "with_write",
        "close",
    ];

    /// Returns the first problem in the `Core` code of `src`, or `None`.
    fn core_problem(src: &str) -> Option<String> {
        let core_struct = block(src, "struct Core {")?;
        for line in code_lines(core_struct) {
            if names(line, "Collection") || names(line, "RwLock") || names(line, "Weak") {
                return Some(format!("struct Core holds a route to the lock: {line}"));
            }
        }
        let core_impl = block(src, "impl Core {")?;
        for line in code_lines(core_impl) {
            for token in FORBIDDEN_IN_CORE {
                if names(line, token)
                    || (token.ends_with('(') && line.contains(token))
                    || (token.contains('.') && line.contains(token))
                {
                    return Some(format!("impl Core uses {token:?}: {line}"));
                }
            }
        }
        None
    }

    /// Returns the name of each method in `src` that takes the state lock, directly or by a call, and is not in
    /// `MAY_LOCK`. Methods are the four-space-indented `fn` lines of the impl blocks.
    fn unexpected_lockers(src: &str) -> Vec<String> {
        let mut current = String::new();
        let mut bad = Vec::new();
        for line in code_lines(src) {
            if let Some(rest) = line.strip_prefix("    ") {
                let rest = rest
                    .strip_prefix("pub(crate) ")
                    .or_else(|| rest.strip_prefix("pub "))
                    .unwrap_or(rest);
                if let Some(after) = rest.strip_prefix("fn ") {
                    current = after
                        .split(|c: char| !(c.is_alphanumeric() || c == '_'))
                        .next()
                        .unwrap_or("")
                        .to_string();
                }
            }
            // `self.state.get_mut()` takes no lock: it needs `&mut self`.
            let takes_lock = [
                "read_state(",
                "write_state(",
                "write_state_op(",
                "self.state.read(",
                "self.state.write(",
            ]
            .iter()
            .any(|t| line.contains(t));
            if takes_lock && !MAY_LOCK.contains(&current.as_str()) && !bad.contains(&current) {
                bad.push(current.clone());
            }
        }
        bad
    }

    #[test]
    fn core_holds_no_route_to_the_lock() {
        let src = production_source();
        assert_eq!(core_problem(src), None);
    }

    #[test]
    fn only_the_lock_helpers_take_the_lock() {
        let src = production_source();
        assert_eq!(unexpected_lockers(src), Vec::<String>::new());
    }

    /// The checks above must catch a violation. Each case plants one in a copy of the source.
    #[test]
    fn the_checks_catch_planted_violations() {
        let src = production_source();
        let core = "impl Core {\n";
        let planted = src.replacen(core, "impl Core {\n    fn leak(&self) { self.count_locked_probe(); let _ = self.read_state(); }\n", 1);
        assert!(core_problem(&planted).is_some());
        let stray = src.replacen(
            "    pub fn count(",
            "    fn stray_probe(&self) { let _s = self.read_state(); }\n    fn count(",
            1,
        );
        assert!(unexpected_lockers(&stray).contains(&"stray_probe".to_string()));
    }
}
