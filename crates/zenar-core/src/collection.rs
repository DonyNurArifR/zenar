use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::catalog::Catalog;
use crate::docstore::{DocRef, DocStore, DocumentRow, FieldStats};
use crate::error::ZenarError;
use crate::filter::{Filter, validate};
use crate::names::is_valid_field_name;
use crate::storage::{VectorStore, validate_dimension, validate_vector};
use crate::value::{FieldKind, MetaValue};

/// Largest id the store accepts. Ids are stored as i64 in the index files.
pub const MAX_ID: u64 = i64::MAX as u64;

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
    /// Set when an in-memory change could not be made durable or undone. Every
    /// operation then fails until the database is reopened, which replays the
    /// durable state.
    poisoned: Option<String>,
}

/// A named set of vectors plus their documents and metadata.
///
/// Writes are serialized behind one lock. Reads run concurrently with each other.
pub struct Collection {
    name: String,
    /// Field statistics for discovery. Cleared by every write, see `write_state`.
    stats: Mutex<Option<Arc<Vec<FieldStats>>>>,
    index_path: PathBuf,
    catalog: Arc<Catalog>,
    /// Sync the document log on every commit, not only at sync boundaries.
    durable: bool,
    state: RwLock<State>,
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
        Self {
            name: name.to_string(),
            stats: Mutex::new(None),
            index_path,
            catalog,
            durable,
            state: RwLock::new(State {
                store,
                docs,
                unsynced: 0,
                poisoned: None,
            }),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn dimension(&self) -> Option<usize> {
        self.read_state().map(|s| s.store.dim()).unwrap_or(None)
    }

    pub fn bit_width(&self) -> usize {
        self.read_state().map(|s| s.store.bit_width()).unwrap_or(0)
    }

    /// Adds one document.
    ///
    /// Order: stage the document write, add the vector in memory, and at a sync boundary
    /// sync the index and the log. Only then make the document visible. A crash before the
    /// boundary can lose the newest adds, and reconciliation removes the rest (STORAGE.md).
    pub fn add(
        &self,
        id: u64,
        vector: &[f32],
        text: &str,
        metadata: Vec<(String, MetaValue)>,
    ) -> Result<(), ZenarError> {
        check_id(id)?;
        let mut state = self.write_state()?;
        healthy(&state)?;
        validate_vector(vector, state.store.dim())?;
        let first_vector = state.store.dim().is_none();
        let (new_fields, values) = resolve_metadata(state.docs.fields(), metadata)?;
        if state.store.contains(id) || state.docs.contains(id) {
            return Err(ZenarError::DuplicateId(id));
        }
        self.extend_schema(&mut state, &new_fields)?;

        let staged = state.docs.stage_puts(&[DocRef {
            id,
            text,
            metadata: &values,
        }])?;
        if let Err(e) = state.store.add(vector, id) {
            state.docs.undo(staged)?;
            return Err(e);
        }
        if self.durable || state.unsynced + 1 >= SYNC_INTERVAL {
            if let Err(e) = self.sync_both(&mut state) {
                let _ = state.store.remove(id);
                let _ = state.docs.undo(staged);
                return Err(e);
            }
        } else {
            state.unsynced += 1;
        }
        state.docs.commit(staged);
        if first_vector {
            self.record_dimension(&state)?;
        }
        Ok(())
    }

    /// Adds many documents atomically. Either every record is added or none is.
    ///
    /// 1. Validate every record before any write: ids, vectors, metadata, duplicates
    ///    within the batch and against stored documents.
    /// 2. Extend the metadata schema if the batch introduces fields.
    /// 3. Write all documents to the log and all vectors to the index.
    /// 4. Sync the index and the log once.
    /// 5. Make the documents visible.
    ///
    /// Any failure in 3 or 4 undoes the batch. The schema change in step 2 stays, which is
    /// harmless because no document uses the new fields.
    pub fn add_many(&self, records: &[Record]) -> Result<usize, ZenarError> {
        if records.is_empty() {
            return Ok(0);
        }
        let mut state = self.write_state()?;
        healthy(&state)?;
        let first_vector = state.store.dim().is_none();

        // 1. Validate.
        if first_vector {
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
        self.extend_schema(&mut state, &new_fields)?;

        // 3. Documents, then vectors.
        let refs: Vec<DocRef<'_>> = records
            .iter()
            .zip(prepared.iter())
            .map(|(r, values)| DocRef {
                id: r.id,
                text: r.text.as_str(),
                metadata: values.as_slice(),
            })
            .collect();
        let staged = state.docs.stage_puts(&refs)?;
        let mut added: Vec<u64> = Vec::with_capacity(records.len());
        for record in records {
            if let Err(e) = state.store.add(&record.vector, record.id) {
                self.rollback(&mut state, &added, staged);
                return Err(e);
            }
            added.push(record.id);
        }

        // 4. One sync for the whole batch.
        if let Err(e) = self.sync_both(&mut state) {
            self.rollback(&mut state, &added, staged);
            return Err(e);
        }

        // 5. Visible.
        state.docs.commit(staged);
        if first_vector {
            self.record_dimension(&state)?;
        }
        Ok(records.len())
    }

    /// Deletes one document. The vector leaves the index and the index is synced before the
    /// delete is logged, so a deleted vector is gone from disk once `delete` returns (spec section 67).
    pub fn delete(&self, id: u64) -> Result<(), ZenarError> {
        let mut state = self.write_state()?;
        healthy(&state)?;
        if !state.docs.contains(id) {
            return Err(ZenarError::VectorNotFound(id));
        }
        // From here on the in-memory state is ahead of the durable state until the delete is
        // logged and synced. Any failure poisons the collection, since it cannot be undone safely.
        if !state.store.remove(id) {
            return Err(ZenarError::VectorNotFound(id));
        }
        if let Err(e) = state.store.sync(&self.index_path) {
            return Err(poison(&mut state, e));
        }
        let staged = match state.docs.stage_delete(id) {
            Ok(staged) => staged,
            Err(e) => return Err(poison(&mut state, e)),
        };
        if let Err(e) = state.docs.sync() {
            return Err(poison(&mut state, e));
        }
        state.docs.commit(staged);
        state.unsynced = 0;
        if state.docs.should_compact() {
            // Compaction is best effort. A failure after the rename leaves the in-memory log stale,
            // so it poisons the collection. Failure before the rename changes nothing.
            if let Err(e) = state.docs.compact() {
                return Err(poison(&mut state, e));
            }
        }
        Ok(())
    }

    /// Deletes many documents with one index sync and one log sync. Either all of them are
    /// deleted or none is: every id must exist and be unique, or nothing changes.
    pub fn delete_many(&self, ids: &[u64]) -> Result<usize, ZenarError> {
        if ids.is_empty() {
            return Ok(0);
        }
        let mut state = self.write_state()?;
        healthy(&state)?;
        let mut seen = HashSet::with_capacity(ids.len());
        for &id in ids {
            if !seen.insert(id) {
                return Err(ZenarError::InvalidArgument(format!(
                    "id {id} appears twice in the batch"
                )));
            }
            if !state.docs.contains(id) {
                return Err(ZenarError::VectorNotFound(id));
            }
        }
        // From here, see `delete`: any failure poisons the collection.
        for &id in ids {
            state.store.remove(id);
        }
        if let Err(e) = state.store.sync(&self.index_path) {
            return Err(poison(&mut state, e));
        }
        let staged = match state.docs.stage_deletes(ids) {
            Ok(staged) => staged,
            Err(e) => return Err(poison(&mut state, e)),
        };
        if let Err(e) = state.docs.sync() {
            return Err(poison(&mut state, e));
        }
        state.docs.commit(staged);
        state.unsynced = 0;
        if state.docs.should_compact()
            && let Err(e) = state.docs.compact()
        {
            return Err(poison(&mut state, e));
        }
        Ok(ids.len())
    }

    /// Makes every add so far durable: the index and the document log are synced.
    pub fn flush(&self) -> Result<(), ZenarError> {
        let mut state = self.write_state()?;
        healthy(&state)?;
        if state.unsynced > 0 || self.durable {
            self.sync_both(&mut state)?;
        }
        Ok(())
    }

    /// Fits the TQ+ calibration from a sample (spec section 88).
    pub fn calibrate(&self, sample: &[f32]) -> Result<(), ZenarError> {
        let mut state = self.write_state()?;
        healthy(&state)?;
        state.store.calibrate(sample)?;
        state.store.sync(&self.index_path)?;
        state.unsynced = 0;
        Ok(())
    }

    pub fn get(&self, id: u64) -> Result<Option<DocumentRow>, ZenarError> {
        let state = self.read_state()?;
        healthy(&state)?;
        Ok(state.docs.fetch(&[id])?.into_iter().next())
    }

    /// Returns found documents in the order of `ids`. Missing ids are skipped.
    pub fn get_many(&self, ids: &[u64]) -> Result<Vec<DocumentRow>, ZenarError> {
        let state = self.read_state()?;
        healthy(&state)?;
        state.docs.fetch(ids)
    }

    pub fn count(&self) -> Result<usize, ZenarError> {
        let state = self.read_state()?;
        healthy(&state)?;
        Ok(state.docs.live_count())
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
        let state = self.read_state()?;
        healthy(&state)?;
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
                let k = top_k.min(state.store.len());
                let (scores, ids) = state.store.search(query, k, None)?;
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
            .collect();
        Ok((hits, matched))
    }

    /// Field statistics for discovery: every field with its type, counts and values.
    pub fn describe(&self) -> Result<serde_json::Value, ZenarError> {
        let state = self.read_state()?;
        healthy(&state)?;
        let stats = self.cached_stats(&state)?;
        let documents = state.docs.live_count();
        let fields: Vec<serde_json::Value> = stats
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

    /// Finds values of one field that contain `text` (case-insensitive), most frequent first.
    /// For fields with many distinct values, where listing them all is not useful.
    pub fn find_values(
        &self,
        field: &str,
        text: &str,
        limit: usize,
    ) -> Result<Vec<serde_json::Value>, ZenarError> {
        let state = self.read_state()?;
        healthy(&state)?;
        let stats = self.cached_stats(&state)?;
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
            .map(|(value, count)| serde_json::json!({"value": crate::agent::value_json(value), "count": count}))
            .collect())
    }

    /// Retrieval for an agent. `where_` is the JSON filter (see `agent`). Returns the hits, how
    /// many documents matched before `top_k`, and warnings about values that match nothing.
    pub fn retrieve(
        &self,
        query: &[f32],
        top_k: usize,
        where_: Option<&serde_json::Value>,
    ) -> Result<Retrieval, ZenarError> {
        let parsed = match where_ {
            None => crate::agent::Parsed {
                filter: None,
                warnings: Vec::new(),
            },
            Some(value) => {
                // A separate lock acquisition from the search below. Holding one read lock across
                // both could deadlock with a writer that is waiting.
                let state = self.read_state()?;
                healthy(&state)?;
                let stats = self.cached_stats(&state)?;
                crate::agent::parse(value, state.docs.fields(), &stats)?
            }
        };
        let (hits, matched) = self.search_counted(query, top_k, parsed.filter.as_ref())?;
        Ok(Retrieval {
            hits,
            matched,
            warnings: parsed.warnings,
        })
    }

    /// Fields and their values as plain text, for a prompt (see `agent::fields_prompt`).
    pub fn fields_prompt(&self) -> Result<String, ZenarError> {
        let state = self.read_state()?;
        healthy(&state)?;
        let stats = self.cached_stats(&state)?;
        Ok(crate::agent::fields_prompt(&stats))
    }

    /// Tool definition for this collection (see `agent::tool_spec`).
    pub fn tool_spec(&self, tool_name: &str) -> Result<serde_json::Value, ZenarError> {
        let state = self.read_state()?;
        healthy(&state)?;
        let stats = self.cached_stats(&state)?;
        Ok(crate::agent::tool_spec(
            tool_name,
            &self.name,
            state.docs.live_count(),
            &stats,
        ))
    }

    /// Reconciles the document log and the vector index after a restart (spec section 26).
    pub fn reconcile(&self) -> Result<RecoveryReport, ZenarError> {
        let mut state = self.write_state()?;
        healthy(&state)?;
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
            self.sync_both(&mut state)?;
        }
        Ok(report)
    }

    /// Writes the current index file (used when a new collection is created with a dimension).
    pub(crate) fn persist(&self) -> Result<(), ZenarError> {
        let mut state = self.write_state()?;
        state.store.sync(&self.index_path)?;
        state.unsynced = 0;
        Ok(())
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
    fn sync_both(&self, state: &mut State) -> Result<(), ZenarError> {
        state.store.sync(&self.index_path)?;
        state.docs.sync()?;
        state.unsynced = 0;
        Ok(())
    }

    /// Undoes a batch that failed before it became visible: removes its vectors from memory,
    /// truncates its log bytes, and syncs the index to match. If that sync fails, recovery on
    /// the next open removes the stray vectors.
    fn rollback(&self, state: &mut State, added: &[u64], staged: crate::docstore::Staged) {
        for id in added {
            state.store.remove(*id);
        }
        if !added.is_empty() {
            let _ = state.store.sync(&self.index_path);
        }
        let _ = state.docs.undo(staged);
    }

    fn read_state(&self) -> Result<RwLockReadGuard<'_, State>, ZenarError> {
        self.state
            .read()
            .map_err(|_| ZenarError::VectorStoreError("collection lock poisoned".into()))
    }

    fn write_state(&self) -> Result<RwLockWriteGuard<'_, State>, ZenarError> {
        let guard = self
            .state
            .write()
            .map_err(|_| ZenarError::VectorStoreError("collection lock poisoned".into()))?;
        // Every mutation takes the write lock first, so clearing here means no reader can store
        // statistics computed before the mutation after it has been cleared.
        if let Ok(mut cache) = self.stats.lock() {
            *cache = None;
        }
        Ok(guard)
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
}

impl Drop for Collection {
    /// A clean close syncs adds that are still unsynced. Errors cannot be reported from
    /// drop. If the sync fails, reconciliation on the next open repairs the state.
    fn drop(&mut self) {
        if let Ok(state) = self.state.get_mut()
            && state.poisoned.is_none()
            && state.unsynced > 0
        {
            let _ = state.store.sync(&self.index_path);
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
            None if schema.is_empty() => {
                new_fields.push((name.clone(), FieldKind::of(&value)));
                value
            }
            None => {
                return Err(ZenarError::InvalidMetadata(format!(
                    "field {name:?} is not part of this collection's metadata schema"
                )));
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
    fn unknown_field_after_schema_lock_rejected() {
        let schema = vec![("tenant".to_string(), FieldKind::Text)];
        let r = resolve_metadata(
            &schema,
            vec![("color".into(), MetaValue::Text("red".into()))],
        );
        assert!(matches!(r, Err(ZenarError::InvalidMetadata(_))));
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
            state.store.sync(&c.index_path).unwrap();
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
}
