use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use crate::document::{DocumentRow, MetadataStore, Status, is_valid_field_name};
use crate::error::ZenarError;
use crate::filter::{Filter, compile};
use crate::storage::{VectorStore, validate_vector};
use crate::value::{FieldKind, MetaValue};

/// Largest id the metadata store can hold (SQL BIGINT).
pub const MAX_ID: u64 = i64::MAX as u64;

/// Adds between index syncs (spec section 86). Syncing on every add made ingest
/// quadratic: about 20 ms per row at 10K rows. Adds since the last sync may be
/// lost on a crash. Recovery removes their rows, and `flush()` makes them durable.
pub const SYNC_INTERVAL: usize = 1000;

/// One search result (spec section 18).
#[derive(Debug, Clone, PartialEq)]
pub struct Hit {
    pub id: u64,
    pub score: f32,
    pub text: String,
    pub metadata: Vec<(String, MetaValue)>,
}

/// Counts of repairs made when a collection was reconciled on open (spec section 26).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RecoveryReport {
    /// PENDING rows whose add never completed: rolled back.
    pub rolled_back_pending: usize,
    /// DELETING rows whose delete never completed: finished.
    pub finished_deletes: usize,
    /// READY rows with no vector in the index: removed, since the vector is gone.
    pub orphaned_rows_removed: usize,
    /// Vectors in the index with no metadata row: removed.
    pub orphaned_vectors_removed: usize,
}

impl RecoveryReport {
    pub fn is_clean(&self) -> bool {
        *self == RecoveryReport::default()
    }
}

struct State {
    store: VectorStore,
    /// Metadata schema, locked by the first add that carries metadata.
    fields: Vec<(String, FieldKind)>,
    /// Vectors added since the index was last synced to disk.
    unsynced: usize,
}

/// A named set of vectors plus their documents and metadata.
///
/// Writes are serialized behind one lock. Reads run concurrently with each other.
pub struct Collection {
    name: String,
    table: String,
    index_path: PathBuf,
    meta: Arc<MetadataStore>,
    state: RwLock<State>,
}

impl Collection {
    pub(crate) fn new(
        name: &str,
        index_path: PathBuf,
        meta: Arc<MetadataStore>,
        store: VectorStore,
        fields: Vec<(String, FieldKind)>,
    ) -> Self {
        Self {
            name: name.to_string(),
            table: MetadataStore::table_name(name),
            index_path,
            meta,
            state: RwLock::new(State {
                store,
                fields,
                unsynced: 0,
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

    /// Adds one document. The vector goes into the index, the index is synced at
    /// each sync boundary, and then the row is written as READY. Crash handling is
    /// described in STORAGE.md and in `reconcile`.
    pub fn add(
        &self,
        id: u64,
        vector: &[f32],
        text: &str,
        metadata: Vec<(String, MetaValue)>,
    ) -> Result<(), ZenarError> {
        check_id(id)?;
        let mut state = self.write_state()?;
        validate_vector(vector, state.store.dim())?;
        let first_vector = state.store.dim().is_none();
        let (new_fields, values) = resolve_metadata(&state.fields, metadata)?;

        if state.store.contains(id) || self.meta.status_of(&self.table, id)?.is_some() {
            return Err(ZenarError::DuplicateId(id));
        }
        if !new_fields.is_empty() {
            self.meta.add_field_columns(&self.table, &new_fields)?;
            state.fields.extend(new_fields);
            self.meta.set_fields(&self.name, &state.fields)?;
        }

        state.store.add(vector, id)?;
        if state.unsynced + 1 >= SYNC_INTERVAL {
            if let Err(e) = state.store.sync(&self.index_path) {
                state.store.remove(id);
                return Err(e);
            }
            state.unsynced = 0;
        } else {
            state.unsynced += 1;
        }
        // The row is written once, after the vector is in the index, so there is no
        // UPDATE per add. A crash before the row exists leaves a stray vector, and
        // recovery removes it. A crash after the row exists but before the next index
        // sync leaves a READY row with no vector, and recovery removes that too.
        if let Err(e) = self
            .meta
            .insert_document(&self.table, id, text, Status::Ready, &values)
        {
            state.store.remove(id);
            return Err(e);
        }
        if first_vector && let Some(dim) = state.store.dim() {
            self.meta.set_dimension(&self.name, dim)?;
        }
        Ok(())
    }

    /// Deletes one document. The vector is removed and synced before the row
    /// is dropped, so a deleted vector cannot survive on disk after `delete` returns (spec section 67).
    pub fn delete(&self, id: u64) -> Result<(), ZenarError> {
        let mut state = self.write_state()?;
        if self.meta.status_of(&self.table, id)?.is_none() {
            return Err(ZenarError::VectorNotFound(id));
        }
        self.meta.set_status(&self.table, id, Status::Deleting)?;
        state.store.remove(id);
        state.store.sync(&self.index_path)?;
        state.unsynced = 0;
        self.meta.delete_document(&self.table, id)
    }

    /// Makes every add so far durable. Returns once the index file is synced.
    pub fn flush(&self) -> Result<(), ZenarError> {
        let mut state = self.write_state()?;
        if state.unsynced > 0 && state.store.dim().is_some() {
            state.store.sync(&self.index_path)?;
            state.unsynced = 0;
        }
        Ok(())
    }

    /// Calibrates the TQ+ quantizer from a sample (spec section 88).
    pub fn calibrate(&self, sample: &[f32]) -> Result<(), ZenarError> {
        let mut state = self.write_state()?;
        state.store.calibrate(sample)?;
        state.store.sync(&self.index_path)?;
        state.unsynced = 0;
        Ok(())
    }

    pub fn get(&self, id: u64) -> Result<Option<DocumentRow>, ZenarError> {
        let state = self.read_state()?;
        Ok(self
            .meta
            .fetch_documents(&self.table, &state.fields, &[id])?
            .into_iter()
            .next())
    }

    /// Returns found documents in the order of `ids`. Missing ids are skipped.
    pub fn get_many(&self, ids: &[u64]) -> Result<Vec<DocumentRow>, ZenarError> {
        let state = self.read_state()?;
        let mut by_id: HashMap<u64, DocumentRow> = self
            .meta
            .fetch_documents(&self.table, &state.fields, ids)?
            .into_iter()
            .map(|d| (d.id, d))
            .collect();
        Ok(ids.iter().filter_map(|id| by_id.remove(id)).collect())
    }

    pub fn count(&self) -> Result<usize, ZenarError> {
        self.meta.count_ready(&self.table)
    }

    /// Top-`top_k` search, optionally filtered (spec sections 18-22).
    ///
    /// * No filter: TurboVec only. Metadata is fetched for the top-K alone.
    /// * Filter: SQL returns candidate ids, which are narrowed to ids still in
    ///   the index (spec section 89), then TurboVec searches with that allowlist.
    /// * Empty candidate set: returns without touching TurboVec (spec section 21).
    pub fn search(
        &self,
        query: &[f32],
        top_k: usize,
        filter: Option<&Filter>,
    ) -> Result<Vec<Hit>, ZenarError> {
        if top_k == 0 {
            return Err(ZenarError::InvalidArgument(
                "top_k must be greater than 0".into(),
            ));
        }
        let state = self.read_state()?;
        if state.store.dim().is_none() {
            return Ok(Vec::new());
        }
        validate_vector(query, state.store.dim())?;

        let (scores, ids) = match filter {
            None => {
                if state.store.is_empty() {
                    return Ok(Vec::new());
                }
                let k = top_k.min(state.store.len());
                state.store.search(query, k, None)?
            }
            Some(filter) => {
                let compiled = compile(filter, &state.fields)?;
                let candidates =
                    self.meta
                        .matching_ids(&self.table, &compiled.sql, compiled.params)?;
                let live: Vec<u64> = candidates
                    .into_iter()
                    .filter(|id| state.store.contains(*id))
                    .collect();
                if live.is_empty() {
                    return Ok(Vec::new());
                }
                let k = top_k.min(live.len());
                state.store.search(query, k, Some(&live))?
            }
        };

        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut docs: HashMap<u64, DocumentRow> = self
            .meta
            .fetch_documents(&self.table, &state.fields, &ids)?
            .into_iter()
            .map(|d| (d.id, d))
            .collect();
        Ok(ids
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
            .collect())
    }

    /// Reconciles the metadata store and the vector index after a restart (spec section 26).
    pub fn reconcile(&self) -> Result<RecoveryReport, ZenarError> {
        let mut state = self.write_state()?;
        let mut report = RecoveryReport::default();

        for id in self.meta.ids_with_status(&self.table, Status::Pending)? {
            state.store.remove(id);
            self.meta.delete_document(&self.table, id)?;
            report.rolled_back_pending += 1;
        }
        for id in self.meta.ids_with_status(&self.table, Status::Deleting)? {
            state.store.remove(id);
            self.meta.delete_document(&self.table, id)?;
            report.finished_deletes += 1;
        }

        let ready: HashSet<u64> = self
            .meta
            .ids_with_status(&self.table, Status::Ready)?
            .into_iter()
            .collect();
        for id in ready
            .iter()
            .copied()
            .filter(|id| !state.store.contains(*id))
        {
            self.meta.delete_document(&self.table, id)?;
            report.orphaned_rows_removed += 1;
        }

        let rows: HashSet<u64> = self.meta.all_ids(&self.table)?.into_iter().collect();
        let stray: Vec<u64> = state.store.ids().filter(|id| !rows.contains(id)).collect();
        for id in stray {
            state.store.remove(id);
            report.orphaned_vectors_removed += 1;
        }

        if !report.is_clean() {
            state.store.sync(&self.index_path)?;
            state.unsynced = 0;
        }
        Ok(report)
    }

    /// Writes the collection's current index to disk (used when a new collection is created).
    pub(crate) fn persist(&self) -> Result<(), ZenarError> {
        let mut state = self.write_state()?;
        state.store.sync(&self.index_path)?;
        state.unsynced = 0;
        Ok(())
    }

    fn read_state(&self) -> Result<std::sync::RwLockReadGuard<'_, State>, ZenarError> {
        self.state
            .read()
            .map_err(|_| ZenarError::VectorStoreError("collection lock poisoned".into()))
    }

    fn write_state(&self) -> Result<std::sync::RwLockWriteGuard<'_, State>, ZenarError> {
        self.state
            .write()
            .map_err(|_| ZenarError::VectorStoreError("collection lock poisoned".into()))
    }
}

impl Drop for Collection {
    fn drop(&mut self) {
        // Best effort: a clean shutdown should not lose the last batch. Errors cannot be reported from Drop.
        if let Ok(state) = self.state.get_mut()
            && state.unsynced > 0
            && state.store.dim().is_some()
        {
            let _ = state.store.sync(&self.index_path);
        }
    }
}

pub fn check_id(id: u64) -> Result<(), ZenarError> {
    if id > MAX_ID {
        return Err(ZenarError::InvalidArgument(format!(
            "id must be at most {MAX_ID}"
        )));
    }
    Ok(())
}

/// Validates metadata against the locked schema (or infers it when unlocked).
/// Returns the newly introduced fields and the values to store.
type Schema = Vec<(String, FieldKind)>;
type Metadata = Vec<(String, MetaValue)>;

/// Returns the newly introduced fields and the values to store.
fn resolve_metadata(
    schema: &[(String, FieldKind)],
    metadata: Metadata,
) -> Result<(Schema, Metadata), ZenarError> {
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
                // The first add that carries metadata defines the schema (spec section 48 style).
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

/// Coerces a value to a field's locked kind. Integers widen to floats; nothing else converts.
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
    use crate::filter::Filter;
    use std::fs;

    fn temp_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("zenar-rec-{tag}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    fn v(seed: f32) -> Vec<f32> {
        (0..8).map(|i| seed + i as f32).collect()
    }

    /// Spec section 89: a READY row whose vector is gone from the index must not
    /// make filtered search fail. The stale id is excluded and the rest still return.
    #[test]
    fn stale_candidate_id_is_excluded_not_an_error() {
        let dir = temp_dir("stale");
        let db = VectorDB::open(&dir).unwrap();
        let c = db.create_collection("stale", Some(8), 4).unwrap();
        c.add(
            1,
            &v(1.0),
            "one",
            vec![("tenant".into(), MetaValue::Text("A".into()))],
        )
        .unwrap();
        c.add(
            2,
            &v(2.0),
            "two",
            vec![("tenant".into(), MetaValue::Text("A".into()))],
        )
        .unwrap();
        // Diverge: remove id 1 from the index only, leaving its READY row.
        c.state.write().unwrap().store.remove(1);

        let f = Filter::Eq {
            field: "tenant".into(),
            value: MetaValue::Text("A".into()),
        };
        let hits = c.search(&v(1.0), 10, Some(&f)).unwrap();
        assert_eq!(hits.iter().map(|h| h.id).collect::<Vec<_>>(), vec![2]);
        fs::remove_dir_all(&dir).ok();
    }

    /// A crash after the PENDING row and in-memory vector, before sync, must roll back on reopen.
    #[test]
    fn crash_before_sync_rolls_back_pending() {
        let dir = temp_dir("pending");
        {
            let db = VectorDB::open(&dir).unwrap();
            let c = db.create_collection("pend", Some(8), 4).unwrap();
            c.add(1, &v(1.0), "kept", vec![]).unwrap();
            // Simulate the crash window: PENDING row written, vector added in memory, no sync.
            c.meta
                .insert_document(&c.table, 2, "lost", Status::Pending, &[])
                .unwrap();
            c.state.write().unwrap().store.add(&v(2.0), 2).unwrap();
            // Process "dies" here: dropping without sync leaves the file as it was.
        }
        let db = VectorDB::open(&dir).unwrap();
        let report = db.recovery_report();
        assert_eq!(report.rolled_back_pending, 1);
        let c = db.get_collection("pend").unwrap();
        assert!(c.get(2).unwrap().is_none());
        assert!(c.get(1).unwrap().is_some());
        assert_eq!(c.count().unwrap(), 1);
        fs::remove_dir_all(&dir).ok();
    }

    /// A delete that crashed after the DELETING mark must be finished on reopen.
    #[test]
    fn crash_mid_delete_is_finished_on_reopen() {
        let dir = temp_dir("deleting");
        {
            let db = VectorDB::open(&dir).unwrap();
            let c = db.create_collection("del", Some(8), 4).unwrap();
            c.add(1, &v(1.0), "gone", vec![]).unwrap();
            c.meta.set_status(&c.table, 1, Status::Deleting).unwrap();
        }
        let db = VectorDB::open(&dir).unwrap();
        assert_eq!(db.recovery_report().finished_deletes, 1);
        assert!(db.get_collection("del").unwrap().get(1).unwrap().is_none());
        fs::remove_dir_all(&dir).ok();
    }

    /// A READY row whose vector was never durably stored is removed on reopen.
    #[test]
    fn ready_row_without_synced_vector_is_removed() {
        let dir = temp_dir("orphan");
        {
            let db = VectorDB::open(&dir).unwrap();
            db.create_collection("orph", Some(8), 4).unwrap();
            let c = db.get_collection("orph").unwrap();
            c.meta
                .insert_document(&c.table, 5, "orphan", Status::Ready, &[])
                .unwrap();
        }
        let db = VectorDB::open(&dir).unwrap();
        assert_eq!(db.recovery_report().orphaned_rows_removed, 1);
        assert_eq!(db.get_collection("orph").unwrap().count().unwrap(), 0);
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn duplicate_id_rejected_and_search_is_exact() {
        let dir = temp_dir("dup");
        let db = VectorDB::open(&dir).unwrap();
        let c = db.create_collection("dup", Some(8), 4).unwrap();
        c.add(1, &v(1.0), "a", vec![]).unwrap();
        assert_eq!(
            c.add(1, &v(1.0), "b", vec![]),
            Err(ZenarError::DuplicateId(1))
        );
        let hits = c.search(&v(1.0), 5, None).unwrap();
        assert_eq!(hits.len(), 1);
        fs::remove_dir_all(&dir).ok();
    }
}
