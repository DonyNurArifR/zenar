use std::collections::HashMap;
use std::fs::{self, File, OpenOptions, TryLockError};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use crate::catalog::{Catalog, Entry};
use crate::collection::{Collection, RecoveryReport};
use crate::docstore::{DocStore, io_err, sync_parent};
use crate::error::ZenarError;
use crate::names::validate_collection_name;
use crate::storage::{VectorStore, validate_dimension};

/// Bit widths TurboVec supports (spec section 88).
const BIT_WIDTHS: [usize; 3] = [2, 3, 4];

/// How hard commits are made durable (spec section 86).
///
/// * `Normal` (default): the index and the document log are synced at each sync boundary
///   (every 1000 adds) and by `flush()`. Adds after the last boundary can be lost on power failure.
/// * `Full`: every add syncs the index and the log before returning. Power-safe per add, at a
///   much lower ingest rate (see docs/BENCHMARKS.md).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MetadataSync {
    #[default]
    Normal,
    Full,
}

/// An embedded, single-writer Zenar database rooted at a directory.
///
/// Layout:
/// ```text
/// <root>/LOCK                 advisory lock held while the database is open
/// <root>/catalog.json        collections: dimension, bit width, metadata schema
/// <root>/docs/<name>.zdocs    document store (text and metadata), with its indexes in memory
/// <root>/vectors/<name>.zvec  vector index (TurboVec) for each collection
/// ```
pub struct VectorDB {
    root: PathBuf,
    _lock: File,
    catalog: Arc<Catalog>,
    durable: bool,
    collections: RwLock<HashMap<String, Arc<Collection>>>,
    recovery: RecoveryReport,
}

impl Drop for VectorDB {
    /// Flushes and closes every collection. Errors cannot be reported from drop; `flush()` before
    /// closing reports them. Handles that outlive the database then fail instead of writing.
    fn drop(&mut self) {
        if let Ok(collections) = self.collections.get_mut() {
            for collection in collections.values() {
                let _ = collection.flush();
                collection.close();
            }
        }
    }
}

impl VectorDB {
    /// Opens (or creates) a database with the default `MetadataSync::Normal`.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, ZenarError> {
        Self::open_with(path, MetadataSync::default())
    }

    /// Opens (or creates) a database. Fails with `DatabaseLocked` if another process, or
    /// another handle in this process, already has it open.
    pub fn open_with(path: impl AsRef<Path>, sync: MetadataSync) -> Result<Self, ZenarError> {
        let root = absolute(path.as_ref())?;
        fs::create_dir_all(root.join("vectors")).map_err(io_err)?;
        fs::create_dir_all(root.join("docs")).map_err(io_err)?;
        let lock = acquire_lock(&root)?;
        let catalog = Arc::new(Catalog::open(&root.join("catalog.json"))?);
        let durable = sync == MetadataSync::Full;

        let mut collections = HashMap::new();
        let mut recovery = RecoveryReport::default();
        for name in catalog.names()? {
            let entry = catalog
                .get(&name)?
                .ok_or_else(|| ZenarError::RecoveryError(format!("{name} vanished")))?;
            let entry = settle_compaction(&catalog, &root, &name, entry)?;
            let schema = entry.schema()?;
            let docs = open_or_create_docs(&root, &name, schema, entry.durable_log_len)?;
            let index_path = index_path(&root, &name);
            let store = if index_path.exists() {
                VectorStore::load(&index_path, entry.dimension)?
            } else {
                match entry.dimension {
                    Some(dim) => VectorStore::new(dim, entry.bit_width)?,
                    None => VectorStore::new_lazy(entry.bit_width)?,
                }
            };
            if docs.log_len() < entry.durable_log_len || store.len() < entry.durable_vectors {
                return Err(ZenarError::RecoveryError(format!(
                    "collection {name:?}: the document log or the vector index is shorter than at its \
                     last sync (log {} of {} bytes, vectors {} of {}); a file was truncated or removed \
                     outside Zenar",
                    docs.log_len(),
                    entry.durable_log_len,
                    store.len(),
                    entry.durable_vectors,
                )));
            }
            let collection = Collection::new(
                &name,
                index_path,
                Arc::clone(&catalog),
                store,
                docs,
                durable,
            );
            let report = collection.reconcile()?;
            recovery.orphaned_rows_removed += report.orphaned_rows_removed;
            recovery.orphaned_vectors_removed += report.orphaned_vectors_removed;
            collections.insert(name, Arc::new(collection));
        }

        Ok(Self {
            root,
            _lock: lock,
            catalog,
            durable,
            collections: RwLock::new(collections),
            recovery,
        })
    }

    pub fn path(&self) -> &Path {
        &self.root
    }

    /// Repairs performed when the database was opened.
    pub fn recovery_report(&self) -> RecoveryReport {
        self.recovery
    }

    /// Creates a collection. With `dimension: None` the dimension is taken from the first add.
    /// `bit_width` must be 2, 3 or 4.
    pub fn create_collection(
        &self,
        name: &str,
        dimension: Option<usize>,
        bit_width: usize,
    ) -> Result<Arc<Collection>, ZenarError> {
        validate_collection_name(name)?;
        if let Some(dim) = dimension {
            validate_dimension(dim)?;
        }
        if !BIT_WIDTHS.contains(&bit_width) {
            return Err(ZenarError::InvalidConfig(format!(
                "bit_width must be 2, 3 or 4, got {bit_width}"
            )));
        }
        let mut collections = self.collections.write().map_err(|_| lock_poisoned())?;
        if collections.contains_key(name) {
            return Err(ZenarError::CollectionAlreadyExists(name.to_string()));
        }

        let store = match dimension {
            Some(dim) => VectorStore::new(dim, bit_width)?,
            None => VectorStore::new_lazy(bit_width)?,
        };
        self.catalog.insert(Entry {
            durable_log_len: 0,
            durable_vectors: 0,
            compaction_pending: None,
            name: name.to_string(),
            dimension,
            bit_width,
            fields: Vec::new(),
        })?;
        // A log left behind by an interrupted create would block this one. The catalog did not
        // list the collection, so the file holds nothing that was acknowledged.
        let log = docs_path(&self.root, name);
        if log.exists() {
            fs::remove_file(&log).map_err(io_err)?;
        }
        // The same goes for a vector index left behind by an interrupted drop. Loading it would
        // bring back the old collection's vectors, and reconciliation would then have to remove them.
        let index = index_path(&self.root, name);
        if index.exists() {
            fs::remove_file(&index).map_err(io_err)?;
        }
        let docs = match DocStore::create(&log, Vec::new()) {
            Ok(docs) => docs,
            Err(e) => {
                let _ = self.catalog.remove(name);
                return Err(e);
            }
        };

        let collection = Arc::new(Collection::new(
            name,
            index_path(&self.root, name),
            Arc::clone(&self.catalog),
            store,
            docs,
            self.durable,
        ));
        // A new index file is written only when the dimension is known. A lazy index has no
        // dimension to write until its first add.
        if dimension.is_some() {
            collection.persist()?;
        }
        collections.insert(name.to_string(), Arc::clone(&collection));
        Ok(collection)
    }

    pub fn get_collection(&self, name: &str) -> Result<Arc<Collection>, ZenarError> {
        self.collections
            .read()
            .map_err(|_| lock_poisoned())?
            .get(name)
            .cloned()
            .ok_or_else(|| ZenarError::CollectionNotFound(name.to_string()))
    }

    pub fn list_collections(&self) -> Result<Vec<String>, ZenarError> {
        let mut names: Vec<String> = self
            .collections
            .read()
            .map_err(|_| lock_poisoned())?
            .keys()
            .cloned()
            .collect();
        names.sort();
        Ok(names)
    }

    /// Makes every collection's adds durable.
    pub fn flush(&self) -> Result<(), ZenarError> {
        let collections: Vec<Arc<Collection>> = self
            .collections
            .read()
            .map_err(|_| lock_poisoned())?
            .values()
            .cloned()
            .collect();
        collections.iter().try_for_each(|c| c.flush())
    }

    /// Deletes a collection, its documents and its vector file.
    pub fn drop_collection(&self, name: &str) -> Result<(), ZenarError> {
        let mut collections = self.collections.write().map_err(|_| lock_poisoned())?;
        let Some(collection) = collections.remove(name) else {
            return Err(ZenarError::CollectionNotFound(name.to_string()));
        };
        // Close the handle before its files go, so a write through it cannot recreate them.
        collection.close();
        self.catalog.remove(name)?;
        for path in [docs_path(&self.root, name), index_path(&self.root, name)] {
            if path.exists() {
                fs::remove_file(&path).map_err(io_err)?;
            }
        }
        sync_parent(&self.root.join("docs"))?;
        Ok(())
    }
}

/// Settles a compaction that did not finish, by the rules of docs/R3.md, section 5. The temporary log is
/// removed: it is never the log. If a pending record exists, the log's length says which side of the rename
/// the crash was on:
///
/// - the new length: the rename happened. The pending record is cleared.
/// - the old length: the rename did not happen, or was undone. The recorded length goes back to the old
///   length, and the pending record is cleared.
/// - any other length, or no log: the log was changed outside Zenar. Opening refuses.
fn settle_compaction(
    catalog: &Catalog,
    root: &Path,
    name: &str,
    entry: Entry,
) -> Result<Entry, ZenarError> {
    let log = docs_path(root, name);
    let _ = fs::remove_file(log.with_extension("log.compact"));
    let Some(old_len) = entry.compaction_pending else {
        return Ok(entry);
    };
    let file_len = fs::metadata(&log)
        .map_err(|_| {
            ZenarError::RecoveryError(format!(
                "collection {name:?}: a compaction was interrupted and the log is missing"
            ))
        })?
        .len();
    let settled = if file_len == entry.durable_log_len {
        entry.durable_log_len
    } else if file_len == old_len {
        old_len
    } else {
        return Err(ZenarError::RecoveryError(format!(
            "collection {name:?}: a compaction was interrupted, and the log is {file_len} bytes, which is \
             neither the compacted length {} nor the length before it, {old_len}. The log was changed \
             outside Zenar",
            entry.durable_log_len
        )));
    };
    catalog.update(name, |e| {
        e.durable_log_len = settled;
        e.compaction_pending = None;
    })?;
    catalog
        .get(name)?
        .ok_or_else(|| ZenarError::RecoveryError(format!("{name} vanished")))
}

/// Opens a collection's document log. Creates an empty one if it is missing and nothing was synced to it.
///
/// A log that is missing while the catalog says it held synced bytes is refused, not recreated:
/// recreating it would hide the loss.
fn open_or_create_docs(
    root: &Path,
    name: &str,
    schema: Vec<(String, crate::value::FieldKind)>,
    durable_len: u64,
) -> Result<DocStore, ZenarError> {
    let path = docs_path(root, name);
    if path.exists() {
        DocStore::open_checked(&path, schema, durable_len)
    } else if durable_len > 0 {
        Err(ZenarError::RecoveryError(format!(
            "collection {name:?}: the document log is missing; it held {durable_len} bytes at its \
             last sync. A file was removed outside Zenar"
        )))
    } else {
        DocStore::create(&path, schema)
    }
}

/// Holds an exclusive advisory lock on `<root>/LOCK` for the life of the handle.
fn acquire_lock(root: &Path) -> Result<File, ZenarError> {
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(root.join("LOCK"))
        .map_err(io_err)?;
    match file.try_lock() {
        Ok(()) => Ok(file),
        Err(TryLockError::WouldBlock) => {
            Err(ZenarError::DatabaseLocked(root.display().to_string()))
        }
        Err(TryLockError::Error(e)) => Err(io_err(e)),
    }
}

fn index_path(root: &Path, name: &str) -> PathBuf {
    root.join("vectors").join(format!("{name}.zvec"))
}

fn docs_path(root: &Path, name: &str) -> PathBuf {
    root.join("docs").join(format!("{name}.zdocs"))
}

fn absolute(path: &Path) -> Result<PathBuf, ZenarError> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        std::env::current_dir()
            .map(|d| d.join(path))
            .map_err(io_err)
    }
}

fn lock_poisoned() -> ZenarError {
    ZenarError::VectorStoreError("database lock poisoned".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("zenar-db-{tag}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    #[test]
    fn name_validation() {
        assert!(validate_collection_name("documents").is_ok());
        assert!(validate_collection_name("faq_2").is_ok());
        assert!(validate_collection_name("../../etc/passwd").is_err());
        assert!(validate_collection_name("1abc").is_err());
        assert!(validate_collection_name("Docs").is_err());
        assert!(validate_collection_name("").is_err());
    }

    #[test]
    fn path_with_spaces_and_metacharacters_works() {
        let dir = temp_dir("paths");
        // Windows does not allow '?' in file names, so the URL-like characters are Unix-only.
        #[cfg(unix)]
        let odd = std::env::temp_dir().join(format!("zenar ?#&{}", std::process::id()));
        #[cfg(not(unix))]
        let odd = std::env::temp_dir().join(format!("zenar space {}", std::process::id()));
        let _ = fs::remove_dir_all(&odd);
        {
            let db = VectorDB::open(&dir).unwrap();
            db.create_collection("sp", Some(8), 4)
                .unwrap()
                .add(1, &[1.0; 8], "ok", vec![])
                .unwrap();
        }
        assert!(VectorDB::open(&dir).is_ok());
        assert!(VectorDB::open(&odd).is_ok());
        fs::remove_dir_all(&dir).ok();
        fs::remove_dir_all(&odd).ok();
    }

    #[test]
    fn second_open_is_locked() {
        let dir = temp_dir("lock");
        let first = VectorDB::open(&dir).unwrap();
        assert!(matches!(
            VectorDB::open(&dir),
            Err(ZenarError::DatabaseLocked(_))
        ));
        drop(first);
        assert!(VectorDB::open(&dir).is_ok());
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn add_search_across_reopen() {
        let dir = temp_dir("reopen");
        {
            let db = VectorDB::open(&dir).unwrap();
            let c = db.create_collection("docs", Some(8), 4).unwrap();
            c.add(
                1,
                &[1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                "hello",
                vec![],
            )
            .unwrap();
            db.flush().unwrap();
        }
        let db = VectorDB::open(&dir).unwrap();
        let c = db.get_collection("docs").unwrap();
        let hits = c
            .search(&[1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], 1, None)
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].text, "hello");
        assert!(db.recovery_report().is_clean());
        drop(db);
        fs::remove_dir_all(&dir).ok();
    }
}
