use std::collections::HashMap;
use std::fs::{self, File, OpenOptions, TryLockError};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use crate::collection::{Collection, RecoveryReport};
use crate::document::MetadataStore;
use crate::error::ZenarError;
use crate::storage::{VectorStore, validate_dimension};

/// Bit widths TurboVec supports (spec section 88).
const BIT_WIDTHS: [usize; 3] = [2, 3, 4];

/// How hard the metadata store fsyncs commits (spec section 86).
///
/// * `Normal` (default): WAL is not fsynced on every commit. Adds are fast.
///   Recent commits can be lost on power failure, but reconciliation on open
///   brings metadata and vectors back into agreement. `flush()` makes every earlier
///   add durable, including on power loss. Adds after the last flush need `Full` for that.
/// * `Full`: fsync on every commit. Every add is power-safe on return, at a lower
///   ingest rate (see BENCHMARKS.md).
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
/// <root>/LOCK              advisory lock held while the database is open
/// <root>/metadata.db       SQLite (WAL): collection config, documents, metadata
/// <root>/vectors/<c>.tvim  TurboVec index per collection
/// ```
pub struct VectorDB {
    root: PathBuf,
    _lock: File,
    meta: Arc<MetadataStore>,
    collections: RwLock<HashMap<String, Arc<Collection>>>,
    recovery: RecoveryReport,
}

impl VectorDB {
    /// Opens (or creates) a database with the default `MetadataSync::Normal`.
    /// Fails with `DatabaseLocked` if another process, or another handle in
    /// this process, already has it open.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, ZenarError> {
        Self::open_with(path, MetadataSync::default())
    }

    pub fn open_with(path: impl AsRef<Path>, sync: MetadataSync) -> Result<Self, ZenarError> {
        let root = absolute(path.as_ref())?;
        fs::create_dir_all(root.join("vectors")).map_err(io_err)?;
        let lock = acquire_lock(&root)?;

        let durable = sync == MetadataSync::Full;
        let meta = Arc::new(MetadataStore::open(&root.join("metadata.db"), durable)?);

        let mut collections = HashMap::new();
        let mut recovery = RecoveryReport::default();
        for row in meta.list_collections()? {
            let index_path = index_path(&root, &row.name);
            let store = if index_path.exists() {
                VectorStore::load(&index_path, row.dimension)?
            } else {
                match row.dimension {
                    Some(dim) => VectorStore::new(dim, row.bit_width)?,
                    None => VectorStore::new_lazy(row.bit_width)?,
                }
            };
            let collection =
                Collection::new(&row.name, index_path, Arc::clone(&meta), store, row.fields);
            let report = collection.reconcile()?;
            recovery.rolled_back_pending += report.rolled_back_pending;
            recovery.finished_deletes += report.finished_deletes;
            recovery.orphaned_rows_removed += report.orphaned_rows_removed;
            recovery.orphaned_vectors_removed += report.orphaned_vectors_removed;
            collections.insert(row.name.clone(), Arc::new(collection));
        }

        Ok(Self {
            root,
            _lock: lock,
            meta,
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

    /// Creates a collection. With `dimension: None` the dimension is taken from
    /// the first `add` (spec section 48). `bit_width` must be 2, 3 or 4.
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
        if collections.contains_key(name) || self.meta.get_collection(name)?.is_some() {
            return Err(ZenarError::CollectionAlreadyExists(name.to_string()));
        }

        let store = match dimension {
            Some(dim) => VectorStore::new(dim, bit_width)?,
            None => VectorStore::new_lazy(bit_width)?,
        };
        self.meta.insert_collection(name, dimension, bit_width)?;
        self.meta
            .create_document_table(&MetadataStore::table_name(name))?;

        let collection = Arc::new(Collection::new(
            name,
            index_path(&self.root, name),
            Arc::clone(&self.meta),
            store,
            Vec::new(),
        ));
        // A lazy index has no dimension to write yet; its file appears on the first add.
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

    /// Makes every collection's adds durable (see `SYNC_INTERVAL` in collection.rs).
    pub fn flush(&self) -> Result<(), ZenarError> {
        let collections: Vec<Arc<Collection>> = self
            .collections
            .read()
            .map_err(|_| lock_poisoned())?
            .values()
            .cloned()
            .collect();
        collections.iter().try_for_each(|c| c.flush())?;
        // The index files are synced above. This checkpoint makes the metadata rows durable too.
        self.meta.checkpoint()
    }

    /// Deletes a collection, its documents and its vector file.
    pub fn drop_collection(&self, name: &str) -> Result<(), ZenarError> {
        let mut collections = self.collections.write().map_err(|_| lock_poisoned())?;
        if collections.remove(name).is_none() {
            return Err(ZenarError::CollectionNotFound(name.to_string()));
        }
        self.meta
            .drop_document_table(&MetadataStore::table_name(name))?;
        self.meta.delete_collection(name)?;
        let index = index_path(&self.root, name);
        if index.exists() {
            fs::remove_file(&index).map_err(io_err)?;
        }
        Ok(())
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

/// Collection names become file names and SQL table name suffixes, so they
/// are restricted to a strict allowlist (spec section 73). Names are rejected, never sanitized.
pub fn validate_collection_name(name: &str) -> Result<(), ZenarError> {
    if crate::document::valid_identifier(name) {
        Ok(())
    } else {
        Err(ZenarError::InvalidCollectionName(name.to_string()))
    }
}

fn index_path(root: &Path, name: &str) -> PathBuf {
    root.join("vectors").join(format!("{name}.tvim"))
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

fn io_err(e: std::io::Error) -> ZenarError {
    ZenarError::PersistenceError(e.to_string())
}

fn lock_poisoned() -> ZenarError {
    ZenarError::VectorStoreError("database lock poisoned".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("zenar-{tag}-{}", std::process::id()));
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
        let dir = std::env::temp_dir().join(format!("zenar space {}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        {
            let db = VectorDB::open(&dir).unwrap();
            let c = db.create_collection("sp", Some(8), 4).unwrap();
            c.add(1, &[1.0; 8], "ok", vec![]).unwrap();
        }
        assert!(VectorDB::open(&dir).is_ok());
        fs::remove_dir_all(&dir).ok();
        let odd = std::env::temp_dir().join(format!("zenar ?#&{}", std::process::id()));
        let _ = fs::remove_dir_all(&odd);
        assert!(VectorDB::open(&odd).is_ok());
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
