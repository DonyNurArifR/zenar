//! Collection catalog: which collections exist, and their dimension, bit width and metadata
//! schema. Stored as one JSON file, replaced atomically on every change (write a temp file,
//! sync it, rename it over the old one, sync the directory).

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

use crate::docstore::{io_err, sync_parent};
use crate::error::ZenarError;
use crate::value::FieldKind;

/// One collection's configuration.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Entry {
    pub name: String,
    pub dimension: Option<usize>,
    pub bit_width: usize,
    /// (field name, kind name), in schema order.
    pub fields: Vec<(String, String)>,
    /// Log length in bytes, and vector count, at the last sync. A file found shorter than these
    /// was truncated or removed outside Zenar, so opening refuses to continue. Entries written
    /// before this field existed read as 0, which disables the check for them.
    #[serde(default)]
    pub durable_log_len: u64,
    #[serde(default)]
    pub durable_vectors: usize,
    /// Set while a compaction has recorded its new log length (`durable_log_len`) but has not finished the
    /// rename and the swap. The value is the log length before the compaction. Opening uses it to tell
    /// an undone rename from a log changed outside Zenar (docs/R3.md, section 5). Omitted when unset, so
    /// catalogs without it are written and read exactly as before.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compaction_pending: Option<u64>,
}

impl Entry {
    pub fn schema(&self) -> Result<Vec<(String, FieldKind)>, ZenarError> {
        self.fields
            .iter()
            .map(|(name, kind)| {
                FieldKind::parse(kind)
                    .map(|k| (name.clone(), k))
                    .ok_or_else(|| {
                        ZenarError::RecoveryError(format!("unknown field kind {kind:?} in catalog"))
                    })
            })
            .collect()
    }
}

#[derive(Serialize, Deserialize)]
struct CatalogFile {
    version: u32,
    collections: Vec<Entry>,
}

/// How a catalog write failed. `NotApplied`: the file still holds the old catalog. `MaybeApplied`: the file
/// was replaced, and the error came from the directory sync that follows.
enum Persist {
    NotApplied(ZenarError),
    MaybeApplied(ZenarError),
}

/// The catalog. Every method that changes it persists before returning.
pub struct Catalog {
    path: PathBuf,
    entries: Mutex<BTreeMap<String, Entry>>,
    /// Test hooks: the next write fails before or after its rename.
    #[cfg(test)]
    pub(crate) fail_persist_before_rename: std::sync::atomic::AtomicBool,
    #[cfg(test)]
    pub(crate) fail_persist_after_rename: std::sync::atomic::AtomicBool,
}

/// On-disk format version of `catalog.json`. It also versions the record format in the logs: version 2
/// stores each document's field count as 32 bits. Bump it for any change that an older build cannot read.
pub const CATALOG_VERSION: u32 = 3;

impl Catalog {
    pub fn open(path: &Path) -> Result<Self, ZenarError> {
        let entries = if path.exists() {
            let text = fs::read_to_string(path).map_err(io_err)?;
            let file: CatalogFile = serde_json::from_str(&text)
                .map_err(|e| ZenarError::RecoveryError(format!("catalog is unreadable: {e}")))?;
            if file.version != CATALOG_VERSION {
                return Err(ZenarError::RecoveryError(format!(
                    "this database has catalog version {}, and this Zenar reads version {CATALOG_VERSION}. \
                     Databases written by earlier releases cannot be read; create a new one",
                    file.version
                )));
            }
            file.collections
                .into_iter()
                .map(|e| (e.name.clone(), e))
                .collect()
        } else {
            BTreeMap::new()
        };
        // A temporary catalog left by an interrupted write is never the catalog: remove it.
        let _ = fs::remove_file(path.with_extension("json.tmp"));
        Ok(Self {
            path: path.to_path_buf(),
            entries: Mutex::new(entries),
            #[cfg(test)]
            fail_persist_before_rename: std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)]
            fail_persist_after_rename: std::sync::atomic::AtomicBool::new(false),
        })
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, BTreeMap<String, Entry>>, ZenarError> {
        self.entries
            .lock()
            .map_err(|_| ZenarError::PersistenceError("catalog lock poisoned".into()))
    }

    pub fn get(&self, name: &str) -> Result<Option<Entry>, ZenarError> {
        Ok(self.lock()?.get(name).cloned())
    }

    pub fn names(&self) -> Result<Vec<String>, ZenarError> {
        Ok(self.lock()?.keys().cloned().collect())
    }

    pub fn insert(&self, entry: Entry) -> Result<(), ZenarError> {
        let mut map = self.lock()?;
        if map.contains_key(&entry.name) {
            return Err(ZenarError::CollectionAlreadyExists(entry.name));
        }
        let name = entry.name.clone();
        map.insert(name.clone(), entry);
        match self.persist(&map) {
            Ok(()) => Ok(()),
            Err(Persist::NotApplied(e)) => {
                map.remove(&name);
                Err(e)
            }
            Err(Persist::MaybeApplied(e)) => Err(e),
        }
    }

    /// Applies `change` to one entry and persists the result. If the change is not applied (the file
    /// still holds the old catalog), the in-memory entry reverts. If the file was replaced but its
    /// directory sync failed, the change may be applied: the in-memory entry keeps it, and the error
    /// says so (`ZenarError::PersistenceError`, see `persist`).
    pub fn update(&self, name: &str, change: impl FnOnce(&mut Entry)) -> Result<(), ZenarError> {
        let mut map = self.lock()?;
        let entry = map
            .get_mut(name)
            .ok_or_else(|| ZenarError::CollectionNotFound(name.to_string()))?;
        let before = entry.clone();
        change(entry);
        match self.persist(&map) {
            Ok(()) => Ok(()),
            Err(Persist::NotApplied(e)) => {
                map.insert(name.to_string(), before);
                Err(e)
            }
            Err(Persist::MaybeApplied(e)) => Err(e),
        }
    }

    pub fn remove(&self, name: &str) -> Result<(), ZenarError> {
        let mut map = self.lock()?;
        let Some(before) = map.remove(name) else {
            return Err(ZenarError::CollectionNotFound(name.to_string()));
        };
        match self.persist(&map) {
            Ok(()) => Ok(()),
            Err(Persist::NotApplied(e)) => {
                map.insert(name.to_string(), before);
                Err(e)
            }
            Err(Persist::MaybeApplied(e)) => Err(e),
        }
    }

    /// Writes the catalog atomically: temporary file, sync, rename, directory sync. A failure before
    /// the rename leaves the old catalog, and the temporary file is removed. A failure after it may
    /// leave the new catalog, and that is reported as `MaybeApplied`.
    fn persist(&self, map: &BTreeMap<String, Entry>) -> Result<(), Persist> {
        let file = CatalogFile {
            version: CATALOG_VERSION,
            collections: map.values().cloned().collect(),
        };
        let text = serde_json::to_string_pretty(&file)
            .map_err(|e| Persist::NotApplied(ZenarError::PersistenceError(e.to_string())))?;
        let tmp = self.path.with_extension("json.tmp");
        let before_rename = (|| -> Result<(), ZenarError> {
            let mut out = File::create(&tmp).map_err(io_err)?;
            out.write_all(text.as_bytes()).map_err(io_err)?;
            out.sync_all().map_err(io_err)?;
            #[cfg(test)]
            if self
                .fail_persist_before_rename
                .swap(false, std::sync::atomic::Ordering::SeqCst)
            {
                return Err(ZenarError::PersistenceError(
                    "injected catalog failure before rename".into(),
                ));
            }
            fs::rename(&tmp, &self.path).map_err(io_err)
        })();
        if let Err(e) = before_rename {
            let _ = fs::remove_file(&tmp);
            return Err(Persist::NotApplied(e));
        }
        #[cfg(test)]
        if self
            .fail_persist_after_rename
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(Persist::MaybeApplied(ZenarError::PersistenceError(
                "injected catalog failure after rename".into(),
            )));
        }
        sync_parent(&self.path).map_err(Persist::MaybeApplied)
    }
}
