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

/// The catalog. Every method that changes it persists before returning.
pub struct Catalog {
    path: PathBuf,
    entries: Mutex<BTreeMap<String, Entry>>,
}

impl Catalog {
    pub fn open(path: &Path) -> Result<Self, ZenarError> {
        let entries = if path.exists() {
            let text = fs::read_to_string(path).map_err(io_err)?;
            let file: CatalogFile = serde_json::from_str(&text)
                .map_err(|e| ZenarError::RecoveryError(format!("catalog is unreadable: {e}")))?;
            if file.version != 1 {
                return Err(ZenarError::RecoveryError(format!(
                    "unsupported catalog version {}",
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
        Ok(Self {
            path: path.to_path_buf(),
            entries: Mutex::new(entries),
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
        map.insert(entry.name.clone(), entry);
        self.persist(&map)
    }

    /// Applies `change` to one entry and persists the result.
    pub fn update(&self, name: &str, change: impl FnOnce(&mut Entry)) -> Result<(), ZenarError> {
        let mut map = self.lock()?;
        let entry = map
            .get_mut(name)
            .ok_or_else(|| ZenarError::CollectionNotFound(name.to_string()))?;
        let before = entry.clone();
        change(entry);
        let result = self.persist(&map);
        if result.is_err() {
            // The file still holds the old state, so the in-memory copy must match it.
            map.insert(name.to_string(), before);
        }
        result
    }

    pub fn remove(&self, name: &str) -> Result<(), ZenarError> {
        let mut map = self.lock()?;
        if map.remove(name).is_none() {
            return Err(ZenarError::CollectionNotFound(name.to_string()));
        }
        self.persist(&map)
    }

    fn persist(&self, map: &BTreeMap<String, Entry>) -> Result<(), ZenarError> {
        let file = CatalogFile {
            version: 1,
            collections: map.values().cloned().collect(),
        };
        let text = serde_json::to_string_pretty(&file)
            .map_err(|e| ZenarError::PersistenceError(e.to_string()))?;
        let tmp = self.path.with_extension("json.tmp");
        {
            let mut out = File::create(&tmp).map_err(io_err)?;
            out.write_all(text.as_bytes()).map_err(io_err)?;
            out.sync_all().map_err(io_err)?;
        }
        fs::rename(&tmp, &self.path).map_err(io_err)?;
        sync_parent(&self.path)
    }
}
