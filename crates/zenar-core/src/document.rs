use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard};

use rusqlite::types::Value as Sql;
use rusqlite::{Connection, OptionalExtension, Row, params_from_iter};

use crate::error::ZenarError;
use crate::value::{FieldKind, MetaValue};

/// Lifecycle state of a document row (spec section 6).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    Pending,
    Ready,
    Deleting,
}

impl Status {
    pub fn as_str(self) -> &'static str {
        match self {
            Status::Pending => "PENDING",
            Status::Ready => "READY",
            Status::Deleting => "DELETING",
        }
    }
}

/// A collection's persisted configuration (spec section 56).
#[derive(Debug, Clone, PartialEq)]
pub struct CollectionRow {
    pub name: String,
    pub dimension: Option<usize>,
    pub bit_width: usize,
    pub fields: Vec<(String, FieldKind)>,
}

/// A document as read back from the metadata store.
#[derive(Debug, Clone, PartialEq)]
pub struct DocumentRow {
    pub id: u64,
    pub text: String,
    pub metadata: Vec<(String, MetaValue)>,
}

/// Reserved column names that metadata fields may not use.
pub const RESERVED_COLUMNS: [&str; 3] = ["id", "text", "status"];

/// Metadata field names: lowercase ASCII letters, digits and `_`, starting
/// with a letter, at most 64 chars. Field names are interpolated into SQL as
/// identifiers, so this allowlist is the only thing standing between a field
/// name and the SQL text (spec section 73 reasoning applies here too).
pub fn is_valid_field_name(name: &str) -> bool {
    valid_identifier(name) && !RESERVED_COLUMNS.contains(&name)
}

pub fn valid_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_lowercase())
        && name.len() <= 64
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

/// Owns the SQLite database that holds collection config and document rows.
///
/// One writer connection, plus a small pool of read-only connections. In WAL mode
/// readers do not block the writer, and the writer does not block readers.
/// `:memory:` databases use the writer for everything, since each connection
/// would otherwise see its own database.
pub struct MetadataStore {
    writer: Mutex<Connection>,
    readers: Vec<Mutex<Connection>>,
    next_reader: AtomicUsize,
}

/// Number of read-only connections for a file-backed database.
const READER_POOL: usize = 4;

/// Identifiers in IN lists are bound one per placeholder. SQLite's default limit is
/// 32766, so chunk well below it.
const IN_CHUNK: usize = 500;

impl MetadataStore {
    /// Opens (or creates) the database at `path`. `durable` selects `synchronous=FULL`,
    /// which fsyncs every commit. The default, `NORMAL`, fsyncs at WAL checkpoints. Both
    /// survive a process crash. Only `FULL` survives power loss for every commit.
    pub fn open(path: &Path, durable: bool) -> Result<Self, ZenarError> {
        let writer = Connection::open(path).map_err(store_err)?;
        configure(&writer, durable, false)?;
        let mut readers = Vec::with_capacity(READER_POOL);
        for _ in 0..READER_POOL {
            let reader = Connection::open(path).map_err(store_err)?;
            configure(&reader, durable, true)?;
            readers.push(Mutex::new(reader));
        }
        let store = Self {
            writer: Mutex::new(writer),
            readers,
            next_reader: AtomicUsize::new(0),
        };
        store.init()?;
        Ok(store)
    }

    pub fn open_in_memory() -> Result<Self, ZenarError> {
        let writer = Connection::open_in_memory().map_err(store_err)?;
        configure(&writer, false, false)?;
        let store = Self {
            writer: Mutex::new(writer),
            readers: Vec::new(),
            next_reader: AtomicUsize::new(0),
        };
        store.init()?;
        Ok(store)
    }

    fn init(&self) -> Result<(), ZenarError> {
        self.write(|c| {
            c.execute_batch(
                "CREATE TABLE IF NOT EXISTS zenar_collections (
                    name TEXT PRIMARY KEY,
                    dimension INTEGER,
                    bit_width INTEGER NOT NULL,
                    fields TEXT NOT NULL,
                    schema_version INTEGER NOT NULL,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                );",
            )
            .map_err(store_err)
        })
    }

    /// Runs `f` on the writer connection.
    fn write<T>(
        &self,
        f: impl FnOnce(&Connection) -> Result<T, ZenarError>,
    ) -> Result<T, ZenarError> {
        let conn = self
            .writer
            .lock()
            .map_err(|_| store_msg("writer lock poisoned"))?;
        f(&conn)
    }

    /// Runs `f` on a read connection. Uses the first idle reader, or waits on one.
    fn read<T>(
        &self,
        f: impl FnOnce(&Connection) -> Result<T, ZenarError>,
    ) -> Result<T, ZenarError> {
        if self.readers.is_empty() {
            return self.write(f);
        }
        let start = self.next_reader.fetch_add(1, Ordering::Relaxed);
        for offset in 0..self.readers.len() {
            let slot = &self.readers[(start + offset) % self.readers.len()];
            if let Ok(guard) = slot.try_lock() {
                return f(&guard);
            }
        }
        let guard: MutexGuard<'_, Connection> = self.readers[start % self.readers.len()]
            .lock()
            .map_err(|_| store_msg("reader lock poisoned"))?;
        f(&guard)
    }

    /// Forces the WAL into the database file and syncs it. This is the barrier `flush()` needs
    /// for power-loss durability.
    pub fn checkpoint(&self) -> Result<(), ZenarError> {
        self.write(|c| {
            c.query_row("PRAGMA wal_checkpoint(FULL)", [], |_| Ok(()))
                .map_err(store_err)
        })
    }

    // ----- collection config ------------------------------------------------

    pub fn insert_collection(
        &self,
        name: &str,
        dimension: Option<usize>,
        bit_width: usize,
    ) -> Result<(), ZenarError> {
        self.write(|c| {
            c.execute(
                "INSERT INTO zenar_collections (name, dimension, bit_width, fields, schema_version) VALUES (?1, ?2, ?3, '', 1)",
                rusqlite::params![name, dimension.map(|d| d as i64), bit_width as i64],
            )
            .map_err(store_err)?;
            Ok(())
        })
    }

    pub fn get_collection(&self, name: &str) -> Result<Option<CollectionRow>, ZenarError> {
        self.read(|c| {
            c.query_row(
                "SELECT name, dimension, bit_width, fields FROM zenar_collections WHERE name = ?1",
                [name],
                collection_from_row,
            )
            .optional()
            .map_err(store_err)?
            .transpose()
        })
    }

    pub fn list_collections(&self) -> Result<Vec<CollectionRow>, ZenarError> {
        self.read(|c| {
            let mut stmt = c
                .prepare_cached("SELECT name, dimension, bit_width, fields FROM zenar_collections ORDER BY name")
                .map_err(store_err)?;
            let rows = stmt.query_map([], collection_from_row).map_err(store_err)?;
            rows.map(|r| r.map_err(store_err)?).collect()
        })
    }

    pub fn set_dimension(&self, name: &str, dimension: usize) -> Result<(), ZenarError> {
        self.write(|c| {
            c.execute(
                "UPDATE zenar_collections SET dimension = ?1 WHERE name = ?2",
                rusqlite::params![dimension as i64, name],
            )
            .map_err(store_err)?;
            Ok(())
        })
    }

    pub fn set_fields(&self, name: &str, fields: &[(String, FieldKind)]) -> Result<(), ZenarError> {
        let encoded = encode_fields(fields);
        self.write(|c| {
            c.execute(
                "UPDATE zenar_collections SET fields = ?1 WHERE name = ?2",
                rusqlite::params![encoded, name],
            )
            .map_err(store_err)?;
            Ok(())
        })
    }

    pub fn delete_collection(&self, name: &str) -> Result<(), ZenarError> {
        self.write(|c| {
            c.execute("DELETE FROM zenar_collections WHERE name = ?1", [name])
                .map_err(store_err)?;
            Ok(())
        })
    }

    // ----- per-collection document table -----------------------------------

    /// Table name for a collection. `name` must already be validated.
    pub fn table_name(collection: &str) -> String {
        format!("zdocs_{collection}")
    }

    /// Identifiers cannot be bound as parameters, so `table` must already be validated.
    pub fn create_document_table(&self, table: &str) -> Result<(), ZenarError> {
        self.write(|c| {
            c.execute_batch(&format!(
                "CREATE TABLE IF NOT EXISTS {table} (
                    id INTEGER PRIMARY KEY,
                    text TEXT NOT NULL,
                    status TEXT NOT NULL
                );"
            ))
            .map_err(store_err)
        })
    }

    pub fn drop_document_table(&self, table: &str) -> Result<(), ZenarError> {
        self.write(|c| {
            c.execute_batch(&format!("DROP TABLE IF EXISTS {table};"))
                .map_err(store_err)
        })
    }

    /// Adds metadata columns and an index on each. Field names must already be validated.
    pub fn add_field_columns(
        &self,
        table: &str,
        fields: &[(String, FieldKind)],
    ) -> Result<(), ZenarError> {
        self.write(|c| {
            for (name, kind) in fields {
                c.execute_batch(&format!(
                    "ALTER TABLE {table} ADD COLUMN \"{name}\" {};
                     CREATE INDEX IF NOT EXISTS \"idx_{table}_{name}\" ON {table} (\"{name}\");",
                    kind.sql_type()
                ))
                .map_err(store_err)?;
            }
            Ok(())
        })
    }

    /// Inserts a row. Metadata columns not present in `metadata` stay SQL NULL.
    pub fn insert_document(
        &self,
        table: &str,
        id: u64,
        text: &str,
        status: Status,
        metadata: &[(String, MetaValue)],
    ) -> Result<(), ZenarError> {
        let mut columns = vec!["id".to_string(), "text".to_string(), "status".to_string()];
        let mut params = vec![
            Sql::Integer(id as i64),
            Sql::Text(text.to_string()),
            Sql::Text(status.as_str().to_string()),
        ];
        for (name, value) in metadata {
            columns.push(format!("\"{name}\""));
            params.push(value.to_sql());
        }
        let placeholders: Vec<String> = (1..=params.len()).map(|i| format!("?{i}")).collect();
        let sql = format!(
            "INSERT INTO {table} ({}) VALUES ({})",
            columns.join(", "),
            placeholders.join(", ")
        );
        self.write(|c| {
            c.prepare_cached(&sql)
                .and_then(|mut s| s.execute(params_from_iter(params)))
                .map_err(store_err)?;
            Ok(())
        })
    }

    pub fn set_status(&self, table: &str, id: u64, status: Status) -> Result<(), ZenarError> {
        self.write(|c| {
            c.execute(
                &format!("UPDATE {table} SET status = ?1 WHERE id = ?2"),
                rusqlite::params![status.as_str(), id as i64],
            )
            .map_err(store_err)?;
            Ok(())
        })
    }

    pub fn delete_document(&self, table: &str, id: u64) -> Result<(), ZenarError> {
        self.write(|c| {
            c.execute(&format!("DELETE FROM {table} WHERE id = ?1"), [id as i64])
                .map_err(store_err)?;
            Ok(())
        })
    }

    /// Returns the status of one row, or `None` if no row has this id.
    pub fn status_of(&self, table: &str, id: u64) -> Result<Option<String>, ZenarError> {
        self.read(|c| {
            c.query_row(
                &format!("SELECT status FROM {table} WHERE id = ?1"),
                [id as i64],
                |r| r.get(0),
            )
            .optional()
            .map_err(store_err)
        })
    }

    /// All ids in the table with the given status (recovery, spec section 26).
    pub fn ids_with_status(&self, table: &str, status: Status) -> Result<Vec<u64>, ZenarError> {
        self.read(|c| {
            query_ids(
                c,
                &format!("SELECT id FROM {table} WHERE status = ?1"),
                [status.as_str()],
            )
        })
    }

    /// Every id in the table regardless of status (recovery, spec section 26).
    pub fn all_ids(&self, table: &str) -> Result<Vec<u64>, ZenarError> {
        self.read(|c| query_ids(c, &format!("SELECT id FROM {table}"), []))
    }

    /// Ids of rows matching a compiled filter. Returns ids only, never text or metadata:
    /// those are fetched for the final top-K alone (spec section 20).
    ///
    /// The `status` predicate is not in this query on purpose. Adding it makes SQLite
    /// look up every matching row in the table, which costs about 10x more than the index
    /// scan (see BENCHMARKS.md). Correctness does not depend on it. A row leaves the
    /// index before its status changes to DELETING, and the caller keeps only ids present
    /// in the index. `fetch_documents` filters on READY.
    pub fn matching_ids(
        &self,
        table: &str,
        where_sql: &str,
        params: Vec<Sql>,
    ) -> Result<Vec<u64>, ZenarError> {
        let sql = format!("SELECT id FROM {table} WHERE {where_sql}");
        self.read(|c| {
            let mut stmt = c.prepare_cached(&sql).map_err(store_err)?;
            let rows = stmt
                .query_map(params_from_iter(params), |r| r.get::<_, i64>(0))
                .map_err(store_err)?;
            rows.map(|r| r.map(|i| i as u64).map_err(store_err))
                .collect()
        })
    }

    /// Fetches READY documents for the given ids, in chunks of `IN_CHUNK`.
    /// Missing and non-READY ids are skipped. The order of the result is not guaranteed.
    pub fn fetch_documents(
        &self,
        table: &str,
        fields: &[(String, FieldKind)],
        ids: &[u64],
    ) -> Result<Vec<DocumentRow>, ZenarError> {
        let mut columns = vec!["id".to_string(), "text".to_string()];
        columns.extend(fields.iter().map(|(n, _)| format!("\"{n}\"")));
        let columns = columns.join(", ");
        let mut docs = Vec::with_capacity(ids.len());
        self.read(|c| {
            for chunk in ids.chunks(IN_CHUNK) {
                let placeholders: Vec<String> =
                    (1..=chunk.len()).map(|i| format!("?{i}")).collect();
                let sql = format!(
                    "SELECT {columns} FROM {table} WHERE status = 'READY' AND id IN ({})",
                    placeholders.join(", ")
                );
                let params = chunk.iter().map(|id| Sql::Integer(*id as i64));
                let mut stmt = c.prepare_cached(&sql).map_err(store_err)?;
                let rows = stmt
                    .query_map(params_from_iter(params), |r| document_from_row(r, fields))
                    .map_err(store_err)?;
                for row in rows {
                    docs.push(row.map_err(store_err)??);
                }
            }
            Ok(())
        })?;
        Ok(docs)
    }

    pub fn count_ready(&self, table: &str) -> Result<usize, ZenarError> {
        self.read(|c| {
            let n: i64 = c
                .query_row(
                    &format!("SELECT COUNT(*) FROM {table} WHERE status = 'READY'"),
                    [],
                    |r| r.get(0),
                )
                .map_err(store_err)?;
            Ok(n.max(0) as usize)
        })
    }
}

fn configure(conn: &Connection, durable: bool, read_only: bool) -> Result<(), ZenarError> {
    let sync = if durable { "FULL" } else { "NORMAL" };
    conn.busy_timeout(std::time::Duration::from_secs(5))
        .map_err(store_err)?;
    conn.execute_batch(&format!(
        "PRAGMA journal_mode = WAL;
         PRAGMA synchronous = {sync};
         PRAGMA foreign_keys = OFF;"
    ))
    .map_err(store_err)?;
    if read_only {
        conn.execute_batch("PRAGMA query_only = ON;")
            .map_err(store_err)?;
    }
    Ok(())
}

fn query_ids<P: rusqlite::Params>(
    conn: &Connection,
    sql: &str,
    params: P,
) -> Result<Vec<u64>, ZenarError> {
    let mut stmt = conn.prepare_cached(sql).map_err(store_err)?;
    let rows = stmt
        .query_map(params, |r| r.get::<_, i64>(0))
        .map_err(store_err)?;
    rows.map(|r| r.map(|i| i as u64).map_err(store_err))
        .collect()
}

fn collection_from_row(row: &Row<'_>) -> rusqlite::Result<Result<CollectionRow, ZenarError>> {
    let name: String = row.get(0)?;
    let dimension: Option<i64> = row.get(1)?;
    let bit_width: i64 = row.get(2)?;
    let encoded: String = row.get(3)?;
    Ok(decode_fields(&encoded).map(|fields| CollectionRow {
        name,
        dimension: dimension.map(|d| d as usize),
        bit_width: bit_width as usize,
        fields,
    }))
}

pub fn encode_fields(fields: &[(String, FieldKind)]) -> String {
    fields
        .iter()
        .map(|(n, k)| format!("{n}:{}", k.as_str()))
        .collect::<Vec<_>>()
        .join(",")
}

fn decode_fields(encoded: &str) -> Result<Vec<(String, FieldKind)>, ZenarError> {
    if encoded.is_empty() {
        return Ok(Vec::new());
    }
    encoded
        .split(',')
        .map(|entry| {
            let (name, kind) = entry
                .split_once(':')
                .ok_or_else(|| store_msg("malformed field schema"))?;
            let kind = FieldKind::parse(kind).ok_or_else(|| store_msg("unknown field kind"))?;
            Ok((name.to_string(), kind))
        })
        .collect()
}

fn document_from_row(
    row: &Row<'_>,
    fields: &[(String, FieldKind)],
) -> rusqlite::Result<Result<DocumentRow, ZenarError>> {
    let id: i64 = row.get(0)?;
    let text: String = row.get(1)?;
    let mut metadata = Vec::new();
    for (i, (name, kind)) in fields.iter().enumerate() {
        let value = row.get_ref(i + 2)?;
        if let Some(v) = MetaValue::from_sql(*kind, value) {
            metadata.push((name.clone(), v));
        }
    }
    Ok(Ok(DocumentRow {
        id: id as u64,
        text,
        metadata,
    }))
}

fn store_err(e: rusqlite::Error) -> ZenarError {
    ZenarError::MetadataStoreError(e.to_string())
}

fn store_msg(msg: &str) -> ZenarError {
    ZenarError::MetadataStoreError(msg.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fields() -> Vec<(String, FieldKind)> {
        vec![("tenant".to_string(), FieldKind::Text)]
    }

    #[test]
    fn open_in_memory() {
        MetadataStore::open_in_memory().unwrap();
    }

    #[test]
    fn insert_select_delete_roundtrip() {
        let store = MetadataStore::open_in_memory().unwrap();
        let table = MetadataStore::table_name("roundtrip");
        store.create_document_table(&table).unwrap();
        store
            .insert_document(&table, 7, "hello", Status::Ready, &[])
            .unwrap();
        let docs = store.fetch_documents(&table, &[], &[7]).unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0].text, "hello");
        store.delete_document(&table, 7).unwrap();
        assert!(store.fetch_documents(&table, &[], &[7]).unwrap().is_empty());
    }

    #[test]
    fn sql_text_is_stored_as_data() {
        let store = MetadataStore::open_in_memory().unwrap();
        let table = MetadataStore::table_name("inject");
        store.create_document_table(&table).unwrap();
        let payload = "'; DROP TABLE zdocs_inject; --";
        store
            .insert_document(&table, 1, payload, Status::Ready, &[])
            .unwrap();
        assert_eq!(
            store.fetch_documents(&table, &[], &[1]).unwrap()[0].text,
            payload
        );
    }

    #[test]
    fn typed_metadata_roundtrip_and_null() {
        let store = MetadataStore::open_in_memory().unwrap();
        let table = MetadataStore::table_name("typed");
        store.create_document_table(&table).unwrap();
        let schema = vec![
            ("tenant".to_string(), FieldKind::Text),
            ("price".to_string(), FieldKind::Float),
            ("active".to_string(), FieldKind::Boolean),
        ];
        store.add_field_columns(&table, &schema).unwrap();
        let meta = vec![
            ("tenant".to_string(), MetaValue::Text("A".into())),
            ("active".to_string(), MetaValue::Boolean(true)),
        ];
        store
            .insert_document(&table, 1, "t", Status::Ready, &meta)
            .unwrap();
        let doc = store
            .fetch_documents(&table, &schema, &[1])
            .unwrap()
            .remove(0);
        assert!(
            doc.metadata
                .contains(&("tenant".to_string(), MetaValue::Text("A".into())))
        );
        assert!(
            doc.metadata
                .contains(&("active".to_string(), MetaValue::Boolean(true)))
        );
        assert!(
            !doc.metadata.iter().any(|(n, _)| n == "price"),
            "missing field must read back as NULL"
        );
    }

    #[test]
    fn fetch_crosses_chunk_boundary_and_skips_pending() {
        let store = MetadataStore::open_in_memory().unwrap();
        let table = MetadataStore::table_name("hydrate");
        store.create_document_table(&table).unwrap();
        for id in 0..1200u64 {
            store
                .insert_document(&table, id, &format!("doc {id}"), Status::Ready, &[])
                .unwrap();
        }
        store
            .insert_document(&table, 5000, "pending", Status::Pending, &[])
            .unwrap();
        for n in [1usize, 499, 500, 501, 1200] {
            let ids: Vec<u64> = (0..n as u64).collect();
            let mut seen: Vec<u64> = store
                .fetch_documents(&table, &[], &ids)
                .unwrap()
                .iter()
                .map(|d| d.id)
                .collect();
            seen.sort();
            assert_eq!(seen, ids, "wrong ids for {n}");
        }
        assert!(
            store
                .fetch_documents(&table, &[], &[5000])
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn field_name_validation() {
        assert!(is_valid_field_name("tenant"));
        assert!(!is_valid_field_name("Tenant"));
        assert!(!is_valid_field_name("id"));
        assert!(!is_valid_field_name("1abc"));
        assert!(!is_valid_field_name("a-b"));
        assert!(fields().len() == 1);
    }

    #[test]
    fn file_store_survives_reopen_and_checkpoints() {
        let dir = std::env::temp_dir().join(format!("zenar-meta-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("metadata.db");
        {
            let store = MetadataStore::open(&path, false).unwrap();
            store.insert_collection("c", Some(8), 4).unwrap();
            store
                .create_document_table(&MetadataStore::table_name("c"))
                .unwrap();
            store
                .insert_document(&MetadataStore::table_name("c"), 1, "x", Status::Ready, &[])
                .unwrap();
            store.checkpoint().unwrap();
        }
        let store = MetadataStore::open(&path, true).unwrap();
        assert_eq!(
            store.get_collection("c").unwrap().unwrap().dimension,
            Some(8)
        );
        assert_eq!(
            store.count_ready(&MetadataStore::table_name("c")).unwrap(),
            1
        );
        drop(store);
        std::fs::remove_dir_all(&dir).ok();
    }
}
