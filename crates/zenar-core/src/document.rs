use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use stoolap::{Database, Statement};

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

/// A document as read back from Stoolap.
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

/// Owns the Stoolap database that holds collection config and document rows.
pub struct MetadataStore {
    db: Database,
    /// Prepared statements keyed by SQL text. Filter SQL differs only in
    /// placeholder count/shape, so repeated query shapes reuse one plan
    /// (spec section 90).
    plans: Mutex<HashMap<String, Arc<Statement>>>,
}

impl MetadataStore {
    /// `dsn` is a Stoolap DSN, e.g. `file:///path/metadata.db?sync_mode=full`.
    pub fn open(dsn: &str) -> Result<Self, ZenarError> {
        let db = Database::open(dsn).map_err(store_err)?;
        Self::init(db)
    }

    pub fn open_in_memory() -> Result<Self, ZenarError> {
        let db = Database::open_in_memory().map_err(store_err)?;
        Self::init(db)
    }

    fn init(db: Database) -> Result<Self, ZenarError> {
        db.execute(
            "CREATE TABLE IF NOT EXISTS zenar_collections (
                name TEXT NOT NULL UNIQUE,
                dimension INTEGER,
                bit_width INTEGER NOT NULL,
                fields TEXT NOT NULL,
                schema_version INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )",
            (),
        )
        .map_err(store_err)?;
        Ok(Self {
            db,
            plans: Mutex::new(HashMap::new()),
        })
    }

    fn prepared(&self, sql: &str) -> Result<Arc<Statement>, ZenarError> {
        let mut plans = self
            .plans
            .lock()
            .map_err(|_| store_msg("plan cache poisoned"))?;
        if let Some(stmt) = plans.get(sql) {
            return Ok(Arc::clone(stmt));
        }
        let stmt = Arc::new(self.db.prepare(sql).map_err(store_err)?);
        plans.insert(sql.to_string(), Arc::clone(&stmt));
        Ok(stmt)
    }

    fn exec(&self, sql: &str, params: Vec<stoolap::Value>) -> Result<i64, ZenarError> {
        self.prepared(sql)?.execute(params).map_err(store_err)
    }

    fn query_rows(
        &self,
        sql: &str,
        params: Vec<stoolap::Value>,
    ) -> Result<Vec<stoolap::api::ResultRow>, ZenarError> {
        self.prepared(sql)?
            .query(params)
            .map_err(store_err)?
            .collect_vec()
            .map_err(store_err)
    }

    // ----- collection config ------------------------------------------------

    pub fn insert_collection(
        &self,
        name: &str,
        dimension: Option<usize>,
        bit_width: usize,
    ) -> Result<(), ZenarError> {
        let dim = match dimension {
            Some(d) => stoolap::Value::integer(d as i64),
            None => stoolap::Value::null(stoolap::DataType::Integer),
        };
        self.exec(
            "INSERT INTO zenar_collections (name, dimension, bit_width, fields, schema_version) VALUES ($1, $2, $3, $4, $5)",
            vec![
                stoolap::Value::text(name),
                dim,
                stoolap::Value::integer(bit_width as i64),
                stoolap::Value::text(""),
                stoolap::Value::integer(1),
            ],
        )?;
        Ok(())
    }

    pub fn get_collection(&self, name: &str) -> Result<Option<CollectionRow>, ZenarError> {
        let rows = self.query_rows(
            "SELECT name, dimension, bit_width, fields FROM zenar_collections WHERE name = $1",
            vec![stoolap::Value::text(name)],
        )?;
        rows.first().map(collection_from_row).transpose()
    }

    pub fn list_collections(&self) -> Result<Vec<CollectionRow>, ZenarError> {
        let rows = self.query_rows(
            "SELECT name, dimension, bit_width, fields FROM zenar_collections ORDER BY name",
            vec![],
        )?;
        rows.iter().map(collection_from_row).collect()
    }

    pub fn set_dimension(&self, name: &str, dimension: usize) -> Result<(), ZenarError> {
        self.exec(
            "UPDATE zenar_collections SET dimension = $1 WHERE name = $2",
            vec![
                stoolap::Value::integer(dimension as i64),
                stoolap::Value::text(name),
            ],
        )?;
        Ok(())
    }

    pub fn set_fields(&self, name: &str, fields: &[(String, FieldKind)]) -> Result<(), ZenarError> {
        let encoded = fields
            .iter()
            .map(|(n, k)| format!("{n}:{}", k.as_str()))
            .collect::<Vec<_>>()
            .join(",");
        self.exec(
            "UPDATE zenar_collections SET fields = $1 WHERE name = $2",
            vec![stoolap::Value::text(encoded), stoolap::Value::text(name)],
        )?;
        Ok(())
    }

    pub fn delete_collection(&self, name: &str) -> Result<(), ZenarError> {
        self.exec(
            "DELETE FROM zenar_collections WHERE name = $1",
            vec![stoolap::Value::text(name)],
        )?;
        Ok(())
    }

    // ----- per-collection document table -----------------------------------

    /// Table name for a collection. `name` must already be validated.
    pub fn table_name(collection: &str) -> String {
        format!("zdocs_{collection}")
    }

    pub fn create_document_table(&self, table: &str) -> Result<(), ZenarError> {
        // Identifier is validated by the caller; DDL cannot take bound identifiers.
        self.db
            .execute(
                &format!(
                    "CREATE TABLE IF NOT EXISTS {table} (
                        id INTEGER PRIMARY KEY,
                        text TEXT,
                        status TEXT NOT NULL
                    )"
                ),
                (),
            )
            .map_err(store_err)?;
        Ok(())
    }

    pub fn drop_document_table(&self, table: &str) -> Result<(), ZenarError> {
        self.db
            .execute(&format!("DROP TABLE IF EXISTS {table}"), ())
            .map_err(store_err)?;
        Ok(())
    }

    pub fn add_field_columns(
        &self,
        table: &str,
        fields: &[(String, FieldKind)],
    ) -> Result<(), ZenarError> {
        for (name, kind) in fields {
            self.db
                .execute(
                    &format!(
                        "ALTER TABLE {table} ADD COLUMN \"{name}\" {}",
                        kind.sql_type()
                    ),
                    (),
                )
                .map_err(store_err)?;
        }
        Ok(())
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
            stoolap::Value::integer(id as i64),
            stoolap::Value::text(text),
            stoolap::Value::text(status.as_str()),
        ];
        for (name, value) in metadata {
            columns.push(format!("\"{name}\""));
            params.push(value.to_sql());
        }
        let placeholders: Vec<String> = (1..=params.len()).map(|i| format!("${i}")).collect();
        let sql = format!(
            "INSERT INTO {table} ({}) VALUES ({})",
            columns.join(", "),
            placeholders.join(", ")
        );
        self.exec(&sql, params)?;
        Ok(())
    }

    pub fn set_status(&self, table: &str, id: u64, status: Status) -> Result<(), ZenarError> {
        self.exec(
            &format!("UPDATE {table} SET status = $1 WHERE id = $2"),
            vec![
                stoolap::Value::text(status.as_str()),
                stoolap::Value::integer(id as i64),
            ],
        )?;
        Ok(())
    }

    pub fn delete_document(&self, table: &str, id: u64) -> Result<(), ZenarError> {
        self.exec(
            &format!("DELETE FROM {table} WHERE id = $1"),
            vec![stoolap::Value::integer(id as i64)],
        )?;
        Ok(())
    }

    /// Returns the status of one row, or `None` if no row has this id.
    pub fn status_of(&self, table: &str, id: u64) -> Result<Option<String>, ZenarError> {
        let rows = self.query_rows(
            &format!("SELECT status FROM {table} WHERE id = $1"),
            vec![stoolap::Value::integer(id as i64)],
        )?;
        Ok(rows
            .first()
            .and_then(|r| r.get_value(0))
            .and_then(|v| v.as_string()))
    }

    /// All ids currently in the table with the given status (recovery, spec section 26).
    pub fn ids_with_status(&self, table: &str, status: Status) -> Result<Vec<u64>, ZenarError> {
        let rows = self.query_rows(
            &format!("SELECT id FROM {table} WHERE status = $1"),
            vec![stoolap::Value::text(status.as_str())],
        )?;
        Ok(rows
            .iter()
            .filter_map(|r| r.get_value(0).and_then(|v| v.as_int64()).map(|i| i as u64))
            .collect())
    }

    /// Ids of READY rows matching a compiled filter. Returns ids only, never
    /// text or metadata: those are fetched for the final top-K alone (spec section 20).
    pub fn matching_ids(
        &self,
        table: &str,
        where_sql: &str,
        params: Vec<stoolap::Value>,
    ) -> Result<Vec<u64>, ZenarError> {
        let sql = format!("SELECT id FROM {table} WHERE status = 'READY' AND ({where_sql})");
        let rows = self.query_rows(&sql, params)?;
        Ok(rows
            .iter()
            .filter_map(|r| r.get_value(0).and_then(|v| v.as_int64()).map(|i| i as u64))
            .collect())
    }

    /// Fetches READY documents for the given ids, in one query.
    pub fn fetch_documents(
        &self,
        table: &str,
        fields: &[(String, FieldKind)],
        ids: &[u64],
    ) -> Result<Vec<DocumentRow>, ZenarError> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        // Small sets: one `id = $1` query per id. Large sets: chunked IN lists.
        // In Stoolap 0.4, an IN list on the primary key scans the table after a reopen,
        // while equality uses the index. A scan costs about the same whatever the list
        // length, so past a few dozen ids one scan per chunk is cheaper. See BENCHMARKS.md.
        const PER_ID_MAX: usize = 32;
        const IN_CHUNK: usize = 256;
        let mut columns = vec!["id".to_string(), "text".to_string()];
        columns.extend(fields.iter().map(|(n, _)| format!("\"{n}\"")));
        let columns = columns.join(", ");
        let mut docs = Vec::with_capacity(ids.len());
        if ids.len() <= PER_ID_MAX {
            let sql = format!("SELECT {columns} FROM {table} WHERE status = 'READY' AND id = $1");
            for id in ids {
                let rows = self.query_rows(&sql, vec![stoolap::Value::integer(*id as i64)])?;
                for row in &rows {
                    docs.push(document_from_row(row, fields)?);
                }
            }
        } else {
            for chunk in ids.chunks(IN_CHUNK) {
                let placeholders: Vec<String> =
                    (1..=chunk.len()).map(|i| format!("${i}")).collect();
                let sql = format!(
                    "SELECT {columns} FROM {table} WHERE status = 'READY' AND id IN ({})",
                    placeholders.join(", ")
                );
                let params = chunk
                    .iter()
                    .map(|id| stoolap::Value::integer(*id as i64))
                    .collect();
                for row in &self.query_rows(&sql, params)? {
                    docs.push(document_from_row(row, fields)?);
                }
            }
        }
        Ok(docs)
    }

    /// Every id in the table regardless of status (recovery, spec section 26).
    pub fn all_ids(&self, table: &str) -> Result<Vec<u64>, ZenarError> {
        let rows = self.query_rows(&format!("SELECT id FROM {table}"), vec![])?;
        Ok(rows
            .iter()
            .filter_map(|r| r.get_value(0).and_then(|v| v.as_int64()).map(|i| i as u64))
            .collect())
    }

    pub fn count_ready(&self, table: &str) -> Result<usize, ZenarError> {
        let rows = self.query_rows(
            &format!("SELECT COUNT(*) FROM {table} WHERE status = 'READY'"),
            vec![],
        )?;
        let n = rows
            .first()
            .and_then(|r| r.get_value(0))
            .and_then(|v| v.as_int64())
            .unwrap_or(0);
        Ok(n.max(0) as usize)
    }
}

fn collection_from_row(row: &stoolap::api::ResultRow) -> Result<CollectionRow, ZenarError> {
    let name = row
        .get_value(0)
        .and_then(|v| v.as_string())
        .ok_or_else(|| store_msg("collection row without name"))?;
    let dimension = row
        .get_value(1)
        .and_then(|v| v.as_int64())
        .map(|d| d as usize);
    let bit_width = row
        .get_value(2)
        .and_then(|v| v.as_int64())
        .ok_or_else(|| store_msg("collection row without bit_width"))? as usize;
    let encoded = row
        .get_value(3)
        .and_then(|v| v.as_string())
        .unwrap_or_default();
    let fields = decode_fields(&encoded)?;
    Ok(CollectionRow {
        name,
        dimension,
        bit_width,
        fields,
    })
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
    row: &stoolap::api::ResultRow,
    fields: &[(String, FieldKind)],
) -> Result<DocumentRow, ZenarError> {
    let id = row
        .get_value(0)
        .and_then(|v| v.as_int64())
        .ok_or_else(|| store_msg("document row without id"))? as u64;
    let text = row
        .get_value(1)
        .and_then(|v| v.as_string())
        .unwrap_or_default();
    let mut metadata = Vec::new();
    for (i, (name, kind)) in fields.iter().enumerate() {
        if let Some(value) = row
            .get_value(i + 2)
            .and_then(|v| MetaValue::from_sql(*kind, v))
        {
            metadata.push((name.clone(), value));
        }
    }
    Ok(DocumentRow { id, text, metadata })
}

fn store_err(e: stoolap::Error) -> ZenarError {
    ZenarError::MetadataStoreError(e.to_string())
}

fn store_msg(msg: &str) -> ZenarError {
    ZenarError::MetadataStoreError(msg.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let docs = store.fetch_documents(&table, &[], &[1]).unwrap();
        assert_eq!(docs[0].text, payload);
    }

    #[test]
    fn field_name_validation() {
        assert!(is_valid_field_name("tenant"));
        assert!(!is_valid_field_name("Tenant"));
        assert!(!is_valid_field_name("id"));
        assert!(!is_valid_field_name("1abc"));
        assert!(!is_valid_field_name("a-b"));
    }

    /// Both hydration paths (per-id and chunked IN) must return every READY row exactly once.
    #[test]
    fn fetch_crosses_per_id_and_chunked_paths() {
        let store = MetadataStore::open_in_memory().unwrap();
        let table = MetadataStore::table_name("hydrate");
        store.create_document_table(&table).unwrap();
        for id in 0..600u64 {
            store
                .insert_document(&table, id, &format!("doc {id}"), Status::Ready, &[])
                .unwrap();
        }
        // A PENDING row must never be returned.
        store
            .insert_document(&table, 1000, "pending", Status::Pending, &[])
            .unwrap();
        for n in [1usize, 32, 33, 256, 257, 600] {
            let ids: Vec<u64> = (0..n as u64).collect();
            let docs = store.fetch_documents(&table, &[], &ids).unwrap();
            assert_eq!(docs.len(), n, "wrong count for {n} ids");
            let mut seen: Vec<u64> = docs.iter().map(|d| d.id).collect();
            seen.sort();
            assert_eq!(seen, ids);
        }
        let pending = store.fetch_documents(&table, &[], &[1000]).unwrap();
        assert!(pending.is_empty());
    }
}
