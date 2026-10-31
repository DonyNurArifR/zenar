//! Zenar's document store: an append-only log of documents, plus in-memory indexes
//! rebuilt from the log on open.
//!
//! The log is the source of truth. Each record is a frame: `len (u32 LE)`,
//! `crc32 (u32 LE)`, then the payload. A PUT stores a document (id, text, metadata).
//! A DEL is a tombstone. On open the log is replayed. A torn tail (a frame cut off by a
//! crash) is truncated. Corruption anywhere else is an error, so it is never silently dropped.
//!
//! Indexes are kept per metadata field. Each field has an ordered map from value to the rows
//! holding it, and the list of rows where the field is present (not NULL). Filters evaluate
//! to two row sets, "true" and "false", and the remaining rows are NULL. That is SQL's
//! three-valued logic, so `NOT`, `!=` and missing fields behave as before (spec section 68).

use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::ops::Bound;
use std::path::{Path, PathBuf};

use crate::error::ZenarError;
use crate::filter::Filter;
use crate::value::{FieldKind, MetaValue};

/// Payload size cap per record. Larger documents are rejected, not split.
pub const MAX_PAYLOAD: usize = 16 * 1024 * 1024;
/// Text size cap per document.
pub const MAX_TEXT: usize = 8 * 1024 * 1024;
/// Rows per collection. Row numbers are u32 internally.
pub const MAX_ROWS: usize = (u32::MAX - 1) as usize;

const PUT: u8 = 1;
const DEL: u8 = 2;
const FRAME_HEADER: usize = 8;

/// A document as read back from the log.
#[derive(Debug, Clone, PartialEq)]
pub struct DocumentRow {
    pub id: u64,
    pub text: String,
    pub metadata: Vec<(String, MetaValue)>,
}

/// A document to write. Borrowed, so batches are not copied.
#[derive(Debug, Clone, Copy)]
pub struct DocRef<'a> {
    pub id: u64,
    pub text: &'a str,
    pub metadata: &'a [(String, MetaValue)],
}

/// Where a live document lives in the log.
#[derive(Debug, Clone, Copy)]
struct Row {
    id: u64,
    payload_off: u64,
    payload_len: u32,
    live: bool,
}

/// A value as an index key. All keys in one field index have the same variant.
#[derive(Debug, Clone, PartialEq)]
enum Key {
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
    Time(i64),
}

impl Eq for Key {}

impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Key {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use Key::*;
        match (self, other) {
            (Bool(a), Bool(b)) => a.cmp(b),
            (Int(a), Int(b)) | (Time(a), Time(b)) => a.cmp(b),
            (Float(a), Float(b)) => a.total_cmp(b),
            (Text(a), Text(b)) => a.cmp(b),
            // Keys in one index always share a variant. Mixed variants never meet.
            _ => std::cmp::Ordering::Equal,
        }
    }
}

/// Summary of one metadata field across live documents. See `DocStore::field_stats`.
#[derive(Debug, Clone, PartialEq)]
pub struct FieldStats {
    pub name: String,
    pub kind: FieldKind,
    /// Live documents that carry this field (not NULL).
    pub present: usize,
    /// Distinct values with the number of live documents holding each, most frequent first.
    pub values: Vec<(MetaValue, usize)>,
    /// Smallest and largest value, for kinds with an order.
    pub min: Option<MetaValue>,
    pub max: Option<MetaValue>,
}

fn key_to_value(key: &Key) -> MetaValue {
    match key {
        Key::Bool(b) => MetaValue::Boolean(*b),
        Key::Int(i) => MetaValue::Integer(*i),
        Key::Float(f) => MetaValue::Float(*f),
        Key::Text(s) => MetaValue::Text(s.clone()),
        Key::Time(us) => MetaValue::Timestamp(
            chrono::DateTime::from_timestamp_micros(*us).unwrap_or(chrono::DateTime::UNIX_EPOCH),
        ),
    }
}

/// Index for one metadata field.
struct FieldIndex {
    kind: FieldKind,
    /// Value to rows, ascending by row. Rows marked dead are filtered out when read.
    values: BTreeMap<Key, Vec<u32>>,
    /// Rows where the field is present, ascending. Dead rows are filtered out when read.
    present: Vec<u32>,
}

/// One staged write, not yet visible to readers.
pub struct Staged {
    old_len: u64,
    puts: Vec<StagedPut>,
    dels: Vec<u64>,
}

struct StagedPut {
    id: u64,
    payload_off: u64,
    payload_len: u32,
    /// (field index, key) for each field the document carries.
    keys: Vec<(usize, Key)>,
}

/// Document log plus indexes for one collection.
pub struct DocStore {
    path: PathBuf,
    /// The append handle. `None` only while compaction has released it for the rename.
    file: Option<File>,
    len: u64,
    fields: Vec<(String, FieldKind)>,
    index: Vec<FieldIndex>,
    rows: Vec<Row>,
    by_id: HashMap<u64, u32>,
    live: usize,
    dead: usize,
}

impl DocStore {
    /// Creates an empty log at `path`. Fails if the file exists.
    pub fn create(path: &Path, fields: Vec<(String, FieldKind)>) -> Result<Self, ZenarError> {
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(path)
            .map_err(io_err)?;
        sync_parent(path)?;
        Ok(Self::empty(path.to_path_buf(), Some(file), 0, fields))
    }

    /// Opens an existing log and replays it. A torn tail is truncated.
    pub fn open(path: &Path, fields: Vec<(String, FieldKind)>) -> Result<Self, ZenarError> {
        let file = open_log(path).map_err(io_err)?;
        let file_len = file.metadata().map_err(io_err)?.len();
        let mut store = Self::empty(path.to_path_buf(), Some(file), 0, fields);
        let good_end = store.replay(file_len)?;
        if good_end < file_len {
            let file = store.handle()?;
            file.set_len(good_end).map_err(io_err)?;
            file.sync_data().map_err(io_err)?;
        }
        store.len = good_end;
        Ok(store)
    }

    fn empty(
        path: PathBuf,
        file: Option<File>,
        len: u64,
        fields: Vec<(String, FieldKind)>,
    ) -> Self {
        let index = fields
            .iter()
            .map(|(_, kind)| FieldIndex {
                kind: *kind,
                values: BTreeMap::new(),
                present: Vec::new(),
            })
            .collect();
        Self {
            path,
            file,
            len,
            fields,
            index,
            rows: Vec::new(),
            by_id: HashMap::new(),
            live: 0,
            dead: 0,
        }
    }

    /// Adds fields to the schema. Their index starts empty, and existing rows lack them (NULL).
    /// The caller must persist the schema before writing records that carry these fields.
    pub fn extend_fields(&mut self, new_fields: &[(String, FieldKind)]) {
        for (name, kind) in new_fields {
            self.fields.push((name.clone(), *kind));
            self.index.push(FieldIndex {
                kind: *kind,
                values: BTreeMap::new(),
                present: Vec::new(),
            });
        }
    }

    pub fn fields(&self) -> &[(String, FieldKind)] {
        &self.fields
    }

    pub fn live_count(&self) -> usize {
        self.live
    }

    pub fn contains(&self, id: u64) -> bool {
        self.by_id.contains_key(&id)
    }

    /// Summary of every metadata field across live documents: how many documents have it, the
    /// distinct values with a count for each, and the range for ordered kinds.
    pub fn field_stats(&self) -> Vec<FieldStats> {
        self.field_stats_with(&|row| self.is_live(row))
    }

    /// Like `field_stats`, but only over the given live rows (sorted ascending). Used for scoped
    /// discovery, so a tenant sees only its own values.
    pub fn field_stats_in(&self, rows: &[u32]) -> Vec<FieldStats> {
        // A bitmap over the rows makes each membership test constant-time.
        let mut bits = vec![0u64; self.rows.len() / 64 + 1];
        for &row in rows {
            bits[(row >> 6) as usize] |= 1u64 << (row & 63);
        }
        self.field_stats_with(&|row| {
            self.is_live(row) && bits[(row >> 6) as usize] & (1u64 << (row & 63)) != 0
        })
    }

    fn field_stats_with(&self, keep: &dyn Fn(u32) -> bool) -> Vec<FieldStats> {
        self.fields
            .iter()
            .enumerate()
            .map(|(fi, (name, kind))| {
                let index = &self.index[fi];
                let mut values: Vec<(MetaValue, usize)> = Vec::new();
                let mut present = 0usize;
                let (mut min, mut max): (Option<MetaValue>, Option<MetaValue>) = (None, None);
                // BTreeMap order is value order, so the first and last kept keys are the range.
                for (key, rows) in &index.values {
                    let kept = rows.iter().filter(|&&r| keep(r)).count();
                    if kept == 0 {
                        continue;
                    }
                    let value = key_to_value(key);
                    present += kept;
                    if min.is_none() {
                        min = Some(value.clone());
                    }
                    max = Some(value.clone());
                    values.push((value, kept));
                }
                // Most frequent first. The sort is stable, so ties keep value order.
                values.sort_by_key(|entry| std::cmp::Reverse(entry.1));
                FieldStats {
                    name: name.clone(),
                    kind: *kind,
                    present,
                    values,
                    min,
                    max,
                }
            })
            .collect()
    }

    /// Live rows for which `filter` is TRUE, ascending. The filter must be validated first.
    pub fn matching_rows(&self, filter: &Filter) -> Result<Vec<u32>, ZenarError> {
        Ok(self.eval(filter, false)?.0)
    }

    /// Ids of every live document.
    pub fn live_ids(&self) -> Vec<u64> {
        self.rows.iter().filter(|r| r.live).map(|r| r.id).collect()
    }

    /// Encodes and writes documents to the end of the log, without making them visible.
    /// On failure the file is truncated back to its previous length.
    pub fn stage_puts(&mut self, docs: &[DocRef<'_>]) -> Result<Staged, ZenarError> {
        let old_len = self.len;
        let mut buf = Vec::new();
        let mut puts = Vec::with_capacity(docs.len());
        for doc in docs {
            let payload = encode_put(doc, &self.fields)?;
            let frame_off = old_len + buf.len() as u64;
            let keys = doc
                .metadata
                .iter()
                .map(|(name, value)| {
                    let fi = self.field_position(name)?;
                    Ok((fi, key_for(self.index[fi].kind, value)?))
                })
                .collect::<Result<Vec<_>, ZenarError>>()?;
            puts.push(StagedPut {
                id: doc.id,
                payload_off: frame_off + FRAME_HEADER as u64,
                payload_len: payload.len() as u32,
                keys,
            });
            push_frame(&mut buf, &payload);
        }
        self.write_bytes(&buf, old_len)?;
        Ok(Staged {
            old_len,
            puts,
            dels: Vec::new(),
        })
    }

    /// Writes a tombstone for `id` to the end of the log, without making it visible.
    pub fn stage_delete(&mut self, id: u64) -> Result<Staged, ZenarError> {
        let old_len = self.len;
        let mut payload = vec![DEL];
        payload.extend_from_slice(&id.to_le_bytes());
        let mut buf = Vec::new();
        push_frame(&mut buf, &payload);
        self.write_bytes(&buf, old_len)?;
        Ok(Staged {
            old_len,
            puts: Vec::new(),
            dels: vec![id],
        })
    }

    /// Writes tombstones for many ids as one write. Equivalent to `stage_delete` for each id.
    pub fn stage_deletes(&mut self, ids: &[u64]) -> Result<Staged, ZenarError> {
        let old_len = self.len;
        let mut buf = Vec::with_capacity(ids.len() * (FRAME_HEADER + 9));
        for &id in ids {
            let mut payload = vec![DEL];
            payload.extend_from_slice(&id.to_le_bytes());
            push_frame(&mut buf, &payload);
        }
        self.write_bytes(&buf, old_len)?;
        Ok(Staged {
            old_len,
            puts: Vec::new(),
            dels: ids.to_vec(),
        })
    }

    fn write_bytes(&mut self, buf: &[u8], old_len: u64) -> Result<(), ZenarError> {
        let file = self.handle_mut()?;
        let result = file
            .seek(SeekFrom::Start(old_len))
            .and_then(|_| file.write_all(buf));
        if let Err(e) = result {
            let _ = file.set_len(old_len);
            return Err(io_err(e));
        }
        self.len = old_len + buf.len() as u64;
        Ok(())
    }

    /// Discards a staged write by truncating the log back to its length before the write.
    pub fn undo(&mut self, staged: Staged) -> Result<(), ZenarError> {
        self.handle()?.set_len(staged.old_len).map_err(io_err)?;
        self.len = staged.old_len;
        Ok(())
    }

    /// Makes a staged write visible to readers. Infallible, and must run after the write is durable.
    pub fn commit(&mut self, staged: Staged) {
        for put in staged.puts {
            let row = self.rows.len() as u32;
            self.rows.push(Row {
                id: put.id,
                payload_off: put.payload_off,
                payload_len: put.payload_len,
                live: true,
            });
            self.by_id.insert(put.id, row);
            self.live += 1;
            for (fi, key) in put.keys {
                let index = &mut self.index[fi];
                index.values.entry(key).or_default().push(row);
                index.present.push(row);
            }
        }
        for id in staged.dels {
            if let Some(row) = self.by_id.remove(&id) {
                self.rows[row as usize].live = false;
                self.live -= 1;
                self.dead += 1;
            }
        }
    }

    /// Flushes the log to stable storage.
    pub fn sync(&mut self) -> Result<(), ZenarError> {
        self.handle()?.sync_data().map_err(io_err)
    }

    /// True when dead rows are worth reclaiming: more dead than live, and enough to matter.
    pub fn should_compact(&self) -> bool {
        self.dead > 4096 && self.dead > self.live
    }

    /// Rewrites the log with only live documents, then replaces the old log atomically.
    ///
    /// The old log stays valid until the rename. Windows refuses to rename over a file that is
    /// still open, so the append handle is released first and reopened afterwards, on both the
    /// success path and the failure path.
    pub fn compact(&mut self) -> Result<(), ZenarError> {
        let tmp = self.path.with_extension("log.compact");
        let mut out = File::create(&tmp).map_err(io_err)?;
        let mut buf = Vec::new();
        for row in self.rows.iter().filter(|r| r.live) {
            let mut payload = vec![0u8; row.payload_len as usize];
            read_exact_at(self.handle()?, &mut payload, row.payload_off)?;
            push_frame(&mut buf, &payload);
            if buf.len() >= 1 << 20 {
                out.write_all(&buf).map_err(io_err)?;
                buf.clear();
            }
        }
        out.write_all(&buf).map_err(io_err)?;
        out.sync_all().map_err(io_err)?;
        drop(out);

        self.file = None;
        if let Err(e) = fs::rename(&tmp, &self.path) {
            let _ = fs::remove_file(&tmp);
            self.file = Some(open_log(&self.path).map_err(io_err)?);
            return Err(io_err(e));
        }
        sync_parent(&self.path)?;
        let fields = self.fields.clone();
        *self = Self::open(&self.path.clone(), fields)?;
        Ok(())
    }

    fn handle(&self) -> Result<&File, ZenarError> {
        self.file
            .as_ref()
            .ok_or_else(|| ZenarError::PersistenceError("document log handle is released".into()))
    }

    fn handle_mut(&mut self) -> Result<&mut File, ZenarError> {
        self.file
            .as_mut()
            .ok_or_else(|| ZenarError::PersistenceError("document log handle is released".into()))
    }

    /// Documents for the given ids, skipping ids that are not live. Order follows `ids`.
    pub fn fetch(&self, ids: &[u64]) -> Result<Vec<DocumentRow>, ZenarError> {
        let mut out = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(&row) = self.by_id.get(id) {
                out.push(self.read_row(row)?);
            }
        }
        Ok(out)
    }

    fn read_row(&self, row: u32) -> Result<DocumentRow, ZenarError> {
        let r = self.rows[row as usize];
        let mut payload = vec![0u8; r.payload_len as usize];
        read_exact_at(self.handle()?, &mut payload, r.payload_off)?;
        decode_put(&payload, &self.fields)
    }

    fn field_position(&self, name: &str) -> Result<usize, ZenarError> {
        self.fields
            .iter()
            .position(|(n, _)| n == name)
            .ok_or_else(|| {
                ZenarError::InvalidMetadata(format!("field {name:?} is not in the schema"))
            })
    }

    /// Ids of live documents for which `filter` is TRUE (SQL semantics: NULL does not match).
    /// The filter must already be validated against the schema.
    pub fn matching_ids(&self, filter: &Filter) -> Result<Vec<u64>, ZenarError> {
        let (t, _) = self.eval(filter, false)?;
        Ok(t.into_iter()
            .map(|row| self.rows[row as usize].id)
            .collect())
    }

    /// Live row numbers, ascending.
    fn live_rows(&self) -> Vec<u32> {
        self.rows
            .iter()
            .enumerate()
            .filter(|(_, r)| r.live)
            .map(|(i, _)| i as u32)
            .collect()
    }

    fn is_live(&self, row: u32) -> bool {
        self.rows[row as usize].live
    }

    /// (TRUE rows, FALSE rows) for a filter. Rows in neither set are NULL.
    ///
    /// The FALSE set is only built when `need_false` is set. A top-level filter does not need
    /// it, and building it costs a pass over every row of the field. Under `Not`, it is needed.
    fn eval(&self, filter: &Filter, need_false: bool) -> Result<(Vec<u32>, Vec<u32>), ZenarError> {
        match filter {
            Filter::And(children) => {
                if children.is_empty() {
                    return Ok((self.live_rows(), Vec::new()));
                }
                let mut trues: Vec<Vec<u32>> = Vec::with_capacity(children.len());
                let mut f: Vec<u32> = Vec::new();
                for child in children {
                    let (ct, cf) = self.eval(child, need_false)?;
                    trues.push(ct);
                    if need_false {
                        f = union(&f, &cf);
                    }
                }
                // Intersect the smallest sets first, so the running result shrinks early.
                trues.sort_by_key(|t| t.len());
                let mut trues = trues.into_iter();
                let mut t = trues.next().unwrap_or_default();
                for next in trues {
                    if t.is_empty() {
                        break;
                    }
                    t = intersect(&t, &next);
                }
                Ok((t, f))
            }
            Filter::Or(children) => {
                if children.is_empty() {
                    return Ok((Vec::new(), self.live_rows()));
                }
                let mut t: Vec<u32> = Vec::new();
                let mut f: Option<Vec<u32>> = None;
                for child in children {
                    let (ct, cf) = self.eval(child, need_false)?;
                    t = union(&t, &ct);
                    if need_false {
                        f = Some(match f {
                            None => cf,
                            Some(acc) => intersect(&acc, &cf),
                        });
                    }
                }
                Ok((t, f.unwrap_or_default()))
            }
            Filter::Not(child) => {
                // NOT needs the child's FALSE rows as its TRUE rows.
                let (t, f) = self.eval(child, true)?;
                Ok((f, t))
            }
            Filter::In { field, values } => {
                if values.is_empty() {
                    let f = if need_false {
                        self.live_rows()
                    } else {
                        Vec::new()
                    };
                    return Ok((Vec::new(), f));
                }
                let (fi, _) = self.resolve(field)?;
                let mut t = Vec::new();
                for v in values {
                    t = union(&t, &self.matching(fi, &Cmp::Eq, v)?);
                }
                let f = if need_false {
                    difference(&self.present_live(fi), &t)
                } else {
                    Vec::new()
                };
                Ok((t, f))
            }
            Filter::Nin { field, values } => {
                let (fi, _) = self.resolve(field)?;
                if values.is_empty() {
                    // `NOT IN ()` excludes only NULL rows.
                    let present = self.present_live(fi);
                    let f = if need_false {
                        difference(&self.live_rows(), &present)
                    } else {
                        Vec::new()
                    };
                    return Ok((present, f));
                }
                let mut hit = Vec::new();
                for v in values {
                    hit = union(&hit, &self.matching(fi, &Cmp::Eq, v)?);
                }
                Ok((difference(&self.present_live(fi), &hit), hit))
            }
            Filter::Eq { field, value } => self.leaf(field, Cmp::Eq, value, need_false),
            Filter::Ne { field, value } => {
                let (fi, _) = self.resolve(field)?;
                let hit = self.matching(fi, &Cmp::Eq, value)?;
                Ok((difference(&self.present_live(fi), &hit), hit))
            }
            Filter::Gt { field, value } => self.leaf(field, Cmp::Gt, value, need_false),
            Filter::Gte { field, value } => self.leaf(field, Cmp::Gte, value, need_false),
            Filter::Lt { field, value } => self.leaf(field, Cmp::Lt, value, need_false),
            Filter::Lte { field, value } => self.leaf(field, Cmp::Lte, value, need_false),
        }
    }

    fn leaf(
        &self,
        field: &str,
        cmp: Cmp,
        value: &MetaValue,
        need_false: bool,
    ) -> Result<(Vec<u32>, Vec<u32>), ZenarError> {
        let (fi, _) = self.resolve(field)?;
        let hit = self.matching(fi, &cmp, value)?;
        let f = if need_false {
            difference(&self.present_live(fi), &hit)
        } else {
            Vec::new()
        };
        Ok((hit, f))
    }

    fn resolve(&self, field: &str) -> Result<(usize, FieldKind), ZenarError> {
        let fi = self
            .fields
            .iter()
            .position(|(n, _)| n == field)
            .ok_or_else(|| {
                ZenarError::InvalidFilter(format!("unknown metadata field {field:?}"))
            })?;
        Ok((fi, self.fields[fi].1))
    }

    /// Rows where field `fi` is present and live.
    fn present_live(&self, fi: usize) -> Vec<u32> {
        self.index[fi]
            .present
            .iter()
            .copied()
            .filter(|&r| self.is_live(r))
            .collect()
    }

    /// Live rows whose value of field `fi` matches `cmp value`.
    fn matching(&self, fi: usize, cmp: &Cmp, value: &MetaValue) -> Result<Vec<u32>, ZenarError> {
        let index = &self.index[fi];
        let kind = index.kind;
        let mut out: Vec<u32> = match key_for(kind, value) {
            Ok(key) => {
                let range: (Bound<&Key>, Bound<&Key>) = match cmp {
                    Cmp::Eq => (Bound::Included(&key), Bound::Included(&key)),
                    Cmp::Gt => (Bound::Excluded(&key), Bound::Unbounded),
                    Cmp::Gte => (Bound::Included(&key), Bound::Unbounded),
                    Cmp::Lt => (Bound::Unbounded, Bound::Excluded(&key)),
                    Cmp::Lte => (Bound::Unbounded, Bound::Included(&key)),
                };
                let groups: Vec<&Vec<u32>> = index
                    .values
                    .range::<Key, _>(range)
                    .map(|(_, rows)| rows)
                    .collect();
                match groups.as_slice() {
                    [] => Vec::new(),
                    // One value: its postings are already in row order.
                    [only] => only.iter().copied().filter(|&r| self.is_live(r)).collect(),
                    // Several values: set bits in a bitmap over the rows, then read them back in order.
                    // That is linear in the matches, where sorting them is not.
                    _ => {
                        let mut bits = vec![0u64; self.rows.len() / 64 + 1];
                        for rows in groups {
                            for &r in rows {
                                if self.is_live(r) {
                                    bits[(r >> 6) as usize] |= 1u64 << (r & 63);
                                }
                            }
                        }
                        let mut out = Vec::new();
                        for (word_index, &word) in bits.iter().enumerate() {
                            let mut w = word;
                            while w != 0 {
                                out.push((word_index as u32) * 64 + w.trailing_zeros());
                                w &= w - 1;
                            }
                        }
                        out
                    }
                }
            }
            // Mixed integer and float comparisons: scan the distinct values of this field.
            Err(_) => {
                let as_f64 = match value {
                    MetaValue::Integer(i) => *i as f64,
                    MetaValue::Float(f) => *f,
                    _ => {
                        return Err(ZenarError::InvalidFilter(format!(
                            "value {value:?} cannot be compared with a {} field",
                            kind.as_str()
                        )));
                    }
                };
                let mut rows = Vec::new();
                for (k, rs) in &index.values {
                    let x = match k {
                        Key::Int(i) => *i as f64,
                        Key::Float(f) => *f,
                        _ => continue,
                    };
                    let ok = match cmp {
                        Cmp::Eq => x == as_f64,
                        Cmp::Gt => x > as_f64,
                        Cmp::Gte => x >= as_f64,
                        Cmp::Lt => x < as_f64,
                        Cmp::Lte => x <= as_f64,
                    };
                    if ok {
                        rows.extend(rs.iter().copied().filter(|&r| self.is_live(r)));
                    }
                }
                rows.sort_unstable();
                rows
            }
        };
        out.dedup();
        Ok(out)
    }

    /// Replays the log bytes into indexes. Returns the offset just past the last valid frame.
    /// Replays the log frame by frame, streaming it so memory use does not grow with the log.
    /// Returns the offset just past the last valid frame. See the module docs for the rules.
    fn replay(&mut self, file_len: u64) -> Result<u64, ZenarError> {
        let reader_file = File::open(&self.path).map_err(io_err)?;
        let mut reader = BufReader::with_capacity(1 << 20, reader_file);
        let mut pos: u64 = 0;
        while pos < file_len {
            let remaining = file_len - pos;
            if remaining < FRAME_HEADER as u64 {
                return Ok(pos); // torn header
            }
            let mut header = [0u8; FRAME_HEADER];
            reader.read_exact(&mut header).map_err(io_err)?;
            let len = u32::from_le_bytes(header[0..4].try_into().unwrap()) as u64;
            let crc = u32::from_le_bytes(header[4..8].try_into().unwrap());
            if header == [0u8; FRAME_HEADER] {
                // A zero header is never a record. It is a torn tail if only zeros follow.
                return self.zeros_or_corrupt(&mut reader, pos, file_len);
            }
            let frame_end = pos + FRAME_HEADER as u64 + len;
            if frame_end > file_len {
                return Ok(pos); // torn payload
            }
            // Check the length before allocating for the payload.
            let oversized = len as usize > MAX_PAYLOAD;
            let payload = if oversized {
                Vec::new()
            } else {
                let mut payload = vec![0u8; len as usize];
                reader.read_exact(&mut payload).map_err(io_err)?;
                payload
            };
            if oversized || crc32(&payload) != crc {
                // A bad frame that runs to EOF is a torn write. Anything after it is corruption.
                if frame_end == file_len {
                    return Ok(pos);
                }
                return Err(ZenarError::RecoveryError(format!(
                    "corrupt record at byte {pos} of {}; refusing to drop later records",
                    self.path.display()
                )));
            }
            self.apply_replayed(&payload, pos + FRAME_HEADER as u64)?;
            pos = frame_end;
        }
        Ok(pos)
    }

    /// Called at a zero header. Returns the torn-tail offset if the rest of the file is zeros.
    fn zeros_or_corrupt(
        &self,
        reader: &mut BufReader<File>,
        pos: u64,
        file_len: u64,
    ) -> Result<u64, ZenarError> {
        let mut buf = vec![0u8; 64 * 1024];
        let mut left = file_len - pos - FRAME_HEADER as u64;
        while left > 0 {
            let n = left.min(buf.len() as u64) as usize;
            reader.read_exact(&mut buf[..n]).map_err(io_err)?;
            if buf[..n].iter().any(|&b| b != 0) {
                return Err(ZenarError::RecoveryError(format!(
                    "corrupt record at byte {pos} of {}; refusing to drop later records",
                    self.path.display()
                )));
            }
            left -= n as u64;
        }
        Ok(pos)
    }

    fn apply_replayed(&mut self, payload: &[u8], payload_off: u64) -> Result<(), ZenarError> {
        match payload.first() {
            Some(&PUT) => {
                let doc = decode_put(payload, &self.fields)?;
                if self.by_id.contains_key(&doc.id) {
                    return Err(ZenarError::RecoveryError(format!(
                        "duplicate live id {} in log",
                        doc.id
                    )));
                }
                let mut keys = Vec::with_capacity(doc.metadata.len());
                for (name, value) in &doc.metadata {
                    let fi = self.field_position(name)?;
                    keys.push((fi, key_for(self.index[fi].kind, value)?));
                }
                let staged = Staged {
                    old_len: 0,
                    puts: vec![StagedPut {
                        id: doc.id,
                        payload_off,
                        payload_len: payload.len() as u32,
                        keys,
                    }],
                    dels: Vec::new(),
                };
                self.commit(staged);
                Ok(())
            }
            Some(&DEL) => {
                let id = read_u64(payload, 1)?;
                if let Some(row) = self.by_id.remove(&id) {
                    self.rows[row as usize].live = false;
                    self.live -= 1;
                    self.dead += 1;
                }
                Ok(())
            }
            _ => Err(ZenarError::RecoveryError(
                "unknown record kind in log".into(),
            )),
        }
    }
}

#[derive(Clone, Copy)]
enum Cmp {
    Eq,
    Gt,
    Gte,
    Lt,
    Lte,
}

/// Index key for a value of a field with this kind. Integers are widened for float fields.
/// Other mismatches return an error, and the caller uses a scan for mixed numeric comparisons.
fn key_for(kind: FieldKind, value: &MetaValue) -> Result<Key, ZenarError> {
    match (kind, value) {
        (FieldKind::Text, MetaValue::Text(s)) => Ok(Key::Text(s.clone())),
        (FieldKind::Integer, MetaValue::Integer(i)) => Ok(Key::Int(*i)),
        (FieldKind::Float, MetaValue::Float(f)) => Ok(Key::Float(if *f == 0.0 { 0.0 } else { *f })),
        (FieldKind::Float, MetaValue::Integer(i)) => Ok(Key::Float(*i as f64)),
        (FieldKind::Boolean, MetaValue::Boolean(b)) => Ok(Key::Bool(*b)),
        (FieldKind::Timestamp, MetaValue::Timestamp(t)) => Ok(Key::Time(t.timestamp_micros())),
        _ => Err(ZenarError::InvalidMetadata(format!(
            "value {value:?} does not match field kind {}",
            kind.as_str()
        ))),
    }
}

// ----- sorted set operations -------------------------------------------------

fn intersect(a: &[u32], b: &[u32]) -> Vec<u32> {
    // A small list against a much larger one: binary-search each element instead of walking the big list.
    let (small, large) = if a.len() <= b.len() { (a, b) } else { (b, a) };
    if small.len() * 16 < large.len() {
        let mut out = Vec::with_capacity(small.len());
        let mut lo = 0;
        for &x in small {
            match large[lo..].binary_search(&x) {
                Ok(i) => {
                    out.push(x);
                    lo += i + 1;
                }
                Err(i) => lo += i,
            }
            if lo >= large.len() {
                break;
            }
        }
        return out;
    }
    let (mut i, mut j) = (0, 0);
    let mut out = Vec::with_capacity(a.len().min(b.len()));
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                out.push(a[i]);
                i += 1;
                j += 1;
            }
        }
    }
    out
}

fn union(a: &[u32], b: &[u32]) -> Vec<u32> {
    let (mut i, mut j) = (0, 0);
    let mut out = Vec::with_capacity(a.len() + b.len());
    while i < a.len() || j < b.len() {
        if j >= b.len() || (i < a.len() && a[i] < b[j]) {
            out.push(a[i]);
            i += 1;
        } else if i >= a.len() || b[j] < a[i] {
            out.push(b[j]);
            j += 1;
        } else {
            out.push(a[i]);
            i += 1;
            j += 1;
        }
    }
    out
}

fn difference(a: &[u32], b: &[u32]) -> Vec<u32> {
    let (mut i, mut j) = (0, 0);
    let mut out = Vec::with_capacity(a.len());
    while i < a.len() {
        if j >= b.len() || a[i] < b[j] {
            out.push(a[i]);
            i += 1;
        } else if a[i] > b[j] {
            j += 1;
        } else {
            i += 1;
            j += 1;
        }
    }
    out
}

// ----- record encoding -------------------------------------------------------

fn encode_put(doc: &DocRef<'_>, fields: &[(String, FieldKind)]) -> Result<Vec<u8>, ZenarError> {
    if doc.text.len() > MAX_TEXT {
        return Err(ZenarError::InvalidArgument(format!(
            "text exceeds {MAX_TEXT} bytes"
        )));
    }
    let mut out = Vec::with_capacity(32 + doc.text.len());
    out.push(PUT);
    out.extend_from_slice(&doc.id.to_le_bytes());
    out.extend_from_slice(&(doc.text.len() as u32).to_le_bytes());
    out.extend_from_slice(doc.text.as_bytes());
    out.extend_from_slice(&(doc.metadata.len() as u16).to_le_bytes());
    for (name, value) in doc.metadata {
        if !fields.iter().any(|(n, _)| n == name) {
            return Err(ZenarError::InvalidMetadata(format!(
                "field {name:?} is not in the schema"
            )));
        }
        out.push(name.len() as u8);
        out.extend_from_slice(name.as_bytes());
        match value {
            MetaValue::Text(s) => {
                out.push(1);
                out.extend_from_slice(&(s.len() as u32).to_le_bytes());
                out.extend_from_slice(s.as_bytes());
            }
            MetaValue::Integer(i) => {
                out.push(2);
                out.extend_from_slice(&i.to_le_bytes());
            }
            MetaValue::Float(f) => {
                out.push(3);
                out.extend_from_slice(&f.to_bits().to_le_bytes());
            }
            MetaValue::Boolean(b) => {
                out.push(4);
                out.push(u8::from(*b));
            }
            MetaValue::Timestamp(t) => {
                out.push(5);
                out.extend_from_slice(&t.timestamp_micros().to_le_bytes());
            }
        }
    }
    if out.len() > MAX_PAYLOAD {
        return Err(ZenarError::InvalidArgument(format!(
            "document exceeds {MAX_PAYLOAD} bytes"
        )));
    }
    Ok(out)
}

fn decode_put(payload: &[u8], fields: &[(String, FieldKind)]) -> Result<DocumentRow, ZenarError> {
    let mut r = Reader {
        buf: payload,
        pos: 0,
    };
    if r.u8()? != PUT {
        return Err(ZenarError::RecoveryError(
            "expected a document record".into(),
        ));
    }
    let id = r.u64()?;
    let text_len = r.u32()? as usize;
    let text = String::from_utf8(r.bytes(text_len)?.to_vec())
        .map_err(|_| ZenarError::RecoveryError("document text is not UTF-8".into()))?;
    let n = r.u16()? as usize;
    let mut metadata = Vec::with_capacity(n);
    for _ in 0..n {
        let name_len = r.u8()? as usize;
        let name = String::from_utf8(r.bytes(name_len)?.to_vec())
            .map_err(|_| ZenarError::RecoveryError("field name is not UTF-8".into()))?;
        let value = match r.u8()? {
            1 => {
                let len = r.u32()? as usize;
                MetaValue::Text(
                    String::from_utf8(r.bytes(len)?.to_vec())
                        .map_err(|_| ZenarError::RecoveryError("text value is not UTF-8".into()))?,
                )
            }
            2 => MetaValue::Integer(r.i64()?),
            3 => MetaValue::Float(f64::from_bits(r.u64()?)),
            4 => MetaValue::Boolean(r.u8()? != 0),
            5 => MetaValue::Timestamp(
                chrono::DateTime::from_timestamp_micros(r.i64()?)
                    .ok_or_else(|| ZenarError::RecoveryError("timestamp out of range".into()))?,
            ),
            tag => {
                return Err(ZenarError::RecoveryError(format!(
                    "unknown value tag {tag}"
                )));
            }
        };
        if !fields.iter().any(|(fname, _)| *fname == name) {
            return Err(ZenarError::RecoveryError(format!(
                "record uses unknown field {name:?}"
            )));
        }
        metadata.push((name, value));
    }
    Ok(DocumentRow { id, text, metadata })
}

struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn bytes(&mut self, n: usize) -> Result<&'a [u8], ZenarError> {
        if self.pos + n > self.buf.len() {
            return Err(ZenarError::RecoveryError("record is truncated".into()));
        }
        let out = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(out)
    }
    fn u8(&mut self) -> Result<u8, ZenarError> {
        Ok(self.bytes(1)?[0])
    }
    fn u16(&mut self) -> Result<u16, ZenarError> {
        Ok(u16::from_le_bytes(self.bytes(2)?.try_into().unwrap()))
    }
    fn u32(&mut self) -> Result<u32, ZenarError> {
        Ok(u32::from_le_bytes(self.bytes(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64, ZenarError> {
        Ok(u64::from_le_bytes(self.bytes(8)?.try_into().unwrap()))
    }
    fn i64(&mut self) -> Result<i64, ZenarError> {
        Ok(i64::from_le_bytes(self.bytes(8)?.try_into().unwrap()))
    }
}

fn read_u64(buf: &[u8], at: usize) -> Result<u64, ZenarError> {
    buf.get(at..at + 8)
        .map(|b| u64::from_le_bytes(b.try_into().unwrap()))
        .ok_or_else(|| ZenarError::RecoveryError("tombstone is truncated".into()))
}

fn push_frame(buf: &mut Vec<u8>, payload: &[u8]) {
    buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    buf.extend_from_slice(&crc32(payload).to_le_bytes());
    buf.extend_from_slice(payload);
}

/// CRC-32 (IEEE, reflected), as used by zlib and gzip.
pub fn crc32(data: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFFu32;
    for &byte in data {
        crc ^= byte as u32;
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
        }
    }
    !crc
}

#[cfg(unix)]
fn read_exact_at(file: &File, buf: &mut [u8], offset: u64) -> Result<(), ZenarError> {
    use std::os::unix::fs::FileExt;
    file.read_exact_at(buf, offset).map_err(io_err)
}

#[cfg(windows)]
fn read_exact_at(file: &File, buf: &mut [u8], offset: u64) -> Result<(), ZenarError> {
    use std::os::windows::fs::FileExt;
    let mut done = 0;
    while done < buf.len() {
        let n = file
            .seek_read(&mut buf[done..], offset + done as u64)
            .map_err(io_err)?;
        if n == 0 {
            return Err(ZenarError::PersistenceError("unexpected end of log".into()));
        }
        done += n;
    }
    Ok(())
}

/// Opens the log for reading and writing. Writes seek to the tracked length first, because
/// append mode on Windows grants no right to truncate, and truncation is needed to undo a write.
fn open_log(path: &Path) -> std::io::Result<File> {
    OpenOptions::new().read(true).write(true).open(path)
}

/// Makes a new file name durable by syncing its directory (no-op where that is not supported).
pub fn sync_parent(path: &Path) -> Result<(), ZenarError> {
    #[cfg(unix)]
    {
        if let Some(dir) = path.parent() {
            File::open(dir).and_then(|d| d.sync_all()).map_err(io_err)?;
        }
    }
    let _ = path;
    Ok(())
}

pub fn io_err(e: std::io::Error) -> ZenarError {
    ZenarError::PersistenceError(e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cmp::Ordering;

    fn schema() -> Vec<(String, FieldKind)> {
        vec![
            ("tenant".to_string(), FieldKind::Text),
            ("qty".to_string(), FieldKind::Integer),
            ("price".to_string(), FieldKind::Float),
            ("active".to_string(), FieldKind::Boolean),
            ("at".to_string(), FieldKind::Timestamp),
        ]
    }

    fn temp_log(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("zenar-docstore-{tag}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir.join("docs.log")
    }

    /// Writes and commits a batch of documents.
    type Doc = (u64, &'static str, Vec<(String, MetaValue)>);
    type OwnedDoc = (u64, String, Vec<(String, MetaValue)>);

    fn put(store: &mut DocStore, docs: &[Doc]) {
        let refs: Vec<DocRef<'_>> = docs
            .iter()
            .map(|(id, text, meta)| DocRef {
                id: *id,
                text,
                metadata: meta.as_slice(),
            })
            .collect();
        let staged = store.stage_puts(&refs).unwrap();
        store.commit(staged);
    }

    fn del(store: &mut DocStore, id: u64) {
        let staged = store.stage_delete(id).unwrap();
        store.commit(staged);
    }

    #[test]
    fn crc32_matches_the_standard_check_value() {
        // The standard check value for CRC-32/IEEE is 0xCBF43926 for "123456789".
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
        assert_eq!(crc32(b""), 0);
    }

    #[test]
    fn set_operations_on_sorted_lists() {
        let a = [1, 3, 5, 7];
        let b = [3, 4, 5, 9];
        assert_eq!(intersect(&a, &b), vec![3, 5]);
        assert_eq!(union(&a, &b), vec![1, 3, 4, 5, 7, 9]);
        assert_eq!(difference(&a, &b), vec![1, 7]);
        assert_eq!(difference(&b, &a), vec![4, 9]);
        assert!(intersect(&a, &[]).is_empty());
        assert_eq!(union(&[], &b), b.to_vec());
    }

    /// The galloping and linear intersections must agree with a naive one, at every size ratio.
    #[test]
    fn intersect_agrees_with_naive_across_size_ratios() {
        let mut state = 0x2545_F491_4F6C_DD1Du64;
        let mut next = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        for small_len in [0usize, 1, 3, 40, 500] {
            for large_len in [0usize, 5, 600, 20_000] {
                let mut a: Vec<u32> = (0..small_len).map(|_| (next() % 50_000) as u32).collect();
                let mut b: Vec<u32> = (0..large_len).map(|_| (next() % 50_000) as u32).collect();
                a.sort_unstable();
                a.dedup();
                b.sort_unstable();
                b.dedup();
                let naive: Vec<u32> = a
                    .iter()
                    .copied()
                    .filter(|x| b.binary_search(x).is_ok())
                    .collect();
                assert_eq!(intersect(&a, &b), naive, "sizes {small_len} x {large_len}");
                assert_eq!(intersect(&b, &a), naive, "sizes {large_len} x {small_len}");
            }
        }
    }

    #[test]
    fn round_trip_and_tombstone() {
        let path = temp_log("roundtrip");
        let mut store = DocStore::create(&path, schema()).unwrap();
        put(
            &mut store,
            &[
                (
                    1,
                    "one",
                    vec![("tenant".into(), MetaValue::Text("A".into()))],
                ),
                (2, "two", vec![]),
            ],
        );
        del(&mut store, 1);
        assert!(!store.contains(1));
        assert_eq!(store.live_count(), 1);
        let docs = store.fetch(&[2, 1]).unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0].text, "two");
        drop(store);

        let reopened = DocStore::open(&path, schema()).unwrap();
        assert_eq!(reopened.live_count(), 1);
        assert!(!reopened.contains(1));
        assert!(reopened.contains(2));
        fs::remove_dir_all(path.parent().unwrap()).ok();
    }

    #[test]
    fn id_can_be_reused_after_delete() {
        let path = temp_log("reuse");
        let mut store = DocStore::create(&path, schema()).unwrap();
        put(&mut store, &[(7, "first", vec![])]);
        del(&mut store, 7);
        put(&mut store, &[(7, "second", vec![])]);
        drop(store);
        let reopened = DocStore::open(&path, schema()).unwrap();
        assert_eq!(reopened.fetch(&[7]).unwrap()[0].text, "second");
        fs::remove_dir_all(path.parent().unwrap()).ok();
    }

    #[test]
    fn torn_tail_is_truncated_and_earlier_records_survive() {
        let path = temp_log("torn");
        {
            let mut store = DocStore::create(&path, schema()).unwrap();
            put(&mut store, &[(1, "kept", vec![]), (2, "also kept", vec![])]);
        }
        let good = fs::metadata(&path).unwrap().len();
        // A frame header promising 100 bytes, with only 5 written.
        let mut bytes = fs::read(&path).unwrap();
        bytes.extend_from_slice(&100u32.to_le_bytes());
        bytes.extend_from_slice(&0u32.to_le_bytes());
        bytes.extend_from_slice(&[1, 2, 3, 4, 5]);
        fs::write(&path, &bytes).unwrap();

        let store = DocStore::open(&path, schema()).unwrap();
        assert_eq!(store.live_count(), 2);
        assert_eq!(
            fs::metadata(&path).unwrap().len(),
            good,
            "torn tail must be truncated"
        );
        fs::remove_dir_all(path.parent().unwrap()).ok();
    }

    /// A crash can leave the last record cut at any byte. For every cut point, and for every
    /// single-byte corruption inside that record, reopening must succeed and must show either
    /// the earlier records alone or all records. It must never show a partial record.
    #[test]
    fn every_cut_and_flip_in_the_last_record_recovers_cleanly() {
        let path = temp_log("every-cut");
        let before;
        {
            let mut store = DocStore::create(&path, schema()).unwrap();
            put(&mut store, &[(1, "kept", vec![]), (2, "also kept", vec![])]);
            before = fs::metadata(&path).unwrap().len();
            put(&mut store, &[(3, "the last record", vec![])]);
        }
        let full = fs::read(&path).unwrap();
        let scratch = path.with_file_name("scratch.log");

        let mut check = |bytes: &[u8], what: String| {
            fs::write(&scratch, bytes).unwrap();
            let store = DocStore::open(&scratch, schema())
                .unwrap_or_else(|e| panic!("{what}: open failed: {e}"));
            let last = store.fetch(&[3]).unwrap().len();
            assert_eq!(
                store.live_count(),
                2 + last,
                "{what}: partial record visible"
            );
            assert_eq!(
                store.fetch(&[1]).unwrap().len(),
                1,
                "{what}: earlier record lost"
            );
            assert_eq!(
                store.fetch(&[2]).unwrap().len(),
                1,
                "{what}: earlier record lost"
            );
            drop(store);
            fs::remove_file(&scratch).ok();
        };

        for cut in before as usize..full.len() {
            check(&full[..cut], format!("cut at byte {cut}"));
        }
        for at in before as usize..full.len() {
            let mut flipped = full.clone();
            flipped[at] ^= 0xFF;
            check(&flipped, format!("flip at byte {at}"));
        }
        fs::remove_dir_all(path.parent().unwrap()).ok();
    }

    #[test]
    fn zero_filled_tail_is_treated_as_torn() {
        let path = temp_log("zeros");
        {
            let mut store = DocStore::create(&path, schema()).unwrap();
            put(&mut store, &[(1, "kept", vec![])]);
        }
        let mut bytes = fs::read(&path).unwrap();
        bytes.extend_from_slice(&[0u8; 64]);
        fs::write(&path, &bytes).unwrap();
        let store = DocStore::open(&path, schema()).unwrap();
        assert_eq!(store.live_count(), 1);
        fs::remove_dir_all(path.parent().unwrap()).ok();
    }

    #[test]
    fn corruption_in_the_middle_is_an_error_not_a_silent_drop() {
        let path = temp_log("corrupt");
        {
            let mut store = DocStore::create(&path, schema()).unwrap();
            put(
                &mut store,
                &[(1, "first record", vec![]), (2, "second record", vec![])],
            );
        }
        let mut bytes = fs::read(&path).unwrap();
        bytes[FRAME_HEADER + 3] ^= 0xFF; // damage the first record's payload
        fs::write(&path, &bytes).unwrap();
        assert!(matches!(
            DocStore::open(&path, schema()),
            Err(ZenarError::RecoveryError(_))
        ));
        fs::remove_dir_all(path.parent().unwrap()).ok();
    }

    #[test]
    fn compaction_keeps_live_documents_and_drops_dead_bytes() {
        let path = temp_log("compact");
        let mut store = DocStore::create(&path, schema()).unwrap();
        let docs: Vec<OwnedDoc> = (0..50u64)
            .map(|i| {
                (
                    i,
                    format!("text {i}"),
                    vec![("qty".to_string(), MetaValue::Integer(i as i64))],
                )
            })
            .collect();
        for chunk in docs.chunks(10) {
            let refs: Vec<DocRef<'_>> = chunk
                .iter()
                .map(|(i, t, m)| DocRef {
                    id: *i,
                    text: t.as_str(),
                    metadata: m.as_slice(),
                })
                .collect();
            let staged = store.stage_puts(&refs).unwrap();
            store.commit(staged);
        }
        for i in 0..40u64 {
            del(&mut store, i);
        }
        let before = fs::metadata(&path).unwrap().len();
        store.compact().unwrap();
        assert!(fs::metadata(&path).unwrap().len() < before);
        assert_eq!(store.live_count(), 10);
        let f = Filter::Gte {
            field: "qty".into(),
            value: MetaValue::Integer(45),
        };
        let mut got = store.matching_ids(&f).unwrap();
        got.sort_unstable();
        assert_eq!(got, vec![45, 46, 47, 48, 49]);
        fs::remove_dir_all(path.parent().unwrap()).ok();
    }

    /// After compaction the handle is reopened on the new file. Writes made after it must land
    /// there, and survive a reopen.
    #[test]
    fn writes_after_compaction_go_to_the_new_log() {
        let path = temp_log("after-compact");
        let mut store = DocStore::create(&path, schema()).unwrap();
        let docs: Vec<Doc> = (0..30u64).map(|i| (i, "x", vec![])).collect();
        put(&mut store, &docs);
        for i in 0..20u64 {
            del(&mut store, i);
        }
        store.compact().unwrap();
        put(&mut store, &[(100, "after compaction", vec![])]);
        assert_eq!(store.fetch(&[100]).unwrap()[0].text, "after compaction");
        drop(store);

        let reopened = DocStore::open(&path, schema()).unwrap();
        assert_eq!(reopened.live_count(), 11);
        assert_eq!(reopened.fetch(&[100]).unwrap()[0].text, "after compaction");
        fs::remove_dir_all(path.parent().unwrap()).ok();
    }

    // ----- three-valued logic: indexed evaluation against a brute-force reference -----

    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
        fn chance(&mut self, percent: u64) -> bool {
            self.below(100) < percent
        }
    }

    /// Reference truth value of a filter for one document (SQL semantics). `None` is NULL.
    fn reference(filter: &Filter, doc: &[(String, MetaValue)]) -> Option<bool> {
        let get = |field: &str| doc.iter().find(|(n, _)| n == field).map(|(_, v)| v);
        match filter {
            Filter::And(children) => {
                if children.is_empty() {
                    return Some(true);
                }
                let vals: Vec<Option<bool>> = children.iter().map(|c| reference(c, doc)).collect();
                if vals.contains(&Some(false)) {
                    Some(false)
                } else if vals.iter().all(|v| *v == Some(true)) {
                    Some(true)
                } else {
                    None
                }
            }
            Filter::Or(children) => {
                if children.is_empty() {
                    return Some(false);
                }
                let vals: Vec<Option<bool>> = children.iter().map(|c| reference(c, doc)).collect();
                if vals.contains(&Some(true)) {
                    Some(true)
                } else if vals.iter().all(|v| *v == Some(false)) {
                    Some(false)
                } else {
                    None
                }
            }
            Filter::Not(child) => reference(child, doc).map(|b| !b),
            Filter::Eq { field, value } => {
                get(field).map(|x| compare(x, value) == Some(Ordering::Equal))
            }
            Filter::Ne { field, value } => {
                get(field).map(|x| compare(x, value) != Some(Ordering::Equal))
            }
            Filter::Gt { field, value } => {
                get(field).map(|x| compare(x, value) == Some(Ordering::Greater))
            }
            Filter::Gte { field, value } => get(field)
                .map(|x| matches!(compare(x, value), Some(Ordering::Greater | Ordering::Equal))),
            Filter::Lt { field, value } => {
                get(field).map(|x| compare(x, value) == Some(Ordering::Less))
            }
            Filter::Lte { field, value } => get(field)
                .map(|x| matches!(compare(x, value), Some(Ordering::Less | Ordering::Equal))),
            Filter::In { field, values } => {
                if values.is_empty() {
                    return Some(false);
                }
                get(field).map(|x| {
                    values
                        .iter()
                        .any(|v| compare(x, v) == Some(Ordering::Equal))
                })
            }
            Filter::Nin { field, values } => {
                if values.is_empty() {
                    return Some(get(field).is_some());
                }
                get(field).map(|x| {
                    !values
                        .iter()
                        .any(|v| compare(x, v) == Some(Ordering::Equal))
                })
            }
        }
    }

    fn compare(a: &MetaValue, b: &MetaValue) -> Option<Ordering> {
        use MetaValue::*;
        match (a, b) {
            (Text(x), Text(y)) => Some(x.cmp(y)),
            (Boolean(x), Boolean(y)) => Some(x.cmp(y)),
            (Timestamp(x), Timestamp(y)) => Some(x.cmp(y)),
            (Integer(x), Integer(y)) => Some(x.cmp(y)),
            (Integer(_) | Float(_), Integer(_) | Float(_)) => {
                let f = |v: &MetaValue| match v {
                    Integer(i) => *i as f64,
                    Float(f) => *f,
                    _ => unreachable!(),
                };
                f(a).partial_cmp(&f(b))
            }
            _ => None,
        }
    }

    fn random_value(rng: &mut Rng, kind: FieldKind) -> MetaValue {
        match kind {
            FieldKind::Text => MetaValue::Text(["A", "B", "C", "a"][rng.below(4) as usize].into()),
            FieldKind::Integer => MetaValue::Integer(rng.below(6) as i64),
            FieldKind::Float => MetaValue::Float(rng.below(6) as f64 * 0.5),
            FieldKind::Boolean => MetaValue::Boolean(rng.chance(50)),
            FieldKind::Timestamp => MetaValue::Timestamp(
                chrono::DateTime::from_timestamp_micros(rng.below(4) as i64).unwrap(),
            ),
        }
    }

    fn random_filter(rng: &mut Rng, depth: usize) -> Filter {
        let sch = schema();
        if depth >= 3 || rng.chance(40) {
            let (field, kind) = sch[rng.below(sch.len() as u64) as usize].clone();
            // Mixed numeric comparisons (integer field vs float value, and back) are included on purpose.
            let value = if kind.is_numeric() && rng.chance(30) {
                if kind == FieldKind::Integer {
                    MetaValue::Float(rng.below(6) as f64 * 0.5)
                } else {
                    MetaValue::Integer(rng.below(6) as i64)
                }
            } else {
                random_value(rng, kind)
            };
            let values: Vec<MetaValue> =
                (0..rng.below(3)).map(|_| random_value(rng, kind)).collect();
            return match rng.below(8) {
                0 => Filter::Eq { field, value },
                1 => Filter::Ne { field, value },
                2 if kind != FieldKind::Boolean => Filter::Gt { field, value },
                3 if kind != FieldKind::Boolean => Filter::Gte { field, value },
                4 if kind != FieldKind::Boolean => Filter::Lt { field, value },
                5 if kind != FieldKind::Boolean => Filter::Lte { field, value },
                6 => Filter::In { field, values },
                _ => Filter::Nin { field, values },
            };
        }
        match rng.below(3) {
            0 => Filter::And(
                (0..rng.below(4))
                    .map(|_| random_filter(rng, depth + 1))
                    .collect(),
            ),
            1 => Filter::Or(
                (0..rng.below(4))
                    .map(|_| random_filter(rng, depth + 1))
                    .collect(),
            ),
            _ => Filter::Not(Box::new(random_filter(rng, depth + 1))),
        }
    }

    /// The indexed evaluator must agree with the brute-force SQL reference on random
    /// documents (some fields missing), random deletes, and random filters.
    #[test]
    fn indexed_evaluation_matches_reference_three_valued_logic() {
        let path = temp_log("oracle");
        let mut store = DocStore::create(&path, schema()).unwrap();
        let mut rng = Rng(0xD1B5_4A32_D192_ED03);

        let mut reference_docs: Vec<(u64, Vec<(String, MetaValue)>)> = Vec::new();
        for id in 0..400u64 {
            let mut meta = Vec::new();
            for (name, kind) in schema() {
                if rng.chance(70) {
                    meta.push((name, random_value(&mut rng, kind)));
                }
            }
            put(&mut store, &[(id, "d", meta.clone())]);
            reference_docs.push((id, meta));
        }
        // Delete a random subset, then check again.
        for id in 0..400u64 {
            if rng.chance(25) {
                del(&mut store, id);
                reference_docs.retain(|(d, _)| *d != id);
            }
        }

        let mut checked = 0;
        let mut nonempty = 0;
        for _ in 0..800 {
            let filter = random_filter(&mut rng, 0);
            if crate::filter::validate(&filter, &schema()).is_err() {
                continue;
            }
            let mut expected: Vec<u64> = reference_docs
                .iter()
                .filter(|(_, meta)| reference(&filter, meta) == Some(true))
                .map(|(id, _)| *id)
                .collect();
            expected.sort_unstable();
            let mut got = store.matching_ids(&filter).unwrap();
            got.sort_unstable();
            assert_eq!(got, expected, "filter disagrees with reference: {filter:?}");
            checked += 1;
            if !expected.is_empty() {
                nonempty += 1;
            }
        }
        assert!(checked > 300, "too few valid filters checked: {checked}");
        assert!(
            nonempty > 100,
            "too few filters matched anything: {nonempty}"
        );
        fs::remove_dir_all(path.parent().unwrap()).ok();
    }
}
