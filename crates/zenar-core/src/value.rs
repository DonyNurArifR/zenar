use chrono::{DateTime, Utc};

/// A typed, scalar metadata value (spec section 12).
#[derive(Debug, Clone, PartialEq)]
pub enum MetaValue {
    Text(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Timestamp(DateTime<Utc>),
}

/// The storage type a metadata field was locked to on its first insert.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldKind {
    Text,
    Integer,
    Float,
    Boolean,
    Timestamp,
}

impl FieldKind {
    pub fn of(value: &MetaValue) -> Self {
        match value {
            MetaValue::Text(_) => FieldKind::Text,
            MetaValue::Integer(_) => FieldKind::Integer,
            MetaValue::Float(_) => FieldKind::Float,
            MetaValue::Boolean(_) => FieldKind::Boolean,
            MetaValue::Timestamp(_) => FieldKind::Timestamp,
        }
    }

    pub fn sql_type(self) -> &'static str {
        match self {
            FieldKind::Text => "TEXT",
            FieldKind::Integer => "INTEGER",
            FieldKind::Float => "REAL",
            FieldKind::Boolean => "INTEGER",
            FieldKind::Timestamp => "INTEGER",
        }
    }

    /// Stable name used when the schema is persisted in the `collections` table.
    pub fn as_str(self) -> &'static str {
        match self {
            FieldKind::Text => "text",
            FieldKind::Integer => "integer",
            FieldKind::Float => "float",
            FieldKind::Boolean => "boolean",
            FieldKind::Timestamp => "timestamp",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "text" => Some(FieldKind::Text),
            "integer" => Some(FieldKind::Integer),
            "float" => Some(FieldKind::Float),
            "boolean" => Some(FieldKind::Boolean),
            "timestamp" => Some(FieldKind::Timestamp),
            _ => None,
        }
    }

    pub fn is_numeric(self) -> bool {
        matches!(self, FieldKind::Integer | FieldKind::Float)
    }
}

impl MetaValue {
    /// Converts to a bound SQLite parameter. Values are always bound, never
    /// interpolated into SQL (spec section 11). Booleans are stored as 0/1, and
    /// timestamps as UTC microseconds since the epoch, so both keep their ordering.
    pub fn to_sql(&self) -> rusqlite::types::Value {
        use rusqlite::types::Value as Sql;
        match self {
            MetaValue::Text(s) => Sql::Text(s.clone()),
            MetaValue::Integer(i) => Sql::Integer(*i),
            MetaValue::Float(f) => Sql::Real(*f),
            MetaValue::Boolean(b) => Sql::Integer(i64::from(*b)),
            MetaValue::Timestamp(t) => Sql::Integer(t.timestamp_micros()),
        }
    }

    /// Reads a stored value back. `None` means SQL NULL.
    pub fn from_sql(kind: FieldKind, value: rusqlite::types::ValueRef<'_>) -> Option<Self> {
        use rusqlite::types::ValueRef;
        match (kind, value) {
            (_, ValueRef::Null) => None,
            (FieldKind::Text, ValueRef::Text(b)) => {
                Some(MetaValue::Text(String::from_utf8_lossy(b).into_owned()))
            }
            (FieldKind::Integer, ValueRef::Integer(i)) => Some(MetaValue::Integer(i)),
            (FieldKind::Float, ValueRef::Real(f)) => Some(MetaValue::Float(f)),
            (FieldKind::Float, ValueRef::Integer(i)) => Some(MetaValue::Float(i as f64)),
            (FieldKind::Boolean, ValueRef::Integer(i)) => Some(MetaValue::Boolean(i != 0)),
            (FieldKind::Timestamp, ValueRef::Integer(us)) => {
                chrono::DateTime::from_timestamp_micros(us).map(MetaValue::Timestamp)
            }
            _ => None,
        }
    }
}
