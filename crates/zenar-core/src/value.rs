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
            FieldKind::Float => "FLOAT",
            FieldKind::Boolean => "BOOLEAN",
            FieldKind::Timestamp => "TIMESTAMP",
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
    /// Converts to a Stoolap bound parameter. Values are always bound, never
    /// interpolated into SQL (spec section 11).
    pub fn to_sql(&self) -> stoolap::Value {
        match self {
            MetaValue::Text(s) => stoolap::Value::text(s.clone()),
            MetaValue::Integer(i) => stoolap::Value::integer(*i),
            MetaValue::Float(f) => stoolap::Value::float(*f),
            MetaValue::Boolean(b) => stoolap::Value::boolean(*b),
            MetaValue::Timestamp(t) => stoolap::Value::timestamp(*t),
        }
    }

    /// Reads a stored value back. `None` means SQL NULL.
    pub fn from_sql(kind: FieldKind, value: &stoolap::Value) -> Option<Self> {
        if value.is_null() {
            return None;
        }
        match kind {
            FieldKind::Text => value.as_string().map(MetaValue::Text),
            FieldKind::Integer => value.as_int64().map(MetaValue::Integer),
            FieldKind::Float => value.as_float64().map(MetaValue::Float),
            FieldKind::Boolean => value.as_boolean().map(MetaValue::Boolean),
            FieldKind::Timestamp => value.as_timestamp().map(MetaValue::Timestamp),
        }
    }
}
