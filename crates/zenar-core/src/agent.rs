//! Agent-facing metadata filters and discovery.
//!
//! An agent (an LLM or other tool caller) needs three things to filter well: what fields exist,
//! which values they hold, and a filter format it can produce reliably. This module provides:
//!
//! * `parse`: a JSON `where` object, such as `{"tenant": "A", "price": {"gte": 100}}`, turned into
//!   a `Filter`. Errors name the valid fields and suggest a close match for a typo.
//! * `where_schema` and `tool_spec`: a JSON Schema for the `where` object and a tool definition,
//!   built from the live collection, so the model sees the real fields and values.
//! * Warnings: when an equality value matches no document, the result says so and lists the values
//!   that do exist. A zero-hit result then has an explanation.
//!
//! The format is plain JSON on purpose. Models produce it reliably, and it needs no escaping.

use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::{Map, Value, json};

use crate::docstore::FieldStats;
use crate::error::ZenarError;
use crate::filter::Filter;
use crate::value::{FieldKind, MetaValue};

/// Fields with at most this many distinct values list them all in the schema and in warnings.
pub const VALUE_LIST_LIMIT: usize = 200;

/// Field names that cannot be used because they are operators in the `where` grammar.
pub const RESERVED_FIELD_NAMES: [&str; 3] = ["and", "or", "not"];

const OPERATORS: [&str; 8] = ["eq", "ne", "gt", "gte", "lt", "lte", "in", "nin"];

fn invalid(message: impl Into<String>) -> ZenarError {
    ZenarError::InvalidFilter(message.into())
}

/// Result of parsing a `where` object.
#[derive(Debug, Clone, PartialEq)]
pub struct Parsed {
    /// `None` for an empty `where` object, which matches everything.
    pub filter: Option<Filter>,
    /// Non-fatal notes for the caller, such as an equality value no document has.
    pub warnings: Vec<String>,
}

/// Parses a `where` object against the schema and the current field statistics.
pub fn parse(
    where_: &Value,
    schema: &[(String, FieldKind)],
    stats: &[FieldStats],
) -> Result<Parsed, ZenarError> {
    let object = where_
        .as_object()
        .ok_or_else(|| invalid("where must be a JSON object, for example {\"tenant\": \"A\"}"))?;
    if object.is_empty() {
        return Ok(Parsed {
            filter: None,
            warnings: Vec::new(),
        });
    }
    let mut warnings = Vec::new();
    let filter = parse_object(object, schema, stats, &mut warnings)?;
    Ok(Parsed {
        filter: Some(filter),
        warnings,
    })
}

fn parse_object(
    object: &Map<String, Value>,
    schema: &[(String, FieldKind)],
    stats: &[FieldStats],
    warnings: &mut Vec<String>,
) -> Result<Filter, ZenarError> {
    let mut parts = Vec::with_capacity(object.len());
    for (key, value) in object {
        let part = match key.as_str() {
            "and" => Filter::And(parse_list(value, schema, stats, warnings, "and")?),
            "or" => Filter::Or(parse_list(value, schema, stats, warnings, "or")?),
            "not" => {
                let inner = value
                    .as_object()
                    .ok_or_else(|| invalid("\"not\" takes one where object, for example {\"not\": {\"tenant\": \"B\"}}"))?;
                Filter::Not(Box::new(parse_object(inner, schema, stats, warnings)?))
            }
            field => parse_field(field, value, schema, stats, warnings)?,
        };
        parts.push(part);
    }
    Ok(if parts.len() == 1 {
        parts.remove(0)
    } else {
        Filter::And(parts)
    })
}

fn parse_list(
    value: &Value,
    schema: &[(String, FieldKind)],
    stats: &[FieldStats],
    warnings: &mut Vec<String>,
    operator: &str,
) -> Result<Vec<Filter>, ZenarError> {
    let items = value
        .as_array()
        .ok_or_else(|| invalid(format!("\"{operator}\" takes a list of where objects")))?;
    items
        .iter()
        .map(|item| {
            let object = item.as_object().ok_or_else(|| {
                invalid(format!(
                    "each entry of \"{operator}\" must be a where object"
                ))
            })?;
            parse_object(object, schema, stats, warnings)
        })
        .collect()
}

fn parse_field(
    field: &str,
    value: &Value,
    schema: &[(String, FieldKind)],
    stats: &[FieldStats],
    warnings: &mut Vec<String>,
) -> Result<Filter, ZenarError> {
    let kind = kind_of(field, schema)?;
    match value {
        Value::Object(conditions) => {
            if conditions.is_empty() {
                return Err(invalid(format!(
                    "field {field:?} needs at least one operator, for example {{\"{field}\": {{\"eq\": ...}}}}"
                )));
            }
            let mut parts = Vec::with_capacity(conditions.len());
            for (operator, argument) in conditions {
                if !OPERATORS.contains(&operator.as_str()) {
                    return Err(invalid(format!(
                        "unknown operator {operator:?} for field {field:?}; use one of {}",
                        OPERATORS.join(", ")
                    )));
                }
                parts.push(condition(field, kind, operator, argument, stats, warnings)?);
            }
            Ok(if parts.len() == 1 {
                parts.remove(0)
            } else {
                Filter::And(parts)
            })
        }
        scalar => {
            let value = to_meta(field, kind, scalar)?;
            note_missing_value(field, &value, stats, warnings);
            Ok(Filter::Eq {
                field: field.to_string(),
                value,
            })
        }
    }
}

fn condition(
    field: &str,
    kind: FieldKind,
    operator: &str,
    argument: &Value,
    stats: &[FieldStats],
    warnings: &mut Vec<String>,
) -> Result<Filter, ZenarError> {
    let field = field.to_string();
    match operator {
        "in" | "nin" => {
            let items = argument
                .as_array()
                .ok_or_else(|| invalid(format!("{operator} for field {field:?} takes a list")))?;
            let values = items
                .iter()
                .map(|v| to_meta(&field, kind, v))
                .collect::<Result<Vec<_>, _>>()?;
            if operator == "in" {
                for value in &values {
                    note_missing_value(&field, value, stats, warnings);
                }
                Ok(Filter::In { field, values })
            } else {
                Ok(Filter::Nin { field, values })
            }
        }
        _ => {
            let value = to_meta(&field, kind, argument)?;
            if operator == "eq" {
                note_missing_value(&field, &value, stats, warnings);
            }
            Ok(match operator {
                "eq" => Filter::Eq { field, value },
                "ne" => Filter::Ne { field, value },
                "gt" => Filter::Gt { field, value },
                "gte" => Filter::Gte { field, value },
                "lt" => Filter::Lt { field, value },
                _ => Filter::Lte { field, value },
            })
        }
    }
}

/// Converts a JSON scalar to a value of the field's kind. Timestamps are RFC 3339 strings.
fn to_meta(field: &str, kind: FieldKind, value: &Value) -> Result<MetaValue, ZenarError> {
    let mismatch = |expected: &str| {
        invalid(format!(
            "field {field:?} is {}; expected {expected}, got {}",
            kind.as_str(),
            describe_json(value)
        ))
    };
    match kind {
        FieldKind::Text => value
            .as_str()
            .map(|s| MetaValue::Text(s.to_string()))
            .ok_or_else(|| mismatch("a string")),
        FieldKind::Integer => value
            .as_i64()
            .map(MetaValue::Integer)
            .ok_or_else(|| mismatch("an integer")),
        FieldKind::Float => value
            .as_f64()
            .map(MetaValue::Float)
            .ok_or_else(|| mismatch("a number")),
        FieldKind::Boolean => value
            .as_bool()
            .map(MetaValue::Boolean)
            .ok_or_else(|| mismatch("true or false")),
        FieldKind::Timestamp => value
            .as_str()
            .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
            .map(|t| MetaValue::Timestamp(t.with_timezone(&Utc)))
            .ok_or_else(|| {
                mismatch("an RFC 3339 date-time string such as \"2026-01-31T00:00:00Z\"")
            }),
    }
}

fn describe_json(value: &Value) -> String {
    match value {
        Value::Null => "null".into(),
        Value::Bool(_) => "a boolean".into(),
        Value::Number(_) => "a number".into(),
        Value::String(s) => format!("the string {s:?}"),
        Value::Array(_) => "a list".into(),
        Value::Object(_) => "an object".into(),
    }
}

fn kind_of(field: &str, schema: &[(String, FieldKind)]) -> Result<FieldKind, ZenarError> {
    if let Some((_, kind)) = schema.iter().find(|(name, _)| name == field) {
        return Ok(*kind);
    }
    let names: Vec<&str> = schema.iter().map(|(n, _)| n.as_str()).collect();
    let suggestion = closest(field, &names);
    let mut message = format!("unknown field {field:?}");
    if let Some(name) = suggestion {
        message.push_str(&format!(", did you mean {name:?}?"));
    }
    if names.is_empty() {
        message.push_str("; this collection has no metadata fields yet");
    } else {
        message.push_str(&format!("; filterable fields: {}", names.join(", ")));
    }
    Err(invalid(message))
}

/// The field name closest to `typo`, if it is close enough to be a plausible typo.
fn closest<'a>(typo: &str, names: &[&'a str]) -> Option<&'a str> {
    names
        .iter()
        .map(|name| (edit_distance(typo, name), *name))
        .filter(|(distance, name)| *distance <= (name.len().max(typo.len()) / 3).max(1))
        .min_by_key(|(distance, _)| *distance)
        .map(|(_, name)| name)
}

/// Levenshtein distance between two strings, over characters.
fn edit_distance(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    let mut dp = vec![vec![0usize; b.len() + 1]; a.len() + 1];
    for (i, row) in dp.iter_mut().enumerate() {
        row[0] = i;
    }
    for (j, cell) in dp[0].iter_mut().enumerate() {
        *cell = j;
    }
    for i in 1..=a.len() {
        for j in 1..=b.len() {
            let cost = usize::from(a[i - 1] != b[j - 1]);
            dp[i][j] = (dp[i - 1][j] + 1)
                .min(dp[i][j - 1] + 1)
                .min(dp[i - 1][j - 1] + cost);
        }
    }
    dp[a.len()][b.len()]
}

/// Records a warning when an equality value matches no live document. Only fields with a
/// short value list are checked, since a long list cannot be scanned meaningfully here.
fn note_missing_value(
    field: &str,
    value: &MetaValue,
    stats: &[FieldStats],
    warnings: &mut Vec<String>,
) {
    let Some(stat) = stats.iter().find(|s| s.name == field) else {
        return;
    };
    if stat.values.len() > VALUE_LIST_LIMIT || stat.values.iter().any(|(v, _)| v == value) {
        return;
    }
    let known: Vec<String> = stat
        .values
        .iter()
        .take(20)
        .map(|(v, _)| value_text(v))
        .collect();
    let known = if known.is_empty() {
        "the field has no values yet".to_string()
    } else {
        format!("values that exist: {}", known.join(", "))
    };
    warnings.push(format!(
        "no live document has {field} = {}; {known}",
        value_text(value)
    ));
}

/// Display form of a value, as a model would write it.
pub fn value_text(value: &MetaValue) -> String {
    match value {
        MetaValue::Text(s) => format!("{s:?}"),
        MetaValue::Integer(i) => i.to_string(),
        MetaValue::Float(f) => f.to_string(),
        MetaValue::Boolean(b) => b.to_string(),
        MetaValue::Timestamp(t) => t.to_rfc3339_opts(SecondsFormat::Micros, true),
    }
}

/// JSON form of a value.
pub fn value_json(value: &MetaValue) -> Value {
    match value {
        MetaValue::Text(s) => json!(s),
        MetaValue::Integer(i) => json!(i),
        MetaValue::Float(f) => json!(f),
        MetaValue::Boolean(b) => json!(b),
        MetaValue::Timestamp(t) => json!(t.to_rfc3339_opts(SecondsFormat::Micros, true)),
    }
}

/// Describes one field for discovery: its type, how many documents have it, and either every
/// value (with counts) or, for a long list, the range and the most frequent values.
pub fn field_json(stat: &FieldStats, documents: usize) -> Value {
    let mut out = Map::new();
    out.insert("name".into(), json!(stat.name));
    out.insert("type".into(), json!(stat.kind.as_str()));
    out.insert("present".into(), json!(stat.present));
    out.insert("documents".into(), json!(documents));
    out.insert("distinct_values".into(), json!(stat.values.len()));
    if let Some(min) = &stat.min {
        out.insert("min".into(), value_json(min));
    }
    if let Some(max) = &stat.max {
        out.insert("max".into(), value_json(max));
    }
    if stat.values.len() <= VALUE_LIST_LIMIT {
        let values: Vec<Value> = stat
            .values
            .iter()
            .map(|(v, n)| json!({"value": value_json(v), "count": n}))
            .collect();
        out.insert("values".into(), Value::Array(values));
    } else {
        let top: Vec<Value> = stat
            .values
            .iter()
            .take(20)
            .map(|(v, n)| json!({"value": value_json(v), "count": n}))
            .collect();
        out.insert("top_values".into(), Value::Array(top));
        out.insert(
            "note".into(),
            json!("too many distinct values to list; use find_values to search them"),
        );
    }
    Value::Object(out)
}

/// JSON Schema for the `where` object, built from the live fields. Fields with a short value
/// list get an `enum`, so the model is steered to values that exist.
pub fn where_schema(stats: &[FieldStats]) -> Value {
    let mut properties = Map::new();
    for stat in stats {
        let scalar = scalar_schema(stat);
        let operator_properties: Map<String, Value> = OPERATORS
            .iter()
            .map(|op| {
                let schema = if matches!(*op, "in" | "nin") {
                    json!({"type": "array", "items": scalar.clone()})
                } else {
                    scalar.clone()
                };
                (op.to_string(), schema)
            })
            .collect();
        properties.insert(
            stat.name.clone(),
            json!({
                "description": format!("{} field", stat.kind.as_str()),
                "anyOf": [
                    scalar,
                    {
                        "type": "object",
                        "properties": operator_properties,
                        "additionalProperties": false,
                        "minProperties": 1
                    }
                ]
            }),
        );
    }
    properties.insert(
        "and".into(),
        json!({"type": "array", "items": {"type": "object"}, "description": "all of these must match"}),
    );
    properties.insert(
        "or".into(),
        json!({"type": "array", "items": {"type": "object"}, "description": "at least one of these must match"}),
    );
    properties.insert(
        "not".into(),
        json!({"type": "object", "description": "the document must not match this"}),
    );
    json!({
        "type": "object",
        "description": "Metadata filter. Keys are field names; a plain value means equality, an object gives operators.",
        "properties": properties,
        "additionalProperties": false
    })
}

fn scalar_schema(stat: &FieldStats) -> Value {
    let mut schema = match stat.kind {
        FieldKind::Text => json!({"type": "string"}),
        FieldKind::Integer => json!({"type": "integer"}),
        FieldKind::Float => json!({"type": "number"}),
        FieldKind::Boolean => json!({"type": "boolean"}),
        FieldKind::Timestamp => json!({"type": "string", "format": "date-time"}),
    };
    if stat.values.len() <= VALUE_LIST_LIMIT && !stat.values.is_empty() {
        let values: Vec<Value> = stat.values.iter().map(|(v, _)| value_json(v)).collect();
        schema["enum"] = Value::Array(values);
    }
    schema
}

/// Tool definition for a collection, in the common `name` / `description` / `parameters` shape.
/// The caller embeds `query` into a vector before calling `retrieve`.
pub fn tool_spec(name: &str, collection: &str, documents: usize, stats: &[FieldStats]) -> Value {
    let mut lines = vec![format!(
        "Semantic search over the {collection:?} collection ({documents} documents). \
         Pass the user's request as `query`. Use `where` to restrict results to matching documents."
    )];
    if stats.is_empty() {
        lines.push("This collection has no metadata fields, so `where` cannot be used yet.".into());
    } else {
        lines.push("Filterable fields:".into());
        for stat in stats {
            lines.push(format!(
                "- {} ({}): {}",
                stat.name,
                stat.kind.as_str(),
                summary(stat)
            ));
        }
    }
    let description = lines.join("\n");
    let parameters = json!({
        "type": "object",
        "properties": {
            "query": {"type": "string", "description": "What to search for, in natural language."},
            "top_k": {"type": "integer", "minimum": 1, "maximum": 100, "default": 10,
                      "description": "How many results to return."},
            "where": where_schema(stats)
        },
        "required": ["query"],
        "additionalProperties": false
    });
    json!({
        "name": name,
        "description": description,
        "parameters": parameters,
        "input_schema": parameters
    })
}

/// Plain text listing only the fields and their values, one per line, ready to paste into a prompt.
pub fn fields_prompt(stats: &[FieldStats]) -> String {
    if stats.is_empty() {
        return "No metadata fields.".to_string();
    }
    stats
        .iter()
        .map(|stat| {
            format!(
                "- {} ({}): {}",
                stat.name,
                stat.kind.as_str(),
                summary(stat)
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn summary(stat: &FieldStats) -> String {
    if stat.values.len() <= VALUE_LIST_LIMIT {
        if stat.values.is_empty() {
            return "no values yet".into();
        }
        let values: Vec<String> = stat
            .values
            .iter()
            .take(20)
            .map(|(v, _)| value_text(v))
            .collect();
        let more = if stat.values.len() > 20 { ", ..." } else { "" };
        format!("values {}{}", values.join(", "), more)
    } else {
        let range = match (&stat.min, &stat.max) {
            (Some(min), Some(max)) => format!("range {} to {}", value_text(min), value_text(max)),
            _ => "many distinct values".into(),
        };
        format!("{range}; use find_values to search")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schema() -> Vec<(String, FieldKind)> {
        vec![
            ("tenant".into(), FieldKind::Text),
            ("price".into(), FieldKind::Float),
            ("active".into(), FieldKind::Boolean),
        ]
    }

    fn stats() -> Vec<FieldStats> {
        vec![
            FieldStats {
                name: "tenant".into(),
                kind: FieldKind::Text,
                present: 3,
                values: vec![
                    (MetaValue::Text("A".into()), 2),
                    (MetaValue::Text("B".into()), 1),
                ],
                min: None,
                max: None,
            },
            FieldStats {
                name: "price".into(),
                kind: FieldKind::Float,
                present: 3,
                values: vec![],
                min: Some(MetaValue::Float(1.0)),
                max: Some(MetaValue::Float(9.0)),
            },
            FieldStats {
                name: "active".into(),
                kind: FieldKind::Boolean,
                present: 3,
                values: vec![(MetaValue::Boolean(true), 3)],
                min: None,
                max: None,
            },
        ]
    }

    #[test]
    fn empty_where_matches_everything() {
        let p = parse(&json!({}), &schema(), &stats()).unwrap();
        assert!(p.filter.is_none());
    }

    #[test]
    fn plain_values_are_equality_and_keys_are_anded() {
        let p = parse(&json!({"tenant": "A", "active": true}), &schema(), &stats()).unwrap();
        match p.filter.unwrap() {
            Filter::And(parts) => assert_eq!(parts.len(), 2),
            other => panic!("expected And, got {other:?}"),
        }
        assert!(p.warnings.is_empty());
    }

    #[test]
    fn operators_compose_with_and_or_not() {
        let p = parse(
            &json!({"price": {"gte": 2, "lt": 8}, "or": [{"tenant": "A"}, {"tenant": "B"}], "not": {"active": false}}),
            &schema(),
            &stats(),
        )
        .unwrap();
        assert!(p.filter.is_some());
    }

    #[test]
    fn unknown_field_suggests_the_close_name() {
        let err = parse(&json!({"tenat": "A"}), &schema(), &stats()).unwrap_err();
        let message = err.to_string();
        assert!(message.contains("did you mean \"tenant\""), "{message}");
        assert!(
            message.contains("filterable fields: tenant, price, active"),
            "{message}"
        );
    }

    #[test]
    fn unknown_field_without_a_close_name_lists_the_fields() {
        let err = parse(&json!({"colour": "red"}), &schema(), &stats())
            .unwrap_err()
            .to_string();
        assert!(!err.contains("did you mean"), "{err}");
        assert!(err.contains("filterable fields"), "{err}");
    }

    #[test]
    fn wrong_value_type_names_the_field_and_the_expected_type() {
        let err = parse(&json!({"price": "cheap"}), &schema(), &stats())
            .unwrap_err()
            .to_string();
        assert!(err.contains("\"price\" is float"), "{err}");
        assert!(err.contains("expected a number"), "{err}");
    }

    #[test]
    fn unknown_operator_lists_the_valid_ones() {
        let err = parse(&json!({"price": {"between": [1, 2]}}), &schema(), &stats())
            .unwrap_err()
            .to_string();
        assert!(err.contains("unknown operator \"between\""), "{err}");
        assert!(err.contains("gte"), "{err}");
    }

    #[test]
    fn value_no_document_has_produces_a_warning_with_the_real_values() {
        let p = parse(&json!({"tenant": "Z"}), &schema(), &stats()).unwrap();
        assert_eq!(p.warnings.len(), 1);
        assert!(
            p.warnings[0].contains("no live document has tenant = \"Z\""),
            "{:?}",
            p.warnings
        );
        assert!(p.warnings[0].contains("\"A\""), "{:?}", p.warnings);
    }

    #[test]
    fn where_schema_lists_enum_values_for_short_fields() {
        let schema = where_schema(&stats());
        let tenant = &schema["properties"]["tenant"]["anyOf"][0];
        assert_eq!(tenant["enum"], json!(["A", "B"]));
        assert!(schema["properties"]["and"].is_object());
    }

    #[test]
    fn edit_distance_is_levenshtein() {
        assert_eq!(edit_distance("kitten", "sitting"), 3);
        assert_eq!(edit_distance("tenant", "tenat"), 1);
        assert_eq!(edit_distance("", "abc"), 3);
    }
}
