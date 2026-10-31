use crate::error::ZenarError;
use crate::value::{FieldKind, MetaValue};

/// Maximum nesting depth of And/Or/Not (spec section 69).
pub const MAX_FILTER_DEPTH: usize = 16;

/// Typed filter AST (spec section 10). Python's `field` expressions and kwargs both
/// compile to this. It is evaluated by `docstore`, which never builds SQL from it.
#[derive(Debug, Clone, PartialEq)]
pub enum Filter {
    Eq {
        field: String,
        value: MetaValue,
    },
    Ne {
        field: String,
        value: MetaValue,
    },
    Gt {
        field: String,
        value: MetaValue,
    },
    Gte {
        field: String,
        value: MetaValue,
    },
    Lt {
        field: String,
        value: MetaValue,
    },
    Lte {
        field: String,
        value: MetaValue,
    },
    In {
        field: String,
        values: Vec<MetaValue>,
    },
    Nin {
        field: String,
        values: Vec<MetaValue>,
    },
    And(Vec<Filter>),
    Or(Vec<Filter>),
    Not(Box<Filter>),
}

/// Checks a filter against a collection's metadata schema. Unknown fields, values of the
/// wrong type, ordering on booleans, and nesting beyond `MAX_FILTER_DEPTH` are rejected.
/// Integer and float values may be compared with each other.
pub fn validate(filter: &Filter, schema: &[(String, FieldKind)]) -> Result<(), ZenarError> {
    check_depth(filter, 1)?;
    check(filter, schema)
}

fn check_depth(filter: &Filter, depth: usize) -> Result<(), ZenarError> {
    if depth > MAX_FILTER_DEPTH {
        return Err(ZenarError::InvalidFilter(format!(
            "nesting depth exceeds {MAX_FILTER_DEPTH}"
        )));
    }
    match filter {
        Filter::And(children) | Filter::Or(children) => {
            children.iter().try_for_each(|c| check_depth(c, depth + 1))
        }
        Filter::Not(child) => check_depth(child, depth + 1),
        _ => Ok(()),
    }
}

fn check(filter: &Filter, schema: &[(String, FieldKind)]) -> Result<(), ZenarError> {
    match filter {
        Filter::Eq { field, value } | Filter::Ne { field, value } => {
            check_value(field, value, schema, false)
        }
        Filter::Gt { field, value }
        | Filter::Gte { field, value }
        | Filter::Lt { field, value }
        | Filter::Lte { field, value } => check_value(field, value, schema, true),
        Filter::In { field, values } | Filter::Nin { field, values } => {
            values
                .iter()
                .try_for_each(|v| check_value(field, v, schema, false))?;
            kind_of(field, schema).map(|_| ())
        }
        Filter::And(children) | Filter::Or(children) => {
            children.iter().try_for_each(|c| check(c, schema))
        }
        Filter::Not(child) => check(child, schema),
    }
}

fn kind_of(field: &str, schema: &[(String, FieldKind)]) -> Result<FieldKind, ZenarError> {
    schema
        .iter()
        .find(|(name, _)| name == field)
        .map(|(_, kind)| *kind)
        .ok_or_else(|| ZenarError::InvalidFilter(format!("unknown metadata field {field:?}")))
}

fn check_value(
    field: &str,
    value: &MetaValue,
    schema: &[(String, FieldKind)],
    ordered: bool,
) -> Result<(), ZenarError> {
    // NaN compares equal to nothing and orders before everything in the index, so `< NaN` would
    // match every document. A NaN filter value is refused, as NaN metadata already is.
    if matches!(value, MetaValue::Float(f) if f.is_nan()) {
        return Err(ZenarError::InvalidFilter(format!(
            "filter on field {field:?} compares with NaN, which has no order"
        )));
    }
    let kind = kind_of(field, schema)?;
    let value_kind = FieldKind::of(value);
    let comparable = value_kind == kind || (kind.is_numeric() && value_kind.is_numeric());
    if !comparable {
        return Err(ZenarError::InvalidFilter(format!(
            "field {field:?} is {}, filter value is {}",
            kind.as_str(),
            value_kind.as_str()
        )));
    }
    if ordered && kind == FieldKind::Boolean {
        return Err(ZenarError::InvalidFilter(format!(
            "ordering comparison is not supported on boolean field {field:?}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schema() -> Vec<(String, FieldKind)> {
        vec![
            ("tenant".to_string(), FieldKind::Text),
            ("price".to_string(), FieldKind::Float),
            ("active".to_string(), FieldKind::Boolean),
        ]
    }

    fn text(s: &str) -> MetaValue {
        MetaValue::Text(s.into())
    }

    #[test]
    fn valid_filters_pass() {
        let f = Filter::And(vec![
            Filter::Eq {
                field: "tenant".into(),
                value: text("A"),
            },
            Filter::Gte {
                field: "price".into(),
                value: MetaValue::Integer(100),
            },
            Filter::Not(Box::new(Filter::Eq {
                field: "active".into(),
                value: MetaValue::Boolean(true),
            })),
        ]);
        assert!(validate(&f, &schema()).is_ok());
    }

    #[test]
    fn unknown_field_rejected() {
        let f = Filter::Eq {
            field: "nope".into(),
            value: text("x"),
        };
        assert!(matches!(
            validate(&f, &schema()),
            Err(ZenarError::InvalidFilter(_))
        ));
    }

    #[test]
    fn wrong_type_rejected() {
        let f = Filter::Eq {
            field: "tenant".into(),
            value: MetaValue::Integer(1),
        };
        assert!(matches!(
            validate(&f, &schema()),
            Err(ZenarError::InvalidFilter(_))
        ));
    }

    #[test]
    fn ordering_on_boolean_rejected() {
        let f = Filter::Gt {
            field: "active".into(),
            value: MetaValue::Boolean(true),
        };
        assert!(matches!(
            validate(&f, &schema()),
            Err(ZenarError::InvalidFilter(_))
        ));
    }

    #[test]
    fn empty_in_is_valid_for_a_known_field() {
        assert!(
            validate(
                &Filter::In {
                    field: "tenant".into(),
                    values: vec![]
                },
                &schema()
            )
            .is_ok()
        );
        assert!(
            validate(
                &Filter::In {
                    field: "nope".into(),
                    values: vec![]
                },
                &schema()
            )
            .is_err()
        );
    }

    #[test]
    fn depth_cap_rejected() {
        let mut f = Filter::Eq {
            field: "tenant".into(),
            value: text("A"),
        };
        for _ in 0..MAX_FILTER_DEPTH {
            f = Filter::Not(Box::new(f));
        }
        assert!(matches!(
            validate(&f, &schema()),
            Err(ZenarError::InvalidFilter(_))
        ));
    }

    /// Deterministic xorshift, so failures reproduce.
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
    }

    fn random_value(rng: &mut Rng, kind: FieldKind) -> MetaValue {
        match kind {
            FieldKind::Text => MetaValue::Text(format!("v'{}\"; DROP", rng.below(1000))),
            FieldKind::Integer => MetaValue::Integer(rng.below(1000) as i64 - 500),
            FieldKind::Float => MetaValue::Float(rng.below(1000) as f64 / 7.0),
            FieldKind::Boolean => MetaValue::Boolean(rng.below(2) == 0),
            FieldKind::Timestamp => MetaValue::Timestamp(chrono::DateTime::UNIX_EPOCH),
        }
    }

    fn random_filter(rng: &mut Rng, depth: usize, schema: &[(String, FieldKind)]) -> Filter {
        if depth >= 4 || rng.below(3) == 0 {
            let (field, kind) = if rng.below(10) == 0 {
                ("unknown_field".to_string(), FieldKind::Text)
            } else {
                schema[rng.below(schema.len() as u64) as usize].clone()
            };
            return match rng.below(8) {
                0 => Filter::Eq {
                    field,
                    value: random_value(rng, kind),
                },
                1 => Filter::Ne {
                    field,
                    value: random_value(rng, kind),
                },
                2 => Filter::Gt {
                    field,
                    value: random_value(rng, kind),
                },
                3 => Filter::Gte {
                    field,
                    value: random_value(rng, kind),
                },
                4 => Filter::Lt {
                    field,
                    value: random_value(rng, kind),
                },
                5 => Filter::Lte {
                    field,
                    value: random_value(rng, kind),
                },
                6 => Filter::In {
                    field,
                    values: (0..rng.below(4)).map(|_| random_value(rng, kind)).collect(),
                },
                _ => Filter::Nin {
                    field,
                    values: (0..rng.below(4)).map(|_| random_value(rng, kind)).collect(),
                },
            };
        }
        match rng.below(3) {
            0 => Filter::And(
                (0..rng.below(4))
                    .map(|_| random_filter(rng, depth + 1, schema))
                    .collect(),
            ),
            1 => Filter::Or(
                (0..rng.below(4))
                    .map(|_| random_filter(rng, depth + 1, schema))
                    .collect(),
            ),
            _ => Filter::Not(Box::new(random_filter(rng, depth + 1, schema))),
        }
    }

    /// Random trees validate or fail with InvalidFilter, and never panic.
    #[test]
    fn random_filters_validate_without_panicking() {
        let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
        let mut accepted = 0;
        for _ in 0..5_000 {
            let f = random_filter(&mut rng, 0, &schema());
            match validate(&f, &schema()) {
                Ok(()) => accepted += 1,
                Err(ZenarError::InvalidFilter(_)) => {}
                Err(other) => panic!("unexpected error kind: {other:?}"),
            }
        }
        assert!(
            accepted > 500,
            "fuzzer should produce many valid filters, got {accepted}"
        );
    }
}
