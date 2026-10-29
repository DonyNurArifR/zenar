use crate::error::ZenarError;
use crate::value::{FieldKind, MetaValue};

/// Maximum nesting depth of And/Or/Not (spec section 69).
pub const MAX_FILTER_DEPTH: usize = 16;

/// Typed filter AST (spec section 10). Python's `field` expressions and
/// kwargs both compile to this before reaching the core.
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

/// A filter translated to parameterized SQL. `sql` contains only `$n`
/// placeholders for values; field names are validated against the schema
/// before they are written into it.
#[derive(Debug)]
pub struct CompiledFilter {
    pub sql: String,
    pub params: Vec<stoolap::Value>,
}

/// Validates a filter against a collection's locked metadata schema and
/// compiles it to SQL. Anything that does not map to a fully specified
/// `Filter` variant is rejected rather than dropped (spec section 81).
pub fn compile(
    filter: &Filter,
    schema: &[(String, FieldKind)],
) -> Result<CompiledFilter, ZenarError> {
    check_depth(filter, 1)?;
    let mut compiler = Compiler {
        schema,
        params: Vec::new(),
    };
    let sql = compiler.node(filter)?;
    Ok(CompiledFilter {
        sql,
        params: compiler.params,
    })
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

struct Compiler<'a> {
    schema: &'a [(String, FieldKind)],
    params: Vec<stoolap::Value>,
}

impl Compiler<'_> {
    fn bind(&mut self, value: &MetaValue) -> String {
        self.params.push(value.to_sql());
        format!("${}", self.params.len())
    }

    fn field_kind(&self, field: &str) -> Result<FieldKind, ZenarError> {
        self.schema
            .iter()
            .find(|(name, _)| name == field)
            .map(|(_, kind)| *kind)
            .ok_or_else(|| ZenarError::InvalidFilter(format!("unknown metadata field {field:?}")))
    }

    /// Checks that a filter value can be compared against a column.
    /// Integer and Float are mutually comparable; everything else must match.
    fn check_comparable(field: &str, kind: FieldKind, value: &MetaValue) -> Result<(), ZenarError> {
        let value_kind = FieldKind::of(value);
        let ok = value_kind == kind || (kind.is_numeric() && value_kind.is_numeric());
        if ok {
            Ok(())
        } else {
            Err(ZenarError::InvalidFilter(format!(
                "field {field:?} is {}, filter value is {}",
                kind.as_str(),
                value_kind.as_str()
            )))
        }
    }

    fn check_ordering(field: &str, kind: FieldKind) -> Result<(), ZenarError> {
        match kind {
            FieldKind::Boolean => Err(ZenarError::InvalidFilter(format!(
                "ordering comparison is not supported on boolean field {field:?}"
            ))),
            _ => Ok(()),
        }
    }

    fn comparison(
        &mut self,
        field: &str,
        op: &str,
        value: &MetaValue,
        ordered: bool,
    ) -> Result<String, ZenarError> {
        let kind = self.field_kind(field)?;
        Self::check_comparable(field, kind, value)?;
        if ordered {
            Self::check_ordering(field, kind)?;
        }
        let placeholder = self.bind(value);
        Ok(format!("\"{field}\" {op} {placeholder}"))
    }

    fn membership(
        &mut self,
        field: &str,
        values: &[MetaValue],
        negated: bool,
    ) -> Result<String, ZenarError> {
        let kind = self.field_kind(field)?;
        values
            .iter()
            .try_for_each(|v| Self::check_comparable(field, kind, v))?;
        if values.is_empty() {
            // Empty IN matches nothing. Empty NOT IN excludes only NULL rows,
            // matching the non-empty case (NULL rows never match, spec section 68).
            return Ok(if negated {
                format!("\"{field}\" IS NOT NULL")
            } else {
                "1 = 0".to_string()
            });
        }
        let placeholders: Vec<String> = values.iter().map(|v| self.bind(v)).collect();
        let op = if negated { "NOT IN" } else { "IN" };
        Ok(format!("\"{field}\" {op} ({})", placeholders.join(", ")))
    }

    fn node(&mut self, filter: &Filter) -> Result<String, ZenarError> {
        match filter {
            Filter::Eq { field, value } => self.comparison(field, "=", value, false),
            Filter::Ne { field, value } => self.comparison(field, "<>", value, false),
            Filter::Gt { field, value } => self.comparison(field, ">", value, true),
            Filter::Gte { field, value } => self.comparison(field, ">=", value, true),
            Filter::Lt { field, value } => self.comparison(field, "<", value, true),
            Filter::Lte { field, value } => self.comparison(field, "<=", value, true),
            Filter::In { field, values } => self.membership(field, values, false),
            Filter::Nin { field, values } => self.membership(field, values, true),
            Filter::And(children) => self.junction(children, "AND", "1 = 1"),
            Filter::Or(children) => self.junction(children, "OR", "1 = 0"),
            Filter::Not(child) => Ok(format!("NOT ({})", self.node(child)?)),
        }
    }

    fn junction(
        &mut self,
        children: &[Filter],
        op: &str,
        empty: &str,
    ) -> Result<String, ZenarError> {
        if children.is_empty() {
            return Ok(empty.to_string());
        }
        let parts: Vec<String> = children
            .iter()
            .map(|c| self.node(c).map(|s| format!("({s})")))
            .collect::<Result<_, _>>()?;
        Ok(parts.join(&format!(" {op} ")))
    }
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

    #[test]
    fn eq_compiles_to_parameter() {
        let f = Filter::Eq {
            field: "tenant".into(),
            value: MetaValue::Text("A".into()),
        };
        let c = compile(&f, &schema()).unwrap();
        assert_eq!(c.sql, "\"tenant\" = $1");
        assert_eq!(c.params.len(), 1);
    }

    #[test]
    fn unknown_field_rejected() {
        let f = Filter::Eq {
            field: "nope".into(),
            value: MetaValue::Integer(1),
        };
        assert!(matches!(
            compile(&f, &schema()),
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
            compile(&f, &schema()),
            Err(ZenarError::InvalidFilter(_))
        ));
    }

    #[test]
    fn integer_value_on_float_field_is_allowed() {
        let f = Filter::Gte {
            field: "price".into(),
            value: MetaValue::Integer(100),
        };
        assert!(compile(&f, &schema()).is_ok());
    }

    #[test]
    fn ordering_on_boolean_rejected() {
        let f = Filter::Gt {
            field: "active".into(),
            value: MetaValue::Boolean(true),
        };
        assert!(compile(&f, &schema()).is_err());
    }

    #[test]
    fn empty_in_matches_nothing() {
        let f = Filter::In {
            field: "tenant".into(),
            values: vec![],
        };
        assert_eq!(compile(&f, &schema()).unwrap().sql, "1 = 0");
    }

    #[test]
    fn empty_and_or_are_identity_elements() {
        assert_eq!(
            compile(&Filter::And(vec![]), &schema()).unwrap().sql,
            "1 = 1"
        );
        assert_eq!(
            compile(&Filter::Or(vec![]), &schema()).unwrap().sql,
            "1 = 0"
        );
    }

    #[test]
    fn nested_and_or_not_compile() {
        let f = Filter::And(vec![
            Filter::Eq {
                field: "tenant".into(),
                value: MetaValue::Text("A".into()),
            },
            Filter::Not(Box::new(Filter::Or(vec![Filter::Eq {
                field: "tenant".into(),
                value: MetaValue::Text("B".into()),
            }]))),
        ]);
        let c = compile(&f, &schema()).unwrap();
        assert_eq!(c.sql, "(\"tenant\" = $1) AND (NOT ((\"tenant\" = $2)))");
        assert_eq!(c.params.len(), 2);
    }

    #[test]
    fn depth_cap_rejected() {
        let mut f = Filter::Eq {
            field: "tenant".into(),
            value: MetaValue::Text("A".into()),
        };
        for _ in 0..MAX_FILTER_DEPTH {
            f = Filter::Not(Box::new(f));
        }
        assert!(matches!(
            compile(&f, &schema()),
            Err(ZenarError::InvalidFilter(_))
        ));
    }

    #[test]
    fn injection_text_is_only_a_parameter() {
        let f = Filter::Eq {
            field: "tenant".into(),
            value: MetaValue::Text("x' OR 1=1 --".into()),
        };
        let c = compile(&f, &schema()).unwrap();
        assert!(!c.sql.contains("OR 1=1"));
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

    /// Builds a random filter over the schema. Some field names are unknown, so
    /// the compiler's rejection path is exercised too.
    fn random_filter(rng: &mut Rng, depth: usize, schema: &[(String, FieldKind)]) -> Filter {
        let leaf = depth >= 4 || rng.below(3) == 0;
        if leaf {
            let (name, kind) = if rng.below(10) == 0 {
                ("unknown_field".to_string(), FieldKind::Text)
            } else {
                schema[rng.below(schema.len() as u64) as usize].clone()
            };
            let field = name;
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

    /// Fuzz (spec Phase 7): random trees never panic, never inline a value into the
    /// SQL text, and always bind exactly one parameter per placeholder.
    #[test]
    fn random_filters_never_panic_and_stay_parameterized() {
        let schema = vec![
            ("tenant".to_string(), FieldKind::Text),
            ("price".to_string(), FieldKind::Float),
            ("qty".to_string(), FieldKind::Integer),
            ("active".to_string(), FieldKind::Boolean),
        ];
        let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
        let mut accepted = 0;
        for _ in 0..5_000 {
            let f = random_filter(&mut rng, 0, &schema);
            match compile(&f, &schema) {
                Ok(c) => {
                    accepted += 1;
                    assert!(!c.sql.contains('\''), "value leaked into SQL: {}", c.sql);
                    assert!(
                        !c.sql.contains("DROP"),
                        "payload leaked into SQL: {}",
                        c.sql
                    );
                    let placeholders = c.sql.matches('$').count();
                    assert_eq!(
                        placeholders,
                        c.params.len(),
                        "placeholder/param mismatch in {}",
                        c.sql
                    );
                }
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
