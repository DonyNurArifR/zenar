# Filtering

A filter is compiled to a typed AST, validated against the collection's metadata schema, and
translated to parameterized SQL. Values are always bound as parameters, never interpolated.
Field names are checked against the schema before they appear in SQL.

## Operators

| Expression | Meaning |
|---|---|
| `f.x == v`, `f.x != v` | Equality / inequality |
| `f.x > v`, `>=`, `<`, `<=` | Ordering (not on booleans) |
| `f.x.isin([...])` | Membership. Empty list matches nothing. |
| `~f.x.isin([...])` | Not in. |
| `a & b`, `a | b`, `~a` | AND, OR, NOT. Parenthesize each comparison. |
| `tenant="A", category="faq"` | Keyword form: equality, ANDed. Cannot be combined with `filter=`. |

Nesting depth is capped at 16. Deeper filters raise `InvalidFilterError`.

## Type rules

- A filter value must match the field's type. Exception: integer and float are compared numerically.
- Using an unknown field raises `InvalidFilterError`. It is never treated as an empty match.
- Text comparison is case-sensitive and byte-exact.
- Timestamps are compared in UTC.

## NULL semantics

A document with no value for a field has SQL NULL in that column. The rules are the SQL-standard ones,
and they are fixed so that filters never match rows they should exclude:

- `f.x == v`, `f.x != v`, `f.x > v`, ... never match a NULL row. This includes `!=`.
- `f.x.isin([...])` never matches a NULL row.
- `~(f.x == v)` never matches a NULL row either. The negation of an unknown is unknown.
- `f.x.isin([])` matches nothing. `~f.x.isin([])` matches every non-NULL row.

A NULL row is never "not equal to v". If you need that behavior, store an explicit default value instead of relying on a missing field.

## Tenant isolation

Filtering happens before vector search. The candidate ids come from SQL, and TurboVec searches only those.
A result from another tenant cannot appear in a filtered search. The test
`tenant_isolation_never_leaks` (Rust) and `test_tenant_isolation_from_python` (Python) check this on
100 documents split across two tenants.
