//! Naming rules. Collection names become file names, and field names become keys in the
//! document log. Both are restricted to a small allowlist. Names are rejected, never sanitized.

/// Lowercase ASCII letters, digits and `_`, starting with a letter, at most 64 characters.
pub fn valid_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_lowercase())
        && name.len() <= 64
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

/// Metadata field names follow the same rule as collection names.
pub fn is_valid_field_name(name: &str) -> bool {
    valid_identifier(name)
}

/// Collection names follow the same rule. Checked before any path is built (spec section 73).
pub fn validate_collection_name(name: &str) -> Result<(), crate::error::ZenarError> {
    if valid_identifier(name) {
        Ok(())
    } else {
        Err(crate::error::ZenarError::InvalidCollectionName(
            name.to_string(),
        ))
    }
}
