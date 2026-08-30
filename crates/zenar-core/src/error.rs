use thiserror::Error;

#[derive(Debug, Error)]
pub enum ZenarError {
    #[error("invalid dimension: expected {expected}, got {actual}")]
    InvalidDimension { expected: usize, actual: usize },

    #[error("invalid vector: {0}")]
    InvalidVector(String),

    #[error("vector store error: {0}")]
    VectorStoreError(String),

    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    #[error("duplicate id: {0}")]
    DuplicateIdError(u64),
}
