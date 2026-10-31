use thiserror::Error;

/// Every failure Zenar can report. zenar-core never panics on user input;
/// each failure path returns one of these (spec sections 47, 66).
#[derive(Debug, Error, Clone, PartialEq)]
pub enum ZenarError {
    #[error("collection not found: {0}")]
    CollectionNotFound(String),

    #[error("collection {0} is closed: its database was closed, or the collection was dropped")]
    CollectionClosed(String),

    #[error("collection already exists: {0}")]
    CollectionAlreadyExists(String),

    #[error("invalid collection name {0:?}: use 1-64 chars from [a-z0-9_], starting with a letter")]
    InvalidCollectionName(String),

    #[error("dimension mismatch: expected {expected}, got {actual}")]
    DimensionMismatch { expected: usize, actual: usize },

    #[error("invalid dimension {0}: must be a positive multiple of 8, at most 16384")]
    InvalidDimension(usize),

    #[error("invalid vector: {0}")]
    InvalidVector(String),

    #[error("duplicate id: {0}")]
    DuplicateId(u64),

    #[error("vector not found: {0}")]
    VectorNotFound(u64),

    #[error("invalid metadata: {0}")]
    InvalidMetadata(String),

    #[error("invalid filter: {0}")]
    InvalidFilter(String),

    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    #[error("database is locked by another process: {0}")]
    DatabaseLocked(String),

    #[error("vector store error: {0}")]
    VectorStoreError(String),

    #[error("metadata store error: {0}")]
    MetadataStoreError(String),

    #[error("persistence error: {0}")]
    PersistenceError(String),

    #[error("recovery error: {0}")]
    RecoveryError(String),

    #[error("internal panic caught at boundary: {0}")]
    Panic(String),
}
