//! Zenar core: an embedded vector database with typed metadata filtering.
//!
//! TurboVec stores and searches vectors; SQLite stores documents, metadata
//! and collection config. This crate has no Python dependency.

pub mod collection;
pub mod database;
pub mod document;
pub mod error;
pub mod filter;
pub mod storage;
pub mod value;

pub use collection::{Collection, Hit, RecoveryReport};
pub use database::{MetadataSync, VectorDB, validate_collection_name};
pub use document::DocumentRow;
pub use error::ZenarError;
pub use filter::Filter;
pub use value::{FieldKind, MetaValue};
