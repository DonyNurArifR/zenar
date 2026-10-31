//! Zenar core: an embedded vector database with typed metadata filtering.
//!
//! TurboVec stores and searches vectors. Zenar's document store keeps documents and
//! metadata in an append-only log with in-memory indexes. No Python dependency.

pub mod catalog;
pub mod collection;
pub mod database;
pub mod docstore;
pub mod error;
pub mod filter;
pub mod names;
pub mod storage;
pub mod value;

pub use collection::{Collection, Hit, Record, RecoveryReport};
pub use database::{MetadataSync, VectorDB};
pub use docstore::DocumentRow;
pub use error::ZenarError;
pub use filter::Filter;
pub use names::validate_collection_name;
pub use value::{FieldKind, MetaValue};
