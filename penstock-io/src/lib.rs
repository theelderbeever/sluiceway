//! Durable checkpoint adapters for Penstock pipelines.
//!
//! The default `filesystem` feature provides [`FileCheckpoint`]. Remote object-store and SQL
//! checkpoints are independently feature gated. Every adapter stores checkpoints as JSON and works
//! with any checkpoint implementing Serde's serialization traits.

pub mod checkpoint;

#[cfg(feature = "filesystem")]
pub use checkpoint::{FileCheckpoint, FileCheckpointError};
#[cfg(feature = "object-store")]
pub use checkpoint::{ObjectStoreCheckpoint, ObjectStoreCheckpointError};
#[cfg(any(
    feature = "sql-postgres",
    feature = "sql-mysql",
    feature = "sql-sqlite"
))]
pub use checkpoint::{SqlCheckpoint, SqlCheckpointError};
