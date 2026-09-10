//! Durable source checkpoints.

#[cfg(feature = "filesystem")]
mod file;
#[cfg(feature = "object-store")]
mod object_store;
#[cfg(any(
    feature = "sql-postgres",
    feature = "sql-mysql",
    feature = "sql-sqlite"
))]
mod sql;

#[cfg(feature = "filesystem")]
pub use file::{FileCheckpoint, FileCheckpointError};
#[cfg(feature = "object-store")]
pub use object_store::{ObjectStoreCheckpoint, ObjectStoreCheckpointError};
#[cfg(any(
    feature = "sql-postgres",
    feature = "sql-mysql",
    feature = "sql-sqlite"
))]
pub use sql::{SqlCheckpoint, SqlCheckpointError};
