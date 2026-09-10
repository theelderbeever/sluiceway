//! Public facade for the Sluiceway typed linear and fanout pipeline framework.
//!
//! Core types are re-exported at the crate root so applications normally need only `sluiceway`.
//! Enable `io`, `object-store`, or one of the `sql-*` features for durable checkpoint adapters.

pub use sluiceway_core as core;
pub use sluiceway_core::*;

#[cfg(feature = "io")]
pub use sluiceway_io as io;
