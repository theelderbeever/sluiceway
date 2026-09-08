//! Public facade for the Penstock typed branching pipeline framework.
//!
//! Core types are re-exported at the crate root so applications normally need only `penstock`.
//! Enable `io`, `object-store`, or one of the `sql-*` features for durable checkpoint adapters.

pub use penstock_core as core;
pub use penstock_core::*;

#[cfg(feature = "io")]
pub use penstock_io as io;
