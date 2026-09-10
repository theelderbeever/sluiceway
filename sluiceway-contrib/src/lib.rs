//! Optional source and sink integrations for Sluiceway pipelines.
//!
//! Integrations are independently feature gated so applications only compile their external
//! dependencies when they opt in.

#[cfg(feature = "kafka")]
pub mod kafka;
