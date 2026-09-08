//! A typed, ordered, branching pipeline runner.
//!
//! One shared transform runs before any number of registered branches. Each branch has its own
//! transform and sink. A batch is committed only after every branch acknowledges its final
//! cursor, providing at-least-once delivery.
//!
//! Pipelines cannot run until at least one branch and a batch policy are configured:
//!
//! ```compile_fail
//! # use penstock_core::{Identity, Pipeline};
//! # fn source() -> impl penstock_core::Source<Payload = (), Cursor = (), Error = std::convert::Infallible> { todo!() }
//! # async fn example() {
//! Pipeline::source(source())
//!     .transform(Identity)
//!     .run_until(std::future::pending())
//!     .await
//!     .unwrap();
//! # }
//! ```

mod branch;
mod checkpoint;
mod error;
mod pipeline;
mod record;
mod sink;
mod source;
mod transform;

pub use branch::{Branch, BranchUnset, Cons, Nil};
#[doc(hidden)]
pub use branch::{BranchMeta, ExecuteBranch, SpawnBranches};
pub use checkpoint::{CheckpointStore, NoCheckpoint};
pub use error::{BatchConfigError, BranchFailure, BranchStage, PipelineError};
pub use pipeline::{BatchPolicy, Batched, Pipeline, Unset};
pub use record::Record;
pub use sink::Sink;
pub use source::Source;
pub use transform::{Identity, Transform, Transformer};
