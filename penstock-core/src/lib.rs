//! Typed, ordered linear and fanout pipeline runners.
//!
//! One consuming transform feeds either a single owned sink or a type-erased fanout of sinks. A
//! batch is committed only after every sink acknowledges its final cursor, providing at-least-once
//! delivery.
//!
//! Pipelines cannot run until a delivery topology and batch policy are configured:
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

mod checkpoint;
mod error;
mod pipeline;
mod record;
mod sink;
mod source;
mod transform;

pub use checkpoint::{CheckpointStore, NoCheckpoint};
pub use error::{BatchConfigError, DeliveryFailure, ErasedError, PipelineError};
pub use pipeline::{
    BatchPolicy, Batched, FanoutBuilder, FanoutPipeline, LinearPipeline, Pipeline, Unset,
};
pub use record::Record;
pub use sink::{Batch, BoxSink, Cloned, FanoutMode, Shared, SharedBatch, Sink};
pub use source::Source;
pub use transform::{Identity, Transform, Transformer};
