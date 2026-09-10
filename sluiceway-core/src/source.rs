use std::future::Future;

use futures_core::Stream;

use crate::Record;

/// Produces ordered records and commits positions that have been delivered by every sink.
///
/// The stream owns its natural termination semantics. A source that supports graceful shutdown
/// can observe its configured signal from [`Source::stream`], stop accepting external input,
/// drain its own buffering, and then return the end of the stream.
pub trait Source: Send + Sync {
    type Payload: Send;
    /// Message-local source position carried through transformation and delivery.
    type Position: Send + Sync + 'static;
    /// Batch-level progress committed after successful delivery.
    type Checkpoint: Send + Sync + 'static;
    type Error: std::error::Error + Send + Sync + 'static;

    fn stream(
        &self,
    ) -> impl Stream<Item = Result<Record<Self::Payload, Self::Position>, Self::Error>> + Send + '_;

    /// Fold one message position into the checkpoint for its batch.
    ///
    /// The runner calls this exactly once per successfully transformed record, in source order.
    /// `checkpoint` is `None` for the first record in each batch. Implementations can select the
    /// final position, aggregate partition positions, or apply other source-specific checkpoint
    /// semantics.
    fn track(checkpoint: Option<Self::Checkpoint>, position: &Self::Position) -> Self::Checkpoint;

    /// Commit a successfully delivered batch checkpoint.
    ///
    /// The source may translate this checkpoint before persisting it, for example to retain a
    /// replay window. The pipeline deliberately does not access checkpoint storage directly.
    fn commit(
        &self,
        checkpoint: Self::Checkpoint,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
