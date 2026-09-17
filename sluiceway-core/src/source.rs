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
    /// Progress accumulated between successful commits.
    type Checkpoint: Send + Sync + 'static;
    type Error: std::error::Error + Send + Sync + 'static;

    fn stream(
        &self,
    ) -> impl Stream<Item = Result<Record<Self::Payload, Self::Position>, Self::Error>> + Send + '_;

    /// Fold one message position into the checkpoint pending its next commit.
    ///
    /// The runner calls this as records enter an ordered sink-delivery attempt. `checkpoint` is
    /// `None` for the first record after a commit. Implementations can select the final position,
    /// aggregate partition positions, or apply other source-specific checkpoint semantics.
    fn track(checkpoint: Option<Self::Checkpoint>, position: &Self::Position) -> Self::Checkpoint;

    /// Commit a successfully delivered checkpoint frontier.
    ///
    /// The source may translate this checkpoint before persisting it, for example to retain a
    /// replay window. The pipeline deliberately does not access checkpoint storage directly.
    fn commit(
        &self,
        checkpoint: Self::Checkpoint,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
