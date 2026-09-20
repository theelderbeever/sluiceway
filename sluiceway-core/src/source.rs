use std::future::Future;

use futures_core::Stream;

use crate::Record;

/// Constructs checkpoint epochs from source positions.
pub trait Checkpoint<P>: Sized + Send + Sync + 'static {
    /// Construct the first checkpoint epoch from its first position.
    fn start_epoch(first_position: &P) -> Self;

    /// Add another position to the currently open checkpoint epoch.
    fn include_position(&mut self, position: &P);

    /// Close this epoch and construct the next epoch containing the boundary position.
    fn start_next_epoch(self, first_position_of_next_epoch: &P) -> CheckpointEpochTransition<Self> {
        CheckpointEpochTransition {
            completed_epoch: self,
            next_epoch: Self::start_epoch(first_position_of_next_epoch),
        }
    }
}

/// Checkpoints on both sides of a source-position checkpoint epoch boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointEpochTransition<C> {
    /// The completed checkpoint epoch preceding the boundary position.
    pub completed_epoch: C,
    /// The new pending checkpoint epoch beginning with the boundary position.
    pub next_epoch: C,
}

/// Produces ordered records and commits positions that have been delivered by every sink.
///
/// The stream owns its natural termination semantics. A source that supports graceful shutdown
/// can observe its configured signal from [`Source::stream`], stop accepting external input,
/// drain its own buffering, and then return the end of the stream.
pub trait Source: Send + Sync {
    type Payload: Send;
    /// Message-local source position carried through transformation and delivery.
    type Position: Send + Sync + 'static;
    /// Progress accumulated within one checkpoint epoch.
    type Checkpoint: Checkpoint<Self::Position>;
    type Error: std::error::Error + Send + Sync + 'static;

    fn stream(
        &self,
    ) -> impl Stream<Item = Result<Record<Self::Payload, Self::Position>, Self::Error>> + Send + '_;

    /// Commit the progress represented by one successfully delivered checkpoint epoch.
    ///
    /// The source may translate this checkpoint before persisting it, for example to retain a
    /// replay window. The pipeline deliberately does not access checkpoint storage directly.
    fn commit(
        &self,
        checkpoint: Self::Checkpoint,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
