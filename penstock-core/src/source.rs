use std::future::Future;

use futures_core::Stream;

use crate::Record;

/// Produces ordered records and commits positions that have been delivered by every sink.
pub trait Source: Send + Sync {
    type Payload: Send;
    /// Message-local source position carried until a batch is sealed.
    type Position: Send;
    /// Batch-level position committed after successful delivery.
    type Cursor: Clone + Send + Sync + 'static;
    type Error: std::error::Error + Send + Sync + 'static;

    fn stream(
        &self,
    ) -> impl Stream<Item = Result<Record<Self::Payload, Self::Position>, Self::Error>> + Send + '_;

    /// Fold one message position into the cursor for its batch.
    ///
    /// The runner calls this exactly once per successfully transformed record, in source order.
    /// `cursor` is `None` for the first record in each batch. Implementations can select the final
    /// position, aggregate partition positions, or apply other source-specific cursor semantics.
    fn track(cursor: Option<Self::Cursor>, position: Self::Position) -> Self::Cursor;

    /// Commit a successfully delivered batch cursor.
    ///
    /// The source may translate this cursor before persisting it, for example to retain a replay
    /// window. The pipeline deliberately does not access checkpoint storage directly.
    fn commit(&self, cursor: Self::Cursor) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
