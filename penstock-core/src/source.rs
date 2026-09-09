use std::{fmt::Debug, future::Future};

use futures_core::Stream;

use crate::Record;

/// Produces ordered records and commits positions that have been delivered by every sink.
pub trait Source: Send + Sync {
    type Payload: Send;
    type Cursor: Clone + Eq + Debug + Send + Sync + 'static;
    type Error: std::error::Error + Send + Sync + 'static;

    fn stream(
        &self,
    ) -> impl Stream<Item = Result<Record<Self::Payload, Self::Cursor>, Self::Error>> + Send + '_;

    /// Commit a successfully delivered cursor.
    ///
    /// The source may translate this cursor before persisting it, for example to retain a replay
    /// window. The pipeline deliberately does not access checkpoint storage directly.
    fn commit(&self, cursor: Self::Cursor) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
