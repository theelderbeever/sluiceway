use std::{fmt::Debug, future::Future};

use crate::Record;

/// Durably delivers one ordered batch and acknowledges the persisted cursor.
pub trait Sink<T: Send>: Send + Sync {
    type Cursor: Clone + Eq + Debug + Send + Sync + 'static;
    type Error: std::error::Error + Send + Sync + 'static;

    fn deliver(
        &self,
        batch: Vec<Record<T, Self::Cursor>>,
    ) -> impl Future<Output = Result<Self::Cursor, Self::Error>> + Send;
}
