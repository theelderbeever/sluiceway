use std::{convert::Infallible, future::Future};

/// Durable storage for an opaque source cursor.
pub trait CheckpointStore<C: Sync>: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;

    fn load(&self) -> impl Future<Output = Result<Option<C>, Self::Error>> + Send;
    fn save(&self, cursor: &C) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// Checkpoint implementation used when persistence is disabled.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoCheckpoint;

impl<C: Sync> CheckpointStore<C> for NoCheckpoint {
    type Error = Infallible;

    async fn load(&self) -> Result<Option<C>, Self::Error> {
        Ok(None)
    }

    async fn save(&self, _cursor: &C) -> Result<(), Self::Error> {
        Ok(())
    }
}

impl<C: Sync, S: CheckpointStore<C>> CheckpointStore<C> for Option<S> {
    type Error = S::Error;

    async fn load(&self) -> Result<Option<C>, Self::Error> {
        match self {
            Some(store) => store.load().await,
            None => Ok(None),
        }
    }

    async fn save(&self, cursor: &C) -> Result<(), Self::Error> {
        match self {
            Some(store) => store.save(cursor).await,
            None => Ok(()),
        }
    }
}
