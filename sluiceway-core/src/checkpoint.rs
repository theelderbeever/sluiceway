use std::{convert::Infallible, future::Future};

/// Durable storage for an opaque source checkpoint.
pub trait CheckpointStore<Cp: Sync>: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;

    fn load(&self) -> impl Future<Output = Result<Option<Cp>, Self::Error>> + Send;
    fn save(&self, checkpoint: &Cp) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// No-op checkpoint storage used when persistence is disabled.
///
/// This is a [`CheckpointStore`], not a checkpoint representation implementing
/// [`crate::Checkpoint`].
#[derive(Debug, Default, Clone, Copy)]
pub struct NoCheckpoint;

impl<Cp: Sync> CheckpointStore<Cp> for NoCheckpoint {
    type Error = Infallible;

    async fn load(&self) -> Result<Option<Cp>, Self::Error> {
        Ok(None)
    }

    async fn save(&self, _checkpoint: &Cp) -> Result<(), Self::Error> {
        Ok(())
    }
}

impl<Cp: Sync, S: CheckpointStore<Cp>> CheckpointStore<Cp> for Option<S> {
    type Error = S::Error;

    async fn load(&self) -> Result<Option<Cp>, Self::Error> {
        match self {
            Some(store) => store.load().await,
            None => Ok(None),
        }
    }

    async fn save(&self, checkpoint: &Cp) -> Result<(), Self::Error> {
        match self {
            Some(store) => store.save(checkpoint).await,
            None => Ok(()),
        }
    }
}
