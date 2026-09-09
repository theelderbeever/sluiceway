use std::{error::Error, fmt};

use thiserror::Error;

type BoxError = Box<dyn Error + Send + Sync + 'static>;

/// An owned, type-erased error returned by a fanout sink.
pub struct ErasedError {
    source: BoxError,
}

impl ErasedError {
    pub(crate) fn new(error: impl Error + Send + Sync + 'static) -> Self {
        Self {
            source: Box::new(error),
        }
    }
}

impl fmt::Debug for ErasedError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.source, formatter)
    }
}

impl fmt::Display for ErasedError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.source, formatter)
    }
}

impl Error for ErasedError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.source.as_ref())
    }
}

/// Invalid batching configuration rejected before a pipeline can run.
#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum BatchConfigError {
    #[error("batch size must be greater than zero")]
    ZeroSize,
    #[error("batch timeout must be greater than zero")]
    ZeroTimeout,
}

/// A sink delivery failure without branch names, indexes, or runner metadata.
#[derive(Debug, Error)]
pub enum DeliveryFailure<E>
where
    E: Error + 'static,
{
    #[error("sink failed")]
    Sink(#[source] E),
    #[error("sink task failed")]
    Task(#[source] tokio::task::JoinError),
}

/// Identifies the pipeline operation that failed.
#[derive(Debug, Error)]
pub enum PipelineError<SourceError, TransformError, SinkError>
where
    SourceError: Error + 'static,
    TransformError: Error + 'static,
    SinkError: Error + 'static,
{
    #[error("fanout requires at least one sink")]
    NoSinks,
    #[error("source failed")]
    Source(#[source] SourceError),
    #[error("transform failed")]
    Transform(#[source] TransformError),
    #[error("sink delivery failed")]
    Sink(#[source] DeliveryFailure<SinkError>),
    #[error("{} sink deliveries failed", .0.len())]
    Sinks(Vec<DeliveryFailure<SinkError>>),
    #[error("source commit failed")]
    Commit(#[source] SourceError),
}
