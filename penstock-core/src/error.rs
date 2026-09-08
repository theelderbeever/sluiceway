use std::{error::Error, fmt};

use thiserror::Error;

pub type BoxError = Box<dyn Error + Send + Sync + 'static>;

/// Invalid batching configuration rejected before a pipeline can run.
#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum BatchConfigError {
    #[error("batch size must be greater than zero")]
    ZeroSize,
    #[error("batch timeout must be greater than zero")]
    ZeroTimeout,
}

/// The operation a branch was performing when it failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BranchStage {
    Transform,
    Sink,
    Cursor,
    Task,
}

impl fmt::Display for BranchStage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transform => formatter.write_str("transform"),
            Self::Sink => formatter.write_str("sink"),
            Self::Cursor => formatter.write_str("cursor acknowledgment"),
            Self::Task => formatter.write_str("task execution"),
        }
    }
}

/// One named branch failure. Multiple failures from the same batch are reported together.
#[derive(Debug, Error)]
#[error("branch {index} ({name:?}) failed during {stage}: {message}")]
pub struct BranchFailure {
    pub name: String,
    pub index: usize,
    pub stage: BranchStage,
    pub message: String,
    #[source]
    pub source: Option<BoxError>,
}

impl BranchFailure {
    pub(crate) fn stage(
        name: impl Into<String>,
        index: usize,
        stage: BranchStage,
        source: impl Error + Send + Sync + 'static,
    ) -> Self {
        let message = source.to_string();
        Self {
            name: name.into(),
            index,
            stage,
            message,
            source: Some(Box::new(source)),
        }
    }

    pub(crate) fn cursor(
        name: impl Into<String>,
        index: usize,
        expected: impl fmt::Debug,
        actual: impl fmt::Debug,
    ) -> Self {
        Self {
            name: name.into(),
            index,
            stage: BranchStage::Cursor,
            message: format!("expected {expected:?}, received {actual:?}"),
            source: None,
        }
    }
}

/// Identifies the pipeline stage that failed.
#[derive(Debug, Error)]
pub enum PipelineError<SourceError, SharedTransformError>
where
    SourceError: Error + 'static,
    SharedTransformError: Error + 'static,
{
    #[error("source failed")]
    Source(#[source] SourceError),
    #[error("shared transform failed")]
    SharedTransform(#[source] SharedTransformError),
    #[error("{} branch(es) failed", .0.len())]
    Branches(Vec<BranchFailure>),
    #[error("source commit failed")]
    Commit(#[source] SourceError),
}
