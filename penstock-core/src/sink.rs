use std::{fmt::Debug, future::Future, marker::PhantomData, pin::Pin, sync::Arc};

use crate::{ErasedError, Record};

/// One owned batch of transformed records.
pub type Batch<T, C> = Vec<Record<T, C>>;

/// One immutable batch shared by every sink in shared fanout.
pub type SharedBatch<T, C> = Arc<[Record<T, C>]>;

/// Type-state marker selecting cloned fanout batches.
#[derive(Debug, Default, Clone, Copy)]
pub struct Cloned;

/// Type-state marker selecting shared fanout batches.
#[derive(Debug, Default, Clone, Copy)]
pub struct Shared;

/// Maps a fanout ownership mode to the input consumed by its sinks.
#[doc(hidden)]
pub trait FanoutMode<T, C> {
    type Input: Send + 'static;
}

impl<T, C> FanoutMode<T, C> for Cloned
where
    T: Send + 'static,
    C: Send + 'static,
{
    type Input = Batch<T, C>;
}

impl<T, C> FanoutMode<T, C> for Shared
where
    T: Send + Sync + 'static,
    C: Send + Sync + 'static,
{
    type Input = SharedBatch<T, C>;
}

/// Durably consumes an input and acknowledges its persisted cursor.
pub trait Sink<Input: Send>: Send + Sync {
    type Cursor: Clone + Eq + Debug + Send + Sync + 'static;
    type Error: std::error::Error + Send + Sync + 'static;

    fn deliver(
        &self,
        input: Input,
    ) -> impl Future<Output = Result<Self::Cursor, Self::Error>> + Send;
}

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

trait ErasedSink<Input, C>: Send + Sync {
    fn deliver(&self, input: Input) -> BoxFuture<'_, Result<C, ErasedError>>;
}

impl<Input, C, S> ErasedSink<Input, C> for S
where
    Input: Send + 'static,
    C: Clone + Eq + Debug + Send + Sync + 'static,
    S: Sink<Input, Cursor = C> + 'static,
{
    fn deliver(&self, input: Input) -> BoxFuture<'_, Result<C, ErasedError>> {
        Box::pin(async move { Sink::deliver(self, input).await.map_err(ErasedError::new) })
    }
}

/// A sink whose concrete type and error have been erased for fanout delivery.
pub struct BoxSink<T, C, Mode = Shared>
where
    Mode: FanoutMode<T, C>,
{
    inner: Arc<dyn ErasedSink<Mode::Input, C>>,
    mode: PhantomData<Mode>,
}

impl<T, C, Mode> Clone for BoxSink<T, C, Mode>
where
    Mode: FanoutMode<T, C>,
{
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            mode: PhantomData,
        }
    }
}

impl<T, C, Mode> BoxSink<T, C, Mode>
where
    Mode: FanoutMode<T, C>,
    C: Clone + Eq + Debug + Send + Sync + 'static,
{
    pub fn new<S>(sink: S) -> Self
    where
        S: Sink<Mode::Input, Cursor = C> + 'static,
    {
        Self {
            inner: Arc::new(sink),
            mode: PhantomData,
        }
    }

    pub(crate) async fn deliver(&self, input: Mode::Input) -> Result<C, ErasedError> {
        self.inner.deliver(input).await
    }
}

impl<T, C, S> From<S> for BoxSink<T, C, Cloned>
where
    T: Send + 'static,
    C: Clone + Eq + Debug + Send + Sync + 'static,
    S: Sink<Batch<T, C>, Cursor = C> + 'static,
{
    fn from(sink: S) -> Self {
        Self::new(sink)
    }
}

impl<T, C, S> From<S> for BoxSink<T, C, Shared>
where
    T: Send + Sync + 'static,
    C: Clone + Eq + Debug + Send + Sync + 'static,
    S: Sink<SharedBatch<T, C>, Cursor = C> + 'static,
{
    fn from(sink: S) -> Self {
        Self::new(sink)
    }
}
