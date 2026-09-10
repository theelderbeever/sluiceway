use std::{future::Future, marker::PhantomData, ops::Deref, pin::Pin, sync::Arc};

use crate::{ErasedError, Record};

/// One owned batch of transformed records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Batch<T, P> {
    pub records: Vec<Record<T, P>>,
}

impl<T, P> Batch<T, P> {
    pub fn new(records: Vec<Record<T, P>>) -> Self {
        Self { records }
    }

    pub fn into_records(self) -> Vec<Record<T, P>> {
        self.records
    }

    pub(crate) fn from_chunk<Cp, E>(
        chunk: Vec<Result<Record<T, P>, E>>,
        mut track: impl FnMut(Option<Cp>, &P) -> Cp,
    ) -> Result<Option<(Self, Cp)>, E> {
        let mut records = Vec::with_capacity(chunk.len());
        let mut checkpoint = None;

        for record in chunk {
            let record = record?;
            checkpoint = Some(track(checkpoint, record.position()));
            records.push(record);
        }

        Ok(checkpoint.map(|checkpoint| (Self::new(records), checkpoint)))
    }
}

impl<T, P> Deref for Batch<T, P> {
    type Target = [Record<T, P>];

    fn deref(&self) -> &Self::Target {
        &self.records
    }
}

impl<T, P> IntoIterator for Batch<T, P> {
    type Item = Record<T, P>;
    type IntoIter = std::vec::IntoIter<Record<T, P>>;

    fn into_iter(self) -> Self::IntoIter {
        self.records.into_iter()
    }
}

/// One immutable batch shared by every sink in shared fanout.
pub type SharedBatch<T, P> = Arc<Batch<T, P>>;

/// Type-state marker selecting cloned fanout batches.
#[derive(Debug, Default, Clone, Copy)]
pub struct Cloned;

/// Type-state marker selecting shared fanout batches.
#[derive(Debug, Default, Clone, Copy)]
pub struct Shared;

/// Maps a fanout ownership mode to the input consumed by its sinks.
#[doc(hidden)]
pub trait FanoutMode<T, P> {
    type Input: Send + 'static;
}

impl<T, P> FanoutMode<T, P> for Cloned
where
    T: Send + 'static,
    P: Send + 'static,
{
    type Input = Batch<T, P>;
}

impl<T, P> FanoutMode<T, P> for Shared
where
    T: Send + Sync + 'static,
    P: Send + Sync + 'static,
{
    type Input = SharedBatch<T, P>;
}

/// Durably consumes an input.
pub trait Sink<Input: Send>: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;

    fn deliver(&self, input: Input) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

trait ErasedSink<Input>: Send + Sync {
    fn deliver(&self, input: Input) -> BoxFuture<'_, Result<(), ErasedError>>;
}

impl<Input, S> ErasedSink<Input> for S
where
    Input: Send + 'static,
    S: Sink<Input> + 'static,
{
    fn deliver(&self, input: Input) -> BoxFuture<'_, Result<(), ErasedError>> {
        Box::pin(async move { Sink::deliver(self, input).await.map_err(ErasedError::new) })
    }
}

/// A sink whose concrete type and error have been erased for fanout delivery.
pub struct BoxSink<T, P, Mode = Shared>
where
    Mode: FanoutMode<T, P>,
{
    inner: Arc<dyn ErasedSink<Mode::Input>>,
    payload: PhantomData<fn() -> T>,
    position: PhantomData<fn() -> P>,
    mode: PhantomData<fn() -> Mode>,
}

impl<T, P, Mode> Clone for BoxSink<T, P, Mode>
where
    Mode: FanoutMode<T, P>,
{
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            payload: PhantomData,
            position: PhantomData,
            mode: PhantomData,
        }
    }
}

impl<T, P, Mode> BoxSink<T, P, Mode>
where
    Mode: FanoutMode<T, P>,
{
    pub fn new<S>(sink: S) -> Self
    where
        S: Sink<Mode::Input> + 'static,
    {
        Self {
            inner: Arc::new(sink),
            payload: PhantomData,
            position: PhantomData,
            mode: PhantomData,
        }
    }

    pub(crate) async fn deliver(&self, input: Mode::Input) -> Result<(), ErasedError> {
        self.inner.deliver(input).await
    }
}

impl<T, P, S> From<S> for BoxSink<T, P, Cloned>
where
    T: Send + 'static,
    P: Send + 'static,
    S: Sink<Batch<T, P>> + 'static,
{
    fn from(sink: S) -> Self {
        Self::new(sink)
    }
}

impl<T, P, S> From<S> for BoxSink<T, P, Shared>
where
    T: Send + Sync + 'static,
    P: Send + Sync + 'static,
    S: Sink<SharedBatch<T, P>> + 'static,
{
    fn from(sink: S) -> Self {
        Self::new(sink)
    }
}
