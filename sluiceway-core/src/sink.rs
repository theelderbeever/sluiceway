use std::{future::Future, marker::PhantomData, ops::Deref, pin::Pin, sync::Arc};

use crate::{ErasedError, Record};

/// An immutable record shared by every sink in shared fanout.
pub type SharedRecord<T, P> = Arc<Record<T, P>>;

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

    pub(crate) fn try_from_chunk<E>(
        chunk: Vec<Result<Record<T, P>, E>>,
    ) -> Result<Option<Self>, E> {
        let records = chunk.into_iter().collect::<Result<Vec<_>, _>>()?;
        Ok((!records.is_empty()).then(|| Self::new(records)))
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

/// Opens batch-scoped sessions that consume records incrementally.
///
/// Naturally synchronous setup can run directly inside an `async fn` implementation or be wrapped
/// with [`std::future::ready`]. Sluiceway never moves collector work to a blocking thread
/// implicitly; implementations that need a blocking pool must opt into one themselves.
pub trait Collector<Input: Send>: Send + Sync {
    type Session: CollectionSession<Input, Error = Self::Error>;
    type Error: std::error::Error + Send + Sync + 'static;

    fn begin(&self) -> impl Future<Output = Result<Self::Session, Self::Error>> + Send;
}

/// One incrementally populated, batch-scoped collector session.
///
/// Synchronous encoding, compression, and in-memory writes may run directly in `push` or `finish`;
/// an implementation does not need a separate synchronous adapter trait.
pub trait CollectionSession<Input: Send>: Send {
    type Error: std::error::Error + Send + Sync + 'static;

    fn push(&mut self, input: Input) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Finish the collection and durably acknowledge every successfully pushed record.
    fn finish(self) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

trait ErasedSink<Input>: Send + Sync {
    fn deliver(&self, input: Input) -> BoxFuture<'_, Result<(), ErasedError>>;
}

trait ErasedCollectionSession<Input>: Send {
    fn push(&mut self, input: Input) -> BoxFuture<'_, Result<(), ErasedError>>;
    fn finish(self: Box<Self>) -> BoxFuture<'static, Result<(), ErasedError>>;
}

impl<Input, S> ErasedCollectionSession<Input> for S
where
    Input: Send + 'static,
    S: CollectionSession<Input> + 'static,
{
    fn push(&mut self, input: Input) -> BoxFuture<'_, Result<(), ErasedError>> {
        Box::pin(async move {
            CollectionSession::push(self, input)
                .await
                .map_err(ErasedError::new)
        })
    }

    fn finish(self: Box<Self>) -> BoxFuture<'static, Result<(), ErasedError>> {
        Box::pin(async move {
            CollectionSession::finish(*self)
                .await
                .map_err(ErasedError::new)
        })
    }
}

pub(crate) struct BoxedCollectionSession<Input>(Box<dyn ErasedCollectionSession<Input>>);

impl<Input: Send + 'static> BoxedCollectionSession<Input> {
    pub(crate) async fn push(&mut self, input: Input) -> Result<(), ErasedError> {
        self.0.push(input).await
    }

    pub(crate) async fn finish(self) -> Result<(), ErasedError> {
        self.0.finish().await
    }
}

trait ErasedCollector<Input>: Send + Sync {
    fn begin(&self) -> BoxFuture<'_, Result<BoxedCollectionSession<Input>, ErasedError>>;
}

impl<Input, C> ErasedCollector<Input> for C
where
    Input: Send + 'static,
    C: Collector<Input> + 'static,
    C::Session: 'static,
{
    fn begin(&self) -> BoxFuture<'_, Result<BoxedCollectionSession<Input>, ErasedError>> {
        Box::pin(async move {
            Collector::begin(self)
                .await
                .map(|session| BoxedCollectionSession(Box::new(session)))
                .map_err(ErasedError::new)
        })
    }
}

/// A collector whose concrete session and error types have been erased for fanout delivery.
pub struct BoxedCollector<Input> {
    inner: Arc<dyn ErasedCollector<Input>>,
}

impl<Input> Clone for BoxedCollector<Input> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<Input: Send + 'static> BoxedCollector<Input> {
    pub fn new<C>(collector: C) -> Self
    where
        C: Collector<Input> + 'static,
        C::Session: 'static,
    {
        Self {
            inner: Arc::new(collector),
        }
    }

    pub(crate) async fn begin(&self) -> Result<BoxedCollectionSession<Input>, ErasedError> {
        self.inner.begin().await
    }
}

pub type BoxCollector<T, P, Mode = Shared> =
    BoxedCollector<<Mode as FanoutRecordMode<T, P>>::Input>;

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
pub struct BoxedSink<Input> {
    inner: Arc<dyn ErasedSink<Input>>,
    input: PhantomData<fn() -> Input>,
}

impl<Input> Clone for BoxedSink<Input> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            input: PhantomData,
        }
    }
}

impl<Input: Send + 'static> BoxedSink<Input> {
    pub fn new<S>(sink: S) -> Self
    where
        S: Sink<Input> + 'static,
    {
        Self {
            inner: Arc::new(sink),
            input: PhantomData,
        }
    }

    pub(crate) async fn deliver(&self, input: Input) -> Result<(), ErasedError> {
        self.inner.deliver(input).await
    }
}

/// Backward-compatible erased batch sink used by fanout pipelines.
pub type BoxSink<T, P, Mode = Shared> = BoxedSink<<Mode as FanoutMode<T, P>>::Input>;

/// Erased one-record sink used by fanout pipelines.
pub type BoxRecordSink<T, P, Mode = Shared> = BoxedSink<<Mode as FanoutRecordMode<T, P>>::Input>;

/// Maps a fanout ownership mode to its one-record input.
#[doc(hidden)]
pub trait FanoutRecordMode<T, P> {
    type Input: Send + 'static;
}

impl<T: Send + 'static, P: Send + 'static> FanoutRecordMode<T, P> for Cloned {
    type Input = Record<T, P>;
}

impl<T: Send + Sync + 'static, P: Send + Sync + 'static> FanoutRecordMode<T, P> for Shared {
    type Input = SharedRecord<T, P>;
}

impl<T, P, S> From<S> for BoxedSink<Batch<T, P>>
where
    T: Send + 'static,
    P: Send + 'static,
    S: Sink<Batch<T, P>> + 'static,
{
    fn from(sink: S) -> Self {
        Self::new(sink)
    }
}

impl<T, P, S> From<S> for BoxedSink<SharedBatch<T, P>>
where
    T: Send + Sync + 'static,
    P: Send + Sync + 'static,
    S: Sink<SharedBatch<T, P>> + 'static,
{
    fn from(sink: S) -> Self {
        Self::new(sink)
    }
}

impl<T, P, S> From<S> for BoxedSink<Record<T, P>>
where
    T: Send + 'static,
    P: Send + 'static,
    S: Sink<Record<T, P>> + 'static,
{
    fn from(sink: S) -> Self {
        Self::new(sink)
    }
}

impl<T, P, S> From<S> for BoxedSink<SharedRecord<T, P>>
where
    T: Send + Sync + 'static,
    P: Send + Sync + 'static,
    S: Sink<SharedRecord<T, P>> + 'static,
{
    fn from(sink: S) -> Self {
        Self::new(sink)
    }
}

impl<T, P, C> From<C> for BoxedCollector<Record<T, P>>
where
    T: Send + 'static,
    P: Send + 'static,
    C: Collector<Record<T, P>> + 'static,
    C::Session: 'static,
{
    fn from(collector: C) -> Self {
        Self::new(collector)
    }
}

impl<T, P, C> From<C> for BoxedCollector<SharedRecord<T, P>>
where
    T: Send + Sync + 'static,
    P: Send + Sync + 'static,
    C: Collector<SharedRecord<T, P>> + 'static,
    C::Session: 'static,
{
    fn from(collector: C) -> Self {
        Self::new(collector)
    }
}
