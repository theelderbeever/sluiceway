use std::{future::Future, marker::PhantomData, ops::Deref, pin::Pin, sync::Arc};

use crate::{ErasedError, Record};

/// One owned batch of transformed items and its source-selected commit cursor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Batch<T, C> {
    pub items: Vec<T>,
    pub cursor: C,
}

impl<T, C> Batch<T, C> {
    pub fn new(items: Vec<T>, cursor: C) -> Self {
        Self { items, cursor }
    }

    pub fn into_parts(self) -> (Vec<T>, C) {
        (self.items, self.cursor)
    }

    pub(crate) fn from_chunk<P, E>(
        chunk: Vec<Result<Record<T, P>, E>>,
        mut track: impl FnMut(Option<C>, P) -> C,
    ) -> Result<Option<Self>, E> {
        let mut items = Vec::with_capacity(chunk.len());
        let mut cursor = None;

        for record in chunk {
            let Record { position, payload } = record?;
            cursor = Some(track(cursor, position));
            items.push(payload);
        }

        Ok(cursor.map(|cursor| Self::new(items, cursor)))
    }
}

impl<T, C> Deref for Batch<T, C> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        &self.items
    }
}

impl<T, C> IntoIterator for Batch<T, C> {
    type Item = T;
    type IntoIter = std::vec::IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        self.items.into_iter()
    }
}

/// One immutable batch shared by every sink in shared fanout.
pub type SharedBatch<T, C> = Arc<Batch<T, C>>;

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
pub struct BoxSink<T, C, Mode = Shared>
where
    Mode: FanoutMode<T, C>,
{
    inner: Arc<dyn ErasedSink<Mode::Input>>,
    payload: PhantomData<fn() -> T>,
    cursor: PhantomData<fn() -> C>,
    mode: PhantomData<fn() -> Mode>,
}

impl<T, C, Mode> Clone for BoxSink<T, C, Mode>
where
    Mode: FanoutMode<T, C>,
{
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            payload: PhantomData,
            cursor: PhantomData,
            mode: PhantomData,
        }
    }
}

impl<T, C, Mode> BoxSink<T, C, Mode>
where
    Mode: FanoutMode<T, C>,
{
    pub fn new<S>(sink: S) -> Self
    where
        S: Sink<Mode::Input> + 'static,
    {
        Self {
            inner: Arc::new(sink),
            payload: PhantomData,
            cursor: PhantomData,
            mode: PhantomData,
        }
    }

    pub(crate) async fn deliver(&self, input: Mode::Input) -> Result<(), ErasedError> {
        self.inner.deliver(input).await
    }
}

impl<T, C, S> From<S> for BoxSink<T, C, Cloned>
where
    T: Send + 'static,
    C: Send + 'static,
    S: Sink<Batch<T, C>> + 'static,
{
    fn from(sink: S) -> Self {
        Self::new(sink)
    }
}

impl<T, C, S> From<S> for BoxSink<T, C, Shared>
where
    T: Send + Sync + 'static,
    C: Send + Sync + 'static,
    S: Sink<SharedBatch<T, C>> + 'static,
{
    fn from(sink: S) -> Self {
        Self::new(sink)
    }
}
