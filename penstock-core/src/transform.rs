use std::{convert::Infallible, future::Future, num::NonZeroUsize};

/// Converts one typed value into another.
pub trait Transform<In: Send>: Send + Sync {
    type Out: Send;
    type Error: std::error::Error + Send + Sync + 'static;

    fn apply(&self, input: In) -> impl Future<Output = Result<Self::Out, Self::Error>> + Send;

    /// Maximum number of inputs the runner may transform concurrently while retaining order.
    fn max_concurrency(&self) -> NonZeroUsize {
        NonZeroUsize::MIN
    }
}

impl<In, Out, E, F, Fut> Transform<In> for F
where
    In: Send,
    Out: Send,
    E: std::error::Error + Send + Sync + 'static,
    F: Fn(In) -> Fut + Send + Sync,
    Fut: Future<Output = Result<Out, E>> + Send,
{
    type Out = Out;
    type Error = E;

    fn apply(&self, input: In) -> impl Future<Output = Result<Out, E>> + Send {
        self(input)
    }
}

/// A transform bundled with its ordered-concurrency setting.
#[derive(Debug, Clone)]
pub struct Transformer<T> {
    inner: T,
    concurrency: NonZeroUsize,
}

impl<T> Transformer<T> {
    pub fn new(inner: T) -> Self {
        Self {
            inner,
            concurrency: NonZeroUsize::MIN,
        }
    }

    pub fn concurrency(mut self, concurrency: NonZeroUsize) -> Self {
        self.concurrency = concurrency;
        self
    }
}

impl<In, T> Transform<In> for Transformer<T>
where
    In: Send,
    T: Transform<In>,
{
    type Out = T::Out;
    type Error = T::Error;

    fn apply(&self, input: In) -> impl Future<Output = Result<Self::Out, Self::Error>> + Send {
        self.inner.apply(input)
    }

    fn max_concurrency(&self) -> NonZeroUsize {
        self.concurrency
    }
}

/// Passes a value through unchanged.
#[derive(Debug, Default, Clone, Copy)]
pub struct Identity;

impl<In: Send> Transform<In> for Identity {
    type Out = In;
    type Error = Infallible;

    async fn apply(&self, input: In) -> Result<In, Self::Error> {
        Ok(input)
    }
}
