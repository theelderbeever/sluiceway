use std::{convert::Infallible, future::Future, num::NonZeroUsize};

use thiserror::Error;

/// A failure returned by a transform dispatched to a Tokio task.
#[derive(Debug, Error)]
pub enum SpawnError<E>
where
    E: std::error::Error + 'static,
{
    #[error(transparent)]
    Transform(E),
    #[error("transform task failed")]
    Task(#[source] tokio::task::JoinError),
}

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

impl Transformer<()> {
    /// Runs each asynchronous transform in its own Tokio task.
    ///
    /// Transform failures and task cancellation or panic are returned as [`SpawnError`].
    pub fn spawn<In, Out, E, F, Fut>(
        concurrency: NonZeroUsize,
        transform: F,
    ) -> Transformer<impl Transform<In, Out = Out, Error = SpawnError<E>>>
    where
        In: Send + 'static,
        Out: Send + 'static,
        E: std::error::Error + Send + Sync + 'static,
        F: Fn(In) -> Fut + Send + Sync,
        Fut: Future<Output = Result<Out, E>> + Send + 'static,
    {
        Transformer::new(move |input| {
            let future = transform(input);
            async move {
                tokio::spawn(future)
                    .await
                    .map_err(SpawnError::Task)?
                    .map_err(SpawnError::Transform)
            }
        })
        .concurrency(concurrency)
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

#[cfg(test)]
mod tests {
    use std::{
        convert::Infallible,
        io,
        num::NonZeroUsize,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
    };

    use super::{SpawnError, Transform, Transformer};

    #[tokio::test]
    async fn spawn_runs_an_async_transform_in_a_task() {
        let concurrency = NonZeroUsize::new(2).unwrap();
        let transform = Transformer::spawn(concurrency, |input| async move {
            Ok::<_, Infallible>((input * 2, tokio::task::id()))
        });

        let (output, _) = transform.apply(21).await.unwrap();

        assert_eq!(output, 42);
        assert_eq!(transform.max_concurrency(), concurrency);
    }

    #[tokio::test]
    async fn spawn_returns_task_panics_as_join_errors() {
        let transform = Transformer::spawn(NonZeroUsize::MIN, |()| async move {
            panic!("transform panicked");
            #[allow(unreachable_code)]
            Ok::<(), Infallible>(())
        });

        assert!(matches!(
            transform.apply(()).await.unwrap_err(),
            SpawnError::Task(error) if error.is_panic()
        ));
    }

    #[tokio::test]
    async fn spawn_preserves_transform_errors() {
        let transform = Transformer::spawn(NonZeroUsize::MIN, |()| async move {
            Err::<(), _>(io::Error::other("transform failed"))
        });

        assert!(matches!(
            transform.apply(()).await.unwrap_err(),
            SpawnError::Transform(error) if error.to_string() == "transform failed"
        ));
    }

    #[tokio::test]
    async fn spawn_waits_until_the_apply_future_is_polled() {
        let async_started = Arc::new(AtomicBool::new(false));
        let transform = Transformer::spawn(NonZeroUsize::MIN, {
            let async_started = Arc::clone(&async_started);
            move |()| {
                let async_started = Arc::clone(&async_started);
                async move {
                    async_started.store(true, Ordering::SeqCst);
                    Ok::<(), Infallible>(())
                }
            }
        });
        let future = transform.apply(());

        tokio::task::yield_now().await;
        assert!(!async_started.load(Ordering::SeqCst));
        future.await.unwrap();
    }
}
