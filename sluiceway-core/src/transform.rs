use std::{convert::Infallible, future::Future, num::NonZeroUsize};

/// Converts one typed value into another with read-only access to its source position.
///
/// The runner retains the owned position and attaches it to the transformed output. A transform
/// that dispatches work to a `'static` Tokio task must copy or clone any position data needed by
/// that task before spawning it.
pub trait Transform<In: Send, P: Sync>: Send + Sync {
    type Out: Send;
    type Error: std::error::Error + Send + Sync + 'static;

    fn apply(
        &self,
        position: &P,
        input: In,
    ) -> impl Future<Output = Result<Self::Out, Self::Error>> + Send;

    /// Maximum number of inputs the runner may transform concurrently while retaining order.
    fn max_concurrency(&self) -> NonZeroUsize {
        NonZeroUsize::MIN
    }
}

impl<In, P, Out, E, F, Fut> Transform<In, P> for F
where
    In: Send,
    P: Sync,
    Out: Send,
    E: std::error::Error + Send + Sync + 'static,
    F: Fn(&P, In) -> Fut + Send + Sync,
    Fut: Future<Output = Result<Out, E>> + Send,
{
    type Out = Out;
    type Error = E;

    fn apply(&self, position: &P, input: In) -> impl Future<Output = Result<Out, E>> + Send {
        self(position, input)
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

impl<In, P, T> Transform<In, P> for Transformer<T>
where
    In: Send,
    P: Sync,
    T: Transform<In, P>,
{
    type Out = T::Out;
    type Error = T::Error;

    fn apply(
        &self,
        position: &P,
        input: In,
    ) -> impl Future<Output = Result<Self::Out, Self::Error>> + Send {
        self.inner.apply(position, input)
    }

    fn max_concurrency(&self) -> NonZeroUsize {
        self.concurrency
    }
}

/// Passes a value through unchanged.
#[derive(Debug, Default, Clone, Copy)]
pub struct Identity;

impl<In: Send, P: Sync> Transform<In, P> for Identity {
    type Out = In;
    type Error = Infallible;

    async fn apply(&self, _position: &P, input: In) -> Result<In, Self::Error> {
        Ok(input)
    }
}

#[cfg(test)]
mod tests {
    use std::{convert::Infallible, num::NonZeroUsize};

    use super::{Transform, Transformer};

    #[tokio::test]
    async fn transformer_controls_ordered_concurrency() {
        let concurrency = NonZeroUsize::new(2).unwrap();
        let transform =
            Transformer::new(
                |_position: &u64, input| async move { Ok::<_, Infallible>(input * 2) },
            )
            .concurrency(concurrency);

        let output = transform.apply(&7, 21).await.unwrap();

        assert_eq!(output, 42);
        assert_eq!(transform.max_concurrency(), concurrency);
    }

    #[tokio::test]
    async fn transform_can_spawn_after_copying_position_data() {
        let transform = |position: &u64, input| {
            let position = *position;
            async move {
                tokio::spawn(async move { Ok::<_, Infallible>(position + input) })
                    .await
                    .unwrap()
            }
        };

        assert_eq!(transform.apply(&20, 22).await.unwrap(), 42);
    }

    #[tokio::test]
    async fn transform_can_spawn_blocking_after_copying_position_data() {
        let transform = |position: &u64, input| {
            let position = *position;
            async move {
                tokio::task::spawn_blocking(move || Ok::<_, Infallible>(position + input))
                    .await
                    .unwrap()
            }
        };

        assert_eq!(transform.apply(&20, 22).await.unwrap(), 42);
    }

    #[tokio::test]
    async fn transform_future_can_borrow_its_transform() {
        struct Add(u64);

        impl Transform<u64, u64> for Add {
            type Out = u64;
            type Error = Infallible;

            async fn apply(&self, position: &u64, input: u64) -> Result<u64, Self::Error> {
                tokio::task::yield_now().await;
                Ok(self.0 + position + input)
            }
        }

        let transform = Add(2);
        assert_eq!(transform.apply(&19, 21).await.unwrap(), 42);
    }

    #[tokio::test]
    async fn identity_ignores_position() {
        let transform = super::Identity;

        assert_eq!(transform.apply(&"position", 42).await.unwrap(), 42);
    }
}
