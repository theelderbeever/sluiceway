use std::{future::Future, marker::PhantomData, num::NonZeroUsize, sync::Arc, time::Duration};

use futures_util::StreamExt;
use tokio::task::JoinSet;

use crate::{
    Batch, BatchConfigError, BoxSink, Cloned, DeliveryFailure, ErasedError, FanoutMode,
    PipelineError, Record, Shared, SharedBatch, Sink, Source, Transform,
};

/// Type-state marker for a pipeline stage that has not been configured.
#[derive(Debug, Default, Clone, Copy)]
pub struct Unset;

/// Type-state marker carrying validated batch settings.
pub struct Batched {
    policy: BatchPolicy,
}

/// Validated limits for flushing record batches.
#[derive(Debug, Clone, Copy)]
pub struct BatchPolicy {
    size: NonZeroUsize,
    timeout: Duration,
}

impl BatchPolicy {
    pub fn new(size: NonZeroUsize, timeout: Duration) -> Result<Self, BatchConfigError> {
        if timeout.is_zero() {
            return Err(BatchConfigError::ZeroTimeout);
        }
        Ok(Self { size, timeout })
    }

    pub fn try_new(size: usize, timeout: Duration) -> Result<Self, BatchConfigError> {
        let size = NonZeroUsize::new(size).ok_or(BatchConfigError::ZeroSize)?;
        Self::new(size, timeout)
    }

    pub fn size(self) -> NonZeroUsize {
        self.size
    }

    pub fn timeout(self) -> Duration {
        self.timeout
    }
}

/// A source and its consuming transform, before delivery topology is selected.
pub struct Pipeline<So, Tr = Unset> {
    source: So,
    transform: Tr,
}

impl<So: Source> Pipeline<So> {
    pub fn source(source: So) -> Self {
        Self {
            source,
            transform: Unset,
        }
    }

    pub fn transform<Tr>(self, transform: Tr) -> Pipeline<So, Tr>
    where
        Tr: Transform<So::Payload>,
    {
        Pipeline {
            source: self.source,
            transform,
        }
    }
}

impl<So, Tr> Pipeline<So, Tr>
where
    So: Source,
    Tr: Transform<So::Payload>,
{
    /// Select linear delivery. Transformed values remain owned and are consumed by one sink.
    pub fn sink<Si>(self, sink: Si) -> LinearPipeline<So, Tr, Si> {
        LinearPipeline {
            source: self.source,
            transform: self.transform,
            sink,
            strategy: Unset,
        }
    }

    /// Begin configuring a fanout delivery topology.
    pub fn fanout(self) -> FanoutBuilder<So, Tr> {
        FanoutBuilder {
            source: self.source,
            transform: self.transform,
            mode: PhantomData,
        }
    }
}

/// A fanout builder requiring an ownership mode before sinks can be attached.
pub struct FanoutBuilder<So, Tr, Mode = Unset> {
    source: So,
    transform: Tr,
    mode: PhantomData<Mode>,
}

impl<So, Tr> FanoutBuilder<So, Tr>
where
    So: Source,
    Tr: Transform<So::Payload>,
{
    /// Give every sink its own owned clone of each batch.
    pub fn cloned(self) -> FanoutBuilder<So, Tr, Cloned> {
        FanoutBuilder {
            source: self.source,
            transform: self.transform,
            mode: PhantomData,
        }
    }

    /// Give every sink the same immutable, reference-counted batch.
    pub fn shared(self) -> FanoutBuilder<So, Tr, Shared> {
        FanoutBuilder {
            source: self.source,
            transform: self.transform,
            mode: PhantomData,
        }
    }
}

impl<So, Tr> FanoutBuilder<So, Tr, Cloned>
where
    So: Source,
    Tr: Transform<So::Payload>,
    Tr::Out: Send + 'static,
{
    pub fn sinks<I>(self, sinks: I) -> FanoutPipeline<So, Tr, Cloned>
    where
        I: IntoIterator<Item = BoxSink<Tr::Out, So::Cursor, Cloned>>,
    {
        FanoutPipeline {
            source: self.source,
            transform: self.transform,
            sinks: sinks.into_iter().collect(),
            strategy: Unset,
        }
    }
}

impl<So, Tr> FanoutBuilder<So, Tr, Shared>
where
    So: Source,
    Tr: Transform<So::Payload>,
    Tr::Out: Send + Sync + 'static,
{
    pub fn sinks<I>(self, sinks: I) -> FanoutPipeline<So, Tr, Shared>
    where
        I: IntoIterator<Item = BoxSink<Tr::Out, So::Cursor, Shared>>,
    {
        FanoutPipeline {
            source: self.source,
            transform: self.transform,
            sinks: sinks.into_iter().collect(),
            strategy: Unset,
        }
    }
}

/// A pipeline that moves each transformed batch into one sink.
pub struct LinearPipeline<So, Tr, Si, St = Unset> {
    source: So,
    transform: Tr,
    sink: Si,
    strategy: St,
}

impl<So, Tr, Si> LinearPipeline<So, Tr, Si>
where
    So: Source,
    Tr: Transform<So::Payload>,
{
    pub fn batched(self, policy: BatchPolicy) -> LinearPipeline<So, Tr, Si, Batched> {
        LinearPipeline {
            source: self.source,
            transform: self.transform,
            sink: self.sink,
            strategy: Batched { policy },
        }
    }
}

impl<So, Tr, Si> LinearPipeline<So, Tr, Si, Batched>
where
    So: Source,
    Tr: Transform<So::Payload>,
    Si: Sink<Batch<Tr::Out, So::Cursor>>,
{
    pub async fn run_until(
        self,
        shutdown: impl Future<Output = ()> + Send,
    ) -> Result<(), PipelineError<So::Error, Tr::Error, Si::Error>> {
        let transform = &self.transform;
        let records = self
            .source
            .stream()
            .take_until(shutdown)
            .map(|item| async move {
                let Record { position, payload } = item.map_err(PipelineError::Source)?;
                let payload = transform
                    .apply(payload)
                    .await
                    .map_err(PipelineError::Transform)?;
                Ok(Record::new(position, payload))
            })
            .buffered(transform.max_concurrency().get());
        tokio::pin!(records);
        let chunks = tokio_stream::StreamExt::chunks_timeout(
            records,
            self.strategy.policy.size.get(),
            self.strategy.policy.timeout,
        );
        tokio::pin!(chunks);

        while let Some(chunk) = chunks.next().await {
            let Some(batch) = Batch::from_chunk(chunk, So::track)? else {
                continue;
            };
            let cursor = batch.cursor.clone();

            self.sink
                .deliver(batch)
                .await
                .map_err(|error| PipelineError::Sink(DeliveryFailure::Sink(error)))?;

            self.source
                .commit(cursor)
                .await
                .map_err(PipelineError::Commit)?;
        }
        Ok(())
    }
}

/// A fanout pipeline whose erased sink input determines cloned or shared delivery.
pub struct FanoutPipeline<So, Tr, Mode, St = Unset>
where
    So: Source,
    Tr: Transform<So::Payload>,
    Mode: FanoutMode<Tr::Out, So::Cursor>,
{
    source: So,
    transform: Tr,
    sinks: Vec<BoxSink<Tr::Out, So::Cursor, Mode>>,
    strategy: St,
}

impl<So, Tr, Mode, St> FanoutPipeline<So, Tr, Mode, St>
where
    So: Source,
    Tr: Transform<So::Payload>,
    Mode: FanoutMode<Tr::Out, So::Cursor>,
{
    async fn drain(
        tasks: &mut JoinSet<Result<(), ErasedError>>,
    ) -> Vec<DeliveryFailure<ErasedError>> {
        let mut failures = Vec::new();
        while let Some(joined) = tasks.join_next().await {
            match joined {
                Ok(Ok(())) => {}
                Ok(Err(error)) => failures.push(DeliveryFailure::Sink(error)),
                Err(error) => failures.push(DeliveryFailure::Task(error)),
            }
        }
        failures
    }
}

impl<So, Tr, Mode> FanoutPipeline<So, Tr, Mode>
where
    So: Source,
    Tr: Transform<So::Payload>,
    Mode: FanoutMode<Tr::Out, So::Cursor>,
{
    pub fn batched(self, policy: BatchPolicy) -> FanoutPipeline<So, Tr, Mode, Batched> {
        FanoutPipeline {
            source: self.source,
            transform: self.transform,
            sinks: self.sinks,
            strategy: Batched { policy },
        }
    }
}

impl<So, Tr> FanoutPipeline<So, Tr, Cloned, Batched>
where
    So: Source,
    Tr: Transform<So::Payload>,
    Tr::Out: Clone + Send + 'static,
{
    pub async fn run_until(
        self,
        shutdown: impl Future<Output = ()> + Send,
    ) -> Result<(), PipelineError<So::Error, Tr::Error, ErasedError>> {
        if self.sinks.is_empty() {
            return Err(PipelineError::NoSinks);
        }

        let transform = &self.transform;
        let records = self
            .source
            .stream()
            .take_until(shutdown)
            .map(|item| async move {
                let Record { position, payload } = item.map_err(PipelineError::Source)?;
                let payload = transform
                    .apply(payload)
                    .await
                    .map_err(PipelineError::Transform)?;
                Ok(Record::new(position, payload))
            })
            .buffered(transform.max_concurrency().get());
        tokio::pin!(records);
        let chunks = tokio_stream::StreamExt::chunks_timeout(
            records,
            self.strategy.policy.size.get(),
            self.strategy.policy.timeout,
        );
        tokio::pin!(chunks);

        while let Some(chunk) = chunks.next().await {
            let Some(batch) = Batch::from_chunk(chunk, So::track)? else {
                continue;
            };
            let cursor = batch.cursor.clone();
            let mut tasks = JoinSet::new();
            let (last_sink, preceding_sinks) = self
                .sinks
                .split_last()
                .expect("empty fanout is rejected before starting the source");

            for sink in preceding_sinks {
                let sink = sink.clone();
                let batch = batch.clone();
                tasks.spawn(async move { sink.deliver(batch).await });
            }
            let sink = last_sink.clone();
            tasks.spawn(async move { sink.deliver(batch).await });

            let failures = Self::drain(&mut tasks).await;
            if !failures.is_empty() {
                return Err(PipelineError::Sinks(failures));
            }

            self.source
                .commit(cursor)
                .await
                .map_err(PipelineError::Commit)?;
        }
        Ok(())
    }
}

impl<So, Tr> FanoutPipeline<So, Tr, Shared, Batched>
where
    So: Source,
    Tr: Transform<So::Payload>,
    Tr::Out: Send + Sync + 'static,
{
    pub async fn run_until(
        self,
        shutdown: impl Future<Output = ()> + Send,
    ) -> Result<(), PipelineError<So::Error, Tr::Error, ErasedError>> {
        if self.sinks.is_empty() {
            return Err(PipelineError::NoSinks);
        }

        let transform = &self.transform;
        let records = self
            .source
            .stream()
            .take_until(shutdown)
            .map(|item| async move {
                let Record { position, payload } = item.map_err(PipelineError::Source)?;
                let payload = transform
                    .apply(payload)
                    .await
                    .map_err(PipelineError::Transform)?;
                Ok(Record::new(position, payload))
            })
            .buffered(transform.max_concurrency().get());
        tokio::pin!(records);
        let chunks = tokio_stream::StreamExt::chunks_timeout(
            records,
            self.strategy.policy.size.get(),
            self.strategy.policy.timeout,
        );
        tokio::pin!(chunks);

        while let Some(chunk) = chunks.next().await {
            let Some(batch) = Batch::from_chunk(chunk, So::track)? else {
                continue;
            };
            let cursor = batch.cursor.clone();
            let batch: SharedBatch<Tr::Out, So::Cursor> = Arc::new(batch);
            let mut tasks = JoinSet::new();

            for sink in &self.sinks {
                let sink = sink.clone();
                let batch = Arc::clone(&batch);
                tasks.spawn(async move { sink.deliver(batch).await });
            }

            let failures = Self::drain(&mut tasks).await;
            if !failures.is_empty() {
                return Err(PipelineError::Sinks(failures));
            }

            self.source
                .commit(cursor)
                .await
                .map_err(PipelineError::Commit)?;
        }
        Ok(())
    }
}
