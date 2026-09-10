use std::{future::Future, marker::PhantomData, num::NonZeroUsize, sync::Arc, time::Duration};

use futures_util::StreamExt;
use tokio::task::JoinSet;

use crate::{
    Batch, BatchConfigError, BoxSink, Cloned, DeliveryFailure, ErasedError, FanoutMode,
    PipelineError, PipelineId, Record, Shared, SharedBatch, Sink, Source, Transform, telemetry,
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

    fn emit_reason(self, records: usize) -> &'static str {
        if records == self.size.get() {
            "full"
        } else {
            "timeout"
        }
    }
}

/// A source and its consuming transform, before delivery topology is selected.
pub struct Pipeline<So, Tr = Unset> {
    id: Option<PipelineId>,
    source: So,
    transform: Tr,
}

impl<So: Source> Pipeline<So> {
    pub fn source(source: So) -> Self {
        Self {
            id: None,
            source,
            transform: Unset,
        }
    }

    pub fn transform<Tr>(self, transform: Tr) -> Pipeline<So, Tr>
    where
        Tr: Transform<So::Payload, So::Position>,
    {
        Pipeline {
            id: self.id,
            source: self.source,
            transform,
        }
    }
}

impl<So, Tr> Pipeline<So, Tr> {
    /// Attach a stable identity used to correlate this pipeline's metrics and checkpoints.
    pub fn id(mut self, id: PipelineId) -> Self {
        self.id = Some(id);
        self
    }
}

impl<So, Tr> Pipeline<So, Tr>
where
    So: Source,
    Tr: Transform<So::Payload, So::Position>,
{
    /// Select linear delivery. Transformed values remain owned and are consumed by one sink.
    pub fn sink<Si>(self, sink: Si) -> LinearPipeline<So, Tr, Si> {
        LinearPipeline {
            id: self.id,
            source: self.source,
            transform: self.transform,
            sink,
            strategy: Unset,
        }
    }

    /// Begin configuring a fanout delivery topology.
    pub fn fanout(self) -> FanoutBuilder<So, Tr> {
        FanoutBuilder {
            id: self.id,
            source: self.source,
            transform: self.transform,
            mode: PhantomData,
        }
    }
}

/// A fanout builder requiring an ownership mode before sinks can be attached.
pub struct FanoutBuilder<So, Tr, Mode = Unset> {
    id: Option<PipelineId>,
    source: So,
    transform: Tr,
    mode: PhantomData<Mode>,
}

impl<So, Tr> FanoutBuilder<So, Tr>
where
    So: Source,
    Tr: Transform<So::Payload, So::Position>,
{
    /// Give every sink its own owned clone of each batch.
    pub fn cloned(self) -> FanoutBuilder<So, Tr, Cloned> {
        FanoutBuilder {
            id: self.id,
            source: self.source,
            transform: self.transform,
            mode: PhantomData,
        }
    }

    /// Give every sink the same immutable, reference-counted batch.
    pub fn shared(self) -> FanoutBuilder<So, Tr, Shared> {
        FanoutBuilder {
            id: self.id,
            source: self.source,
            transform: self.transform,
            mode: PhantomData,
        }
    }
}

impl<So, Tr> FanoutBuilder<So, Tr, Cloned>
where
    So: Source,
    Tr: Transform<So::Payload, So::Position>,
    Tr::Out: Send + 'static,
{
    pub fn sinks<I>(self, sinks: I) -> FanoutPipeline<So, Tr, Cloned>
    where
        I: IntoIterator<Item = BoxSink<Tr::Out, So::Position, Cloned>>,
    {
        FanoutPipeline {
            id: self.id,
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
    Tr: Transform<So::Payload, So::Position>,
    Tr::Out: Send + Sync + 'static,
{
    pub fn sinks<I>(self, sinks: I) -> FanoutPipeline<So, Tr, Shared>
    where
        I: IntoIterator<Item = BoxSink<Tr::Out, So::Position, Shared>>,
    {
        FanoutPipeline {
            id: self.id,
            source: self.source,
            transform: self.transform,
            sinks: sinks.into_iter().collect(),
            strategy: Unset,
        }
    }
}

/// A pipeline that moves each transformed batch into one sink.
pub struct LinearPipeline<So, Tr, Si, St = Unset> {
    id: Option<PipelineId>,
    source: So,
    transform: Tr,
    sink: Si,
    strategy: St,
}

impl<So, Tr, Si> LinearPipeline<So, Tr, Si>
where
    So: Source,
    Tr: Transform<So::Payload, So::Position>,
{
    pub fn batched(self, policy: BatchPolicy) -> LinearPipeline<So, Tr, Si, Batched> {
        LinearPipeline {
            id: self.id,
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
    Tr: Transform<So::Payload, So::Position>,
    Si: Sink<Batch<Tr::Out, So::Position>>,
{
    /// Run until the source stream reaches its natural end.
    ///
    /// Sources that need graceful shutdown should incorporate their shutdown signal into the
    /// stream and return `None` only after any source-owned buffering has drained.
    pub async fn run(self) -> Result<(), PipelineError<So::Error, Tr::Error, Si::Error>> {
        self.run_until(std::future::pending()).await
    }

    /// Run until the source ends or `shutdown` requests an immediate source-polling cutoff.
    ///
    /// Records already admitted to the transform stage are drained before the final batch is
    /// delivered and committed.
    pub async fn run_until(
        self,
        shutdown: impl Future<Output = ()> + Send,
    ) -> Result<(), PipelineError<So::Error, Tr::Error, Si::Error>> {
        const TOPOLOGY: &str = "linear";
        let pipeline_id = self.id.clone().unwrap_or_else(PipelineId::unnamed);
        let _run = telemetry::RunGuard::new(TOPOLOGY, &pipeline_id);
        let transform = &self.transform;
        let records = self
            .source
            .stream()
            .take_until(shutdown)
            .map(|item| {
                let pipeline_id = pipeline_id.clone();
                async move {
                    let (position, payload) = item
                        .map_err(|error| {
                            telemetry::error(TOPOLOGY, &pipeline_id, "source");
                            PipelineError::Source(error)
                        })?
                        .into_parts();
                    let _timer = telemetry::StageTimer::new(TOPOLOGY, "transform", &pipeline_id);
                    let payload = transform.apply(&position, payload).await.map_err(|error| {
                        telemetry::error(TOPOLOGY, &pipeline_id, "transform");
                        PipelineError::Transform(error)
                    })?;
                    telemetry::record(TOPOLOGY, &pipeline_id);
                    Ok(Record::new(position, payload))
                }
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
            let Some((batch, checkpoint)) = Batch::from_chunk(chunk, So::track)? else {
                continue;
            };
            let reason = self.strategy.policy.emit_reason(batch.len());
            telemetry::batch(TOPOLOGY, &pipeline_id, batch.len(), reason);

            let delivery = {
                let _timer = telemetry::StageTimer::new(TOPOLOGY, "sink", &pipeline_id);
                self.sink.deliver(batch).await
            };
            telemetry::sink_delivery(TOPOLOGY, &pipeline_id, delivery.is_ok());
            delivery.map_err(|error| {
                telemetry::error(TOPOLOGY, &pipeline_id, "sink");
                PipelineError::Sink(DeliveryFailure::Sink(error))
            })?;

            let commit = {
                let _timer = telemetry::StageTimer::new(TOPOLOGY, "commit", &pipeline_id);
                self.source.commit(checkpoint).await
            };
            telemetry::commit(TOPOLOGY, &pipeline_id, commit.is_ok());
            commit.map_err(|error| {
                telemetry::error(TOPOLOGY, &pipeline_id, "commit");
                PipelineError::Commit(error)
            })?;
        }
        Ok(())
    }
}

/// A fanout pipeline whose erased sink input determines cloned or shared delivery.
pub struct FanoutPipeline<So, Tr, Mode, St = Unset>
where
    So: Source,
    Tr: Transform<So::Payload, So::Position>,
    Mode: FanoutMode<Tr::Out, So::Position>,
{
    id: Option<PipelineId>,
    source: So,
    transform: Tr,
    sinks: Vec<BoxSink<Tr::Out, So::Position, Mode>>,
    strategy: St,
}

impl<So, Tr, Mode, St> FanoutPipeline<So, Tr, Mode, St>
where
    So: Source,
    Tr: Transform<So::Payload, So::Position>,
    Mode: FanoutMode<Tr::Out, So::Position>,
{
    async fn drain(
        tasks: &mut JoinSet<Result<(), ErasedError>>,
        topology: &'static str,
        pipeline_id: &PipelineId,
    ) -> Vec<DeliveryFailure<ErasedError>> {
        let mut failures = Vec::new();
        while let Some(joined) = tasks.join_next().await {
            match joined {
                Ok(Ok(())) => telemetry::sink_delivery(topology, pipeline_id, true),
                Ok(Err(error)) => {
                    telemetry::sink_delivery(topology, pipeline_id, false);
                    telemetry::error(topology, pipeline_id, "sink");
                    failures.push(DeliveryFailure::Sink(error));
                }
                Err(error) => {
                    telemetry::sink_delivery(topology, pipeline_id, false);
                    telemetry::error(topology, pipeline_id, "sink_task");
                    failures.push(DeliveryFailure::Task(error));
                }
            }
        }
        failures
    }
}

impl<So, Tr, Mode> FanoutPipeline<So, Tr, Mode>
where
    So: Source,
    Tr: Transform<So::Payload, So::Position>,
    Mode: FanoutMode<Tr::Out, So::Position>,
{
    pub fn batched(self, policy: BatchPolicy) -> FanoutPipeline<So, Tr, Mode, Batched> {
        FanoutPipeline {
            id: self.id,
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
    Tr: Transform<So::Payload, So::Position>,
    Tr::Out: Clone + Send + 'static,
    So::Position: Clone,
{
    /// Run until the source stream reaches its natural end.
    pub async fn run(self) -> Result<(), PipelineError<So::Error, Tr::Error, ErasedError>> {
        self.run_until(std::future::pending()).await
    }

    /// Run until the source ends or `shutdown` requests an immediate source-polling cutoff.
    pub async fn run_until(
        self,
        shutdown: impl Future<Output = ()> + Send,
    ) -> Result<(), PipelineError<So::Error, Tr::Error, ErasedError>> {
        const TOPOLOGY: &str = "fanout_cloned";
        let pipeline_id = self.id.clone().unwrap_or_else(PipelineId::unnamed);
        let _run = telemetry::RunGuard::new(TOPOLOGY, &pipeline_id);
        if self.sinks.is_empty() {
            telemetry::error(TOPOLOGY, &pipeline_id, "configuration");
            return Err(PipelineError::NoSinks);
        }

        let transform = &self.transform;
        let records = self
            .source
            .stream()
            .take_until(shutdown)
            .map(|item| {
                let pipeline_id = pipeline_id.clone();
                async move {
                    let (position, payload) = item
                        .map_err(|error| {
                            telemetry::error(TOPOLOGY, &pipeline_id, "source");
                            PipelineError::Source(error)
                        })?
                        .into_parts();
                    let _timer = telemetry::StageTimer::new(TOPOLOGY, "transform", &pipeline_id);
                    let payload = transform.apply(&position, payload).await.map_err(|error| {
                        telemetry::error(TOPOLOGY, &pipeline_id, "transform");
                        PipelineError::Transform(error)
                    })?;
                    telemetry::record(TOPOLOGY, &pipeline_id);
                    Ok(Record::new(position, payload))
                }
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
            let Some((batch, checkpoint)) = Batch::from_chunk(chunk, So::track)? else {
                continue;
            };
            let reason = self.strategy.policy.emit_reason(batch.len());
            telemetry::batch(TOPOLOGY, &pipeline_id, batch.len(), reason);
            let mut tasks = JoinSet::new();
            let (last_sink, preceding_sinks) =
                self.sinks.split_last().ok_or(PipelineError::NoSinks)?;

            for sink in preceding_sinks {
                let sink = sink.clone();
                let batch = batch.clone();
                let delivery_pipeline_id = pipeline_id.clone();
                tasks.spawn(async move {
                    let _timer =
                        telemetry::StageTimer::new(TOPOLOGY, "sink", &delivery_pipeline_id);
                    sink.deliver(batch).await
                });
            }
            let sink = last_sink.clone();
            let delivery_pipeline_id = pipeline_id.clone();
            tasks.spawn(async move {
                let _timer = telemetry::StageTimer::new(TOPOLOGY, "sink", &delivery_pipeline_id);
                sink.deliver(batch).await
            });

            let failures = Self::drain(&mut tasks, TOPOLOGY, &pipeline_id).await;
            if !failures.is_empty() {
                return Err(PipelineError::Sinks(failures));
            }

            let commit = {
                let _timer = telemetry::StageTimer::new(TOPOLOGY, "commit", &pipeline_id);
                self.source.commit(checkpoint).await
            };
            telemetry::commit(TOPOLOGY, &pipeline_id, commit.is_ok());
            commit.map_err(|error| {
                telemetry::error(TOPOLOGY, &pipeline_id, "commit");
                PipelineError::Commit(error)
            })?;
        }
        Ok(())
    }
}

impl<So, Tr> FanoutPipeline<So, Tr, Shared, Batched>
where
    So: Source,
    Tr: Transform<So::Payload, So::Position>,
    Tr::Out: Send + Sync + 'static,
{
    /// Run until the source stream reaches its natural end.
    pub async fn run(self) -> Result<(), PipelineError<So::Error, Tr::Error, ErasedError>> {
        self.run_until(std::future::pending()).await
    }

    /// Run until the source ends or `shutdown` requests an immediate source-polling cutoff.
    pub async fn run_until(
        self,
        shutdown: impl Future<Output = ()> + Send,
    ) -> Result<(), PipelineError<So::Error, Tr::Error, ErasedError>> {
        const TOPOLOGY: &str = "fanout_shared";
        let pipeline_id = self.id.clone().unwrap_or_else(PipelineId::unnamed);
        let _run = telemetry::RunGuard::new(TOPOLOGY, &pipeline_id);
        if self.sinks.is_empty() {
            telemetry::error(TOPOLOGY, &pipeline_id, "configuration");
            return Err(PipelineError::NoSinks);
        }

        let transform = &self.transform;
        let records = self
            .source
            .stream()
            .take_until(shutdown)
            .map(|item| {
                let pipeline_id = pipeline_id.clone();
                async move {
                    let (position, payload) = item
                        .map_err(|error| {
                            telemetry::error(TOPOLOGY, &pipeline_id, "source");
                            PipelineError::Source(error)
                        })?
                        .into_parts();
                    let _timer = telemetry::StageTimer::new(TOPOLOGY, "transform", &pipeline_id);
                    let payload = transform.apply(&position, payload).await.map_err(|error| {
                        telemetry::error(TOPOLOGY, &pipeline_id, "transform");
                        PipelineError::Transform(error)
                    })?;
                    telemetry::record(TOPOLOGY, &pipeline_id);
                    Ok(Record::new(position, payload))
                }
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
            let Some((batch, checkpoint)) = Batch::from_chunk(chunk, So::track)? else {
                continue;
            };
            let reason = self.strategy.policy.emit_reason(batch.len());
            telemetry::batch(TOPOLOGY, &pipeline_id, batch.len(), reason);
            let batch: SharedBatch<Tr::Out, So::Position> = Arc::new(batch);
            let mut tasks = JoinSet::new();

            for sink in &self.sinks {
                let sink = sink.clone();
                let batch = Arc::clone(&batch);
                let delivery_pipeline_id = pipeline_id.clone();
                tasks.spawn(async move {
                    let _timer =
                        telemetry::StageTimer::new(TOPOLOGY, "sink", &delivery_pipeline_id);
                    sink.deliver(batch).await
                });
            }

            let failures = Self::drain(&mut tasks, TOPOLOGY, &pipeline_id).await;
            if !failures.is_empty() {
                return Err(PipelineError::Sinks(failures));
            }

            let commit = {
                let _timer = telemetry::StageTimer::new(TOPOLOGY, "commit", &pipeline_id);
                self.source.commit(checkpoint).await
            };
            telemetry::commit(TOPOLOGY, &pipeline_id, commit.is_ok());
            commit.map_err(|error| {
                telemetry::error(TOPOLOGY, &pipeline_id, "commit");
                PipelineError::Commit(error)
            })?;
        }
        Ok(())
    }
}
