use std::{future::Future, sync::Arc};

use futures_util::StreamExt;
use tokio::task::JoinSet;

use crate::{
    Batch, Cloned, ErasedError, PipelineError, PipelineId, Record, Shared, SharedBatch, Source,
    Transform, telemetry,
};

use super::FanoutPipeline;
use crate::pipeline::{
    Batched, CommitPolicy,
    policy::{CommitState, commit_eligible},
};

impl<So, Tr, C> FanoutPipeline<So, Tr, Cloned, Batch<Tr::Out, So::Position>, Batched<C>>
where
    So: Source,
    Tr: Transform<So::Payload, So::Position>,
    Tr::Out: Clone + Send + 'static,
    So::Position: Clone,
    C: CommitPolicy<So::Position>,
{
    const TOPOLOGY: &str = "fanout_cloned";
    /// Run until the source stream reaches its natural end.
    pub async fn run(self) -> Result<(), PipelineError<So::Error, Tr::Error, ErasedError>> {
        self.run_until(std::future::pending()).await
    }

    /// Run until the source ends or `shutdown` requests an immediate source-polling cutoff.
    pub async fn run_until(
        self,
        shutdown: impl Future<Output = ()> + Send,
    ) -> Result<(), PipelineError<So::Error, Tr::Error, ErasedError>> {
        let pipeline_id = self.id.clone().unwrap_or_else(PipelineId::unnamed);
        let _run = telemetry::RunGuard::new(Self::TOPOLOGY, &pipeline_id);
        if self.sinks.is_empty() {
            telemetry::error(Self::TOPOLOGY, &pipeline_id, "configuration");
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
                            telemetry::error(Self::TOPOLOGY, &pipeline_id, "source");
                            PipelineError::Source(error)
                        })?
                        .into_parts();
                    let _timer =
                        telemetry::StageTimer::new(Self::TOPOLOGY, "transform", &pipeline_id);
                    let payload = transform.apply(&position, payload).await.map_err(|error| {
                        telemetry::error(Self::TOPOLOGY, &pipeline_id, "transform");
                        PipelineError::Transform(error)
                    })?;
                    telemetry::record(Self::TOPOLOGY, &pipeline_id);
                    Ok(Record::new(position, payload))
                }
            })
            .buffered(transform.max_concurrency().get());
        tokio::pin!(records);
        let chunks = tokio_stream::StreamExt::chunks_timeout(
            records,
            self.strategy.policy.size().get(),
            self.strategy.policy.timeout(),
        );
        tokio::pin!(chunks);

        let batches = chunks.map(Batch::try_from_chunk);
        let batch_policy = self.strategy.policy;
        let commits = Arc::new(tokio::sync::Mutex::new(CommitState::new(
            self.strategy.commit_policy,
        )));
        let source = &self.source;
        let sinks = &self.sinks;

        batch_policy
            .consume(
                batches,
                |batch| {
                    let commits = Arc::clone(&commits);
                    let pipeline_id = pipeline_id.clone();
                    async move {
                        let mut commits = commits.lock().await;
                        let reason = batch_policy.emit_reason(batch.len());
                        telemetry::batch(Self::TOPOLOGY, &pipeline_id, batch.len(), reason);
                        for record in batch.iter() {
                            commits.track(record.position());
                        }
                        let records = batch.len();
                        let mut tasks = JoinSet::new();
                        let (last_sink, preceding_sinks) =
                            sinks.split_last().ok_or(PipelineError::NoSinks)?;

                        for sink in preceding_sinks {
                            let sink = sink.clone();
                            let batch = batch.clone();
                            let delivery_pipeline_id = pipeline_id.clone();
                            tasks.spawn(async move {
                                let _timer = telemetry::StageTimer::new(
                                    Self::TOPOLOGY,
                                    "sink",
                                    &delivery_pipeline_id,
                                );
                                sink.deliver(batch).await
                            });
                        }
                        let sink = last_sink.clone();
                        let delivery_pipeline_id = pipeline_id.clone();
                        tasks.spawn(async move {
                            let _timer = telemetry::StageTimer::new(
                                Self::TOPOLOGY,
                                "sink",
                                &delivery_pipeline_id,
                            );
                            sink.deliver(batch).await
                        });

                        let failures = Self::drain(&mut tasks, Self::TOPOLOGY, &pipeline_id).await;
                        if !failures.is_empty() {
                            return Err(PipelineError::Sinks(failures));
                        }

                        commits.acknowledge_delivery(records);
                        if commit_eligible(&mut commits, source)
                            .await
                            .map_err(PipelineError::Commit)?
                        {
                            telemetry::commit(Self::TOPOLOGY, &pipeline_id, true);
                        }
                        Ok(())
                    }
                },
                {
                    let commits = Arc::clone(&commits);
                    move || {
                        commits
                            .try_lock()
                            .ok()
                            .and_then(|commits| commits.deadline())
                    }
                },
                {
                    let commits = Arc::clone(&commits);
                    move || {
                        let commits = Arc::clone(&commits);
                        async move {
                            let mut commits = commits.lock().await;
                            commits.handle_timeout();
                            commit_eligible(&mut commits, source)
                                .await
                                .map(|_| ())
                                .map_err(PipelineError::Commit)
                        }
                    }
                },
            )
            .await?;
        let mut commits = commits.lock().await;
        commits.finish();
        commit_eligible(&mut commits, source)
            .await
            .map(|_| ())
            .map_err(PipelineError::Commit)
    }
}

impl<So, Tr, C> FanoutPipeline<So, Tr, Shared, SharedBatch<Tr::Out, So::Position>, Batched<C>>
where
    So: Source,
    Tr: Transform<So::Payload, So::Position>,
    Tr::Out: Send + Sync + 'static,
    C: CommitPolicy<So::Position>,
{
    const TOPOLOGY: &str = "fanout_shared";
    /// Run until the source stream reaches its natural end.
    pub async fn run(self) -> Result<(), PipelineError<So::Error, Tr::Error, ErasedError>> {
        self.run_until(std::future::pending()).await
    }

    /// Run until the source ends or `shutdown` requests an immediate source-polling cutoff.
    pub async fn run_until(
        self,
        shutdown: impl Future<Output = ()> + Send,
    ) -> Result<(), PipelineError<So::Error, Tr::Error, ErasedError>> {
        let pipeline_id = self.id.clone().unwrap_or_else(PipelineId::unnamed);
        let _run = telemetry::RunGuard::new(Self::TOPOLOGY, &pipeline_id);
        if self.sinks.is_empty() {
            telemetry::error(Self::TOPOLOGY, &pipeline_id, "configuration");
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
                            telemetry::error(Self::TOPOLOGY, &pipeline_id, "source");
                            PipelineError::Source(error)
                        })?
                        .into_parts();
                    let _timer =
                        telemetry::StageTimer::new(Self::TOPOLOGY, "transform", &pipeline_id);
                    let payload = transform.apply(&position, payload).await.map_err(|error| {
                        telemetry::error(Self::TOPOLOGY, &pipeline_id, "transform");
                        PipelineError::Transform(error)
                    })?;
                    telemetry::record(Self::TOPOLOGY, &pipeline_id);
                    Ok(Record::new(position, payload))
                }
            })
            .buffered(transform.max_concurrency().get());
        tokio::pin!(records);
        let chunks = tokio_stream::StreamExt::chunks_timeout(
            records,
            self.strategy.policy.size().get(),
            self.strategy.policy.timeout(),
        );
        tokio::pin!(chunks);

        let batches = chunks.map(Batch::try_from_chunk);
        let batch_policy = self.strategy.policy;
        let commits = Arc::new(tokio::sync::Mutex::new(CommitState::new(
            self.strategy.commit_policy,
        )));
        let source = &self.source;
        let sinks = &self.sinks;

        batch_policy
            .consume(
                batches,
                |batch| {
                    let commits = Arc::clone(&commits);
                    let pipeline_id = pipeline_id.clone();
                    async move {
                        let mut commits = commits.lock().await;
                        let reason = batch_policy.emit_reason(batch.len());
                        telemetry::batch(Self::TOPOLOGY, &pipeline_id, batch.len(), reason);
                        for record in batch.iter() {
                            commits.track(record.position());
                        }
                        let records = batch.len();
                        let batch: SharedBatch<Tr::Out, So::Position> = Arc::new(batch);
                        let mut tasks = JoinSet::new();

                        for sink in sinks {
                            let sink = sink.clone();
                            let batch = Arc::clone(&batch);
                            let delivery_pipeline_id = pipeline_id.clone();
                            tasks.spawn(async move {
                                let _timer = telemetry::StageTimer::new(
                                    Self::TOPOLOGY,
                                    "sink",
                                    &delivery_pipeline_id,
                                );
                                sink.deliver(batch).await
                            });
                        }

                        let failures = Self::drain(&mut tasks, Self::TOPOLOGY, &pipeline_id).await;
                        if !failures.is_empty() {
                            return Err(PipelineError::Sinks(failures));
                        }

                        commits.acknowledge_delivery(records);
                        if commit_eligible(&mut commits, source)
                            .await
                            .map_err(PipelineError::Commit)?
                        {
                            telemetry::commit(Self::TOPOLOGY, &pipeline_id, true);
                        }
                        Ok(())
                    }
                },
                {
                    let commits = Arc::clone(&commits);
                    move || {
                        commits
                            .try_lock()
                            .ok()
                            .and_then(|commits| commits.deadline())
                    }
                },
                {
                    let commits = Arc::clone(&commits);
                    move || {
                        let commits = Arc::clone(&commits);
                        async move {
                            let mut commits = commits.lock().await;
                            commits.handle_timeout();
                            commit_eligible(&mut commits, source)
                                .await
                                .map(|_| ())
                                .map_err(PipelineError::Commit)
                        }
                    }
                },
            )
            .await?;
        let mut commits = commits.lock().await;
        commits.finish();
        commit_eligible(&mut commits, source)
            .await
            .map(|_| ())
            .map_err(PipelineError::Commit)
    }
}
