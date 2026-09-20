use std::{future::Future, sync::Arc};

use futures_util::StreamExt;

use crate::{
    Batch, Collection, Collector, DeliveryFailure, PipelineError, PipelineId, Record, Sink, Source,
    Transform, telemetry,
};

use super::{
    BatchPolicy, Batched, CollectPolicy, Collected, CommitEach, CommitPolicy, Each, Unset,
    policy::{CommitState, commit_eligible},
};

/// A pipeline that moves each transformed batch into one sink.
pub struct LinearPipeline<So, Tr, Si, St = Unset> {
    pub(super) id: Option<PipelineId>,
    pub(super) source: So,
    pub(super) transform: Tr,
    pub(super) sink: Si,
    pub(super) strategy: St,
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
            strategy: Batched {
                policy,
                commit_policy: CommitEach,
            },
        }
    }

    pub fn each(self) -> LinearPipeline<So, Tr, Si, Each> {
        LinearPipeline {
            id: self.id,
            source: self.source,
            transform: self.transform,
            sink: self.sink,
            strategy: Each {
                commit_policy: CommitEach,
            },
        }
    }

    pub fn collect(self, policy: CollectPolicy) -> LinearPipeline<So, Tr, Si, Collected> {
        LinearPipeline {
            id: self.id,
            source: self.source,
            transform: self.transform,
            sink: self.sink,
            strategy: Collected {
                policy,
                commit_policy: CommitEach,
            },
        }
    }
}

impl<So, Tr, Si, C> LinearPipeline<So, Tr, Si, Batched<C>> {
    pub fn commit_policy<Next>(self, policy: Next) -> LinearPipeline<So, Tr, Si, Batched<Next>>
    where
        So: Source,
        Next: CommitPolicy<So::Position>,
    {
        LinearPipeline {
            id: self.id,
            source: self.source,
            transform: self.transform,
            sink: self.sink,
            strategy: Batched {
                policy: self.strategy.policy,
                commit_policy: policy,
            },
        }
    }
}

impl<So, Tr, Si, C> LinearPipeline<So, Tr, Si, Each<C>> {
    pub fn commit_policy<Next>(self, policy: Next) -> LinearPipeline<So, Tr, Si, Each<Next>>
    where
        So: Source,
        Next: CommitPolicy<So::Position>,
    {
        LinearPipeline {
            id: self.id,
            source: self.source,
            transform: self.transform,
            sink: self.sink,
            strategy: Each {
                commit_policy: policy,
            },
        }
    }
}

impl<So, Tr, Si, C> LinearPipeline<So, Tr, Si, Collected<C>> {
    pub fn commit_policy<Next>(self, policy: Next) -> LinearPipeline<So, Tr, Si, Collected<Next>>
    where
        So: Source,
        Next: CommitPolicy<So::Position>,
    {
        LinearPipeline {
            id: self.id,
            source: self.source,
            transform: self.transform,
            sink: self.sink,
            strategy: Collected {
                policy: self.strategy.policy,
                commit_policy: policy,
            },
        }
    }
}

impl<So, Tr, Si, C> LinearPipeline<So, Tr, Si, Batched<C>>
where
    So: Source,
    Tr: Transform<So::Payload, So::Position>,
    Si: Sink<Batch<Tr::Out, So::Position>>,
    C: CommitPolicy<So::Position>,
{
    const TOPOLOGY: &str = "linear";
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
        let pipeline_id = self.id.clone().unwrap_or_else(PipelineId::unnamed);
        let _run = telemetry::RunGuard::new(Self::TOPOLOGY, &pipeline_id);
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
        let sink = &self.sink;

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
                        let delivery = {
                            let _timer =
                                telemetry::StageTimer::new(Self::TOPOLOGY, "sink", &pipeline_id);
                            sink.deliver(batch).await
                        };
                        telemetry::sink_delivery(Self::TOPOLOGY, &pipeline_id, delivery.is_ok());
                        delivery.map_err(|error| {
                            telemetry::error(Self::TOPOLOGY, &pipeline_id, "sink");
                            PipelineError::Sink(DeliveryFailure::Sink(error))
                        })?;

                        commits.acknowledge_delivery(records);
                        let commit = {
                            let _timer =
                                telemetry::StageTimer::new(Self::TOPOLOGY, "commit", &pipeline_id);
                            commit_eligible(&mut commits, source).await
                        };
                        if matches!(commit, Ok(true)) {
                            telemetry::commit(Self::TOPOLOGY, &pipeline_id, true);
                        }
                        commit.map_err(|error| {
                            telemetry::commit(Self::TOPOLOGY, &pipeline_id, false);
                            telemetry::error(Self::TOPOLOGY, &pipeline_id, "commit");
                            PipelineError::Commit(error)
                        })?;
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
                    let pipeline_id = pipeline_id.clone();
                    move || {
                        let commits = Arc::clone(&commits);
                        let pipeline_id = pipeline_id.clone();
                        async move {
                            let mut commits = commits.lock().await;
                            commits.handle_timeout();
                            let result = commit_eligible(&mut commits, source).await;
                            telemetry::commit(Self::TOPOLOGY, &pipeline_id, result.is_ok());
                            result.map(|_| ()).map_err(PipelineError::Commit)
                        }
                    }
                },
            )
            .await?;

        let mut commits = commits.lock().await;
        commits.finish();
        let commit = commit_eligible(&mut commits, source).await;
        if matches!(commit, Ok(true)) {
            telemetry::commit(Self::TOPOLOGY, &pipeline_id, true);
        }
        commit.map(|_| ()).map_err(|error| {
            telemetry::commit(Self::TOPOLOGY, &pipeline_id, false);
            telemetry::error(Self::TOPOLOGY, &pipeline_id, "commit");
            PipelineError::Commit(error)
        })
    }
}

impl<So, Tr, Si, C> LinearPipeline<So, Tr, Si, Each<C>>
where
    So: Source,
    Tr: Transform<So::Payload, So::Position>,
    Si: Sink<Record<Tr::Out, So::Position>>,
    C: CommitPolicy<So::Position>,
{
    const TOPOLOGY: &str = "linear";
    pub async fn run(self) -> Result<(), PipelineError<So::Error, Tr::Error, Si::Error>> {
        self.run_until(std::future::pending()).await
    }

    pub async fn run_until(
        self,
        shutdown: impl Future<Output = ()> + Send,
    ) -> Result<(), PipelineError<So::Error, Tr::Error, Si::Error>> {
        let pipeline_id = self.id.clone().unwrap_or_else(PipelineId::unnamed);
        let _run = telemetry::RunGuard::new(Self::TOPOLOGY, &pipeline_id);
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
        let mut commits = CommitState::new(self.strategy.commit_policy);

        loop {
            let next = if let Some(deadline) = commits.deadline() {
                tokio::select! {
                    item = records.next() => item,
                    () = tokio::time::sleep_until(deadline) => {
                        commits.handle_timeout();
                        let commit = commit_eligible(&mut commits, &self.source).await;
                        telemetry::commit(Self::TOPOLOGY, &pipeline_id, commit.is_ok());
                        commit.map_err(|error| {
                            telemetry::error(Self::TOPOLOGY, &pipeline_id, "commit");
                            PipelineError::Commit(error)
                        })?;
                        continue;
                    }
                }
            } else {
                records.next().await
            };
            let Some(record) = next else { break };
            let record = record?;
            commits.track(record.position());
            let delivery = {
                let _timer = telemetry::StageTimer::new(Self::TOPOLOGY, "sink", &pipeline_id);
                self.sink.deliver(record).await
            };
            telemetry::sink_delivery(Self::TOPOLOGY, &pipeline_id, delivery.is_ok());
            delivery.map_err(|error| {
                telemetry::error(Self::TOPOLOGY, &pipeline_id, "sink");
                PipelineError::Sink(DeliveryFailure::Sink(error))
            })?;
            commits.acknowledge_delivery(1);
            let commit = commit_eligible(&mut commits, &self.source).await;
            if matches!(commit, Ok(true)) {
                telemetry::commit(Self::TOPOLOGY, &pipeline_id, true);
            }
            commit.map_err(|error| {
                telemetry::commit(Self::TOPOLOGY, &pipeline_id, false);
                telemetry::error(Self::TOPOLOGY, &pipeline_id, "commit");
                PipelineError::Commit(error)
            })?;
        }

        commits.finish();
        let commit = commit_eligible(&mut commits, &self.source).await;
        if matches!(commit, Ok(true)) {
            telemetry::commit(Self::TOPOLOGY, &pipeline_id, true);
        }
        commit.map(|_| ()).map_err(|error| {
            telemetry::commit(Self::TOPOLOGY, &pipeline_id, false);
            telemetry::error(Self::TOPOLOGY, &pipeline_id, "commit");
            PipelineError::Commit(error)
        })
    }
}

impl<So, Tr, Si, C> LinearPipeline<So, Tr, Si, Collected<C>>
where
    So: Source,
    Tr: Transform<So::Payload, So::Position>,
    Si: Collector<Record<Tr::Out, So::Position>>,
    C: CommitPolicy<So::Position>,
{
    const TOPOLOGY: &str = "linear";
    pub async fn run(self) -> Result<(), PipelineError<So::Error, Tr::Error, Si::Error>> {
        self.run_until(std::future::pending()).await
    }

    pub async fn run_until(
        self,
        shutdown: impl Future<Output = ()> + Send,
    ) -> Result<(), PipelineError<So::Error, Tr::Error, Si::Error>> {
        let pipeline_id = self.id.clone().unwrap_or_else(PipelineId::unnamed);
        let _run = telemetry::RunGuard::new(Self::TOPOLOGY, &pipeline_id);
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
        let mut commits = CommitState::new(self.strategy.commit_policy);
        let mut next = None;

        loop {
            let first = if let Some(record) = next.take() {
                Some(record)
            } else if let Some(deadline) = commits.deadline() {
                tokio::select! {
                    item = records.next() => item,
                    () = tokio::time::sleep_until(deadline) => {
                        commits.handle_timeout();
                        let commit = commit_eligible(&mut commits, &self.source).await;
                        telemetry::commit(Self::TOPOLOGY, &pipeline_id, commit.is_ok());
                        commit.map_err(PipelineError::Commit)?;
                        continue;
                    }
                }
            } else {
                records.next().await
            };
            let Some(first) = first else { break };

            let mut session = self.sink.begin().await.map_err(|error| {
                telemetry::error(Self::TOPOLOGY, &pipeline_id, "sink");
                PipelineError::Sink(DeliveryFailure::Sink(error))
            })?;
            let deadline = tokio::time::Instant::now() + self.strategy.policy.timeout();
            let mut count = 0;
            let mut current = Some(first?);
            let mut ended = false;

            loop {
                if let Some(record) = current.take() {
                    commits.track(record.position());
                    session.push(record).await.map_err(|error| {
                        telemetry::error(Self::TOPOLOGY, &pipeline_id, "sink");
                        PipelineError::Sink(DeliveryFailure::Sink(error))
                    })?;
                    count += 1;
                    if count == self.strategy.policy.size().get() {
                        break;
                    }
                }

                tokio::select! {
                    biased;
                    item = records.next() => match item {
                        Some(record) => current = Some(record?),
                        None => {
                            ended = true;
                            break;
                        }
                    },
                    () = tokio::time::sleep_until(deadline) => break,
                }
            }

            let delivery = session.finish().await;
            telemetry::sink_delivery(Self::TOPOLOGY, &pipeline_id, delivery.is_ok());
            delivery.map_err(|error| {
                telemetry::error(Self::TOPOLOGY, &pipeline_id, "sink");
                PipelineError::Sink(DeliveryFailure::Sink(error))
            })?;
            telemetry::batch(
                Self::TOPOLOGY,
                &pipeline_id,
                count,
                if count == self.strategy.policy.size().get() {
                    "full"
                } else {
                    "timeout"
                },
            );
            commits.acknowledge_delivery(count);
            if commit_eligible(&mut commits, &self.source)
                .await
                .map_err(PipelineError::Commit)?
            {
                telemetry::commit(Self::TOPOLOGY, &pipeline_id, true);
            }
            if ended {
                break;
            }
        }

        commits.finish();
        commit_eligible(&mut commits, &self.source)
            .await
            .map(|_| ())
            .map_err(PipelineError::Commit)
    }
}
