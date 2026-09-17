use std::{future::Future, sync::Arc};

use futures_util::StreamExt;

use crate::{
    Batch, Collection, Collector, DeliveryFailure, PipelineError, PipelineId, Record, Sink, Source,
    Transform, telemetry,
};

use super::{
    BatchPolicy, Batched, CollectPolicy, Collected, CommitPolicy, Each, Unset, policy::CommitState,
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
                commit: CommitPolicy::default(),
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
                commit: CommitPolicy::default(),
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
                commit: CommitPolicy::default(),
            },
        }
    }
}

impl<So, Tr, Si> LinearPipeline<So, Tr, Si, Batched> {
    pub fn commit_policy(mut self, policy: CommitPolicy) -> Self {
        self.strategy.commit = policy;
        self
    }
}

impl<So, Tr, Si> LinearPipeline<So, Tr, Si, Each> {
    pub fn commit_policy(mut self, policy: CommitPolicy) -> Self {
        self.strategy.commit = policy;
        self
    }
}

impl<So, Tr, Si> LinearPipeline<So, Tr, Si, Collected> {
    pub fn commit_policy(mut self, policy: CommitPolicy) -> Self {
        self.strategy.commit = policy;
        self
    }
}

impl<So, Tr, Si> LinearPipeline<So, Tr, Si, Batched>
where
    So: Source,
    Tr: Transform<So::Payload, So::Position>,
    Si: Sink<Batch<Tr::Out, So::Position>>,
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

        let commits = Arc::new(tokio::sync::Mutex::new(CommitState::new(
            self.strategy.commit,
        )));
        let source = &self.source;
        let sink = &self.sink;
        let strategy = &self.strategy;

        strategy
            .consume(
                batches,
                |batch| {
                    let commits = Arc::clone(&commits);
                    let pipeline_id = pipeline_id.clone();
                    async move {
                        let mut commits = commits.lock().await;
                        let reason = strategy.policy.emit_reason(batch.len());
                        telemetry::batch(Self::TOPOLOGY, &pipeline_id, batch.len(), reason);
                        for record in batch.iter() {
                            commits.track::<So>(record.position());
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

                        commits.acknowledge(records);
                        if commits.due() {
                            let commit = {
                                let _timer = telemetry::StageTimer::new(
                                    Self::TOPOLOGY,
                                    "commit",
                                    &pipeline_id,
                                );
                                commits.commit(source).await
                            };
                            telemetry::commit(Self::TOPOLOGY, &pipeline_id, commit.is_ok());
                            commit.map_err(|error| {
                                telemetry::error(Self::TOPOLOGY, &pipeline_id, "commit");
                                PipelineError::Commit(error)
                            })?;
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
                    let pipeline_id = pipeline_id.clone();
                    move || {
                        let commits = Arc::clone(&commits);
                        let pipeline_id = pipeline_id.clone();
                        async move {
                            let result = commits.lock().await.commit(source).await;
                            telemetry::commit(Self::TOPOLOGY, &pipeline_id, result.is_ok());
                            result.map(|_| ()).map_err(PipelineError::Commit)
                        }
                    }
                },
            )
            .await?;

        let mut commits = commits.lock().await;
        let commit = commits.commit(source).await;
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

impl<So, Tr, Si> LinearPipeline<So, Tr, Si, Each>
where
    So: Source,
    Tr: Transform<So::Payload, So::Position>,
    Si: Sink<Record<Tr::Out, So::Position>>,
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
        let mut commits = CommitState::new(self.strategy.commit);

        loop {
            let next = if let Some(deadline) = commits.deadline() {
                tokio::select! {
                    item = records.next() => item,
                    () = tokio::time::sleep_until(deadline) => {
                        let commit = commits.commit(&self.source).await;
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
            commits.track::<So>(record.position());
            let delivery = {
                let _timer = telemetry::StageTimer::new(Self::TOPOLOGY, "sink", &pipeline_id);
                self.sink.deliver(record).await
            };
            telemetry::sink_delivery(Self::TOPOLOGY, &pipeline_id, delivery.is_ok());
            delivery.map_err(|error| {
                telemetry::error(Self::TOPOLOGY, &pipeline_id, "sink");
                PipelineError::Sink(DeliveryFailure::Sink(error))
            })?;
            commits.acknowledge(1);
            if commits.due() {
                let commit = commits.commit(&self.source).await;
                telemetry::commit(Self::TOPOLOGY, &pipeline_id, commit.is_ok());
                commit.map_err(|error| {
                    telemetry::error(Self::TOPOLOGY, &pipeline_id, "commit");
                    PipelineError::Commit(error)
                })?;
            }
        }

        let commit = commits.commit(&self.source).await;
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

impl<So, Tr, Si> LinearPipeline<So, Tr, Si, Collected>
where
    So: Source,
    Tr: Transform<So::Payload, So::Position>,
    Si: Collector<Record<Tr::Out, So::Position>>,
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
        let mut commits = CommitState::new(self.strategy.commit);
        let mut next = None;

        loop {
            let first = if let Some(record) = next.take() {
                Some(record)
            } else if let Some(deadline) = commits.deadline() {
                tokio::select! {
                    item = records.next() => item,
                    () = tokio::time::sleep_until(deadline) => {
                        let commit = commits.commit(&self.source).await;
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
                    commits.track::<So>(record.position());
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
            commits.acknowledge(count);
            if commits.due() {
                commits
                    .commit(&self.source)
                    .await
                    .map_err(PipelineError::Commit)?;
                telemetry::commit(Self::TOPOLOGY, &pipeline_id, true);
            }
            if ended {
                break;
            }
        }

        commits
            .commit(&self.source)
            .await
            .map(|_| ())
            .map_err(PipelineError::Commit)
    }
}
