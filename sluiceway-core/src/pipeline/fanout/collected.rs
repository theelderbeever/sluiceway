use std::{future::Future, sync::Arc};

use futures_util::StreamExt;
use tokio::task::JoinSet;

use crate::{
    BoxedCollector, Cloned, DeliveryFailure, ErasedError, PipelineError, PipelineId, Record,
    Shared, SharedRecord, Source, Transform, sink::BoxedCollection, telemetry,
};

use super::FanoutCollectorPipeline;
use crate::pipeline::{Collected, policy::CommitState};

enum Deadline<T> {
    Ready(T),
    Elapsed,
}

async fn next_or_commit_deadline<T>(
    next: impl Future<Output = T>,
    deadline: Option<tokio::time::Instant>,
) -> Deadline<T> {
    let Some(deadline) = deadline else {
        return Deadline::Ready(next.await);
    };

    tokio::select! {
        item = next => Deadline::Ready(item),
        () = tokio::time::sleep_until(deadline) => Deadline::Elapsed,
    }
}

async fn next_or_collection_deadline<T>(
    next: impl Future<Output = T>,
    deadline: tokio::time::Instant,
) -> Deadline<T> {
    tokio::select! {
        biased;
        item = next => Deadline::Ready(item),
        () = tokio::time::sleep_until(deadline) => Deadline::Elapsed,
    }
}

async fn begin_collections<Input: Send + 'static>(
    collectors: &[BoxedCollector<Input>],
) -> Result<Vec<BoxedCollection<Input>>, Vec<DeliveryFailure<ErasedError>>> {
    let mut collections = Vec::with_capacity(collectors.len());
    let mut failures = Vec::new();
    for collector in collectors {
        match collector.begin().await {
            Ok(collection) => collections.push(collection),
            Err(error) => failures.push(DeliveryFailure::Sink(error)),
        }
    }
    if failures.is_empty() {
        Ok(collections)
    } else {
        Err(failures)
    }
}

async fn finish_collections<Input: Send + 'static>(
    collections: Vec<BoxedCollection<Input>>,
    topology: &'static str,
    pipeline_id: &PipelineId,
) -> Result<(), Vec<DeliveryFailure<ErasedError>>> {
    let mut tasks = JoinSet::new();
    for collection in collections {
        tasks.spawn(collection.finish());
    }

    let mut failures = Vec::new();
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(Ok(())) => telemetry::sink_delivery(topology, pipeline_id, true),
            Ok(Err(error)) => failures.push(DeliveryFailure::Sink(error)),
            Err(error) => failures.push(DeliveryFailure::Task(error)),
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures)
    }
}

impl<So, Tr> FanoutCollectorPipeline<So, Tr, Cloned, Record<Tr::Out, So::Position>, Collected>
where
    So: Source,
    Tr: Transform<So::Payload, So::Position>,
    Tr::Out: Clone + Send + 'static,
    So::Position: Clone,
{
    const TOPOLOGY: &str = "fanout_cloned";
    pub async fn run(self) -> Result<(), PipelineError<So::Error, Tr::Error, ErasedError>> {
        self.run_until(std::future::pending()).await
    }

    pub async fn run_until(
        self,
        shutdown: impl Future<Output = ()> + Send,
    ) -> Result<(), PipelineError<So::Error, Tr::Error, ErasedError>> {
        let pipeline_id = self.id.clone().unwrap_or_else(PipelineId::unnamed);
        let _run = telemetry::RunGuard::new(Self::TOPOLOGY, &pipeline_id);
        if self.collectors.is_empty() {
            return Err(PipelineError::NoSinks);
        }
        let transform = &self.transform;
        let records = self
            .source
            .stream()
            .take_until(shutdown)
            .map(|item| async move {
                let (position, payload) = item.map_err(PipelineError::Source)?.into_parts();
                let payload = transform
                    .apply(&position, payload)
                    .await
                    .map_err(PipelineError::Transform)?;
                Ok(Record::new(position, payload))
            })
            .buffered(transform.max_concurrency().get());
        tokio::pin!(records);
        let mut commits = CommitState::new(self.strategy.commit);

        loop {
            let first = match next_or_commit_deadline(records.next(), commits.deadline()).await {
                Deadline::Ready(first) => first,
                Deadline::Elapsed => {
                    commits
                        .commit(&self.source)
                        .await
                        .map_err(PipelineError::Commit)?;
                    telemetry::commit(Self::TOPOLOGY, &pipeline_id, true);
                    continue;
                }
            };
            let Some(first) = first else {
                break;
            };
            let mut collections = begin_collections(&self.collectors)
                .await
                .map_err(PipelineError::Sinks)?;

            let collection_deadline = tokio::time::Instant::now() + self.strategy.policy.timeout();
            let mut current = Some(first?);
            let mut count = 0;
            let mut ended = false;
            loop {
                if let Some(record) = current.take() {
                    commits.track::<So>(record.position());
                    let inputs = (0..collections.len())
                        .map(|_| record.clone())
                        .collect::<Vec<_>>();
                    let results = futures_util::future::join_all(
                        collections
                            .iter_mut()
                            .zip(inputs)
                            .map(|(collection, input)| collection.push(input)),
                    )
                    .await;
                    let failures = results
                        .into_iter()
                        .filter_map(|result| result.err().map(DeliveryFailure::Sink))
                        .collect::<Vec<_>>();
                    if !failures.is_empty() {
                        return Err(PipelineError::Sinks(failures));
                    }
                    count += 1;
                    if count == self.strategy.policy.size().get() {
                        break;
                    }
                }
                match next_or_collection_deadline(records.next(), collection_deadline).await {
                    Deadline::Ready(item) => match item {
                        Some(record) => current = Some(record?),
                        None => {
                            ended = true;
                            break;
                        }
                    },
                    Deadline::Elapsed => break,
                }
            }

            finish_collections(collections, Self::TOPOLOGY, &pipeline_id)
                .await
                .map_err(PipelineError::Sinks)?;
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

impl<So, Tr> FanoutCollectorPipeline<So, Tr, Shared, SharedRecord<Tr::Out, So::Position>, Collected>
where
    So: Source,
    Tr: Transform<So::Payload, So::Position>,
    Tr::Out: Send + Sync + 'static,
{
    const TOPOLOGY: &str = "fanout_shared";
    pub async fn run(self) -> Result<(), PipelineError<So::Error, Tr::Error, ErasedError>> {
        self.run_until(std::future::pending()).await
    }

    pub async fn run_until(
        self,
        shutdown: impl Future<Output = ()> + Send,
    ) -> Result<(), PipelineError<So::Error, Tr::Error, ErasedError>> {
        let pipeline_id = self.id.clone().unwrap_or_else(PipelineId::unnamed);
        let _run = telemetry::RunGuard::new(Self::TOPOLOGY, &pipeline_id);
        if self.collectors.is_empty() {
            return Err(PipelineError::NoSinks);
        }
        let transform = &self.transform;
        let records = self
            .source
            .stream()
            .take_until(shutdown)
            .map(|item| async move {
                let (position, payload) = item.map_err(PipelineError::Source)?.into_parts();
                let payload = transform
                    .apply(&position, payload)
                    .await
                    .map_err(PipelineError::Transform)?;
                Ok(Record::new(position, payload))
            })
            .buffered(transform.max_concurrency().get());
        tokio::pin!(records);
        let mut commits = CommitState::new(self.strategy.commit);

        loop {
            let first = match next_or_commit_deadline(records.next(), commits.deadline()).await {
                Deadline::Ready(first) => first,
                Deadline::Elapsed => {
                    commits
                        .commit(&self.source)
                        .await
                        .map_err(PipelineError::Commit)?;
                    telemetry::commit(Self::TOPOLOGY, &pipeline_id, true);
                    continue;
                }
            };
            let Some(first) = first else {
                break;
            };
            let mut collections = begin_collections(&self.collectors)
                .await
                .map_err(PipelineError::Sinks)?;
            let collection_deadline = tokio::time::Instant::now() + self.strategy.policy.timeout();
            let mut current = Some(first?);
            let mut count = 0;
            let mut ended = false;
            loop {
                if let Some(record) = current.take() {
                    let record = Arc::new(record);
                    commits.track::<So>(record.position());
                    let results = futures_util::future::join_all(
                        collections
                            .iter_mut()
                            .map(|collection| collection.push(Arc::clone(&record))),
                    )
                    .await;
                    let failures = results
                        .into_iter()
                        .filter_map(|result| result.err().map(DeliveryFailure::Sink))
                        .collect::<Vec<_>>();
                    if !failures.is_empty() {
                        return Err(PipelineError::Sinks(failures));
                    }
                    count += 1;
                    if count == self.strategy.policy.size().get() {
                        break;
                    }
                }
                match next_or_collection_deadline(records.next(), collection_deadline).await {
                    Deadline::Ready(item) => match item {
                        Some(record) => current = Some(record?),
                        None => {
                            ended = true;
                            break;
                        }
                    },
                    Deadline::Elapsed => break,
                }
            }
            finish_collections(collections, Self::TOPOLOGY, &pipeline_id)
                .await
                .map_err(PipelineError::Sinks)?;
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
