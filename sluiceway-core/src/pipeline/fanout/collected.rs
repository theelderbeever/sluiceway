use std::{future::Future, sync::Arc};

use futures_util::StreamExt;
use tokio::task::JoinSet;

use crate::{
    Cloned, DeliveryFailure, ErasedError, PipelineError, PipelineId, Record, Shared, SharedRecord,
    Source, Transform, sink::BoxedCollectionSession, telemetry,
};

use super::FanoutCollectorPipeline;
use crate::pipeline::{Collected, policy::CommitState};

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
            let first = if let Some(deadline) = commits.deadline() {
                tokio::select! {
                    item = records.next() => item,
                    () = tokio::time::sleep_until(deadline) => {
                        commits.commit(&self.source).await.map_err(PipelineError::Commit)?;
                        telemetry::commit(Self::TOPOLOGY, &pipeline_id, true);
                        continue;
                    }
                }
            } else {
                records.next().await
            };
            let Some(first) = first else {
                break;
            };
            let mut sessions = Vec::with_capacity(self.collectors.len());
            let mut begin_failures = Vec::new();
            for collector in &self.collectors {
                match collector.begin().await {
                    Ok(session) => sessions.push(session),
                    Err(error) => begin_failures.push(DeliveryFailure::Sink(error)),
                }
            }
            if !begin_failures.is_empty() {
                return Err(PipelineError::Sinks(begin_failures));
            }

            let deadline = tokio::time::Instant::now() + self.strategy.policy.timeout();
            let mut current = Some(first?);
            let mut count = 0;
            let mut ended = false;
            loop {
                if let Some(record) = current.take() {
                    commits.track::<So>(record.position());
                    let inputs = (0..sessions.len())
                        .map(|_| record.clone())
                        .collect::<Vec<_>>();
                    let results = futures_util::future::join_all(
                        sessions
                            .iter_mut()
                            .zip(inputs)
                            .map(|(session, input)| session.push(input)),
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
                tokio::select! {
                    biased;
                    item = records.next() => match item {
                        Some(record) => current = Some(record?),
                        None => { ended = true; break; }
                    },
                    () = tokio::time::sleep_until(deadline) => break,
                }
            }

            let mut tasks = JoinSet::new();
            for session in sessions {
                tasks.spawn(session.finish());
            }
            let mut failures = Vec::new();
            while let Some(result) = tasks.join_next().await {
                match result {
                    Ok(Ok(())) => telemetry::sink_delivery(Self::TOPOLOGY, &pipeline_id, true),
                    Ok(Err(error)) => failures.push(DeliveryFailure::Sink(error)),
                    Err(error) => failures.push(DeliveryFailure::Task(error)),
                }
            }
            if !failures.is_empty() {
                return Err(PipelineError::Sinks(failures));
            }
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
            let first = if let Some(deadline) = commits.deadline() {
                tokio::select! {
                    item = records.next() => item,
                    () = tokio::time::sleep_until(deadline) => {
                        commits.commit(&self.source).await.map_err(PipelineError::Commit)?;
                        telemetry::commit(Self::TOPOLOGY, &pipeline_id, true);
                        continue;
                    }
                }
            } else {
                records.next().await
            };
            let Some(first) = first else {
                break;
            };
            let mut sessions: Vec<BoxedCollectionSession<SharedRecord<Tr::Out, So::Position>>> =
                Vec::with_capacity(self.collectors.len());
            let mut begin_failures = Vec::new();
            for collector in &self.collectors {
                match collector.begin().await {
                    Ok(session) => sessions.push(session),
                    Err(error) => begin_failures.push(DeliveryFailure::Sink(error)),
                }
            }
            if !begin_failures.is_empty() {
                return Err(PipelineError::Sinks(begin_failures));
            }
            let deadline = tokio::time::Instant::now() + self.strategy.policy.timeout();
            let mut current = Some(first?);
            let mut count = 0;
            let mut ended = false;
            loop {
                if let Some(record) = current.take() {
                    let record = Arc::new(record);
                    commits.track::<So>(record.position());
                    let results = futures_util::future::join_all(
                        sessions
                            .iter_mut()
                            .map(|session| session.push(Arc::clone(&record))),
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
                tokio::select! {
                    biased;
                    item = records.next() => match item {
                        Some(record) => current = Some(record?),
                        None => { ended = true; break; }
                    },
                    () = tokio::time::sleep_until(deadline) => break,
                }
            }
            let mut tasks = JoinSet::new();
            for session in sessions {
                tasks.spawn(session.finish());
            }
            let mut failures = Vec::new();
            while let Some(result) = tasks.join_next().await {
                match result {
                    Ok(Ok(())) => telemetry::sink_delivery(Self::TOPOLOGY, &pipeline_id, true),
                    Ok(Err(error)) => failures.push(DeliveryFailure::Sink(error)),
                    Err(error) => failures.push(DeliveryFailure::Task(error)),
                }
            }
            if !failures.is_empty() {
                return Err(PipelineError::Sinks(failures));
            }
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
