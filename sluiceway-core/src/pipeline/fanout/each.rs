use std::{future::Future, sync::Arc};

use futures_util::StreamExt;
use tokio::task::JoinSet;

use crate::{
    Cloned, ErasedError, PipelineError, PipelineId, Record, Shared, SharedRecord, Source,
    Transform, telemetry,
};

use super::FanoutPipeline;
use crate::pipeline::{
    CommitPolicy, Each,
    policy::{CommitState, commit_eligible},
};

impl<So, Tr, C> FanoutPipeline<So, Tr, Cloned, Record<Tr::Out, So::Position>, Each<C>>
where
    So: Source,
    Tr: Transform<So::Payload, So::Position>,
    Tr::Out: Clone + Send + 'static,
    So::Position: Clone,
    C: CommitPolicy<So::Position>,
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
        if self.sinks.is_empty() {
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
                    let (position, payload) = item.map_err(PipelineError::Source)?.into_parts();
                    let payload = transform
                        .apply(&position, payload)
                        .await
                        .map_err(PipelineError::Transform)?;
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
                        commit_eligible(&mut commits, &self.source)
                            .await
                            .map_err(PipelineError::Commit)?;
                        telemetry::commit(Self::TOPOLOGY, &pipeline_id, true);
                        continue;
                    }
                }
            } else {
                records.next().await
            };
            let Some(record) = next else { break };
            let record = record?;
            commits.track(record.position());
            let mut tasks = JoinSet::new();
            for sink in &self.sinks {
                let sink = sink.clone();
                let record = record.clone();
                tasks.spawn(async move { sink.deliver(record).await });
            }
            let failures = Self::drain(&mut tasks, Self::TOPOLOGY, &pipeline_id).await;
            if !failures.is_empty() {
                return Err(PipelineError::Sinks(failures));
            }
            commits.acknowledge_delivery(1);
            if commit_eligible(&mut commits, &self.source)
                .await
                .map_err(PipelineError::Commit)?
            {
                telemetry::commit(Self::TOPOLOGY, &pipeline_id, true);
            }
        }
        commits.finish();
        commit_eligible(&mut commits, &self.source)
            .await
            .map(|_| ())
            .map_err(PipelineError::Commit)
    }
}

impl<So, Tr, C> FanoutPipeline<So, Tr, Shared, SharedRecord<Tr::Out, So::Position>, Each<C>>
where
    So: Source,
    Tr: Transform<So::Payload, So::Position>,
    Tr::Out: Send + Sync + 'static,
    C: CommitPolicy<So::Position>,
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
        if self.sinks.is_empty() {
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
                    let (position, payload) = item.map_err(PipelineError::Source)?.into_parts();
                    let payload = transform
                        .apply(&position, payload)
                        .await
                        .map_err(PipelineError::Transform)?;
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
                        commit_eligible(&mut commits, &self.source)
                            .await
                            .map_err(PipelineError::Commit)?;
                        telemetry::commit(Self::TOPOLOGY, &pipeline_id, true);
                        continue;
                    }
                }
            } else {
                records.next().await
            };
            let Some(record) = next else { break };
            let record = Arc::new(record?);
            commits.track(record.position());
            let mut tasks = JoinSet::new();
            for sink in &self.sinks {
                let sink = sink.clone();
                let record = Arc::clone(&record);
                tasks.spawn(async move { sink.deliver(record).await });
            }
            let failures = Self::drain(&mut tasks, Self::TOPOLOGY, &pipeline_id).await;
            if !failures.is_empty() {
                return Err(PipelineError::Sinks(failures));
            }
            commits.acknowledge_delivery(1);
            if commit_eligible(&mut commits, &self.source)
                .await
                .map_err(PipelineError::Commit)?
            {
                telemetry::commit(Self::TOPOLOGY, &pipeline_id, true);
            }
        }
        commits.finish();
        commit_eligible(&mut commits, &self.source)
            .await
            .map(|_| ())
            .map_err(PipelineError::Commit)
    }
}
