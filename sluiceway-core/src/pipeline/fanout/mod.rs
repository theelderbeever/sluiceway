use std::marker::PhantomData;

use tokio::task::JoinSet;

use crate::{
    BoxedCollector, BoxedSink, DeliveryFailure, ErasedError, PipelineId, Source, Transform,
    telemetry,
};

use super::{BatchPolicy, Batched, CollectPolicy, Collected, CommitEach, Each, Unset};

mod batched;
mod collected;
mod each;

/// A fanout pipeline whose erased sink input determines cloned or shared delivery.
pub struct FanoutPipeline<So, Tr, Mode, Input, St = Unset>
where
    So: Source,
    Tr: Transform<So::Payload, So::Position>,
    Input: Send + 'static,
{
    pub(super) id: Option<PipelineId>,
    pub(super) source: So,
    pub(super) transform: Tr,
    pub(super) sinks: Vec<BoxedSink<Input>>,
    pub(super) strategy: St,
    pub(super) mode: PhantomData<Mode>,
}

/// A fanout pipeline with one incremental collection per branch.
pub struct FanoutCollectorPipeline<So, Tr, Mode, Input, St = Unset>
where
    So: Source,
    Tr: Transform<So::Payload, So::Position>,
    Input: Send + 'static,
{
    pub(super) id: Option<PipelineId>,
    pub(super) source: So,
    pub(super) transform: Tr,
    pub(super) collectors: Vec<BoxedCollector<Input>>,
    pub(super) strategy: St,
    pub(super) mode: PhantomData<Mode>,
}

impl<So, Tr, Mode, Input> FanoutCollectorPipeline<So, Tr, Mode, Input>
where
    So: Source,
    Tr: Transform<So::Payload, So::Position>,
    Input: Send + 'static,
{
    pub fn collect(
        self,
        policy: CollectPolicy,
    ) -> FanoutCollectorPipeline<So, Tr, Mode, Input, Collected> {
        FanoutCollectorPipeline {
            id: self.id,
            source: self.source,
            transform: self.transform,
            collectors: self.collectors,
            strategy: Collected {
                policy,
                commit_policy: CommitEach,
            },
            mode: PhantomData,
        }
    }
}

impl<So, Tr, Mode, Input, C> FanoutCollectorPipeline<So, Tr, Mode, Input, Collected<C>>
where
    So: Source,
    Tr: Transform<So::Payload, So::Position>,
    Input: Send + 'static,
{
    pub fn commit_policy<Next>(
        self,
        policy: Next,
    ) -> FanoutCollectorPipeline<So, Tr, Mode, Input, Collected<Next>>
    where
        Next: super::CommitPolicy<So::Position>,
    {
        FanoutCollectorPipeline {
            id: self.id,
            source: self.source,
            transform: self.transform,
            collectors: self.collectors,
            strategy: Collected {
                policy: self.strategy.policy,
                commit_policy: policy,
            },
            mode: PhantomData,
        }
    }
}

impl<So, Tr, Mode, Input, St> FanoutPipeline<So, Tr, Mode, Input, St>
where
    So: Source,
    Tr: Transform<So::Payload, So::Position>,
    Input: Send + 'static,
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

impl<So, Tr, Mode, Input> FanoutPipeline<So, Tr, Mode, Input>
where
    So: Source,
    Tr: Transform<So::Payload, So::Position>,
    Input: Send + 'static,
{
    pub fn batched(self, policy: BatchPolicy) -> FanoutPipeline<So, Tr, Mode, Input, Batched> {
        FanoutPipeline {
            id: self.id,
            source: self.source,
            transform: self.transform,
            sinks: self.sinks,
            strategy: Batched {
                policy,
                commit_policy: CommitEach,
            },
            mode: PhantomData,
        }
    }

    pub fn each(self) -> FanoutPipeline<So, Tr, Mode, Input, Each> {
        FanoutPipeline {
            id: self.id,
            source: self.source,
            transform: self.transform,
            sinks: self.sinks,
            strategy: Each {
                commit_policy: CommitEach,
            },
            mode: PhantomData,
        }
    }
}

impl<So, Tr, Mode, Input, C> FanoutPipeline<So, Tr, Mode, Input, Batched<C>>
where
    So: Source,
    Tr: Transform<So::Payload, So::Position>,
    Input: Send + 'static,
{
    pub fn commit_policy<Next>(
        self,
        policy: Next,
    ) -> FanoutPipeline<So, Tr, Mode, Input, Batched<Next>>
    where
        Next: super::CommitPolicy<So::Position>,
    {
        FanoutPipeline {
            id: self.id,
            source: self.source,
            transform: self.transform,
            sinks: self.sinks,
            strategy: Batched {
                policy: self.strategy.policy,
                commit_policy: policy,
            },
            mode: PhantomData,
        }
    }
}

impl<So, Tr, Mode, Input, C> FanoutPipeline<So, Tr, Mode, Input, Each<C>>
where
    So: Source,
    Tr: Transform<So::Payload, So::Position>,
    Input: Send + 'static,
{
    pub fn commit_policy<Next>(
        self,
        policy: Next,
    ) -> FanoutPipeline<So, Tr, Mode, Input, Each<Next>>
    where
        Next: super::CommitPolicy<So::Position>,
    {
        FanoutPipeline {
            id: self.id,
            source: self.source,
            transform: self.transform,
            sinks: self.sinks,
            strategy: Each {
                commit_policy: policy,
            },
            mode: PhantomData,
        }
    }
}
