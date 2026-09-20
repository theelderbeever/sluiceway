use std::marker::PhantomData;

use crate::{BoxedCollector, BoxedSink, Cloned, PipelineId, Shared, Source, Transform};

mod fanout;
mod linear;
mod policy;

pub use fanout::{FanoutCollectorPipeline, FanoutPipeline};
pub use linear::LinearPipeline;
pub use policy::{
    AfterRecords, AfterRecordsOrTimeout, BatchPolicy, Batched, CollectPolicy, Collected,
    CommitAction, CommitEach, CommitPolicy, Each, PositionAction, Unset,
};

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
    pub fn sinks<I, Input>(self, sinks: I) -> FanoutPipeline<So, Tr, Cloned, Input>
    where
        Input: Send + 'static,
        I: IntoIterator<Item = BoxedSink<Input>>,
    {
        FanoutPipeline {
            id: self.id,
            source: self.source,
            transform: self.transform,
            sinks: sinks.into_iter().collect(),
            strategy: Unset,
            mode: PhantomData,
        }
    }

    pub fn collectors<I, Input>(
        self,
        collectors: I,
    ) -> FanoutCollectorPipeline<So, Tr, Cloned, Input>
    where
        Input: Send + 'static,
        I: IntoIterator<Item = BoxedCollector<Input>>,
    {
        FanoutCollectorPipeline {
            id: self.id,
            source: self.source,
            transform: self.transform,
            collectors: collectors.into_iter().collect(),
            strategy: Unset,
            mode: PhantomData,
        }
    }
}

impl<So, Tr> FanoutBuilder<So, Tr, Shared>
where
    So: Source,
    Tr: Transform<So::Payload, So::Position>,
    Tr::Out: Send + Sync + 'static,
{
    pub fn sinks<I, Input>(self, sinks: I) -> FanoutPipeline<So, Tr, Shared, Input>
    where
        Input: Send + 'static,
        I: IntoIterator<Item = BoxedSink<Input>>,
    {
        FanoutPipeline {
            id: self.id,
            source: self.source,
            transform: self.transform,
            sinks: sinks.into_iter().collect(),
            strategy: Unset,
            mode: PhantomData,
        }
    }

    pub fn collectors<I, Input>(
        self,
        collectors: I,
    ) -> FanoutCollectorPipeline<So, Tr, Shared, Input>
    where
        Input: Send + 'static,
        I: IntoIterator<Item = BoxedCollector<Input>>,
    {
        FanoutCollectorPipeline {
            id: self.id,
            source: self.source,
            transform: self.transform,
            collectors: collectors.into_iter().collect(),
            strategy: Unset,
            mode: PhantomData,
        }
    }
}
