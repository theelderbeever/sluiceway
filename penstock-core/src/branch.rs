use std::{collections::HashMap, fmt::Debug, future::Future, sync::Arc};

use futures_util::{StreamExt, stream};
use tokio::task::{Id, JoinSet};

use crate::{BranchFailure, BranchStage, Identity, Record, Sink, Transform};

pub(crate) type SharedBatch<I, C> = Arc<[Record<Arc<I>, C>]>;

/// Type-state marker for a branch without a sink.
#[derive(Debug, Default, Clone, Copy)]
pub struct BranchUnset;

/// One sink-specific transform and its terminal sink.
pub struct Branch<T, S = BranchUnset> {
    pub(crate) name: Arc<str>,
    pub(crate) index: usize,
    transform: T,
    sink: S,
}

impl<T> Branch<T, BranchUnset> {
    pub fn new(name: impl Into<Arc<str>>, transform: T) -> Self {
        Self {
            name: name.into(),
            index: 0,
            transform,
            sink: BranchUnset,
        }
    }

    pub fn sink<S>(self, sink: S) -> Branch<T, S> {
        Branch {
            name: self.name,
            index: self.index,
            transform: self.transform,
            sink,
        }
    }
}

impl Branch<Identity, BranchUnset> {
    pub fn identity(name: impl Into<Arc<str>>) -> Self {
        Self::new(name, Identity)
    }
}

/// Empty branch-list terminator.
#[derive(Debug, Default, Clone, Copy)]
pub struct Nil;

/// A compile-time heterogeneous list of registered branches.
pub struct Cons<H, T> {
    pub(crate) head: Arc<H>,
    pub(crate) tail: T,
}

#[derive(Debug, Clone)]
#[doc(hidden)]
pub struct BranchMeta {
    pub name: String,
    pub index: usize,
}

#[doc(hidden)]
pub trait ExecuteBranch<I, C>: Send + Sync {
    fn metadata(&self) -> BranchMeta;

    fn execute(
        &self,
        batch: SharedBatch<I, C>,
    ) -> impl Future<Output = Result<C, BranchFailure>> + Send;
}

impl<I, C, T, S> ExecuteBranch<I, C> for Branch<T, S>
where
    I: Send + Sync + 'static,
    C: Clone + Eq + Debug + Send + Sync + 'static,
    T: Transform<Arc<I>> + 'static,
    T::Out: 'static,
    S: Sink<T::Out, Cursor = C> + 'static,
{
    fn metadata(&self) -> BranchMeta {
        BranchMeta {
            name: self.name.to_string(),
            index: self.index,
        }
    }

    async fn execute(&self, batch: SharedBatch<I, C>) -> Result<C, BranchFailure> {
        let transform = &self.transform;
        let transformed = stream::iter(batch.iter().cloned())
            .map(|Record { cursor, payload }| async move {
                transform
                    .apply(payload)
                    .await
                    .map(|payload| Record::new(cursor, payload))
            })
            .buffered(transform.max_concurrency().get())
            .collect::<Vec<_>>()
            .await;

        let mut records = Vec::with_capacity(transformed.len());
        for result in transformed {
            match result {
                Ok(record) => records.push(record),
                Err(error) => {
                    return Err(BranchFailure::stage(
                        self.name.to_string(),
                        self.index,
                        BranchStage::Transform,
                        error,
                    ));
                }
            }
        }

        self.sink.deliver(records).await.map_err(|error| {
            BranchFailure::stage(self.name.to_string(), self.index, BranchStage::Sink, error)
        })
    }
}

#[doc(hidden)]
pub trait SpawnBranches<I, C> {
    fn spawn_all(
        &self,
        batch: SharedBatch<I, C>,
        tasks: &mut JoinSet<Result<C, BranchFailure>>,
        metadata: &mut HashMap<Id, BranchMeta>,
    );
}

impl<I, C> SpawnBranches<I, C> for Nil {
    fn spawn_all(
        &self,
        _batch: SharedBatch<I, C>,
        _tasks: &mut JoinSet<Result<C, BranchFailure>>,
        _metadata: &mut HashMap<Id, BranchMeta>,
    ) {
    }
}

impl<I, C, H, T> SpawnBranches<I, C> for Cons<H, T>
where
    I: Send + Sync + 'static,
    C: Clone + Eq + Debug + Send + Sync + 'static,
    H: ExecuteBranch<I, C> + 'static,
    T: SpawnBranches<I, C>,
{
    fn spawn_all(
        &self,
        batch: SharedBatch<I, C>,
        tasks: &mut JoinSet<Result<C, BranchFailure>>,
        metadata: &mut HashMap<Id, BranchMeta>,
    ) {
        let branch = Arc::clone(&self.head);
        let branch_metadata = branch.metadata();
        let branch_batch = Arc::clone(&batch);
        let handle = tasks.spawn(async move { branch.execute(branch_batch).await });
        metadata.insert(handle.id(), branch_metadata);
        self.tail.spawn_all(batch, tasks, metadata);
    }
}
