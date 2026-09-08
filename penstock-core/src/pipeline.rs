use std::{
    collections::HashMap, fmt::Debug, future::Future, num::NonZeroUsize, sync::Arc, time::Duration,
};

use futures_util::StreamExt;
use tokio::task::JoinSet;

use crate::{
    BatchConfigError, Branch, BranchFailure, BranchStage, Cons, ExecuteBranch, Nil, PipelineError,
    Record, Source, SpawnBranches, Transform, branch::SharedBatch,
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
}

/// Type-state builder and runner for a source, one shared transform, and typed branches.
pub struct Pipeline<So, Sh = Unset, Bs = Nil, St = Unset> {
    source: So,
    shared: Sh,
    branches: Bs,
    strategy: St,
    branch_count: usize,
}

impl<So: Source> Pipeline<So> {
    pub fn source(source: So) -> Self {
        Self {
            source,
            shared: Unset,
            branches: Nil,
            strategy: Unset,
            branch_count: 0,
        }
    }

    pub fn transform<Sh>(self, shared: Sh) -> Pipeline<So, Sh>
    where
        Sh: Transform<So::Payload>,
    {
        Pipeline {
            source: self.source,
            shared,
            branches: self.branches,
            strategy: self.strategy,
            branch_count: self.branch_count,
        }
    }
}

impl<So, Sh, Bs> Pipeline<So, Sh, Bs>
where
    So: Source,
    Sh: Transform<So::Payload>,
{
    pub fn branch<T, S>(
        self,
        mut branch: Branch<T, S>,
    ) -> Pipeline<So, Sh, Cons<Branch<T, S>, Bs>> {
        branch.index = self.branch_count;
        Pipeline {
            source: self.source,
            shared: self.shared,
            branches: Cons {
                head: Arc::new(branch),
                tail: self.branches,
            },
            strategy: self.strategy,
            branch_count: self.branch_count + 1,
        }
    }
}

impl<So, Sh, H, T> Pipeline<So, Sh, Cons<H, T>>
where
    So: Source,
    Sh: Transform<So::Payload>,
{
    pub fn batched(self, policy: BatchPolicy) -> Pipeline<So, Sh, Cons<H, T>, Batched> {
        Pipeline {
            source: self.source,
            shared: self.shared,
            branches: self.branches,
            strategy: Batched { policy },
            branch_count: self.branch_count,
        }
    }
}

impl<So, Sh, H, Tail> Pipeline<So, Sh, Cons<H, Tail>, Batched>
where
    So: Source,
    So::Cursor: Clone + Eq + Debug + Send + Sync + 'static,
    Sh: Transform<So::Payload>,
    Sh::Out: Send + Sync + 'static,
    H: ExecuteBranch<Sh::Out, So::Cursor> + 'static,
    Tail: SpawnBranches<Sh::Out, So::Cursor>,
{
    pub async fn run_until(
        self,
        shutdown: impl Future<Output = ()> + Send,
    ) -> Result<(), PipelineError<So::Error, Sh::Error>> {
        let shared = &self.shared;
        // Stop accepting source records on shutdown, then drain transforms already admitted by
        // the ordered buffer and flush its final partial batch.
        let records = self
            .source
            .stream()
            .take_until(shutdown)
            .map(|item| async move {
                let Record { cursor, payload } = item.map_err(PipelineError::Source)?;
                let payload = shared
                    .apply(payload)
                    .await
                    .map_err(PipelineError::SharedTransform)?;
                Ok(Record::new(cursor, Arc::new(payload)))
            })
            .buffered(shared.max_concurrency().get());
        tokio::pin!(records);
        let chunks = tokio_stream::StreamExt::chunks_timeout(
            records,
            self.strategy.policy.size.get(),
            self.strategy.policy.timeout,
        );
        tokio::pin!(chunks);

        while let Some(chunk) = chunks.next().await {
            let records: Vec<Record<Arc<Sh::Out>, So::Cursor>> =
                chunk.into_iter().collect::<Result<_, _>>()?;
            let Some(expected) = records.last().map(|record| record.cursor.clone()) else {
                continue;
            };
            let batch: SharedBatch<Sh::Out, So::Cursor> = records.into();
            let mut tasks = JoinSet::new();
            let mut metadata = HashMap::with_capacity(self.branch_count);
            self.branches.spawn_all(batch, &mut tasks, &mut metadata);

            let mut failures = Vec::new();
            while let Some(joined) = tasks.join_next_with_id().await {
                match joined {
                    Ok((id, Ok(actual))) => {
                        let branch = metadata
                            .remove(&id)
                            .expect("every spawned branch task has metadata");
                        if actual != expected {
                            failures.push(BranchFailure::cursor(
                                branch.name,
                                branch.index,
                                &expected,
                                actual,
                            ));
                        }
                    }
                    Ok((id, Err(failure))) => {
                        metadata.remove(&id);
                        failures.push(failure);
                    }
                    Err(error) => {
                        let branch =
                            metadata
                                .remove(&error.id())
                                .unwrap_or_else(|| crate::BranchMeta {
                                    name: "unknown".to_owned(),
                                    index: usize::MAX,
                                });
                        failures.push(BranchFailure::stage(
                            branch.name,
                            branch.index,
                            BranchStage::Task,
                            error,
                        ));
                    }
                }
            }

            if !failures.is_empty() {
                failures.sort_by_key(|failure| failure.index);
                return Err(PipelineError::Branches(failures));
            }

            self.source
                .commit(expected)
                .await
                .map_err(PipelineError::Commit)?;
        }
        Ok(())
    }
}
