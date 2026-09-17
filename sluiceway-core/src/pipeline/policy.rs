use std::{future::Future, num::NonZeroUsize, time::Duration};

use futures_core::Stream;
use futures_util::StreamExt;

use crate::{BatchConfigError, CollectConfigError, CommitConfigError, Source};

/// Type-state marker for a pipeline stage that has not been configured.
#[derive(Debug, Default, Clone, Copy)]
pub struct Unset;

/// Type-state marker carrying validated batch settings.
pub struct Batched {
    pub(super) policy: BatchPolicy,
    pub(super) commit: CommitPolicy,
}

/// Type-state marker selecting one-record-at-a-time sink delivery.
pub struct Each {
    pub(super) commit: CommitPolicy,
}

/// Type-state marker selecting incremental, batch-scoped collection.
pub struct Collected {
    pub(super) policy: CollectPolicy,
    pub(super) commit: CommitPolicy,
}

/// Controls when successfully delivered source progress is committed.
#[derive(Debug, Clone, Copy, Default)]
pub struct CommitPolicy {
    records: Option<NonZeroUsize>,
    timeout: Option<Duration>,
}

impl CommitPolicy {
    /// Commit after every successfully acknowledged sink operation or collection.
    pub const fn each() -> Self {
        Self {
            records: None,
            timeout: None,
        }
    }

    /// Commit after at least `records` source records have been acknowledged.
    pub fn after(records: usize) -> Result<Self, CommitConfigError> {
        let records = NonZeroUsize::new(records).ok_or(CommitConfigError::ZeroRecords)?;
        Ok(Self {
            records: Some(records),
            timeout: None,
        })
    }

    /// Commit after `records` acknowledgements or `timeout`, whichever is reached first.
    pub fn after_or_timeout(records: usize, timeout: Duration) -> Result<Self, CommitConfigError> {
        let records = NonZeroUsize::new(records).ok_or(CommitConfigError::ZeroRecords)?;
        if timeout.is_zero() {
            return Err(CommitConfigError::ZeroTimeout);
        }
        Ok(Self {
            records: Some(records),
            timeout: Some(timeout),
        })
    }
}

/// Validated limits for an incremental collector session.
#[derive(Debug, Clone, Copy)]
pub struct CollectPolicy {
    size: NonZeroUsize,
    timeout: Duration,
}

impl CollectPolicy {
    pub fn new(size: NonZeroUsize, timeout: Duration) -> Result<Self, CollectConfigError> {
        if timeout.is_zero() {
            return Err(CollectConfigError::ZeroTimeout);
        }
        Ok(Self { size, timeout })
    }

    pub fn try_new(size: usize, timeout: Duration) -> Result<Self, CollectConfigError> {
        let size = NonZeroUsize::new(size).ok_or(CollectConfigError::ZeroSize)?;
        Self::new(size, timeout)
    }

    pub fn size(self) -> NonZeroUsize {
        self.size
    }

    pub fn timeout(self) -> Duration {
        self.timeout
    }
}

pub(super) struct CommitState<C> {
    policy: CommitPolicy,
    checkpoint: Option<C>,
    records: usize,
    deadline: Option<tokio::time::Instant>,
}

impl<C> CommitState<C> {
    pub(super) fn new(policy: CommitPolicy) -> Self {
        Self {
            policy,
            checkpoint: None,
            records: 0,
            deadline: None,
        }
    }

    pub(super) fn track<S>(&mut self, position: &S::Position)
    where
        S: Source<Checkpoint = C>,
    {
        self.checkpoint = Some(S::track(self.checkpoint.take(), position));
    }

    pub(super) fn acknowledge(&mut self, records: usize) {
        self.records = self.records.saturating_add(records);
        if self.deadline.is_none()
            && let Some(timeout) = self.policy.timeout
        {
            self.deadline = Some(tokio::time::Instant::now() + timeout);
        }
    }

    pub(super) fn due(&self) -> bool {
        self.policy
            .records
            .is_none_or(|records| self.records >= records.get())
            && self.checkpoint.is_some()
    }

    pub(super) fn deadline(&self) -> Option<tokio::time::Instant> {
        self.deadline.filter(|_| self.checkpoint.is_some())
    }

    pub(super) async fn commit<S>(&mut self, source: &S) -> Result<bool, S::Error>
    where
        S: Source<Checkpoint = C>,
    {
        let Some(checkpoint) = self.checkpoint.take() else {
            return Ok(false);
        };
        source.commit(checkpoint).await?;
        self.records = 0;
        self.deadline = None;
        Ok(true)
    }
}

/// Validated limits for flushing record batches.
#[derive(Debug, Clone, Copy)]
pub struct BatchPolicy {
    size: NonZeroUsize,
    timeout: Duration,
    prefetch: usize,
}

impl BatchPolicy {
    pub fn new(size: NonZeroUsize, timeout: Duration) -> Result<Self, BatchConfigError> {
        if timeout.is_zero() {
            return Err(BatchConfigError::ZeroTimeout);
        }
        Ok(Self {
            size,
            timeout,
            prefetch: 0,
        })
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

    /// Materialize up to `batches` ahead of the batch currently being delivered.
    ///
    /// Prefetching overlaps source polling, transformation, and batching with serial delivery and
    /// checkpoint commits. The default is zero, which preserves demand-driven processing.
    pub fn prefetch(mut self, batches: usize) -> Self {
        self.prefetch = batches;
        self
    }

    pub(super) fn emit_reason(self, records: usize) -> &'static str {
        if records == self.size.get() {
            "full"
        } else {
            "timeout"
        }
    }
}

impl Batched {
    pub(super) async fn consume<S, T, E, Consume, Consuming, Deadline, OnTimer, Timing>(
        &self,
        batches: S,
        mut consume: Consume,
        mut deadline: Deadline,
        mut on_timer: OnTimer,
    ) -> Result<(), E>
    where
        S: Stream<Item = Result<Option<T>, E>>,
        Consume: FnMut(T) -> Consuming,
        Consuming: Future<Output = Result<(), E>>,
        Deadline: FnMut() -> Option<tokio::time::Instant>,
        OnTimer: FnMut() -> Timing,
        Timing: Future<Output = Result<(), E>>,
    {
        tokio::pin!(batches);

        if self.policy.prefetch == 0 {
            loop {
                let batch = if let Some(deadline) = deadline() {
                    tokio::select! {
                        batch = batches.next() => batch,
                        () = tokio::time::sleep_until(deadline) => {
                            on_timer().await?;
                            continue;
                        }
                    }
                } else {
                    batches.next().await
                };
                let Some(batch) = batch else { break };
                if let Some(batch) = batch? {
                    consume(batch).await?;
                }
            }
            return Ok(());
        }

        let (sender, mut receiver) = tokio::sync::mpsc::channel(self.policy.prefetch);
        let producer = async move {
            loop {
                let Ok(permit) = sender.reserve().await else {
                    return;
                };
                match batches.next().await {
                    Some(Ok(Some(batch))) => permit.send(Ok(batch)),
                    Some(Ok(None)) => {}
                    Some(Err(error)) => {
                        permit.send(Err(error));
                        return;
                    }
                    None => return,
                }
            }
        };
        let consumer = async move {
            loop {
                let batch = if let Some(deadline) = deadline() {
                    tokio::select! {
                        batch = receiver.recv() => batch,
                        () = tokio::time::sleep_until(deadline) => {
                            on_timer().await?;
                            continue;
                        }
                    }
                } else {
                    receiver.recv().await
                };
                let Some(batch) = batch else { break };
                consume(batch?).await?;
            }
            Ok(())
        };
        tokio::pin!(producer);
        tokio::pin!(consumer);

        tokio::select! {
            biased;
            result = &mut consumer => result,
            () = &mut producer => consumer.await,
        }
    }
}
