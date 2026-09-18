use std::{
    collections::VecDeque, future::Future, marker::PhantomData, num::NonZeroUsize, time::Duration,
};

use futures_core::Stream;
use futures_util::StreamExt;

use crate::{BatchConfigError, Checkpoint, CollectConfigError, CommitConfigError, Source};

/// Type-state marker for a pipeline stage that has not been configured.
#[derive(Debug, Default, Clone, Copy)]
pub struct Unset;

/// Type-state marker carrying validated batch settings.
pub struct Batched<CP = CommitEach> {
    pub(super) policy: BatchPolicy,
    pub(super) commit_policy: CP,
}

/// Type-state marker selecting one-record-at-a-time sink delivery.
pub struct Each<CP = CommitEach> {
    pub(super) commit_policy: CP,
}

/// Type-state marker selecting incremental, batch-scoped collection.
pub struct Collected<CP = CommitEach> {
    pub(super) policy: CollectPolicy,
    pub(super) commit_policy: CP,
}

/// Controls when successfully delivered source progress is committed.
///
/// Position actions are evaluated as records enter delivery, but completed checkpoint epochs do not
/// become eligible until the complete delivery unit is acknowledged. A delivery unit is one record
/// for [`Each`], one complete batch for [`Batched`], or one collector [`crate::Collection`] whose
/// [`crate::Collection::finish`] call succeeded for [`Collected`]. Fanout acknowledges the unit only
/// after every branch succeeds.
///
/// A timeout starts with the first acknowledgement after the previous commit. It is not a
/// cancellation deadline: if it expires while a sink or collector is running, that operation is
/// allowed to finish and the commit occurs at the next safe boundary. Consequently, a slow
/// operation can make the elapsed time between commits longer than the configured timeout.
/// Clean EOF and graceful shutdown commit any remaining acknowledged progress regardless of the
/// configured count. A failed delivery is not acknowledged and does not cause an opportunistic
/// commit.
pub trait CommitPolicy<P>: Send {
    /// Observe a position immediately before it enters an ordered delivery attempt.
    fn on_position(&mut self, _position: &P) -> PositionAction {
        PositionAction::Continue
    }

    /// Observe one successfully acknowledged delivery unit.
    fn on_acknowledged(&mut self, records: usize) -> CommitAction;

    /// Return the next commit deadline, if any.
    fn deadline(&self) -> Option<tokio::time::Instant> {
        None
    }

    /// Handle a deadline at a safe boundary.
    ///
    /// An implementation returning [`CommitAction::Continue`] must clear or advance its deadline
    /// to avoid the runner immediately observing the same deadline again.
    fn on_timeout(&mut self) -> CommitAction {
        CommitAction::CommitLatest
    }

    /// Observe one successfully persisted checkpoint epoch.
    fn on_committed(&mut self);
}

/// Action requested while observing a source position.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PositionAction {
    /// Add the current position to the open checkpoint epoch.
    Continue,
    /// Close the open checkpoint epoch before the current position, then start the next epoch with
    /// the current position.
    StartNewEpoch,
}

/// Action requested at an acknowledgement or timeout boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitAction {
    Continue,
    CommitLatest,
}

/// Commit after every successfully acknowledged delivery unit.
#[derive(Debug, Default, Clone, Copy)]
pub struct CommitEach;

impl<P> CommitPolicy<P> for CommitEach {
    fn on_acknowledged(&mut self, _records: usize) -> CommitAction {
        CommitAction::CommitLatest
    }

    fn on_committed(&mut self) {}
}

/// Commit after at least a configured number of records have been acknowledged.
#[derive(Debug, Clone, Copy)]
pub struct AfterRecords {
    records: NonZeroUsize,
    acknowledged: usize,
}

impl AfterRecords {
    /// Construct a policy from a validated nonzero record count.
    pub const fn new(records: NonZeroUsize) -> Self {
        Self {
            records,
            acknowledged: 0,
        }
    }

    /// Validate a record count and construct a policy.
    pub fn try_new(records: usize) -> Result<Self, CommitConfigError> {
        let records = NonZeroUsize::new(records).ok_or(CommitConfigError::ZeroRecords)?;
        Ok(Self::new(records))
    }
}

impl<P> CommitPolicy<P> for AfterRecords {
    // Intentionally a softmax so batches can go over the threshold
    fn on_acknowledged(&mut self, records: usize) -> CommitAction {
        self.acknowledged = self.acknowledged.saturating_add(records);
        if self.acknowledged >= self.records.get() {
            CommitAction::CommitLatest
        } else {
            CommitAction::Continue
        }
    }

    fn on_committed(&mut self) {
        self.acknowledged = 0;
    }
}

/// Commit after a record count or timeout, whichever is reached first.
#[derive(Debug, Clone, Copy)]
pub struct AfterRecordsOrTimeout {
    records: NonZeroUsize,
    timeout: Duration,
    acknowledged: usize,
    deadline: Option<tokio::time::Instant>,
}

impl AfterRecordsOrTimeout {
    /// Construct a policy from a validated count and a nonzero timeout.
    pub fn new(records: NonZeroUsize, timeout: Duration) -> Result<Self, CommitConfigError> {
        if timeout.is_zero() {
            return Err(CommitConfigError::ZeroTimeout);
        }
        Ok(Self {
            records,
            timeout,
            acknowledged: 0,
            deadline: None,
        })
    }

    /// Validate a record count and timeout and construct a policy.
    pub fn try_new(records: usize, timeout: Duration) -> Result<Self, CommitConfigError> {
        let records = NonZeroUsize::new(records).ok_or(CommitConfigError::ZeroRecords)?;
        Self::new(records, timeout)
    }
}

impl<P> CommitPolicy<P> for AfterRecordsOrTimeout {
    fn on_acknowledged(&mut self, records: usize) -> CommitAction {
        self.acknowledged = self.acknowledged.saturating_add(records);
        if self.deadline.is_none() {
            self.deadline = Some(tokio::time::Instant::now() + self.timeout);
        }
        if self.acknowledged >= self.records.get() {
            CommitAction::CommitLatest
        } else {
            CommitAction::Continue
        }
    }

    fn deadline(&self) -> Option<tokio::time::Instant> {
        self.deadline
    }

    fn on_committed(&mut self) {
        self.acknowledged = 0;
        self.deadline = None;
    }
}

/// Validated limits for an incremental collection.
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

/// Runner-owned checkpoint state spanning delivery units between commits.
///
/// Checkpoints are opaque, so an epoch boundary cannot be represented as an index into the source
/// positions or reconstructed later. Each transition moves the completed epoch into
/// `staged_epochs` until the delivery unit succeeds, then into `eligible_epochs` until it is
/// committed in source order.
pub(super) struct CommitState<C, Policy, Position>
where
    C: Checkpoint<Position>,
    Policy: CommitPolicy<Position>,
{
    policy: Policy,
    /// The open checkpoint epoch.
    current_epoch: Option<C>,
    /// Epochs closed during the current, not-yet-acknowledged delivery unit.
    staged_epochs: Vec<C>,
    /// Acknowledged checkpoint epochs waiting to be committed in source order.
    eligible_epochs: VecDeque<C>,
    /// Whether the open epoch contains only acknowledged progress.
    current_epoch_acknowledged: bool,
    position: PhantomData<fn(&Position)>,
}

impl<C, Policy, Position> CommitState<C, Policy, Position>
where
    C: Checkpoint<Position>,
    Policy: CommitPolicy<Position>,
{
    pub(super) fn new(policy: Policy) -> Self {
        Self {
            policy,
            current_epoch: None,
            staged_epochs: Vec::new(),
            eligible_epochs: VecDeque::new(),
            current_epoch_acknowledged: false,
            position: PhantomData,
        }
    }

    /// Observe and fold a position immediately before it enters sink delivery.
    ///
    /// Starting a new epoch closes the checkpoint preceding `position`; the current position always
    /// belongs to the next epoch. Closed epochs remain staged because the surrounding record, batch,
    /// collection, or fanout operation can still fail.
    pub(super) fn track(&mut self, position: &Position) {
        match self.policy.on_position(position) {
            PositionAction::Continue => match self.current_epoch.as_mut() {
                Some(epoch) => epoch.include_position(position),
                None => self.current_epoch = Some(C::start_epoch(position)),
            },
            PositionAction::StartNewEpoch => match self.current_epoch.take() {
                Some(current_epoch) => {
                    let transition = current_epoch.start_next_epoch(position);
                    self.staged_epochs.push(transition.completed_epoch);
                    self.current_epoch = Some(transition.next_epoch);
                }
                None => {
                    self.current_epoch = Some(C::start_epoch(position));
                }
            },
        }
        self.current_epoch_acknowledged = false;
    }

    /// Mark one complete delivery unit as successful and apply the policy at that safe boundary.
    ///
    /// All epochs closed within the unit become eligible together. A count action may also close
    /// the latest epoch. When no count action fires, an already-passed deadline is applied after
    /// including this unit's progress, so timeouts never interrupt sink work.
    pub(super) fn acknowledge_delivery(&mut self, records: usize) {
        self.eligible_epochs.extend(self.staged_epochs.drain(..));
        self.current_epoch_acknowledged = self.current_epoch.is_some();
        let action = self.policy.on_acknowledged(records);
        let timed_out = action == CommitAction::Continue
            && self
                .policy
                .deadline()
                .is_some_and(|deadline| deadline <= tokio::time::Instant::now());
        if action == CommitAction::CommitLatest
            || timed_out && self.policy.on_timeout() == CommitAction::CommitLatest
        {
            self.queue_latest();
        }
    }

    /// Return a deadline only when the open epoch is safe to commit.
    ///
    /// Runners call this while waiting between delivery units. During a delivery, the open
    /// checkpoint may include unacknowledged records and must not be exposed to a timeout commit.
    pub(super) fn deadline(&self) -> Option<tokio::time::Instant> {
        self.current_epoch_acknowledged
            .then(|| self.policy.deadline())
            .flatten()
    }

    /// Apply a reached policy deadline while the runner is at a safe boundary.
    pub(super) fn handle_timeout(&mut self) {
        if self.policy.on_timeout() == CommitAction::CommitLatest {
            self.queue_latest();
        }
    }

    /// Make all remaining progress eligible after clean EOF or graceful shutdown.
    ///
    /// Error paths deliberately do not call this method, preserving the rule that operational
    /// failures never cause an opportunistic final commit.
    pub(super) fn finish(&mut self) {
        self.eligible_epochs.extend(self.staged_epochs.drain(..));
        self.queue_latest();
    }

    /// Close the open epoch and append it after any earlier completed epochs.
    fn queue_latest(&mut self) {
        if let Some(checkpoint) = self.current_epoch.take() {
            self.eligible_epochs.push_back(checkpoint);
        }
        self.current_epoch_acknowledged = false;
    }

    pub(super) fn pop_eligible(&mut self) -> Option<C> {
        self.eligible_epochs.pop_front()
    }

    pub(super) fn committed(&mut self) {
        self.policy.on_committed();
    }
}

pub(super) async fn commit_eligible<S, C, Policy>(
    state: &mut CommitState<C, Policy, S::Position>,
    source: &S,
) -> Result<bool, S::Error>
where
    S: Source<Checkpoint = C>,
    C: Checkpoint<S::Position>,
    Policy: CommitPolicy<S::Position>,
{
    let mut committed = false;
    while let Some(epoch) = state.pop_eligible() {
        source.commit(epoch).await?;
        state.committed();
        committed = true;
    }
    Ok(committed)
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

impl BatchPolicy {
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

        if self.prefetch == 0 {
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

        let (sender, mut receiver) = tokio::sync::mpsc::channel(self.prefetch);
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

#[cfg(test)]
mod tests {
    use super::{AfterRecords, CommitAction, CommitPolicy};

    #[test]
    fn after_records_resets_only_after_a_successful_commit() {
        let mut policy = AfterRecords::try_new(3).unwrap();

        assert_eq!(
            <AfterRecords as CommitPolicy<()>>::on_acknowledged(&mut policy, 2),
            CommitAction::Continue
        );
        assert_eq!(
            <AfterRecords as CommitPolicy<()>>::on_acknowledged(&mut policy, 1),
            CommitAction::CommitLatest
        );
        <AfterRecords as CommitPolicy<()>>::on_committed(&mut policy);
        assert_eq!(
            <AfterRecords as CommitPolicy<()>>::on_acknowledged(&mut policy, 1),
            CommitAction::Continue
        );
    }

    #[test]
    fn after_records_uses_saturating_accounting() {
        let mut policy = AfterRecords::try_new(2).unwrap();
        policy.acknowledged = usize::MAX;

        assert_eq!(
            <AfterRecords as CommitPolicy<()>>::on_acknowledged(&mut policy, 1),
            CommitAction::CommitLatest
        );
    }
}
