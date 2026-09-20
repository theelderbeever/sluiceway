# sluiceway-core

`sluiceway-core` is the adapter-independent engine beneath the `sluiceway` facade. Applications
should normally import `sluiceway`; adapter crates may depend directly on this package.

It provides two mutually exclusive delivery topologies:

```text
Source -> transform -> LinearPipeline -> sink -> commit checkpoint
                    \-> FanoutPipeline --> sink --+
                                      \-> sink --+-> commit checkpoint
```

Calling `.sink(sink)` creates a `LinearPipeline`. Its sink consumes an owned
`Batch<T, P>` containing transformed `Record<T, P>` values.

Calling `.fanout()` creates a type-state builder that requires `.cloned()` or `.shared()` before
sinks can be attached. Cloned fanout gives every sink an owned `Batch<T, P>` and therefore requires
`T: Clone` and `P: Clone`. Shared fanout gives every sink the same `SharedBatch<T, P>`, an alias for
`Arc<Batch<T, P>>`, and requires `T: Sync` instead. Arrays and dynamically assembled vectors of
type-erased sinks are both accepted.

Every fanout sink task is drained. The source checkpoint is committed only after every sink succeeds
for the batch. No branch names, indexes, transform stages, or task-ID metadata are retained.

Source records carry message-local positions. Transforms receive each position by immutable
reference alongside the owned payload. Completed batches retain the transformed records and their
positions, while the runner folds positions into values implementing `Checkpoint<P>`. Checkpoint
persistence remains owned by sources through `Source::commit`; the generic `CheckpointStore<C>`
interface persists already constructed values. Custom policies can start a new checkpoint epoch at
a position boundary. Checkpoint types that represent complete frontiers can override
`Checkpoint::start_next_epoch`; the default starts an independently committable next epoch.

`run()` consumes until the source stream naturally ends. A source can own graceful shutdown by
observing a signal in `Source::stream`, stopping intake, draining its internal buffer, and then
returning `None`. `run_until(shutdown)` is an explicit cutoff: it stops polling the source when the
future resolves, completes already admitted transforms, and flushes the partial batch.

Transforms may run concurrently but their outputs remain in observed source order. Sinks may consume
individual records with `.each()`, materialized batches with `.batched(...)`, or incremental
batch-scoped `Collection`s with `.collect(...)`. Collection shape is independent from
`CommitPolicy`, so successfully delivered records can be committed after every acknowledgement,
after a record count, after a count-or-time threshold, or at custom source-position boundaries.
The built-ins are `CommitEach` (the default), `AfterRecords`, and `AfterRecordsOrTimeout`.

Commit policies operate at acknowledged delivery boundaries:

| Collection shape | Acknowledged after | Effect of a passed commit deadline |
| --- | --- | --- |
| `.each()` | The record sink returns success | The current record finishes, then pending progress is committed |
| `.batched(...)` | The whole batch sink returns success | The in-flight batch finishes, then pending progress, including that batch, is committed |
| `.collect(...)` | The `Collection`'s `finish()` returns success | The in-flight collection finishes, then pending progress, including that collection, is committed |

For fanout, acknowledgement requires every branch to succeed. A commit timeout starts with the
first successful acknowledgement after the preceding commit. It is a maximum idle wait at a safe
boundary, not a cancellation deadline, so slow sink work can extend the elapsed time between
commits. Collection and batch timeouts only decide when to close their respective delivery unit;
they likewise do not interrupt an in-progress sink method. Clean EOF or graceful shutdown commits
remaining acknowledged progress, while a delivery failure does not trigger an opportunistic
commit.

Batch prefetch defaults to zero; configuring `BatchPolicy::prefetch` overlaps bounded batch
materialization with delivery and commit without changing their order. The batch timeout starts
when the first transformed record enters an empty batch.
