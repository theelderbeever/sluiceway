# penstock-core

`penstock-core` is the adapter-independent engine beneath the `penstock` facade. Applications
should normally import `penstock`; adapter crates may depend directly on this package.

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
positions, while the source separately folds the positions into an internal checkpoint. Checkpoint
persistence remains owned by sources through the generic `CheckpointStore<C>` interface.

`run()` consumes until the source stream naturally ends. A source can own graceful shutdown by
observing a signal in `Source::stream`, stopping intake, draining its internal buffer, and then
returning `None`. `run_until(shutdown)` is an explicit cutoff: it stops polling the source when the
future resolves, completes already admitted transforms, and flushes the partial batch.

Transforms may run concurrently but their outputs remain in observed source order. Batches are
delivered serially, and a checkpoint is committed only after successful delivery. The batch timeout
starts when the first transformed record enters an empty batch.
