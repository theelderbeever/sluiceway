# penstock-core

`penstock-core` is the adapter-independent engine beneath the `penstock` facade. Applications
should normally import `penstock`; adapter crates may depend directly on this package.

It provides two mutually exclusive delivery topologies:

```text
Source -> transform -> LinearPipeline -> sink -> commit cursor
                    \-> FanoutPipeline --> sink --+
                                      \-> sink --+-> commit cursor
```

Calling `.sink(sink)` creates a `LinearPipeline`. Its sink consumes an owned
`Batch<T, C>` containing transformed `items` and one source-selected `cursor`.

Calling `.fanout()` creates a type-state builder that requires `.cloned()` or `.shared()` before
sinks can be attached. Cloned fanout gives every sink an owned `Batch<T, C>` and therefore requires
`T: Clone`. Shared fanout gives every sink the same `SharedBatch<T, C>`, an alias for
`Arc<Batch<T, C>>`, and requires `T: Sync` instead. Arrays and dynamically assembled vectors of
type-erased sinks are both accepted.

Every fanout sink task is drained. The source cursor is committed only after every sink succeeds
for the batch. No branch names, indexes, transform stages, or task-ID metadata are retained.

Source records carry message-local positions. The source folds them into one cursor as transformed
payloads are moved into a completed batch. The cursor remains opaque to the runner. Checkpoint
persistence remains owned by sources through the generic `CheckpointStore<C>` interface.
