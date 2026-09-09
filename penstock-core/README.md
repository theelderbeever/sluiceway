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
`Batch<T, C>`, which is an alias for `Vec<Record<T, C>>`.

Calling `.fanout()` creates a type-state builder that requires `.cloned()` or `.shared()` before
sinks can be attached. Cloned fanout gives every sink an owned `Batch<T, C>` and therefore requires
`T: Clone`. Shared fanout gives every sink the same `SharedBatch<T, C>`, an alias for
`Arc<[Record<T, C>]>`, and requires `T: Sync` instead. Arrays and dynamically assembled vectors of
type-erased sinks are both accepted.

Every fanout sink task is drained. The source cursor is committed only after every sink succeeds
and acknowledges the batch's final cursor. No branch names, indexes, transform stages, or task-ID
metadata are retained.

The cursor is opaque to the runner: it needs equality for acknowledgment validation but does not
need to be numeric or ordered. Checkpoint persistence remains owned by sources through the generic
`CheckpointStore<C>` interface.
