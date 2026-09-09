# Penstock

Penstock is a typed, Tokio-based framework for ordered pipelines. A transformed batch is either
moved into one sink or shared with a fanout of type-erased sinks:

```text
Source -> transform -> LinearPipeline -> sink -> commit cursor
                  `-> FanoutPipeline -> sink --+
                                    `-> sink --+-> commit cursor
```

Applications import the `penstock` facade. The low-level engine lives in `penstock-core`, while
`penstock-io` provides opt-in durable checkpoint adapters.

## Packages

| Package | Purpose |
| --- | --- |
| `penstock` | Public facade and feature-gated adapter re-exports |
| `penstock-core` | Records, sources, transforms, sinks, checkpoints, and runners |
| `penstock-io` | Filesystem, object-store, and SQL checkpoint adapters |

A linear sink owns and consumes each batch:

```rust,no_run
use std::{convert::Infallible, time::Duration};
use penstock::{Batch, BatchPolicy, Pipeline, Sink};

# async fn run<S, K>(source: S, sink: K) -> Result<(), Box<dyn std::error::Error>>
# where
#     S: penstock::Source<Payload = Vec<u8>>,
#     K: Sink<Batch<usize, S::Cursor>, Cursor = S::Cursor>,
#     S::Error: 'static,
# {
Pipeline::source(source)
    .transform(|payload: Vec<u8>| async move {
        Ok::<_, Infallible>(payload.len())
    })
    .sink(sink)
    .batched(BatchPolicy::try_new(100, Duration::from_secs(1))?)
    .run_until(std::future::pending())
    .await?;
# Ok(())
# }
```

Fanout sinks consume a shared batch. Concrete sink and error types are erased by `.into()` at the
fanout boundary, allowing heterogeneous sinks in one array:

```rust,no_run
# use std::{convert::Infallible, time::Duration};
# use penstock::{BatchPolicy, Pipeline, SharedBatch, Sink};
# async fn run<S, A, B>(source: S, analytics: A, archive: B) -> Result<(), Box<dyn std::error::Error>>
# where
#     S: penstock::Source<Payload = Vec<u8>>,
#     S::Error: 'static,
#     A: Sink<SharedBatch<Vec<u8>, S::Cursor>, Cursor = S::Cursor> + 'static,
#     B: Sink<SharedBatch<Vec<u8>, S::Cursor>, Cursor = S::Cursor> + 'static,
# {
Pipeline::source(source)
    .transform(|payload: Vec<u8>| async move { Ok::<_, Infallible>(payload) })
    .fanout()
    .shared()
    .sinks([analytics.into(), archive.into()])
    .batched(BatchPolicy::try_new(100, Duration::from_secs(1))?)
    .run_until(std::future::pending())
    .await?;
# Ok(())
# }
```

Fanout requires an explicit ownership mode. `.cloned()` gives each sink an owned `Batch<T, C>` and
can reuse linear sinks; `.shared()` gives every sink the same `SharedBatch<T, C>`, an alias for
`Arc<[Record<T, C>]>`. `.sinks(...)` accepts arrays or dynamically assembled vectors of the
corresponding `BoxSink`. An empty fanout returns `PipelineError::NoSinks` before the source starts.
Every fanout task is drained, and the source cursor is committed only when every sink acknowledges
the final cursor.

The cursor is opaque to the runner: it needs equality for acknowledgment validation but does not
need to be numeric or ordered. Checkpoint persistence remains source-owned through
`CheckpointStore<C>`.

Checkpoint adapters are available through facade features. `io` enables local files,
`object-store` adds caller-configured object stores, and `sql-postgres`, `sql-mysql`, and
`sql-sqlite` add SQLx-backed stores. Adapters encode cursors as JSON, so structured cursor types
can be used when they implement Serde's `Serialize` and `DeserializeOwned` traits.
