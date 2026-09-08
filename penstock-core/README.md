# penstock-core

`penstock-core` is the adapter-independent engine beneath the `penstock` facade. Applications
should normally import `penstock`; adapter crates may depend directly on this package.

It provides a typed, Tokio-based runner for ordered, branching pipelines:

```text
Source -> shared transform -> branch transform -> sink --+
                           `-> branch transform -> sink --+-> commit cursor
```

Every registered branch runs automatically. For each shared batch, the runner spawns one task per
branch into an internal `JoinSet`, drains every task, validates every sink's cursor acknowledgment,
and commits only when all branches succeeded.

```rust,no_run
use std::{convert::Infallible, sync::Arc, time::Duration};
use penstock_core::{BatchPolicy, Branch, Pipeline};

# async fn run<S, A, B>(source: S, analytics: A, archive: B) -> Result<(), Box<dyn std::error::Error>>
# where
#     S: penstock_core::Source<Payload = Vec<u8>>,
#     A: penstock_core::Sink<usize, Cursor = S::Cursor> + 'static,
#     B: penstock_core::Sink<Vec<u8>, Cursor = S::Cursor> + 'static,
#     S::Error: 'static,
# {
Pipeline::source(source)
    .transform(|payload: Vec<u8>| async move {
        Ok::<_, Infallible>(payload)
    })
    .branch(
        Branch::new("analytics", |payload: Arc<Vec<u8>>| async move {
            Ok::<_, Infallible>(payload.len())
        })
        .sink(analytics),
    )
    .branch(
        Branch::new("archive", |payload: Arc<Vec<u8>>| async move {
            Ok::<_, Infallible>((*payload).clone())
        })
        .sink(archive),
    )
    .batched(BatchPolicy::try_new(100, Duration::from_secs(1))?)
    .run_until(std::future::pending())
    .await?;
# Ok(())
# }
```

The cursor is opaque to the runner: it needs equality for acknowledgment validation but does not
need to be numeric or ordered. Checkpoint persistence remains owned by sources through the generic
`CheckpointStore<C>` interface, allowing a source to lag or otherwise translate a delivered cursor
before persisting it.
