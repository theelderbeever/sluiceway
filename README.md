# Penstock

Penstock is a typed, Tokio-based framework for ordered, branching pipelines:

```text
Source -> shared transform -> branch transform -> sink --+
                           `-> branch transform -> sink --+-> commit cursor
```

Applications import the `penstock` facade. The low-level engine lives in `penstock-core`, while
`penstock-io` provides opt-in durable checkpoint adapters.

## Packages

| Package | Purpose |
| --- | --- |
| `penstock` | Public facade and feature-gated adapter re-exports |
| `penstock-core` | Records, sources, transforms, sinks, checkpoints, branches, and runner |
| `penstock-io` | Filesystem, object-store, and SQL checkpoint adapters |

```rust,no_run
use std::{convert::Infallible, sync::Arc, time::Duration};
use penstock::{BatchPolicy, Branch, Pipeline};

# async fn run<S, A, B>(source: S, analytics: A, archive: B) -> Result<(), Box<dyn std::error::Error>>
# where
#     S: penstock::Source<Payload = Vec<u8>>,
#     A: penstock::Sink<usize, Cursor = S::Cursor> + 'static,
#     B: penstock::Sink<Vec<u8>, Cursor = S::Cursor> + 'static,
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

Every registered branch runs automatically. For each batch, the runner spawns one task per branch
into an internal `JoinSet`, drains every task, validates every sink's cursor acknowledgment, and
commits only when all branches succeed.

The cursor is opaque to the runner: it needs equality for acknowledgment validation but does not
need to be numeric or ordered. Checkpoint persistence remains source-owned through
`CheckpointStore<C>`, allowing a source to lag or otherwise translate progress before persisting
it.

Checkpoint adapters are available through facade features. `io` enables local files,
`object-store` adds caller-configured object stores, and `sql-postgres`, `sql-mysql`, and
`sql-sqlite` add SQLx-backed stores. Adapters encode cursors as JSON, so structured cursor types
can be used when they implement Serde's `Serialize` and `DeserializeOwned` traits.
