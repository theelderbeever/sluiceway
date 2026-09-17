# Sluiceway

Sluiceway is a typed, Tokio-based framework for ordered pipelines. A transformed batch is either
moved into one sink or shared with a fanout of type-erased sinks:

```text
Source -> transform -> LinearPipeline -> sink -> commit checkpoint
                  `-> FanoutPipeline -> sink --+
                                    `-> sink --+-> commit checkpoint
```

Applications import the `sluiceway` facade. The low-level engine lives in `sluiceway-core`, while
`sluiceway-io` provides opt-in durable checkpoint adapters.

Runnable linear and shared-fanout examples live under `sluiceway/examples`:

```shell
cargo run -p sluiceway --example linear
cargo run -p sluiceway --example shared
cargo run -p sluiceway --example each
cargo run -p sluiceway --example collect
```

## Packages

| Package | Purpose |
| --- | --- |
| `sluiceway` | Public facade and feature-gated adapter re-exports |
| `sluiceway-core` | Records, sources, transforms, sinks, checkpoints, and runners |
| `sluiceway-io` | Filesystem, object-store, and SQL checkpoint adapters |
| `sluiceway-contrib` | Feature-gated external source and sink integrations |

A linear sink owns and consumes each batch:

```rust,no_run
use std::{convert::Infallible, time::Duration};
use sluiceway::{Batch, BatchPolicy, Pipeline, PipelineId, Sink};

# async fn run<S, K>(source: S, sink: K) -> Result<(), Box<dyn std::error::Error>>
# where
#     S: sluiceway::Source<Payload = Vec<u8>>,
#     K: Sink<Batch<usize, S::Position>>,
#     S::Error: 'static,
# {
Pipeline::source(source)
    .id(PipelineId::new("payload-lengths")?)
    .transform(|_position: &S::Position, payload: Vec<u8>| async move {
        Ok::<_, Infallible>(payload.len())
    })
    .sink(sink)
    .batched(BatchPolicy::try_new(100, Duration::from_secs(1))?)
    .run()
    .await?;
# Ok(())
# }
```

Fanout sinks consume a shared batch. Concrete sink and error types are erased by `.into()` at the
fanout boundary, allowing heterogeneous sinks in one array:

```rust,no_run
# use std::{convert::Infallible, time::Duration};
# use sluiceway::{BatchPolicy, Pipeline, SharedBatch, Sink};
# async fn run<S, A, B>(source: S, analytics: A, archive: B) -> Result<(), Box<dyn std::error::Error>>
# where
#     S: sluiceway::Source<Payload = Vec<u8>>,
#     S::Error: 'static,
#     A: Sink<SharedBatch<Vec<u8>, S::Position>> + 'static,
#     B: Sink<SharedBatch<Vec<u8>, S::Position>> + 'static,
# {
Pipeline::source(source)
    .transform(|_position: &S::Position, payload: Vec<u8>| async move {
        Ok::<_, Infallible>(payload)
    })
    .fanout()
    .shared()
    .sinks([analytics.into(), archive.into()])
    .batched(BatchPolicy::try_new(100, Duration::from_secs(1))?)
    .run()
    .await?;
# Ok(())
# }
```

`Batch<T, P>` contains transformed `Record<T, P>` values. Fanout requires an
explicit ownership mode. `.cloned()` gives each sink an owned batch and can reuse linear sinks;
`.shared()` gives every sink the same `Arc<Batch<T, P>>`. `.sinks(...)` accepts arrays or dynamically
assembled vectors of the corresponding `BoxSink`. An empty fanout returns `PipelineError::NoSinks`
before the source starts. Every fanout task is drained, and the source checkpoint is committed only when
every sink succeeds.

Individual source records carry message-local positions. Transforms receive an immutable position
reference alongside each owned payload, and the resulting records retain those positions through
delivery. Collection and checkpoint cadence are independent: `.each()` delivers `Record<T, P>`
values individually, `.batched(...)` delivers materialized `Batch<T, P>` values, and
`.collect(...)` pushes records into an incremental `Collector` session before acknowledging the
collection with `finish()`. `CommitPolicy::each()`, `after(records)`, and
`after_or_timeout(records, timeout)` control when the successfully delivered frontier is persisted.
Checkpoint persistence remains source-owned through `CheckpointStore<C>`.

Commit timing follows delivery acknowledgements rather than interrupting delivery:

| Sink shape | Acknowledgement boundary | If the commit deadline passes in flight |
| --- | --- | --- |
| `.each()` | One successful record delivery | Commit after that record completes |
| `.batched(...)` | One successful whole-batch delivery | Commit after that batch completes |
| `.collect(...)` | A successful collector `finish()` | Commit after that collector session completes |

Fanout reaches the boundary only after every branch succeeds. The commit timer starts with the
first acknowledgement after the previous commit, and a passed deadline includes the newly
acknowledged unit in the committed frontier. Thus the timeout bounds waiting only while the runner
is at a safe boundary; it cannot bound a slow sink call. Clean EOF or graceful shutdown commits
remaining acknowledged progress. A failed delivery is not acknowledged and does not cause an
opportunistic commit.

`run()` consumes through the source stream's natural end. Sources that own graceful shutdown
should observe their configured signal inside `Source::stream`, stop external intake, drain any
source-owned buffer, and then return `None`. `run_until(shutdown)` instead imposes a cutoff on
source polling when its future resolves; transforms already admitted are completed and the final
partial batch is flushed.

Transforms run concurrently up to `Transform::max_concurrency` while their outputs remain in the
source stream's observed order. Batches are delivered one at a time. By default, the next batch is
not polled until delivery and commit finish. Configure `.prefetch(count)` on the
`BatchPolicy` to materialize up to `count` batches concurrently with serial delivery. A batch closes
at its size limit, at source EOF, or when its timeout expires; the timeout starts when the first
transformed record enters an empty batch. Sink delivery completes before commit begins, and any
transform or sink failure prevents that batch's checkpoint from being committed.

Checkpoint adapters are available through facade features. `io` enables local files,
`object-store` adds caller-configured object stores, and `sql-postgres`, `sql-mysql`, and
`sql-sqlite` add SQLx-backed stores. Adapters encode checkpoints as JSON, so structured checkpoint types
can be used when they implement Serde's `Serialize` and `DeserializeOwned` traits.

## Metrics

Enable the facade's `metrics` feature to emit pipeline metrics through the
[`metrics`](https://crates.io/crates/metrics) facade. Sluiceway does not install a recorder; the
application chooses and configures one (for example, a Prometheus exporter).

```toml
sluiceway = { version = "0.1.0", features = ["metrics"] }
```

Metric names and labels are intentionally bounded:

| Metric | Kind | Labels |
| --- | --- | --- |
| `sluiceway_pipeline_active` | gauge | `pipeline_id`, `topology` |
| `sluiceway_records_total` | counter | `pipeline_id`, `topology` |
| `sluiceway_batches_total` | counter | `pipeline_id`, `topology`, `reason` |
| `sluiceway_batch_records` | histogram | `pipeline_id`, `topology` |
| `sluiceway_sink_deliveries_total` | counter | `pipeline_id`, `topology`, `status` |
| `sluiceway_commits_total` | counter | `pipeline_id`, `topology`, `status` |
| `sluiceway_errors_total` | counter | `pipeline_id`, `topology`, `stage` |
| `sluiceway_stage_duration_seconds` | histogram | `pipeline_id`, `topology`, `stage` |

`topology` is one of `linear`, `fanout_cloned`, or `fanout_shared`. Stage durations cover
transforms, individual sink deliveries, and checkpoint commits. Metrics emission is compiled out when
the feature is disabled, and the optional dependency is omitted. `sluiceway-core` users can enable
its feature directly. Construct a stable `PipelineId` once and pass a clone to both `Pipeline::id`
and `SqlCheckpoint::with_id` to correlate metrics with durable progress. Pipelines without a
configured identity use the fixed `unnamed` label. The batch `reason` is `full` or `timeout`;
partial batches flushed when the source ends are included in `timeout`.
`PipelineId` permits ASCII letters, digits, `-`, `_`, `.`, `:`, and `/`; this invariant is enforced
when the value is constructed and does not need to be checked again by checkpoint or metrics code.

Kafka-compatible consumers, including Redpanda, are available from the separate
`sluiceway-contrib` crate with its `kafka` feature. The required payload deserializer and optional
key deserializer run directly against librdkafka's borrowed byte slices, so large payloads are not
copied before decoding. Without a key deserializer, keys are discarded and the record key type is
`()`. The source emits owned typed records with Kafka metadata and commits consumer-group offsets
only after the pipeline's sinks acknowledge a batch.

`sluiceway-contrib/examples/kafka_json.rs` demonstrates configuring key and JSON payload
deserializers on the source. Run it with
`cargo run -p sluiceway-contrib --features kafka --example kafka_json` and optionally set
`KAFKA_BOOTSTRAP_SERVERS` and `KAFKA_TOPIC`.
