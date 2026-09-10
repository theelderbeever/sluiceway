# Penstock

Penstock is a typed, Tokio-based framework for ordered pipelines. A transformed batch is either
moved into one sink or shared with a fanout of type-erased sinks:

```text
Source -> transform -> LinearPipeline -> sink -> commit checkpoint
                  `-> FanoutPipeline -> sink --+
                                    `-> sink --+-> commit checkpoint
```

Applications import the `penstock` facade. The low-level engine lives in `penstock-core`, while
`penstock-io` provides opt-in durable checkpoint adapters.

Runnable linear and shared-fanout examples live under `penstock/examples`:

```shell
cargo run -p penstock --example linear
cargo run -p penstock --example shared
```

## Packages

| Package | Purpose |
| --- | --- |
| `penstock` | Public facade and feature-gated adapter re-exports |
| `penstock-core` | Records, sources, transforms, sinks, checkpoints, and runners |
| `penstock-io` | Filesystem, object-store, and SQL checkpoint adapters |
| `penstock-contrib` | Feature-gated external source and sink integrations |

A linear sink owns and consumes each batch:

```rust,no_run
use std::{convert::Infallible, time::Duration};
use penstock::{Batch, BatchPolicy, Pipeline, PipelineId, Sink};

# async fn run<S, K>(source: S, sink: K) -> Result<(), Box<dyn std::error::Error>>
# where
#     S: penstock::Source<Payload = Vec<u8>>,
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
# use penstock::{BatchPolicy, Pipeline, SharedBatch, Sink};
# async fn run<S, A, B>(source: S, analytics: A, archive: B) -> Result<(), Box<dyn std::error::Error>>
# where
#     S: penstock::Source<Payload = Vec<u8>>,
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
delivery. When a batch closes, the source also folds the positions into an internal batch checkpoint.
Checkpoint persistence remains source-owned through `CheckpointStore<C>`.

`run()` consumes through the source stream's natural end. Sources that own graceful shutdown
should observe their configured signal inside `Source::stream`, stop external intake, drain any
source-owned buffer, and then return `None`. `run_until(shutdown)` instead imposes a cutoff on
source polling when its future resolves; transforms already admitted are completed and the final
partial batch is flushed.

Transforms run concurrently up to `Transform::max_concurrency` while their outputs remain in the
source stream's observed order. Batches are delivered one at a time. A batch closes at its size
limit, at source EOF, or when its timeout expires; the timeout starts when the first transformed
record enters an empty batch. Sink delivery completes before commit begins, and any transform or
sink failure prevents that batch's checkpoint from being committed.

Checkpoint adapters are available through facade features. `io` enables local files,
`object-store` adds caller-configured object stores, and `sql-postgres`, `sql-mysql`, and
`sql-sqlite` add SQLx-backed stores. Adapters encode checkpoints as JSON, so structured checkpoint types
can be used when they implement Serde's `Serialize` and `DeserializeOwned` traits.

## Metrics

Enable the facade's `metrics` feature to emit pipeline metrics through the
[`metrics`](https://crates.io/crates/metrics) facade. Penstock does not install a recorder; the
application chooses and configures one (for example, a Prometheus exporter).

```toml
penstock = { version = "0.1.0", features = ["metrics"] }
```

Metric names and labels are intentionally bounded:

| Metric | Kind | Labels |
| --- | --- | --- |
| `penstock_pipeline_active` | gauge | `pipeline_id`, `topology` |
| `penstock_records_total` | counter | `pipeline_id`, `topology` |
| `penstock_batches_total` | counter | `pipeline_id`, `topology`, `reason` |
| `penstock_batch_records` | histogram | `pipeline_id`, `topology` |
| `penstock_sink_deliveries_total` | counter | `pipeline_id`, `topology`, `status` |
| `penstock_commits_total` | counter | `pipeline_id`, `topology`, `status` |
| `penstock_errors_total` | counter | `pipeline_id`, `topology`, `stage` |
| `penstock_stage_duration_seconds` | histogram | `pipeline_id`, `topology`, `stage` |

`topology` is one of `linear`, `fanout_cloned`, or `fanout_shared`. Stage durations cover
transforms, individual sink deliveries, and checkpoint commits. Metrics emission is compiled out when
the feature is disabled, and the optional dependency is omitted. `penstock-core` users can enable
its feature directly. Construct a stable `PipelineId` once and pass a clone to both `Pipeline::id`
and `SqlCheckpoint::with_id` to correlate metrics with durable progress. Pipelines without a
configured identity use the fixed `unnamed` label. The batch `reason` is `full` or `timeout`;
partial batches flushed when the source ends are included in `timeout`.
`PipelineId` permits ASCII letters, digits, `-`, `_`, `.`, `:`, and `/`; this invariant is enforced
when the value is constructed and does not need to be checked again by checkpoint or metrics code.

Kafka-compatible consumers, including Redpanda, are available from the separate
`penstock-contrib` crate with its `kafka` feature. The required payload deserializer and optional
key deserializer run directly against librdkafka's borrowed byte slices, so large payloads are not
copied before decoding. Without a key deserializer, keys are discarded and the record key type is
`()`. The source emits owned typed records with Kafka metadata and commits consumer-group offsets
only after the pipeline's sinks acknowledge a batch.

`penstock-contrib/examples/kafka_json.rs` demonstrates configuring key and JSON payload
deserializers on the source. Run it with
`cargo run -p penstock-contrib --features kafka --example kafka_json` and optionally set
`KAFKA_BOOTSTRAP_SERVERS` and `KAFKA_TOPIC`.
