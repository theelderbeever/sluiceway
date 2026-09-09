use std::{
    cell::Cell,
    convert::Infallible,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use futures_core::Stream;
use futures_util::{StreamExt, stream};
use penstock::{
    Batch, BatchPolicy, BoxSink, CheckpointStore, DeliveryFailure, Identity, NoCheckpoint,
    Pipeline, PipelineError, Record, SharedBatch, Sink, Source,
};

#[derive(Debug, Clone, PartialEq, Eq)]
struct Cursor {
    partition: &'static str,
    offset: u64,
}

impl Cursor {
    fn at(offset: u64) -> Self {
        Self {
            partition: "test",
            offset,
        }
    }
}

struct Numbers {
    values: Vec<u64>,
    committed: Arc<Mutex<Vec<Cursor>>>,
}

impl Source for Numbers {
    type Payload = u64;
    type Position = Cursor;
    type Cursor = Cursor;
    type Error = Infallible;

    fn stream(
        &self,
    ) -> impl Stream<Item = Result<Record<Self::Payload, Self::Position>, Self::Error>> + Send + '_
    {
        stream::iter(
            self.values
                .clone()
                .into_iter()
                .enumerate()
                .map(|(offset, payload)| Ok(Record::new(Cursor::at(offset as u64), payload))),
        )
    }

    fn track(_cursor: Option<Self::Cursor>, position: Self::Position) -> Self::Cursor {
        position
    }

    async fn commit(&self, cursor: Self::Cursor) -> Result<(), Self::Error> {
        self.committed.lock().unwrap().push(cursor);
        Ok(())
    }
}

fn source(values: impl Into<Vec<u64>>) -> (Numbers, Arc<Mutex<Vec<Cursor>>>) {
    let committed = Arc::new(Mutex::new(Vec::new()));
    (
        Numbers {
            values: values.into(),
            committed: Arc::clone(&committed),
        },
        committed,
    )
}

#[derive(Debug, thiserror::Error)]
#[error("test sink failed")]
struct TestSinkError;

#[derive(Clone, Copy)]
enum Ack {
    Exact,
    Fail,
}

struct LinearCollector<T> {
    records: LinearRecords<T>,
    ack: Ack,
}

type LinearRecords<T> = Arc<Mutex<Vec<T>>>;

impl<T: Send> Sink<Batch<T, Cursor>> for LinearCollector<T> {
    type Error = TestSinkError;

    async fn deliver(&self, batch: Batch<T, Cursor>) -> Result<(), Self::Error> {
        self.records.lock().unwrap().extend(batch);
        acknowledge(self.ack)
    }
}

fn linear_collector<T>(ack: Ack) -> (LinearCollector<T>, LinearRecords<T>) {
    let records = Arc::new(Mutex::new(Vec::new()));
    (
        LinearCollector {
            records: Arc::clone(&records),
            ack,
        },
        records,
    )
}

type SharedBatches<T> = Arc<Mutex<Vec<SharedBatch<T, Cursor>>>>;

struct SharedCollector<T> {
    batches: SharedBatches<T>,
    ack: Ack,
}

impl<T: Send + Sync + 'static> Sink<SharedBatch<T, Cursor>> for SharedCollector<T> {
    type Error = TestSinkError;

    async fn deliver(&self, batch: SharedBatch<T, Cursor>) -> Result<(), Self::Error> {
        self.batches.lock().unwrap().push(batch);
        acknowledge(self.ack)
    }
}

fn shared_collector<T>(ack: Ack) -> (SharedCollector<T>, SharedBatches<T>) {
    let batches = Arc::new(Mutex::new(Vec::new()));
    (
        SharedCollector {
            batches: Arc::clone(&batches),
            ack,
        },
        batches,
    )
}

fn acknowledge(ack: Ack) -> Result<(), TestSinkError> {
    match ack {
        Ack::Exact => Ok(()),
        Ack::Fail => Err(TestSinkError),
    }
}

struct CountingSink {
    records: Arc<AtomicUsize>,
}

impl Sink<SharedBatch<String, Cursor>> for CountingSink {
    type Error = Infallible;

    async fn deliver(&self, batch: SharedBatch<String, Cursor>) -> Result<(), Self::Error> {
        self.records.fetch_add(batch.len(), Ordering::SeqCst);
        Ok(())
    }
}

struct OwnedCountingSink {
    records: Arc<AtomicUsize>,
}

impl Sink<Batch<String, Cursor>> for OwnedCountingSink {
    type Error = Infallible;

    async fn deliver(&self, batch: Batch<String, Cursor>) -> Result<(), Self::Error> {
        self.records.fetch_add(batch.len(), Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn linear_pipeline_moves_owned_transformed_batches_to_one_sink() {
    let (source, committed) = source(vec![2, 30, 400]);
    let (sink, records) = linear_collector(Ack::Exact);

    Pipeline::source(source)
        .transform(|number: u64| async move { Ok::<_, Infallible>(number.to_string()) })
        .sink(sink)
        .batched(BatchPolicy::try_new(2, Duration::from_secs(1)).unwrap())
        .run_until(std::future::pending())
        .await
        .unwrap();

    assert_eq!(
        records
            .lock()
            .unwrap()
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        vec!["2", "30", "400"]
    );
    assert_eq!(
        *committed.lock().unwrap(),
        vec![Cursor::at(1), Cursor::at(2)]
    );
}

#[tokio::test]
async fn cloned_fanout_reuses_owned_sinks() {
    let (source, committed) = source(vec![2, 30, 400]);
    let (collector, records) = linear_collector(Ack::Exact);
    let count = Arc::new(AtomicUsize::new(0));
    let counter = OwnedCountingSink {
        records: Arc::clone(&count),
    };

    Pipeline::source(source)
        .transform(|number: u64| async move { Ok::<_, Infallible>(number.to_string()) })
        .fanout()
        .cloned()
        .sinks([collector.into(), counter.into()])
        .batched(BatchPolicy::try_new(2, Duration::from_secs(1)).unwrap())
        .run_until(std::future::pending())
        .await
        .unwrap();

    assert_eq!(records.lock().unwrap().len(), 3);
    assert_eq!(count.load(Ordering::SeqCst), 3);
    assert_eq!(
        *committed.lock().unwrap(),
        vec![Cursor::at(1), Cursor::at(2)]
    );
}

#[tokio::test]
async fn cloned_fanout_does_not_require_sync_payloads() {
    let (source, _) = source(vec![7]);
    let (first, first_records) = linear_collector(Ack::Exact);
    let (second, second_records) = linear_collector(Ack::Exact);

    Pipeline::source(source)
        .transform(|number| async move { Ok::<_, Infallible>(Cell::new(number)) })
        .fanout()
        .cloned()
        .sinks([first.into(), second.into()])
        .batched(BatchPolicy::try_new(1, Duration::from_secs(1)).unwrap())
        .run_until(std::future::pending())
        .await
        .unwrap();

    assert_eq!(first_records.lock().unwrap()[0].get(), 7);
    assert_eq!(second_records.lock().unwrap()[0].get(), 7);
}

struct NonClone(u64);

#[tokio::test]
async fn shared_fanout_does_not_require_clone_payloads() {
    let (source, _) = source(vec![7]);
    let (first, first_batches) = shared_collector(Ack::Exact);
    let (second, second_batches) = shared_collector(Ack::Exact);

    Pipeline::source(source)
        .transform(|number| async move { Ok::<_, Infallible>(NonClone(number)) })
        .fanout()
        .shared()
        .sinks([first.into(), second.into()])
        .batched(BatchPolicy::try_new(1, Duration::from_secs(1)).unwrap())
        .run_until(std::future::pending())
        .await
        .unwrap();

    assert_eq!(first_batches.lock().unwrap()[0][0].0, 7);
    assert_eq!(second_batches.lock().unwrap()[0][0].0, 7);
}

#[tokio::test]
async fn fanout_accepts_heterogeneous_array_and_shares_whole_batches() {
    let (source, committed) = source(vec![2, 30, 400]);
    let transform_calls = Arc::new(AtomicUsize::new(0));
    let transform = {
        let calls = Arc::clone(&transform_calls);
        move |number: u64| {
            calls.fetch_add(1, Ordering::SeqCst);
            async move { Ok::<_, Infallible>(number.to_string()) }
        }
    };
    let (first, first_batches) = shared_collector(Ack::Exact);
    let (second, second_batches) = shared_collector(Ack::Exact);
    let count = Arc::new(AtomicUsize::new(0));
    let counter = CountingSink {
        records: Arc::clone(&count),
    };

    Pipeline::source(source)
        .transform(transform)
        .fanout()
        .shared()
        .sinks([first.into(), second.into(), counter.into()])
        .batched(BatchPolicy::try_new(2, Duration::from_secs(1)).unwrap())
        .run_until(std::future::pending())
        .await
        .unwrap();

    assert_eq!(transform_calls.load(Ordering::SeqCst), 3);
    assert_eq!(count.load(Ordering::SeqCst), 3);
    assert_eq!(
        *committed.lock().unwrap(),
        vec![Cursor::at(1), Cursor::at(2)]
    );
    let first = first_batches.lock().unwrap();
    let second = second_batches.lock().unwrap();
    assert_eq!(first.len(), 2);
    assert_eq!(second.len(), 2);
    assert_eq!(first[0].cursor, Cursor::at(1));
    assert_eq!(first[1].cursor, Cursor::at(2));
    assert!(Arc::ptr_eq(&first[0], &second[0]));
    assert!(Arc::ptr_eq(&first[1], &second[1]));
}

#[tokio::test]
async fn fanout_accepts_a_dynamically_built_sink_vec() {
    let (source, committed) = source(vec![7]);
    let (first, first_batches) = shared_collector(Ack::Exact);
    let (second, second_batches) = shared_collector(Ack::Exact);
    let mut sinks: Vec<BoxSink<u64, Cursor>> = vec![first.into()];
    sinks.push(second.into());

    Pipeline::source(source)
        .transform(Identity)
        .fanout()
        .shared()
        .sinks(sinks)
        .batched(BatchPolicy::try_new(1, Duration::from_secs(1)).unwrap())
        .run_until(std::future::pending())
        .await
        .unwrap();

    assert_eq!(*committed.lock().unwrap(), vec![Cursor::at(0)]);
    assert!(Arc::ptr_eq(
        &first_batches.lock().unwrap()[0],
        &second_batches.lock().unwrap()[0]
    ));
}

struct StartedSource {
    started: Arc<AtomicBool>,
}

impl Source for StartedSource {
    type Payload = u64;
    type Position = Cursor;
    type Cursor = Cursor;
    type Error = Infallible;

    fn stream(
        &self,
    ) -> impl Stream<Item = Result<Record<Self::Payload, Self::Position>, Self::Error>> + Send + '_
    {
        self.started.store(true, Ordering::SeqCst);
        stream::empty()
    }

    fn track(_cursor: Option<Self::Cursor>, position: Self::Position) -> Self::Cursor {
        position
    }

    async fn commit(&self, _cursor: Self::Cursor) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[tokio::test]
async fn empty_fanout_fails_before_starting_the_source() {
    let started = Arc::new(AtomicBool::new(false));
    let source = StartedSource {
        started: Arc::clone(&started),
    };
    let sinks: Vec<BoxSink<u64, Cursor>> = Vec::new();

    let result = Pipeline::source(source)
        .transform(Identity)
        .fanout()
        .shared()
        .sinks(sinks)
        .batched(BatchPolicy::try_new(1, Duration::from_secs(1)).unwrap())
        .run_until(std::future::pending())
        .await;

    assert!(matches!(result, Err(PipelineError::NoSinks)));
    assert!(!started.load(Ordering::SeqCst));
}

enum ProbeBehavior {
    Exact,
    Fail,
    Slow(Arc<AtomicBool>),
    Barrier(Arc<tokio::sync::Barrier>),
    Panic,
}

struct ProbeSink {
    behavior: ProbeBehavior,
}

impl Sink<SharedBatch<u64, Cursor>> for ProbeSink {
    type Error = TestSinkError;

    async fn deliver(&self, _batch: SharedBatch<u64, Cursor>) -> Result<(), Self::Error> {
        match &self.behavior {
            ProbeBehavior::Exact => Ok(()),
            ProbeBehavior::Fail => Err(TestSinkError),
            ProbeBehavior::Slow(completed) => {
                tokio::time::sleep(Duration::from_millis(20)).await;
                completed.store(true, Ordering::SeqCst);
                Ok(())
            }
            ProbeBehavior::Barrier(barrier) => {
                barrier.wait().await;
                Ok(())
            }
            ProbeBehavior::Panic => panic!("sink panic"),
        }
    }
}

#[tokio::test]
async fn fanout_sinks_run_concurrently() {
    let (source, committed) = source(vec![7]);
    let barrier = Arc::new(tokio::sync::Barrier::new(3));
    let sink = || ProbeSink {
        behavior: ProbeBehavior::Barrier(Arc::clone(&barrier)),
    };

    tokio::time::timeout(
        Duration::from_secs(1),
        Pipeline::source(source)
            .transform(Identity)
            .fanout()
            .shared()
            .sinks([sink().into(), sink().into(), sink().into()])
            .batched(BatchPolicy::try_new(1, Duration::from_secs(1)).unwrap())
            .run_until(std::future::pending()),
    )
    .await
    .expect("all sinks should reach the barrier")
    .unwrap();

    assert_eq!(*committed.lock().unwrap(), vec![Cursor::at(0)]);
}

#[tokio::test]
async fn fanout_drains_started_sinks_and_reports_failures_without_metadata() {
    let (source, committed) = source(vec![1]);
    let completed = Arc::new(AtomicBool::new(false));
    let slow = ProbeSink {
        behavior: ProbeBehavior::Slow(Arc::clone(&completed)),
    };
    let failed = ProbeSink {
        behavior: ProbeBehavior::Fail,
    };

    let result = Pipeline::source(source)
        .transform(Identity)
        .fanout()
        .shared()
        .sinks([slow.into(), failed.into()])
        .batched(BatchPolicy::try_new(1, Duration::from_secs(1)).unwrap())
        .run_until(std::future::pending())
        .await;

    let failures = match result {
        Err(PipelineError::Sinks(failures)) => failures,
        other => panic!("expected sink failures, got {other:?}"),
    };
    assert_eq!(failures.len(), 1);
    assert!(matches!(failures[0], DeliveryFailure::Sink(_)));
    assert!(completed.load(Ordering::SeqCst));
    assert!(committed.lock().unwrap().is_empty());
}

#[tokio::test]
async fn fanout_sink_panics_are_reported_and_prevent_commit() {
    let (source, committed) = source(vec![1]);
    let panics = ProbeSink {
        behavior: ProbeBehavior::Panic,
    };
    let succeeds = ProbeSink {
        behavior: ProbeBehavior::Exact,
    };

    let result = Pipeline::source(source)
        .transform(Identity)
        .fanout()
        .shared()
        .sinks([panics.into(), succeeds.into()])
        .batched(BatchPolicy::try_new(1, Duration::from_secs(1)).unwrap())
        .run_until(std::future::pending())
        .await;

    let failures = match result {
        Err(PipelineError::Sinks(failures)) => failures,
        other => panic!("expected sink task failure, got {other:?}"),
    };
    assert!(matches!(failures[0], DeliveryFailure::Task(_)));
    assert!(committed.lock().unwrap().is_empty());
}

#[tokio::test]
async fn sink_failure_prevents_commit() {
    let (source, committed) = source(vec![1]);
    let (sink, _) = linear_collector(Ack::Fail);

    let result = Pipeline::source(source)
        .transform(Identity)
        .sink(sink)
        .batched(BatchPolicy::try_new(1, Duration::from_secs(1)).unwrap())
        .run_until(std::future::pending())
        .await;

    assert!(matches!(
        result,
        Err(PipelineError::Sink(DeliveryFailure::Sink(_)))
    ));
    assert!(committed.lock().unwrap().is_empty());
}

#[tokio::test]
async fn no_checkpoint_is_generic_over_cursor_type() {
    let store = Some(NoCheckpoint);
    assert_eq!(
        <Option<NoCheckpoint> as CheckpointStore<Cursor>>::load(&store)
            .await
            .unwrap(),
        None
    );
    <Option<NoCheckpoint> as CheckpointStore<Cursor>>::save(&store, &Cursor::at(4))
        .await
        .unwrap();
}

#[test]
fn batching_rejects_zero_bounds() {
    assert!(matches!(
        BatchPolicy::try_new(0, Duration::from_secs(1)),
        Err(penstock::BatchConfigError::ZeroSize)
    ));
    assert!(matches!(
        BatchPolicy::try_new(1, Duration::ZERO),
        Err(penstock::BatchConfigError::ZeroTimeout)
    ));
}

#[derive(Debug, thiserror::Error)]
#[error("source failed")]
struct SourceFailure;

struct FallibleSource {
    fail_stream: bool,
    committed: Arc<AtomicBool>,
}

impl Source for FallibleSource {
    type Payload = u64;
    type Position = Cursor;
    type Cursor = Cursor;
    type Error = SourceFailure;

    fn stream(
        &self,
    ) -> impl Stream<Item = Result<Record<Self::Payload, Self::Position>, Self::Error>> + Send + '_
    {
        stream::iter(if self.fail_stream {
            vec![Err(SourceFailure)]
        } else {
            vec![Ok(Record::new(Cursor::at(0), 1))]
        })
    }

    fn track(_cursor: Option<Self::Cursor>, position: Self::Position) -> Self::Cursor {
        position
    }

    async fn commit(&self, _cursor: Self::Cursor) -> Result<(), Self::Error> {
        self.committed.store(true, Ordering::SeqCst);
        Err(SourceFailure)
    }
}

#[derive(Debug, thiserror::Error)]
#[error("transform failed")]
struct TransformFailure;

#[tokio::test]
async fn source_transform_and_commit_errors_retain_their_stage() {
    let (sink, _) = linear_collector::<u64>(Ack::Exact);
    let failing_source = FallibleSource {
        fail_stream: true,
        committed: Arc::new(AtomicBool::new(false)),
    };
    let source_result = Pipeline::source(failing_source)
        .transform(Identity)
        .sink(sink)
        .batched(BatchPolicy::try_new(1, Duration::from_secs(1)).unwrap())
        .run_until(std::future::pending())
        .await;
    assert!(matches!(source_result, Err(PipelineError::Source(_))));

    let (source, _) = source(vec![1]);
    let (sink, _) = linear_collector::<u64>(Ack::Exact);
    let transform_result = Pipeline::source(source)
        .transform(|_: u64| async move { Err::<u64, _>(TransformFailure) })
        .sink(sink)
        .batched(BatchPolicy::try_new(1, Duration::from_secs(1)).unwrap())
        .run_until(std::future::pending())
        .await;
    assert!(matches!(
        transform_result,
        Err(PipelineError::Transform(TransformFailure))
    ));

    let committed = Arc::new(AtomicBool::new(false));
    let source = FallibleSource {
        fail_stream: false,
        committed: Arc::clone(&committed),
    };
    let (sink, _) = linear_collector::<u64>(Ack::Exact);
    let commit_result = Pipeline::source(source)
        .transform(Identity)
        .sink(sink)
        .batched(BatchPolicy::try_new(1, Duration::from_secs(1)).unwrap())
        .run_until(std::future::pending())
        .await;
    assert!(matches!(commit_result, Err(PipelineError::Commit(_))));
    assert!(committed.load(Ordering::SeqCst));
}

struct DelayedSource {
    committed: Arc<Mutex<Vec<Cursor>>>,
}

impl Source for DelayedSource {
    type Payload = u64;
    type Position = Cursor;
    type Cursor = Cursor;
    type Error = Infallible;

    fn stream(
        &self,
    ) -> impl Stream<Item = Result<Record<Self::Payload, Self::Position>, Self::Error>> + Send + '_
    {
        stream::iter(0..3_u64).then(|offset| async move {
            if offset == 1 {
                tokio::time::sleep(Duration::from_millis(30)).await;
            }
            Ok(Record::new(Cursor::at(offset), offset))
        })
    }

    fn track(_cursor: Option<Self::Cursor>, position: Self::Position) -> Self::Cursor {
        position
    }

    async fn commit(&self, cursor: Self::Cursor) -> Result<(), Self::Error> {
        self.committed.lock().unwrap().push(cursor);
        Ok(())
    }
}

#[tokio::test]
async fn timeout_and_end_of_stream_flush_partial_batches() {
    let committed = Arc::new(Mutex::new(Vec::new()));
    let source = DelayedSource {
        committed: Arc::clone(&committed),
    };
    let (sink, _) = linear_collector(Ack::Exact);

    Pipeline::source(source)
        .transform(Identity)
        .sink(sink)
        .batched(BatchPolicy::try_new(10, Duration::from_millis(10)).unwrap())
        .run_until(std::future::pending())
        .await
        .unwrap();

    assert_eq!(
        *committed.lock().unwrap(),
        vec![Cursor::at(0), Cursor::at(2)]
    );
}

struct OpenSource {
    committed: Arc<Mutex<Vec<Cursor>>>,
}

impl Source for OpenSource {
    type Payload = u64;
    type Position = Cursor;
    type Cursor = Cursor;
    type Error = Infallible;

    fn stream(
        &self,
    ) -> impl Stream<Item = Result<Record<Self::Payload, Self::Position>, Self::Error>> + Send + '_
    {
        stream::once(async { Ok(Record::new(Cursor::at(0), 1)) }).chain(stream::pending())
    }

    fn track(_cursor: Option<Self::Cursor>, position: Self::Position) -> Self::Cursor {
        position
    }

    async fn commit(&self, cursor: Self::Cursor) -> Result<(), Self::Error> {
        self.committed.lock().unwrap().push(cursor);
        Ok(())
    }
}

#[tokio::test]
async fn shutdown_drains_admitted_transform_and_flushes_its_batch() {
    let committed = Arc::new(Mutex::new(Vec::new()));
    let source = OpenSource {
        committed: Arc::clone(&committed),
    };
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let shutdown_tx = Arc::new(Mutex::new(Some(shutdown_tx)));
    let transform = {
        let shutdown_tx = Arc::clone(&shutdown_tx);
        move |value: u64| {
            shutdown_tx
                .lock()
                .unwrap()
                .take()
                .unwrap()
                .send(())
                .unwrap();
            async move { Ok::<_, Infallible>(value) }
        }
    };
    let (sink, records) = linear_collector(Ack::Exact);

    Pipeline::source(source)
        .transform(transform)
        .sink(sink)
        .batched(BatchPolicy::try_new(10, Duration::from_secs(1)).unwrap())
        .run_until(async {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();

    assert_eq!(*committed.lock().unwrap(), vec![Cursor::at(0)]);
    assert_eq!(records.lock().unwrap().len(), 1);
}

struct PositionedSource {
    cursors: Vec<Cursor>,
    committed: Arc<Mutex<Vec<Cursor>>>,
}

impl Source for PositionedSource {
    type Payload = ();
    type Position = Cursor;
    type Cursor = Cursor;
    type Error = Infallible;

    fn stream(
        &self,
    ) -> impl Stream<Item = Result<Record<Self::Payload, Self::Position>, Self::Error>> + Send + '_
    {
        stream::iter(
            self.cursors
                .clone()
                .into_iter()
                .map(|cursor| Ok(Record::new(cursor, ()))),
        )
    }

    fn track(_cursor: Option<Self::Cursor>, position: Self::Position) -> Self::Cursor {
        position
    }

    async fn commit(&self, cursor: Self::Cursor) -> Result<(), Self::Error> {
        self.committed.lock().unwrap().push(cursor);
        Ok(())
    }
}

#[tokio::test]
async fn repeated_and_non_monotonic_cursors_remain_opaque() {
    let committed = Arc::new(Mutex::new(Vec::new()));
    let source = PositionedSource {
        cursors: vec![Cursor::at(4), Cursor::at(4), Cursor::at(2)],
        committed: Arc::clone(&committed),
    };
    let (sink, _) = linear_collector(Ack::Exact);

    Pipeline::source(source)
        .transform(Identity)
        .sink(sink)
        .batched(BatchPolicy::try_new(2, Duration::from_secs(1)).unwrap())
        .run_until(std::future::pending())
        .await
        .unwrap();

    assert_eq!(
        *committed.lock().unwrap(),
        vec![Cursor::at(4), Cursor::at(2)]
    );
}
