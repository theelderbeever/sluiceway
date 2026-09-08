use std::{
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
    BatchPolicy, Branch, BranchStage, CheckpointStore, Identity, NoCheckpoint, Pipeline,
    PipelineError, Record, Sink, Source,
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
    type Cursor = Cursor;
    type Error = Infallible;

    fn stream(
        &self,
    ) -> impl Stream<Item = Result<Record<Self::Payload, Self::Cursor>, Self::Error>> + Send + '_
    {
        stream::iter(
            self.values
                .clone()
                .into_iter()
                .enumerate()
                .map(|(offset, payload)| Ok(Record::new(Cursor::at(offset as u64), payload))),
        )
    }

    async fn commit(&self, cursor: Self::Cursor) -> Result<(), Self::Error> {
        self.committed.lock().unwrap().push(cursor);
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
#[error("test sink failed")]
struct SinkFailure;

enum Ack {
    Exact,
    Next,
    Fail,
}

struct Collected<T> {
    records: CollectedRecords<T>,
    ack: Ack,
}

type CollectedRecords<T> = Arc<Mutex<Vec<Record<T, Cursor>>>>;

impl<T: Send> Sink<T> for Collected<T> {
    type Cursor = Cursor;
    type Error = SinkFailure;

    async fn deliver(
        &self,
        batch: Vec<Record<T, Self::Cursor>>,
    ) -> Result<Self::Cursor, Self::Error> {
        let cursor = batch.last().expect("batches are non-empty").cursor.clone();
        self.records.lock().unwrap().extend(batch);
        match self.ack {
            Ack::Exact => Ok(cursor),
            Ack::Next => Ok(Cursor::at(cursor.offset + 1)),
            Ack::Fail => Err(SinkFailure),
        }
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

fn collector<T>(ack: Ack) -> (Collected<T>, CollectedRecords<T>) {
    let records = Arc::new(Mutex::new(Vec::new()));
    (
        Collected {
            records: Arc::clone(&records),
            ack,
        },
        records,
    )
}

#[tokio::test]
async fn registered_typed_branches_run_and_commit_together() {
    let (source, committed) = source(vec![2, 30, 400]);
    let shared_calls = Arc::new(AtomicUsize::new(0));
    let shared = {
        let calls = Arc::clone(&shared_calls);
        move |number: u64| {
            calls.fetch_add(1, Ordering::SeqCst);
            async move { Ok::<_, Infallible>(number.to_string()) }
        }
    };
    let (lengths, length_records) = collector(Ack::Exact);
    let (uppercase, uppercase_records) = collector(Ack::Exact);
    let (identity, identity_records) = collector(Ack::Exact);

    Pipeline::source(source)
        .transform(shared)
        .branch(
            Branch::new("lengths", |value: Arc<String>| async move {
                Ok::<_, Infallible>(value.len())
            })
            .sink(lengths),
        )
        .branch(
            Branch::new("uppercase", |value: Arc<String>| async move {
                Ok::<_, Infallible>(value.to_uppercase())
            })
            .sink(uppercase),
        )
        .branch(Branch::identity("identity").sink(identity))
        .batched(BatchPolicy::try_new(2, Duration::from_secs(1)).unwrap())
        .run_until(std::future::pending())
        .await
        .unwrap();

    assert_eq!(shared_calls.load(Ordering::SeqCst), 3);
    assert_eq!(
        *committed.lock().unwrap(),
        vec![Cursor::at(1), Cursor::at(2)]
    );
    assert_eq!(
        length_records
            .lock()
            .unwrap()
            .iter()
            .map(|record| record.payload)
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert_eq!(
        uppercase_records
            .lock()
            .unwrap()
            .iter()
            .map(|record| record.payload.as_str())
            .collect::<Vec<_>>(),
        vec!["2", "30", "400"]
    );
    assert_eq!(
        identity_records
            .lock()
            .unwrap()
            .iter()
            .map(|record| record.payload.as_str())
            .collect::<Vec<_>>(),
        vec!["2", "30", "400"]
    );
}

#[tokio::test]
async fn join_set_runs_all_registered_branches_concurrently() {
    let (source, committed) = source(vec![7]);
    let barrier = Arc::new(tokio::sync::Barrier::new(3));
    let (one, _) = collector(Ack::Exact);
    let (two, _) = collector(Ack::Exact);
    let (three, _) = collector(Ack::Exact);

    let branch = |name, sink| {
        let barrier = Arc::clone(&barrier);
        Branch::new(name, move |value: Arc<u64>| {
            let barrier = Arc::clone(&barrier);
            async move {
                barrier.wait().await;
                Ok::<_, Infallible>(*value)
            }
        })
        .sink(sink)
    };

    tokio::time::timeout(
        Duration::from_secs(1),
        Pipeline::source(source)
            .transform(Identity)
            .branch(branch("one", one))
            .branch(branch("two", two))
            .branch(branch("three", three))
            .batched(BatchPolicy::try_new(1, Duration::from_secs(1)).unwrap())
            .run_until(std::future::pending()),
    )
    .await
    .expect("all branch transforms should reach the barrier")
    .unwrap();

    assert_eq!(*committed.lock().unwrap(), vec![Cursor::at(0)]);
}

#[tokio::test]
async fn cursor_mismatch_prevents_commit() {
    let (source, committed) = source(vec![1]);
    let (sink, _) = collector(Ack::Next);

    let result = Pipeline::source(source)
        .transform(Identity)
        .branch(Branch::identity("wrong-cursor").sink(sink))
        .batched(BatchPolicy::try_new(1, Duration::from_secs(1)).unwrap())
        .run_until(std::future::pending())
        .await;

    let failures = match result {
        Err(PipelineError::Branches(failures)) => failures,
        other => panic!("expected branch failure, got {other:?}"),
    };
    assert_eq!(failures.len(), 1);
    assert_eq!(failures[0].stage, BranchStage::Cursor);
    assert_eq!(failures[0].name, "wrong-cursor");
    assert!(committed.lock().unwrap().is_empty());
}

#[tokio::test]
async fn sink_failure_drains_other_started_branches_before_returning() {
    let (source, committed) = source(vec![1]);
    let completed = Arc::new(AtomicBool::new(false));
    let (slow, _) = collector(Ack::Exact);
    let (failed, _) = collector(Ack::Fail);
    let slow_transform = {
        let completed = Arc::clone(&completed);
        move |value: Arc<u64>| {
            let completed = Arc::clone(&completed);
            async move {
                tokio::time::sleep(Duration::from_millis(20)).await;
                completed.store(true, Ordering::SeqCst);
                Ok::<_, Infallible>(*value)
            }
        }
    };

    let result = Pipeline::source(source)
        .transform(Identity)
        .branch(Branch::new("slow", slow_transform).sink(slow))
        .branch(Branch::identity("failed").sink(failed))
        .batched(BatchPolicy::try_new(1, Duration::from_secs(1)).unwrap())
        .run_until(std::future::pending())
        .await;

    assert!(matches!(result, Err(PipelineError::Branches(_))));
    assert!(completed.load(Ordering::SeqCst));
    assert!(committed.lock().unwrap().is_empty());
}

#[tokio::test]
async fn all_branch_failures_are_reported_in_registration_order() {
    let (source, committed) = source(vec![1]);
    let (first, _) = collector::<Arc<u64>>(Ack::Fail);
    let (second, _) = collector::<Arc<u64>>(Ack::Fail);

    let result = Pipeline::source(source)
        .transform(Identity)
        .branch(Branch::identity("first").sink(first))
        .branch(Branch::identity("second").sink(second))
        .batched(BatchPolicy::try_new(1, Duration::from_secs(1)).unwrap())
        .run_until(std::future::pending())
        .await;

    let failures = match result {
        Err(PipelineError::Branches(failures)) => failures,
        other => panic!("expected branch failures, got {other:?}"),
    };
    assert_eq!(
        failures
            .iter()
            .map(|failure| failure.name.as_str())
            .collect::<Vec<_>>(),
        vec!["first", "second"]
    );
    assert!(
        failures
            .iter()
            .all(|failure| failure.stage == BranchStage::Sink)
    );
    assert!(committed.lock().unwrap().is_empty());
}

#[tokio::test]
async fn branch_task_panics_are_reported_and_prevent_commit() {
    let (source, committed) = source(vec![1]);
    let (sink, _) = collector::<u64>(Ack::Exact);
    let panic_transform = |_: Arc<u64>| async move {
        panic!("branch panic");
        #[allow(unreachable_code)]
        Ok::<_, Infallible>(0)
    };

    let result = Pipeline::source(source)
        .transform(Identity)
        .branch(Branch::new("panics", panic_transform).sink(sink))
        .batched(BatchPolicy::try_new(1, Duration::from_secs(1)).unwrap())
        .run_until(std::future::pending())
        .await;

    let failures = match result {
        Err(PipelineError::Branches(failures)) => failures,
        other => panic!("expected task failure, got {other:?}"),
    };
    assert_eq!(failures[0].stage, BranchStage::Task);
    assert_eq!(failures[0].name, "panics");
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
    type Cursor = Cursor;
    type Error = SourceFailure;

    fn stream(
        &self,
    ) -> impl Stream<Item = Result<Record<Self::Payload, Self::Cursor>, Self::Error>> + Send + '_
    {
        stream::iter(if self.fail_stream {
            vec![Err(SourceFailure)]
        } else {
            vec![Ok(Record::new(Cursor::at(0), 1))]
        })
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
async fn source_shared_transform_and_commit_errors_retain_their_stage() {
    let (sink, _) = collector::<Arc<u64>>(Ack::Exact);
    let failing_source = FallibleSource {
        fail_stream: true,
        committed: Arc::new(AtomicBool::new(false)),
    };
    let source_result = Pipeline::source(failing_source)
        .transform(Identity)
        .branch(Branch::identity("sink").sink(sink))
        .batched(BatchPolicy::try_new(1, Duration::from_secs(1)).unwrap())
        .run_until(std::future::pending())
        .await;
    assert!(matches!(source_result, Err(PipelineError::Source(_))));

    let (source, _) = source(vec![1]);
    let (sink, _) = collector::<Arc<u64>>(Ack::Exact);
    let transform_result = Pipeline::source(source)
        .transform(|_: u64| async move { Err::<u64, _>(TransformFailure) })
        .branch(Branch::identity("sink").sink(sink))
        .batched(BatchPolicy::try_new(1, Duration::from_secs(1)).unwrap())
        .run_until(std::future::pending())
        .await;
    assert!(matches!(
        transform_result,
        Err(PipelineError::SharedTransform(TransformFailure))
    ));

    let committed = Arc::new(AtomicBool::new(false));
    let source = FallibleSource {
        fail_stream: false,
        committed: Arc::clone(&committed),
    };
    let (sink, _) = collector::<Arc<u64>>(Ack::Exact);
    let commit_result = Pipeline::source(source)
        .transform(Identity)
        .branch(Branch::identity("sink").sink(sink))
        .batched(BatchPolicy::try_new(1, Duration::from_secs(1)).unwrap())
        .run_until(std::future::pending())
        .await;
    assert!(matches!(commit_result, Err(PipelineError::Commit(_))));
    assert!(committed.load(Ordering::SeqCst));
}

#[tokio::test]
async fn branch_transform_failure_prevents_delivery_and_commit() {
    let (source, committed) = source(vec![1]);
    let (sink, records) = collector::<u64>(Ack::Exact);

    let result = Pipeline::source(source)
        .transform(Identity)
        .branch(
            Branch::new("transform", |_: Arc<u64>| async move {
                Err::<u64, _>(TransformFailure)
            })
            .sink(sink),
        )
        .batched(BatchPolicy::try_new(1, Duration::from_secs(1)).unwrap())
        .run_until(std::future::pending())
        .await;

    let failures = match result {
        Err(PipelineError::Branches(failures)) => failures,
        other => panic!("expected branch transform failure, got {other:?}"),
    };
    assert_eq!(failures[0].stage, BranchStage::Transform);
    assert!(records.lock().unwrap().is_empty());
    assert!(committed.lock().unwrap().is_empty());
}

struct DelayedSource {
    committed: Arc<Mutex<Vec<Cursor>>>,
}

impl Source for DelayedSource {
    type Payload = u64;
    type Cursor = Cursor;
    type Error = Infallible;

    fn stream(
        &self,
    ) -> impl Stream<Item = Result<Record<Self::Payload, Self::Cursor>, Self::Error>> + Send + '_
    {
        stream::iter(0..3_u64).then(|offset| async move {
            if offset == 1 {
                tokio::time::sleep(Duration::from_millis(30)).await;
            }
            Ok(Record::new(Cursor::at(offset), offset))
        })
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
    let (sink, _) = collector::<Arc<u64>>(Ack::Exact);

    Pipeline::source(source)
        .transform(Identity)
        .branch(Branch::identity("sink").sink(sink))
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
    type Cursor = Cursor;
    type Error = Infallible;

    fn stream(
        &self,
    ) -> impl Stream<Item = Result<Record<Self::Payload, Self::Cursor>, Self::Error>> + Send + '_
    {
        stream::once(async { Ok(Record::new(Cursor::at(0), 1)) }).chain(stream::pending())
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
    let shared = {
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
    let (sink, records) = collector::<Arc<u64>>(Ack::Exact);

    Pipeline::source(source)
        .transform(shared)
        .branch(Branch::identity("sink").sink(sink))
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
    type Cursor = Cursor;
    type Error = Infallible;

    fn stream(
        &self,
    ) -> impl Stream<Item = Result<Record<Self::Payload, Self::Cursor>, Self::Error>> + Send + '_
    {
        stream::iter(
            self.cursors
                .clone()
                .into_iter()
                .map(|cursor| Ok(Record::new(cursor, ()))),
        )
    }

    async fn commit(&self, cursor: Self::Cursor) -> Result<(), Self::Error> {
        self.committed.lock().unwrap().push(cursor);
        Ok(())
    }
}

#[tokio::test]
async fn repeated_and_non_monotonic_cursors_are_opaque_to_the_pipeline() {
    let committed = Arc::new(Mutex::new(Vec::new()));
    let source = PositionedSource {
        cursors: vec![Cursor::at(4), Cursor::at(4), Cursor::at(2)],
        committed: Arc::clone(&committed),
    };
    let (sink, _) = collector::<Arc<()>>(Ack::Exact);

    Pipeline::source(source)
        .transform(Identity)
        .branch(Branch::identity("sink").sink(sink))
        .batched(BatchPolicy::try_new(2, Duration::from_secs(1)).unwrap())
        .run_until(std::future::pending())
        .await
        .unwrap();

    assert_eq!(
        *committed.lock().unwrap(),
        vec![Cursor::at(4), Cursor::at(2)]
    );
}
