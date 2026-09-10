#![cfg(feature = "kafka")]
#![allow(clippy::unwrap_used)]

use std::{
    convert::Infallible,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use rdkafka::{
    ClientContext,
    consumer::{BaseConsumer, Consumer, ConsumerContext, Rebalance, StreamConsumer},
    mocking::MockCluster,
    producer::{FutureProducer, FutureRecord},
    topic_partition_list::{Offset, TopicPartitionList},
    util::Timeout,
};
use sluiceway_contrib::kafka::{ClientConfig, KafkaPosition, KafkaRecord, KafkaSource};
use sluiceway_core::{Batch, BatchPolicy, Identity, Pipeline, PipelineError, Sink};
use thiserror::Error;
use tokio::sync::Notify;

type DecodedMessages = Arc<Mutex<Vec<(i32, Option<()>, Option<Vec<u8>>)>>>;
type IgnoredKeyRecord = KafkaRecord<(), Vec<u8>>;
type BytesRecord = KafkaRecord<Vec<u8>, Vec<u8>>;

struct CollectSink {
    messages: DecodedMessages,
    delivered: Arc<Notify>,
}

impl Sink<Batch<IgnoredKeyRecord, KafkaPosition>> for CollectSink {
    type Error = Infallible;

    async fn deliver(
        &self,
        batch: Batch<IgnoredKeyRecord, KafkaPosition>,
    ) -> Result<(), Self::Error> {
        self.messages
            .lock()
            .unwrap()
            .extend(batch.into_iter().map(|record| {
                let partition = record.position().partition;
                let message = record.payload;
                (
                    partition,
                    message.key.transpose().unwrap(),
                    message.payload.transpose().unwrap(),
                )
            }));
        self.delivered.notify_one();
        Ok(())
    }
}

#[derive(Debug, Error)]
#[error("sink rejected batch")]
struct RejectBatch;

struct RejectSink;

impl Sink<Batch<BytesRecord, KafkaPosition>> for RejectSink {
    type Error = RejectBatch;

    async fn deliver(&self, _batch: Batch<BytesRecord, KafkaPosition>) -> Result<(), Self::Error> {
        Err(RejectBatch)
    }
}

struct TestContext {
    rebalanced: Arc<AtomicBool>,
}

impl ClientContext for TestContext {}

impl ConsumerContext for TestContext {
    fn post_rebalance(&self, _consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        if matches!(rebalance, Rebalance::Assign(_)) {
            self.rebalanced.store(true, Ordering::SeqCst);
        }
    }
}

fn consumer_config(bootstrap_servers: &str, group: &str) -> ClientConfig {
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", bootstrap_servers)
        .set("group.id", group)
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "6000");
    config
}

async fn produce(producer: &FutureProducer, topic: &str, partition: i32, payload: &str) {
    producer
        .send(
            FutureRecord::to(topic)
                .partition(partition)
                .key("key")
                .payload(payload),
            Timeout::After(Duration::from_secs(5)),
        )
        .await
        .unwrap();
}

fn bytes(value: &[u8]) -> Result<Vec<u8>, Infallible> {
    Ok(value.to_vec())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "librdkafka's experimental mock cluster is unreliable after other test processes"]
async fn broker_committer_contract() {
    const SUCCESS_TOPIC: &str = "successful-batch";
    const REJECTED_TOPIC: &str = "rejected-batch";
    let cluster = MockCluster::new(1).unwrap();
    cluster.create_topic(SUCCESS_TOPIC, 2, 1).unwrap();
    cluster.create_topic(REJECTED_TOPIC, 1, 1).unwrap();
    let bootstrap_servers = cluster.bootstrap_servers();
    // librdkafka 2.12.1 can double-destroy explicitly owned mock clusters during teardown.
    // Keep the test broker alive until process exit; no production resource is leaked.
    std::mem::forget(cluster);
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .create()
        .unwrap();
    produce(&producer, SUCCESS_TOPIC, 0, "zero").await;
    produce(&producer, SUCCESS_TOPIC, 1, "one").await;
    produce(&producer, REJECTED_TOPIC, 0, "retry-me").await;

    let rebalanced = Arc::new(AtomicBool::new(false));
    let mut config = consumer_config(&bootstrap_servers, "successful-batch-group");
    config
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false");
    let consumer: Arc<StreamConsumer<TestContext>> = Arc::new(
        config
            .create_with_context(TestContext {
                rebalanced: Arc::clone(&rebalanced),
            })
            .unwrap(),
    );
    consumer.subscribe(&[SUCCESS_TOPIC]).unwrap();
    let source = KafkaSource::from_consumer(Arc::clone(&consumer))
        .payload_deserializer(bytes)
        .build();
    let messages = Arc::new(Mutex::new(Vec::new()));
    let delivered = Arc::new(Notify::new());
    let sink = CollectSink {
        messages: Arc::clone(&messages),
        delivered: Arc::clone(&delivered),
    };
    tokio::time::timeout(
        Duration::from_secs(15),
        Pipeline::source(source)
            .transform(Identity)
            .sink(sink)
            .batched(BatchPolicy::try_new(2, Duration::from_secs(5)).unwrap())
            .run_until(delivered.notified()),
    )
    .await
    .unwrap()
    .unwrap();

    let mut messages = messages.lock().unwrap().clone();
    messages.sort_unstable_by_key(|message| message.0);
    assert_eq!(
        messages,
        vec![
            (0, Some(()), Some(b"zero".to_vec())),
            (1, Some(()), Some(b"one".to_vec())),
        ]
    );
    assert!(rebalanced.load(Ordering::SeqCst));

    let mut requested = TopicPartitionList::new();
    requested.add_partition(SUCCESS_TOPIC, 0);
    requested.add_partition(SUCCESS_TOPIC, 1);
    let committed = consumer
        .committed_offsets(requested, Timeout::After(Duration::from_secs(5)))
        .unwrap();
    assert_eq!(
        committed.find_partition(SUCCESS_TOPIC, 0).unwrap().offset(),
        Offset::Offset(1)
    );
    assert_eq!(
        committed.find_partition(SUCCESS_TOPIC, 1).unwrap().offset(),
        Offset::Offset(1)
    );

    let source = KafkaSource::from_config(
        consumer_config(&bootstrap_servers, "rejected-batch-group"),
        &[REJECTED_TOPIC],
    )
    .unwrap()
    .key_deserializer(bytes)
    .payload_deserializer(bytes)
    .build();
    let result = tokio::time::timeout(
        Duration::from_secs(15),
        Pipeline::source(source)
            .transform(Identity)
            .sink(RejectSink)
            .batched(BatchPolicy::try_new(1, Duration::from_secs(5)).unwrap())
            .run_until(std::future::pending()),
    )
    .await
    .unwrap();
    assert!(matches!(result, Err(PipelineError::Sink(_))));
}
