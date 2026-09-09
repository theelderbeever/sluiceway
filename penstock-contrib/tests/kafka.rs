#![cfg(feature = "kafka")]

use std::{
    convert::Infallible,
    sync::{Arc, Mutex},
    time::Duration,
};

use penstock_contrib::kafka::{ClientConfig, KafkaCursor, KafkaMessage, KafkaSource};
use penstock_core::{Batch, BatchPolicy, Identity, Pipeline, PipelineError, Sink};
use rdkafka::{
    mocking::MockCluster,
    producer::{FutureProducer, FutureRecord},
    util::Timeout,
};
use thiserror::Error;
use tokio::sync::Notify;

type RawMessages = Arc<Mutex<Vec<KafkaMessage<Option<Vec<u8>>>>>>;

struct CollectSink {
    messages: RawMessages,
    delivered: Arc<Notify>,
}

impl Sink<Batch<KafkaMessage<Option<Vec<u8>>>, KafkaCursor>> for CollectSink {
    type Error = Infallible;

    async fn deliver(
        &self,
        batch: Batch<KafkaMessage<Option<Vec<u8>>>, KafkaCursor>,
    ) -> Result<(), Self::Error> {
        self.messages.lock().unwrap().extend(batch);
        self.delivered.notify_one();
        Ok(())
    }
}

#[derive(Debug, Error)]
#[error("sink rejected batch")]
struct RejectBatch;

struct RejectSink;

impl Sink<Batch<KafkaMessage<Option<Vec<u8>>>, KafkaCursor>> for RejectSink {
    type Error = RejectBatch;

    async fn deliver(
        &self,
        _batch: Batch<KafkaMessage<Option<Vec<u8>>>, KafkaCursor>,
    ) -> Result<(), Self::Error> {
        Err(RejectBatch)
    }
}

struct JsonSink;

impl Sink<Batch<KafkaMessage<Vec<u64>>, KafkaCursor>> for JsonSink {
    type Error = Infallible;

    async fn deliver(
        &self,
        _batch: Batch<KafkaMessage<Vec<u64>>, KafkaCursor>,
    ) -> Result<(), Self::Error> {
        Ok(())
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "librdkafka's experimental mock cluster is unreliable after other test processes"]
async fn broker_checkpoint_contract() {
    const SUCCESS_TOPIC: &str = "successful-batch";
    const REJECTED_TOPIC: &str = "rejected-batch";
    const MALFORMED_TOPIC: &str = "malformed-json";
    let cluster = MockCluster::new(1).unwrap();
    cluster.create_topic(SUCCESS_TOPIC, 2, 1).unwrap();
    cluster.create_topic(REJECTED_TOPIC, 1, 1).unwrap();
    cluster.create_topic(MALFORMED_TOPIC, 1, 1).unwrap();
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
    produce(&producer, MALFORMED_TOPIC, 0, "not-json").await;

    let source = KafkaSource::raw(
        consumer_config(&bootstrap_servers, "successful-batch-group"),
        &[SUCCESS_TOPIC],
    )
    .unwrap();
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

    let mut partitions = messages
        .lock()
        .unwrap()
        .iter()
        .map(|message| message.partition)
        .collect::<Vec<_>>();
    partitions.sort_unstable();
    assert_eq!(partitions, vec![0, 1]);

    let source = KafkaSource::raw(
        consumer_config(&bootstrap_servers, "rejected-batch-group"),
        &[REJECTED_TOPIC],
    )
    .unwrap();
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

    let source = KafkaSource::json::<Vec<u64>>(
        consumer_config(&bootstrap_servers, "malformed-json-group"),
        &[MALFORMED_TOPIC],
    )
    .unwrap();
    let result = tokio::time::timeout(
        Duration::from_secs(15),
        Pipeline::source(source)
            .transform(Identity)
            .sink(JsonSink)
            .batched(BatchPolicy::try_new(1, Duration::from_secs(5)).unwrap())
            .run_until(std::future::pending()),
    )
    .await
    .unwrap();
    assert!(matches!(result, Err(PipelineError::Source(_))));
}
