//! Kafka-compatible sources, including Redpanda and Redpanda Cloud.
//!
//! User-provided key and payload deserializers run inline against librdkafka's borrowed byte
//! slices. Missing keys and payloads remain `None`; present values carry either their decoded
//! value or their deserialization error. The resulting record is owned before it enters the
//! pipeline:
//!
//! ```no_run
//! use sluiceway_contrib::kafka::{ClientConfig, KafkaSource};
//! let mut config = ClientConfig::new();
//! config
//!     .set("bootstrap.servers", "localhost:9092")
//!     .set("group.id", "event-pipeline")
//!     .set("auto.offset.reset", "earliest");
//!
//! let source = KafkaSource::from_config(config, &["events"])?
//!     .payload_deserializer(|bytes| Ok::<_, std::convert::Infallible>(bytes.to_vec()))
//!     .build();
//! # Ok::<_, sluiceway_contrib::kafka::KafkaSourceError>(())
//! ```
//!
//! The source disables automatic commits and offset storage. It synchronously commits exact
//! per-partition offsets only after Sluiceway reports successful delivery. Configure
//! `BatchPolicy::prefetch` to overlap bounded polling with delivery, and keep
//! `max.poll.interval.ms` above the worst-case pause after that bounded buffer fills.
//!
//! Callers that need a custom consumer context can construct the consumer with this module's
//! re-exported [`rdkafka`] version and pass its `Arc` to [`KafkaSource::from_consumer`]. Retaining
//! another clone lets the caller manage subscriptions and inspect consumer metadata.

use std::{
    collections::HashMap, convert::Infallible, future::Future, marker::PhantomData, sync::Arc,
};

use futures_core::Stream;
use futures_util::StreamExt;
pub use rdkafka::{self, config::ClientConfig};
use rdkafka::{
    Message,
    consumer::{CommitMode, Consumer, ConsumerContext, DefaultConsumerContext, StreamConsumer},
    error::KafkaError,
    message::{Header, Headers, OwnedHeaders, Timestamp},
    topic_partition_list::{Offset, TopicPartitionList},
};
use sluiceway_core::{Record, Source};
use thiserror::Error;

/// Decoded Kafka contents with owned values and per-field decode outcomes.
///
/// Deserialization failures are emitted as records instead of terminating the source stream. A
/// downstream stage can inspect them and decide whether accepting the record should advance its
/// Kafka offset. Topic, partition, and raw consumed offset live in the enclosing
/// [`Record`]'s [`KafkaPosition`]; this type holds the message contents and other broker metadata.
#[derive(Debug, Clone)]
pub struct KafkaRecord<K, P, KeyError = Infallible, PayloadError = Infallible> {
    /// `None` when no key was present; otherwise the key's deserialization outcome.
    pub key: Option<Result<K, KeyError>>,
    /// `None` for a tombstone; otherwise the payload's deserialization outcome.
    pub payload: Option<Result<P, PayloadError>>,
    pub timestamp: Timestamp,
    pub headers: Option<OwnedHeaders>,
}

/// Synchronously converts borrowed Kafka bytes into an owned value.
///
/// Both successful values and errors are carried in [`KafkaRecord`] for downstream handling.
pub trait KafkaDeserializer: Send + Sync {
    type Output: Send;
    type Error: std::error::Error + Send + Sync + 'static;

    fn deserialize(&self, bytes: &[u8]) -> Result<Self::Output, Self::Error>;
}

/// Default key policy: preserve key presence but discard its bytes.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoKeyDeserializer;

impl KafkaDeserializer for NoKeyDeserializer {
    type Output = ();
    type Error = Infallible;

    fn deserialize(&self, _bytes: &[u8]) -> Result<Self::Output, Self::Error> {
        Ok(())
    }
}

/// Adapter used by the builder for a deserializer function or closure.
pub struct FnDeserializer<F, T, E> {
    function: F,
    output: PhantomData<fn() -> Result<T, E>>,
}

impl<F, T, E> KafkaDeserializer for FnDeserializer<F, T, E>
where
    F: Fn(&[u8]) -> Result<T, E> + Send + Sync,
    T: Send,
    E: std::error::Error + Send + Sync + 'static,
{
    type Output = T;
    type Error = E;

    fn deserialize(&self, bytes: &[u8]) -> Result<Self::Output, Self::Error> {
        (self.function)(bytes)
    }
}

/// Last delivered offsets keyed by topic and partition.
///
/// The source folds the message-local positions in each batch into this map, which translates
/// into Kafka's next-offset convention when the batch is committed.
pub type KafkaCheckpoint = HashMap<(String, i32), i64>;

/// The broker position of one consumed message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KafkaPosition {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
}

/// Kafka source construction, consumption, or commit failure.
#[derive(Debug, Error)]
pub enum KafkaSourceError<CommitError = BrokerCommitterError>
where
    CommitError: std::error::Error + 'static,
{
    #[error("at least one Kafka topic is required")]
    NoTopics,
    #[error("Kafka operation failed")]
    Kafka(#[from] KafkaError),
    #[error("Kafka checkpoint commit failed")]
    Commit(#[source] CommitError),
}

/// Failure while synchronously committing a checkpoint to Kafka consumer-group offsets.
#[derive(Debug, Error)]
pub enum BrokerCommitterError {
    #[error("Kafka commit task failed")]
    Task(#[source] tokio::task::JoinError),
    #[error("Kafka commit failed")]
    Kafka(#[from] KafkaError),
    #[error("Kafka offset at {topic}[{partition}] cannot be advanced beyond {offset}")]
    OffsetExhausted {
        topic: String,
        partition: i32,
        offset: i64,
    },
}

fn message_position<M>(message: &M) -> KafkaPosition
where
    M: Message + ?Sized,
{
    KafkaPosition {
        topic: message.topic().to_owned(),
        partition: message.partition(),
        offset: message.offset(),
    }
}

fn copy_headers<H>(headers: &H) -> OwnedHeaders
where
    H: Headers,
{
    headers.iter().fold(
        OwnedHeaders::new_with_capacity(headers.count()),
        |owned, header| {
            owned.insert(Header {
                key: header.key,
                value: header.value,
            })
        },
    )
}

type DeserializedRecord<Kd, Pd> = KafkaRecord<
    <Kd as KafkaDeserializer>::Output,
    <Pd as KafkaDeserializer>::Output,
    <Kd as KafkaDeserializer>::Error,
    <Pd as KafkaDeserializer>::Error,
>;

/// Commits successfully delivered Kafka checkpoints.
pub trait KafkaCommitter<C>: Send + Sync
where
    C: ConsumerContext + 'static,
{
    type Error: std::error::Error + Send + Sync + 'static;

    fn commit(
        &self,
        consumer: Arc<StreamConsumer<C>>,
        checkpoint: KafkaCheckpoint,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// Commits checkpoints to Kafka consumer-group offsets.
#[derive(Debug, Default, Clone, Copy)]
pub struct BrokerCommitter;

impl<C> KafkaCommitter<C> for BrokerCommitter
where
    C: ConsumerContext + 'static,
{
    type Error = BrokerCommitterError;

    async fn commit(
        &self,
        consumer: Arc<StreamConsumer<C>>,
        checkpoint: KafkaCheckpoint,
    ) -> Result<(), Self::Error> {
        tokio::task::spawn_blocking(move || -> Result<(), BrokerCommitterError> {
            let mut partitions = TopicPartitionList::with_capacity(checkpoint.len());
            for ((topic, partition), offset) in checkpoint {
                let next_offset =
                    offset
                        .checked_add(1)
                        .ok_or_else(|| BrokerCommitterError::OffsetExhausted {
                            topic: topic.clone(),
                            partition,
                            offset,
                        })?;
                partitions.add_partition_offset(&topic, partition, Offset::Offset(next_offset))?;
            }
            consumer.commit(&partitions, CommitMode::Sync)?;
            Ok(())
        })
        .await
        .map_err(BrokerCommitterError::Task)??;
        Ok(())
    }
}

/// Disables Kafka consumer-group offset commits.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoCommitter;

impl<C> KafkaCommitter<C> for NoCommitter
where
    C: ConsumerContext + 'static,
{
    type Error = Infallible;

    async fn commit(
        &self,
        _consumer: Arc<StreamConsumer<C>>,
        _checkpoint: KafkaCheckpoint,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

/// Type-state marker for a deserializer that has not been configured.
#[derive(Debug, Default, Clone, Copy)]
pub struct MissingDeserializer;

/// Builds a typed Kafka source from an externally managed consumer.
pub struct KafkaSourceBuilder<
    C,
    Kd = NoKeyDeserializer,
    Pd = MissingDeserializer,
    Cm = BrokerCommitter,
> where
    C: ConsumerContext + 'static,
{
    consumer: Arc<StreamConsumer<C>>,
    key_deserializer: Kd,
    payload_deserializer: Pd,
    committer: Cm,
}

impl<C, Kd, Pd, Cm> KafkaSourceBuilder<C, Kd, Pd, Cm>
where
    C: ConsumerContext + 'static,
{
    pub fn key_deserializer<F, T, E>(
        self,
        deserializer: F,
    ) -> KafkaSourceBuilder<C, FnDeserializer<F, T, E>, Pd, Cm>
    where
        F: Fn(&[u8]) -> Result<T, E> + Send + Sync,
        T: Send,
        E: std::error::Error + Send + Sync + 'static,
    {
        KafkaSourceBuilder {
            consumer: self.consumer,
            key_deserializer: FnDeserializer {
                function: deserializer,
                output: PhantomData,
            },
            payload_deserializer: self.payload_deserializer,
            committer: self.committer,
        }
    }

    pub fn payload_deserializer<F, T, E>(
        self,
        deserializer: F,
    ) -> KafkaSourceBuilder<C, Kd, FnDeserializer<F, T, E>, Cm>
    where
        F: Fn(&[u8]) -> Result<T, E> + Send + Sync,
        T: Send,
        E: std::error::Error + Send + Sync + 'static,
    {
        KafkaSourceBuilder {
            consumer: self.consumer,
            key_deserializer: self.key_deserializer,
            payload_deserializer: FnDeserializer {
                function: deserializer,
                output: PhantomData,
            },
            committer: self.committer,
        }
    }

    pub fn committer<Next>(self, committer: Next) -> KafkaSourceBuilder<C, Kd, Pd, Next> {
        KafkaSourceBuilder {
            consumer: self.consumer,
            key_deserializer: self.key_deserializer,
            payload_deserializer: self.payload_deserializer,
            committer,
        }
    }
}

impl<C, Kd, Pd, Cm> KafkaSourceBuilder<C, Kd, Pd, Cm>
where
    C: ConsumerContext + 'static,
    Kd: KafkaDeserializer,
    Pd: KafkaDeserializer,
    Cm: KafkaCommitter<C>,
{
    pub fn build(self) -> KafkaSource<C, Kd, Pd, Cm> {
        KafkaSource {
            consumer: self.consumer,
            key_deserializer: self.key_deserializer,
            payload_deserializer: self.payload_deserializer,
            committer: self.committer,
        }
    }
}

/// A Kafka-compatible source with inline key and payload deserialization.
pub struct KafkaSource<C, Kd, Pd, Cm = BrokerCommitter>
where
    C: ConsumerContext + 'static,
{
    consumer: Arc<StreamConsumer<C>>,
    key_deserializer: Kd,
    payload_deserializer: Pd,
    committer: Cm,
}

impl KafkaSource<DefaultConsumerContext, NoKeyDeserializer, MissingDeserializer> {
    /// Builds a Kafka source around an existing consumer.
    ///
    /// Unlike [`KafkaSource::from_config`], this constructor does not modify the consumer
    /// configuration or subscribe it to any topics. The caller is responsible for configuring
    /// safe offset management before creating the consumer, subscribing it before the source is
    /// polled, and managing any subsequent subscription changes.
    ///
    /// For the offset-management settings used by the default broker committer, see
    /// [`KafkaSource::from_config`]. In particular, automatic commits and automatic offset storage
    /// should be disabled.
    pub fn from_consumer<C>(
        consumer: Arc<StreamConsumer<C>>,
    ) -> KafkaSourceBuilder<C, NoKeyDeserializer, MissingDeserializer>
    where
        C: ConsumerContext + 'static,
    {
        KafkaSourceBuilder {
            consumer,
            key_deserializer: NoKeyDeserializer,
            payload_deserializer: MissingDeserializer,
            committer: BrokerCommitter,
        }
    }

    /// Creates, safely configures, and subscribes a default-context consumer.
    ///
    /// This forces `enable.auto.commit=false` and `enable.auto.offset.store=false`, ensuring that
    /// offsets are advanced only through the source's committer after successful
    /// delivery.
    pub fn from_config(
        mut config: ClientConfig,
        topics: &[&str],
    ) -> Result<
        KafkaSourceBuilder<DefaultConsumerContext, NoKeyDeserializer, MissingDeserializer>,
        KafkaSourceError,
    > {
        if topics.is_empty() {
            return Err(KafkaSourceError::NoTopics);
        }
        config
            .set("enable.auto.commit", "false")
            .set("enable.auto.offset.store", "false");
        let consumer: StreamConsumer = config.create()?;
        consumer.subscribe(topics)?;

        Ok(Self::from_consumer(Arc::new(consumer)))
    }
}

fn track_checkpoint(
    checkpoint: Option<KafkaCheckpoint>,
    position: &KafkaPosition,
) -> KafkaCheckpoint {
    let mut checkpoint = checkpoint.unwrap_or_default();
    checkpoint
        .entry((position.topic.clone(), position.partition))
        .and_modify(|offset| *offset = (*offset).max(position.offset))
        .or_insert(position.offset);
    checkpoint
}

impl<C, Kd, Pd, Cm> KafkaSource<C, Kd, Pd, Cm>
where
    C: ConsumerContext + 'static,
    Kd: KafkaDeserializer,
    Pd: KafkaDeserializer,
    Cm: KafkaCommitter<C>,
{
    fn record_from_message<M>(
        &self,
        message: &M,
    ) -> Record<DeserializedRecord<Kd, Pd>, KafkaPosition>
    where
        M: Message + ?Sized,
    {
        let position = message_position(message);
        let record = KafkaRecord {
            key: message
                .key()
                .map(|bytes| self.key_deserializer.deserialize(bytes)),
            payload: message
                .payload()
                .map(|bytes| self.payload_deserializer.deserialize(bytes)),
            timestamp: message.timestamp(),
            headers: message.headers().map(copy_headers),
        };
        Record::new(position, record)
    }
}

impl<C, Kd, Pd, Cm> Source for KafkaSource<C, Kd, Pd, Cm>
where
    C: ConsumerContext + 'static,
    Kd: KafkaDeserializer,
    Pd: KafkaDeserializer,
    Cm: KafkaCommitter<C>,
{
    type Payload = DeserializedRecord<Kd, Pd>;
    type Position = KafkaPosition;
    type Checkpoint = KafkaCheckpoint;
    type Error = KafkaSourceError<Cm::Error>;

    fn stream(
        &self,
    ) -> impl Stream<Item = Result<Record<Self::Payload, Self::Position>, Self::Error>> + Send + '_
    {
        self.consumer.stream().map(|message| {
            message
                .map(|message| self.record_from_message(&message))
                .map_err(KafkaSourceError::Kafka)
        })
    }

    fn track(checkpoint: Option<Self::Checkpoint>, position: &Self::Position) -> Self::Checkpoint {
        track_checkpoint(checkpoint, position)
    }

    async fn commit(&self, checkpoint: Self::Checkpoint) -> Result<(), Self::Error> {
        self.committer
            .commit(Arc::clone(&self.consumer), checkpoint)
            .await
            .map_err(KafkaSourceError::Commit)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use rdkafka::{
        ClientContext,
        message::{OwnedMessage, Timestamp},
    };
    use sluiceway_core::Source;

    use super::*;

    fn message_at(partition: i32, offset: i64) -> OwnedMessage {
        OwnedMessage::new(
            Some(b"payload".to_vec()),
            None,
            "events".to_owned(),
            Timestamp::NotAvailable,
            partition,
            offset,
            None,
        )
    }

    #[test]
    fn checkpoint_folds_interleaved_partition_positions() {
        let checkpoint = [message_at(0, 4), message_at(1, 9), message_at(0, 5)]
            .iter()
            .map(message_position)
            .fold(None, |checkpoint, position| {
                Some(track_checkpoint(checkpoint, &position))
            })
            .unwrap();

        assert_eq!(checkpoint.get(&("events".to_owned(), 0)), Some(&5));
        assert_eq!(checkpoint.get(&("events".to_owned(), 1)), Some(&9));
    }

    #[tokio::test]
    async fn broker_committer_rejects_an_exhausted_offset() {
        let checkpoint = HashMap::from([(("events".to_owned(), 0), i64::MAX)]);

        assert!(matches!(
            BrokerCommitter.commit(custom_consumer(), checkpoint).await,
            Err(BrokerCommitterError::OffsetExhausted { .. })
        ));
    }

    #[derive(Debug, Clone, Copy)]
    struct Utf8Deserializer;

    impl KafkaDeserializer for Utf8Deserializer {
        type Output = String;
        type Error = std::str::Utf8Error;

        fn deserialize(&self, bytes: &[u8]) -> Result<Self::Output, Self::Error> {
            Ok(std::str::from_utf8(bytes)?.to_owned())
        }
    }

    #[tokio::test]
    async fn record_preserves_missing_successful_and_failed_decode_outcomes() {
        let decoding_source = KafkaSource {
            consumer: custom_consumer(),
            key_deserializer: Utf8Deserializer,
            payload_deserializer: Utf8Deserializer,
            committer: BrokerCommitter,
        };
        let invalid_payload = OwnedMessage::new(
            Some(vec![0xff]),
            Some(b"key".to_vec()),
            "events".to_owned(),
            Timestamp::NotAvailable,
            0,
            0,
            None,
        );
        let decoded = decoding_source
            .record_from_message(&invalid_payload)
            .payload;

        assert_eq!(decoded.key.unwrap().unwrap(), "key");
        assert!(matches!(decoded.payload, Some(Err(_))));

        let absent = OwnedMessage::new(
            None,
            None,
            "events".to_owned(),
            Timestamp::NotAvailable,
            0,
            1,
            None,
        );
        let ignoring_source = KafkaSource {
            consumer: custom_consumer(),
            key_deserializer: NoKeyDeserializer,
            payload_deserializer: Utf8Deserializer,
            committer: BrokerCommitter,
        };
        let decoded = ignoring_source.record_from_message(&absent).payload;

        assert!(decoded.key.is_none());
        assert!(decoded.payload.is_none());

        let ignored_key = OwnedMessage::new(
            None,
            Some(b"ignored".to_vec()),
            "events".to_owned(),
            Timestamp::NotAvailable,
            0,
            2,
            None,
        );
        let decoded = ignoring_source.record_from_message(&ignored_key).payload;

        assert!(matches!(decoded.key, Some(Ok(()))));
    }

    #[test]
    fn source_rejects_an_empty_subscription() {
        assert!(matches!(
            KafkaSource::from_config(ClientConfig::new(), &[]),
            Err(KafkaSourceError::NoTopics)
        ));
    }

    #[derive(Clone, Default)]
    struct TestContext;

    impl ClientContext for TestContext {}
    impl ConsumerContext for TestContext {}

    fn custom_consumer() -> Arc<StreamConsumer<TestContext>> {
        let mut config = ClientConfig::new();
        config
            .set("bootstrap.servers", "localhost:1")
            .set("group.id", "sluiceway-custom-context-test")
            .set("enable.auto.commit", "false")
            .set("enable.auto.offset.store", "false");
        Arc::new(config.create_with_context(TestContext).unwrap())
    }

    fn bytes(value: &[u8]) -> Result<Vec<u8>, Infallible> {
        Ok(value.to_vec())
    }

    #[tokio::test]
    async fn custom_context_source_uses_externally_controlled_consumer() {
        let consumer = custom_consumer();
        consumer.subscribe(&["events"]).unwrap();
        assert_eq!(
            consumer.subscription().unwrap().elements()[0].topic(),
            "events"
        );
        assert_eq!(consumer.assignment().unwrap().count(), 0);

        let _source = KafkaSource::from_consumer(Arc::clone(&consumer))
            .key_deserializer(bytes)
            .payload_deserializer(bytes)
            .build();
    }

    #[derive(Clone)]
    struct RecordingCommitter(Arc<Mutex<Vec<KafkaCheckpoint>>>);

    impl<C> KafkaCommitter<C> for RecordingCommitter
    where
        C: ConsumerContext + 'static,
    {
        type Error = Infallible;

        async fn commit(
            &self,
            _consumer: Arc<StreamConsumer<C>>,
            checkpoint: KafkaCheckpoint,
        ) -> Result<(), Self::Error> {
            self.0.lock().unwrap().push(checkpoint);
            Ok(())
        }
    }

    #[tokio::test]
    async fn committer_is_injected_and_observes_exact_checkpoint() {
        let commits = Arc::new(Mutex::new(Vec::new()));
        let source = KafkaSource::from_consumer(custom_consumer())
            .key_deserializer(bytes)
            .payload_deserializer(bytes)
            .committer(RecordingCommitter(Arc::clone(&commits)))
            .build();
        let checkpoint = HashMap::from([(("events".to_owned(), 2), 41)]);

        source.commit(checkpoint.clone()).await.unwrap();

        assert_eq!(*commits.lock().unwrap(), vec![checkpoint]);
    }

    #[tokio::test]
    async fn no_committer_accepts_a_checkpoint_without_broker_commit() {
        let source = KafkaSource::from_consumer(custom_consumer())
            .key_deserializer(bytes)
            .payload_deserializer(bytes)
            .committer(NoCommitter)
            .build();
        let checkpoint = HashMap::from([(("events".to_owned(), 0), 1)]);

        source.commit(checkpoint).await.unwrap();
    }

    #[derive(Debug, thiserror::Error)]
    #[error("recording commit failed")]
    struct RecordingCommitError;

    struct FailingCommitter;

    impl<C> KafkaCommitter<C> for FailingCommitter
    where
        C: ConsumerContext + 'static,
    {
        type Error = RecordingCommitError;

        async fn commit(
            &self,
            _consumer: Arc<StreamConsumer<C>>,
            _checkpoint: KafkaCheckpoint,
        ) -> Result<(), Self::Error> {
            Err(RecordingCommitError)
        }
    }

    #[tokio::test]
    async fn commit_failure_retains_its_typed_cause() {
        let source = KafkaSource::from_consumer(custom_consumer())
            .key_deserializer(bytes)
            .payload_deserializer(bytes)
            .committer(FailingCommitter)
            .build();

        assert!(matches!(
            source.commit(KafkaCheckpoint::new()).await,
            Err(KafkaSourceError::Commit(RecordingCommitError))
        ));
    }

    #[allow(dead_code)]
    fn source_contract_is_send_sync() {
        fn assert_source<S: Source + Send + Sync>(_: &S) {}
        let source = KafkaSource::from_consumer(custom_consumer())
            .key_deserializer(bytes)
            .payload_deserializer(bytes)
            .build();
        assert_source(&source);
    }
}
