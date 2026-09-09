//! Kafka-compatible sources, including Redpanda and Redpanda Cloud.
//!
//! Both source modes preserve the message's Kafka metadata. Raw mode owns optional payload bytes,
//! including tombstones, while JSON mode strictly decodes a present payload into a known type:
//!
//! ```no_run
//! use penstock_contrib::kafka::{ClientConfig, KafkaSource};
//! use serde::Deserialize;
//!
//! #[derive(Deserialize)]
//! struct Event {
//!     id: u64,
//! }
//!
//! let mut config = ClientConfig::new();
//! config
//!     .set("bootstrap.servers", "localhost:9092")
//!     .set("group.id", "event-pipeline")
//!     .set("auto.offset.reset", "earliest");
//!
//! let raw = KafkaSource::raw(config.clone(), &["events"])?;
//! let json = KafkaSource::json::<Event>(config, &["events"])?;
//! # Ok::<_, penstock_contrib::kafka::KafkaSourceError>(())
//! ```
//!
//! The source disables automatic commits and offset storage. It synchronously commits exact
//! per-partition offsets only after Penstock reports successful delivery. Configure
//! `max.poll.interval.ms` above the worst-case time spent transforming and delivering a batch.

use std::{collections::HashMap, future, marker::PhantomData, sync::Arc};

use futures_core::Stream;
use futures_util::StreamExt;
use penstock_core::{Record, Source};
pub use rdkafka::config::ClientConfig;
use rdkafka::{
    Message,
    consumer::{CommitMode, Consumer, StreamConsumer},
    error::KafkaError,
    message::{Headers, Timestamp},
    topic_partition_list::{Offset, TopicPartitionList},
};
use serde::de::DeserializeOwned;
use thiserror::Error;

/// A Kafka header with owned key and value storage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KafkaHeader {
    pub key: String,
    pub value: Option<Vec<u8>>,
}

/// The broker timestamp attached to a Kafka message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KafkaTimestamp {
    NotAvailable,
    CreateTime(i64),
    LogAppendTime(i64),
}

impl From<Timestamp> for KafkaTimestamp {
    fn from(timestamp: Timestamp) -> Self {
        match timestamp {
            Timestamp::NotAvailable => Self::NotAvailable,
            Timestamp::CreateTime(milliseconds) => Self::CreateTime(milliseconds),
            Timestamp::LogAppendTime(milliseconds) => Self::LogAppendTime(milliseconds),
        }
    }
}

/// An owned Kafka message whose payload representation depends on the source decoding mode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KafkaMessage<P> {
    pub payload: P,
    pub key: Option<Vec<u8>>,
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: KafkaTimestamp,
    pub headers: Vec<KafkaHeader>,
}

/// Absolute next offsets keyed by topic and partition.
///
/// Each record carries a complete snapshot through that point in the consumer stream. The map can
/// therefore be translated directly into a [`TopicPartitionList`] when a batch is acknowledged.
pub type KafkaCursor = HashMap<(String, i32), i64>;

/// Kafka source construction, consumption, decoding, or commit failure.
#[derive(Debug, Error)]
pub enum KafkaSourceError {
    #[error("at least one Kafka topic is required")]
    NoTopics,
    #[error("Kafka operation failed")]
    Kafka(#[from] KafkaError),
    #[error("Kafka message at {topic}[{partition}] offset {offset} has no payload")]
    MissingPayload {
        topic: String,
        partition: i32,
        offset: i64,
    },
    #[error("Kafka JSON payload could not be decoded")]
    Json(#[from] serde_json::Error),
    #[error("Kafka offset at {topic}[{partition}] cannot be advanced beyond {offset}")]
    OffsetExhausted {
        topic: String,
        partition: i32,
        offset: i64,
    },
    #[error("Kafka commit task failed")]
    CommitTask(#[source] tokio::task::JoinError),
}

/// Marker selecting owned raw payload bytes.
#[derive(Debug, Default, Clone, Copy)]
pub struct Raw;

/// Marker selecting JSON decoding into `T`.
#[derive(Debug, Default, Clone, Copy)]
pub struct Json<T>(PhantomData<fn() -> T>);

fn raw_payload<M: Message + ?Sized>(message: &M) -> Option<Vec<u8>> {
    message.payload().map(<[u8]>::to_vec)
}

fn json_payload<T, M>(message: &M) -> Result<T, KafkaSourceError>
where
    T: DeserializeOwned,
    M: Message + ?Sized,
{
    let payload = message
        .payload()
        .ok_or_else(|| KafkaSourceError::MissingPayload {
            topic: message.topic().to_owned(),
            partition: message.partition(),
            offset: message.offset(),
        })?;
    Ok(serde_json::from_slice(payload)?)
}

fn message_with_payload<M, P>(message: &M, payload: P) -> KafkaMessage<P>
where
    M: Message + ?Sized,
{
    let headers = message
        .headers()
        .map(|headers| {
            headers
                .iter()
                .map(|header| KafkaHeader {
                    key: header.key.to_owned(),
                    value: header.value.map(<[u8]>::to_vec),
                })
                .collect()
        })
        .unwrap_or_default();
    KafkaMessage {
        payload,
        key: message.key().map(<[u8]>::to_vec),
        topic: message.topic().to_owned(),
        partition: message.partition(),
        offset: message.offset(),
        timestamp: message.timestamp().into(),
        headers,
    }
}

fn advance_cursor<M>(cursor: &mut KafkaCursor, message: &M) -> Result<KafkaCursor, KafkaSourceError>
where
    M: Message + ?Sized,
{
    let offset = message.offset();
    let next_offset = offset
        .checked_add(1)
        .ok_or_else(|| KafkaSourceError::OffsetExhausted {
            topic: message.topic().to_owned(),
            partition: message.partition(),
            offset,
        })?;
    cursor.insert(
        (message.topic().to_owned(), message.partition()),
        next_offset,
    );
    Ok(cursor.clone())
}

/// Persists successful cursor snapshots to Kafka consumer-group offsets.
#[derive(Debug, Default, Clone, Copy)]
pub struct BrokerCheckpoint;

impl BrokerCheckpoint {
    async fn commit(
        self,
        consumer: Arc<StreamConsumer>,
        cursor: KafkaCursor,
    ) -> Result<(), KafkaSourceError> {
        tokio::task::spawn_blocking(move || {
            let mut partitions = TopicPartitionList::with_capacity(cursor.len());
            for ((topic, partition), offset) in cursor {
                partitions.add_partition_offset(&topic, partition, Offset::Offset(offset))?;
            }
            consumer.commit(&partitions, CommitMode::Sync)
        })
        .await
        .map_err(KafkaSourceError::CommitTask)??;
        Ok(())
    }
}

/// A subscribed Kafka-compatible consumer source.
///
/// Use [`KafkaSource::raw`] for owned bytes or [`KafkaSource::json`] for strict Serde JSON
/// decoding. The supplied consumer config is forced to use manual offset management.
pub struct KafkaSource<D = Raw> {
    consumer: Arc<StreamConsumer>,
    mode: PhantomData<fn() -> D>,
    checkpoint: BrokerCheckpoint,
}

impl KafkaSource<Raw> {
    pub fn raw(config: ClientConfig, topics: &[&str]) -> Result<Self, KafkaSourceError> {
        Self::build(config, topics)
    }

    pub fn json<T>(
        config: ClientConfig,
        topics: &[&str],
    ) -> Result<KafkaSource<Json<T>>, KafkaSourceError>
    where
        T: DeserializeOwned + Send,
    {
        KafkaSource::build(config, topics)
    }
}

impl<D> KafkaSource<D> {
    fn build(config: ClientConfig, topics: &[&str]) -> Result<Self, KafkaSourceError> {
        let mut config = config;
        if topics.is_empty() {
            return Err(KafkaSourceError::NoTopics);
        }

        config
            .set("enable.auto.commit", "false")
            .set("enable.auto.offset.store", "false");
        let consumer: StreamConsumer = config.create()?;
        consumer.subscribe(topics)?;

        Ok(Self {
            consumer: Arc::new(consumer),
            mode: PhantomData,
            checkpoint: BrokerCheckpoint,
        })
    }

    fn record<P, M>(
        &self,
        cursor: &mut KafkaCursor,
        source: &M,
        payload: P,
    ) -> Result<Record<KafkaMessage<P>, KafkaCursor>, KafkaSourceError>
    where
        M: Message + ?Sized,
    {
        let cursor = advance_cursor(cursor, source)?;
        Ok(Record::new(cursor, message_with_payload(source, payload)))
    }

    async fn commit_cursor(&self, cursor: KafkaCursor) -> Result<(), KafkaSourceError> {
        self.checkpoint
            .commit(Arc::clone(&self.consumer), cursor)
            .await
    }
}

impl Source for KafkaSource<Raw> {
    type Payload = KafkaMessage<Option<Vec<u8>>>;
    type Cursor = KafkaCursor;
    type Error = KafkaSourceError;

    fn stream(
        &self,
    ) -> impl Stream<Item = Result<Record<Self::Payload, Self::Cursor>, Self::Error>> + Send + '_
    {
        self.consumer
            .stream()
            .scan(KafkaCursor::new(), |cursor, source| {
                let record = source
                    .map_err(KafkaSourceError::Kafka)
                    .and_then(|source| self.record(cursor, &source, raw_payload(&source)));
                future::ready(Some(record))
            })
    }

    async fn commit(&self, cursor: Self::Cursor) -> Result<(), Self::Error> {
        self.commit_cursor(cursor).await
    }
}

impl<T> Source for KafkaSource<Json<T>>
where
    T: DeserializeOwned + Send,
{
    type Payload = KafkaMessage<T>;
    type Cursor = KafkaCursor;
    type Error = KafkaSourceError;

    fn stream(
        &self,
    ) -> impl Stream<Item = Result<Record<Self::Payload, Self::Cursor>, Self::Error>> + Send + '_
    {
        self.consumer
            .stream()
            .scan(KafkaCursor::new(), |cursor, source| {
                let record = source.map_err(KafkaSourceError::Kafka).and_then(|source| {
                    let payload = json_payload(&source)?;
                    self.record(cursor, &source, payload)
                });
                future::ready(Some(record))
            })
    }

    async fn commit(&self, cursor: Self::Cursor) -> Result<(), Self::Error> {
        self.commit_cursor(cursor).await
    }
}

#[cfg(test)]
mod tests {
    use penstock_core::Source;
    use rdkafka::message::{Header, OwnedHeaders, OwnedMessage};

    use super::*;

    fn message(payload: Option<Vec<u8>>) -> OwnedMessage {
        OwnedMessage::new(
            payload,
            Some(b"key".to_vec()),
            "events".to_owned(),
            Timestamp::CreateTime(42),
            3,
            7,
            Some(
                OwnedHeaders::new()
                    .insert(Header {
                        key: "trace",
                        value: Some(b"first"),
                    })
                    .insert(Header {
                        key: "trace",
                        value: None::<&[u8]>,
                    }),
            ),
        )
    }

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
    fn raw_decoder_preserves_tombstones() {
        assert_eq!(raw_payload(&message(None)), None);
    }

    #[test]
    fn raw_source_preserves_owned_message_metadata() {
        let source = message(Some(b"payload".to_vec()));
        let message = message_with_payload(&source, raw_payload(&source));

        assert_eq!(message.payload, Some(b"payload".to_vec()));
        assert_eq!(message.key, Some(b"key".to_vec()));
        assert_eq!(message.topic, "events");
        assert_eq!(message.partition, 3);
        assert_eq!(message.offset, 7);
        assert_eq!(message.timestamp, KafkaTimestamp::CreateTime(42));
        assert_eq!(
            message.headers,
            vec![
                KafkaHeader {
                    key: "trace".to_owned(),
                    value: Some(b"first".to_vec()),
                },
                KafkaHeader {
                    key: "trace".to_owned(),
                    value: None,
                },
            ]
        );
    }

    #[test]
    fn json_decoder_decodes_known_types_and_rejects_bad_payloads() {
        assert_eq!(
            json_payload::<Option<Vec<u64>>, _>(&message(Some(b"[1,2]".to_vec()))).unwrap(),
            Some(vec![1, 2])
        );
        assert_eq!(
            json_payload::<Option<Vec<u64>>, _>(&message(Some(b"null".to_vec()))).unwrap(),
            None
        );
        assert!(matches!(
            json_payload::<Vec<u64>, _>(&message(None)),
            Err(KafkaSourceError::MissingPayload { .. })
        ));
        assert!(matches!(
            json_payload::<Vec<u64>, _>(&message(Some(b"not-json".to_vec()))),
            Err(KafkaSourceError::Json(_))
        ));
    }

    #[test]
    fn cursor_snapshots_interleaved_partitions() {
        let mut cursor = KafkaCursor::new();
        let first = advance_cursor(&mut cursor, &message_at(0, 4)).unwrap();
        let second = advance_cursor(&mut cursor, &message_at(1, 9)).unwrap();
        let third = advance_cursor(&mut cursor, &message_at(0, 5)).unwrap();

        assert_eq!(first.get(&("events".to_owned(), 0)), Some(&5));
        assert_eq!(first.get(&("events".to_owned(), 1)), None);
        assert_eq!(second.get(&("events".to_owned(), 0)), Some(&5));
        assert_eq!(second.get(&("events".to_owned(), 1)), Some(&10));
        assert_eq!(third.get(&("events".to_owned(), 0)), Some(&6));
        assert_eq!(third.get(&("events".to_owned(), 1)), Some(&10));
    }

    #[test]
    fn cursor_rejects_exhausted_offsets() {
        let mut cursor = KafkaCursor::new();
        assert!(matches!(
            advance_cursor(&mut cursor, &message_at(0, i64::MAX)),
            Err(KafkaSourceError::OffsetExhausted { .. })
        ));
    }

    #[test]
    fn source_rejects_an_empty_subscription() {
        assert!(matches!(
            KafkaSource::raw(ClientConfig::new(), &[]),
            Err(KafkaSourceError::NoTopics)
        ));
    }

    #[allow(dead_code)]
    fn source_contract_is_send_sync() {
        fn assert_source<S: Source + Send + Sync>() {}
        assert_source::<KafkaSource<Raw>>();
    }
}
