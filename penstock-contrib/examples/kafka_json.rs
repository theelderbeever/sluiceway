use std::{convert::Infallible, env, str, time::Duration};

use penstock_contrib::kafka::{ClientConfig, KafkaPosition, KafkaRecord, KafkaSource};
use penstock_core::{Batch, BatchPolicy, Identity, Pipeline, Sink};
use serde::Deserialize;
use thiserror::Error;

#[derive(Debug, Deserialize)]
struct Event {
    id: String,
    value: u64,
}

#[derive(Debug, Error)]
enum DecodeError {
    #[error("Kafka payload is not a valid event")]
    Json(#[from] serde_json::Error),
}

struct PrintEvents;

fn decode_key(bytes: &[u8]) -> Result<String, str::Utf8Error> {
    Ok(str::from_utf8(bytes)?.to_owned())
}

fn decode_event(bytes: &[u8]) -> Result<Event, DecodeError> {
    Ok(serde_json::from_slice(bytes)?)
}

impl Sink<Batch<KafkaRecord<String, Event, str::Utf8Error, DecodeError>, KafkaPosition>>
    for PrintEvents
{
    type Error = Infallible;

    async fn deliver(
        &self,
        batch: Batch<KafkaRecord<String, Event, str::Utf8Error, DecodeError>, KafkaPosition>,
    ) -> Result<(), Self::Error> {
        for record in batch {
            match record.payload.payload {
                Some(Ok(payload)) => {
                    println!("event {} has value {}", payload.id, payload.value);
                }
                Some(Err(error)) => eprintln!("could not decode event: {error}"),
                None => eprintln!("Kafka message had no payload"),
            }
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bootstrap_servers =
        env::var("KAFKA_BOOTSTRAP_SERVERS").unwrap_or_else(|_| "localhost:9092".to_owned());
    let topic = env::var("KAFKA_TOPIC").unwrap_or_else(|_| "events".to_owned());
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", bootstrap_servers)
        .set("group.id", "penstock-json-example")
        .set("auto.offset.reset", "earliest");
    let source = KafkaSource::from_config(config, &[&topic])?
        .key_deserializer(decode_key)
        .payload_deserializer(decode_event)
        .build();

    Pipeline::source(source)
        .transform(Identity)
        .sink(PrintEvents)
        .batched(BatchPolicy::try_new(100, Duration::from_secs(1))?)
        .run_until(async { tokio::signal::ctrl_c().await.unwrap_or(()) })
        .await?;

    Ok(())
}
