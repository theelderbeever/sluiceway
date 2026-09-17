#![allow(clippy::print_stdout)]

use std::{convert::Infallible, time::Duration};

use futures_core::Stream;
use futures_util::stream;
use sluiceway::{CommitPolicy, Pipeline, Record, Sink, Source};

struct Numbers {
    end: u64,
}

impl Source for Numbers {
    type Payload = u64;
    type Position = u64;
    type Checkpoint = u64;
    type Error = Infallible;

    fn stream(
        &self,
    ) -> impl Stream<Item = Result<Record<Self::Payload, Self::Position>, Self::Error>> + Send + '_
    {
        stream::iter((0..self.end).map(|number| Ok(Record::new(number, number))))
    }

    fn track(_checkpoint: Option<Self::Checkpoint>, position: &Self::Position) -> Self::Checkpoint {
        *position
    }

    async fn commit(&self, checkpoint: Self::Checkpoint) -> Result<(), Self::Error> {
        println!("committed through source position {checkpoint}");
        Ok(())
    }
}

struct PrintRecord;

impl Sink<Record<String, u64>> for PrintRecord {
    type Error = Infallible;

    async fn deliver(&self, record: Record<String, u64>) -> Result<(), Self::Error> {
        println!("position {}: {}", record.position(), record.payload);
        Ok(())
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    Pipeline::source(Numbers { end: 5 })
        .transform(|_position: &u64, number| async move {
            Ok::<_, Infallible>(format!("number-{number}"))
        })
        .sink(PrintRecord)
        .each()
        // Delivery remains per record while checkpoints advance every two records.
        .commit_policy(CommitPolicy::after_or_timeout(2, Duration::from_secs(1))?)
        .run()
        .await?;

    Ok(())
}
