#![allow(clippy::unwrap_used, clippy::expect_used, clippy::print_stdout)]

use std::{convert::Infallible, time::Duration};

use futures_core::Stream;
use futures_util::stream;
use sluiceway::{Batch, BatchPolicy, Checkpoint, Pipeline, Record, Sink, Source};

struct Cursor(u64);

impl Checkpoint<u64> for Cursor {
    fn start_epoch(position: &u64) -> Self {
        Self(*position)
    }

    fn include_position(&mut self, position: &u64) {
        self.0 = *position;
    }
}

struct Numbers {
    end: u64,
}

impl Source for Numbers {
    type Payload = u64;
    type Position = u64;
    type Checkpoint = Cursor;
    type Error = Infallible;

    fn stream(
        &self,
    ) -> impl Stream<Item = Result<Record<Self::Payload, Self::Position>, Self::Error>> + Send + '_
    {
        stream::iter((0..self.end).map(|number| Ok(Record::new(number, number))))
    }

    async fn commit(&self, checkpoint: Self::Checkpoint) -> Result<(), Self::Error> {
        println!("committed through source position {}", checkpoint.0);
        Ok(())
    }
}

struct PrintBatch;

impl Sink<Batch<String, u64>> for PrintBatch {
    type Error = Infallible;

    async fn deliver(&self, batch: Batch<String, u64>) -> Result<(), Self::Error> {
        tokio::time::sleep(Duration::from_secs(1)).await;
        for record in batch {
            println!("position {}: {}", record.position(), record.payload);
        }
        Ok(())
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    Pipeline::source(Numbers { end: 5 })
        .transform(|_position: &u64, number| async move {
            Ok::<_, Infallible>(format!("number-{number}"))
        })
        .sink(PrintBatch)
        .batched(BatchPolicy::try_new(2, Duration::from_secs(1))?)
        .run_until(std::future::pending())
        .await?;

    Ok(())
}
