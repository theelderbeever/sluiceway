#![allow(clippy::unwrap_used, clippy::expect_used, clippy::print_stdout)]

use std::{
    convert::Infallible,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use futures_core::Stream;
use futures_util::stream;
use penstock::{BatchPolicy, Pipeline, Record, SharedBatch, Sink, Source};

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

struct PrintBatch;

impl Sink<SharedBatch<u64, u64>> for PrintBatch {
    type Error = Infallible;

    async fn deliver(&self, batch: SharedBatch<u64, u64>) -> Result<(), Self::Error> {
        tokio::time::sleep(Duration::from_secs(1)).await;
        for record in batch.iter() {
            println!("position {}: {}", record.position(), record.payload);
        }
        Ok(())
    }
}

struct CountItems {
    count: Arc<AtomicUsize>,
}

impl Sink<SharedBatch<u64, u64>> for CountItems {
    type Error = Infallible;

    async fn deliver(&self, batch: SharedBatch<u64, u64>) -> Result<(), Self::Error> {
        self.count.fetch_add(batch.len(), Ordering::Relaxed);
        Ok(())
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let count = Arc::new(AtomicUsize::new(0));

    Pipeline::source(Numbers { end: 5 })
        .transform(|_position: &u64, number| async move { Ok::<_, Infallible>(number * number) })
        .fanout()
        .shared()
        .sinks([
            PrintBatch.into(),
            CountItems {
                count: Arc::clone(&count),
            }
            .into(),
        ])
        .batched(BatchPolicy::try_new(2, Duration::from_secs(1))?)
        .run_until(std::future::pending())
        .await?;

    println!("counted {} items", count.load(Ordering::Relaxed));
    Ok(())
}
