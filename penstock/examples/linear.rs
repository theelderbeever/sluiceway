use std::{convert::Infallible, time::Duration};

use futures_core::Stream;
use futures_util::stream;
use penstock::{Batch, BatchPolicy, Pipeline, Record, Sink, Source};

struct Numbers {
    end: u64,
}

impl Source for Numbers {
    type Payload = u64;
    type Position = u64;
    type Cursor = u64;
    type Error = Infallible;

    fn stream(
        &self,
    ) -> impl Stream<Item = Result<Record<Self::Payload, Self::Position>, Self::Error>> + Send + '_
    {
        stream::iter((0..self.end).map(|number| Ok(Record::new(number, number))))
    }

    fn track(_cursor: Option<Self::Cursor>, position: Self::Position) -> Self::Cursor {
        position
    }

    async fn commit(&self, cursor: Self::Cursor) -> Result<(), Self::Error> {
        println!("committed through source position {cursor}");
        Ok(())
    }
}

struct PrintBatch;

impl Sink<Batch<String, u64>> for PrintBatch {
    type Error = Infallible;

    async fn deliver(&self, batch: Batch<String, u64>) -> Result<(), Self::Error> {
        tokio::time::sleep(Duration::from_secs(1)).await;
        println!("cursor {}: {:?}", batch.cursor, batch.items);
        Ok(())
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    Pipeline::source(Numbers { end: 5 })
        .transform(|number| async move { Ok::<_, Infallible>(format!("number-{number}")) })
        .sink(PrintBatch)
        .batched(BatchPolicy::try_new(2, Duration::from_secs(1))?)
        .run_until(std::future::pending())
        .await?;

    Ok(())
}
