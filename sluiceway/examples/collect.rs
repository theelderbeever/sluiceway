#![allow(clippy::print_stdout)]

use std::{convert::Infallible, fmt::Write as _, time::Duration};

use futures_core::Stream;
use futures_util::stream;
use sluiceway::{
    CollectPolicy, CollectionSession, Collector, CommitPolicy, Pipeline, Record, Source,
};

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

/// Builds an NDJSON-shaped buffer as records arrive instead of retaining a Vec<Record<...>>.
struct Ndjson;

struct NdjsonSession {
    buffer: String,
}

impl Collector<Record<u64, u64>> for Ndjson {
    type Session = NdjsonSession;
    type Error = std::fmt::Error;

    async fn begin(&self) -> Result<Self::Session, Self::Error> {
        Ok(NdjsonSession {
            buffer: String::new(),
        })
    }
}

impl CollectionSession<Record<u64, u64>> for NdjsonSession {
    type Error = std::fmt::Error;

    async fn push(&mut self, record: Record<u64, u64>) -> Result<(), Self::Error> {
        writeln!(self.buffer, "{{\"number\":{}}}", record.payload)
    }

    async fn finish(self) -> Result<(), Self::Error> {
        // A real collector would durably flush its encoder, upload, or database insert here.
        print!("{}", self.buffer);
        Ok(())
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    Pipeline::source(Numbers { end: 5 })
        .transform(|_position: &u64, number| async move { Ok::<_, Infallible>(number * number) })
        .sink(Ndjson)
        .collect(CollectPolicy::try_new(2, Duration::from_secs(1))?)
        .commit_policy(CommitPolicy::after(4)?)
        .run()
        .await?;

    Ok(())
}
