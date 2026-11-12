// Copyright (c) Solace Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! A partition that holds no state for an idempotent producer accepts the producer's first batch
//! at any sequence, as in Kafka.

use std::{
    assert_matches, io,
    sync::{Arc, Mutex},
};

use bytes::Bytes;
use nisshi_sans_io::{
    ErrorCode,
    create_topics_request::CreatableTopic,
    record::{Record, deflated, inflated},
};
use nisshi_storage::{Error, Storage as _, Topition};
use object_store::memory::InMemory;

use crate::dynostore::{DynoStore, tests::init_tracing};

fn storage() -> Result<DynoStore, Error> {
    DynoStore::new("nisshi", 111, InMemory::new())
}

fn idempotent_batch(producer_id: i64, base_sequence: i32) -> Result<deflated::Batch, Error> {
    inflated::Batch::builder()
        .producer_id(producer_id)
        .producer_epoch(0)
        .base_sequence(base_sequence)
        .record(Record::builder().value(Bytes::from_static(b"value").into()))
        .build()
        .and_then(deflated::Batch::try_from)
        .map_err(Into::into)
}

async fn create_topic(storage: &DynoStore, name: &str, partitions: i32) -> Result<(), Error> {
    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(name.into())
                .num_partitions(partitions)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    Ok(())
}

#[tokio::test]
async fn a_producer_the_broker_holds_starts_a_new_partition_at_any_sequence() -> Result<(), Error> {
    let _guard = init_tracing()?;
    let storage = storage()?;

    create_topic(&storage, "abc", 2).await?;
    let producer = storage.init_producer(None, 0, Some(-1), Some(-1)).await?.id;

    let first = Topition::new("abc", 0);

    // The producer never wrote to the partition, which holds no state for it.
    assert_eq!(
        0,
        storage
            .produce(None, &first, idempotent_batch(producer, 5)?)
            .await?
    );

    // The partition now holds the producer's state, so a batch that skips a sequence is refused
    // and the batch that continues where the first one ended is stored.
    assert_matches!(
        storage
            .produce(None, &first, idempotent_batch(producer, 7)?)
            .await,
        Err(Error::Api(ErrorCode::OutOfOrderSequenceNumber))
    );
    assert_eq!(
        1,
        storage
            .produce(None, &first, idempotent_batch(producer, 6)?)
            .await?
    );

    // The other partition still holds no state for the producer.
    assert_eq!(
        0,
        storage
            .produce(
                None,
                &Topition::new("abc", 1),
                idempotent_batch(producer, 100)?
            )
            .await?
    );

    Ok(())
}

#[tokio::test]
async fn a_first_sequence_that_is_negative_is_refused_and_stores_nothing() -> Result<(), Error> {
    let _guard = init_tracing()?;
    let storage = storage()?;

    create_topic(&storage, "abc", 1).await?;
    let producer = storage.init_producer(None, 0, Some(-1), Some(-1)).await?.id;
    let topition = Topition::new("abc", 0);

    assert_matches!(
        storage
            .produce(None, &topition, idempotent_batch(producer, -3)?)
            .await,
        Err(Error::Api(ErrorCode::InvalidRecord))
    );

    // The refused batch took no offset and left no sequence: the partition still holds no state.
    assert_eq!(0, storage.offset_stage(&topition).await?.high_watermark);
    assert_eq!(
        0,
        storage
            .produce(None, &topition, idempotent_batch(producer, 40)?)
            .await?
    );

    Ok(())
}

/// Collects the log lines that a test writes, so that the test can look for one.
#[derive(Clone, Default)]
struct CapturedLog(Arc<Mutex<Vec<u8>>>);

impl io::Write for CapturedLog {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.lock().expect("log lock").extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl CapturedLog {
    fn lines_containing(&self, text: &str) -> usize {
        String::from_utf8_lossy(&self.0.lock().expect("log lock"))
            .lines()
            .filter(|line| line.contains(text))
            .count()
    }
}

#[tokio::test]
async fn a_first_batch_above_sequence_zero_is_logged_once_for_its_partition() -> Result<(), Error> {
    let log = CapturedLog::default();
    let _guard = tracing::subscriber::set_default(
        tracing_subscriber::fmt()
            .with_ansi(false)
            .with_max_level(tracing::Level::INFO)
            .with_writer({
                let log = log.clone();
                move || log.clone()
            })
            .finish(),
    );

    let storage = storage()?;
    create_topic(&storage, "abc", 2).await?;
    let producer = storage.init_producer(None, 0, Some(-1), Some(-1)).await?.id;
    let message = "accepted a producer's first batch to a partition at a sequence above 0";

    // A first batch at sequence 0 starts the producer's sequence in the ordinary way.
    _ = storage
        .produce(
            None,
            &Topition::new("abc", 1),
            idempotent_batch(producer, 0)?,
        )
        .await?;
    assert_eq!(0, log.lines_containing(message));

    // The partition holds no state for the producer, so it accepts sequence 5 and logs it.
    let first = Topition::new("abc", 0);
    _ = storage
        .produce(None, &first, idempotent_batch(producer, 5)?)
        .await?;
    assert_eq!(1, log.lines_containing(message));
    assert_eq!(1, log.lines_containing(&format!("producer_id={producer}")));

    // The next batch continues the sequence that the partition now holds, so nothing is logged.
    _ = storage
        .produce(None, &first, idempotent_batch(producer, 6)?)
        .await?;
    assert_eq!(1, log.lines_containing(message));

    Ok(())
}
