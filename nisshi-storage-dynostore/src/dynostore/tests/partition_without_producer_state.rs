// Copyright ⓒ 2024-2026 Peter Morgan <peter.james.morgan@gmail.com>
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

use std::assert_matches;

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
