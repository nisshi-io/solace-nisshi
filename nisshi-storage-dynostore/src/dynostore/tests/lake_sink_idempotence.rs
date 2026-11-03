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

//! A produce to a topic with `nisshi.lake.sink=true` checks an idempotent batch's sequence, as a
//! produce to any other topic does.

use std::assert_matches;

use bytes::Bytes;
use nisshi_sans_io::{
    ErrorCode,
    create_topics_request::{CreatableTopic, CreatableTopicConfig},
    record::{Record, deflated, inflated},
    topic_config,
};
use nisshi_schema::lake::House;
use nisshi_storage::{Error, Storage as _, Topition};
use object_store::memory::InMemory;

use crate::dynostore::{DynoStore, tests::init_tracing};

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

/// Returns a store with a lake and one lake-sink topic, `abc`, with one partition.
async fn lake_sink_storage() -> Result<DynoStore, Error> {
    // `House::None` sends the produce down the lake-sink path, and without a schema registry
    // that path writes nothing to the lake, so the test needs no lake service.
    let storage = DynoStore::new("nisshi", 111, InMemory::new())?.lake(Some(House::None));

    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name("abc".into())
                .num_partitions(1)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some(
                    [CreatableTopicConfig::default()
                        .name(topic_config::LAKE_SINK.into())
                        .value(Some("true".into()))]
                    .into(),
                )),
            false,
        )
        .await?;

    Ok(storage)
}

#[tokio::test]
async fn retried_idempotent_batch_to_lake_sink_returns_first_offset() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = lake_sink_storage().await?;
    let topition = Topition::new("abc", 0);
    let producer = storage.init_producer(None, 0, Some(-1), Some(-1)).await?;

    assert_eq!(
        0,
        storage
            .produce(None, &topition, idempotent_batch(producer.id, 0)?)
            .await?
    );

    assert_eq!(
        0,
        storage
            .produce(None, &topition, idempotent_batch(producer.id, 0)?)
            .await?
    );

    // The retry takes no offset, so the partition still holds one record.
    assert_eq!(1, storage.offset_stage(&topition).await?.high_watermark);

    Ok(())
}

#[tokio::test]
async fn out_of_order_idempotent_batch_to_lake_sink_is_rejected() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = lake_sink_storage().await?;
    let topition = Topition::new("abc", 0);
    let producer = storage.init_producer(None, 0, Some(-1), Some(-1)).await?;

    assert_eq!(
        0,
        storage
            .produce(None, &topition, idempotent_batch(producer.id, 0)?)
            .await?
    );

    // The next batch must start at sequence 1, so sequence 5 leaves a gap.
    assert_matches!(
        storage
            .produce(None, &topition, idempotent_batch(producer.id, 5)?)
            .await,
        Err(Error::Api(ErrorCode::OutOfOrderSequenceNumber))
    );

    Ok(())
}
