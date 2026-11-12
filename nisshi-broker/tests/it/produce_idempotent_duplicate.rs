// Copyright ⓒ Solace Corporation
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

//! An idempotent batch whose sequence is below the producer's next sequence is
//! a duplicate only when it repeats one of the producer's last 5 batches on the
//! partition, as in Kafka. The broker answers a duplicate with the offset of
//! the stored batch, and answers any other sequence that is not the next one
//! with `OUT_OF_ORDER_SEQUENCE_NUMBER`. Clients count a duplicate as delivered,
//! so a new batch answered as one would be acknowledged and lost.

use nisshi_storage::fetched::FetchUntilDeadline as _;
use std::{assert_matches, time::Duration};
use tokio::time::Instant;

use crate::common::{alphanumeric_string, init_tracing, register_broker};
use bytes::Bytes;
use nisshi_broker::Result;
use nisshi_sans_io::{
    ErrorCode, IsolationLevel,
    create_topics_request::CreatableTopic,
    record::{Record, deflated, inflated},
};
use nisshi_storage::{Error, Storage, Topition};
use rand::{RngExt as _, rng};
use uuid::Uuid;

/// An idempotent producer and the partition it writes to.
struct Producer<G> {
    storage: G,
    topition: Topition,
    id: i64,
    epoch: i16,
}

impl<G> Producer<G>
where
    G: Storage,
{
    async fn new(storage: G) -> Result<Self> {
        let topic = alphanumeric_string(15);

        _ = storage
            .create_topic(
                CreatableTopic::default()
                    .name(topic.clone())
                    .num_partitions(1)
                    .replication_factor(0)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            )
            .await?;

        let producer = storage.init_producer(None, 0, Some(-1), Some(-1)).await?;
        assert_eq!(ErrorCode::None, producer.error);

        Ok(Self {
            storage,
            topition: Topition::new(topic, 0),
            id: producer.id,
            epoch: producer.epoch,
        })
    }

    /// Produces a batch of `record_count` records, starting at `base_sequence`.
    /// Each record's value names its sequence, so that a fetch shows which
    /// batch stored it.
    async fn produce(
        &self,
        epoch: i16,
        base_sequence: i32,
        record_count: i32,
    ) -> nisshi_storage::Result<i64> {
        let mut builder = inflated::Batch::builder()
            .producer_id(self.id)
            .producer_epoch(epoch)
            .base_sequence(base_sequence)
            .last_offset_delta(record_count - 1);

        for delta in 0..record_count {
            builder = builder.record(Record::builder().offset_delta(delta).value(Some(
                Bytes::from(format!("epoch-{epoch}-sequence-{}", base_sequence + delta)),
            )));
        }

        let batch = builder
            .build()
            .and_then(deflated::Batch::try_from)
            .expect("well-formed batch");

        self.storage.produce(None, &self.topition, batch).await
    }

    async fn high_watermark(&self) -> Result<i64> {
        Ok(self
            .storage
            .offset_stage(&self.topition)
            .await
            .map(|stage| stage.high_watermark())?)
    }

    /// Counts the records that a fetch from the start of the partition returns,
    /// up to its high watermark.
    async fn stored_record_count(&self) -> Result<i64> {
        let high_watermark = self.high_watermark().await?;
        let mut offset = 0;
        let mut record_count = 0;

        while offset < high_watermark {
            let batches = self
                .storage
                .fetch_until_deadline(
                    &self.topition,
                    offset,
                    1,
                    1 << 20,
                    IsolationLevel::ReadUncommitted,
                    Instant::now() + Duration::from_millis(500),
                )
                .await
                .map(|read| read.batches)?;

            let Some(last) = batches.last() else {
                break;
            };

            offset = last.base_offset + i64::from(last.last_offset_delta) + 1;
            record_count += batches
                .iter()
                .map(|batch| i64::from(batch.record_count))
                .sum::<i64>();
        }

        Ok(record_count)
    }
}

pub async fn retry_of_a_recent_batch_answers_its_offset<G>(storage: G) -> Result<()>
where
    G: Storage,
{
    let producer = Producer::new(storage).await?;
    let epoch = producer.epoch;

    assert_eq!(0, producer.produce(epoch, 0, 3).await?);
    assert_eq!(3, producer.produce(epoch, 3, 2).await?);

    assert_eq!(3, producer.produce(epoch, 3, 2).await?);
    assert_eq!(0, producer.produce(epoch, 0, 3).await?);

    assert_eq!(5, producer.high_watermark().await?);
    assert_eq!(5, producer.stored_record_count().await?);

    assert_eq!(5, producer.produce(epoch, 5, 1).await?);

    Ok(())
}

pub async fn retry_older_than_the_last_five_batches_is_out_of_order<G>(storage: G) -> Result<()>
where
    G: Storage,
{
    let producer = Producer::new(storage).await?;
    let epoch = producer.epoch;

    for batch in 0..6 {
        assert_eq!(
            i64::from(batch * 2),
            producer.produce(epoch, batch * 2, 2).await?
        );
    }

    assert_matches!(
        producer.produce(epoch, 0, 2).await,
        Err(Error::Api(ErrorCode::OutOfOrderSequenceNumber))
    );

    assert_eq!(2, producer.produce(epoch, 2, 2).await?);
    assert_eq!(12, producer.high_watermark().await?);

    Ok(())
}

pub async fn lower_sequence_inside_a_batch_is_out_of_order<G>(storage: G) -> Result<()>
where
    G: Storage,
{
    let producer = Producer::new(storage).await?;
    let epoch = producer.epoch;

    assert_eq!(0, producer.produce(epoch, 0, 4).await?);
    assert_eq!(4, producer.produce(epoch, 4, 4).await?);

    assert_matches!(
        producer.produce(epoch, 2, 1).await,
        Err(Error::Api(ErrorCode::OutOfOrderSequenceNumber))
    );

    assert_eq!(8, producer.high_watermark().await?);
    assert_eq!(8, producer.stored_record_count().await?);

    Ok(())
}

pub async fn same_base_sequence_with_another_record_count_is_out_of_order<G>(
    storage: G,
) -> Result<()>
where
    G: Storage,
{
    let producer = Producer::new(storage).await?;
    let epoch = producer.epoch;

    assert_eq!(0, producer.produce(epoch, 0, 3).await?);

    assert_matches!(
        producer.produce(epoch, 0, 2).await,
        Err(Error::Api(ErrorCode::OutOfOrderSequenceNumber))
    );

    assert_eq!(3, producer.high_watermark().await?);

    Ok(())
}

/// A client whose epoch wraps back to the stored epoch restarts its sequences
/// at 0, so its new batches have sequences that the producer already used.
pub async fn new_batch_at_a_used_sequence_is_out_of_order<G>(storage: G) -> Result<()>
where
    G: Storage,
{
    let producer = Producer::new(storage).await?;
    let epoch = producer.epoch;

    for batch in 0..4 {
        assert_eq!(
            i64::from(batch * 5),
            producer.produce(epoch, batch * 5, 5).await?
        );
    }

    assert_matches!(
        producer.produce(epoch, 0, 1).await,
        Err(Error::Api(ErrorCode::OutOfOrderSequenceNumber))
    );

    assert_eq!(20, producer.high_watermark().await?);
    assert_eq!(20, producer.stored_record_count().await?);

    Ok(())
}

mod in_memory {
    use super::*;
    use crate::common::memory_storage;
    use nisshi_storage::ArcDynStorage;

    async fn storage() -> Result<ArcDynStorage> {
        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = memory_storage(cluster_id, broker_id).await?;
        register_broker(cluster_id, broker_id, &storage).await?;

        Ok(storage)
    }

    #[tokio::test]
    async fn retry_of_a_recent_batch_answers_its_offset() -> Result<()> {
        let _guard = init_tracing()?;
        super::retry_of_a_recent_batch_answers_its_offset(storage().await?).await
    }

    #[tokio::test]
    async fn retry_older_than_the_last_five_batches_is_out_of_order() -> Result<()> {
        let _guard = init_tracing()?;
        super::retry_older_than_the_last_five_batches_is_out_of_order(storage().await?).await
    }

    #[tokio::test]
    async fn lower_sequence_inside_a_batch_is_out_of_order() -> Result<()> {
        let _guard = init_tracing()?;
        super::lower_sequence_inside_a_batch_is_out_of_order(storage().await?).await
    }

    #[tokio::test]
    async fn same_base_sequence_with_another_record_count_is_out_of_order() -> Result<()> {
        let _guard = init_tracing()?;
        super::same_base_sequence_with_another_record_count_is_out_of_order(storage().await?).await
    }

    #[tokio::test]
    async fn new_batch_at_a_used_sequence_is_out_of_order() -> Result<()> {
        let _guard = init_tracing()?;
        super::new_batch_at_a_used_sequence_is_out_of_order(storage().await?).await
    }
}
