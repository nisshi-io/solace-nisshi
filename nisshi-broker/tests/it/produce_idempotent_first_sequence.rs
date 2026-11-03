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
//! at any sequence, as in Kafka. A producer that the broker lost state for (or whose topic was
//! deleted) resumes at the sequence it had, and a client that is answered an error for that
//! never recovers.

use std::assert_matches;

use crate::common::{alphanumeric_string, init_tracing};
use bytes::Bytes;
use nisshi_broker::Result;
use nisshi_sans_io::{
    ErrorCode,
    create_topics_request::CreatableTopic,
    record::{Record, deflated, inflated},
};
use nisshi_storage::{Error, Storage, Topition};

/// Produces to `topition` as `producer`, a batch of `record_count` records at `base_sequence`.
async fn produce<G>(
    storage: &G,
    topition: &Topition,
    producer: i64,
    base_sequence: i32,
    record_count: i32,
) -> nisshi_storage::Result<i64>
where
    G: Storage,
{
    let mut builder = inflated::Batch::builder()
        .producer_id(producer)
        .producer_epoch(0)
        .base_sequence(base_sequence)
        .last_offset_delta(record_count - 1);

    for delta in 0..record_count {
        builder = builder.record(
            Record::builder()
                .offset_delta(delta)
                .value(Some(Bytes::from_static(b"value"))),
        );
    }

    let batch = builder
        .build()
        .and_then(deflated::Batch::try_from)
        .expect("well-formed batch");

    storage.produce(None, topition, batch).await
}

/// Creates a topic of one partition and an idempotent producer.
async fn topic_and_producer<G>(storage: &G) -> Result<(Topition, i64)>
where
    G: Storage,
{
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

    Ok((Topition::new(topic, 0), producer.id))
}

pub async fn a_new_partition_accepts_any_first_sequence<G>(storage: G) -> Result<()>
where
    G: Storage,
{
    let (topition, producer) = topic_and_producer(&storage).await?;

    // The partition holds no state for the producer, so it accepts sequence 5, and the next
    // sequence it stores for the producer is 8.
    assert_eq!(0, produce(&storage, &topition, producer, 5, 3).await?);

    assert_matches!(
        produce(&storage, &topition, producer, 9, 1).await,
        Err(Error::Api(ErrorCode::OutOfOrderSequenceNumber))
    );
    assert_eq!(3, produce(&storage, &topition, producer, 8, 2).await?);
    assert_eq!(5, storage.offset_stage(&topition).await?.high_watermark());

    Ok(())
}

pub async fn a_new_partition_accepts_the_largest_first_sequence_and_wraps<G>(
    storage: G,
) -> Result<()>
where
    G: Storage,
{
    let (topition, producer) = topic_and_producer(&storage).await?;

    // The batch ends at the largest sequence, so the next one wraps to 0, as in Kafka.
    assert_eq!(
        0,
        produce(&storage, &topition, producer, i32::MAX - 1, 2).await?
    );
    assert_eq!(2, produce(&storage, &topition, producer, 0, 1).await?);

    Ok(())
}

pub async fn a_negative_first_sequence_is_refused_and_stores_nothing<G>(storage: G) -> Result<()>
where
    G: Storage,
{
    let (topition, producer) = topic_and_producer(&storage).await?;

    assert_matches!(
        produce(&storage, &topition, producer, -3, 1).await,
        Err(Error::Api(ErrorCode::InvalidRecord))
    );
    assert_eq!(0, storage.offset_stage(&topition).await?.high_watermark());

    // The refused batch left no state, so the producer still starts at any sequence.
    assert_eq!(0, produce(&storage, &topition, producer, 40, 1).await?);

    Ok(())
}

macro_rules! backend_tests {
    ($module:ident, $feature:literal, $storage:path) => {
        #[cfg(feature = $feature)]
        mod $module {
            use nisshi_broker::Result;
            use rand::{RngExt as _, rng};
            use uuid::Uuid;

            use super::*;

            async fn storage() -> Result<impl Storage + Clone> {
                $storage(Uuid::now_v7(), rng().random_range(0..i32::MAX)).await
            }

            #[tokio::test]
            async fn a_new_partition_accepts_any_first_sequence() -> Result<()> {
                let _guard = init_tracing()?;
                super::a_new_partition_accepts_any_first_sequence(storage().await?).await
            }

            #[tokio::test]
            async fn a_new_partition_accepts_the_largest_first_sequence_and_wraps() -> Result<()> {
                let _guard = init_tracing()?;
                super::a_new_partition_accepts_the_largest_first_sequence_and_wraps(
                    storage().await?,
                )
                .await
            }

            #[tokio::test]
            async fn a_negative_first_sequence_is_refused_and_stores_nothing() -> Result<()> {
                let _guard = init_tracing()?;
                super::a_negative_first_sequence_is_refused_and_stores_nothing(storage().await?)
                    .await
            }
        }
    };
}

#[cfg(feature = "turso")]
async fn turso_storage(cluster: uuid::Uuid, node: i32) -> Result<nisshi_storage::ArcDynStorage> {
    crate::common::storage_container(
        crate::common::StorageType::Turso,
        cluster,
        node,
        url::Url::parse("tcp://127.0.0.1/")?,
        None,
    )
    .await
}

backend_tests!(in_memory, "dynostore", crate::common::memory_storage);
backend_tests!(lite, "libsql", crate::common::lite_storage);
backend_tests!(pg, "postgres", crate::common::postgres_storage);
backend_tests!(turso, "turso", turso_storage);
