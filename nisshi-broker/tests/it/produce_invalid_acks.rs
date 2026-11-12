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

//! A Produce request whose `acks` is not -1, 0 or 1 gets `INVALID_REQUIRED_ACKS`
//! for every partition, and nothing from the request is written.

use crate::common::{StorageType, alphanumeric_string, init_tracing};
use bytes::Bytes;
use nisshi_broker::Result;
use nisshi_sans_io::{
    Ack, CreateTopicsRequest, ErrorCode, IsolationLevel, ListOffset, ListOffsetsRequest,
    ProduceRequest, RequestInput,
    create_topics_request::CreatableTopic,
    list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic},
    produce_request::{PartitionProduceData, TopicProduceData},
    produce_response::PartitionProduceResponse,
    record::{
        Record,
        deflated::{self, Frame},
        inflated,
    },
};
use nisshi_storage::{
    ArcDynStorage, CreateTopicsService, CreateTopicsValidationLayer, ListOffsetsService,
    ProduceService, Storage,
};
use rama::{Layer as _, Service as _, extensions::Extensions};
use rand::{RngExt as _, rng};
use url::Url;
use uuid::Uuid;

const PARTITIONS: [i32; 2] = [0, 1];

/// Values outside -1, 0 and 1, including both ends of the range.
const INVALID_ACKS: [i16; 6] = [-2, 2, -129, 32767, i16::MIN, i16::MAX];

async fn storage(storage_type: StorageType) -> Result<ArcDynStorage> {
    crate::common::storage_container(
        storage_type,
        Uuid::now_v7(),
        rng().random_range(0..i32::MAX),
        Url::parse("tcp://127.0.0.1/")?,
        None,
    )
    .await
}

async fn create_topic(storage: &(impl Storage + Clone), name: &str) -> Result<()> {
    let response = CreateTopicsValidationLayer::new()
        .layer(CreateTopicsService {
            storage: storage.clone(),
        })
        .serve(RequestInput {
            request: CreateTopicsRequest::default()
                .validate_only(Some(false))
                .topics(Some(
                    [CreatableTopic::default()
                        .name(name.into())
                        .num_partitions(PARTITIONS.len() as i32)
                        .replication_factor(1)
                        .assignments(Some([].into()))
                        .configs(Some([].into()))]
                    .into(),
                )),
            extensions: Extensions::default(),
        })
        .await?;

    let topics = response.topics.as_deref().unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);

    Ok(())
}

fn well_formed(value: &'static [u8]) -> Result<deflated::Batch> {
    inflated::Batch::builder()
        .record(Record::builder().value(Bytes::from_static(value).into()))
        .build()
        .and_then(deflated::Batch::try_from)
        .map_err(Into::into)
}

/// Produces one well-formed batch to each partition with `acks`, and returns
/// the partition responses.
async fn produce(
    storage: &(impl Storage + Clone),
    name: &str,
    acks: i16,
) -> Result<Vec<PartitionProduceResponse>> {
    let partition_data = PARTITIONS
        .into_iter()
        .map(|index| {
            well_formed(b"Lorem ipsum dolor sit amet").map(|batch| {
                PartitionProduceData::default()
                    .index(index)
                    .records(Some(Frame {
                        batches: vec![batch],
                    }))
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let response = ProduceService::new(storage.clone())
        .with_validation()
        .serve(RequestInput {
            request: ProduceRequest::default().acks(acks).topic_data(Some(
                [TopicProduceData::default()
                    .name(name.into())
                    .partition_data(Some(partition_data))]
                .into(),
            )),
            extensions: Extensions::default(),
        })
        .await?;

    let topics = response.responses.unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(name, topics[0].name);

    let partitions = topics[0].partition_responses.clone().unwrap_or_default();
    assert_eq!(PARTITIONS.len(), partitions.len());

    Ok(partitions)
}

/// The partition's high watermark, from ListOffsets(latest).
async fn latest(
    storage: &(impl Storage + Clone),
    name: &str,
    partition: i32,
) -> Result<Option<i64>> {
    let response = ListOffsetsService {
        storage: storage.clone(),
    }
    .serve(RequestInput {
        request: ListOffsetsRequest::default()
            .isolation_level(Some(IsolationLevel::ReadUncommitted.into()))
            .topics(Some(
                [ListOffsetsTopic::default()
                    .name(name.into())
                    .partitions(Some(
                        [ListOffsetsPartition::default()
                            .partition_index(partition)
                            .timestamp(ListOffset::Latest.try_into()?)]
                        .into(),
                    ))]
                .into(),
            )),
        extensions: Extensions::default(),
    })
    .await?;

    let topics = response.topics.as_deref().unwrap_or_default();
    assert_eq!(1, topics.len());

    let partitions = topics[0].partitions.as_deref().unwrap_or_default();
    assert_eq!(1, partitions.len());
    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(partitions[0].error_code)?
    );

    Ok(partitions[0].offset)
}

/// Every partition is refused for each invalid `acks`. Afterwards both
/// partitions are still empty, and a produce with `acks=-1` lands at offset 0
/// on each, so the refused produces consumed no offset. On Postgres and libSQL
/// a leftover row would also make that insert collide with it.
async fn invalid_acks_is_refused(storage: impl Storage + Clone) -> Result<()> {
    let name = &alphanumeric_string(15)[..];
    create_topic(&storage, name).await?;

    for acks in INVALID_ACKS {
        for (index, response) in PARTITIONS
            .into_iter()
            .zip(produce(&storage, name, acks).await?)
        {
            assert_eq!(index, response.index);
            assert_eq!(
                ErrorCode::InvalidRequiredAcks,
                ErrorCode::try_from(response.error_code)?,
                "acks: {acks}, {response:?}"
            );
            assert_eq!(-1, response.base_offset, "acks: {acks}");
        }
    }

    for partition in PARTITIONS {
        assert_eq!(Some(0), latest(&storage, name, partition).await?);
    }

    for response in produce(&storage, name, i16::from(Ack::FullIsr)).await? {
        assert_eq!(
            ErrorCode::None,
            ErrorCode::try_from(response.error_code)?,
            "{response:?}"
        );
        assert_eq!(0, response.base_offset);
    }

    for partition in PARTITIONS {
        assert_eq!(Some(1), latest(&storage, name, partition).await?);
    }

    Ok(())
}

/// A partition of a topic that does not exist gets `INVALID_REQUIRED_ACKS`.
/// Kafka answers it `UNKNOWN_TOPIC_OR_PARTITION`, because it checks `acks`
/// after it finds the partition. We check `acks` before any storage call.
async fn unknown_topic_is_refused_for_invalid_acks(storage: impl Storage + Clone) -> Result<()> {
    let name = &alphanumeric_string(15)[..];

    for response in produce(&storage, name, 2).await? {
        assert_eq!(
            ErrorCode::InvalidRequiredAcks,
            ErrorCode::try_from(response.error_code)?,
            "{response:?}"
        );
        assert_eq!(-1, response.base_offset);
    }

    Ok(())
}

#[cfg(feature = "dynostore")]
mod in_memory {
    use super::*;

    #[tokio::test]
    async fn unknown_topic_is_refused_for_invalid_acks() -> Result<()> {
        let _guard = init_tracing()?;
        super::unknown_topic_is_refused_for_invalid_acks(storage(StorageType::InMemory).await?)
            .await
    }

    #[tokio::test]
    async fn invalid_acks_is_refused() -> Result<()> {
        let _guard = init_tracing()?;
        super::invalid_acks_is_refused(storage(StorageType::InMemory).await?).await
    }
}

#[cfg(feature = "libsql")]
mod lite {
    use super::*;

    #[tokio::test]
    async fn invalid_acks_is_refused() -> Result<()> {
        let _guard = init_tracing()?;
        super::invalid_acks_is_refused(storage(StorageType::Lite).await?).await
    }
}

#[cfg(feature = "postgres")]
mod pg {
    use super::*;

    #[tokio::test]
    async fn invalid_acks_is_refused() -> Result<()> {
        let _guard = init_tracing()?;
        super::invalid_acks_is_refused(storage(StorageType::Postgres).await?).await
    }
}
