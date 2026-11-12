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

//! A batch that `ProduceService` rejects, sent to a partition that does not exist, gets
//! `UNKNOWN_TOPIC_OR_PARTITION`, as in Kafka. Kafka looks up the partition in
//! `ReplicaManager.appendToLocalLog` before `UnifiedLog.append` validates the batch.
//!
//! `ProduceService` validates the batch before it calls storage, so it asks storage for the
//! partition when it rejects a batch. The same batch sent to a partition that exists keeps its
//! own error code.

use std::assert_matches;

use crate::common::{StorageType, alphanumeric_string, init_tracing};
use bytes::Bytes;
use nisshi_broker::Result;
use nisshi_sans_io::{
    Ack, BatchAttribute, CreateTopicsRequest, ErrorCode, ProduceRequest, RequestInput,
    create_topics_request::CreatableTopic,
    produce_request::{PartitionProduceData, TopicProduceData},
    produce_response::PartitionProduceResponse,
    record::{
        Record,
        deflated::{self, Frame},
        inflated,
    },
};
use nisshi_storage::{
    ArcDynStorage, CreateTopicsService, CreateTopicsValidationLayer, ProduceService, Storage,
};
use rama::{Layer as _, Service as _, extensions::Extensions};
use rand::{RngExt as _, rng};
use url::Url;
use uuid::Uuid;

const EXISTING: i32 = 0;
const MISSING: i32 = 7;

/// Bytes from the start of a v2 batch to the first byte covered by its CRC: base offset, batch
/// length, partition leader epoch, magic and the CRC.
const CRC_START: usize = 8 + 4 + 4 + 1 + 4;

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
                        .num_partitions(1)
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

async fn produce(
    storage: &(impl Storage + Clone),
    name: &str,
    index: i32,
    batch: deflated::Batch,
) -> Result<PartitionProduceResponse> {
    let response = ProduceService::new(storage.clone())
        .with_validation()
        .serve(RequestInput {
            request: ProduceRequest::default()
                .acks(i16::from(Ack::Leader))
                .topic_data(Some(
                    [TopicProduceData::default()
                        .name(name.into())
                        .partition_data(Some(
                            [PartitionProduceData::default()
                                .index(index)
                                .records(Some(Frame {
                                    batches: vec![batch],
                                }))]
                            .into(),
                        ))]
                    .into(),
                )),
            extensions: Extensions::default(),
        })
        .await?;

    let topics = response.responses.unwrap_or_default();
    assert_eq!(1, topics.len());

    let mut partitions = topics[0].partition_responses.clone().unwrap_or_default();
    assert_eq!(1, partitions.len());

    Ok(partitions.remove(0))
}

fn well_formed() -> Result<deflated::Batch> {
    inflated::Batch::builder()
        .record(Record::builder().value(Bytes::from_static(b"well formed").into()))
        .build()
        .and_then(deflated::Batch::try_from)
        .map_err(Into::into)
}

/// `batch` with its attributes and record data replaced, and its batch length and CRC recomputed
/// to match, so that only the decode, not the CRC check, can reject it.
fn rebuilt(mut batch: deflated::Batch, attributes: i16, record_data: Bytes) -> deflated::Batch {
    batch.batch_length =
        batch.batch_length - batch.record_data.len() as i32 + record_data.len() as i32;
    batch.attributes = attributes;
    batch.record_data = record_data;

    let encoded = Bytes::from(batch.clone());
    let mut digest = crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc32Iscsi);
    digest.update(&encoded[CRC_START..]);
    batch.crc = digest.finalize() as u32;

    batch
}

/// A batch whose one record is cut short in its value, so that it does not decode.
fn truncated() -> Result<deflated::Batch> {
    let batch = well_formed()?;
    let record_data = batch.record_data.slice(..batch.record_data.len() - 4);

    Ok(rebuilt(
        batch,
        BatchAttribute::default().into(),
        record_data,
    ))
}

/// A batch whose codec bits hold 7, which no Kafka codec uses.
fn unknown_codec() -> Result<deflated::Batch> {
    let batch = well_formed()?;
    let attributes = i16::from(BatchAttribute::default()) | 0b111;
    let record_data = batch.record_data.clone();

    Ok(rebuilt(batch, attributes, record_data))
}

async fn rejected_batch_checks_the_partition_first(storage: impl Storage + Clone) -> Result<()> {
    let name = &alphanumeric_string(15)[..];
    create_topic(&storage, name).await?;

    for (reason, batch) in [
        ("undecodable records", truncated()?),
        ("unknown codec", unknown_codec()?),
    ] {
        let response = produce(&storage, name, MISSING, batch.clone()).await?;
        assert_eq!(
            ErrorCode::UnknownTopicOrPartition,
            ErrorCode::try_from(response.error_code)?,
            "{reason}: {response:?}"
        );
        assert_eq!(-1, response.base_offset);

        let response = produce(&storage, name, EXISTING, batch).await?;
        assert_eq!(
            ErrorCode::InvalidRecord,
            ErrorCode::try_from(response.error_code)?,
            "{reason}: {response:?}"
        );
    }

    // The partition that exists still takes a well-formed batch.
    let response = produce(&storage, name, EXISTING, well_formed()?).await?;
    assert_matches!(ErrorCode::try_from(response.error_code)?, ErrorCode::None);
    assert_eq!(0, response.base_offset);

    Ok(())
}

#[cfg(feature = "dynostore")]
mod in_memory {
    use super::*;

    #[tokio::test]
    async fn rejected_batch_checks_the_partition_first() -> Result<()> {
        let _guard = init_tracing()?;
        super::rejected_batch_checks_the_partition_first(storage(StorageType::InMemory).await?)
            .await
    }
}

#[cfg(feature = "libsql")]
mod lite {
    use super::*;

    #[tokio::test]
    async fn rejected_batch_checks_the_partition_first() -> Result<()> {
        let _guard = init_tracing()?;
        super::rejected_batch_checks_the_partition_first(storage(StorageType::Lite).await?).await
    }
}

#[cfg(feature = "postgres")]
mod pg {
    use super::*;

    #[tokio::test]
    async fn rejected_batch_checks_the_partition_first() -> Result<()> {
        let _guard = init_tracing()?;
        super::rejected_batch_checks_the_partition_first(storage(StorageType::Postgres).await?)
            .await
    }
}
