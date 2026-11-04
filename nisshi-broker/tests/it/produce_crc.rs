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

//! A produced batch whose CRC does not match its contents is rejected with
//! `CORRUPT_MESSAGE` on its own partition, as Kafka does, and nothing is
//! stored. The other partitions in the request are unaffected.
//!
//! Each batch is a `CreateTime` batch whose header `max_timestamp` already
//! matches its record, so the broker does not rewrite the header. A rewrite
//! checks the CRC on its own, so only a batch like this one shows that the
//! check covers every produced batch.

use crate::common::{alphanumeric_string, init_tracing};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::{Bytes, BytesMut};
use nisshi_broker::Result;
use nisshi_sans_io::{
    BatchAttribute, Compression, CreateTopicsRequest, ErrorCode, InitProducerIdRequest,
    ProduceRequest, RequestInput,
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
    CreateTopicsService, CreateTopicsValidationLayer, InitProducerIdService, ProduceService,
    Storage,
};
use rama::{Layer as _, Service as _, extensions::Extensions};
use rand::{RngExt as _, rng};
use uuid::Uuid;

const CORRUPT: &str = "batch crc does not match its contents";

fn now_ms() -> Result<i64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(Into::into)
        .and_then(|duration| i64::try_from(duration.as_millis()).map_err(Into::into))
}

async fn create_topic(storage: impl Storage + Clone, partitions: i32) -> Result<String> {
    let topic = alphanumeric_string(15);

    let response = CreateTopicsValidationLayer::new()
        .layer(CreateTopicsService { storage })
        .serve(RequestInput {
            request: CreateTopicsRequest::default()
                .validate_only(Some(false))
                .topics(Some(
                    [CreatableTopic::default()
                        .name(topic.clone())
                        .num_partitions(partitions)
                        .replication_factor(1)
                        .assignments(Some([].into()))
                        .configs(Some([].into()))]
                    .into(),
                )),
            extensions: Extensions::default(),
        })
        .await?;

    let topics = response.topics.as_deref().unwrap_or_default();
    assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);
    Ok(topic)
}

/// Sends one request with one batch for partition 0, the next for partition
/// 1, and so on, and returns the partition responses in that order.
async fn produce(
    storage: impl Storage + Clone,
    topic: &str,
    batches: Vec<deflated::Batch>,
) -> Result<Vec<PartitionProduceResponse>> {
    let partition_data = batches
        .into_iter()
        .enumerate()
        .map(|(index, batch)| {
            i32::try_from(index).map(|index| {
                PartitionProduceData::default()
                    .index(index)
                    .records(Some(Frame {
                        batches: vec![batch],
                    }))
            })
        })
        .collect::<std::result::Result<Vec<_>, _>>()?;

    let response = ProduceService::new(storage)
        .with_validation()
        .serve(RequestInput {
            request: ProduceRequest::default().acks(-1).topic_data(Some(vec![
                TopicProduceData::default()
                    .name(topic.into())
                    .partition_data(Some(partition_data)),
            ])),
            extensions: Extensions::default(),
        })
        .await?;

    let mut topics = response.responses.unwrap_or_default();
    assert_eq!(1, topics.len());
    Ok(topics.remove(0).partition_responses.unwrap_or_default())
}

/// A `CreateTime` batch with one record at `now`, whose header
/// `max_timestamp` is already `now`, so the broker does not rewrite it.
fn batch(
    compression: Compression,
    now: i64,
    builder: inflated::Builder,
) -> Result<deflated::Batch> {
    builder
        .attributes(BatchAttribute::default().compression(compression).into())
        .base_timestamp(now)
        .max_timestamp(now)
        .record(Record::builder().value(Bytes::from_static(b"Lorem ipsum dolor sit amet").into()))
        .build()
        .and_then(deflated::Batch::try_from)
        .map_err(Into::into)
}

/// Flips one bit in the middle byte of the record data, leaving `crc` as
/// sent. In an uncompressed batch that byte is in the record's value, so the
/// batch still decodes.
fn flipped(batch: deflated::Batch) -> deflated::Batch {
    let mut record_data = BytesMut::from(&batch.record_data[..]);
    let middle = record_data.len() / 2;
    record_data[middle] ^= 1;

    let flipped = deflated::Batch {
        record_data: record_data.freeze(),
        ..batch
    };

    assert_ne!(flipped.computed_crc(), flipped.crc);
    flipped
}

fn assert_corrupt(response: &PartitionProduceResponse) -> Result<()> {
    assert_eq!(
        ErrorCode::CorruptMessage,
        ErrorCode::try_from(response.error_code)?,
        "{response:?}"
    );
    assert_eq!(Some(CORRUPT), response.error_message.as_deref());
    Ok(())
}

fn assert_accepted_at(base_offset: i64, response: &PartitionProduceResponse) -> Result<()> {
    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(response.error_code)?,
        "{response:?}"
    );
    assert_eq!(base_offset, response.base_offset);
    Ok(())
}

/// The codecs without a checksum of their own (none, snappy, zstd as the
/// broker writes it) decode the flipped byte as a changed value. Without the
/// CRC check the broker stores those batches.
async fn flipped_byte_is_corrupt_for_every_codec(storage: impl Storage + Clone) -> Result<()> {
    let now = now_ms()?;

    for compression in [
        Compression::None,
        Compression::Gzip,
        Compression::Snappy,
        Compression::Lz4,
        Compression::Zstd,
    ] {
        let topic = create_topic(storage.clone(), 1).await?;
        let corrupt = flipped(batch(compression.clone(), now, inflated::Batch::builder())?);

        let responses = produce(storage.clone(), &topic, vec![corrupt]).await?;
        assert_corrupt(&responses[0])?;

        let responses = produce(
            storage.clone(),
            &topic,
            vec![batch(compression, now, inflated::Batch::builder())?],
        )
        .await?;
        assert_accepted_at(0, &responses[0])?;
    }

    Ok(())
}

/// Partition 1 is rejected. Partitions 0 and 2 store their batches at offset
/// 0, and the next request finds nothing stored on partition 1.
async fn only_the_corrupt_partition_fails(storage: impl Storage + Clone) -> Result<()> {
    let now = now_ms()?;
    let topic = create_topic(storage.clone(), 3).await?;
    let good = || batch(Compression::None, now, inflated::Batch::builder());

    let responses = produce(
        storage.clone(),
        &topic,
        vec![good()?, flipped(good()?), good()?],
    )
    .await?;
    assert_eq!(3, responses.len());
    assert_accepted_at(0, &responses[0])?;
    assert_corrupt(&responses[1])?;
    assert_accepted_at(0, &responses[2])?;

    let responses = produce(storage, &topic, vec![good()?, good()?, good()?]).await?;
    assert_accepted_at(1, &responses[0])?;
    assert_accepted_at(0, &responses[1])?;
    assert_accepted_at(1, &responses[2])?;

    Ok(())
}

/// A rejected idempotent batch leaves the producer's sequence where it was,
/// so the producer's retry with the same sequence is stored. If the rejected
/// batch had moved the sequence, the retry would get
/// `DUPLICATE_SEQUENCE_NUMBER`, which a client reports as success.
async fn corrupt_idempotent_batch_keeps_its_sequence(storage: impl Storage + Clone) -> Result<()> {
    let now = now_ms()?;
    let topic = create_topic(storage.clone(), 1).await?;

    let producer = InitProducerIdService {
        storage: storage.clone(),
    }
    .serve(RequestInput {
        request: InitProducerIdRequest::default()
            .transactional_id(None)
            .transaction_timeout_ms(0)
            .producer_id(Some(-1))
            .producer_epoch(Some(-1)),
        extensions: Extensions::default(),
    })
    .await?;
    assert_eq!(ErrorCode::None, ErrorCode::try_from(producer.error_code)?);

    let idempotent = || {
        batch(
            Compression::None,
            now,
            inflated::Batch::builder()
                .producer_id(producer.producer_id)
                .producer_epoch(producer.producer_epoch)
                .base_sequence(0),
        )
    };

    let responses = produce(storage.clone(), &topic, vec![flipped(idempotent()?)]).await?;
    assert_corrupt(&responses[0])?;

    let responses = produce(storage, &topic, vec![idempotent()?]).await?;
    assert_accepted_at(0, &responses[0])
}

#[cfg(feature = "dynostore")]
mod in_memory {
    use super::*;
    use crate::common::memory_storage;

    async fn storage() -> Result<nisshi_storage::ArcDynStorage> {
        memory_storage(Uuid::now_v7(), rng().random_range(0..i32::MAX)).await
    }

    #[tokio::test]
    /// The codecs without a checksum of their own (none, snappy, zstd as the
    /// broker writes it) decode the flipped byte as a changed value. Without the
    /// CRC check the broker stores those batches.
    async fn flipped_byte_is_corrupt_for_every_codec() -> Result<()> {
        let _guard = init_tracing()?;
        super::flipped_byte_is_corrupt_for_every_codec(storage().await?).await
    }

    #[tokio::test]
    /// Partition 1 is rejected. Partitions 0 and 2 store their batches at offset
    /// 0, and the next request finds nothing stored on partition 1.
    async fn only_the_corrupt_partition_fails() -> Result<()> {
        let _guard = init_tracing()?;
        super::only_the_corrupt_partition_fails(storage().await?).await
    }

    #[tokio::test]
    /// A rejected idempotent batch leaves the producer's sequence where it was,
    /// so the producer's retry with the same sequence is stored. If the rejected
    /// batch had moved the sequence, the retry would get
    /// `DUPLICATE_SEQUENCE_NUMBER`, which a client reports as success.
    async fn corrupt_idempotent_batch_keeps_its_sequence() -> Result<()> {
        let _guard = init_tracing()?;
        super::corrupt_idempotent_batch_keeps_its_sequence(storage().await?).await
    }
}

#[cfg(feature = "libsql")]
mod lite {
    use super::*;
    use crate::common::lite_storage;

    async fn storage() -> Result<nisshi_storage::ArcDynStorage> {
        lite_storage(Uuid::now_v7(), rng().random_range(0..i32::MAX)).await
    }

    #[tokio::test]
    /// The codecs without a checksum of their own (none, snappy, zstd as the
    /// broker writes it) decode the flipped byte as a changed value. Without the
    /// CRC check the broker stores those batches.
    async fn flipped_byte_is_corrupt_for_every_codec() -> Result<()> {
        let _guard = init_tracing()?;
        super::flipped_byte_is_corrupt_for_every_codec(storage().await?).await
    }

    #[tokio::test]
    /// Partition 1 is rejected. Partitions 0 and 2 store their batches at offset
    /// 0, and the next request finds nothing stored on partition 1.
    async fn only_the_corrupt_partition_fails() -> Result<()> {
        let _guard = init_tracing()?;
        super::only_the_corrupt_partition_fails(storage().await?).await
    }

    #[tokio::test]
    /// A rejected idempotent batch leaves the producer's sequence where it was,
    /// so the producer's retry with the same sequence is stored. If the rejected
    /// batch had moved the sequence, the retry would get
    /// `DUPLICATE_SEQUENCE_NUMBER`, which a client reports as success.
    async fn corrupt_idempotent_batch_keeps_its_sequence() -> Result<()> {
        let _guard = init_tracing()?;
        super::corrupt_idempotent_batch_keeps_its_sequence(storage().await?).await
    }
}

#[cfg(feature = "postgres")]
mod pg {
    use super::*;
    use crate::common::postgres_storage;

    async fn storage() -> Result<nisshi_storage::ArcDynStorage> {
        postgres_storage(Uuid::now_v7(), rng().random_range(0..i32::MAX)).await
    }

    #[tokio::test]
    /// The codecs without a checksum of their own (none, snappy, zstd as the
    /// broker writes it) decode the flipped byte as a changed value. Without the
    /// CRC check the broker stores those batches.
    async fn flipped_byte_is_corrupt_for_every_codec() -> Result<()> {
        let _guard = init_tracing()?;
        super::flipped_byte_is_corrupt_for_every_codec(storage().await?).await
    }

    #[tokio::test]
    /// Partition 1 is rejected. Partitions 0 and 2 store their batches at offset
    /// 0, and the next request finds nothing stored on partition 1.
    async fn only_the_corrupt_partition_fails() -> Result<()> {
        let _guard = init_tracing()?;
        super::only_the_corrupt_partition_fails(storage().await?).await
    }

    #[tokio::test]
    /// A rejected idempotent batch leaves the producer's sequence where it was,
    /// so the producer's retry with the same sequence is stored. If the rejected
    /// batch had moved the sequence, the retry would get
    /// `DUPLICATE_SEQUENCE_NUMBER`, which a client reports as success.
    async fn corrupt_idempotent_batch_keeps_its_sequence() -> Result<()> {
        let _guard = init_tracing()?;
        super::corrupt_idempotent_batch_keeps_its_sequence(storage().await?).await
    }
}
