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

//! A Produce request whose batches are malformed gets `INVALID_RECORD` for the
//! partition, and nothing from the request is written.
//!
//! Malformed means: records that do not decode, an unknown compression codec,
//! or other than exactly one batch for the partition, including a zero-length
//! records field. Every malformed batch here carries a valid CRC as sent, so
//! the tests still exercise decoding if produce starts enforcing the CRC.
//!
//! These tests check the error code that a client gets on each backend. `ProduceService` decodes
//! every batch before a storage backend gets it, so a batch that does not decode never reaches a
//! backend.

use crate::common::{StorageType, alphanumeric_string, init_tracing};
use bytes::Bytes;
use nisshi_broker::{Error, Result};
use nisshi_sans_io::{
    Ack, BatchAttribute, Compression, CreateTopicsRequest, ErrorCode, InitProducerIdRequest,
    IsolationLevel, ListOffset, ListOffsetsRequest, ProduceRequest, RequestInput,
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
use nisshi_schema::Registry;
use nisshi_storage::{
    ArcDynStorage, CreateTopicsService, CreateTopicsValidationLayer, InitProducerIdService,
    ListOffsetsService, ProduceService, Storage,
};
use rama::{Layer as _, Service as _, extensions::Extensions};
use rand::{RngExt as _, rng};
use url::Url;
use uuid::Uuid;

const PARTITION: i32 = 0;

/// The error message for a partition with other than one batch.
const NOT_EXACTLY_ONE_BATCH: &str =
    "a produce request must contain exactly one record batch per partition";

/// Bytes from the start of a v2 batch to the first byte covered by its CRC:
/// base offset, batch length, partition leader epoch, magic and the CRC.
const CRC_START: usize = 8 + 4 + 4 + 1 + 4;

async fn storage(storage_type: StorageType, with_registry: bool) -> Result<ArcDynStorage> {
    let schemas = if with_registry {
        Url::parse("file://../etc/schema")
            .map_err(Error::from)
            .and_then(|url| {
                Registry::builder_try_from_url(&url)
                    .map(|builder| builder.build())
                    .map_err(Into::into)
            })
            .map(Some)?
    } else {
        None
    };

    crate::common::storage_container(
        storage_type,
        Uuid::now_v7(),
        rng().random_range(0..i32::MAX),
        Url::parse("tcp://127.0.0.1/")?,
        schemas,
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
    batches: Vec<deflated::Batch>,
) -> Result<PartitionProduceResponse> {
    produce_records(storage, name, Some(Frame { batches })).await
}

/// Produces `records` as sent, where `None` is what the decoder yields for a
/// zero-length records field.
async fn produce_records(
    storage: &(impl Storage + Clone),
    name: &str,
    records: Option<Frame>,
) -> Result<PartitionProduceResponse> {
    let response = ProduceService {
        storage: storage.clone(),
    }
    .serve(RequestInput {
        request: ProduceRequest::default()
            .acks(i16::from(Ack::Leader))
            .topic_data(Some(
                [TopicProduceData::default()
                    .name(name.into())
                    .partition_data(Some(
                        [PartitionProduceData::default()
                            .index(PARTITION)
                            .records(records)]
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

/// The partition's high watermark, from ListOffsets(latest).
async fn latest(storage: &(impl Storage + Clone), name: &str) -> Result<Option<i64>> {
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
                            .partition_index(PARTITION)
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

fn well_formed(value: &'static [u8]) -> Result<deflated::Batch> {
    from_builder(inflated::Batch::builder(), value)
}

fn from_builder(builder: inflated::Builder, value: &'static [u8]) -> Result<deflated::Batch> {
    builder
        .record(Record::builder().value(Bytes::from_static(value).into()))
        .build()
        .and_then(deflated::Batch::try_from)
        .map_err(Into::into)
}

/// `batch` with its attributes and record data replaced, and its batch length
/// and CRC recomputed to match.
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

/// An uncompressed batch whose one record is cut short in its value. Decoded
/// by value this fails with `Overflow` or `TryGet`, and by reference with
/// `Io(UnexpectedEof)`.
fn truncated_record() -> Result<deflated::Batch> {
    truncated(well_formed(b"Lorem ipsum dolor sit amet")?)
}

fn truncated(batch: deflated::Batch) -> Result<deflated::Batch> {
    let record_data = batch.record_data.slice(..batch.record_data.len() - 10);

    Ok(rebuilt(
        batch,
        BatchAttribute::default().into(),
        record_data,
    ))
}

/// A batch flagged as gzip whose record data is not a gzip stream.
fn garbage_gzip() -> Result<deflated::Batch> {
    let batch = well_formed(b"Lorem ipsum dolor sit amet")?;
    let attributes = BatchAttribute::default()
        .compression(Compression::Gzip)
        .into();

    Ok(rebuilt(
        batch,
        attributes,
        Bytes::from_static(b"this is not a gzip stream"),
    ))
}

/// A batch whose codec bits (the low three of the attributes) hold 7, which
/// no Kafka codec uses.
fn unknown_codec() -> Result<deflated::Batch> {
    let batch = well_formed(b"Lorem ipsum dolor sit amet")?;
    let attributes = i16::from(BatchAttribute::default()) | 0b111;
    let record_data = batch.record_data.clone();

    Ok(rebuilt(batch, attributes, record_data))
}

fn assert_invalid_record(response: &PartitionProduceResponse) -> Result<()> {
    assert_eq!(
        ErrorCode::InvalidRecord,
        ErrorCode::try_from(response.error_code)?,
        "{response:?}"
    );
    assert_eq!(-1, response.base_offset);

    Ok(())
}

/// A later well-formed produce lands at offset 0 and moves the high watermark
/// to 1, so the rejected produces before it wrote nothing and consumed no
/// offset. On Postgres and libSQL a leftover row would also make this insert
/// collide with it.
async fn assert_nothing_written(storage: &(impl Storage + Clone), name: &str) -> Result<()> {
    assert_eq!(Some(0), latest(storage, name).await?);

    let response = produce(storage, name, vec![well_formed(b"well formed")?]).await?;
    assert_eq!(ErrorCode::None, ErrorCode::try_from(response.error_code)?);
    assert_eq!(0, response.base_offset);

    assert_eq!(Some(1), latest(storage, name).await?);

    Ok(())
}

/// Records that do not decode, whether uncompressed and cut short or behind a
/// corrupt codec stream. Only runs against configurations that decode a
/// produced batch: dynostore and slatedb store it without decoding unless a
/// schema registry (or lake) is configured.
async fn undecodable_records(storage: impl Storage + Clone) -> Result<()> {
    let name = &alphanumeric_string(15)[..];
    create_topic(&storage, name).await?;

    assert_invalid_record(&produce(&storage, name, vec![truncated_record()?]).await?)?;
    assert_invalid_record(&produce(&storage, name, vec![garbage_gzip()?]).await?)?;

    assert_nothing_written(&storage, name).await
}

/// An unknown codec id is caught before storage, so on every configuration.
async fn unknown_compression_codec(storage: impl Storage + Clone) -> Result<()> {
    let name = &alphanumeric_string(15)[..];
    create_topic(&storage, name).await?;

    let response = produce(&storage, name, vec![unknown_codec()?]).await?;
    assert_invalid_record(&response)?;
    assert_eq!(
        Some("unknown compression codec"),
        response.error_message.as_deref()
    );

    assert_nothing_written(&storage, name).await
}

/// Kafka allows exactly one batch per partition. Each batch is stored on its
/// own, so a second batch that failed after the first was stored would report
/// the partition as failed while the first batch stayed in the log. The
/// request is rejected before either is stored, including when both batches
/// are well formed. A zero-length records field, which decodes to `None`, is
/// rejected the same way.
///
/// The count is checked before any batch is validated, as Kafka does. The
/// case with an unknown codec in the second batch pins that order: its
/// message would name the codec if the batches were validated first.
async fn not_exactly_one_batch(storage: impl Storage + Clone) -> Result<()> {
    let name = &alphanumeric_string(15)[..];
    create_topic(&storage, name).await?;

    for records in [
        Some(vec![well_formed(b"first")?, truncated_record()?]),
        Some(vec![well_formed(b"first")?, unknown_codec()?]),
        Some(vec![well_formed(b"first")?, well_formed(b"second")?]),
        Some(vec![]),
        None,
    ] {
        let response =
            produce_records(&storage, name, records.map(|batches| Frame { batches })).await?;
        assert_invalid_record(&response)?;
        assert_eq!(
            Some(NOT_EXACTLY_ONE_BATCH),
            response.error_message.as_deref()
        );
    }

    assert_nothing_written(&storage, name).await
}

/// An idempotent batch that does not decode leaves the producer's sequence
/// where it was. The producer can then send a well-formed batch with the same
/// sequence, and that batch is stored. If the rejected batch had moved the
/// sequence, the well-formed batch would get `DUPLICATE_SEQUENCE_NUMBER`, which
/// a client reports as success.
async fn rejected_idempotent_batch_keeps_its_sequence(storage: impl Storage + Clone) -> Result<()> {
    let name = &alphanumeric_string(15)[..];
    create_topic(&storage, name).await?;

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

    let idempotent = |value| {
        from_builder(
            inflated::Batch::builder()
                .producer_id(producer.producer_id)
                .producer_epoch(producer.producer_epoch)
                .base_sequence(0),
            value,
        )
    };

    assert_invalid_record(
        &produce(&storage, name, vec![truncated(idempotent(b"rejected")?)?]).await?,
    )?;

    let response = produce(&storage, name, vec![idempotent(b"well formed")?]).await?;
    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(response.error_code)?,
        "{response:?}"
    );
    assert_eq!(0, response.base_offset);

    assert_eq!(Some(1), latest(&storage, name).await?);

    Ok(())
}

#[cfg(feature = "dynostore")]
mod in_memory {
    use super::*;

    #[tokio::test]
    async fn unknown_compression_codec() -> Result<()> {
        let _guard = init_tracing()?;
        super::unknown_compression_codec(storage(StorageType::InMemory, false).await?).await
    }

    #[tokio::test]
    async fn not_exactly_one_batch() -> Result<()> {
        let _guard = init_tracing()?;
        super::not_exactly_one_batch(storage(StorageType::InMemory, false).await?).await
    }

    #[tokio::test]
    async fn undecodable_records() -> Result<()> {
        let _guard = init_tracing()?;
        super::undecodable_records(storage(StorageType::InMemory, false).await?).await
    }

    /// With a registry, dynostore decodes by reference, through a different
    /// decoder than the by-value path the SQL backends use.
    #[tokio::test]
    async fn undecodable_records_with_registry() -> Result<()> {
        let _guard = init_tracing()?;
        super::undecodable_records(storage(StorageType::InMemory, true).await?).await
    }

    #[tokio::test]
    async fn not_exactly_one_batch_with_registry() -> Result<()> {
        let _guard = init_tracing()?;
        super::not_exactly_one_batch(storage(StorageType::InMemory, true).await?).await
    }

    #[tokio::test]
    async fn rejected_idempotent_batch_keeps_its_sequence_with_registry() -> Result<()> {
        let _guard = init_tracing()?;
        rejected_idempotent_batch_keeps_its_sequence(storage(StorageType::InMemory, true).await?)
            .await
    }
}

#[cfg(feature = "libsql")]
mod lite {
    use super::*;

    #[tokio::test]
    async fn undecodable_records() -> Result<()> {
        let _guard = init_tracing()?;
        super::undecodable_records(storage(StorageType::Lite, false).await?).await
    }

    #[tokio::test]
    async fn unknown_compression_codec() -> Result<()> {
        let _guard = init_tracing()?;
        super::unknown_compression_codec(storage(StorageType::Lite, false).await?).await
    }

    #[tokio::test]
    async fn not_exactly_one_batch() -> Result<()> {
        let _guard = init_tracing()?;
        super::not_exactly_one_batch(storage(StorageType::Lite, false).await?).await
    }

    #[tokio::test]
    async fn rejected_idempotent_batch_keeps_its_sequence() -> Result<()> {
        let _guard = init_tracing()?;
        super::rejected_idempotent_batch_keeps_its_sequence(
            storage(StorageType::Lite, false).await?,
        )
        .await
    }
}

#[cfg(feature = "postgres")]
mod pg {
    use super::*;

    #[tokio::test]
    async fn undecodable_records() -> Result<()> {
        let _guard = init_tracing()?;
        super::undecodable_records(storage(StorageType::Postgres, false).await?).await
    }

    #[tokio::test]
    async fn unknown_compression_codec() -> Result<()> {
        let _guard = init_tracing()?;
        super::unknown_compression_codec(storage(StorageType::Postgres, false).await?).await
    }

    #[tokio::test]
    async fn not_exactly_one_batch() -> Result<()> {
        let _guard = init_tracing()?;
        super::not_exactly_one_batch(storage(StorageType::Postgres, false).await?).await
    }
}

#[cfg(feature = "slatedb")]
mod slate {
    use super::*;

    #[tokio::test]
    async fn unknown_compression_codec() -> Result<()> {
        let _guard = init_tracing()?;
        super::unknown_compression_codec(storage(StorageType::SlateDb, false).await?).await
    }

    #[tokio::test]
    async fn not_exactly_one_batch() -> Result<()> {
        let _guard = init_tracing()?;
        super::not_exactly_one_batch(storage(StorageType::SlateDb, false).await?).await
    }

    #[tokio::test]
    async fn undecodable_records() -> Result<()> {
        let _guard = init_tracing()?;
        super::undecodable_records(storage(StorageType::SlateDb, false).await?).await
    }

    #[tokio::test]
    async fn undecodable_records_with_registry() -> Result<()> {
        let _guard = init_tracing()?;
        super::undecodable_records(storage(StorageType::SlateDb, true).await?).await
    }

    #[tokio::test]
    async fn rejected_idempotent_batch_keeps_its_sequence_with_registry() -> Result<()> {
        let _guard = init_tracing()?;
        rejected_idempotent_batch_keeps_its_sequence(storage(StorageType::SlateDb, true).await?)
            .await
    }
}

#[cfg(feature = "turso")]
mod turso {
    use super::*;

    #[tokio::test]
    async fn undecodable_records() -> Result<()> {
        let _guard = init_tracing()?;
        super::undecodable_records(storage(StorageType::Turso, false).await?).await
    }

    #[tokio::test]
    async fn unknown_compression_codec() -> Result<()> {
        let _guard = init_tracing()?;
        super::unknown_compression_codec(storage(StorageType::Turso, false).await?).await
    }

    #[tokio::test]
    async fn not_exactly_one_batch() -> Result<()> {
        let _guard = init_tracing()?;
        super::not_exactly_one_batch(storage(StorageType::Turso, false).await?).await
    }
}

/// A zero-length records field over a real connection. The check runs in
/// `ProduceService` before any backend is touched, so only the memory backend
/// is used.
mod wire {
    use std::time::Duration;

    use anyhow::anyhow;
    use nisshi_sans_io::{ApiKey as _, CreateTopicsResponse, ProduceResponse};
    use tokio::{net::TcpStream, time::timeout};

    use super::*;
    use crate::common::wire::{round_trip, spawn_broker};

    const TEST_TIMEOUT: Duration = Duration::from_secs(30);

    async fn create_topic(stream: &mut TcpStream, name: &str) -> anyhow::Result<()> {
        let frame = round_trip(
            stream,
            CreateTopicsRequest::KEY,
            7,
            1,
            CreateTopicsRequest::default()
                .validate_only(Some(false))
                .topics(Some(
                    [CreatableTopic::default()
                        .name(name.into())
                        .num_partitions(1)
                        .replication_factor(1)
                        .assignments(Some([].into()))
                        .configs(Some([].into()))]
                    .into(),
                ))
                .into(),
        )
        .await?;

        let response = CreateTopicsResponse::try_from(frame.body)?;
        let topics = response.topics.unwrap_or_default();
        assert_eq!(1, topics.len());
        assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);

        Ok(())
    }

    /// Sends a Produce v9 for `records` and returns the partition's response.
    /// The encoder writes `None` as a zero-length records field.
    async fn produce(
        stream: &mut TcpStream,
        correlation_id: i32,
        name: &str,
        records: Option<Frame>,
    ) -> anyhow::Result<PartitionProduceResponse> {
        let frame = round_trip(
            stream,
            ProduceRequest::KEY,
            9,
            correlation_id,
            ProduceRequest::default()
                .acks(i16::from(Ack::Leader))
                .timeout_ms(5_000)
                .topic_data(Some(
                    [TopicProduceData::default()
                        .name(name.into())
                        .partition_data(Some(
                            [PartitionProduceData::default()
                                .index(PARTITION)
                                .records(records)]
                            .into(),
                        ))]
                    .into(),
                ))
                .into(),
        )
        .await?;

        let response = ProduceResponse::try_from(frame.body)?;
        let topics = response.responses.unwrap_or_default();
        assert_eq!(1, topics.len());

        let mut partitions = topics[0].partition_responses.clone().unwrap_or_default();
        assert_eq!(1, partitions.len());

        Ok(partitions.remove(0))
    }

    /// The partition gets `INVALID_RECORD`, and the connection then carries a
    /// well-formed produce, which lands at offset 0.
    #[tokio::test]
    async fn zero_length_records_field_is_an_invalid_record() -> anyhow::Result<()> {
        let _guard = init_tracing()?;

        timeout(TEST_TIMEOUT, async {
            let broker = spawn_broker(None).await?;
            let mut stream = TcpStream::connect(broker.addr).await?;

            let name = &alphanumeric_string(15)[..];
            create_topic(&mut stream, name).await?;

            let response = produce(&mut stream, 2, name, None).await?;
            assert_invalid_record(&response)?;
            assert_eq!(
                Some(NOT_EXACTLY_ONE_BATCH),
                response.error_message.as_deref()
            );

            let batches = vec![well_formed(b"well formed")?];
            let response = produce(&mut stream, 3, name, Some(Frame { batches })).await?;
            assert_eq!(ErrorCode::None, ErrorCode::try_from(response.error_code)?);
            assert_eq!(0, response.base_offset);

            Ok::<_, anyhow::Error>(())
        })
        .await
        .map_err(|_| anyhow!("timed out after {TEST_TIMEOUT:?}"))?
    }
}
