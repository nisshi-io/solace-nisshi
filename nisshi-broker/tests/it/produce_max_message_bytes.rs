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

//! Produce rejects a batch larger than its topic's `max.message.bytes` with `MESSAGE_TOO_LARGE`.
//! Kafka does the same.

use crate::common::{alphanumeric_string, init_tracing};

use bytes::Bytes;
use nisshi_broker::Result;
use nisshi_sans_io::{
    ByteSize as _, ConfigResource, CreateTopicsRequest, ErrorCode, IncrementalAlterConfigsRequest,
    InitProducerIdRequest, OpType, ProduceRequest, RequestInput,
    create_topics_request::{CreatableTopic, CreatableTopicConfig},
    create_topics_response::CreatableTopicResult,
    incremental_alter_configs_request::{AlterConfigsResource, AlterableConfig},
    incremental_alter_configs_response::AlterConfigsResourceResponse,
    produce_request::{PartitionProduceData, TopicProduceData},
    produce_response::PartitionProduceResponse,
    record::{
        Record,
        deflated::{self, Frame},
        inflated,
    },
};
use nisshi_storage::{
    CreateTopicsService, CreateTopicsValidationLayer, IncrementalAlterConfigsService,
    InitProducerIdService, ProduceService, Storage, max_message_bytes,
};
use rama::{Layer as _, Service as _, extensions::Extensions};
use rand::{RngExt as _, rng};
use uuid::Uuid;

const MAX_MESSAGE_BYTES: usize = 10_000;

/// Creates a topic named `topic` with `configs` through CreateTopics, and returns its result.
pub(crate) async fn create_topic_result(
    storage: impl Storage + Clone,
    topic: &str,
    configs: &[(&str, &str)],
) -> Result<CreatableTopicResult> {
    let response = CreateTopicsValidationLayer::new()
        .layer(CreateTopicsService { storage })
        .serve(RequestInput {
            request: CreateTopicsRequest::default()
                .validate_only(Some(false))
                .topics(Some(
                    [CreatableTopic::default()
                        .name(topic.into())
                        .num_partitions(1)
                        .replication_factor(1)
                        .assignments(Some([].into()))
                        .configs(Some(
                            configs
                                .iter()
                                .map(|(name, value)| {
                                    CreatableTopicConfig::default()
                                        .name((*name).into())
                                        .value(Some((*value).into()))
                                })
                                .collect(),
                        ))]
                    .into(),
                )),
            extensions: Extensions::default(),
        })
        .await?;

    let mut topics = response.topics.unwrap_or_default();
    assert_eq!(1, topics.len());
    Ok(topics.remove(0))
}

async fn create_topic(storage: impl Storage + Clone, configs: &[(&str, &str)]) -> Result<String> {
    let topic = alphanumeric_string(15);

    let result = create_topic_result(storage, &topic, configs).await?;
    assert_eq!(ErrorCode::None, ErrorCode::try_from(result.error_code)?);

    Ok(topic)
}

/// Returns an IncrementalAlterConfigs resource that sets the topic's `max.message.bytes`.
fn max_message_bytes_resource(topic: &str, configured: &str) -> AlterConfigsResource {
    AlterConfigsResource::default()
        .resource_type(ConfigResource::Topic.into())
        .resource_name(topic.into())
        .configs(Some(vec![
            AlterableConfig::default()
                .name(max_message_bytes::KEY.into())
                .config_operation(OpType::Set.into())
                .value(Some(configured.into())),
        ]))
}

/// Sets the topic's `max.message.bytes` to `configured` through IncrementalAlterConfigs, and
/// returns its result.
async fn alter_max_message_bytes(
    storage: impl Storage + Clone,
    topic: &str,
    configured: &str,
) -> Result<AlterConfigsResourceResponse> {
    let response = IncrementalAlterConfigsService { storage }
        .serve(RequestInput {
            request: IncrementalAlterConfigsRequest::default()
                .resources(Some(vec![max_message_bytes_resource(topic, configured)]))
                .validate_only(false),
            extensions: Extensions::default(),
        })
        .await?;

    let mut resources = response.responses.unwrap_or_default();
    assert_eq!(1, resources.len());
    Ok(resources.remove(0))
}

async fn produce(
    storage: impl Storage + Clone,
    topic: &str,
    batch: deflated::Batch,
) -> Result<PartitionProduceResponse> {
    let response = ProduceService::new(storage)
        .with_validation()
        .serve(RequestInput {
            request: ProduceRequest::default().acks(-1).topic_data(Some(vec![
                TopicProduceData::default()
                    .name(topic.into())
                    .partition_data(Some(vec![
                        PartitionProduceData::default()
                            .index(0)
                            .records(Some(Frame {
                                batches: vec![batch],
                            })),
                    ])),
            ])),
            extensions: Extensions::default(),
        })
        .await?;

    let mut topics = response.responses.unwrap_or_default();
    assert_eq!(1, topics.len());
    let mut partitions = topics.remove(0).partition_responses.unwrap_or_default();
    assert_eq!(1, partitions.len());
    Ok(partitions.remove(0))
}

/// Returns a batch from `producer` with one record of `value_len` bytes.
fn batch_with_value(value_len: usize, producer: inflated::Builder) -> Result<deflated::Batch> {
    producer
        .record(Record::builder().value(Bytes::from(vec![b'a'; value_len]).into()))
        .build()
        .and_then(deflated::Batch::try_from)
        .map_err(Into::into)
}

#[track_caller]
fn assert_message_too_large(response: &PartitionProduceResponse) -> Result<()> {
    assert_eq!(
        ErrorCode::MessageTooLarge,
        ErrorCode::try_from(response.error_code)?,
        "{response:?}"
    );
    Ok(())
}

#[track_caller]
fn assert_stored_at(base_offset: i64, response: &PartitionProduceResponse) -> Result<()> {
    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(response.error_code)?,
        "{response:?}"
    );
    assert_eq!(base_offset, response.base_offset, "{response:?}");
    Ok(())
}

/// The response carries Kafka's error message. The next batch is stored at offset 0, so the
/// rejected batch was not stored.
async fn batch_over_the_limit_is_message_too_large(storage: impl Storage + Clone) -> Result<()> {
    let topic = create_topic(
        storage.clone(),
        &[(max_message_bytes::KEY, &MAX_MESSAGE_BYTES.to_string())],
    )
    .await?;

    let oversized = batch_with_value(2 * MAX_MESSAGE_BYTES, inflated::Batch::builder())?;
    let size = oversized.size_in_bytes()?;

    let response = produce(storage.clone(), &topic, oversized).await?;
    assert_message_too_large(&response)?;
    assert_eq!(
        Some(format!(
            "The record batch size in the append to {topic}-0 is {size} bytes which exceeds the \
             maximum configured value of {MAX_MESSAGE_BYTES}."
        )),
        response.error_message
    );

    let response = produce(
        storage,
        &topic,
        batch_with_value(1, inflated::Batch::builder())?,
    )
    .await?;
    assert_stored_at(0, &response)
}

/// Kafka rejects only a batch larger than the limit. The test sets the limits from the batch's
/// measured size: one byte below it, and equal to it.
async fn batch_of_exactly_the_limit_is_stored(storage: impl Storage + Clone) -> Result<()> {
    let batch = batch_with_value(MAX_MESSAGE_BYTES, inflated::Batch::builder())?;
    let size = batch.size_in_bytes()?;

    let topic_with_limit_below_batch = create_topic(
        storage.clone(),
        &[(max_message_bytes::KEY, &(size - 1).to_string())],
    )
    .await?;
    let response = produce(
        storage.clone(),
        &topic_with_limit_below_batch,
        batch.clone(),
    )
    .await?;
    assert_message_too_large(&response)?;

    let topic_with_limit_at_batch = create_topic(
        storage.clone(),
        &[(max_message_bytes::KEY, &size.to_string())],
    )
    .await?;
    let response = produce(storage, &topic_with_limit_at_batch, batch).await?;
    assert_stored_at(0, &response)
}

/// The default applies when a topic does not set `max.message.bytes`, and when its stored value is
/// invalid.
async fn default_limit_applies(storage: impl Storage + Clone) -> Result<()> {
    for stored in [None, Some("abc")] {
        let topic = create_topic(storage.clone(), &[]).await?;

        // CreateTopics and IncrementalAlterConfigs reject "abc", so the test writes it to storage
        // directly.
        if let Some(stored) = stored {
            let altered = storage
                .incremental_alter_resource(max_message_bytes_resource(&topic, stored))
                .await?;
            assert_eq!(ErrorCode::None, ErrorCode::try_from(altered.error_code)?);
        }

        let response = produce(
            storage.clone(),
            &topic,
            batch_with_value(2 * max_message_bytes::DEFAULT, inflated::Batch::builder())?,
        )
        .await?;
        assert_message_too_large(&response)?;

        let response = produce(
            storage.clone(),
            &topic,
            batch_with_value(max_message_bytes::DEFAULT / 2, inflated::Batch::builder())?,
        )
        .await?;
        assert_stored_at(0, &response)?;
    }

    Ok(())
}

/// Produce reads the limit for every batch, so a raised limit applies to the next batch.
async fn altered_limit_applies_to_the_next_produce(storage: impl Storage + Clone) -> Result<()> {
    let topic = create_topic(
        storage.clone(),
        &[(max_message_bytes::KEY, &MAX_MESSAGE_BYTES.to_string())],
    )
    .await?;

    let oversized = || batch_with_value(2 * MAX_MESSAGE_BYTES, inflated::Batch::builder());

    let response = produce(storage.clone(), &topic, oversized()?).await?;
    assert_message_too_large(&response)?;

    let altered = alter_max_message_bytes(
        storage.clone(),
        &topic,
        &(4 * MAX_MESSAGE_BYTES).to_string(),
    )
    .await?;
    assert_eq!(ErrorCode::None, ErrorCode::try_from(altered.error_code)?);

    let response = produce(storage, &topic, oversized()?).await?;
    assert_stored_at(0, &response)
}

/// The response carries Kafka's error message, and the topic is not created.
async fn create_topics_rejects_an_invalid_limit(storage: impl Storage + Clone) -> Result<()> {
    let topic = alphanumeric_string(15);

    let result =
        create_topic_result(storage.clone(), &topic, &[(max_message_bytes::KEY, "1MB")]).await?;
    assert_eq!(
        ErrorCode::InvalidConfig,
        ErrorCode::try_from(result.error_code)?
    );
    assert_eq!(
        Some("Invalid value 1MB for configuration max.message.bytes: Not a number of type INT"),
        result.error_message.as_deref()
    );

    let result = create_topic_result(storage, &topic, &[]).await?;
    assert_eq!(ErrorCode::None, ErrorCode::try_from(result.error_code)?);

    Ok(())
}

/// The topic keeps its previous limit.
async fn incremental_alter_configs_rejects_an_invalid_limit(
    storage: impl Storage + Clone,
) -> Result<()> {
    let topic = create_topic(
        storage.clone(),
        &[(max_message_bytes::KEY, &MAX_MESSAGE_BYTES.to_string())],
    )
    .await?;

    let altered = alter_max_message_bytes(storage.clone(), &topic, "-1").await?;
    assert_eq!(
        ErrorCode::InvalidConfig,
        ErrorCode::try_from(altered.error_code)?
    );
    assert_eq!(
        Some("Invalid value -1 for configuration max.message.bytes: Value must be at least 0"),
        altered.error_message.as_deref()
    );

    let response = produce(
        storage,
        &topic,
        batch_with_value(2 * MAX_MESSAGE_BYTES, inflated::Batch::builder())?,
    )
    .await?;
    assert_message_too_large(&response)
}

/// A rejected batch does not advance the producer's sequence, so the next batch with that sequence
/// is stored. Otherwise the client would get `DUPLICATE_SEQUENCE_NUMBER`, which it reports as
/// success.
async fn rejected_idempotent_batch_keeps_its_sequence(storage: impl Storage + Clone) -> Result<()> {
    let topic = create_topic(
        storage.clone(),
        &[(max_message_bytes::KEY, &MAX_MESSAGE_BYTES.to_string())],
    )
    .await?;

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
        inflated::Batch::builder()
            .producer_id(producer.producer_id)
            .producer_epoch(producer.producer_epoch)
            .base_sequence(0)
    };

    let response = produce(
        storage.clone(),
        &topic,
        batch_with_value(2 * MAX_MESSAGE_BYTES, idempotent())?,
    )
    .await?;
    assert_message_too_large(&response)?;

    let response = produce(storage, &topic, batch_with_value(1, idempotent())?).await?;
    assert_stored_at(0, &response)
}

/// Kafka checks that the partition exists before it checks the batch's size.
async fn unknown_topic_is_reported_before_the_size(storage: impl Storage + Clone) -> Result<()> {
    let response = produce(
        storage,
        &alphanumeric_string(15),
        batch_with_value(2 * max_message_bytes::DEFAULT, inflated::Batch::builder())?,
    )
    .await?;

    assert_eq!(
        ErrorCode::UnknownTopicOrPartition,
        ErrorCode::try_from(response.error_code)?,
        "{response:?}"
    );
    Ok(())
}

macro_rules! engine_tests {
    ($engine:ident, $feature:literal, $storage:path $(, #[$ignore:meta])?) => {
        #[cfg(feature = $feature)]
        mod $engine {
            use super::*;
            use $storage as engine_storage;

            async fn storage() -> Result<nisshi_storage::ArcDynStorage> {
                engine_storage(Uuid::now_v7(), rng().random_range(0..i32::MAX)).await
            }

            $(#[$ignore])?
            #[tokio::test]
            async fn batch_over_the_limit_is_message_too_large() -> Result<()> {
                let _guard = init_tracing()?;
                super::batch_over_the_limit_is_message_too_large(storage().await?).await
            }

            $(#[$ignore])?
            #[tokio::test]
            async fn batch_of_exactly_the_limit_is_stored() -> Result<()> {
                let _guard = init_tracing()?;
                super::batch_of_exactly_the_limit_is_stored(storage().await?).await
            }

            $(#[$ignore])?
            #[tokio::test]
            async fn default_limit_applies() -> Result<()> {
                let _guard = init_tracing()?;
                super::default_limit_applies(storage().await?).await
            }

            $(#[$ignore])?
            #[tokio::test]
            async fn altered_limit_applies_to_the_next_produce() -> Result<()> {
                let _guard = init_tracing()?;
                super::altered_limit_applies_to_the_next_produce(storage().await?).await
            }

            $(#[$ignore])?
            #[tokio::test]
            async fn create_topics_rejects_an_invalid_limit() -> Result<()> {
                let _guard = init_tracing()?;
                super::create_topics_rejects_an_invalid_limit(storage().await?).await
            }

            $(#[$ignore])?
            #[tokio::test]
            async fn incremental_alter_configs_rejects_an_invalid_limit() -> Result<()> {
                let _guard = init_tracing()?;
                super::incremental_alter_configs_rejects_an_invalid_limit(storage().await?).await
            }

            $(#[$ignore])?
            #[tokio::test]
            async fn rejected_idempotent_batch_keeps_its_sequence() -> Result<()> {
                let _guard = init_tracing()?;
                super::rejected_idempotent_batch_keeps_its_sequence(storage().await?).await
            }

            $(#[$ignore])?
            #[tokio::test]
            async fn unknown_topic_is_reported_before_the_size() -> Result<()> {
                let _guard = init_tracing()?;
                super::unknown_topic_is_reported_before_the_size(storage().await?).await
            }
        }
    };
}

#[cfg(feature = "turso")]
async fn turso_storage(cluster: Uuid, node: i32) -> Result<nisshi_storage::ArcDynStorage> {
    use crate::common::{StorageType, storage_container};

    storage_container(
        StorageType::Turso,
        cluster,
        node,
        url::Url::parse("tcp://127.0.0.1/")?,
        None,
    )
    .await
}

engine_tests!(in_memory, "dynostore", crate::common::memory_storage);
engine_tests!(lite, "libsql", crate::common::lite_storage);
engine_tests!(pg, "postgres", crate::common::postgres_storage);
engine_tests!(slate, "slatedb", crate::common::slate_storage);
engine_tests!(
    turso,
    "turso",
    super::turso_storage,
    #[ignore = "the turso backend fails its connection check in tests"]
);
