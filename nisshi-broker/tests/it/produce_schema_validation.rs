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

//! [`ProduceService`](nisshi_storage::ProduceService) validates each batch against the topic's
//! schema, on every storage engine, because the engines do not validate it themselves.

use bytes::Bytes;
use nisshi_broker::{Error, Result};
use nisshi_sans_io::{
    ErrorCode,
    create_topics_request::{CreatableTopic, CreatableTopicConfig},
    record::{Record, deflated, inflated},
    topic_config,
};
use nisshi_schema::Registry;
use nisshi_storage::{ArcDynStorage, Storage, max_message_bytes};
use rand::{prelude::*, rng};
use serde_json::json;
use url::Url;
use uuid::Uuid;

use crate::{
    common::{self, StorageType, register_broker},
    produce::produce_batches_with_schemas,
};

const TOPIC: &str = "person";

fn person_schemas() -> Result<Registry> {
    Url::parse("file://../etc/schema")
        .map_err(Error::from)
        .and_then(|url| {
            Registry::builder_try_from_url(&url)
                .map(|builder| builder.build())
                .map_err(Into::into)
        })
}

fn person_batch(age: i64) -> Result<deflated::Batch> {
    inflated::Batch::builder()
        .record(
            Record::builder()
                .key(Some(Bytes::from_static(b"\"345-67-6543\"")))
                .value(Some(Bytes::from(serde_json::to_vec(&json!({
                    "firstName": "John",
                    "lastName": "Doe",
                    "age": age,
                }))?))),
        )
        .build()
        .and_then(TryInto::try_into)
        .map_err(Into::into)
}

/// Creates the `person` topic with `configs` in a storage engine of `storage_type`, which holds
/// the person schema as well, and produces a person of `age` through a service that has
/// `schemas`. Returns the error code of the partition's response.
async fn produce_person(
    storage_type: StorageType,
    configs: Vec<CreatableTopicConfig>,
    schemas: Option<Registry>,
    age: i64,
) -> Result<ErrorCode> {
    produce_person_to_partition(storage_type, configs, schemas, 0, age).await
}

/// [`produce_person`] to `partition`, which the topic has only when it is 0.
async fn produce_person_to_partition(
    storage_type: StorageType,
    configs: Vec<CreatableTopicConfig>,
    schemas: Option<Registry>,
    partition: i32,
    age: i64,
) -> Result<ErrorCode> {
    let cluster = Uuid::now_v7();
    let node = rng().random_range(0..i32::MAX);

    let storage: ArcDynStorage = common::storage_container(
        storage_type,
        cluster,
        node,
        Url::parse("tcp://127.0.0.1/")?,
        Some(person_schemas()?),
    )
    .await?;
    register_broker(cluster, node, &storage).await?;

    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(TOPIC.into())
                .num_partitions(1)
                .replication_factor(0)
                .assignments(Some([].into()))
                .configs(Some(configs)),
            false,
        )
        .await?;

    let response =
        produce_batches_with_schemas(storage, schemas, TOPIC, partition, vec![person_batch(age)?])
            .await?;

    ErrorCode::try_from(response.error_code).map_err(Into::into)
}

fn schema_validation(value: &str) -> CreatableTopicConfig {
    CreatableTopicConfig::default()
        .name(topic_config::SCHEMA_VALIDATION.into())
        .value(Some(value.into()))
}

/// A `max.message.bytes` that is smaller than a batch of one person.
fn small_max_message_bytes() -> CreatableTopicConfig {
    CreatableTopicConfig::default()
        .name(max_message_bytes::KEY.into())
        .value(Some("100".into()))
}

macro_rules! engine {
    ($module:ident, $feature:literal, $storage_type:expr) => {
        #[cfg(feature = $feature)]
        mod $module {
            use super::*;

            #[tokio::test]
            async fn a_record_that_matches_the_schema_is_stored() -> Result<()> {
                assert_eq!(
                    ErrorCode::None,
                    produce_person($storage_type, vec![], Some(person_schemas()?), 21).await?
                );
                Ok(())
            }

            #[tokio::test]
            async fn a_record_that_breaks_the_schema_is_an_invalid_record() -> Result<()> {
                assert_eq!(
                    ErrorCode::InvalidRecord,
                    produce_person($storage_type, vec![], Some(person_schemas()?), -1).await?
                );
                Ok(())
            }

            #[tokio::test]
            async fn schema_validation_false_stores_a_record_that_breaks_the_schema() -> Result<()> {
                assert_eq!(
                    ErrorCode::None,
                    produce_person(
                        $storage_type,
                        vec![schema_validation("false")],
                        Some(person_schemas()?),
                        -1
                    )
                    .await?
                );
                Ok(())
            }

            /// Kafka checks the size of a batch before it validates a record, and the Java
            /// producer splits a batch that is too large, so the valid records land.
            #[tokio::test]
            async fn a_batch_over_max_message_bytes_that_breaks_the_schema_is_message_too_large()
            -> Result<()> {
                assert_eq!(
                    ErrorCode::MessageTooLarge,
                    produce_person(
                        $storage_type,
                        vec![small_max_message_bytes()],
                        Some(person_schemas()?),
                        -1
                    )
                    .await?
                );
                Ok(())
            }

            /// Schema validation runs before the storage engine looks for the partition, so a
            /// batch that breaks the schema does not reach the partition check.
            #[tokio::test]
            async fn a_batch_that_breaks_the_schema_is_an_invalid_record_for_an_unknown_partition()
            -> Result<()> {
                assert_eq!(
                    ErrorCode::InvalidRecord,
                    produce_person_to_partition(
                        $storage_type,
                        vec![],
                        Some(person_schemas()?),
                        7,
                        -1
                    )
                    .await?
                );
                Ok(())
            }

            #[tokio::test]
            async fn a_service_without_schemas_stores_a_record_that_breaks_the_schema() -> Result<()>
            {
                assert_eq!(
                    ErrorCode::None,
                    produce_person($storage_type, vec![], None, -1).await?
                );
                Ok(())
            }
        }
    };
}

engine!(in_memory, "dynostore", StorageType::InMemory);
engine!(lite, "libsql", StorageType::Lite);
engine!(pg, "postgres", StorageType::Postgres);

/// The broker hands the registry that it is built with to its Produce handler.
#[cfg(feature = "dynostore")]
mod broker {
    use anyhow::{Result, anyhow};
    use nisshi_sans_io::{
        ApiKey as _, CreateTopicsRequest, CreateTopicsResponse, ErrorCode, ProduceRequest,
        ProduceResponse,
        create_topics_request::CreatableTopic,
        produce_request::{PartitionProduceData, TopicProduceData},
        record::deflated::Frame,
    };
    use tokio::net::TcpStream;

    use super::{TOPIC, person_batch, person_schemas};
    use crate::common::wire::{round_trip, spawn_broker_with_schemas};

    /// Creates the `person` topic on `stream`'s broker, and produces one person of `age` to it.
    /// Returns the error code of the partition's response.
    async fn produce_person(schemas: bool, age: i64) -> Result<ErrorCode> {
        let schemas = if schemas {
            Some(person_schemas().map_err(|err| anyhow!("{err:?}"))?)
        } else {
            None
        };

        let broker = spawn_broker_with_schemas(schemas).await?;
        let mut stream = TcpStream::connect(broker.addr).await?;

        let created = CreateTopicsResponse::try_from(
            round_trip(
                &mut stream,
                CreateTopicsRequest::KEY,
                7,
                1,
                CreateTopicsRequest::default()
                    .validate_only(Some(false))
                    .topics(Some(
                        [CreatableTopic::default()
                            .name(TOPIC.into())
                            .num_partitions(1)
                            .replication_factor(1)
                            .assignments(Some([].into()))
                            .configs(Some([].into()))]
                        .into(),
                    ))
                    .into(),
            )
            .await?
            .body,
        )?;
        assert_eq!(
            Some(i16::from(ErrorCode::None)),
            created
                .topics
                .unwrap_or_default()
                .first()
                .map(|topic| topic.error_code)
        );

        let produced = ProduceResponse::try_from(
            round_trip(
                &mut stream,
                ProduceRequest::KEY,
                9,
                2,
                ProduceRequest::default()
                    .acks(1)
                    .timeout_ms(5_000)
                    .topic_data(Some(
                        [TopicProduceData::default()
                            .name(TOPIC.into())
                            .partition_data(Some(
                                [PartitionProduceData::default()
                                    .index(0)
                                    .records(Some(Frame {
                                        batches: vec![
                                            person_batch(age).map_err(|err| anyhow!("{err:?}"))?,
                                        ],
                                    }))]
                                .into(),
                            ))]
                        .into(),
                    ))
                    .into(),
            )
            .await?
            .body,
        )?;

        let partition = produced
            .responses
            .unwrap_or_default()
            .first()
            .and_then(|topic| topic.partition_responses.as_deref())
            .and_then(|partitions| partitions.first())
            .map(|partition| partition.error_code)
            .ok_or_else(|| anyhow!("the response has no partition"))?;

        ErrorCode::try_from(partition).map_err(Into::into)
    }

    #[tokio::test]
    async fn a_broker_with_a_registry_rejects_a_record_that_breaks_the_schema() -> Result<()> {
        assert_eq!(ErrorCode::InvalidRecord, produce_person(true, -1).await?);
        Ok(())
    }

    #[tokio::test]
    async fn a_broker_with_a_registry_stores_a_record_that_matches_the_schema() -> Result<()> {
        assert_eq!(ErrorCode::None, produce_person(true, 21).await?);
        Ok(())
    }

    /// The control for the test above: a broker that has no registry stores the same record, so
    /// the rejection there comes from the registry.
    #[tokio::test]
    async fn a_broker_without_a_registry_stores_a_record_that_breaks_the_schema() -> Result<()> {
        assert_eq!(ErrorCode::None, produce_person(false, -1).await?);
        Ok(())
    }
}
