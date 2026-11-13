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

//! DescribeConfigs for a broker logger resource compares the resource's name with the broker's
//! node id, as Kafka does ([`ConfigHelper.scala#L122-L131`]), on every storage engine.
//!
//! [`ConfigHelper.scala#L122-L131`]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/ConfigHelper.scala#L122-L131

use std::assert_matches;

use crate::common::{self, StorageType, alphanumeric_string, init_tracing};
use nisshi_broker::Result;
use nisshi_sans_io::{
    ConfigResource, DescribeConfigsRequest, ErrorCode, RequestInput,
    create_topics_request::{CreatableTopic, CreatableTopicConfig},
    describe_configs_request::DescribeConfigsResource,
    describe_configs_response::DescribeConfigsResult,
};
use nisshi_storage::{DescribeConfigsService, Storage};
use rama::{Service, extensions::Extensions};
use rand::{prelude::*, rng};
use url::Url;
use uuid::Uuid;

async fn describe(
    storage: impl Storage + Clone,
    resources: impl IntoIterator<Item = (ConfigResource, String)>,
) -> Result<Vec<DescribeConfigsResult>> {
    let response = DescribeConfigsService { storage }
        .serve(RequestInput {
            request: DescribeConfigsRequest::default()
                .include_documentation(Some(false))
                .include_synonyms(Some(false))
                .resources(Some(
                    resources
                        .into_iter()
                        .map(|(resource_type, resource_name)| {
                            DescribeConfigsResource::default()
                                .resource_name(resource_name)
                                .resource_type(resource_type.into())
                                .configuration_keys(None)
                        })
                        .collect(),
                )),
            extensions: Extensions::default(),
        })
        .await?;

    Ok(response.results.unwrap_or_default())
}

async fn describe_broker_logger(
    storage: impl Storage + Clone,
    name: String,
) -> Result<DescribeConfigsResult> {
    let results = describe(storage, [(ConfigResource::BrokerLogger, name)]).await?;

    assert_eq!(1, results.len());
    Ok(results.into_iter().next().expect("one result"))
}

fn assert_no_loggers(result: &DescribeConfigsResult, name: &str) -> Result<()> {
    assert_eq!(ErrorCode::None, ErrorCode::try_from(result.error_code)?);
    assert_eq!(None, result.error_message);
    assert_eq!(name, result.resource_name);
    assert_eq!(i8::from(ConfigResource::BrokerLogger), result.resource_type);
    assert_matches!(result.configs.as_deref(), Some([]));
    Ok(())
}

fn assert_invalid_request(
    result: &DescribeConfigsResult,
    name: &str,
    error_message: &str,
) -> Result<()> {
    assert_eq!(
        ErrorCode::InvalidRequest,
        ErrorCode::try_from(result.error_code)?
    );
    assert_eq!(Some(error_message), result.error_message.as_deref());
    assert_eq!(name, result.resource_name);
    assert_eq!(i8::from(ConfigResource::BrokerLogger), result.resource_type);
    assert_matches!(result.configs.as_deref(), Some([]));
    Ok(())
}

async fn own_broker_id_has_no_loggers(storage: impl Storage + Clone, broker_id: i32) -> Result<()> {
    let name = broker_id.to_string();
    let result = describe_broker_logger(storage, name.clone()).await?;
    assert_no_loggers(&result, &name)
}

async fn empty_name_is_invalid_request(storage: impl Storage + Clone) -> Result<()> {
    let result = describe_broker_logger(storage, String::new()).await?;
    assert_invalid_request(&result, "", "Broker id must not be empty")
}

async fn other_broker_id_is_invalid_request(
    storage: impl Storage + Clone,
    broker_id: i32,
) -> Result<()> {
    let name = (broker_id + 1).to_string();
    let result = describe_broker_logger(storage, name.clone()).await?;

    // Kafka's message for a broker logger resource has no "or empty string", unlike the message
    // for a broker resource.
    assert_invalid_request(
        &result,
        &name,
        &format!("Unexpected broker id, expected {broker_id} but received {name}"),
    )
}

async fn non_integer_broker_id_is_invalid_request(storage: impl Storage + Clone) -> Result<()> {
    let result = describe_broker_logger(storage, "abc".into()).await?;
    assert_invalid_request(
        &result,
        "abc",
        "Broker id must be an integer, but it is: abc",
    )
}

async fn topic_named_after_broker_id_is_not_described_as_logger(
    storage: impl Storage + Clone,
    broker_id: i32,
) -> Result<()> {
    let name = broker_id.to_string();

    // The topic has a config, so a lookup of the logger's name as a topic name would list it.
    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(name.clone())
                .num_partitions(1)
                .replication_factor(0)
                .assignments(Some([].into()))
                .configs(Some(
                    [CreatableTopicConfig::default()
                        .name("cleanup.policy".into())
                        .value(Some("compact".into()))]
                    .into(),
                )),
            false,
        )
        .await?;

    let result = describe_broker_logger(storage, name.clone()).await?;
    assert_no_loggers(&result, &name)
}

async fn logger_error_stays_with_its_resource(
    storage: impl Storage + Clone,
    broker_id: i32,
) -> Result<()> {
    let topic_name = alphanumeric_string(15);

    let results = describe(
        storage,
        [
            (ConfigResource::BrokerLogger, (broker_id + 1).to_string()),
            (ConfigResource::Topic, topic_name.clone()),
        ],
    )
    .await?;

    assert_eq!(2, results.len());
    assert_eq!(
        ErrorCode::InvalidRequest,
        ErrorCode::try_from(results[0].error_code)?
    );
    assert_eq!(topic_name, results[1].resource_name);
    assert_eq!(i8::from(ConfigResource::Topic), results[1].resource_type);
    assert_eq!(
        ErrorCode::UnknownTopicOrPartition,
        ErrorCode::try_from(results[1].error_code)?
    );
    Ok(())
}

macro_rules! backend {
    ($module:ident, $storage_type:expr, $feature:literal) => {
        #[cfg(feature = $feature)]
        mod $module {
            use super::*;
            use nisshi_storage::ArcDynStorage;

            async fn storage_container(
                cluster: impl Into<String> + Clone,
                node: i32,
            ) -> Result<ArcDynStorage> {
                common::storage_container(
                    $storage_type,
                    cluster,
                    node,
                    Url::parse("tcp://127.0.0.1/")?,
                    None,
                )
                .await
            }

            #[tokio::test]
            async fn own_broker_id_has_no_loggers() -> Result<()> {
                let _guard = init_tracing()?;

                let broker_id = rng().random_range(0..i32::MAX);
                let storage = storage_container(Uuid::now_v7(), broker_id).await?;

                super::own_broker_id_has_no_loggers(storage, broker_id).await
            }

            #[tokio::test]
            async fn empty_name_is_invalid_request() -> Result<()> {
                let _guard = init_tracing()?;

                let broker_id = rng().random_range(0..i32::MAX);
                let storage = storage_container(Uuid::now_v7(), broker_id).await?;

                super::empty_name_is_invalid_request(storage).await
            }

            #[tokio::test]
            async fn other_broker_id_is_invalid_request() -> Result<()> {
                let _guard = init_tracing()?;

                let broker_id = rng().random_range(0..i32::MAX);
                let storage = storage_container(Uuid::now_v7(), broker_id).await?;

                super::other_broker_id_is_invalid_request(storage, broker_id).await
            }

            #[tokio::test]
            async fn non_integer_broker_id_is_invalid_request() -> Result<()> {
                let _guard = init_tracing()?;

                let broker_id = rng().random_range(0..i32::MAX);
                let storage = storage_container(Uuid::now_v7(), broker_id).await?;

                super::non_integer_broker_id_is_invalid_request(storage).await
            }

            #[tokio::test]
            async fn topic_named_after_broker_id_is_not_described_as_logger() -> Result<()> {
                let _guard = init_tracing()?;

                let broker_id = rng().random_range(0..i32::MAX);
                let storage = storage_container(Uuid::now_v7(), broker_id).await?;

                super::topic_named_after_broker_id_is_not_described_as_logger(storage, broker_id)
                    .await
            }

            #[tokio::test]
            async fn logger_error_stays_with_its_resource() -> Result<()> {
                let _guard = init_tracing()?;

                let broker_id = rng().random_range(0..i32::MAX);
                let storage = storage_container(Uuid::now_v7(), broker_id).await?;

                super::logger_error_stays_with_its_resource(storage, broker_id).await
            }
        }
    };
}

backend!(in_memory, StorageType::InMemory, "dynostore");
backend!(lite, StorageType::Lite, "libsql");
backend!(pg, StorageType::Postgres, "postgres");
