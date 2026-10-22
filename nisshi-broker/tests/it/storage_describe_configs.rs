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

use std::assert_matches;

use crate::common::{
    alphanumeric_string, init_tracing, lite_storage, memory_storage, postgres_storage,
    slate_storage,
};
use nisshi_broker::Result;
use nisshi_sans_io::{
    ConfigResource, DescribeConfigsRequest, DescribeConfigsResponse, ErrorCode, RequestInput,
    create_topics_request::{CreatableTopic, CreatableTopicConfig},
    describe_configs_request::DescribeConfigsResource,
    describe_configs_response::DescribeConfigsResult,
};
use nisshi_storage::{ArcDynStorage, DescribeConfigsService, Storage};
use rama::{Service, extensions::Extensions};
use rand::{prelude::*, rng};
use uuid::Uuid;

async fn simple(storage: impl Storage + Clone) -> Result<()> {
    let resource_name = &alphanumeric_string(15)[..];

    let service = DescribeConfigsService { storage };

    let response = service
        .serve(RequestInput {
            request: DescribeConfigsRequest::default()
                .include_documentation(Some(false))
                .include_synonyms(Some(false))
                .resources(Some(
                    [DescribeConfigsResource::default()
                        .resource_name(resource_name.into())
                        .resource_type(ConfigResource::Topic.into())
                        .configuration_keys(Some([].into()))]
                    .into(),
                )),
            extensions: Extensions::default(),
        })
        .await?;

    let results = response.results.unwrap_or_default();
    assert_eq!(1, results.len());

    assert_eq!(
        ErrorCode::UnknownTopicOrPartition,
        ErrorCode::try_from(results[0].error_code)?
    );

    assert!(results[0].configs.as_deref().unwrap_or_default().is_empty());

    Ok(())
}

async fn describe(
    storage: impl Storage + Clone,
    resources: impl IntoIterator<Item = (ConfigResource, String)>,
) -> Result<DescribeConfigsResponse> {
    DescribeConfigsService { storage }
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
        .await
        .map_err(Into::into)
}

async fn describe_broker(
    storage: impl Storage + Clone,
    broker_name: String,
) -> Result<DescribeConfigsResult> {
    let results = describe(storage, [(ConfigResource::Broker, broker_name)])
        .await?
        .results
        .unwrap_or_default();

    assert_eq!(1, results.len());
    Ok(results.into_iter().next().expect("one result"))
}

fn assert_empty_broker_result(result: &DescribeConfigsResult, broker_name: &str) -> Result<()> {
    assert_eq!(ErrorCode::None, ErrorCode::try_from(result.error_code)?);
    assert_eq!(None, result.error_message);
    assert_eq!(broker_name, result.resource_name);
    assert_eq!(i8::from(ConfigResource::Broker), result.resource_type);
    assert_matches!(result.configs.as_deref(), Some([]));
    Ok(())
}

async fn broker_default_configs_are_empty(storage: impl Storage + Clone) -> Result<()> {
    let result = describe_broker(storage, String::new()).await?;
    assert_empty_broker_result(&result, "")
}

async fn own_broker_id_is_described(storage: impl Storage + Clone, broker_id: i32) -> Result<()> {
    let broker_name = broker_id.to_string();
    let result = describe_broker(storage, broker_name.clone()).await?;
    assert_empty_broker_result(&result, &broker_name)
}

async fn other_broker_id_is_invalid_request(
    storage: impl Storage + Clone,
    broker_id: i32,
) -> Result<()> {
    let broker_name = (broker_id + 1).to_string();
    let result = describe_broker(storage, broker_name.clone()).await?;

    assert_eq!(
        ErrorCode::InvalidRequest,
        ErrorCode::try_from(result.error_code)?
    );
    assert_eq!(
        Some(format!(
            "Unexpected broker id, expected {broker_id} or empty string, but received {broker_name}"
        )),
        result.error_message
    );
    assert_eq!(broker_name, result.resource_name);
    assert_eq!(i8::from(ConfigResource::Broker), result.resource_type);
    assert_matches!(result.configs.as_deref(), Some([]));
    Ok(())
}

async fn non_integer_broker_id_is_invalid_request(storage: impl Storage + Clone) -> Result<()> {
    let result = describe_broker(storage, "abc".into()).await?;

    assert_eq!(
        ErrorCode::InvalidRequest,
        ErrorCode::try_from(result.error_code)?
    );
    assert_eq!(
        Some(String::from("Broker id must be an integer, but it is: abc")),
        result.error_message
    );
    assert_eq!("abc", result.resource_name);
    assert_matches!(result.configs.as_deref(), Some([]));
    Ok(())
}

async fn broker_id_is_parsed_as_kafka_parses_it(
    storage: impl Storage + Clone,
    broker_id: i32,
) -> Result<()> {
    let own_id = format!("Unexpected broker id, expected {broker_id} or empty string");
    let not_an_integer = "Broker id must be an integer";

    for (broker_name, expected) in [
        (format!("+{broker_id}"), None),
        (format!("0{broker_id}"), None),
        (format!(" {broker_id}"), Some(not_an_integer)),
        (String::from("-1"), Some(own_id.as_str())),
        (String::from("2147483648"), Some(not_an_integer)),
    ] {
        let result = describe_broker(storage.clone(), broker_name.clone()).await?;

        match expected {
            None => assert_empty_broker_result(&result, &broker_name)?,
            Some(message) => {
                assert_eq!(
                    ErrorCode::InvalidRequest,
                    ErrorCode::try_from(result.error_code)?,
                    "{broker_name:?}"
                );
                assert_matches!(
                    result.error_message.as_deref(),
                    Some(error_message) if error_message.starts_with(message),
                    "{broker_name:?}"
                );
            }
        }
    }

    Ok(())
}

async fn topic_named_after_broker_id_is_not_described_as_broker(
    storage: impl Storage + Clone,
    broker_id: i32,
) -> Result<()> {
    let broker_name = broker_id.to_string();

    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(broker_name.clone())
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

    let result = describe_broker(storage, broker_name.clone()).await?;
    assert_empty_broker_result(&result, &broker_name)
}

async fn broker_error_stays_with_its_resource(
    storage: impl Storage + Clone,
    broker_id: i32,
) -> Result<()> {
    let topic_name = alphanumeric_string(15);

    let results = describe(
        storage,
        [
            (ConfigResource::Broker, (broker_id + 1).to_string()),
            (ConfigResource::Topic, topic_name.clone()),
        ],
    )
    .await?
    .results
    .unwrap_or_default();

    assert_eq!(2, results.len());
    assert_eq!(
        ErrorCode::InvalidRequest,
        ErrorCode::try_from(results[0].error_code)?
    );
    assert_eq!(topic_name, results[1].resource_name);
    assert_eq!(i8::from(ConfigResource::Topic), results[1].resource_type);
    assert_matches!(
        ErrorCode::try_from(results[1].error_code)?,
        ErrorCode::None | ErrorCode::UnknownTopicOrPartition
    );
    Ok(())
}

/// A describe that names keys returns only the configs among those keys that the topic sets, as
/// Kafka does.
#[cfg(feature = "postgres")]
async fn named_keys_return_only_those_configs(storage: impl Storage + Clone) -> Result<()> {
    let topic = alphanumeric_string(15);

    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(topic.clone())
                .num_partitions(1)
                .replication_factor(0)
                .assignments(Some([].into()))
                .configs(Some(
                    [
                        CreatableTopicConfig::default()
                            .name("cleanup.policy".into())
                            .value(Some("compact".into())),
                        CreatableTopicConfig::default()
                            .name("message.timestamp.type".into())
                            .value(Some("LogAppendTime".into())),
                    ]
                    .into(),
                )),
            false,
        )
        .await?;

    let described = storage
        .describe_config(
            &topic,
            ConfigResource::Topic,
            Some(&["message.timestamp.type".into(), "retention.ms".into()]),
        )
        .await?;

    assert_eq!(ErrorCode::None, ErrorCode::try_from(described.error_code)?);
    let described_configs = described
        .configs
        .unwrap_or_default()
        .into_iter()
        .map(|config| (config.name, config.value))
        .collect::<Vec<_>>();
    assert_eq!(
        vec![(
            String::from("message.timestamp.type"),
            Some(String::from("LogAppendTime"))
        )],
        described_configs
    );

    let unset = storage
        .describe_config(
            &topic,
            ConfigResource::Topic,
            Some(&["retention.ms".into()]),
        )
        .await?;

    assert_eq!(ErrorCode::None, ErrorCode::try_from(unset.error_code)?);
    assert_matches!(unset.configs.as_deref(), Some([]));

    Ok(())
}

#[cfg(feature = "dynostore")]
mod in_memory {
    use super::*;

    async fn storage_container(
        cluster: impl Into<String> + Clone,
        node: i32,
    ) -> Result<ArcDynStorage> {
        memory_storage(cluster, node).await
    }

    #[tokio::test]
    async fn simple() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::simple(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn broker_default_configs_are_empty() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::broker_default_configs_are_empty(storage).await
    }

    #[tokio::test]
    async fn own_broker_id_is_described() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::own_broker_id_is_described(storage, broker_id).await
    }

    #[tokio::test]
    async fn other_broker_id_is_invalid_request() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::other_broker_id_is_invalid_request(storage, broker_id).await
    }

    #[tokio::test]
    async fn non_integer_broker_id_is_invalid_request() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::non_integer_broker_id_is_invalid_request(storage).await
    }

    #[tokio::test]
    async fn broker_id_is_parsed_as_kafka_parses_it() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::broker_id_is_parsed_as_kafka_parses_it(storage, broker_id).await
    }

    #[tokio::test]
    async fn topic_named_after_broker_id_is_not_described_as_broker() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::topic_named_after_broker_id_is_not_described_as_broker(storage, broker_id).await
    }

    #[tokio::test]
    async fn broker_error_stays_with_its_resource() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::broker_error_stays_with_its_resource(storage, broker_id).await
    }
}

#[cfg(feature = "libsql")]
mod lite {
    use super::*;

    async fn storage_container(
        cluster: impl Into<String> + Clone,
        node: i32,
    ) -> Result<ArcDynStorage> {
        lite_storage(cluster, node).await
    }

    #[tokio::test]
    async fn simple() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::simple(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn broker_default_configs_are_empty() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::broker_default_configs_are_empty(storage).await
    }

    #[tokio::test]
    async fn own_broker_id_is_described() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::own_broker_id_is_described(storage, broker_id).await
    }

    #[tokio::test]
    async fn other_broker_id_is_invalid_request() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::other_broker_id_is_invalid_request(storage, broker_id).await
    }

    #[tokio::test]
    async fn non_integer_broker_id_is_invalid_request() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::non_integer_broker_id_is_invalid_request(storage).await
    }

    #[tokio::test]
    async fn broker_id_is_parsed_as_kafka_parses_it() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::broker_id_is_parsed_as_kafka_parses_it(storage, broker_id).await
    }

    #[tokio::test]
    async fn topic_named_after_broker_id_is_not_described_as_broker() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::topic_named_after_broker_id_is_not_described_as_broker(storage, broker_id).await
    }

    #[tokio::test]
    async fn broker_error_stays_with_its_resource() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::broker_error_stays_with_its_resource(storage, broker_id).await
    }
}

#[cfg(feature = "turso")]
mod turso {
    use super::*;

    async fn storage_container(
        cluster: impl Into<String> + Clone,
        node: i32,
    ) -> Result<ArcDynStorage> {
        lite_storage(cluster, node).await
    }

    #[tokio::test]
    async fn simple() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::simple(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn broker_default_configs_are_empty() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::broker_default_configs_are_empty(storage).await
    }

    #[tokio::test]
    async fn own_broker_id_is_described() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::own_broker_id_is_described(storage, broker_id).await
    }

    #[tokio::test]
    async fn other_broker_id_is_invalid_request() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::other_broker_id_is_invalid_request(storage, broker_id).await
    }

    #[tokio::test]
    async fn non_integer_broker_id_is_invalid_request() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::non_integer_broker_id_is_invalid_request(storage).await
    }

    #[tokio::test]
    async fn broker_id_is_parsed_as_kafka_parses_it() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::broker_id_is_parsed_as_kafka_parses_it(storage, broker_id).await
    }

    #[tokio::test]
    async fn topic_named_after_broker_id_is_not_described_as_broker() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::topic_named_after_broker_id_is_not_described_as_broker(storage, broker_id).await
    }

    #[tokio::test]
    async fn broker_error_stays_with_its_resource() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::broker_error_stays_with_its_resource(storage, broker_id).await
    }
}

#[cfg(feature = "slatedb")]
mod slatedb {
    use super::*;

    async fn storage_container(
        cluster: impl Into<String> + Clone,
        node: i32,
    ) -> Result<ArcDynStorage> {
        slate_storage(cluster, node).await
    }

    #[tokio::test]
    async fn simple() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::simple(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn broker_default_configs_are_empty() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::broker_default_configs_are_empty(storage).await
    }

    #[tokio::test]
    async fn own_broker_id_is_described() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::own_broker_id_is_described(storage, broker_id).await
    }

    #[tokio::test]
    async fn other_broker_id_is_invalid_request() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::other_broker_id_is_invalid_request(storage, broker_id).await
    }

    #[tokio::test]
    async fn non_integer_broker_id_is_invalid_request() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::non_integer_broker_id_is_invalid_request(storage).await
    }

    #[tokio::test]
    async fn broker_id_is_parsed_as_kafka_parses_it() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::broker_id_is_parsed_as_kafka_parses_it(storage, broker_id).await
    }

    #[tokio::test]
    async fn topic_named_after_broker_id_is_not_described_as_broker() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::topic_named_after_broker_id_is_not_described_as_broker(storage, broker_id).await
    }

    #[tokio::test]
    async fn broker_error_stays_with_its_resource() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::broker_error_stays_with_its_resource(storage, broker_id).await
    }
}

#[cfg(feature = "postgres")]
mod pg {
    use super::*;

    #[tokio::test]
    async fn named_keys_return_only_those_configs() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::named_keys_return_only_those_configs(storage).await
    }

    async fn storage_container(
        cluster: impl Into<String> + Clone,
        node: i32,
    ) -> Result<ArcDynStorage> {
        postgres_storage(cluster, node).await
    }

    #[tokio::test]
    async fn simple() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::simple(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn broker_default_configs_are_empty() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::broker_default_configs_are_empty(storage).await
    }

    #[tokio::test]
    async fn own_broker_id_is_described() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::own_broker_id_is_described(storage, broker_id).await
    }

    #[tokio::test]
    async fn other_broker_id_is_invalid_request() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::other_broker_id_is_invalid_request(storage, broker_id).await
    }

    #[tokio::test]
    async fn non_integer_broker_id_is_invalid_request() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::non_integer_broker_id_is_invalid_request(storage).await
    }

    #[tokio::test]
    async fn broker_id_is_parsed_as_kafka_parses_it() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::broker_id_is_parsed_as_kafka_parses_it(storage, broker_id).await
    }

    #[tokio::test]
    async fn topic_named_after_broker_id_is_not_described_as_broker() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::topic_named_after_broker_id_is_not_described_as_broker(storage, broker_id).await
    }

    #[tokio::test]
    async fn broker_error_stays_with_its_resource() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::broker_error_stays_with_its_resource(storage, broker_id).await
    }
}
