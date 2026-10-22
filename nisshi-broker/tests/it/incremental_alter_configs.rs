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

use crate::common::{
    alphanumeric_string, init_tracing, lite_storage, memory_storage, postgres_storage,
    slate_storage,
};
use nisshi_broker::Result;
use nisshi_sans_io::{
    ConfigResource, CreateTopicsRequest, DescribeConfigsRequest, ErrorCode,
    IncrementalAlterConfigsRequest, IncrementalAlterConfigsResponse, OpType, RequestInput,
    create_topics_request::CreatableTopic,
    describe_configs_request::DescribeConfigsResource,
    incremental_alter_configs_request::{AlterConfigsResource, AlterableConfig},
};
use nisshi_storage::{
    ArcDynStorage, CreateTopicsService, CreateTopicsValidationLayer, DescribeConfigsService,
    IncrementalAlterConfigsService, Storage,
};
use rama::{Layer as _, Service, extensions::Extensions};
use rand::{RngExt as _, rng};
use std::collections::BTreeSet;
use tokio::task::JoinSet;
use tracing::debug;
use uuid::Uuid;

async fn simple(storage: impl Storage + Clone) -> Result<()> {
    let resource_name = &alphanumeric_string(15)[..];

    let create_topic = CreateTopicsValidationLayer::new().layer(CreateTopicsService {
        storage: storage.clone(),
    });

    let extensions = Extensions::default();

    let response = create_topic
        .serve(RequestInput {
            request: CreateTopicsRequest::default().topics(Some(
                [CreatableTopic::default()
                    .name(resource_name.into())
                    .num_partitions(3)
                    .replication_factor(1)]
                .into(),
            )),
            extensions: extensions.clone(),
        })
        .await
        .inspect(|create_topic_response| debug!(?create_topic_response))?;

    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(response.topics.unwrap_or_default()[0].error_code)?
    );

    let config_name = "x.y.z";
    let config_value = "pqr";

    let describe_configs = DescribeConfigsService {
        storage: storage.clone(),
    };

    let response = describe_configs
        .serve(RequestInput {
            request: DescribeConfigsRequest::default()
                .include_documentation(Some(false))
                .include_synonyms(Some(false))
                .resources(Some(
                    [DescribeConfigsResource::default()
                        .resource_name(resource_name.into())
                        .resource_type(ConfigResource::Topic.into())
                        .configuration_keys(Some([config_name.into()].into()))]
                    .into(),
                )),
            extensions: extensions.clone(),
        })
        .await
        .inspect(|describe_configs_response| debug!(?describe_configs_response))?;

    assert!(
        response
            .results
            .unwrap_or_default()
            .first()
            .is_some_and(|first| first.configs.as_deref().unwrap_or_default().is_empty())
    );

    let alter_configs = IncrementalAlterConfigsService {
        storage: storage.clone(),
    };

    let _response = alter_configs
        .serve(RequestInput {
            request: IncrementalAlterConfigsRequest::default().resources(Some(
                [AlterConfigsResource::default()
                    .resource_name(resource_name.into())
                    .resource_type(ConfigResource::Topic.into())
                    .configs(Some(
                        [AlterableConfig::default()
                            .config_operation(OpType::Set.into())
                            .name(config_name.into())
                            .value(Some(config_value.into()))]
                        .into(),
                    ))]
                .into(),
            )),
            extensions: extensions.clone(),
        })
        .await?;

    let response = describe_configs
        .serve(RequestInput {
            request: DescribeConfigsRequest::default()
                .include_documentation(Some(false))
                .include_synonyms(Some(false))
                .resources(Some(
                    [DescribeConfigsResource::default()
                        .resource_name(resource_name.into())
                        .resource_type(ConfigResource::Topic.into())
                        .configuration_keys(Some([config_name.into()].into()))]
                    .into(),
                )),
            extensions: extensions.clone(),
        })
        .await?;

    let results = response.results.as_deref().unwrap_or(&[]);
    assert_eq!(1, results.len());
    assert_eq!(resource_name, results[0].resource_name.as_str());

    let configs = results[0].configs.as_deref().unwrap_or(&[]);
    assert_eq!(1, configs.len());
    assert_eq!(Some(config_value), configs[0].value.as_deref());

    Ok(())
}

const CLEANUP_POLICY: &str = "cleanup.policy";

async fn create_topic(storage: impl Storage + Clone, name: &str) -> Result<()> {
    let response = CreateTopicsValidationLayer::new()
        .layer(CreateTopicsService { storage })
        .serve(RequestInput {
            request: CreateTopicsRequest::default().topics(Some(
                [CreatableTopic::default()
                    .name(name.into())
                    .num_partitions(1)
                    .replication_factor(1)]
                .into(),
            )),
            extensions: Extensions::default(),
        })
        .await?;

    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(response.topics.unwrap_or_default()[0].error_code)?
    );

    Ok(())
}

/// Returns the value of `key` on topic `name`, or `None` when the topic doesn't
/// set it.
async fn config_value(
    storage: impl Storage + Clone,
    name: &str,
    key: &str,
) -> Result<Option<String>> {
    // `Storage::describe_config` returns only the configs that the topic sets.
    // `DescribeConfigsService` also lists Kafka's defaults.
    let result = storage
        .describe_config(name, ConfigResource::Topic, Some(&[key.into()]))
        .await?;

    Ok(result
        .configs
        .unwrap_or_default()
        .into_iter()
        .find(|config| config.name == key)
        .and_then(|config| config.value))
}

fn topic(name: &str, configs: impl Into<Vec<AlterableConfig>>) -> AlterConfigsResource {
    AlterConfigsResource::default()
        .resource_name(name.into())
        .resource_type(ConfigResource::Topic.into())
        .configs(Some(configs.into()))
}

fn config(op: i8, name: &str, value: Option<&str>) -> AlterableConfig {
    AlterableConfig::default()
        .config_operation(op)
        .name(name.into())
        .value(value.map(Into::into))
}

async fn alter(
    storage: impl Storage + Clone,
    resources: impl Into<Vec<AlterConfigsResource>>,
    validate_only: bool,
) -> Result<IncrementalAlterConfigsResponse> {
    IncrementalAlterConfigsService { storage }
        .serve(RequestInput {
            request: IncrementalAlterConfigsRequest::default()
                .resources(Some(resources.into()))
                .validate_only(validate_only),
            extensions: Extensions::default(),
        })
        .await
        .inspect(|response| debug!(?response))
        .map_err(Into::into)
}

/// Returns the error code of each resource in `response`, in order.
fn error_codes(response: &IncrementalAlterConfigsResponse) -> Result<Vec<ErrorCode>> {
    response
        .responses
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(|response| ErrorCode::try_from(response.error_code).map_err(Into::into))
        .collect()
}

async fn append_and_subtract(storage: impl Storage + Clone) -> Result<()> {
    let name = &alphanumeric_string(15)[..];
    create_topic(storage.clone(), name).await?;

    // An unset cleanup.policy is Kafka's default of `delete`, so the first
    // APPEND keeps deletion on.
    for (op, value, expected) in [
        (OpType::Append, "compact", "delete,compact"),
        (OpType::Append, "compact", "delete,compact"),
        (OpType::Append, "delete,x", "delete,compact,x"),
        (OpType::Subtract, "delete", "compact,x"),
        (OpType::Subtract, "x,compact", ""),
    ] {
        let response = alter(
            storage.clone(),
            [topic(
                name,
                [config(op.into(), CLEANUP_POLICY, Some(value))],
            )],
            false,
        )
        .await?;

        assert_eq!(vec![ErrorCode::None], error_codes(&response)?);
        assert_eq!(
            Some(expected),
            config_value(storage.clone(), name, CLEANUP_POLICY)
                .await?
                .as_deref(),
            "{op:?} {value}"
        );
    }

    Ok(())
}

async fn append_to_non_list_is_atomic(storage: impl Storage + Clone) -> Result<()> {
    let name = &alphanumeric_string(15)[..];
    create_topic(storage.clone(), name).await?;

    let response = alter(
        storage.clone(),
        [topic(
            name,
            [
                config(OpType::Set.into(), "x.y.z", Some("pqr")),
                config(OpType::Append.into(), "retention.ms", Some("1000")),
            ],
        )],
        false,
    )
    .await?;

    assert_eq!(vec![ErrorCode::InvalidConfig], error_codes(&response)?);
    assert_eq!(
        Some("Can't APPEND to key retention.ms because its type is not LIST."),
        response.responses.as_deref().unwrap_or_default()[0]
            .error_message
            .as_deref()
    );

    assert_eq!(None, config_value(storage.clone(), name, "x.y.z").await?);
    assert_eq!(None, config_value(storage, name, "retention.ms").await?);

    Ok(())
}

async fn invalid_request(storage: impl Storage + Clone) -> Result<()> {
    let name = &alphanumeric_string(15)[..];
    create_topic(storage.clone(), name).await?;

    let unknown_op = topic(name, [config(7, "x.y.z", Some("pqr"))]);

    let duplicate_keys = topic(
        name,
        [
            config(OpType::Set.into(), "x.y.z", Some("pqr")),
            config(OpType::Delete.into(), "x.y.z", None),
        ],
    );

    let null_value = topic(name, [config(OpType::Set.into(), "x.y.z", None)]);

    for resource in [unknown_op, duplicate_keys, null_value] {
        let response = alter(storage.clone(), [resource], false).await?;
        assert_eq!(vec![ErrorCode::InvalidRequest], error_codes(&response)?);
    }

    let set = topic(name, [config(OpType::Set.into(), "x.y.z", Some("pqr"))]);
    let response = alter(storage.clone(), [set.clone(), set], false).await?;
    assert_eq!(
        vec![ErrorCode::InvalidRequest, ErrorCode::InvalidRequest],
        error_codes(&response)?
    );

    assert_eq!(None, config_value(storage, name, "x.y.z").await?);

    Ok(())
}

async fn unknown_topic(storage: impl Storage + Clone) -> Result<()> {
    let name = &alphanumeric_string(15)[..];

    for op in [OpType::Set, OpType::Append] {
        let response = alter(
            storage.clone(),
            [topic(
                name,
                [config(op.into(), CLEANUP_POLICY, Some("compact"))],
            )],
            false,
        )
        .await?;

        assert_eq!(
            vec![ErrorCode::UnknownTopicOrPartition],
            error_codes(&response)?
        );
        assert_eq!(
            Some(format!("The topic '{name}' does not exist.").as_str()),
            response.responses.as_deref().unwrap_or_default()[0]
                .error_message
                .as_deref()
        );
    }

    Ok(())
}

async fn multiple_changes(storage: impl Storage + Clone) -> Result<()> {
    let name = &alphanumeric_string(15)[..];
    create_topic(storage.clone(), name).await?;

    let response = alter(
        storage.clone(),
        [topic(name, [config(OpType::Set.into(), "b", Some("2"))])],
        false,
    )
    .await?;
    assert_eq!(vec![ErrorCode::None], error_codes(&response)?);

    let response = alter(
        storage.clone(),
        [topic(
            name,
            [
                config(OpType::Set.into(), "a", Some("1")),
                config(OpType::Append.into(), CLEANUP_POLICY, Some("compact")),
                config(OpType::Delete.into(), "b", None),
            ],
        )],
        false,
    )
    .await?;
    assert_eq!(vec![ErrorCode::None], error_codes(&response)?);

    assert_eq!(
        Some("1"),
        config_value(storage.clone(), name, "a").await?.as_deref()
    );
    assert_eq!(
        Some("delete,compact"),
        config_value(storage.clone(), name, CLEANUP_POLICY)
            .await?
            .as_deref()
    );
    assert_eq!(None, config_value(storage, name, "b").await?);

    Ok(())
}

/// Calls storage directly, without the service's checks, so that the second
/// change of a resource fails after storage has applied the first.
async fn storage_rolls_back_failed_resource(storage: impl Storage + Clone) -> Result<()> {
    let name = &alphanumeric_string(15)[..];
    create_topic(storage.clone(), name).await?;

    let unknown_op = storage
        .incremental_alter_resource(topic(
            name,
            [
                config(OpType::Set.into(), "x.y.z", Some("pqr")),
                config(7, "a.b.c", Some("abc")),
            ],
        ))
        .await;
    assert!(
        matches!(unknown_op, Err(ref error) if !matches!(error, nisshi_storage::Error::Api(_))),
        "{unknown_op:?}"
    );

    let not_a_list = storage
        .incremental_alter_resource(topic(
            name,
            [
                config(OpType::Set.into(), "x.y.z", Some("pqr")),
                config(OpType::Append.into(), "retention.ms", Some("1000")),
            ],
        ))
        .await;
    assert!(
        matches!(
            not_a_list,
            Err(nisshi_storage::Error::Api(ErrorCode::InvalidConfig))
        ),
        "{not_a_list:?}"
    );

    assert_eq!(None, config_value(storage.clone(), name, "x.y.z").await?);

    // The storage still accepts an alter after it rolled back the others.
    let response = alter(
        storage.clone(),
        [topic(
            name,
            [config(OpType::Set.into(), "x.y.z", Some("pqr"))],
        )],
        false,
    )
    .await?;
    assert_eq!(vec![ErrorCode::None], error_codes(&response)?);
    assert_eq!(
        Some("pqr"),
        config_value(storage, name, "x.y.z").await?.as_deref()
    );

    Ok(())
}

/// Each APPEND reads the current list and writes a new one, so concurrent
/// APPEND operations on one topic lose an item unless storage orders them.
async fn concurrent_appends(storage: impl Storage + Clone) -> Result<()> {
    const APPENDS: usize = 8;

    let name = alphanumeric_string(15);
    create_topic(storage.clone(), &name).await?;

    let mut appends = JoinSet::new();

    for item in 0..APPENDS {
        let storage = storage.clone();
        let name = name.clone();

        _ = appends.spawn(async move {
            let value = format!("item-{item}");

            alter(
                storage,
                [topic(
                    &name,
                    [config(OpType::Append.into(), CLEANUP_POLICY, Some(&value))],
                )],
                false,
            )
            .await
            .and_then(|response| error_codes(&response))
        });
    }

    while let Some(codes) = appends.join_next().await {
        assert_eq!(vec![ErrorCode::None], codes??);
    }

    let value = config_value(storage, &name, CLEANUP_POLICY)
        .await?
        .unwrap_or_default();
    let items: BTreeSet<&str> = value.split(',').collect();

    assert_eq!(
        // The topic starts with Kafka's default cleanup.policy, delete.
        std::iter::once("delete".to_owned())
            .chain((0..APPENDS).map(|item| format!("item-{item}")))
            .collect::<BTreeSet<_>>(),
        items.into_iter().map(ToOwned::to_owned).collect()
    );

    Ok(())
}

async fn validate_only(storage: impl Storage + Clone) -> Result<()> {
    let name = &alphanumeric_string(15)[..];
    create_topic(storage.clone(), name).await?;

    let missing = &alphanumeric_string(15)[..];

    let response = alter(
        storage.clone(),
        [
            topic(name, [config(OpType::Set.into(), "x.y.z", Some("pqr"))]),
            topic(missing, [config(OpType::Set.into(), "x.y.z", Some("pqr"))]),
        ],
        true,
    )
    .await?;

    assert_eq!(
        vec![ErrorCode::None, ErrorCode::UnknownTopicOrPartition],
        error_codes(&response)?
    );

    assert_eq!(None, config_value(storage, name, "x.y.z").await?);

    Ok(())
}

/// A `retention.ms` or `retention.bytes` that Kafka rejects must not be stored: maintenance reads
/// the value, and a `retention.ms` below -1 or a value that isn't a long makes it delete every
/// record or fail for every topic. Kafka checks the value before it checks that the topic exists,
/// so a missing topic gets the same error.
async fn invalid_retention_config_rejected(storage: impl Storage + Clone) -> Result<()> {
    let name = &alphanumeric_string(15)[..];
    create_topic(storage.clone(), name).await?;
    let missing = &alphanumeric_string(15)[..];

    for (key, value, reason) in [
        ("retention.ms", "-2", "Value must be at least -1"),
        ("retention.ms", "abc", "Not a number of type LONG"),
        (
            "retention.bytes",
            "99999999999999999999",
            "Not a number of type LONG",
        ),
    ] {
        for validate_only in [false, true] {
            let response = alter(
                storage.clone(),
                [
                    topic(name, [config(OpType::Set.into(), key, Some(value))]),
                    topic(missing, [config(OpType::Set.into(), key, Some(value))]),
                ],
                validate_only,
            )
            .await?;

            assert_eq!(
                vec![ErrorCode::InvalidConfig, ErrorCode::InvalidConfig],
                error_codes(&response)?,
                "{key}={value}, validate_only = {validate_only}"
            );
            assert_eq!(
                Some(format!("Invalid value {value} for configuration {key}: {reason}").as_str()),
                response.responses.as_deref().unwrap_or_default()[0]
                    .error_message
                    .as_deref()
            );
        }

        assert_eq!(None, config_value(storage.clone(), name, key).await?);
    }

    Ok(())
}

/// Kafka accepts a `retention.ms` with white space or a `+` around the number, but maintenance
/// reads only a plain number, and reads anything else as no limit. The broker must store the plain
/// number, or the topic keeps every record.
async fn retention_ms_is_stored_as_a_plain_number(storage: impl Storage + Clone) -> Result<()> {
    let name = &alphanumeric_string(15)[..];
    create_topic(storage.clone(), name).await?;

    let response = alter(
        storage.clone(),
        [topic(
            name,
            [config(
                OpType::Set.into(),
                "retention.ms",
                Some("+600000\n"),
            )],
        )],
        false,
    )
    .await?;

    assert_eq!(vec![ErrorCode::None], error_codes(&response)?);
    assert_eq!(
        Some("600000".to_owned()),
        config_value(storage, name, "retention.ms").await?
    );

    Ok(())
}

macro_rules! backend_tests {
    ($($name:ident),+ $(,)?) => {
        $(
            #[tokio::test]
            async fn $name() -> Result<()> {
                let _guard = init_tracing()?;

                let cluster_id = Uuid::now_v7();
                let broker_id = rng().random_range(0..i32::MAX);

                let storage = storage_container(cluster_id, broker_id).await?;

                super::$name(storage).await
            }
        )+
    };
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

    backend_tests!(
        simple,
        append_and_subtract,
        append_to_non_list_is_atomic,
        invalid_request,
        unknown_topic,
        validate_only,
        multiple_changes,
        storage_rolls_back_failed_resource,
        concurrent_appends,
        invalid_retention_config_rejected,
        retention_ms_is_stored_as_a_plain_number,
    );
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

    backend_tests!(
        simple,
        append_and_subtract,
        append_to_non_list_is_atomic,
        invalid_request,
        unknown_topic,
        validate_only,
        multiple_changes,
        storage_rolls_back_failed_resource,
        concurrent_appends,
        invalid_retention_config_rejected,
        retention_ms_is_stored_as_a_plain_number,
    );
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

    backend_tests!(
        simple,
        append_and_subtract,
        append_to_non_list_is_atomic,
        invalid_request,
        unknown_topic,
        validate_only,
        multiple_changes,
        storage_rolls_back_failed_resource,
        concurrent_appends,
    );
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

    backend_tests!(
        simple,
        append_and_subtract,
        append_to_non_list_is_atomic,
        invalid_request,
        unknown_topic,
        validate_only,
        multiple_changes,
        storage_rolls_back_failed_resource,
        concurrent_appends,
        invalid_retention_config_rejected,
        retention_ms_is_stored_as_a_plain_number,
    );
}

#[cfg(feature = "postgres")]
mod pg {
    use super::*;

    async fn storage_container(
        cluster: impl Into<String> + Clone,
        node: i32,
    ) -> Result<ArcDynStorage> {
        postgres_storage(cluster, node).await
    }

    backend_tests!(
        simple,
        append_and_subtract,
        append_to_non_list_is_atomic,
        invalid_request,
        unknown_topic,
        validate_only,
        multiple_changes,
        storage_rolls_back_failed_resource,
        concurrent_appends,
        invalid_retention_config_rejected,
        retention_ms_is_stored_as_a_plain_number,
    );
}
