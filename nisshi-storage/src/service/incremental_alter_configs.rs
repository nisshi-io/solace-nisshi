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

use std::collections::{BTreeMap, BTreeSet};

use nisshi_sans_io::{
    ApiKey, ConfigResource, ErrorCode, IncrementalAlterConfigsRequest,
    IncrementalAlterConfigsResponse, OpType, RequestInput,
    incremental_alter_configs_request::AlterConfigsResource,
    incremental_alter_configs_response::AlterConfigsResourceResponse,
};
use rama::Service;
use tracing::{debug, instrument, warn};

use crate::{
    Error, Result, Storage, TopicId,
    config::{is_list_config, validate_topic_config},
    max_message_bytes,
};

/// A [`Service`] using its [`Storage`] taking [`IncrementalAlterConfigsRequest`] returning [`IncrementalAlterConfigsResponse`].
/// ```no_run
/// use rama::{Layer as _, Service};
/// use nisshi_sans_io::{
///     ConfigResource, CreateTopicsRequest, DescribeConfigsRequest, ErrorCode,
///     IncrementalAlterConfigsRequest, OpType,
///     create_topics_request::CreatableTopic,
///     describe_configs_request::DescribeConfigsResource,
///     incremental_alter_configs_request::{AlterConfigsResource, AlterableConfig},
/// };
/// use nisshi_storage::{
///     CreateTopicsService, CreateTopicsValidationLayer, DescribeConfigsService, Error,
///     IncrementalAlterConfigsService, StorageContainer,
/// };
/// use url::Url;
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Error> {
/// const HOST: &str = "localhost";
/// const PORT: i32 = 9092;
/// const NODE_ID: i32 = 111;
///
/// let storage = StorageContainer::builder()
///     .cluster_id("nisshi")
///     .node_id(NODE_ID)
///     .advertised_listener(Url::parse(&format!("tcp://{HOST}:{PORT}"))?)
///     .storage(Url::parse("memory://nisshi/")?)
///     .build()
///     .await?;
///
/// let resource_name = "abcba";
///
/// let create_topic = CreateTopicsValidationLayer::new().layer(CreateTopicsService {
///     storage: storage.clone(),
/// });
///
/// let response = create_topic
///     .serve(
///         CreateTopicsRequest::default().topics(Some(
///             [CreatableTopic::default()
///                 .name(resource_name.into())
///                 .num_partitions(3)
///                 .replication_factor(1)]
///             .into(),
///         )),
///     )
///     .await?;
///
/// assert_eq!(
///     ErrorCode::None,
///     ErrorCode::try_from(response.topics.unwrap_or_default()[0].error_code)?
/// );
///
/// let config_name = "x.y.z";
/// let config_value = "pqr";
///
/// let describe_configs = DescribeConfigsService {
///     storage: storage.clone(),
/// };
///
/// let response = describe_configs
///     .serve(
///         DescribeConfigsRequest::default()
///             .include_documentation(Some(false))
///             .include_synonyms(Some(false))
///             .resources(Some(
///                 [DescribeConfigsResource::default()
///                     .resource_name(resource_name.into())
///                     .resource_type(ConfigResource::Topic.into())
///                     .configuration_keys(Some([config_name.into()].into()))]
///                 .into(),
///             )),
///     )
///     .await?;
///
/// let results = response.results.unwrap_or_default();
/// assert!(results[0].configs.as_deref().unwrap_or_default().is_empty());
///
/// let alter_configs = IncrementalAlterConfigsService {
///     storage: storage.clone(),
/// };
///
/// let _response = alter_configs
///     .serve(
///         IncrementalAlterConfigsRequest::default().resources(Some(
///             [AlterConfigsResource::default()
///                 .resource_name(resource_name.into())
///                 .resource_type(ConfigResource::Topic.into())
///                 .configs(Some(
///                     [AlterableConfig::default()
///                         .config_operation(OpType::Set.into())
///                         .name(config_name.into())
///                         .value(Some(config_value.into()))]
///                     .into(),
///                 ))]
///             .into(),
///         )),
///     )
///     .await?;
///
/// let response = describe_configs
///     .serve(
///         DescribeConfigsRequest::default()
///             .include_documentation(Some(false))
///             .include_synonyms(Some(false))
///             .resources(Some(
///                 [DescribeConfigsResource::default()
///                     .resource_name(resource_name.into())
///                     .resource_type(ConfigResource::Topic.into())
///                     .configuration_keys(Some([config_name.into()].into()))]
///                 .into(),
///             )),
///     )
///     .await?;
///
/// let results = response.results.as_deref().unwrap_or(&[]);
/// assert_eq!(1, results.len());
/// assert_eq!(resource_name, results[0].resource_name.as_str());
///
/// let configs = results[0].configs.as_deref().unwrap_or(&[]);
/// assert_eq!(1, configs.len());
/// assert_eq!(Some(config_value), configs[0].value.as_deref());
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct IncrementalAlterConfigsService<G> {
    pub storage: G,
}

impl<G> ApiKey for IncrementalAlterConfigsService<G> {
    const KEY: i16 = IncrementalAlterConfigsRequest::KEY;
}

impl<G, I> Service<I> for IncrementalAlterConfigsService<G>
where
    G: Storage,
    I: Into<RequestInput<IncrementalAlterConfigsRequest>> + Send + 'static,
{
    type Output = IncrementalAlterConfigsResponse;
    type Error = Error;

    #[instrument(skip(self, input))]
    async fn serve(&self, input: I) -> Result<Self::Output, Self::Error> {
        let input = input.into();
        let validate_only = input.request.validate_only;
        let resources = input.request.resources.unwrap_or_default();

        let mut occurrences = BTreeMap::new();
        for resource in &resources {
            *occurrences
                .entry((resource.resource_type, resource.resource_name.as_str()))
                .or_insert(0) += 1;
        }

        let duplicates: BTreeSet<_> = occurrences
            .into_iter()
            .filter_map(|(id, count)| (count > 1).then_some(id))
            .map(|(resource_type, resource_name)| (resource_type, resource_name.to_owned()))
            .collect();

        let mut responses = vec![];

        for mut resource in resources {
            let rejection =
                if duplicates.contains(&(resource.resource_type, resource.resource_name.clone())) {
                    Some((
                        ErrorCode::InvalidRequest,
                        "Each resource must appear at most once.".into(),
                    ))
                } else {
                    validate(&mut resource)
                };

            let response = match rejection {
                Some((error_code, message)) => {
                    debug!(?resource, ?error_code, message);
                    resource_response(&resource, error_code, Some(message))
                }

                None if validate_only => self.validate_existence(&resource).await,

                None => match self
                    .storage
                    .incremental_alter_resource(resource.clone())
                    .await
                {
                    Ok(response) => response,
                    Err(Error::Api(error_code)) => error_response(&resource, error_code),
                    Err(error) => {
                        warn!(
                            resource_type = resource.resource_type,
                            resource_name = resource.resource_name,
                            ?error
                        );
                        error_response(&resource, ErrorCode::UnknownServerError)
                    }
                },
            };

            responses.push(response);
        }

        Ok(IncrementalAlterConfigsResponse::default()
            .throttle_time_ms(0)
            .responses(Some(responses)))
    }
}

impl<G> IncrementalAlterConfigsService<G>
where
    G: Storage,
{
    /// Answers a `validate_only` request for `resource` without changing it:
    /// only a topic that doesn't exist fails.
    async fn validate_existence(
        &self,
        resource: &AlterConfigsResource,
    ) -> AlterConfigsResourceResponse {
        if ConfigResource::from(resource.resource_type) != ConfigResource::Topic {
            return resource_response(resource, ErrorCode::None, None);
        }

        let error_code = match self
            .storage
            .metadata(Some(&[TopicId::Name(resource.resource_name.clone())]))
            .await
        {
            Ok(metadata) => metadata
                .topics()
                .first()
                .map_or(Ok(ErrorCode::UnknownTopicOrPartition), |topic| {
                    ErrorCode::try_from(topic.error_code)
                })
                .unwrap_or(ErrorCode::UnknownServerError),

            Err(error) => {
                warn!(
                    resource_type = resource.resource_type,
                    resource_name = resource.resource_name,
                    ?error
                );
                ErrorCode::UnknownServerError
            }
        };

        error_response(resource, error_code)
    }
}

/// Returns the error for a `resource` that Kafka rejects before it reads or changes any
/// configuration, checked in the order of Kafka's [`ConfigAdminManager.preprocess`] and
/// [`incrementalAlterConfigResource`], then the value of each topic configuration, which Kafka
/// checks in [`validateAlterConfig`] before it checks that the topic exists. Each checked value
/// that `SET` stores is replaced with the number that Kafka parses.
///
/// [`ConfigAdminManager.preprocess`]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/ConfigAdminManager.scala#L105-L165
/// [`incrementalAlterConfigResource`]: https://github.com/apache/kafka/blob/3.9.1/metadata/src/main/java/org/apache/kafka/controller/ConfigurationControlManager.java#L232-L237
/// [`validateAlterConfig`]: https://github.com/apache/kafka/blob/3.9.1/metadata/src/main/java/org/apache/kafka/controller/ConfigurationControlManager.java#L298-L301
fn validate(resource: &mut AlterConfigsResource) -> Option<(ErrorCode, String)> {
    let configs = resource.configs.as_deref().unwrap_or_default();
    let is_topic = ConfigResource::from(resource.resource_type) == ConfigResource::Topic;

    let mut names = BTreeSet::new();
    if !configs
        .iter()
        .all(|config| names.insert(config.name.as_str()))
    {
        return Some((
            ErrorCode::InvalidRequest,
            "Error due to duplicate config keys".into(),
        ));
    }

    let null_updates: Vec<&str> = configs
        .iter()
        .filter(|config| {
            config.config_operation != i8::from(OpType::Delete) && config.value.is_none()
        })
        .map(|config| config.name.as_str())
        .collect();

    if !null_updates.is_empty() {
        return Some((
            ErrorCode::InvalidRequest,
            format!("Null value not supported for : {}", null_updates.join(", ")),
        ));
    }

    for config in resource.configs.iter_mut().flatten() {
        // Kafka rejects an unknown operation with INVALID_REQUEST only on a
        // broker resource ([ConfigAdminManager.scala#L176-L179]). On a
        // topic resource its controller looks the operation up as null
        // ([ControllerApis.scala#L745]). We reject an unknown operation
        // with INVALID_REQUEST on every resource, because the request is
        // malformed whatever the resource.
        //
        // [ConfigAdminManager.scala#L176-L179]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/ConfigAdminManager.scala#L176-L179
        // [ControllerApis.scala#L745]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/ControllerApis.scala#L745
        let Ok(op) = OpType::try_from(config.config_operation) else {
            return Some((
                ErrorCode::InvalidRequest,
                format!("Unknown operations type {}", config.config_operation),
            ));
        };

        if is_topic
            && matches!(op, OpType::Append | OpType::Subtract)
            && !is_list_config(ConfigResource::Topic, &config.name)
        {
            let op = if op == OpType::Append {
                "APPEND"
            } else {
                "SUBTRACT"
            };

            return Some((
                ErrorCode::InvalidConfig,
                format!(
                    "Can't {op} to key {} because its type is not LIST.",
                    config.name
                ),
            ));
        }

        if ConfigResource::from(resource.resource_type) == ConfigResource::Topic
            && op == OpType::Set
            && config.name == max_message_bytes::KEY
            && let Some(Err(reason)) = config.value.as_deref().map(max_message_bytes::parse)
        {
            return Some((ErrorCode::InvalidConfig, reason));
        }

        if is_topic && op == OpType::Set {
            match validate_topic_config(&config.name, config.value.as_deref()) {
                Ok(value) => config.value = value,
                Err(message) => return Some((ErrorCode::InvalidConfig, message)),
            }
        }
    }

    None
}

/// Returns the response for a `resource` that failed with `error_code`, with
/// the message that Kafka sends for that error, if any.
fn error_response(
    resource: &AlterConfigsResource,
    error_code: ErrorCode,
) -> AlterConfigsResourceResponse {
    // This is the message that Kafka sends for a missing topic ([QuorumController.java#L488-L489]).
    //
    // [QuorumController.java#L488-L489]: https://github.com/apache/kafka/blob/3.9.1/metadata/src/main/java/org/apache/kafka/controller/QuorumController.java#L488-L489
    let message = (error_code == ErrorCode::UnknownTopicOrPartition
        && ConfigResource::from(resource.resource_type) == ConfigResource::Topic)
        .then(|| format!("The topic '{}' does not exist.", resource.resource_name));

    resource_response(resource, error_code, message)
}

fn resource_response(
    resource: &AlterConfigsResource,
    error_code: ErrorCode,
    error_message: Option<String>,
) -> AlterConfigsResourceResponse {
    AlterConfigsResourceResponse::default()
        .error_code(error_code.into())
        .error_message(error_message)
        .resource_type(resource.resource_type)
        .resource_name(resource.resource_name.clone())
}
