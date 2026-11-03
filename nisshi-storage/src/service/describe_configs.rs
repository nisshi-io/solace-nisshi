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

use nisshi_sans_io::{
    ApiKey, ConfigResource, DescribeConfigsRequest, DescribeConfigsResponse, ErrorCode,
    RequestInput, describe_configs_request::DescribeConfigsResource,
    describe_configs_response::DescribeConfigsResult,
};
use rama::Service;
use tracing::{error, instrument};

use crate::{Error, Result, Storage, topic_defaults};

/// A [`Service`] using its [`Storage`] taking [`DescribeConfigsRequest`] returning [`DescribeConfigsResponse`].
///
/// For a topic, it lists every Kafka topic config, as Kafka does: each config that the topic sets,
/// as [`Storage::describe_config`] returns it, and each other config with its Kafka default.
/// ```no_run
/// use rama::Service as _;
/// use nisshi_sans_io::{ConfigResource, DescribeConfigsRequest,
///     EndpointType, ErrorCode, describe_configs_request::DescribeConfigsResource};
/// use nisshi_storage::{DescribeConfigsService, Error, StorageContainer};
/// use url::Url;
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Error> {
/// let storage = StorageContainer::builder()
///     .cluster_id("nisshi")
///     .node_id(111)
///     .advertised_listener(Url::parse("tcp://localhost:9092")?)
///     .storage(Url::parse("memory://nisshi/")?)
///     .build()
///     .await?;
///
/// let service = DescribeConfigsService { storage };
///
/// let response = service
///     .serve(
///         DescribeConfigsRequest::default()
///             .include_documentation(Some(false))
///             .include_synonyms(Some(false))
///             .resources(Some(
///                 [DescribeConfigsResource::default()
///                     .resource_name("abcba".into())
///                     .resource_type(ConfigResource::Topic.into())
///                     .configuration_keys(Some([].into()))]
///                 .into(),
///             )),
///     )
///     .await?;
///
/// let results = response.results.unwrap_or_default();
/// assert_eq!(1, results.len());
/// assert_eq!(
///     ErrorCode::UnknownTopicOrPartition,
///     ErrorCode::try_from(results[0].error_code)?
/// );
/// assert!(results[0].configs.as_deref().unwrap_or_default().is_empty());
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct DescribeConfigsService<G> {
    pub storage: G,
}

impl<G> ApiKey for DescribeConfigsService<G> {
    const KEY: i16 = DescribeConfigsRequest::KEY;
}

impl<G, I> Service<I> for DescribeConfigsService<G>
where
    G: Storage,
    I: Into<RequestInput<DescribeConfigsRequest>> + Send + 'static,
{
    type Output = DescribeConfigsResponse;
    type Error = Error;

    #[instrument(skip(self, input))]
    async fn serve(&self, input: I) -> Result<Self::Output, Self::Error> {
        let input = input.into();
        let mut results = vec![];

        for resource in input.request.resources.unwrap_or_default() {
            let result = match ConfigResource::from(resource.resource_type) {
                ConfigResource::Broker => self.describe_broker(&resource.resource_name).await,
                ConfigResource::Topic => {
                    self.describe_topic(
                        &resource.resource_name,
                        resource.configuration_keys.as_deref(),
                    )
                    .await
                }
                resource_type => {
                    self.storage
                        .describe_config(
                            resource.resource_name.as_str(),
                            resource_type,
                            resource.configuration_keys.as_deref(),
                        )
                        .await
                }
            };

            results.push(result.unwrap_or_else(|error| {
                error!(?error, resource.resource_name);
                error_result(&resource, &error)
            }));
        }

        Ok(DescribeConfigsResponse::default().results(Some(results)))
    }
}

/// Returns the result for a `resource` whose describe failed with `error`, as Kafka 3.9.1 does
/// ([ConfigHelper.scala#L156-L170]): the error's code, no error message, and an empty config
/// list. The other resources in the request still get an answer.
///
/// [`Error::Api`] answers its own code, and any other error answers `UNKNOWN_SERVER_ERROR`.
/// Kafka's [`ApiError.fromThrowable`] sends no message for `UNKNOWN_SERVER_ERROR`, so that it
/// does not leak server details, or for an error whose message is the code's own description.
///
/// [ConfigHelper.scala#L156-L170]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/ConfigHelper.scala#L156-L170
/// [`ApiError.fromThrowable`]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/common/requests/ApiError.java#L38-L46
fn error_result(resource: &DescribeConfigsResource, error: &Error) -> DescribeConfigsResult {
    let error_code = match error {
        Error::Api(error_code) => *error_code,
        _ => ErrorCode::UnknownServerError,
    };

    DescribeConfigsResult::default()
        .error_code(error_code.into())
        .error_message(None)
        .resource_name(resource.resource_name.clone())
        .resource_type(resource.resource_type)
        .configs(Some(vec![]))
}

impl<G> DescribeConfigsService<G>
where
    G: Storage,
{
    /// Describes the topic `name`, as Kafka 3.9.1 does: the configs that `keys` selects from the
    /// configs that the topic sets and Kafka's defaults for the rest.
    ///
    /// The defaults are added here, not in [`Storage::describe_config`], because the broker reads
    /// a topic's configs through that method too, and treats a config that the topic doesn't set
    /// as Nisshi's own default.
    async fn describe_topic(
        &self,
        name: &str,
        keys: Option<&[String]>,
    ) -> Result<DescribeConfigsResult> {
        let mut result = self
            .storage
            .describe_config(name, ConfigResource::Topic, keys)
            .await?;
        topic_defaults::add_to(&mut result, keys);
        Ok(result)
    }

    /// Describes the broker resource `name`, as Kafka 3.9.1 does
    /// ([ConfigHelper.scala#L113-L121]). It compares `name` with this broker's node id, and
    /// does not look it up as a topic.
    ///
    /// An empty name selects the dynamic defaults for every broker, and our node id selects
    /// this broker. Both get an empty config list. Nisshi takes its settings from its command
    /// line, not from Kafka broker configs, and every engine discards an alter of a broker
    /// config, so it has no broker config to report. For our node id, Kafka lists every config
    /// of the broker, so `kafka-configs --describe --all` prints them there. Without `--all`,
    /// the tool prints only dynamic configs, so it prints what Kafka prints for a broker that
    /// has none.
    ///
    /// Any other name gets `INVALID_REQUEST` with Kafka's message, in this resource's result
    /// only. The other resources in the request still get an answer.
    ///
    /// [ConfigHelper.scala#L113-L121]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/ConfigHelper.scala#L113-L121
    async fn describe_broker(&self, name: &str) -> Result<DescribeConfigsResult> {
        let result = DescribeConfigsResult::default()
            .resource_name(name.into())
            .resource_type(ConfigResource::Broker.into())
            .configs(Some(vec![]));

        if name.is_empty() {
            return Ok(result
                .error_code(ErrorCode::None.into())
                .error_message(None));
        }

        let node = self.storage.node().await?;

        let error_message = match name.parse::<i32>() {
            Ok(id) if id == node => {
                return Ok(result
                    .error_code(ErrorCode::None.into())
                    .error_message(None));
            }
            Ok(_) => format!(
                "Unexpected broker id, expected {node} or empty string, but received {name}"
            ),
            Err(_) => format!("Broker id must be an integer, but it is: {name}"),
        };

        Ok(result
            .error_code(ErrorCode::InvalidRequest.into())
            .error_message(Some(error_message)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn topic_resource() -> DescribeConfigsResource {
        DescribeConfigsResource::default()
            .resource_name("abc".into())
            .resource_type(ConfigResource::Topic.into())
    }

    #[test]
    fn api_error_answers_its_code() {
        assert_eq!(
            DescribeConfigsResult::default()
                .error_code(ErrorCode::KafkaStorageError.into())
                .error_message(None)
                .resource_name("abc".into())
                .resource_type(ConfigResource::Topic.into())
                .configs(Some(vec![])),
            error_result(&topic_resource(), &Error::Api(ErrorCode::KafkaStorageError))
        );
    }

    #[test]
    fn other_error_answers_unknown_server_error() {
        assert_eq!(
            DescribeConfigsResult::default()
                .error_code(ErrorCode::UnknownServerError.into())
                .error_message(None)
                .resource_name("abc".into())
                .resource_type(ConfigResource::Topic.into())
                .configs(Some(vec![])),
            error_result(&topic_resource(), &Error::Message("broken".into()))
        );
    }
}
