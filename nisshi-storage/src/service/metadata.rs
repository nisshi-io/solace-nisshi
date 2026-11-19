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

use std::collections::BTreeSet;

use nisshi_sans_io::{
    ApiKey, CreateTopicsRequest, ErrorCode, MetadataRequest, MetadataResponse, NULL_TOPIC_ID,
    RequestInput,
    create_topics_request::CreatableTopic,
    create_topics_response::CreatableTopicResult,
    metadata_request::MetadataRequestTopic,
    metadata_response::MetadataResponseTopic,
    topic::{is_internal, is_valid_topic_name},
};
use rama::Service;
use tracing::{debug, error, info, instrument, warn};

pub use auto_create_refusals::AutoCreateRefusals;

use super::{
    create_topics::validation::ValidatedCreateTopicsRequest, fetch::storage_failure_warns_now,
};
use crate::{
    CreateTopicsService, Error, MAX_PARTITIONS_PER_REQUEST, Result, Storage, StorageOperation,
    TopicCreationConfig, TopicId, authorized_operations, topic_creation,
};

mod auto_create_refusals;

/// A [`Service`] using its [`Storage`] taking [`MetadataRequest`] returning [`MetadataResponse`].
/// ```no_run
/// use rama::Service;
/// use nisshi_sans_io::MetadataRequest;
/// use nisshi_storage::{
///     AutoCreateRefusals, Error, MetadataService, StorageContainer, TopicCreationConfig,
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
/// let service = MetadataService {
///     storage,
///     topic_creation: TopicCreationConfig::default(),
///     auto_create_refusals: AutoCreateRefusals::default(),
/// };
///
/// let response = service
///     .serve(
///         MetadataRequest::default()
///             .allow_auto_topic_creation(Some(false))
///             .include_cluster_authorized_operations(Some(false))
///             .include_topic_authorized_operations(Some(false))
///             .topics(Some([].into())),
///     )
///     .await?;
///
/// let brokers = response.brokers.as_deref().unwrap_or_default();
/// assert_eq!(1, brokers.len());
/// assert_eq!(HOST, brokers[0].host);
/// assert_eq!(PORT, brokers[0].port);
/// assert_eq!(NODE_ID, brokers[0].node_id);
/// assert!(brokers[0].rack.is_none());
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct MetadataService<G> {
    pub storage: G,
    pub topic_creation: TopicCreationConfig,

    /// The names that auto-creation refused recently. Give each service of a broker a clone of
    /// one store, so that a refusal on one connection counts on every connection.
    pub auto_create_refusals: AutoCreateRefusals,
}

impl<G> MetadataService<G>
where
    G: Storage,
{
    /// Reads the metadata of `topics` from storage. When the read fails and the request names
    /// topics, it answers each named topic with the code of the error, and keeps the brokers.
    ///
    /// A failed read does not close the connection, because the client sends its other requests
    /// on it. A retriable code makes the Java client request the metadata again, as it does for
    /// any `InvalidMetadataException`
    /// ([`Metadata.handleMetadataResponse`]). The brokers come from a read of no topics, which
    /// reads no topic rows. If that read fails too, this returns the original error.
    ///
    /// A request that names no topics asks for every topic, and this returns the original error
    /// for it. An answer of the brokers with an empty topic list would tell the client that no
    /// topic exists.
    ///
    /// [`Metadata.handleMetadataResponse`]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/clients/Metadata.java#L530-L533
    async fn read_metadata(&self, topics: Option<&[TopicId]>) -> Result<crate::MetadataResponse> {
        let error = match self.storage.metadata(topics).await {
            Ok(response) => return Ok(response),
            Err(error) => error,
        };

        let named = topics.filter(|topics| !topics.is_empty());

        let brokers = match named {
            Some(_) => self.storage.metadata(Some(&[])).await,
            None => Err(error.clone()),
        };

        let (Some(named), Ok(mut response)) = (named, brokers) else {
            error!(?error);
            return Err(error);
        };

        let error_code = error.error_code(ErrorCode::KafkaStorageError);

        if error_code == ErrorCode::KafkaStorageError {
            if storage_failure_warns_now(StorageOperation::Metadata) {
                warn!(topics = named.len(), ?error, "metadata read failed");
            } else {
                debug!(topics = named.len(), ?error, "metadata read failed");
            }
        } else {
            error!(
                topics = named.len(),
                ?error,
                ?error_code,
                "metadata read failed"
            );
        }

        response.topics = named
            .iter()
            .map(|topic| {
                let (name, topic_id) = match topic {
                    TopicId::Name(name) => (name.as_str(), NULL_TOPIC_ID),
                    TopicId::Id(id) => ("", id.into_bytes()),
                };

                MetadataResponseTopic::default()
                    .error_code(error_code.into())
                    .name(Some(name.into()))
                    .topic_id(Some(topic_id))
                    .is_internal(Some(false))
                    .partitions(Some([].into()))
                    .topic_authorized_operations(Some(-2147483648))
            })
            .collect();

        Ok(response)
    }
}

impl<G> ApiKey for MetadataService<G> {
    const KEY: i16 = MetadataRequest::KEY;
}

impl<G, I> Service<I> for MetadataService<G>
where
    G: Storage + Clone,
    I: Into<RequestInput<MetadataRequest>> + Send + 'static,
{
    type Output = MetadataResponse;
    type Error = Error;

    #[instrument(skip(self, input))]
    async fn serve(&self, input: I) -> Result<Self::Output, Self::Error> {
        let input = input.into();

        let topics = input.request.topics.as_deref().map(requested_topics);

        // An empty list asks for no topics, and null for all of them
        // ([`MetadataRequest.isAllTopics`]). The v0 rule, where an empty list means all of them,
        // needs the API version, so `MetadataTopicIdLayer` applies it before the request gets here.
        // We answer no topics whatever the storage lists, since a backend that lists every topic
        // for an empty slice would otherwise send them all to a client that only wants the brokers.
        //
        // [`MetadataRequest.isAllTopics`]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/common/requests/MetadataRequest.java#L162-L165
        let no_topics = topics.as_deref().is_some_and(<[TopicId]>::is_empty);

        let mut response = self.read_metadata(topics.as_deref()).await?;

        // versions prior to 4 do not have allow_auto_topic_creation,
        // and behave as if it were true
        if input.request.allow_auto_topic_creation.unwrap_or(true)
            && self.topic_creation.auto_create_topics_enable
        {
            let unknown = response
                .topics()
                .iter()
                .filter(|topic| topic.error_code == i16::from(ErrorCode::UnknownTopicOrPartition))
                .filter_map(|topic| topic.name.clone())
                .filter(|name| is_valid_topic_name(name))
                .filter(|name| {
                    let creatable = topic_creation::auto_creatable(name);
                    if !creatable {
                        debug!(topic = name, "not auto-creating an internal topic name");
                    }
                    creatable
                })
                .filter(|name| {
                    let refused = self.auto_create_refusals.is_refused(name);
                    if refused {
                        debug!(topic = name, "not auto-creating a recently refused topic");
                    }
                    !refused
                })
                .collect::<BTreeSet<_>>();

            // We create the unknown topics with one `CreateTopics` that asks for the broker
            // defaults, so they get the checks of any other `CreateTopics`, and we create none
            // of them when together they exceed the cap, instead of creating as many as fit.
            // Kafka sends every unknown name of one request to the controller as one
            // `CreateTopics` ([`sendCreateTopicRequest`]), and the controller rejects all of it
            // over the cap ([`validateTotalNumberOfPartitions`]). So each name stays
            // `UNKNOWN_TOPIC_OR_PARTITION`.
            //
            // [`sendCreateTopicRequest`]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/AutoTopicCreationManager.scala#L158-L224
            // [`validateTotalNumberOfPartitions`]: https://github.com/apache/kafka/blob/3.9.1/metadata/src/main/java/org/apache/kafka/controller/ReplicationControlManager.java#L1152-L1169
            let request = CreateTopicsRequest::default()
                .validate_only(Some(false))
                .topics(Some(
                    unknown
                        .into_iter()
                        .map(|name| {
                            CreatableTopic::default()
                                .name(name)
                                .num_partitions(-1)
                                .replication_factor(-1)
                                .assignments(Some([].into()))
                                .configs(Some([].into()))
                        })
                        .collect(),
                ));

            let results = match ValidatedCreateTopicsRequest::new(
                request,
                self.topic_creation.num_partitions,
            ) {
                Ok(validated) => CreateTopicsService {
                    storage: self.storage.clone(),
                }
                .serve(validated)
                .await
                .inspect_err(|err| error!(?err))?
                .topics
                .unwrap_or_default(),

                Err(malformed) => {
                    warn!(
                        requested_partitions = malformed.total,
                        num_partitions = self.topic_creation.num_partitions,
                        limit = MAX_PARTITIONS_PER_REQUEST,
                        "not auto-creating topics: too many partitions in one request"
                    );
                    Vec::new()
                }
            };

            let mut created = false;

            for result in &results {
                // `unknown` holds only valid names, so `CreateTopics` refuses one of them with
                // INVALID_TOPIC_EXCEPTION only for colliding with an existing topic.
                if result.error_code == i16::from(ErrorCode::InvalidTopicException) {
                    self.auto_create_refusals.refuse(&result.name);
                }

                created |= topic_exists_after(result);
            }

            if created {
                response = self.read_metadata(topics.as_deref()).await?;
            }
        }
        // The zero ID is left out: Kafka treats it as no ID at all.
        let requested_ids = topics
            .iter()
            .flatten()
            .filter_map(|topic| match topic {
                TopicId::Id(id) if id.into_bytes() != NULL_TOPIC_ID => Some(id.into_bytes()),
                _ => None,
            })
            .collect::<BTreeSet<_>>();

        let brokers = Some(response.brokers().to_owned());
        let cluster_id = response.cluster().map(|s| s.into());
        let controller_id = response.controller();
        let mut topics = response
            .topics()
            .iter()
            .filter(|_| !no_topics)
            .map(|topic| {
                let topic = if topic.error_code == i16::from(ErrorCode::UnknownTopicOrPartition)
                    && topic
                        .name
                        .as_deref()
                        .is_some_and(|name| !is_valid_topic_name(name))
                {
                    topic
                        .clone()
                        .error_code(ErrorCode::InvalidTopicException.into())
                } else if topic.error_code == i16::from(ErrorCode::UnknownTopicOrPartition)
                    && topic.name.is_none()
                    && topic
                        .topic_id
                        .is_some_and(|topic_id| requested_ids.contains(&topic_id))
                {
                    // Kafka picks the code in `KafkaApis` from whether the ID resolved to
                    // a name, so the choice sits here and not in each storage engine,
                    // which answers a miss the same for an ID and a name. The row's shape
                    // alone does not say how the topic was asked for, so the request does.
                    //
                    // https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/KafkaApis.scala#L1336-L1340
                    topic.clone().error_code(ErrorCode::UnknownTopicId.into())
                } else {
                    topic.clone()
                };

                let operations = authorized_operations::topic(
                    input.request.include_topic_authorized_operations,
                    topic.name.as_deref(),
                );

                // Kafka answers `is_internal` from the name alone, for a known topic
                // ([`KRaftMetadataCache.getTopicMetadata`]) and for an unknown name
                // ([`KafkaApis.getTopicMetadata`]), and false for a row without a name
                // ([`handleTopicMetadataRequest`]), so the service sets it here and no engine
                // has to know the names.
                //
                // [`KRaftMetadataCache.getTopicMetadata`]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/metadata/KRaftMetadataCache.scala#L256
                // [`KafkaApis.getTopicMetadata`]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/KafkaApis.scala#L1306
                // [`handleTopicMetadataRequest`]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/KafkaApis.scala#L1339-L1340
                let internal = topic.name.as_deref().is_some_and(is_internal);

                topic
                    .is_internal(Some(internal))
                    .topic_authorized_operations(Some(operations))
            })
            .collect::<Vec<_>>();

        // Kafka answers the unknown IDs before the other topics
        // ([`KafkaApis.handleTopicMetadataRequest`]). The sort is stable, so the other topics
        // keep the order that the storage answered them in.
        //
        // [`KafkaApis.handleTopicMetadataRequest`]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/KafkaApis.scala#L1419-L1420
        topics.sort_by_key(|topic| topic.error_code != i16::from(ErrorCode::UnknownTopicId));

        let topics = Some(topics);
        let cluster_authorized_operations = Some(authorized_operations::cluster(
            input.request.include_cluster_authorized_operations,
        ));

        let throttle_time_ms = Some(0);

        Ok(MetadataResponse::default()
            .throttle_time_ms(throttle_time_ms)
            .brokers(brokers)
            .cluster_id(cluster_id)
            .controller_id(controller_id)
            .topics(topics)
            .cluster_authorized_operations(cluster_authorized_operations))
    }
}

/// The topics that a Metadata request for `topics` asks the storage for.
///
/// When an entry carries a topic ID other than the zero one, the request asks only by its IDs,
/// each once, and its names are dropped, also the name of an entry that carries an ID, as Kafka
/// reads it ([`KafkaApis.handleTopicMetadataRequest`]). Otherwise each entry asks by its name,
/// and a name asked for twice is asked for once.
///
/// [`KafkaApis.handleTopicMetadataRequest`]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/KafkaApis.scala#L1331-L1347
fn requested_topics(topics: &[MetadataRequestTopic]) -> Vec<TopicId> {
    let mut seen = BTreeSet::new();

    let ids = topics
        .iter()
        .filter_map(|topic| topic.topic_id)
        .filter(|id| *id != NULL_TOPIC_ID && seen.insert(*id))
        .map(TopicId::from)
        .collect::<Vec<_>>();

    if ids.is_empty() {
        let mut asked = BTreeSet::new();

        topics
            .iter()
            .map(TopicId::from)
            .filter(|topic| asked.insert(topic.clone()))
            .collect()
    } else {
        ids
    }
}

/// Logs the result of auto-creating one topic, and returns whether the topic exists afterwards.
fn topic_exists_after(result: &CreatableTopicResult) -> bool {
    match ErrorCode::try_from(result.error_code) {
        Ok(ErrorCode::None) => {
            info!(topic = %result.name, num_partitions = ?result.num_partitions, "auto-created topic");
            true
        }

        // concurrent metadata requests can race to create
        Ok(ErrorCode::TopicAlreadyExists) => true,

        // A client sends a colliding name again on each retry, so we log the refusal at debug,
        // instead of at warn on every retry.
        Ok(ErrorCode::InvalidTopicException) => {
            debug!(?result, "not auto-creating topic");
            false
        }

        // Any other failure is the broker's, such as a storage error, and the producer sees
        // only `UNKNOWN_TOPIC_OR_PARTITION`, so we log it where an operator sees it.
        _ => {
            warn!(
                topic = %result.name,
                error_code = result.error_code,
                error_message = ?result.error_message,
                "auto-creating topic failed"
            );
            false
        }
    }
}

#[cfg(test)]
mod tests;
