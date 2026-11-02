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

use std::collections::BTreeSet;

use nisshi_sans_io::{
    ApiKey, CreateTopicsRequest, ErrorCode, MetadataRequest, MetadataResponse, RequestInput,
    create_topics_request::CreatableTopic, create_topics_response::CreatableTopicResult,
    topic::is_valid_topic_name,
};
use rama::Service;
use tracing::{debug, error, info, instrument, warn};

pub use auto_create_refusals::AutoCreateRefusals;

use super::create_topics::validation::ValidatedCreateTopicsRequest;
use crate::{
    CreateTopicsService, Error, MAX_PARTITIONS_PER_REQUEST, Result, Storage, TopicCreationConfig,
    TopicId,
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

        let topics = input
            .request
            .topics
            .map(|topics| topics.iter().map(TopicId::from).collect::<Vec<_>>());

        let mut response = self
            .storage
            .metadata(topics.as_deref())
            .await
            .inspect_err(|err| error!(?err))?;

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
                response = self
                    .storage
                    .metadata(topics.as_deref())
                    .await
                    .inspect_err(|err| error!(?err))?;
            }
        }
        let brokers = Some(response.brokers().to_owned());
        let cluster_id = response.cluster().map(|s| s.into());
        let controller_id = response.controller();
        let topics = Some(
            response
                .topics()
                .iter()
                .map(|topic| {
                    if topic.error_code == i16::from(ErrorCode::UnknownTopicOrPartition)
                        && topic
                            .name
                            .as_deref()
                            .is_some_and(|name| !is_valid_topic_name(name))
                    {
                        topic
                            .clone()
                            .error_code(ErrorCode::InvalidTopicException.into())
                    } else {
                        topic.clone()
                    }
                })
                .collect::<Vec<_>>(),
        );
        let cluster_authorized_operations = Some(-1);

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
mod tests {
    use std::{
        io,
        sync::{Arc, Mutex},
    };

    use nisshi_sans_io::{
        ErrorCode, MetadataRequest, create_topics_response::CreatableTopicResult,
        metadata_request::MetadataRequestTopic,
    };
    use rama::Service as _;
    use tokio::time::advance;
    use tracing::Level;

    use super::{
        AutoCreateRefusals, MetadataService, auto_create_refusals::REFUSAL_TTL, topic_exists_after,
    };
    use crate::{
        Result, TopicCreationConfig,
        service::counting_storage::{CountingStorage, Listing},
    };

    fn service(
        storage: &CountingStorage,
        refusals: &AutoCreateRefusals,
    ) -> MetadataService<CountingStorage> {
        MetadataService {
            storage: storage.clone(),
            topic_creation: TopicCreationConfig::default(),
            auto_create_refusals: refusals.clone(),
        }
    }

    /// Sends a Metadata request that allows auto-creation for `names`, and returns the error
    /// code of each topic in the response.
    async fn metadata(
        service: &MetadataService<CountingStorage>,
        names: &[&str],
    ) -> Result<Vec<(String, ErrorCode)>> {
        let response = service
            .serve(
                MetadataRequest::default()
                    .allow_auto_topic_creation(Some(true))
                    .topics(Some(
                        names
                            .iter()
                            .map(|name| MetadataRequestTopic::default().name(Some((*name).into())))
                            .collect(),
                    )),
            )
            .await?;

        response
            .topics
            .unwrap_or_default()
            .into_iter()
            .map(|topic| {
                Ok((
                    topic.name.unwrap_or_default(),
                    ErrorCode::try_from(topic.error_code)?,
                ))
            })
            .collect()
    }

    fn unknown(name: &str) -> (String, ErrorCode) {
        (name.to_owned(), ErrorCode::UnknownTopicOrPartition)
    }

    /// A producer retries Metadata for a name that collides with an existing topic. Only the
    /// first request lists every topic for the collision check. Each request still looks up its
    /// names once.
    #[tokio::test(start_paused = true)]
    async fn a_retried_colliding_name_lists_topics_once() -> Result<()> {
        let storage = CountingStorage::new(Listing::Topics, &["a.b"]);
        let service = service(&storage, &AutoCreateRefusals::default());

        for _ in 0..5 {
            assert_eq!(vec![unknown("a_b")], metadata(&service, &["a_b"]).await?);
        }

        assert_eq!(1, storage.listings());
        assert_eq!(5, storage.lookups());
        assert!(storage.created().is_empty());
        Ok(())
    }

    /// The broker gives the service of each connection a clone of one store, so a refusal on
    /// one connection saves the listing on another.
    #[tokio::test(start_paused = true)]
    async fn a_refusal_is_shared_by_clones() -> Result<()> {
        let storage = CountingStorage::new(Listing::Topics, &["a.b"]);
        let refusals = AutoCreateRefusals::default();

        for service in [service(&storage, &refusals), service(&storage, &refusals)] {
            assert_eq!(vec![unknown("a_b")], metadata(&service, &["a_b"]).await?);
        }

        assert_eq!(1, storage.listings());
        Ok(())
    }

    /// Once the colliding topic is deleted, the name is created when its refusal expires, and
    /// not before.
    #[tokio::test(start_paused = true)]
    async fn a_refused_name_is_created_once_its_refusal_expires() -> Result<()> {
        let storage = CountingStorage::new(Listing::Topics, &["a.b"]);
        let service = service(&storage, &AutoCreateRefusals::default());

        assert_eq!(vec![unknown("a_b")], metadata(&service, &["a_b"]).await?);
        storage.delete("a.b");

        assert_eq!(vec![unknown("a_b")], metadata(&service, &["a_b"]).await?);
        assert!(storage.created().is_empty());
        assert_eq!(1, storage.listings());

        advance(REFUSAL_TTL).await;

        assert_eq!(
            vec![(String::from("a_b"), ErrorCode::None)],
            metadata(&service, &["a_b"]).await?
        );
        assert_eq!(vec![String::from("a_b")], storage.created());
        assert_eq!(2, storage.listings());
        Ok(())
    }

    /// A failed listing is the broker's failure, not a refusal, so the next request tries again.
    #[tokio::test(start_paused = true)]
    async fn a_failed_listing_is_not_remembered() -> Result<()> {
        for listing in [Listing::Error, Listing::NamelessTopic] {
            let storage = CountingStorage::new(listing, &[]);
            let service = service(&storage, &AutoCreateRefusals::default());

            for _ in 0..2 {
                assert_eq!(vec![unknown("a_b")], metadata(&service, &["a_b"]).await?);
            }

            assert_eq!(2, storage.listings(), "{listing:?}");
        }

        Ok(())
    }

    /// A refused name is left out of auto-creation, and the other names of the request are
    /// still created.
    #[tokio::test(start_paused = true)]
    async fn a_refused_name_does_not_block_others() -> Result<()> {
        let storage = CountingStorage::new(Listing::Topics, &["a.b"]);
        let service = service(&storage, &AutoCreateRefusals::default());

        assert_eq!(vec![unknown("a_b")], metadata(&service, &["a_b"]).await?);

        assert_eq!(
            vec![unknown("a_b"), (String::from("c_d"), ErrorCode::None)],
            metadata(&service, &["a_b", "c_d"]).await?
        );
        assert_eq!(vec![String::from("c_d")], storage.created());
        Ok(())
    }

    /// The log lines written at `warn` or above.
    #[derive(Clone, Default)]
    struct WarnLog(Arc<Mutex<Vec<u8>>>);

    impl io::Write for WarnLog {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().expect("log").extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    /// Returns whether the topic exists after `error_code`, and what was logged at `warn` or
    /// above.
    fn outcome(error_code: ErrorCode) -> (bool, String) {
        let log = WarnLog::default();
        let writer = log.clone();

        let exists = tracing::subscriber::with_default(
            tracing_subscriber::fmt()
                .with_max_level(Level::WARN)
                .with_ansi(false)
                .with_writer(move || writer.clone())
                .finish(),
            || {
                topic_exists_after(
                    &CreatableTopicResult::default()
                        .name("orders".into())
                        .error_code(error_code.into()),
                )
            },
        );

        let text = String::from_utf8(log.0.lock().expect("log").clone()).expect("utf-8");
        (exists, text)
    }

    /// A storage failure is the broker's problem, not the client's, so the operator sees it at
    /// the default log level. The producer only sees `UNKNOWN_TOPIC_OR_PARTITION`.
    #[test]
    fn a_storage_failure_is_logged_at_warn() {
        for error_code in [ErrorCode::UnknownServerError, ErrorCode::KafkaStorageError] {
            let (exists, log) = outcome(error_code);

            assert!(!exists, "{error_code:?}");
            assert!(log.contains("orders"), "{error_code:?}: {log:?}");
        }
    }

    /// A client sends a colliding name again on each retry, so the refusal stays below `warn`.
    #[test]
    fn a_refused_name_is_not_logged_at_warn() {
        let (exists, log) = outcome(ErrorCode::InvalidTopicException);

        assert!(!exists);
        assert_eq!("", log);
    }

    #[test]
    fn a_created_or_existing_topic_exists() {
        for error_code in [ErrorCode::None, ErrorCode::TopicAlreadyExists] {
            assert!(outcome(error_code).0, "{error_code:?}");
        }
    }
}
