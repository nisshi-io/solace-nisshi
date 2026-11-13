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

use nisshi_sans_io::{
    ApiKey, DeleteTopicsRequest, DeleteTopicsResponse, ErrorCode, NULL_TOPIC_ID, RequestInput,
    delete_topics_response::DeletableTopicResult,
};
use std::time::Duration;

use rama::Service;
use tokio::time::Instant;
use tracing::{instrument, warn};

use crate::{Error, Result, Storage, TopicId};

/// A [`Service`] using its [`Storage`] taking [`DeleteTopicsRequest`] returning [`DeleteTopicsResponse`].
/// ```no_run
/// use rama::Service as _;
/// use nisshi_sans_io::{DeleteTopicsRequest, DeleteTopicsResponse,
///     delete_topics_response::DeletableTopicResult, ErrorCode};
/// use nisshi_storage::{DeleteTopicsService, Error, StorageContainer};
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
/// let service = DeleteTopicsService { storage };
///
/// let topic = "pqr";
///
/// let error_code = ErrorCode::UnknownTopicOrPartition;
///
/// assert_eq!(
///     DeleteTopicsResponse::default()
///         .throttle_time_ms(Some(0))
///         .responses(Some(vec![
///             DeletableTopicResult::default()
///                 .error_code(error_code.into())
///                 .error_message(Some(error_code.to_string()))
///                 .name(Some(topic.into())),
///         ])),
///     service
///         .serve(
///             DeleteTopicsRequest::default().topic_names(Some(vec![topic.into()]))
///         )
///         .await?
/// );
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct DeleteTopicsService<G> {
    pub storage: G,
}

impl<G> ApiKey for DeleteTopicsService<G> {
    const KEY: i16 = DeleteTopicsRequest::KEY;
}

impl<G, I> Service<I> for DeleteTopicsService<G>
where
    G: Storage,
    I: Into<RequestInput<DeleteTopicsRequest>> + Send + 'static,
{
    type Output = DeleteTopicsResponse;
    type Error = Error;

    #[instrument(skip(self, input))]
    async fn serve(&self, input: I) -> Result<Self::Output, Self::Error> {
        let input = input.into();

        let mut budget = Budget::new(input.request.timeout_ms);
        let mut responses = vec![];

        for topic in input.request.topics.unwrap_or_default() {
            let topic_id = TopicId::from(topic.clone());
            let (error_code, error_message) = self.delete(&topic_id, &mut budget).await;

            responses.push(
                DeletableTopicResult::default()
                    .name(topic.name.clone())
                    .topic_id(Some(topic.topic_id))
                    .error_code(i16::from(error_code))
                    .error_message(error_message),
            );
        }

        for topic in input.request.topic_names.unwrap_or_default() {
            let (error_code, error_message) = self.delete(&topic.clone().into(), &mut budget).await;

            responses.push(
                DeletableTopicResult::default()
                    .name(Some(topic))
                    .topic_id(None)
                    .error_code(i16::from(error_code))
                    .error_message(error_message),
            );
        }

        Ok(DeleteTopicsResponse::default()
            .throttle_time_ms(Some(0))
            .responses(Some(responses)))
    }
}

impl<G> DeleteTopicsService<G>
where
    G: Storage,
{
    /// Deletes `topic_id` and returns the code and message that its result carries. A storage
    /// error does not leave the service: it becomes the topic's own answer, and the caller goes
    /// on to the next topic.
    async fn delete(&self, topic_id: &TopicId, budget: &mut Budget) -> (ErrorCode, Option<String>) {
        if budget.is_spent() {
            return (
                ErrorCode::RequestTimedOut,
                Some(ErrorCode::RequestTimedOut.to_string()),
            );
        }

        let (mut error_code, mut error_message) = match self.storage.delete_topic(topic_id).await {
            Ok(error_code) => (error_code, Some(error_code.to_string())),

            // An engine's own code is that topic's answer, not an outage, so it neither logs
            // nor spends the request's time.
            Err(Error::Api(error_code)) => (error_code, Some(error_code.to_string())),

            Err(error) => {
                warn!(?topic_id, ?error, "deleting a topic failed in storage");
                budget.after_failure();
                storage_error(&error)
            }
        };

        // Kafka answers a topic ID that it cannot resolve with `UNKNOWN_TOPIC_ID`, and a
        // name that it does not know with `UNKNOWN_TOPIC_OR_PARTITION`. It picks the code
        // from whether the ID resolved, before it looks at the topic, so the choice sits
        // here and not in each storage engine. A zero ID with no name is not mapped:
        // Kafka's KRaft controller answers that `INVALID_REQUEST`, which Nisshi does not do
        // yet.
        //
        // https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/KafkaApis.scala#L2199-L2201
        // https://github.com/apache/kafka/blob/3.9.1/metadata/src/main/java/org/apache/kafka/controller/ReplicationControlManager.java#L923-L929
        if error_code == ErrorCode::UnknownTopicOrPartition
            && matches!(topic_id, TopicId::Id(id) if id.into_bytes() != NULL_TOPIC_ID)
        {
            error_code = ErrorCode::UnknownTopicId;
            error_message = Some(error_code.to_string());
        }

        (error_code, error_message)
    }
}

/// How long a request has to delete its topics, from its `timeout_ms`.
///
/// Storage fails a topic only at its own deadline, so a request that meets a storage outage
/// would otherwise wait that long for each topic, and the client would give up first. After a
/// topic fails and the request's time has passed, the topics that remain answer
/// `REQUEST_TIMED_OUT` without a try. A request in which no topic fails reads the clock once and
/// never compares it.
struct Budget {
    started: Instant,
    timeout: Duration,
    spent: bool,
}

impl Budget {
    fn new(timeout_ms: i32) -> Self {
        Self {
            started: Instant::now(),
            timeout: Duration::from_millis(u64::try_from(timeout_ms).unwrap_or(0)),
            spent: false,
        }
    }

    fn after_failure(&mut self) {
        self.spent = self.started.elapsed() >= self.timeout;
    }

    fn is_spent(&self) -> bool {
        self.spent
    }
}

/// Returns the code and the message of a topic whose delete failed in storage.
///
/// A transient error answers `REQUEST_TIMED_OUT`, which Kafka answers for a deletion that did
/// not finish in the request's time. Kafka answers it for the whole request, with the same code
/// on every topic and no message ([`ControllerApis.handleDeleteTopics`] and
/// [`DeleteTopicsRequest.getErrorResponse`]). Nisshi answers each topic by itself, because it
/// deletes one topic after another and not in one batch. A permanent error answers
/// `UNKNOWN_SERVER_ERROR`. The caller handles an [`Error::Api`] before it gets here.
///
/// The Java admin client does not retry a topic that answers `REQUEST_TIMED_OUT`. It completes
/// that topic's future with a `TimeoutException` ([`KafkaAdminClient.java#L1983-L2007`]), and
/// retries only a throttled topic or a response with `NOT_CONTROLLER`. `NOT_CONTROLLER` would
/// make it send the whole request again, and a topic that this request already deleted would
/// then answer `UNKNOWN_TOPIC_OR_PARTITION`. librdkafka turns a topic's `REQUEST_TIMED_OUT` into
/// a success when the request has an `operation_timeout` of 0 ([rdkafka_admin.c#L2343-L2351]).
/// Its default is `admin.request_timeout_ms`, so a default client reports the error.
///
/// The message of a transient error is fixed, and a permanent error has none, as Kafka's
/// [`ApiError.fromThrowable`] sends none for `UNKNOWN_SERVER_ERROR`. The `Display` of [`Error`]
/// is its `Debug` text, which can hold a connection string or a query, and a client must not
/// see it.
///
/// [`ControllerApis.handleDeleteTopics`]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/ControllerApis.scala#L197-L219
/// [`DeleteTopicsRequest.getErrorResponse`]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/common/requests/DeleteTopicsRequest.java#L78-L91
/// [`KafkaAdminClient.java#L1983-L2007`]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/clients/admin/KafkaAdminClient.java#L1983-L2007
/// [`ApiError.fromThrowable`]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/common/requests/ApiError.java#L38-L46
/// [rdkafka_admin.c#L2343-L2351]: https://github.com/confluentinc/librdkafka/blob/v2.6.1/src/rdkafka_admin.c#L2343-L2351
fn storage_error(error: &Error) -> (ErrorCode, Option<String>) {
    let error_code = error.error_code(ErrorCode::RequestTimedOut);

    let error_message = error
        .is_transient()
        .then(|| "The storage is temporarily unavailable.".to_owned());

    (error_code, error_message)
}

#[cfg(test)]
mod tests;
