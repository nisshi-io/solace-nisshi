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
use std::time::{Duration, Instant};

use rama::Service;
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
/// `REQUEST_TIMED_OUT` without a try. A request in which no topic fails never reads the time.
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
/// on every topic and no message ([`ControllerApis.deleteTopics`] and
/// [`DeleteTopicsRequest.getErrorResponse`]). Nisshi answers each topic by itself, because it
/// deletes one topic after another and not in one batch. A permanent error answers
/// `UNKNOWN_SERVER_ERROR`, and an [`Error::Api`] answers its own code.
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
/// [`ControllerApis.deleteTopics`]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/ControllerApis.scala#L197-L219
/// [`DeleteTopicsRequest.getErrorResponse`]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/common/requests/DeleteTopicsRequest.java#L78-L91
/// [`KafkaAdminClient.java#L1983-L2007`]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/clients/admin/KafkaAdminClient.java#L1983-L2007
/// [`ApiError.fromThrowable`]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/common/requests/ApiError.java#L38-L46
/// [rdkafka_admin.c#L2343-L2351]: https://github.com/confluentinc/librdkafka/blob/v2.6.1/src/rdkafka_admin.c#L2343-L2351
fn storage_error(error: &Error) -> (ErrorCode, Option<String>) {
    let error_code = error.error_code(ErrorCode::RequestTimedOut);

    let error_message = if error.is_transient() {
        Some("The storage is temporarily unavailable.".to_owned())
    } else if error_code == ErrorCode::UnknownServerError {
        None
    } else {
        Some(error_code.to_string())
    };

    (error_code, error_message)
}

#[cfg(test)]
mod tests {
    use std::assert_matches;

    use nisshi_sans_io::{
        ApiKey as _, DeleteTopicsRequest, DeleteTopicsResponse, ErrorCode, Frame, Header,
        NULL_TOPIC_ID, RequestInput, delete_topics_request::DeleteTopicState,
        delete_topics_response::DeletableTopicResult,
    };
    use rama::{Service as _, extensions::Extensions};
    use uuid::Uuid;

    use super::DeleteTopicsService;
    use crate::{Error, Result, TopicId, service::counting_storage::CountingStorage};

    /// A timeout that no test reaches.
    const LONG: i32 = 600_000;

    /// An error that a retry may clear.
    fn transient() -> Error {
        Error::from(std::io::Error::other("connection reset"))
    }

    /// An error that a retry can't clear, with text that must not reach a client.
    fn permanent() -> Error {
        Error::Message("password=hunter2".into())
    }

    async fn serve(
        storage: &CountingStorage,
        request: DeleteTopicsRequest,
    ) -> Result<DeleteTopicsResponse> {
        DeleteTopicsService {
            storage: storage.clone(),
        }
        .serve(RequestInput {
            request,
            extensions: Extensions::default(),
        })
        .await
    }

    fn by_name(names: &[&str], timeout_ms: i32) -> DeleteTopicsRequest {
        DeleteTopicsRequest::default()
            .topic_names(Some(names.iter().map(|name| (*name).to_owned()).collect()))
            .timeout_ms(timeout_ms)
    }

    fn answers(response: DeleteTopicsResponse) -> Vec<DeletableTopicResult> {
        response.responses.unwrap_or_default()
    }

    fn codes(results: &[DeletableTopicResult]) -> Result<Vec<ErrorCode>> {
        results
            .iter()
            .map(|result| ErrorCode::try_from(result.error_code).map_err(Into::into))
            .collect()
    }

    fn names(results: &[DeletableTopicResult]) -> Vec<Option<&str>> {
        results
            .iter()
            .map(|result| result.name.as_deref())
            .collect()
    }

    /// A storage error that leaves `serve` closes the connection, because the stream returns it
    /// before it writes a response. The service answers the topic instead.
    #[tokio::test]
    async fn storage_error_answers_a_response() -> Result<()> {
        let storage = CountingStorage::failing_delete_topic("a".into(), transient());

        let response = serve(&storage, by_name(&["a"], LONG)).await?;

        assert_eq!(vec![Some("a")], names(&answers(response)));

        Ok(())
    }

    /// An error that a retry may clear answers `REQUEST_TIMED_OUT`, as Kafka answers a deletion
    /// that timed out. Any other error answers `UNKNOWN_SERVER_ERROR`, and an error that is a
    /// Kafka code answers that code.
    #[tokio::test]
    async fn the_error_chooses_the_code() -> Result<()> {
        for (error, expected) in [
            (transient(), ErrorCode::RequestTimedOut),
            (permanent(), ErrorCode::UnknownServerError),
            (
                Error::Api(ErrorCode::TopicAuthorizationFailed),
                ErrorCode::TopicAuthorizationFailed,
            ),
        ] {
            let storage = CountingStorage::failing_delete_topic("a".into(), error.clone());

            let response = serve(&storage, by_name(&["a"], LONG)).await?;

            assert_eq!(vec![expected], codes(&answers(response))?, "{error:?}");
        }

        Ok(())
    }

    /// A failure on one topic does not stop the others. Each topic gets its own result, in the
    /// order of the request.
    #[tokio::test]
    async fn a_failed_topic_does_not_stop_the_others() -> Result<()> {
        let storage = CountingStorage::failing_delete_topic("failed".into(), transient());

        let response = serve(&storage, by_name(&["first", "failed", "last"], LONG)).await?;

        let results = answers(response);
        assert_eq!(
            vec![ErrorCode::None, ErrorCode::RequestTimedOut, ErrorCode::None],
            codes(&results)?
        );
        assert_eq!(
            vec![Some("first"), Some("failed"), Some("last")],
            names(&results)
        );
        assert_eq!(
            vec![TopicId::from("first"), TopicId::from("last")],
            storage.deleted()?
        );

        Ok(())
    }

    /// The v6 form names a topic by name or by ID. A failed topic keeps the name and the ID that
    /// the request gave it, and the topics after it are still deleted.
    #[tokio::test]
    async fn a_failed_topic_keeps_its_name_and_id_in_a_v6_request() -> Result<()> {
        let failed_id = Uuid::new_v4();
        let last_id = Uuid::new_v4();
        let storage = CountingStorage::failing_delete_topic(failed_id.into(), transient());

        let response = serve(
            &storage,
            DeleteTopicsRequest::default()
                .timeout_ms(LONG)
                .topics(Some(vec![
                    DeleteTopicState::default().name(Some("named".into())),
                    DeleteTopicState::default().topic_id(failed_id.into_bytes()),
                    DeleteTopicState::default().topic_id(last_id.into_bytes()),
                ])),
        )
        .await?;

        let results = answers(response);
        assert_eq!(
            vec![ErrorCode::None, ErrorCode::RequestTimedOut, ErrorCode::None],
            codes(&results)?
        );
        assert_eq!(vec![Some("named"), None, None], names(&results));
        assert_eq!(Some(NULL_TOPIC_ID), results[0].topic_id);
        assert_eq!(Some(failed_id.into_bytes()), results[1].topic_id);
        assert_eq!(Some(last_id.into_bytes()), results[2].topic_id);
        assert_eq!(
            vec![TopicId::from("named"), TopicId::from(last_id)],
            storage.deleted()?
        );

        Ok(())
    }

    /// Once a topic has failed and the request's `timeout_ms` has passed, the topics after it
    /// answer `REQUEST_TIMED_OUT` without a call to storage. A request with no failure still
    /// deletes every topic, whatever its timeout.
    #[tokio::test]
    async fn an_expired_timeout_answers_the_rest_without_a_try() -> Result<()> {
        let storage = CountingStorage::failing_delete_topic("failed".into(), permanent());

        let response = serve(&storage, by_name(&["failed", "a", "b"], 0)).await?;

        let results = answers(response);
        assert_eq!(
            vec![
                ErrorCode::UnknownServerError,
                ErrorCode::RequestTimedOut,
                ErrorCode::RequestTimedOut
            ],
            codes(&results)?
        );
        assert_eq!(vec![Some("failed"), Some("a"), Some("b")], names(&results));
        assert_eq!(Vec::<TopicId>::new(), storage.deleted()?);

        let storage = CountingStorage::default();

        let response = serve(&storage, by_name(&["a", "b"], 0)).await?;

        assert_eq!(
            vec![ErrorCode::None, ErrorCode::None],
            codes(&answers(response))?
        );
        assert_eq!(
            vec![TopicId::from("a"), TopicId::from("b")],
            storage.deleted()?
        );

        Ok(())
    }

    /// A transient failure carries a fixed message. A permanent failure carries none, as Kafka's
    /// `UNKNOWN_SERVER_ERROR` does, so the text of the storage error does not reach the client.
    /// The response encodes at versions 0 and 5, where the request names topics, and at version 6,
    /// where it can name them by ID.
    #[tokio::test]
    async fn the_message_is_fixed_and_the_response_encodes() -> Result<()> {
        for (error, message) in [
            (
                transient(),
                Some("The storage is temporarily unavailable.".to_owned()),
            ),
            (permanent(), None),
        ] {
            let storage = CountingStorage::failing_delete_topic("a".into(), error);

            let response = serve(&storage, by_name(&["a"], LONG)).await?;

            assert_eq!(message, answers(response.clone())[0].error_message);

            let v6 = serve(
                &storage,
                DeleteTopicsRequest::default()
                    .timeout_ms(LONG)
                    .topics(Some(vec![
                        DeleteTopicState::default().name(Some("a".into())),
                    ])),
            )
            .await?;

            assert_eq!(message, answers(v6.clone())[0].error_message);

            for (response, version) in [(response.clone(), 0), (response, 5), (v6, 6)] {
                _ = Frame::response(
                    Header::Response { correlation_id: 0 },
                    response.into(),
                    DeleteTopicsRequest::KEY,
                    version,
                )?;
            }
        }

        Ok(())
    }

    /// A storage error that is `UNKNOWN_TOPIC_OR_PARTITION` answers `UNKNOWN_TOPIC_ID` for a topic
    /// that the request names by ID, and keeps its code for a name, as an `Ok` answer does.
    #[tokio::test]
    async fn an_unknown_topic_error_is_remapped_for_an_id() -> Result<()> {
        let id = Uuid::new_v4();
        let error = Error::Api(ErrorCode::UnknownTopicOrPartition);

        for (storage, request, expected) in [
            (
                CountingStorage::failing_delete_topic(id.into(), error.clone()),
                DeleteTopicsRequest::default()
                    .timeout_ms(LONG)
                    .topics(Some(vec![
                        DeleteTopicState::default().topic_id(id.into_bytes()),
                    ])),
                ErrorCode::UnknownTopicId,
            ),
            (
                CountingStorage::failing_delete_topic("a".into(), error),
                by_name(&["a"], LONG),
                ErrorCode::UnknownTopicOrPartition,
            ),
        ] {
            let response = serve(&storage, request).await?;

            let results = answers(response);
            assert_eq!(vec![expected], codes(&results)?);
            assert_matches!(
                results[0].error_message,
                Some(ref message) if *message == expected.to_string()
            );
        }

        Ok(())
    }
}
