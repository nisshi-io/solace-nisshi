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
    ApiKey, ErrorCode, FindCoordinatorRequest, FindCoordinatorResponse, RequestInput,
    find_coordinator_response::Coordinator,
};
use rama::Service;
use tracing::{error, instrument};
use url::Url;

use crate::{Error, Result, Storage};

/// A [`Service`] using its [`Storage`] taking [`FindCoordinatorRequest`] returning [`FindCoordinatorResponse`].
/// ```no_run
/// use rama::Service;
/// use nisshi_sans_io::{ErrorCode, FindCoordinatorRequest};
/// use nisshi_storage::{Error, FindCoordinatorService, StorageContainer};
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
/// let service = FindCoordinatorService { storage };
///
/// let response = service
///     .serve(
///         FindCoordinatorRequest::default()
///             .key(Some("abcba".into()))
///             .key_type(Some(0))
///             .coordinator_keys(Some(["xyzyx".into()].into())),
///     )
///     .await?;
///
/// assert_eq!(
///     Some(ErrorCode::None.into()),
///     if let Some(error_code) = response.error_code {
///         ErrorCode::try_from(error_code).map(Some)?
///     } else {
///         None
///     }
/// );
///
/// assert_eq!(Some(NODE_ID), response.node_id);
/// assert_eq!(Some(HOST), response.host.as_deref());
/// assert_eq!(Some(PORT), response.port);
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct FindCoordinatorService<G> {
    pub storage: G,
}

impl<G> ApiKey for FindCoordinatorService<G> {
    const KEY: i16 = FindCoordinatorRequest::KEY;
}

impl<G, I> Service<I> for FindCoordinatorService<G>
where
    G: Storage,
    I: Into<RequestInput<FindCoordinatorRequest>> + Send + 'static,
{
    type Output = FindCoordinatorResponse;
    type Error = Error;

    #[instrument(skip(self, input))]
    async fn serve(&self, input: I) -> Result<Self::Output, Self::Error> {
        let input = input.into();

        let answer = match self.node_and_listener().await {
            Ok((node_id, listener)) => CoordinatorAnswer::this_broker(node_id, &listener),

            Err(error) => {
                error!(?error, "finding the coordinator failed in storage");
                CoordinatorAnswer::storage_error(&error)
            }
        };

        // The message is the code's description, or `NONE` for no error, because Kafka's
        // `Errors.message()` is the exception's message, and NONE has no exception.
        //
        // https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/common/protocol/Errors.java#L489-L493
        let error_message = answer
            .error_message
            .clone()
            .unwrap_or_else(|| "NONE".into());

        Ok(FindCoordinatorResponse::default()
            .throttle_time_ms(Some(0))
            .error_code(Some(answer.error_code.into()))
            .error_message(Some(error_message))
            .node_id(Some(answer.node_id))
            .host(Some(answer.host.clone()))
            .port(Some(answer.port))
            .coordinators(input.request.coordinator_keys.map(|keys| {
                keys.iter()
                    .map(|key| {
                        Coordinator::default()
                            .key(key.to_string())
                            .node_id(answer.node_id)
                            .host(answer.host.clone())
                            .port(answer.port)
                            .error_code(answer.error_code.into())
                            .error_message(answer.error_message.clone())
                    })
                    .collect()
            })))
    }
}

impl<G> FindCoordinatorService<G>
where
    G: Storage,
{
    /// Reads this broker's node id and advertised listener from storage.
    async fn node_and_listener(&self) -> Result<(i32, Url)> {
        let node_id = self.storage.node().await?;
        let listener = self.storage.advertised_listener().await?;

        Ok((node_id, listener))
    }
}

/// What a response says about the coordinator: this broker, or the error that stands in for it.
struct CoordinatorAnswer {
    error_code: ErrorCode,
    /// The message of an error. A found coordinator has none.
    error_message: Option<String>,
    node_id: i32,
    host: String,
    port: i32,
}

impl CoordinatorAnswer {
    /// The answer that names this broker, `node_id` at `listener`, as the coordinator.
    fn this_broker(node_id: i32, listener: &Url) -> Self {
        Self {
            error_code: ErrorCode::None,
            error_message: None,
            node_id,
            host: listener.host_str().unwrap_or("localhost").into(),
            port: i32::from(listener.port().unwrap_or(9092)),
        }
    }

    /// The answer for a lookup that failed in storage with `error`, in the shape that Kafka
    /// 3.9.1 answers an exception from its handler ([`FindCoordinatorRequest.getErrorResponse`]):
    /// the error's code and message, with no node (id -1, no host, port -1). The connection
    /// stays open, as it does in Kafka.
    ///
    /// A transient error answers `COORDINATOR_NOT_AVAILABLE`, as Kafka answers whenever it
    /// cannot name a coordinator ([`KafkaApis.getCoordinator`]). Every client retries it: the
    /// Java admin client ([`CoordinatorStrategy.java#L152-L174`]), the classic consumer
    /// ([`AbstractCoordinator.java#L299-L317`]), the async consumer
    /// ([`CoordinatorRequestManager.java#L163-L181`]) and librdkafka, which clears the
    /// coordinator and queries again ([rdkafka_cgrp.c#L779-L796]). A permanent error answers
    /// `UNKNOWN_SERVER_ERROR`, as Kafka's [`Errors.forException`] answers an exception it does
    /// not know. Each client reports that one to its application: the Java consumer's `poll`
    /// throws it, and librdkafka raises one consumer error and keeps querying
    /// ([rdkafka_cgrp.c#L797-L812]). An [`Error::Api`] answers its own code.
    ///
    /// The message is the code's description, which is the exception's message that Kafka's
    /// [`FindCoordinatorResponse.prepareErrorResponse`] sends.
    ///
    /// [`FindCoordinatorRequest.getErrorResponse`]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/common/requests/FindCoordinatorRequest.java#L95-L106
    /// [`KafkaApis.getCoordinator`]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/KafkaApis.scala#L1696-L1745
    /// [`CoordinatorStrategy.java#L152-L174`]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/clients/admin/internals/CoordinatorStrategy.java#L152-L174
    /// [`AbstractCoordinator.java#L299-L317`]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/clients/consumer/internals/AbstractCoordinator.java#L299-L317
    /// [`CoordinatorRequestManager.java#L163-L181`]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/clients/consumer/internals/CoordinatorRequestManager.java#L163-L181
    /// [rdkafka_cgrp.c#L779-L796]: https://github.com/confluentinc/librdkafka/blob/v2.10.0/src/rdkafka_cgrp.c#L779-L796
    /// [rdkafka_cgrp.c#L797-L812]: https://github.com/confluentinc/librdkafka/blob/v2.10.0/src/rdkafka_cgrp.c#L797-L812
    /// [`Errors.forException`]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/common/protocol/Errors.java#L512-L522
    /// [`FindCoordinatorResponse.prepareErrorResponse`]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/common/requests/FindCoordinatorResponse.java#L172-L187
    fn storage_error(error: &Error) -> Self {
        let error_code = error.error_code(ErrorCode::CoordinatorNotAvailable);

        Self {
            error_code,
            error_message: Some(error_code.to_string()),
            node_id: -1,
            host: String::new(),
            port: -1,
        }
    }
}

#[cfg(test)]
mod tests {
    use nisshi_sans_io::{Frame, Header};
    use rama::extensions::Extensions;

    use super::*;
    use crate::service::counting_storage::{BrokerCall, CountingStorage, HOST, NODE_ID, PORT};

    /// An error that a retry may clear.
    fn transient() -> Error {
        Error::from(std::io::Error::other("connection reset"))
    }

    /// An error that a retry can't clear, with text that must not reach a client.
    fn permanent() -> Error {
        Error::Message("password=hunter2".into())
    }

    /// Serves a request that names the group `abcba` in the v0 to v3 field and the groups
    /// `abcba` and `xyzyx` in the v4 and later list, so that one response fills both shapes.
    async fn serve(storage: CountingStorage) -> Result<FindCoordinatorResponse> {
        FindCoordinatorService { storage }
            .serve(RequestInput {
                request: FindCoordinatorRequest::default()
                    .key(Some("abcba".into()))
                    .key_type(Some(0))
                    .coordinator_keys(Some(vec!["abcba".into(), "xyzyx".into()])),
                extensions: Extensions::default(),
            })
            .await
    }

    /// Encodes `response` at every version of the API, so that a field that a version needs
    /// and the response leaves unset fails here and not at a client.
    fn encodes_at_every_version(response: &FindCoordinatorResponse) -> Result<()> {
        for version in 0..=6 {
            _ = Frame::response(
                Header::Response { correlation_id: 0 },
                response.clone().into(),
                FindCoordinatorRequest::KEY,
                version,
            )?;
        }

        Ok(())
    }

    fn coordinators(response: &FindCoordinatorResponse) -> Vec<Coordinator> {
        response.coordinators.clone().unwrap_or_default()
    }

    /// A storage error that leaves `serve` closes the connection, because the stream returns it
    /// before it writes a response. The service answers the request instead, whichever of its
    /// two storage reads fails, with the code and no node on the response and on each key, as
    /// Kafka answers an exception from its handler.
    #[tokio::test]
    async fn storage_error_answers_a_response() -> Result<()> {
        for call in [BrokerCall::Node, BrokerCall::AdvertisedListener] {
            let response = serve(CountingStorage::failing_broker_call(call, transient())).await?;

            assert_eq!(
                Some(ErrorCode::CoordinatorNotAvailable),
                response.error_code.map(ErrorCode::try_from).transpose()?,
                "{call:?}"
            );
            assert_eq!(
                Some("The coordinator is not available."),
                response.error_message.as_deref(),
                "{call:?}"
            );
            assert_eq!(Some(-1), response.node_id, "{call:?}");
            assert_eq!(Some(""), response.host.as_deref(), "{call:?}");
            assert_eq!(Some(-1), response.port, "{call:?}");

            let coordinators = coordinators(&response);
            assert_eq!(2, coordinators.len(), "{call:?}");
            for coordinator in &coordinators {
                assert_eq!(
                    ErrorCode::CoordinatorNotAvailable,
                    ErrorCode::try_from(coordinator.error_code)?,
                    "{call:?} {coordinator:?}"
                );
                assert_eq!(
                    Some("The coordinator is not available."),
                    coordinator.error_message.as_deref(),
                    "{call:?} {coordinator:?}"
                );
                assert_eq!(-1, coordinator.node_id, "{call:?} {coordinator:?}");
                assert_eq!("", coordinator.host, "{call:?} {coordinator:?}");
                assert_eq!(-1, coordinator.port, "{call:?} {coordinator:?}");
            }

            encodes_at_every_version(&response)?;
        }

        Ok(())
    }

    /// A permanent error answers `UNKNOWN_SERVER_ERROR`, with the code's own description and
    /// not the error's text. An error that is a Kafka code answers that code.
    #[tokio::test]
    async fn the_error_chooses_the_code() -> Result<()> {
        for (error, expected) in [
            (permanent(), ErrorCode::UnknownServerError),
            (
                Error::Api(ErrorCode::GroupAuthorizationFailed),
                ErrorCode::GroupAuthorizationFailed,
            ),
        ] {
            let response = serve(CountingStorage::failing_broker_call(
                BrokerCall::Node,
                error.clone(),
            ))
            .await?;

            assert_eq!(
                Some(expected),
                response.error_code.map(ErrorCode::try_from).transpose()?,
                "{error:?}"
            );
            assert_eq!(
                Some(expected.to_string()),
                response.error_message,
                "{error:?}"
            );
            assert_eq!(
                vec![expected, expected],
                coordinators(&response)
                    .iter()
                    .map(|coordinator| ErrorCode::try_from(coordinator.error_code))
                    .collect::<Result<Vec<_>, _>>()?,
                "{error:?}"
            );
            encodes_at_every_version(&response)?;
        }

        Ok(())
    }

    /// A storage that answers both reads names this broker as the coordinator of every key.
    #[tokio::test]
    async fn a_successful_lookup_names_this_broker() -> Result<()> {
        let response = serve(CountingStorage::default()).await?;

        assert_eq!(
            Some(ErrorCode::None),
            response.error_code.map(ErrorCode::try_from).transpose()?
        );
        assert_eq!(Some("NONE"), response.error_message.as_deref());
        assert_eq!(Some(NODE_ID), response.node_id);
        assert_eq!(Some(HOST), response.host.as_deref());
        assert_eq!(Some(PORT), response.port);

        for coordinator in coordinators(&response) {
            assert_eq!(
                ErrorCode::None,
                ErrorCode::try_from(coordinator.error_code)?
            );
            assert_eq!(None, coordinator.error_message);
            assert_eq!(NODE_ID, coordinator.node_id);
            assert_eq!(HOST, coordinator.host);
            assert_eq!(PORT, coordinator.port);
        }

        Ok(())
    }
}
