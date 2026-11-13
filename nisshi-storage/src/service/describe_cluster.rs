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
    ApiKey, DescribeClusterRequest, DescribeClusterResponse, ErrorCode, RequestInput,
    describe_cluster_response::DescribeClusterBroker,
};
use rama::Service;
use tracing::{debug, error, instrument};

use crate::{Error, Result, Storage};

/// A [`Service`] using its [`Storage`] taking [`DescribeClusterRequest`] returning [`DescribeClusterResponse`].
/// ```no_run
/// use rama::Service as _;
/// use nisshi_sans_io::{DescribeClusterRequest, EndpointType, ErrorCode};
/// use nisshi_storage::{DescribeClusterService, Error, StorageContainer};
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
/// let service = DescribeClusterService { storage };
///
/// let response = service
///     .serve(
///         DescribeClusterRequest::default()
///             .endpoint_type(Some(EndpointType::Broker.into()))
///             .include_cluster_authorized_operations(false),
///     )
///     .await?;
///
/// let brokers = response.brokers.unwrap_or_default();
/// assert_eq!(1, brokers.len());
/// assert_eq!(NODE_ID, brokers[0].broker_id);
/// assert_eq!(HOST, brokers[0].host.as_str());
/// assert_eq!(PORT, brokers[0].port);
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct DescribeClusterService<G> {
    pub storage: G,
}

impl<G> ApiKey for DescribeClusterService<G> {
    const KEY: i16 = DescribeClusterRequest::KEY;
}

impl<G, I> Service<I> for DescribeClusterService<G>
where
    G: Storage,
    I: Into<RequestInput<DescribeClusterRequest>> + Send + 'static,
{
    type Output = DescribeClusterResponse;
    type Error = Error;

    #[instrument(skip(self, input))]
    async fn serve(&self, input: I) -> Result<Self::Output, Self::Error> {
        let input = input.into();

        let response = match self.brokers_and_cluster_id().await {
            Ok((brokers, cluster_id)) => DescribeClusterResponse::default()
                .error_code(ErrorCode::None.into())
                .error_message(None)
                .controller_id(brokers.first().map(|broker| broker.broker_id).unwrap_or(-1))
                .cluster_id(cluster_id)
                .brokers(Some(brokers)),

            Err(error) => {
                error!(?error, "describing the cluster failed in storage");
                storage_error_response(&error)
            }
        };

        Ok(response
            .throttle_time_ms(0)
            .endpoint_type(input.request.endpoint_type)
            .cluster_authorized_operations(-2_147_483_648))
    }
}

impl<G> DescribeClusterService<G>
where
    G: Storage,
{
    /// Reads the brokers and the cluster id from storage.
    async fn brokers_and_cluster_id(&self) -> Result<(Vec<DescribeClusterBroker>, String)> {
        let brokers = self.storage.brokers().await?;
        debug!(?brokers);

        let cluster_id = self.storage.cluster_id().await?;

        Ok((brokers, cluster_id))
    }
}

/// Returns the response for a describe that failed in storage with `error`, in the shape that
/// Kafka 3.9.1 answers an exception from its handler ([`DescribeClusterRequest.getErrorResponse`]):
/// the error's code, no brokers, no cluster id and no controller. The connection stays open, as
/// it does in Kafka.
///
/// A transient error answers `KAFKA_STORAGE_ERROR`, the code that Kafka's [`Errors.forException`]
/// gives its own `KafkaStorageException`, with a fixed message. A permanent error answers
/// `UNKNOWN_SERVER_ERROR` with no message, as Kafka's [`ApiError.fromThrowable`] sends none for
/// that code, so that the storage error's text does not reach the client. An [`Error::Api`]
/// answers its own code.
///
/// The Java admin client fails a `describeCluster` at once on any code
/// ([`KafkaAdminClient.java#L2498-L2507`]). The exception class still tells its caller that a
/// retry may clear `KAFKA_STORAGE_ERROR`: `KafkaStorageException` is a `RetriableException`, and
/// `UnknownServerException` is not. librdkafka serves its `DescribeCluster` with a Metadata
/// request ([rdkafka_admin.c#L9422-L9446]), so it never sees this response.
///
/// [`DescribeClusterRequest.getErrorResponse`]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/common/requests/DescribeClusterRequest.java#L62-L67
/// [`Errors.forException`]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/common/protocol/Errors.java#L512-L522
/// [`ApiError.fromThrowable`]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/common/requests/ApiError.java#L38-L46
/// [`KafkaAdminClient.java#L2498-L2507`]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/clients/admin/KafkaAdminClient.java#L2498-L2507
/// [rdkafka_admin.c#L9422-L9446]: https://github.com/confluentinc/librdkafka/blob/v2.10.0/src/rdkafka_admin.c#L9422-L9446
fn storage_error_response(error: &Error) -> DescribeClusterResponse {
    let error_message = error
        .is_transient()
        .then(|| "The storage is temporarily unavailable.".to_owned());

    DescribeClusterResponse::default()
        .error_code(error.error_code(ErrorCode::KafkaStorageError).into())
        .error_message(error_message)
        .controller_id(-1)
        .cluster_id(String::new())
        .brokers(Some(vec![]))
}

#[cfg(test)]
mod tests {
    use nisshi_sans_io::{Frame, Header};
    use rama::extensions::Extensions;

    use super::*;
    use crate::service::counting_storage::{BrokerCall, CountingStorage, NODE_ID};

    /// An error that a retry may clear.
    fn transient() -> Error {
        Error::from(std::io::Error::other("connection reset"))
    }

    /// An error that a retry can't clear, with text that must not reach a client.
    fn permanent() -> Error {
        Error::Message("password=hunter2".into())
    }

    /// Serves a v1 request, which names the endpoint type that the response echoes.
    async fn serve(storage: CountingStorage) -> Result<DescribeClusterResponse> {
        DescribeClusterService { storage }
            .serve(RequestInput {
                request: DescribeClusterRequest::default()
                    .endpoint_type(Some(1))
                    .include_cluster_authorized_operations(false),
                extensions: Extensions::default(),
            })
            .await
    }

    /// Encodes `response` at every version of the API, so that a field that a version needs
    /// and the response leaves unset fails here and not at a client.
    fn encodes_at_every_version(response: &DescribeClusterResponse) -> Result<()> {
        for version in 0..=1 {
            _ = Frame::response(
                Header::Response { correlation_id: 0 },
                response.clone().into(),
                DescribeClusterRequest::KEY,
                version,
            )?;
        }

        Ok(())
    }

    /// A storage error that leaves `serve` closes the connection, because the stream returns it
    /// before it writes a response. The service answers the request instead, whichever of its
    /// two storage reads fails, with the code, no brokers and no controller, as Kafka answers an
    /// exception from its handler.
    #[tokio::test]
    async fn storage_error_answers_a_response() -> Result<()> {
        for call in [BrokerCall::Brokers, BrokerCall::ClusterId] {
            let response = serve(CountingStorage::failing_broker_call(call, transient())).await?;

            assert_eq!(
                ErrorCode::KafkaStorageError,
                ErrorCode::try_from(response.error_code)?,
                "{call:?}"
            );
            assert_eq!(
                Some("The storage is temporarily unavailable."),
                response.error_message.as_deref(),
                "{call:?}"
            );
            assert_eq!(-1, response.controller_id, "{call:?}");
            assert_eq!("", response.cluster_id, "{call:?}");
            assert_eq!(Some(vec![]), response.brokers, "{call:?}");
            assert_eq!(Some(1), response.endpoint_type, "{call:?}");
            encodes_at_every_version(&response)?;
        }

        Ok(())
    }

    /// A permanent error answers `UNKNOWN_SERVER_ERROR` with no message, so the error's text
    /// stays on the broker. An error that is a Kafka code answers that code.
    #[tokio::test]
    async fn the_error_chooses_the_code() -> Result<()> {
        for (error, expected) in [
            (permanent(), ErrorCode::UnknownServerError),
            (
                Error::Api(ErrorCode::ClusterAuthorizationFailed),
                ErrorCode::ClusterAuthorizationFailed,
            ),
        ] {
            let response = serve(CountingStorage::failing_broker_call(
                BrokerCall::Brokers,
                error.clone(),
            ))
            .await?;

            assert_eq!(
                expected,
                ErrorCode::try_from(response.error_code)?,
                "{error:?}"
            );
            assert_eq!(None, response.error_message, "{error:?}");
            encodes_at_every_version(&response)?;
        }

        Ok(())
    }

    /// A storage that answers both reads gets the brokers and the cluster id, with the first
    /// broker as the controller.
    #[tokio::test]
    async fn a_successful_describe_lists_the_broker() -> Result<()> {
        let response = serve(CountingStorage::default()).await?;

        assert_eq!(ErrorCode::None, ErrorCode::try_from(response.error_code)?);
        assert_eq!(NODE_ID, response.controller_id);
        assert_eq!("nisshi", response.cluster_id);
        assert_eq!(
            vec![NODE_ID],
            response
                .brokers
                .unwrap_or_default()
                .iter()
                .map(|broker| broker.broker_id)
                .collect::<Vec<_>>()
        );

        Ok(())
    }
}
