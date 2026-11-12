// Copyright ⓒ Solace Corporation
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

use std::fmt::Debug;

use nisshi_sans_io::{
    Ack, ErrorCode, ProduceRequest, ProduceResponse, RequestInput, SuppressResponseExtension,
};
use rama::{Layer, Service};
use tracing::debug;

use crate::Error;

/// A [`Layer`] of [`AcksZeroService`].
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct AcksZeroLayer;

impl<S> Layer<S> for AcksZeroLayer {
    type Service = AcksZeroService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        AcksZeroService { inner }
    }
}

/// Answers an `acks=0` Produce as Kafka does: the client reads no response, and a
/// failed write closes the client's connection.
///
/// The service forwards an `acks=0` Produce at `acks=1`, waits for the origin's
/// response, and then suppresses the response to the client. It does not forward
/// the request at `acks=0` and return the origin connection to the pool after the
/// write, because the pool would send the client's next batch on another origin
/// connection, and the origin could then write the batches out of order. The origin
/// also closes a connection after a failed `acks=0` write, and that failure would
/// reach the next client of the pooled connection. Kafka answers an `acks=1` write
/// after the same append to the leader as an `acks=0` write ([`ReplicaManager`]), so
/// the higher `acks` adds the response, and the origin counts the request against
/// its request quota, which exempts `acks=0` ([`KafkaApis`]).
///
/// A batched topic can combine an `acks=0` request with an `acks=-1` request. The
/// origin then answers after the full ISR has the batch, and a replication error
/// closes the `acks=0` client's connection too.
///
/// [`ReplicaManager`]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/ReplicaManager.scala#L1357-L1363
/// [`KafkaApis`]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/KafkaApis.scala#L687-L689
#[derive(Clone, Debug)]
pub(crate) struct AcksZeroService<S> {
    inner: S,
}

impl<S> Service<RequestInput<ProduceRequest>> for AcksZeroService<S>
where
    S: Service<RequestInput<ProduceRequest>, Output = ProduceResponse>,
    S::Error: Into<Error>,
{
    type Output = ProduceResponse;
    type Error = Error;

    async fn serve(
        &self,
        input: RequestInput<ProduceRequest>,
    ) -> Result<Self::Output, Self::Error> {
        if input.request.acks != i16::from(Ack::None) {
            return self.inner.serve(input).await.map_err(Into::into);
        }

        let extensions = input.extensions.clone();

        let response = self
            .inner
            .serve(RequestInput {
                request: input.request.acks(Ack::Leader.into()),
                extensions: input.extensions,
            })
            .await
            .map_err(Into::into)?;

        // Kafka closes the connection of an `acks=0` client when any partition
        // fails, because the client reads no response that could report the error
        // ([`KafkaApis`]).
        //
        // [`KafkaApis`]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/KafkaApis.scala#L701-L714
        if let Some(failure) = first_failure(&response) {
            debug!(?failure);
            return Err(Error::Message(format!("acks=0 produce failed: {failure}")));
        }

        SuppressResponseExtension::mark(&extensions);
        Ok(response)
    }
}

/// Returns the topic, partition and error code of the first partition in `response`
/// that reports an error.
fn first_failure(response: &ProduceResponse) -> Option<String> {
    response.responses.iter().flatten().find_map(|topic| {
        topic
            .partition_responses
            .iter()
            .flatten()
            .find(|partition| partition.error_code != i16::from(ErrorCode::None))
            .map(|partition| {
                format!(
                    "topic: {}, partition: {}, error code: {}",
                    topic.name, partition.index, partition.error_code
                )
            })
    })
}

#[cfg(test)]
mod tests {
    use std::{
        assert_matches,
        sync::{Arc, Mutex},
    };

    use nisshi_sans_io::produce_response::{PartitionProduceResponse, TopicProduceResponse};
    use nisshi_service::ResponseService;
    use rama::extensions::Extensions;

    use super::*;

    fn connection_extensions() -> Extensions {
        let extensions = Extensions::default();
        _ = extensions.insert(SuppressResponseExtension::default());
        extensions
    }

    fn response(error_code: ErrorCode) -> ProduceResponse {
        ProduceResponse::default().responses(Some(
            [TopicProduceResponse::default()
                .name("topic".into())
                .partition_responses(Some(
                    [PartitionProduceResponse::default()
                        .index(3)
                        .error_code(error_code.into())]
                    .into(),
                ))]
            .into(),
        ))
    }

    /// Serves a Produce with `acks` through the layer, in front of an origin that
    /// answers with `error_code`. Returns the result, the `acks` that reached the
    /// origin, and whether the layer suppressed the response.
    async fn serve(
        acks: Ack,
        error_code: ErrorCode,
    ) -> (Result<ProduceResponse, Error>, Option<i16>, bool) {
        let forwarded = Arc::new(Mutex::new(None));

        let service = AcksZeroLayer.layer(ResponseService::new({
            let forwarded = forwarded.clone();
            move |input: RequestInput<ProduceRequest>| {
                *forwarded.lock().unwrap() = Some(input.request.acks);
                Ok::<_, Error>(response(error_code))
            }
        }));

        let extensions = connection_extensions();
        let result = service
            .serve(RequestInput {
                request: ProduceRequest::default().acks(acks.into()),
                extensions: extensions.clone(),
            })
            .await;

        let forwarded = *forwarded.lock().unwrap();
        (
            result,
            forwarded,
            SuppressResponseExtension::take(&extensions),
        )
    }

    #[tokio::test]
    async fn acks_zero_is_forwarded_at_leader_and_suppressed() {
        let (result, forwarded, suppressed) = serve(Ack::None, ErrorCode::None).await;

        assert_matches!(result, Ok(_));
        assert_eq!(Some(i16::from(Ack::Leader)), forwarded);
        assert!(suppressed);
    }

    #[tokio::test]
    async fn acks_zero_partition_error_fails_without_suppressing() {
        let (result, _, suppressed) = serve(Ack::None, ErrorCode::NotLeaderOrFollower).await;

        assert_matches!(
            &result,
            Err(Error::Message(message)) if message.contains("partition: 3")
        );
        assert!(!suppressed);
    }

    #[tokio::test]
    async fn other_acks_are_forwarded_unchanged_and_answered() {
        for acks in [Ack::Leader, Ack::FullIsr] {
            let (result, forwarded, suppressed) = serve(acks, ErrorCode::NotLeaderOrFollower).await;

            assert_matches!(result, Ok(_));
            assert_eq!(Some(i16::from(acks)), forwarded);
            assert!(!suppressed);
        }
    }
}
