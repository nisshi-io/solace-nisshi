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

use std::{
    fmt::{self, Debug},
    sync::LazyLock,
};

use nisshi_sans_io::{
    Ack, ErrorCode, ProduceRequest, ProduceResponse, RequestInput, SuppressResponseExtension,
    produce_response::TopicProduceResponse,
};
use opentelemetry::{KeyValue, metrics::Counter};
use rama::{Layer, Service};
use tracing::instrument;

use crate::{Error, METER, Result};

/// Counts the connections closed by a failed `acks=0` Produce, by the error
/// code of the first failing partition.
///
/// The failed request does not reach the request and error counts, because
/// it ends the connection before a response is encoded. A producer that
/// keeps failing shows here as a reconnect loop.
static ACKS_ZERO_PRODUCE_FAILED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("nisshi_produce_acks_zero_failed")
        .with_description("Connections closed by a failed acks=0 Produce")
        .build()
});

/// The first partition in `responses` carrying a non-[`ErrorCode::None`]
/// error code, for the message an `acks=0` closed connection logs: the
/// client gets no response at all, so an operator needs to know which
/// partition actually failed.
fn first_error(responses: &[TopicProduceResponse]) -> Option<(String, i32, ErrorCode)> {
    responses.iter().find_map(|topic| {
        topic
            .partition_responses
            .as_deref()
            .unwrap_or_default()
            .iter()
            .find(|partition| partition.error_code != i16::from(ErrorCode::None))
            .map(|partition| {
                (
                    topic.name.clone(),
                    partition.index,
                    ErrorCode::try_from(partition.error_code)
                        .unwrap_or(ErrorCode::UnknownServerError),
                )
            })
    })
}

/// A [`Layer`] that gives a Produce `acks=0` request Kafka's answer: no response, and a closed
/// connection when any partition failed.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct AcksZeroLayer;

impl AcksZeroLayer {
    pub fn new() -> Self {
        Self
    }
}

impl<S> Layer<S> for AcksZeroLayer {
    type Service = AcksZeroService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        AcksZeroService { inner }
    }
}

/// A [`Service`] that marks the response to an `acks=0` [`ProduceRequest`] as suppressed, or
/// fails with [`Error::AcksZeroProduceFailed`] when `inner` answered any partition with an error.
#[derive(Clone, Copy, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct AcksZeroService<S> {
    inner: S,
}

impl<S> Debug for AcksZeroService<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(stringify!(AcksZeroService)).finish()
    }
}

impl<S, I> Service<I> for AcksZeroService<S>
where
    S: Service<RequestInput<ProduceRequest>, Output = ProduceResponse, Error = Error>,
    I: Into<RequestInput<ProduceRequest>> + Send + 'static,
{
    type Output = ProduceResponse;
    type Error = Error;

    #[instrument(skip(self, input))]
    async fn serve(&self, input: I) -> Result<Self::Output> {
        let input = input.into();
        let acks = input.request.acks;
        let extensions = input.extensions.clone();

        let response = self.inner.serve(input).await?;

        // Kafka's `KafkaApis.handleProduceRequest` closes the connection for an
        // `acks=0` request whenever any partition in the assembled response map
        // carries an error, not only a storage failure: `unauthorizedTopicResponses`,
        // `nonExistingTopicResponses` and `invalidRequestResponses` are merged into
        // that map before the check runs. The client reads no response under
        // `acks=0` regardless, so closing on any error (rather than silently
        // dropping the batch) matches that behaviour and gives a real Kafka
        // producer a retriable `NetworkException` instead of nothing at all.
        if acks == i16::from(Ack::None) {
            if let Some((topic, partition, error_code)) =
                first_error(response.responses.as_deref().unwrap_or_default())
            {
                ACKS_ZERO_PRODUCE_FAILED
                    .add(1, &[KeyValue::new("error_code", format!("{error_code:?}"))]);

                return Err(Error::AcksZeroProduceFailed {
                    topic,
                    partition,
                    error_code,
                });
            }

            SuppressResponseExtension::mark(&extensions);
        }

        Ok(response)
    }
}
