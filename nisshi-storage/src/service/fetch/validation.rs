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
    time::Duration,
};

use nisshi_sans_io::{
    ErrorCode, FetchRequest, FetchResponse, IsolationLevel, RequestInput, fetch_request::FetchTopic,
};
use rama::{Layer, Service, extensions::Extensions};
use tracing::{instrument, warn};

use super::topic_error_response;
use crate::{Error, Result};

const DEFAULT_MAX_BYTES: u32 = 5 * 1024 * 1024;

/// A [`FetchRequest`] whose whole-request fields have decoded to valid values.
///
/// [`FetchService`](super::FetchService) takes this instead of a [`FetchRequest`], so it
/// never parses those fields itself.
#[derive(Clone, Debug)]
pub struct ValidatedFetchRequest {
    pub(super) isolation_level: IsolationLevel,
    pub(super) max_wait: Duration,
    pub(super) min_bytes: u32,
    pub(super) max_bytes: u32,
    pub(super) topics: Vec<FetchTopic>,
    /// The request's extensions, carried so a service behind the layer sees the same
    /// per-connection state (e.g. the authenticated principal) as one without it.
    pub(super) extensions: Extensions,
}

/// The topics of a [`FetchRequest`] that failed to convert into a [`ValidatedFetchRequest`].
#[derive(Clone, Debug)]
pub struct MalformedFetchRequest {
    field: &'static str,
    value: i64,
    topics: Vec<FetchTopic>,
}

impl MalformedFetchRequest {
    /// Builds the [`FetchResponse`] reporting `INVALID_REQUEST` for the whole request.
    ///
    /// `FetchResponse` has a top-level `error_code`, but that field is `versions: 7+`, so the
    /// same error is also mirrored onto every requested partition for clients on earlier
    /// versions.
    pub fn into_response(self) -> Result<FetchResponse> {
        self.topics
            .iter()
            .map(|topic| topic_error_response(topic, ErrorCode::InvalidRequest))
            .collect::<Result<Vec<_>>>()
            .map(|responses| {
                FetchResponse::default()
                    .throttle_time_ms(Some(0))
                    .error_code(Some(ErrorCode::InvalidRequest.into()))
                    .session_id(Some(0))
                    .node_endpoints(Some([].into()))
                    .responses(Some(responses))
            })
    }
}

impl TryFrom<FetchRequest> for ValidatedFetchRequest {
    type Error = MalformedFetchRequest;

    fn try_from(request: FetchRequest) -> Result<Self, Self::Error> {
        let Some(topics) = request.topics else {
            // A request without topics fetches nothing, so its other fields are never used.
            return Ok(Self {
                isolation_level: IsolationLevel::ReadUncommitted,
                max_wait: Duration::ZERO,
                min_bytes: 0,
                max_bytes: 0,
                topics: vec![],
                extensions: Extensions::default(),
            });
        };

        let isolation_level = request
            .isolation_level
            .map_or(
                Ok(IsolationLevel::ReadUncommitted),
                IsolationLevel::try_from,
            )
            .map_err(|_| MalformedFetchRequest {
                field: "isolation_level",
                value: request.isolation_level.map_or(0, i64::from),
                topics: topics.clone(),
            })?;

        // Stricter than Kafka, which treats a negative `max_wait_ms` as "respond
        // now" (ReplicaManager.scala). No real client sends one.
        let max_wait = u64::try_from(request.max_wait_ms)
            .map(Duration::from_millis)
            .map_err(|_| MalformedFetchRequest {
                field: "max_wait_ms",
                value: i64::from(request.max_wait_ms),
                topics: topics.clone(),
            })?;

        let min_bytes = u32::try_from(request.min_bytes).map_err(|_| MalformedFetchRequest {
            field: "min_bytes",
            value: i64::from(request.min_bytes),
            topics: topics.clone(),
        })?;

        let max_bytes = request
            .max_bytes
            .map_or(Ok(DEFAULT_MAX_BYTES), |max_bytes| {
                u32::try_from(max_bytes).map(|max_bytes| max_bytes.min(DEFAULT_MAX_BYTES))
            })
            .map_err(|_| MalformedFetchRequest {
                field: "max_bytes",
                value: request.max_bytes.map_or(0, i64::from),
                topics: topics.clone(),
            })?;

        // Kafka waits for at most `max_bytes`, so that a round that fills
        // `max_bytes` ends the wait ([KafkaApis.scala#L1043-L1044]).
        //
        // [KafkaApis.scala#L1043-L1044]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/KafkaApis.scala#L1043-L1044
        let min_bytes = min_bytes.min(max_bytes);

        Ok(Self {
            isolation_level,
            max_wait,
            min_bytes,
            max_bytes,
            topics,
            extensions: Extensions::default(),
        })
    }
}

/// A [`Layer`] converting a [`FetchRequest`] into a [`ValidatedFetchRequest`] for the wrapped
/// [`Service`], and answering a malformed one itself.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FetchValidationLayer;

impl FetchValidationLayer {
    pub fn new() -> Self {
        Self
    }
}

impl<S> Layer<S> for FetchValidationLayer {
    type Service = FetchValidationService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        FetchValidationService { inner }
    }
}

/// A [`Service`] passing a [`ValidatedFetchRequest`] to `inner`, or answering `INVALID_REQUEST`
/// for a [`FetchRequest`] that does not convert into one.
#[derive(Clone, Copy, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FetchValidationService<S> {
    inner: S,
}

impl<S> Debug for FetchValidationService<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(stringify!(FetchValidationService)).finish()
    }
}

impl<S, I> Service<I> for FetchValidationService<S>
where
    S: Service<ValidatedFetchRequest, Output = FetchResponse, Error = Error>,
    I: Into<RequestInput<FetchRequest>> + Send + 'static,
{
    type Output = FetchResponse;
    type Error = Error;

    #[instrument(skip(self, input))]
    async fn serve(&self, input: I) -> Result<Self::Output> {
        let RequestInput {
            request,
            extensions,
        } = input.into();

        match ValidatedFetchRequest::try_from(request) {
            Ok(request) => {
                self.inner
                    .serve(ValidatedFetchRequest {
                        extensions,
                        ..request
                    })
                    .await
            }

            Err(malformed) => {
                warn!(
                    field = malformed.field,
                    value = malformed.value,
                    "malformed fetch request, answering INVALID_REQUEST"
                );
                malformed.into_response()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use nisshi_sans_io::{ErrorCode, FetchRequest, FetchResponse, RequestInput};
    use rama::{Layer as _, Service as _, extensions::Extensions};

    use super::FetchValidationLayer;
    use crate::{Error, Result};

    #[derive(Clone, Debug, Default)]
    struct CountingInner {
        calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    }

    impl rama::Service<super::ValidatedFetchRequest> for CountingInner {
        type Output = FetchResponse;
        type Error = Error;

        async fn serve(&self, _input: super::ValidatedFetchRequest) -> Result<Self::Output> {
            _ = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

            Ok(FetchResponse::default()
                .error_code(Some(ErrorCode::None.into()))
                .responses(Some([].into())))
        }
    }

    #[tokio::test]
    async fn malformed_isolation_level_short_circuits() -> Result<()> {
        let inner = CountingInner::default();
        let service = FetchValidationLayer::new().layer(inner.clone());

        let request = FetchRequest::default()
            .isolation_level(Some(111))
            .max_wait_ms(0)
            .min_bytes(0)
            .max_bytes(None)
            .topics(Some(
                [nisshi_sans_io::fetch_request::FetchTopic::default()
                    .topic(Some("t".into()))
                    .partitions(Some(
                        [nisshi_sans_io::fetch_request::FetchPartition::default().partition(0)]
                            .into(),
                    ))]
                .into(),
            ));

        let response = service
            .serve(RequestInput {
                request,
                extensions: Extensions::default(),
            })
            .await?;

        assert_eq!(Some(ErrorCode::InvalidRequest.into()), response.error_code);
        assert_eq!(0, inner.calls.load(std::sync::atomic::Ordering::SeqCst));

        let responses = response.responses.expect("responses");
        assert_eq!(1, responses.len());

        let partitions = responses[0].partitions.as_ref().expect("partitions");
        assert_eq!(
            ErrorCode::InvalidRequest,
            ErrorCode::try_from(partitions[0].error_code)?
        );

        Ok(())
    }

    #[tokio::test]
    async fn well_formed_request_calls_inner() -> Result<()> {
        let inner = CountingInner::default();
        let service = FetchValidationLayer::new().layer(inner.clone());

        let request = FetchRequest::default()
            .isolation_level(Some(0))
            .max_wait_ms(0)
            .min_bytes(0)
            .max_bytes(None)
            .topics(Some([].into()));

        let response = service
            .serve(RequestInput {
                request,
                extensions: Extensions::default(),
            })
            .await?;

        assert_eq!(Some(ErrorCode::None.into()), response.error_code);
        assert_eq!(1, inner.calls.load(std::sync::atomic::Ordering::SeqCst));

        Ok(())
    }

    /// `min_bytes` above `max_bytes` waits for no more than `max_bytes`, as
    /// Kafka caps it, so that a round that fills `max_bytes` ends the wait.
    #[test]
    fn min_bytes_is_capped_at_max_bytes() -> Result<()> {
        for (min_bytes, max_bytes, expected) in [
            (i32::MAX, Some(1024), 1024),
            (i32::MAX, None, super::DEFAULT_MAX_BYTES),
            (100, Some(1024), 100),
        ] {
            let validated = super::ValidatedFetchRequest::try_from(
                FetchRequest::default()
                    .isolation_level(Some(0))
                    .min_bytes(min_bytes)
                    .max_bytes(max_bytes)
                    .topics(Some([].into())),
            )
            .expect("a well-formed request");

            assert_eq!(expected, validated.min_bytes, "{min_bytes}, {max_bytes:?}");
        }

        Ok(())
    }

    #[tokio::test]
    async fn no_topics_calls_inner_without_validating() -> Result<()> {
        let inner = CountingInner::default();
        let service = FetchValidationLayer::new().layer(inner.clone());

        let request = FetchRequest::default()
            .isolation_level(Some(111))
            .max_wait_ms(0)
            .min_bytes(0)
            .max_bytes(None)
            .topics(None);

        let response = service
            .serve(RequestInput {
                request,
                extensions: Extensions::default(),
            })
            .await?;

        assert_eq!(Some(ErrorCode::None.into()), response.error_code);
        assert_eq!(1, inner.calls.load(std::sync::atomic::Ordering::SeqCst));

        Ok(())
    }

    fn request_with(
        isolation_level: Option<i8>,
        max_wait_ms: i32,
        min_bytes: i32,
        max_bytes: Option<i32>,
    ) -> FetchRequest {
        FetchRequest::default()
            .isolation_level(isolation_level)
            .max_wait_ms(max_wait_ms)
            .min_bytes(min_bytes)
            .max_bytes(max_bytes)
            .topics(Some(
                [nisshi_sans_io::fetch_request::FetchTopic::default()
                    .topic(Some("t".into()))
                    .partitions(Some(
                        [nisshi_sans_io::fetch_request::FetchPartition::default().partition(0)]
                            .into(),
                    ))]
                .into(),
            ))
    }

    /// Every malformed whole-request field is answered `INVALID_REQUEST` without calling the
    /// inner service, and the answer survives an encode/decode round trip at the versions
    /// that differ in how the response is laid out.
    #[tokio::test]
    async fn each_malformed_field_is_invalid_request_and_encodes() -> Result<()> {
        use nisshi_sans_io::{ApiKey as _, Frame, Header};

        for request in [
            request_with(Some(7), 0, 0, None),
            request_with(Some(0), -1, 0, None),
            request_with(Some(0), 0, -1, None),
            request_with(Some(0), 0, 0, Some(-1)),
        ] {
            let inner = CountingInner::default();
            let service = FetchValidationLayer::new().layer(inner.clone());

            let response = service
                .serve(RequestInput {
                    request,
                    extensions: Extensions::default(),
                })
                .await?;

            assert_eq!(0, inner.calls.load(std::sync::atomic::Ordering::SeqCst));
            assert_eq!(Some(ErrorCode::InvalidRequest.into()), response.error_code);

            let responses = response.responses.as_ref().expect("responses");
            assert_eq!(1, responses.len());

            let partitions = responses[0].partitions.as_ref().expect("partitions");
            assert_eq!(
                ErrorCode::InvalidRequest,
                ErrorCode::try_from(partitions[0].error_code)?
            );

            for version in [4, 7, 11, 12, 13] {
                let encoded = Frame::response(
                    Header::Response { correlation_id: 0 },
                    response.clone().into(),
                    FetchResponse::KEY,
                    version,
                )?;

                _ = Frame::response_from_bytes(encoded, FetchResponse::KEY, version)
                    .unwrap_or_else(|err| panic!("decode at v{version}: {err}"));
            }
        }

        Ok(())
    }
}
