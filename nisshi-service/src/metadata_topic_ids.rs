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

use std::fmt;

use nisshi_sans_io::{
    Body, ErrorCode, Frame, FrameInput, Header, MetadataRequest, MetadataResponse,
    metadata_response::MetadataResponseTopic,
};
use rama::{Layer, Service};
use tracing::{debug, instrument};

/// The first Metadata version whose request may name a topic by its ID, or leave its name null.
const FIRST_TOPIC_ID_METADATA_VERSION: i16 = 12;

/// A [`Layer`] for the Metadata route that answers `INVALID_REQUEST` to a Metadata v10 or v11
/// request with a null topic name or a topic ID.
///
/// The Metadata request schema allows both from v10, but Kafka implemented them only at v12, and
/// its schema says "Versions 10 and 11 should not use the topicId field or set topic name to
/// null" ([MetadataRequest.json#L36-L37]). So its broker refuses such a request with
/// `INVALID_REQUEST` ([KafkaApis.scala#L1320-L1329]), and answers with one topic per requested
/// topic that carries the error, the requested ID, and an empty string for a null name, since the
/// response name is not nullable before v12 ([MetadataRequest.java#L142-L160]).
///
/// Without this, the storage answers a topic ID it doesn't know with a topic that has no name,
/// and at v10 and v11, where the response name is not nullable, the encoder writes nothing for
/// it, so the frame that goes out doesn't decode.
///
/// [MetadataRequest.json#L36-L37]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/resources/common/message/MetadataRequest.json#L36-L37
/// [KafkaApis.scala#L1320-L1329]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/KafkaApis.scala#L1320-L1329
/// [MetadataRequest.java#L142-L160]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/common/requests/MetadataRequest.java#L142-L160
#[derive(Clone, Copy, Debug, Default)]
pub struct MetadataTopicIdLayer;

impl<S> Layer<S> for MetadataTopicIdLayer {
    type Service = MetadataTopicIdService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        MetadataTopicIdService { inner }
    }
}

/// A [`Service`] built by [`MetadataTopicIdLayer`].
#[derive(Clone)]
pub struct MetadataTopicIdService<S> {
    inner: S,
}

impl<S> fmt::Debug for MetadataTopicIdService<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(stringify!(MetadataTopicIdService)).finish()
    }
}

impl<S> Service<FrameInput> for MetadataTopicIdService<S>
where
    S: Service<FrameInput, Output = Frame>,
    S::Error: From<nisshi_sans_io::Error>,
{
    type Output = Frame;
    type Error = S::Error;

    #[instrument(skip_all)]
    async fn serve(&self, req: FrameInput) -> Result<Self::Output, Self::Error> {
        if let Body::MetadataRequest(ref request) = req.frame.body
            && let Some(response) = refuse_topic_ids(req.frame.api_version()?, request)
        {
            return Ok(Frame {
                size: 0,
                header: Header::Response {
                    correlation_id: req.frame.correlation_id()?,
                },
                body: response.into(),
            });
        }

        self.inner.serve(req).await
    }
}

/// Returns the `INVALID_REQUEST` response that Kafka answers to `request` at `api_version`, or
/// `None` when Kafka serves it.
///
/// [`MetadataTopicIdLayer`] calls this, and a test that serves a Metadata request to
/// `MetadataService` without the layer, as a fuzz target does, can call it first so that it
/// sends only what the layer lets through.
pub fn refuse_topic_ids(api_version: i16, request: &MetadataRequest) -> Option<MetadataResponse> {
    if api_version < FIRST_TOPIC_ID_METADATA_VERSION && names_by_id_or_null(request) {
        debug!(
            api_version,
            "refusing a topic ID or null topic name before Metadata v12"
        );

        Some(invalid_request(request))
    } else {
        None
    }
}

/// Whether any topic of `request` has no name, or a topic ID other than the zero one.
fn names_by_id_or_null(request: &MetadataRequest) -> bool {
    request
        .topics
        .iter()
        .flatten()
        .any(|topic| topic.name.is_none() || topic.topic_id.is_some_and(|id| id != [0; 16]))
}

/// Kafka's `MetadataRequest.getErrorResponse` for `INVALID_REQUEST`.
fn invalid_request(request: &MetadataRequest) -> MetadataResponse {
    MetadataResponse::default()
        .throttle_time_ms(Some(0))
        .brokers(Some(Vec::new()))
        .controller_id(Some(-1))
        .cluster_authorized_operations(Some(i32::MIN))
        .topics(Some(
            request
                .topics
                .iter()
                .flatten()
                .map(|topic| {
                    MetadataResponseTopic::default()
                        .error_code(ErrorCode::InvalidRequest.into())
                        .name(Some(topic.name.clone().unwrap_or_default()))
                        .topic_id(Some(topic.topic_id.unwrap_or([0; 16])))
                        .is_internal(Some(false))
                        .partitions(Some(Vec::new()))
                        .topic_authorized_operations(Some(i32::MIN))
                })
                .collect(),
        ))
}

#[cfg(test)]
mod tests {
    use nisshi_sans_io::{ApiKey as _, metadata_request::MetadataRequestTopic};
    use rama::extensions::Extensions;

    use super::*;
    use crate::Error;

    /// A handler that answers an empty Metadata response, and so shows the request got through.
    struct Pass;

    impl Service<FrameInput> for Pass {
        type Output = Frame;
        type Error = Error;

        async fn serve(&self, _req: FrameInput) -> Result<Self::Output, Self::Error> {
            Ok(Frame {
                size: 0,
                header: Header::Response { correlation_id: 6 },
                body: MetadataResponse::default().into(),
            })
        }
    }

    async fn serve(api_version: i16, topic: MetadataRequestTopic) -> Result<Frame, Error> {
        MetadataTopicIdLayer
            .layer(Pass)
            .serve(FrameInput {
                frame: Frame {
                    size: 0,
                    header: Header::Request {
                        api_key: MetadataRequest::KEY,
                        api_version,
                        correlation_id: 6,
                        client_id: None,
                    },
                    body: MetadataRequest::default().topics(Some(vec![topic])).into(),
                },
                extensions: Extensions::default(),
            })
            .await
    }

    fn unnamed() -> MetadataRequestTopic {
        MetadataRequestTopic::default()
            .topic_id(Some([7; 16]))
            .name(None)
    }

    fn topics(frame: &Frame) -> Vec<MetadataResponseTopic> {
        let Body::MetadataResponse(ref response) = frame.body else {
            panic!("a metadata response: {frame:?}");
        };

        response.topics.clone().unwrap_or_default()
    }

    /// The error response encodes and decodes at the version it answers.
    fn encodes(api_version: i16, frame: Frame) -> Result<(), Error> {
        let bytes = Frame::response(frame.header, frame.body, MetadataRequest::KEY, api_version)?;
        _ = Frame::response_from_bytes(bytes, MetadataRequest::KEY, api_version)?;
        Ok(())
    }

    #[tokio::test]
    async fn an_unnamed_topic_is_invalid_at_v10_and_v11() -> Result<(), Error> {
        for api_version in [10, 11] {
            let frame = serve(api_version, unnamed()).await?;

            let [topic] = topics(&frame).try_into().expect("one topic");
            assert_eq!(i16::from(ErrorCode::InvalidRequest), topic.error_code);
            assert_eq!(Some(String::new()), topic.name);
            assert_eq!(Some([7; 16]), topic.topic_id);

            encodes(api_version, frame)?;
        }

        Ok(())
    }

    #[tokio::test]
    async fn a_named_topic_with_an_id_is_invalid_at_v10_and_v11() -> Result<(), Error> {
        for api_version in [10, 11] {
            let frame = serve(api_version, unnamed().name(Some("orders".into()))).await?;

            let [topic] = topics(&frame).try_into().expect("one topic");
            assert_eq!(i16::from(ErrorCode::InvalidRequest), topic.error_code);
            assert_eq!(Some("orders".into()), topic.name);
            encodes(api_version, frame)?;
        }

        Ok(())
    }

    #[tokio::test]
    async fn a_named_topic_goes_through_before_v12() -> Result<(), Error> {
        for api_version in 0..=11 {
            let named = MetadataRequestTopic::default().name(Some("orders".into()));

            assert_eq!(
                Vec::<MetadataResponseTopic>::new(),
                topics(&serve(api_version, named).await?)
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn v12_may_name_a_topic_by_its_id() -> Result<(), Error> {
        assert_eq!(
            Vec::<MetadataResponseTopic>::new(),
            topics(&serve(12, unnamed()).await?)
        );

        Ok(())
    }
}
