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
    Frame, FrameInput, MetadataRequest, MetadataResponse, NULL_TOPIC_ID, RequestInput,
    metadata_request::MetadataRequestTopic, metadata_response::MetadataResponseBroker,
};
use nisshi_service::{FrameRequestLayer, MetadataTopicIdLayer};
use rama::{
    Layer, Service,
    layer::{MapInputLayer, MapOutputLayer},
};

use crate::Error;

/// Returns the service that answers a client's Metadata request from `origin`, with the
/// brokers in the response replaced by the proxy's own `host` and `port`.
///
/// The service refuses a topic ID or a null topic name before Metadata v12 itself, as the
/// broker does. The request to `origin` carries no version from the client: it goes at the
/// version that the proxy negotiated with `origin`, so `origin` would otherwise answer a
/// request that Kafka refuses at the client's version, and the proxy would encode that answer
/// at the client's version.
pub(crate) fn service<O>(
    origin: O,
    host: String,
    port: i32,
) -> impl Service<FrameInput, Output = Frame, Error = Error> + Clone
where
    O: Service<MetadataRequest, Output = MetadataResponse, Error = Error> + Clone,
{
    (
        MetadataTopicIdLayer,
        FrameRequestLayer::<MetadataRequest>::new(),
        MapInputLayer::new(|input: RequestInput<MetadataRequest>| {
            MetadataRequest::default()
                .topics(input.request.topics.map(|topics| {
                    topics
                        .into_iter()
                        .map(|topic| {
                            MetadataRequestTopic::default()
                                .name(topic.name)
                                .topic_id(topic.topic_id.or(Some(NULL_TOPIC_ID)))
                        })
                        .collect()
                }))
                .allow_auto_topic_creation(input.request.allow_auto_topic_creation.or(Some(false)))
                .include_cluster_authorized_operations(
                    input.request.include_cluster_authorized_operations,
                )
                .include_topic_authorized_operations(
                    input
                        .request
                        .include_topic_authorized_operations
                        .or(Some(false)),
                )
        }),
        MapOutputLayer::new(move |response: MetadataResponse| {
            let brokers = response.brokers.as_ref().map(|brokers| {
                brokers
                    .iter()
                    .map(|broker| {
                        MetadataResponseBroker::default()
                            .node_id(broker.node_id)
                            .host(host.clone())
                            .port(port)
                            .rack(broker.rack.clone())
                    })
                    .collect()
            });

            response.brokers(brokers)
        }),
    )
        .into_layer(origin)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use nisshi_sans_io::{ApiKey as _, Body, ErrorCode, Header};
    use rama::extensions::Extensions;

    use super::*;

    /// An origin that answers one broker and counts the requests that reach it.
    #[derive(Clone, Default)]
    struct Origin {
        requests: Arc<AtomicUsize>,
        topics: Arc<Mutex<Option<Option<Vec<MetadataRequestTopic>>>>>,
    }

    impl Service<MetadataRequest> for Origin {
        type Output = MetadataResponse;
        type Error = Error;

        async fn serve(&self, req: MetadataRequest) -> Result<Self::Output, Self::Error> {
            _ = self.requests.fetch_add(1, Ordering::SeqCst);
            *self.topics.lock().expect("lock") = Some(req.topics);

            Ok(MetadataResponse::default().brokers(Some(vec![
                MetadataResponseBroker::default()
                    .node_id(1)
                    .host("origin".into())
                    .port(9092),
            ])))
        }
    }

    async fn serve(
        origin: &Origin,
        api_version: i16,
        topic: MetadataRequestTopic,
    ) -> Result<Frame, Error> {
        serve_topics(origin, api_version, Some(vec![topic])).await
    }

    async fn serve_topics(
        origin: &Origin,
        api_version: i16,
        topics: Option<Vec<MetadataRequestTopic>>,
    ) -> Result<Frame, Error> {
        service(origin.clone(), "proxy".into(), 19092)
            .serve(FrameInput {
                frame: Frame {
                    size: 0,
                    header: Header::Request {
                        api_key: MetadataRequest::KEY,
                        api_version,
                        correlation_id: 12,
                        client_id: None,
                    },
                    body: MetadataRequest::default().topics(topics).into(),
                },
                extensions: Extensions::default(),
            })
            .await
    }

    #[tokio::test]
    async fn a_topic_id_before_v12_is_refused_without_asking_the_origin() -> Result<(), Error> {
        let origin = Origin::default();

        for api_version in [10, 11] {
            // With the zero ID or none, only the null name is left to refuse.
            for topic_id in [Some([7; 16]), Some(NULL_TOPIC_ID), None] {
                let frame = serve(
                    &origin,
                    api_version,
                    MetadataRequestTopic::default()
                        .name(None)
                        .topic_id(topic_id),
                )
                .await?;

                let Body::MetadataResponse(response) = frame.body else {
                    panic!("a metadata response");
                };

                let topics = response.topics.unwrap_or_default();
                assert_eq!(1, topics.len(), "v{api_version} {topic_id:?}");
                assert_eq!(
                    i16::from(ErrorCode::InvalidRequest),
                    topics[0].error_code,
                    "v{api_version} {topic_id:?}"
                );
                assert_eq!(Some(String::new()), topics[0].name);
            }
        }

        assert_eq!(0, origin.requests.load(Ordering::SeqCst));

        Ok(())
    }

    /// The request to the origin goes at the version that the proxy negotiated with it, not the
    /// client's, so the origin would read a v0 client's empty list, which means all topics, as
    /// no topics. From v1 the empty list means no topics at both, and must reach the origin as
    /// it is, or every producer's bootstrap through the proxy would list all topics there.
    #[tokio::test]
    async fn an_empty_list_reaches_the_origin_as_all_topics_at_v0_only() -> Result<(), Error> {
        let origin = Origin::default();

        _ = serve_topics(&origin, 0, Some(Vec::new())).await?;
        assert_eq!(Some(None), origin.topics.lock().expect("lock").take());

        for api_version in [1, 12] {
            _ = serve_topics(&origin, api_version, Some(Vec::new())).await?;
            assert_eq!(
                Some(Some(Vec::new())),
                origin.topics.lock().expect("lock").take(),
                "v{api_version}"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn a_named_topic_goes_to_the_origin_and_the_response_names_the_proxy() -> Result<(), Error>
    {
        let origin = Origin::default();

        let frame = serve(
            &origin,
            11,
            MetadataRequestTopic::default().name(Some("orders".into())),
        )
        .await?;

        let Body::MetadataResponse(response) = frame.body else {
            panic!("a metadata response");
        };

        let brokers = response.brokers.unwrap_or_default();
        assert_eq!(1, brokers.len());
        assert_eq!("proxy", brokers[0].host);
        assert_eq!(19092, brokers[0].port);
        assert_eq!(1, origin.requests.load(Ordering::SeqCst));

        Ok(())
    }
}
