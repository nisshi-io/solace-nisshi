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

//! The authorized operations of a Metadata response over a real connection. The broker has no
//! authorizer, so as Kafka does without one it reports every operation that the cluster and each
//! topic support, when the request asks for them, and leaves the field at "omitted" otherwise
//! ([`KafkaApis.handleTopicMetadataRequest`]).
//!
//! The check is independent of the storage backend, so only the memory backend is used.
//!
//! [`KafkaApis.handleTopicMetadataRequest`]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/KafkaApis.scala#L1396-L1419

use std::time::Duration;

use anyhow::{Result, anyhow};
use nisshi_sans_io::{
    ApiKey as _, CreateTopicsRequest, CreateTopicsResponse, ErrorCode, MetadataRequest,
    MetadataResponse, NULL_TOPIC_ID, create_topics_request::CreatableTopic,
    metadata_request::MetadataRequestTopic, metadata_response::MetadataResponseTopic,
};
use tokio::{net::TcpStream, time::timeout};

use crate::common::{
    alphanumeric_string, init_tracing,
    wire::{round_trip, spawn_broker},
};

const TEST_TIMEOUT: Duration = Duration::from_secs(30);

const OMITTED: i32 = i32::MIN;

/// Every operation that a cluster supports without an authorizer.
const CLUSTER: i32 = 8096;

/// Every operation that a topic supports without an authorizer.
const TOPIC: i32 = 3576;

async fn create_topic(stream: &mut TcpStream, name: &str) -> Result<()> {
    let frame = round_trip(
        stream,
        CreateTopicsRequest::KEY,
        7,
        1,
        CreateTopicsRequest::default()
            .topics(Some(vec![
                CreatableTopic::default()
                    .name(name.into())
                    .num_partitions(1)
                    .replication_factor(1)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
            ]))
            .timeout_ms(30_000)
            .validate_only(Some(false))
            .into(),
    )
    .await?;

    for topic in CreateTopicsResponse::try_from(frame.body)?
        .topics
        .unwrap_or_default()
    {
        assert_eq!(i16::from(ErrorCode::None), topic.error_code, "{topic:?}");
    }

    Ok(())
}

async fn metadata(
    stream: &mut TcpStream,
    api_version: i16,
    include: bool,
    topics: Vec<MetadataRequestTopic>,
) -> Result<MetadataResponse> {
    let frame = round_trip(
        stream,
        MetadataRequest::KEY,
        api_version,
        100 + i32::from(api_version),
        MetadataRequest::default()
            .topics(Some(topics))
            // a version before 4 has no such field, and the broker treats it as true, so a
            // request for an unknown name at one of those versions creates the topic
            .allow_auto_topic_creation(Some(false))
            .include_cluster_authorized_operations(Some(include))
            .include_topic_authorized_operations(Some(include))
            .into(),
    )
    .await?;

    MetadataResponse::try_from(frame.body).map_err(Into::into)
}

/// A topic that the request names, with the zero topic ID that a v10 or later client writes for it.
fn named(name: &str) -> MetadataRequestTopic {
    MetadataRequestTopic::default()
        .name(Some(name.into()))
        .topic_id(Some(NULL_TOPIC_ID))
}

fn row<'a>(response: &'a MetadataResponse, name: &str) -> Result<&'a MetadataResponseTopic> {
    response
        .topics
        .iter()
        .flatten()
        .find(|topic| topic.name.as_deref() == Some(name))
        .ok_or_else(|| anyhow!("no row for {name}: {response:?}"))
}

#[tokio::test]
async fn the_cluster_operations_are_reported_only_when_asked_for_at_v8_to_v10() -> Result<()> {
    let _guard = init_tracing()?;

    timeout(TEST_TIMEOUT, async {
        let broker = spawn_broker(None).await?;
        let mut stream = TcpStream::connect(broker.addr).await?;

        for api_version in 0..=12 {
            for include in [false, true] {
                let response = metadata(&mut stream, api_version, include, vec![]).await?;

                // the field is on the wire only at v8 to v10
                let expected = match (api_version, include) {
                    (8..=10, true) => Some(CLUSTER),
                    (8..=10, false) => Some(OMITTED),
                    _ => None,
                };

                assert_eq!(
                    expected, response.cluster_authorized_operations,
                    "v{api_version} include={include}"
                );
            }
        }

        Ok::<_, anyhow::Error>(())
    })
    .await
    .map_err(|_| anyhow!("timed out after {TEST_TIMEOUT:?}"))?
}

#[tokio::test]
async fn the_topic_operations_are_reported_only_when_asked_for_from_v8() -> Result<()> {
    let _guard = init_tracing()?;

    timeout(TEST_TIMEOUT, async {
        let broker = spawn_broker(None).await?;
        let mut stream = TcpStream::connect(broker.addr).await?;

        let existing = alphanumeric_string(15);
        create_topic(&mut stream, &existing).await?;

        for api_version in 0..=12 {
            for include in [false, true] {
                let response =
                    metadata(&mut stream, api_version, include, vec![named(&existing)]).await?;

                // the field is on the wire from v8
                let expected = match (api_version, include) {
                    (8.., true) => Some(TOPIC),
                    (8.., false) => Some(OMITTED),
                    _ => None,
                };

                assert_eq!(
                    expected,
                    row(&response, &existing)?.topic_authorized_operations,
                    "v{api_version} include={include}"
                );
            }
        }

        Ok::<_, anyhow::Error>(())
    })
    .await
    .map_err(|_| anyhow!("timed out after {TEST_TIMEOUT:?}"))?
}

/// Kafka sets the operations on every row that it looked up by name, so an unknown name and an
/// invalid one get them too.
#[tokio::test]
async fn an_unknown_or_invalid_name_carries_the_topic_operations_when_asked_for() -> Result<()> {
    let _guard = init_tracing()?;

    timeout(TEST_TIMEOUT, async {
        let broker = spawn_broker(None).await?;
        let mut stream = TcpStream::connect(broker.addr).await?;

        // from v4 a request can refuse auto-creation, and from v8 the field is on the wire
        for api_version in 8..=12 {
            let unknown = alphanumeric_string(15);
            let invalid = format!("{} ", alphanumeric_string(15));

            let response = metadata(
                &mut stream,
                api_version,
                true,
                vec![named(&unknown), named(&invalid)],
            )
            .await?;

            let unknown = row(&response, &unknown)?;
            assert_eq!(
                i16::from(ErrorCode::UnknownTopicOrPartition),
                unknown.error_code,
                "v{api_version}"
            );
            assert_eq!(
                Some(TOPIC),
                unknown.topic_authorized_operations,
                "v{api_version}"
            );

            let invalid = row(&response, &invalid)?;
            assert_eq!(
                i16::from(ErrorCode::InvalidTopicException),
                invalid.error_code,
                "v{api_version}"
            );
            assert_eq!(
                Some(TOPIC),
                invalid.topic_authorized_operations,
                "v{api_version}"
            );
        }

        Ok::<_, anyhow::Error>(())
    })
    .await
    .map_err(|_| anyhow!("timed out after {TEST_TIMEOUT:?}"))?
}

/// Kafka builds the row for an unknown topic ID outside the rows that it sets the operations on,
/// and the row has no name.
#[tokio::test]
async fn an_unknown_topic_id_omits_the_topic_operations_at_v12() -> Result<()> {
    let _guard = init_tracing()?;

    timeout(TEST_TIMEOUT, async {
        let broker = spawn_broker(None).await?;
        let mut stream = TcpStream::connect(broker.addr).await?;

        let response = metadata(
            &mut stream,
            12,
            true,
            vec![MetadataRequestTopic::default().topic_id(Some([7; 16]))],
        )
        .await?;

        let topics = response.topics.unwrap_or_default();
        assert_eq!(1, topics.len(), "{topics:?}");
        assert_eq!(None, topics[0].name);
        assert_eq!(i16::from(ErrorCode::UnknownTopicId), topics[0].error_code);
        assert_eq!(Some(OMITTED), topics[0].topic_authorized_operations);

        Ok::<_, anyhow::Error>(())
    })
    .await
    .map_err(|_| anyhow!("timed out after {TEST_TIMEOUT:?}"))?
}
