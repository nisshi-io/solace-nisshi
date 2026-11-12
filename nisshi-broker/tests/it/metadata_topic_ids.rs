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

//! A Metadata v10 or v11 request that names a topic by its ID, or leaves its name null, over a
//! real connection: the broker answers `INVALID_REQUEST` in a frame that decodes at that
//! version, as Kafka does, and a request that names a topic reaches the storage. A v0 request with
//! a null list of topics gets no response, as in Kafka, which closes the connection.
//!
//! The check is independent of the storage backend, so only the memory backend is used.

use std::{assert_matches, time::Duration};

use anyhow::{Result, anyhow};
use nisshi_sans_io::{
    ApiKey as _, ErrorCode, Frame, Header, MetadataRequest, MetadataResponse, NULL_TOPIC_ID,
    metadata_request::MetadataRequestTopic,
};
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::TcpStream,
    time::timeout,
};

use crate::common::{
    alphanumeric_string, init_tracing,
    wire::{round_trip, spawn_broker},
};

const TEST_TIMEOUT: Duration = Duration::from_secs(30);

async fn metadata(
    stream: &mut TcpStream,
    api_version: i16,
    correlation_id: i32,
    topic: MetadataRequestTopic,
) -> Result<MetadataResponse> {
    let frame = round_trip(
        stream,
        MetadataRequest::KEY,
        api_version,
        correlation_id,
        MetadataRequest::default()
            .topics(Some(vec![topic]))
            .allow_auto_topic_creation(Some(false))
            .include_cluster_authorized_operations(Some(false))
            .include_topic_authorized_operations(Some(false))
            .into(),
    )
    .await?;

    MetadataResponse::try_from(frame.body).map_err(Into::into)
}

fn only_topic(response: MetadataResponse) -> Result<(ErrorCode, Option<String>)> {
    let topics = response.topics.unwrap_or_default();
    assert_eq!(1, topics.len());

    Ok((
        ErrorCode::try_from(topics[0].error_code)?,
        topics[0].name.clone(),
    ))
}

#[tokio::test]
async fn a_null_topic_name_is_refused_before_v12_and_the_connection_survives() -> Result<()> {
    let _guard = init_tracing()?;

    timeout(TEST_TIMEOUT, async {
        let broker = spawn_broker(None).await?;
        let mut stream = TcpStream::connect(broker.addr).await?;

        let mut correlation_id = 0;

        for api_version in [10, 11] {
            // With the zero ID, only the null name is left to refuse.
            for topic_id in [[7; 16], NULL_TOPIC_ID] {
                correlation_id += 1;

                let response = metadata(
                    &mut stream,
                    api_version,
                    correlation_id,
                    MetadataRequestTopic::default()
                        .topic_id(Some(topic_id))
                        .name(None),
                )
                .await?;

                assert_matches!(
                    only_topic(response)?,
                    (ErrorCode::InvalidRequest, Some(name)) if name.is_empty(),
                    "v{api_version} {topic_id:?}"
                );
            }
        }

        Ok::<_, anyhow::Error>(())
    })
    .await
    .map_err(|_| anyhow!("timed out after {TEST_TIMEOUT:?}"))?
}

#[tokio::test]
async fn an_unknown_named_topic_with_the_zero_id_reaches_the_storage() -> Result<()> {
    let _guard = init_tracing()?;

    timeout(TEST_TIMEOUT, async {
        let broker = spawn_broker(None).await?;
        let mut stream = TcpStream::connect(broker.addr).await?;

        let name = alphanumeric_string(15);

        let response = metadata(
            &mut stream,
            11,
            1,
            MetadataRequestTopic::default()
                .name(Some(name.clone()))
                .topic_id(Some(NULL_TOPIC_ID)),
        )
        .await?;

        assert_eq!(
            (ErrorCode::UnknownTopicOrPartition, Some(name)),
            only_topic(response)?
        );

        Ok::<_, anyhow::Error>(())
    })
    .await
    .map_err(|_| anyhow!("timed out after {TEST_TIMEOUT:?}"))?
}

/// The topic list is nullable from v1, so Kafka fails to decode a v0 request that sends it null,
/// and closes the connection without a response ([`RequestContext.parseRequest`],
/// [`SocketServer.processCompletedReceives`]).
///
/// [`RequestContext.parseRequest`]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/common/requests/RequestContext.java#L111-L131
/// [`SocketServer.processCompletedReceives`]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/network/SocketServer.scala#L1129-L1180
#[tokio::test]
async fn a_null_list_at_v0_closes_the_connection() -> Result<()> {
    let _guard = init_tracing()?;

    timeout(TEST_TIMEOUT, async {
        let broker = spawn_broker(None).await?;

        // The encoder writes an empty list for a null one at v0, so encode an empty list and
        // set its length, the last four bytes of the frame, to -1.
        let mut request = Frame::request(
            Header::Request {
                api_key: MetadataRequest::KEY,
                api_version: 0,
                correlation_id: 1,
                client_id: Some(env!("CARGO_PKG_NAME").into()),
            },
            MetadataRequest::default().topics(Some(Vec::new())).into(),
        )?
        .to_vec();
        let length = request.len() - 4;
        request[length..].copy_from_slice(&(-1_i32).to_be_bytes());

        let mut stream = TcpStream::connect(broker.addr).await?;
        stream.write_all(&request).await?;
        stream.flush().await?;

        let mut response = Vec::new();
        _ = stream.read_to_end(&mut response).await?;
        assert!(
            response.is_empty(),
            "a v0 request with a null list got a response: {response:02x?}"
        );

        Ok::<_, anyhow::Error>(())
    })
    .await
    .map_err(|_| anyhow!("timed out after {TEST_TIMEOUT:?}"))?
}
