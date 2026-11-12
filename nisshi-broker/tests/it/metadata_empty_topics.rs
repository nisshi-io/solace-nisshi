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

//! A Metadata request for an empty list of topics over a real connection: from v1 the broker
//! answers the brokers and no topic, and at v0, where an empty list means all topics, it answers
//! every topic, as Kafka does ([`MetadataRequest.isAllTopics`]). A null list asks for all topics
//! at every version that can write one.
//!
//! The check is independent of the storage backend, so only the memory backend is used. The
//! storage-level contract is in `metadata.rs`.
//!
//! [`MetadataRequest.isAllTopics`]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/common/requests/MetadataRequest.java#L162-L165

use std::{collections::BTreeSet, time::Duration};

use anyhow::{Result, anyhow};
use nisshi_sans_io::{
    ApiKey as _, CreateTopicsRequest, CreateTopicsResponse, ErrorCode, MetadataRequest,
    MetadataResponse, create_topics_request::CreatableTopic,
    metadata_request::MetadataRequestTopic,
};
use tokio::{net::TcpStream, time::timeout};

use crate::common::{
    alphanumeric_string, init_tracing,
    wire::{round_trip, spawn_broker},
};

const TEST_TIMEOUT: Duration = Duration::from_secs(30);

const CREATE_TOPICS_VERSION: i16 = 7;

async fn create_topics(stream: &mut TcpStream, names: &BTreeSet<String>) -> Result<()> {
    let frame = round_trip(
        stream,
        CreateTopicsRequest::KEY,
        CREATE_TOPICS_VERSION,
        1,
        CreateTopicsRequest::default()
            .topics(Some(
                names
                    .iter()
                    .map(|name| {
                        CreatableTopic::default()
                            .name(name.clone())
                            .num_partitions(1)
                            .replication_factor(1)
                            .assignments(Some([].into()))
                            .configs(Some([].into()))
                    })
                    .collect(),
            ))
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
    correlation_id: i32,
    topics: Option<Vec<MetadataRequestTopic>>,
) -> Result<MetadataResponse> {
    let frame = round_trip(
        stream,
        MetadataRequest::KEY,
        api_version,
        correlation_id,
        MetadataRequest::default()
            .topics(topics)
            // the broker would create a topic that a client names and it lacks, so this shows
            // that an empty list names none
            .allow_auto_topic_creation(Some(true))
            .include_cluster_authorized_operations(Some(false))
            .include_topic_authorized_operations(Some(false))
            .into(),
    )
    .await?;

    MetadataResponse::try_from(frame.body).map_err(Into::into)
}

fn names(response: &MetadataResponse) -> BTreeSet<String> {
    response
        .topics
        .iter()
        .flatten()
        .filter_map(|topic| topic.name.clone())
        .collect()
}

#[tokio::test]
async fn an_empty_list_asks_for_no_topics_from_v1() -> Result<()> {
    let _guard = init_tracing()?;

    timeout(TEST_TIMEOUT, async {
        let broker = spawn_broker(None).await?;
        let mut stream = TcpStream::connect(broker.addr).await?;

        let created = BTreeSet::from([alphanumeric_string(15), alphanumeric_string(15)]);
        create_topics(&mut stream, &created).await?;

        let mut correlation_id = 1;

        // 1 is the first version that can ask for no topics, 4 the first with
        // allow_auto_topic_creation, 9 the first flexible version, and 12 the last version.
        for api_version in [1, 4, 9, 12] {
            correlation_id += 1;
            let none = metadata(&mut stream, api_version, correlation_id, Some(Vec::new())).await?;

            assert_eq!(
                1,
                none.brokers.iter().flatten().count(),
                "v{api_version}: the brokers are still answered"
            );
            assert_eq!(
                BTreeSet::new(),
                names(&none),
                "v{api_version}: an empty list asks for no topics"
            );

            correlation_id += 1;
            let all = metadata(&mut stream, api_version, correlation_id, None).await?;

            assert_eq!(
                created,
                names(&all),
                "v{api_version}: a null list asks for all topics, and the empty list created none"
            );
        }

        Ok::<_, anyhow::Error>(())
    })
    .await
    .map_err(|_| anyhow!("timed out after {TEST_TIMEOUT:?}"))?
}

#[tokio::test]
async fn an_empty_list_asks_for_all_topics_at_v0() -> Result<()> {
    let _guard = init_tracing()?;

    timeout(TEST_TIMEOUT, async {
        let broker = spawn_broker(None).await?;
        let mut stream = TcpStream::connect(broker.addr).await?;

        let created = BTreeSet::from([alphanumeric_string(15), alphanumeric_string(15)]);
        create_topics(&mut stream, &created).await?;

        let response = metadata(&mut stream, 0, 2, Some(Vec::new())).await?;

        assert_eq!(1, response.brokers.iter().flatten().count());
        assert_eq!(created, names(&response));

        Ok::<_, anyhow::Error>(())
    })
    .await
    .map_err(|_| anyhow!("timed out after {TEST_TIMEOUT:?}"))?
}
