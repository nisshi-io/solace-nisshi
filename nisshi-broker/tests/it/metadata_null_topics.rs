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

//! A Metadata request that names topics, over a real connection to a broker on `null://`: the
//! response has a row for each name asked and none for the topics that are not asked for, and a
//! name the engine doesn't track is `UNKNOWN_TOPIC_OR_PARTITION` or, when the request allows it,
//! created, as on every other engine ([`handleTopicMetadataRequest`]).
//!
//! [`handleTopicMetadataRequest`]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/KafkaApis.scala#L1315-L1400

use std::{collections::BTreeSet, time::Duration};

use anyhow::{Result, anyhow};
use nisshi_sans_io::{
    ApiKey as _, CreateTopicsRequest, CreateTopicsResponse, ErrorCode, MetadataRequest,
    MetadataResponse, NULL_TOPIC_ID, create_topics_request::CreatableTopic,
    metadata_request::MetadataRequestTopic,
};
use tokio::{net::TcpStream, time::timeout};
use url::Url;

use crate::common::{
    alphanumeric_string, init_tracing,
    wire::{round_trip, spawn_broker_on},
};

const TEST_TIMEOUT: Duration = Duration::from_secs(30);

const CREATE_TOPICS_VERSION: i16 = 7;

async fn create_topic(stream: &mut TcpStream, name: &str) -> Result<()> {
    let frame = round_trip(
        stream,
        CreateTopicsRequest::KEY,
        CREATE_TOPICS_VERSION,
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
    correlation_id: i32,
    names: &[&str],
    allow_auto_topic_creation: bool,
) -> Result<Vec<(String, i16)>> {
    let frame = round_trip(
        stream,
        MetadataRequest::KEY,
        api_version,
        correlation_id,
        MetadataRequest::default()
            .topics(Some(
                names
                    .iter()
                    .map(|name| {
                        MetadataRequestTopic::default()
                            .name(Some((*name).into()))
                            .topic_id(Some(NULL_TOPIC_ID))
                    })
                    .collect(),
            ))
            .allow_auto_topic_creation(Some(allow_auto_topic_creation))
            .include_cluster_authorized_operations(Some(false))
            .include_topic_authorized_operations(Some(false))
            .into(),
    )
    .await?;

    Ok(MetadataResponse::try_from(frame.body)?
        .topics
        .into_iter()
        .flatten()
        .map(|topic| (topic.name.unwrap_or_default(), topic.error_code))
        .collect())
}

#[tokio::test]
async fn only_the_topics_asked_for_are_answered() -> Result<()> {
    let _guard = init_tracing()?;

    timeout(TEST_TIMEOUT, async {
        let broker = spawn_broker_on(None, false, Url::parse("null://")?).await?;
        let mut stream = TcpStream::connect(broker.addr).await?;

        let wanted = alphanumeric_string(15);
        let other = alphanumeric_string(15);
        let missing = alphanumeric_string(15);
        create_topic(&mut stream, &wanted).await?;
        create_topic(&mut stream, &other).await?;

        let none = i16::from(ErrorCode::None);
        let unknown = i16::from(ErrorCode::UnknownTopicOrPartition);
        let mut correlation_id = 1;

        // 1 is the first version that can ask for named topics only, 4 the first with
        // allow_auto_topic_creation, 9 the first flexible version, and 12 the last version.
        for api_version in [1, 4, 9, 12] {
            correlation_id += 1;
            assert_eq!(
                vec![(wanted.clone(), none)],
                metadata(&mut stream, api_version, correlation_id, &[&wanted], false).await?,
                "v{api_version}: the other topic is not asked for"
            );
        }

        // Below v4 the request can't refuse auto-creation, and the broker creates the name.
        for api_version in [4, 9, 12] {
            correlation_id += 1;
            assert_eq!(
                vec![(wanted.clone(), none), (missing.clone(), unknown)],
                metadata(
                    &mut stream,
                    api_version,
                    correlation_id,
                    &[&wanted, &missing],
                    false
                )
                .await?,
                "v{api_version}: a name the engine lacks is unknown"
            );
        }

        Ok::<_, anyhow::Error>(())
    })
    .await
    .map_err(|_| anyhow!("timed out after {TEST_TIMEOUT:?}"))?
}

#[tokio::test]
async fn a_name_that_the_engine_lacks_is_auto_created_when_the_request_allows_it() -> Result<()> {
    let _guard = init_tracing()?;

    timeout(TEST_TIMEOUT, async {
        let broker = spawn_broker_on(None, false, Url::parse("null://")?).await?;
        let mut stream = TcpStream::connect(broker.addr).await?;

        let existing = alphanumeric_string(15);
        let created = alphanumeric_string(15);
        create_topic(&mut stream, &existing).await?;

        let none = i16::from(ErrorCode::None);

        // The service creates the topic and asks the engine again, so the first answer has it.
        assert_eq!(
            vec![(created.clone(), none)],
            metadata(&mut stream, 12, 2, &[&created], true).await?
        );
        assert_eq!(
            BTreeSet::from([(existing, none), (created, none)]),
            metadata_all(&mut stream, 3).await?
        );

        Ok::<_, anyhow::Error>(())
    })
    .await
    .map_err(|_| anyhow!("timed out after {TEST_TIMEOUT:?}"))?
}

async fn metadata_all(
    stream: &mut TcpStream,
    correlation_id: i32,
) -> Result<BTreeSet<(String, i16)>> {
    let frame = round_trip(
        stream,
        MetadataRequest::KEY,
        12,
        correlation_id,
        MetadataRequest::default()
            .topics(None)
            .allow_auto_topic_creation(Some(false))
            .include_cluster_authorized_operations(Some(false))
            .include_topic_authorized_operations(Some(false))
            .into(),
    )
    .await?;

    Ok(MetadataResponse::try_from(frame.body)?
        .topics
        .into_iter()
        .flatten()
        .map(|topic| (topic.name.unwrap_or_default(), topic.error_code))
        .collect())
}
