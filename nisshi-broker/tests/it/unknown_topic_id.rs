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

//! A Metadata request that carries a topic ID is answered as Kafka 3.9.1 answers it: a topic ID
//! that the broker does not know gets `UNKNOWN_TOPIC_ID` with no name, the unknown IDs come before
//! the other topics, and the request asks only by its IDs, so a named entry is dropped. A topic
//! name that the broker does not know stays `UNKNOWN_TOPIC_OR_PARTITION`. The service chooses the
//! code and the order, so every storage engine answers the same.

use std::assert_matches;

use crate::common::{init_tracing, lite_storage, memory_storage, postgres_storage};
use nisshi_broker::Result;
use nisshi_sans_io::{
    ErrorCode, MetadataRequest, NULL_TOPIC_ID, RequestInput, create_topics_request::CreatableTopic,
    metadata_request::MetadataRequestTopic,
};
use nisshi_storage::{
    ArcDynStorage, AutoCreateRefusals, MetadataService, Storage, TopicCreationConfig, TopicId,
};
use rama::{Service, extensions::Extensions};
use rand::{prelude::*, rng};
use uuid::Uuid;

/// Creates a topic with one partition and returns its ID.
async fn create_topic(storage: &impl Storage, name: &str) -> Result<[u8; 16]> {
    storage
        .create_topic(
            CreatableTopic::default()
                .name(name.into())
                .num_partitions(1)
                .replication_factor(0)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await
        .map(Uuid::into_bytes)
        .map_err(Into::into)
}

/// The error code, name and ID of each topic that a Metadata v12 request for `topics` answers,
/// in the order answered.
async fn metadata(
    storage: impl Storage + Clone,
    topics: Vec<MetadataRequestTopic>,
) -> Result<Vec<(ErrorCode, Option<String>, Option<[u8; 16]>)>> {
    let service = MetadataService {
        storage,
        topic_creation: TopicCreationConfig::default(),
        auto_create_refusals: AutoCreateRefusals::default(),
    };

    let response = service
        .serve(RequestInput {
            request: MetadataRequest::default()
                // With auto-creation on, the service creates each unknown name that reaches
                // the storage, so a name that is not created was dropped.
                .allow_auto_topic_creation(Some(true))
                .include_cluster_authorized_operations(Some(false))
                .include_topic_authorized_operations(Some(false))
                .topics(Some(topics)),
            extensions: Extensions::default(),
        })
        .await?;

    response
        .topics
        .unwrap_or_default()
        .into_iter()
        .map(|topic| {
            Ok((
                ErrorCode::try_from(topic.error_code)?,
                topic.name,
                topic.topic_id,
            ))
        })
        .collect()
}

fn by_id(id: [u8; 16]) -> MetadataRequestTopic {
    MetadataRequestTopic::default().topic_id(Some(id))
}

fn by_name(name: &str) -> MetadataRequestTopic {
    MetadataRequestTopic::default().name(Some(name.into()))
}

/// Kafka answers the unknown IDs before the other topics
/// ([`KafkaApis.handleTopicMetadataRequest`]), so a request for a known ID and then an unknown
/// one is answered in the other order.
///
/// [`KafkaApis.handleTopicMetadataRequest`]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/KafkaApis.scala#L1419-L1420
async fn an_unknown_id_is_answered_before_a_known_one(storage: impl Storage + Clone) -> Result<()> {
    let name = crate::common::alphanumeric_string(15);
    let known = create_topic(&storage, &name).await?;
    let unknown = Uuid::now_v7().into_bytes();

    let answered = metadata(storage, vec![by_id(known), by_id(unknown)]).await?;

    assert_matches!(
        answered.as_slice(),
        [
            (ErrorCode::UnknownTopicId, None, Some(first)),
            (ErrorCode::None, Some(second), Some(second_id)),
        ] if *first == unknown && *second == name && *second_id == known
    );

    Ok(())
}

/// Kafka reads a request that carries a topic ID other than the zero one as a request by IDs
/// only ([`KafkaApis.handleTopicMetadataRequest`]): a named entry is dropped and not created,
/// the name of an entry that also carries an ID is ignored, and an ID asked for twice is
/// answered once. An entry that names a topic with the zero ID is still asked for by name.
///
/// [`KafkaApis.handleTopicMetadataRequest`]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/KafkaApis.scala#L1331-L1347
async fn a_name_is_dropped_when_an_id_is_set(storage: impl Storage + Clone) -> Result<()> {
    let name = crate::common::alphanumeric_string(15);
    let known = create_topic(&storage, &name).await?;
    let unknown = Uuid::now_v7().into_bytes();
    let unknown_name = crate::common::alphanumeric_string(15);

    let answered = metadata(
        storage.clone(),
        vec![by_id(unknown), by_name(&unknown_name), by_id(unknown)],
    )
    .await?;
    assert_matches!(
        answered.as_slice(),
        [(ErrorCode::UnknownTopicId, None, Some(id))] if *id == unknown
    );

    let not_created = storage
        .metadata(Some(&[TopicId::from(unknown_name.as_str())]))
        .await?;
    assert_matches!(
        not_created.topics.as_slice(),
        [topic] if topic.error_code == i16::from(ErrorCode::UnknownTopicOrPartition),
        "the dropped name was auto-created"
    );

    let answered = metadata(
        storage.clone(),
        vec![by_name(&name).topic_id(Some(unknown))],
    )
    .await?;
    assert_matches!(
        answered.as_slice(),
        [(ErrorCode::UnknownTopicId, None, Some(id))] if *id == unknown
    );

    let answered = metadata(
        storage.clone(),
        vec![by_name(&unknown_name).topic_id(Some(known))],
    )
    .await?;
    assert_matches!(
        answered.as_slice(),
        [(ErrorCode::None, Some(answered_name), Some(id))] if *answered_name == name && *id == known
    );

    let answered = metadata(
        storage,
        vec![
            by_name(&name).topic_id(Some(NULL_TOPIC_ID)),
            by_name(&unknown_name),
        ],
    )
    .await?;
    assert_matches!(
        answered.as_slice(),
        [
            (ErrorCode::None, Some(first), Some(first_id)),
            (ErrorCode::None, Some(second), Some(_)),
        ] if *first == name && *first_id == known && *second == unknown_name,
        "the zero ID is no ID, so both names are asked for, and the unknown one is created"
    );

    Ok(())
}

macro_rules! backend {
    ($name:ident, $feature:literal, $storage:path) => {
        #[cfg(feature = $feature)]
        mod $name {
            use super::*;

            async fn storage_container() -> Result<ArcDynStorage> {
                $storage(Uuid::now_v7(), rng().random_range(0..i32::MAX)).await
            }

            #[tokio::test]
            async fn an_unknown_id_is_answered_before_a_known_one() -> Result<()> {
                let _guard = init_tracing()?;
                super::an_unknown_id_is_answered_before_a_known_one(storage_container().await?)
                    .await
            }

            #[tokio::test]
            async fn a_name_is_dropped_when_an_id_is_set() -> Result<()> {
                let _guard = init_tracing()?;
                super::a_name_is_dropped_when_an_id_is_set(storage_container().await?).await
            }
        }
    };
}

backend!(in_memory, "dynostore", memory_storage);
backend!(lite, "libsql", lite_storage);
backend!(pg, "postgres", postgres_storage);
