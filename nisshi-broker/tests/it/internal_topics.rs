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

//! Metadata and DescribeTopicPartitions answer `is_internal` from the topic name, as Kafka 3.9.1
//! does: true for `__consumer_offsets`, `__transaction_state` and `__share_group_state`, whether
//! the topic exists or not, and false for every other name and for an unknown topic ID. The
//! services set the flag, so every storage engine answers the same.

use std::assert_matches;

use crate::common::{
    alphanumeric_string, init_tracing, lite_storage, memory_storage, postgres_storage,
};
use nisshi_broker::Result;
use nisshi_sans_io::{
    DescribeTopicPartitionsRequest, ErrorCode, MetadataRequest, RequestInput,
    create_topics_request::CreatableTopic, describe_topic_partitions_request::TopicRequest,
    metadata_request::MetadataRequestTopic,
};
use nisshi_storage::{
    ArcDynStorage, AutoCreateRefusals, DescribeTopicPartitionsService, MetadataService, Storage,
    TopicCreationConfig,
};
use rama::{Service, extensions::Extensions};
use rand::{prelude::*, rng};
use uuid::Uuid;

const CONSUMER_OFFSETS: &str = "__consumer_offsets";
const TRANSACTION_STATE: &str = "__transaction_state";
const SHARE_GROUP_STATE: &str = "__share_group_state";

/// Creates the topic `name` with one partition and returns its ID.
async fn create_topic(storage: &impl Storage, name: &str) -> Result<Uuid> {
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
        .map_err(Into::into)
}

/// The names, with their error codes and `is_internal` flags, that a Metadata request for
/// `topics` answers. A `None` request asks for every topic.
async fn metadata(
    service: &MetadataService<ArcDynStorage>,
    topics: Option<Vec<MetadataRequestTopic>>,
) -> Result<Vec<(Option<String>, ErrorCode, Option<bool>)>> {
    let response = service
        .serve(RequestInput {
            request: MetadataRequest::default()
                .allow_auto_topic_creation(Some(false))
                .include_cluster_authorized_operations(Some(false))
                .include_topic_authorized_operations(Some(false))
                .topics(topics),
            extensions: Extensions::default(),
        })
        .await?;

    response
        .topics
        .unwrap_or_default()
        .into_iter()
        .map(|topic| {
            Ok((
                topic.name,
                ErrorCode::try_from(topic.error_code)?,
                topic.is_internal,
            ))
        })
        .collect()
}

/// Metadata by name answers true for each internal name, known or unknown, and false for a
/// known and an unknown name that is not internal. A request for every topic answers the same
/// for the topics that exist.
async fn metadata_answers_is_internal_from_the_name(storage: ArcDynStorage) -> Result<()> {
    let known = alphanumeric_string(15);
    let unknown = alphanumeric_string(15);

    // One internal name exists and the other two do not, so the request sees the flag on a
    // known and on an unknown internal topic.
    _ = create_topic(&storage, CONSUMER_OFFSETS).await?;
    _ = create_topic(&storage, &known).await?;

    let service = MetadataService {
        storage,
        topic_creation: TopicCreationConfig::default(),
        auto_create_refusals: AutoCreateRefusals::default(),
    };

    let by_name = |name: &str| MetadataRequestTopic::default().name(Some(name.into()));

    let mut topics = metadata(
        &service,
        Some(vec![
            by_name(CONSUMER_OFFSETS),
            by_name(TRANSACTION_STATE),
            by_name(SHARE_GROUP_STATE),
            by_name(&known),
            by_name(&unknown),
        ]),
    )
    .await?;
    topics.sort();

    let mut expected = vec![
        (Some(CONSUMER_OFFSETS.into()), ErrorCode::None, Some(true)),
        (
            Some(TRANSACTION_STATE.into()),
            ErrorCode::UnknownTopicOrPartition,
            Some(true),
        ),
        (
            Some(SHARE_GROUP_STATE.into()),
            ErrorCode::UnknownTopicOrPartition,
            Some(true),
        ),
        (Some(known.clone()), ErrorCode::None, Some(false)),
        (
            Some(unknown),
            ErrorCode::UnknownTopicOrPartition,
            Some(false),
        ),
    ];
    expected.sort();

    assert_eq!(expected, topics);

    let all = metadata(&service, None).await?;

    assert_matches!(
        all.iter()
            .find(|(name, ..)| name.as_deref() == Some(CONSUMER_OFFSETS)),
        Some((_, ErrorCode::None, Some(true))),
        "{all:?}"
    );
    assert_matches!(
        all.iter()
            .find(|(name, ..)| name.as_deref() == Some(known.as_str())),
        Some((_, ErrorCode::None, Some(false))),
        "{all:?}"
    );

    Ok(())
}

/// Metadata by ID answers true for a known internal topic, since the row carries its name, and
/// false for an unknown ID, which has no name.
async fn metadata_by_id_answers_is_internal_from_the_name(storage: ArcDynStorage) -> Result<()> {
    let known = create_topic(&storage, CONSUMER_OFFSETS).await?;
    let unknown = Uuid::now_v7();

    let service = MetadataService {
        storage,
        topic_creation: TopicCreationConfig::default(),
        auto_create_refusals: AutoCreateRefusals::default(),
    };

    let by_id = |id: Uuid| MetadataRequestTopic::default().topic_id(Some(id.into_bytes()));

    let topics = metadata(&service, Some(vec![by_id(known)])).await?;
    assert_matches!(
        topics.as_slice(),
        [(Some(name), ErrorCode::None, Some(true))] if name == CONSUMER_OFFSETS,
        "{topics:?}"
    );

    let topics = metadata(&service, Some(vec![by_id(unknown)])).await?;
    assert_matches!(
        topics.as_slice(),
        [(None, ErrorCode::UnknownTopicId, Some(false))],
        "{topics:?}"
    );

    Ok(())
}

/// DescribeTopicPartitions answers true for each internal name, known or unknown, and false for
/// a known and an unknown name that is not internal.
async fn describe_topic_partitions_answers_is_internal_from_the_name(
    storage: ArcDynStorage,
) -> Result<()> {
    let known = alphanumeric_string(15);
    let unknown = alphanumeric_string(15);

    // One internal name exists and the other two do not, so the request sees the flag on a
    // known and on an unknown internal topic.
    _ = create_topic(&storage, CONSUMER_OFFSETS).await?;
    _ = create_topic(&storage, &known).await?;

    let service = DescribeTopicPartitionsService { storage };

    let response = service
        .serve(RequestInput {
            request: DescribeTopicPartitionsRequest::default()
                .topics(Some(
                    [
                        CONSUMER_OFFSETS,
                        TRANSACTION_STATE,
                        SHARE_GROUP_STATE,
                        &known,
                        &unknown,
                    ]
                    .into_iter()
                    .map(|name| TopicRequest::default().name(name.into()))
                    .collect(),
                ))
                .response_partition_limit(i32::MAX),
            extensions: Extensions::default(),
        })
        .await?;

    let mut topics = response
        .topics
        .unwrap_or_default()
        .into_iter()
        .map(|topic| {
            Ok((
                topic.name,
                ErrorCode::try_from(topic.error_code)?,
                topic.is_internal,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    topics.sort();

    let mut expected = vec![
        (Some(CONSUMER_OFFSETS.into()), ErrorCode::None, true),
        (
            Some(TRANSACTION_STATE.into()),
            ErrorCode::UnknownTopicOrPartition,
            true,
        ),
        (
            Some(SHARE_GROUP_STATE.into()),
            ErrorCode::UnknownTopicOrPartition,
            true,
        ),
        (Some(known), ErrorCode::None, false),
        (Some(unknown), ErrorCode::UnknownTopicOrPartition, false),
    ];
    expected.sort();

    assert_eq!(expected, topics);

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
            async fn metadata_answers_is_internal_from_the_name() -> Result<()> {
                let _guard = init_tracing()?;
                super::metadata_answers_is_internal_from_the_name(storage_container().await?).await
            }

            #[tokio::test]
            async fn metadata_by_id_answers_is_internal_from_the_name() -> Result<()> {
                let _guard = init_tracing()?;
                super::metadata_by_id_answers_is_internal_from_the_name(storage_container().await?)
                    .await
            }

            #[tokio::test]
            async fn describe_topic_partitions_answers_is_internal_from_the_name() -> Result<()> {
                let _guard = init_tracing()?;
                super::describe_topic_partitions_answers_is_internal_from_the_name(
                    storage_container().await?,
                )
                .await
            }
        }
    };
}

backend!(in_memory, "dynostore", memory_storage);
backend!(lite, "libsql", lite_storage);
backend!(pg, "postgres", postgres_storage);
