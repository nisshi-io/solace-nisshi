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

use std::{collections::BTreeMap, slice, time::Duration};

use crate::common::{
    alphanumeric_string, init_tracing, lite_storage, memory_storage, postgres_storage,
    slate_storage,
};
use bytes::Bytes;
use nisshi_broker::Error;
use nisshi_broker::Result;
use nisshi_sans_io::{
    CreateTopicsRequest, CreateTopicsResponse, DeleteTopicsRequest, DeleteTopicsResponse,
    ErrorCode, IsolationLevel, NULL_TOPIC_ID, RequestInput,
    create_topics_request::CreatableTopic,
    delete_topics_request::DeleteTopicState,
    delete_topics_response::DeletableTopicResult,
    record::{Record, inflated},
};
use nisshi_storage::{
    ArcDynStorage, CreateTopicsService, CreateTopicsValidationLayer, DeleteTopicsService,
    OffsetCommitRequest, Storage, Topition,
};
use rama::{Layer as _, Service as _, extensions::Extensions};
use rand::{RngExt as _, rng};
use std::assert_matches;
use uuid::Uuid;

async fn delete_unknown_by_name(storage: impl Storage + Clone) -> Result<(), Error> {
    let service = DeleteTopicsService {
        storage: storage.clone(),
    };

    let topic = alphanumeric_string(15);

    let error_code = ErrorCode::UnknownTopicOrPartition;

    assert_eq!(
        DeleteTopicsResponse::default()
            .throttle_time_ms(Some(0))
            .responses(Some(vec![
                DeletableTopicResult::default()
                    .error_code(error_code.into())
                    .error_message(Some(error_code.to_string()))
                    .name(Some(topic.clone())),
            ])),
        service
            .serve(RequestInput {
                request: DeleteTopicsRequest::default().topic_names(Some(vec![topic])),
                extensions: Extensions::default()
            })
            .await?
    );

    Ok(())
}

async fn delete_unknown_by_uuid(storage: impl Storage + Clone) -> Result<(), Error> {
    let service = DeleteTopicsService {
        storage: storage.clone(),
    };

    let topic = Uuid::new_v4();

    let error_code = ErrorCode::UnknownTopicOrPartition;

    assert_eq!(
        DeleteTopicsResponse::default()
            .throttle_time_ms(Some(0))
            .responses(Some(vec![
                DeletableTopicResult::default()
                    .error_code(error_code.into())
                    .error_message(Some(error_code.to_string()))
                    .topic_id(Some(topic.into_bytes()))
            ])),
        service
            .serve(RequestInput {
                request: DeleteTopicsRequest::default().topics(Some(vec![
                    DeleteTopicState::default().topic_id(topic.into_bytes())
                ])),
                extensions: Extensions::default()
            },)
            .await?
    );

    Ok(())
}

async fn create_delete_create_by_name(storage: impl Storage + Clone) -> Result<(), Error> {
    let create_topics = CreateTopicsValidationLayer::new().layer(CreateTopicsService {
        storage: storage.clone(),
    });

    let name = alphanumeric_string(15);
    let num_partitions = 5;
    let replication_factor = 3;
    let assignments = Some([].into());
    let configs = Some([].into());

    let error_code = ErrorCode::None;

    let extensions = Extensions::default();

    let response = create_topics
        .serve(RequestInput {
            request: CreateTopicsRequest::default()
                .topics(Some(
                    [CreatableTopic::default()
                        .name(name.clone())
                        .num_partitions(num_partitions)
                        .replication_factor(replication_factor)
                        .assignments(assignments.clone())
                        .configs(configs.clone())]
                    .into(),
                ))
                .validate_only(Some(false)),
            extensions: extensions.clone(),
        })
        .await?;
    let CreateTopicsResponse {
        topics: Some(topics),
        ..
    } = response
    else {
        panic!("unexpected response: {response:?}");
    };
    assert_eq!(topics.len(), 1);
    assert_eq!(name, topics[0].name.as_str());
    assert_matches!(topics[0].configs.as_ref(), Some(configs) if configs.is_empty());
    assert_eq!(topics[0].topic_config_error_code, Some(0));
    assert_eq!(topics[0].num_partitions, Some(num_partitions));
    assert_eq!(topics[0].replication_factor, Some(replication_factor));
    assert_eq!(topics[0].error_code, i16::from(error_code));

    let delete_topics = DeleteTopicsService {
        storage: storage.clone(),
    };

    let error_code = ErrorCode::None;

    assert_eq!(
        DeleteTopicsResponse::default()
            .throttle_time_ms(Some(0))
            .responses(Some(vec![
                DeletableTopicResult::default()
                    .error_code(error_code.into())
                    .error_message(Some(error_code.to_string()))
                    .name(Some(name.clone()))
                    .topic_id(Some(NULL_TOPIC_ID)),
            ])),
        delete_topics
            .serve(RequestInput {
                request: DeleteTopicsRequest::default().topics(Some(vec![
                    DeleteTopicState::default()
                        .name(Some(name.clone()))
                        .topic_id(NULL_TOPIC_ID),
                ])),
                extensions: extensions.clone()
            })
            .await?
    );

    let response = create_topics
        .serve(RequestInput {
            request: CreateTopicsRequest::default()
                .topics(Some(
                    [CreatableTopic::default()
                        .name(name.clone())
                        .num_partitions(num_partitions)
                        .replication_factor(replication_factor)
                        .assignments(assignments.clone())
                        .configs(configs.clone())]
                    .into(),
                ))
                .validate_only(Some(false)),
            extensions: extensions.clone(),
        })
        .await?;
    let CreateTopicsResponse {
        topics: Some(topics),
        ..
    } = response
    else {
        panic!("unexpected response: {response:?}");
    };
    assert_eq!(topics.len(), 1);
    assert_eq!(name, topics[0].name.as_str());
    assert_matches!(topics[0].configs.as_ref(), Some(configs) if configs.is_empty());
    assert_eq!(topics[0].topic_config_error_code, Some(0));
    assert_eq!(topics[0].num_partitions, Some(num_partitions));
    assert_eq!(topics[0].replication_factor, Some(replication_factor));
    assert_eq!(topics[0].error_code, i16::from(error_code));

    Ok(())
}

/// Deleting a topic whose name is not a safe object key prefix (empty, or
/// containing "/") must remove the topic, by name or by id, and must never
/// touch a topic that was not named. On dynostore, `Path::from` collapses an
/// empty path segment, so an empty name, or a name with a leading, trailing,
/// or doubled "/", widens the delete's key prefix: an empty name's prefix
/// matches every topic's own data, and a trailing "/" makes the second pass
/// (deleting a topic's consumer-group offsets) match another topic's offsets
/// instead of its own.
async fn invalid_topic_name_mixed_list(storage: impl Storage + Clone) -> Result<(), Error> {
    let good_name = alphanumeric_string(15);
    let empty_name = String::new();
    let trailing_slash_name = format!("{good_name}/");
    let leading_slash_name = format!("/{good_name}");
    let doomed_name = alphanumeric_string(15);

    // Every topic is created before any of them is produced to.
    // create_topic resets its own partitions' watermarks to none, and
    // trailing_slash_name's watermark path collapses onto good_name's own
    // (object_store drops the empty segment the trailing "/" leaves before
    // "/partitions/..."), so creating it after producing to good_name would
    // reset good_name's watermark back to none.
    let mut empty_id = None;

    for name in [
        good_name.clone(),
        empty_name.clone(),
        trailing_slash_name.clone(),
        leading_slash_name.clone(),
        doomed_name.clone(),
    ] {
        let id = storage
            .create_topic(
                CreatableTopic::default()
                    .name(name.clone())
                    .num_partitions(1)
                    .replication_factor(0)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            )
            .await?;

        if name.is_empty() {
            empty_id = Some(id);
        }
    }

    let empty_id = empty_id.expect("empty_name was created");

    // The slash-named topics are never produced to: producing would land on
    // good_name's own object path for the same reason, and collide with it.
    for name in [good_name.clone(), empty_name.clone(), doomed_name.clone()] {
        let topition = Topition::new(name.as_str(), 0);
        let value = Bytes::copy_from_slice(alphanumeric_string(15).as_bytes());

        let batch = inflated::Batch::builder()
            .record(Record::builder().value(Some(value)))
            .build()
            .and_then(TryInto::try_into)?;

        _ = storage.produce(None, &topition, batch).await?;
    }

    let good_topition = Topition::new(good_name.clone(), 0);
    let group_id = alphanumeric_string(15);
    let offset = rng().random_range(0..i64::MAX);

    let commit = storage
        .offset_commit(
            group_id.as_str(),
            None,
            &[(
                good_topition.clone(),
                OffsetCommitRequest::default().offset(offset),
            )],
        )
        .await?;
    assert_eq!(1, commit.len());
    assert_eq!(ErrorCode::None, commit[0].1);

    let delete_topics = DeleteTopicsService {
        storage: storage.clone(),
    };

    // A Java client refuses an empty name before sending, so a delete by id
    // is the only way that topic reaches the broker.
    let by_id = |id: Uuid| {
        DeleteTopicsRequest::default().topics(Some(vec![
            DeleteTopicState::default().topic_id(id.into_bytes()),
        ]))
    };
    let by_name = || {
        DeleteTopicsRequest::default().topic_names(Some(vec![
            trailing_slash_name.clone(),
            leading_slash_name.clone(),
            doomed_name.clone(),
        ]))
    };

    for (request, expected) in [
        (by_id(empty_id), ErrorCode::None),
        (by_name(), ErrorCode::None),
        // Every topic is gone: a second delete finds nothing.
        (by_id(empty_id), ErrorCode::UnknownTopicOrPartition),
        (by_name(), ErrorCode::UnknownTopicOrPartition),
    ] {
        let response = delete_topics
            .serve(RequestInput {
                request,
                extensions: Extensions::default(),
            })
            .await?;

        for result in response.responses.unwrap_or_default() {
            assert_eq!(
                expected,
                ErrorCode::try_from(result.error_code)?,
                "name = {:?}, topic_id = {:?}",
                result.name,
                result.topic_id
            );
        }
    }

    let min_bytes = 1;
    let max_bytes = 50 * 1024;
    let isolation = IsolationLevel::ReadUncommitted;
    let max_wait = Duration::from_millis(500);

    // good_name was never named in a request, so its data and committed
    // offset must still be there, exactly as produced/committed above.
    let good_fetch = storage
        .fetch(&good_topition, 0, min_bytes, max_bytes, isolation, max_wait)
        .await?;
    assert!(!good_fetch.is_empty());

    let offset_fetch = storage
        .offset_fetch(
            Some(group_id.as_str()),
            slice::from_ref(&good_topition),
            None,
        )
        .await?;
    assert_eq!(
        Some(offset),
        offset_fetch
            .get(&good_topition)
            .map(|committed| committed.offset)
    );

    let doomed_topition = Topition::new(doomed_name.clone(), 0);

    assert_matches!(
        storage
            .fetch(
                &doomed_topition,
                0,
                min_bytes,
                max_bytes,
                isolation,
                max_wait
            )
            .await,
        Err(nisshi_storage::Error::Api(
            ErrorCode::UnknownTopicOrPartition
        ))
    );

    // doomed_name has a safe name, so its data was swept, not only its
    // metadata: dynostore's fetch reads by path, so a guard that skipped the
    // sweep for too many names would find the records again in a topic
    // created with the same name.
    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(doomed_name.clone())
                .num_partitions(1)
                .replication_factor(0)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    let doomed_fetch = storage
        .fetch(
            &doomed_topition,
            0,
            min_bytes,
            max_bytes,
            isolation,
            max_wait,
        )
        .await?;
    assert!(doomed_fetch.is_empty(), "{doomed_fetch:?}");

    Ok(())
}

/// A name with "/" but no empty segment, such as "a/b", keeps its own
/// offset keys: dynostore matches whole segments after `offsets/`, so the
/// offsets sweep reaches only that topic's keys and must run. Left in place,
/// an "a/b" offset key makes `committed_offset_topitions` fail for the whole
/// group, which is what an OffsetFetch for all topics reads, and deleting
/// the topic is the only way through the Kafka API to clear it short of
/// deleting the group.
async fn slash_topic_name_offsets_swept(storage: impl Storage + Clone) -> Result<(), Error> {
    let good_name = alphanumeric_string(15);
    let slash_name = format!("{good_name}/{}", alphanumeric_string(15));

    for name in [good_name.clone(), slash_name.clone()] {
        _ = storage
            .create_topic(
                CreatableTopic::default()
                    .name(name)
                    .num_partitions(1)
                    .replication_factor(0)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            )
            .await?;
    }

    let good_topition = Topition::new(good_name.clone(), 0);
    let slash_topition = Topition::new(slash_name.clone(), 0);
    let group_id = alphanumeric_string(15);
    let offset = rng().random_range(0..i64::MAX);

    let commit = storage
        .offset_commit(
            group_id.as_str(),
            None,
            &[
                (
                    good_topition.clone(),
                    OffsetCommitRequest::default().offset(offset),
                ),
                (
                    slash_topition.clone(),
                    OffsetCommitRequest::default().offset(offset),
                ),
            ],
        )
        .await?;
    assert_eq!(2, commit.len());
    assert!(commit.iter().all(|(_, code)| *code == ErrorCode::None));

    let response = DeleteTopicsService {
        storage: storage.clone(),
    }
    .serve(RequestInput {
        request: DeleteTopicsRequest::default().topic_names(Some(vec![slash_name.clone()])),
        extensions: Extensions::default(),
    })
    .await?;

    for result in response.responses.unwrap_or_default() {
        assert_eq!(ErrorCode::None, ErrorCode::try_from(result.error_code)?);
    }

    // The group's offsets for all topics still read, and only good_name's
    // commit is left.
    let committed = storage
        .committed_offset_topitions(group_id.as_str())
        .await?;
    assert_eq!(
        [(good_topition, offset)]
            .into_iter()
            .collect::<BTreeMap<_, _>>(),
        committed
            .into_iter()
            .map(|(topition, committed)| (topition, committed.offset))
            .collect::<BTreeMap<_, _>>()
    );

    Ok(())
}

#[cfg(feature = "dynostore")]
mod in_memory {
    use super::*;

    async fn storage_container(
        cluster: impl Into<String> + Clone,
        node: i32,
    ) -> Result<ArcDynStorage> {
        memory_storage(cluster, node).await
    }

    #[tokio::test]
    async fn delete_unknown_by_name() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::delete_unknown_by_name(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn delete_unknown_by_uuid() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::delete_unknown_by_uuid(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn create_delete_create_by_name() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::create_delete_create_by_name(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn invalid_topic_name_mixed_list() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::invalid_topic_name_mixed_list(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn slash_topic_name_offsets_swept() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::slash_topic_name_offsets_swept(storage).await?;

        Ok(())
    }
}

#[cfg(feature = "libsql")]
mod lite {
    use super::*;

    async fn storage_container(
        cluster: impl Into<String> + Clone,
        node: i32,
    ) -> Result<ArcDynStorage> {
        lite_storage(cluster, node).await
    }

    #[tokio::test]
    async fn delete_unknown_by_name() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::delete_unknown_by_name(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn delete_unknown_by_uuid() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::delete_unknown_by_uuid(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn create_delete_create_by_name() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::create_delete_create_by_name(storage).await?;

        Ok(())
    }
}

#[cfg(feature = "turso")]
mod turso {
    use super::*;

    async fn storage_container(
        cluster: impl Into<String> + Clone,
        node: i32,
    ) -> Result<ArcDynStorage> {
        crate::common::turso_storage(cluster, node).await
    }

    #[tokio::test]
    async fn delete_unknown_by_name() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::delete_unknown_by_name(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn delete_unknown_by_uuid() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::delete_unknown_by_uuid(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn create_delete_create_by_name() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::create_delete_create_by_name(storage).await?;

        Ok(())
    }
}

#[cfg(feature = "slatedb")]
mod slatedb {
    use super::*;

    async fn storage_container(
        cluster: impl Into<String> + Clone,
        node: i32,
    ) -> Result<ArcDynStorage> {
        slate_storage(cluster, node).await
    }

    #[tokio::test]
    async fn delete_unknown_by_name() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::delete_unknown_by_name(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn delete_unknown_by_uuid() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::delete_unknown_by_uuid(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn create_delete_create_by_name() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::create_delete_create_by_name(storage).await?;

        Ok(())
    }
}

#[cfg(feature = "postgres")]
mod pg {
    use super::*;

    async fn storage_container(
        cluster: impl Into<String> + Clone,
        node: i32,
    ) -> Result<ArcDynStorage> {
        postgres_storage(cluster, node).await
    }

    #[tokio::test]
    async fn delete_unknown_by_name() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::delete_unknown_by_name(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn delete_unknown_by_uuid() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::delete_unknown_by_uuid(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn create_delete_create_by_name() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::create_delete_create_by_name(storage).await?;

        Ok(())
    }
}
