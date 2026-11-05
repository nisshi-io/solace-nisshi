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

use std::time::SystemTime;

use crate::common::{
    alphanumeric_string, init_tracing, lite_storage, memory_storage, postgres_storage,
    slate_storage,
};
use nisshi_broker::Result;
use nisshi_sans_io::{
    CreateTopicsRequest, ErrorCode, IsolationLevel, ListOffset, ListOffsetsRequest, RequestInput,
    create_topics_request::CreatableTopic,
    list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic},
};
use nisshi_storage::{
    ArcDynStorage, CreateTopicsService, CreateTopicsValidationLayer, LatencyIntroducingStorage,
    ListOffsetsService, Storage,
};
use rama::{Layer as _, Service, extensions::Extensions};
use rand::{prelude::*, rng};
use uuid::Uuid;

async fn simple(storage: impl Storage + Clone, broker_id: i32) -> Result<()> {
    let extensions = Extensions::default();

    let create_topic = CreateTopicsValidationLayer::new().layer(CreateTopicsService {
        storage: storage.clone(),
    });

    let topic = &alphanumeric_string(15)[..];

    let num_partitions = rng().random_range(1..64);
    let replication_factor = rng().random_range(1..64);

    {
        let response = create_topic
            .serve(RequestInput {
                request: CreateTopicsRequest::default()
                    .validate_only(Some(false))
                    .topics(Some(
                        [CreatableTopic::default()
                            .name(topic.into())
                            .num_partitions(num_partitions)
                            .replication_factor(replication_factor)
                            .assignments(Some([].into()))
                            .configs(Some([].into()))]
                        .into(),
                    )),
                extensions: extensions.clone(),
            })
            .await?;

        let topics = response.topics.as_deref().unwrap_or_default();
        assert_eq!(1, topics.len());
        assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);
    }

    let service = ListOffsetsService {
        storage: storage.clone(),
    };

    let response = service
        .serve(RequestInput {
            request: ListOffsetsRequest::default()
                .isolation_level(Some(IsolationLevel::ReadUncommitted.into()))
                .replica_id(broker_id)
                .topics(Some(
                    [ListOffsetsTopic::default()
                        .name(topic.into())
                        .partitions(Some(
                            [ListOffsetsPartition::default()
                                .current_leader_epoch(Some(-1))
                                .max_num_offsets(Some(3))
                                .partition_index(0)
                                .timestamp(ListOffset::Earliest.try_into()?)]
                            .into(),
                        ))]
                    .into(),
                )),
            extensions: extensions.clone(),
        })
        .await?;

    let topics = response.topics.as_deref().unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(topic, topics[0].name);

    let partitions = topics[0].partitions.as_deref().unwrap_or_default();
    assert_eq!(1, partitions.len());
    assert_eq!(0, partitions[0].partition_index);
    assert_eq!(Some(vec![0]), partitions[0].old_style_offsets);
    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(partitions[0].error_code)?
    );
    assert_eq!(Some(-1), partitions[0].timestamp);
    assert_eq!(Some(0), partitions[0].offset);
    assert_eq!(Some(0), partitions[0].leader_epoch);

    Ok(())
}

/// A Timestamp lookup that matches no record (here: an empty topic) is a
/// not-found result, not an empty-partition result. At the wire level, Kafka's
/// `buildErrorResponse` (KafkaApis.scala) sends offset=-1/timestamp=-1 with
/// error_code=NONE for this case, and `ListOffsetsService` must apply that
/// default rather than the Earliest/Latest empty-partition default of 0.
/// Kafka sends leader epoch -1 here; nisshi sends 0, a known difference that
/// clients ignore when the offset is -1.
async fn timestamp_no_match(storage: impl Storage + Clone, broker_id: i32) -> Result<()> {
    let extensions = Extensions::default();

    let create_topic = CreateTopicsValidationLayer::new().layer(CreateTopicsService {
        storage: storage.clone(),
    });

    let topic = &alphanumeric_string(15)[..];

    let num_partitions = 1;
    let replication_factor = 1;

    {
        let response = create_topic
            .serve(RequestInput {
                request: CreateTopicsRequest::default()
                    .validate_only(Some(false))
                    .topics(Some(
                        [CreatableTopic::default()
                            .name(topic.into())
                            .num_partitions(num_partitions)
                            .replication_factor(replication_factor)
                            .assignments(Some([].into()))
                            .configs(Some([].into()))]
                        .into(),
                    )),
                extensions: extensions.clone(),
            })
            .await?;

        let topics = response.topics.as_deref().unwrap_or_default();
        assert_eq!(1, topics.len());
        assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);
    }

    let service = ListOffsetsService {
        storage: storage.clone(),
    };

    let response = service
        .serve(RequestInput {
            request: ListOffsetsRequest::default()
                .isolation_level(Some(IsolationLevel::ReadUncommitted.into()))
                .replica_id(broker_id)
                .topics(Some(
                    [ListOffsetsTopic::default()
                        .name(topic.into())
                        .partitions(Some(
                            [ListOffsetsPartition::default()
                                .current_leader_epoch(Some(-1))
                                .max_num_offsets(Some(3))
                                .partition_index(0)
                                .timestamp(ListOffset::Timestamp(SystemTime::now()).try_into()?)]
                            .into(),
                        ))]
                    .into(),
                )),
            extensions: extensions.clone(),
        })
        .await?;

    let topics = response.topics.as_deref().unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(topic, topics[0].name);

    let partitions = topics[0].partitions.as_deref().unwrap_or_default();
    assert_eq!(1, partitions.len());
    assert_eq!(0, partitions[0].partition_index);
    assert_eq!(Some(vec![]), partitions[0].old_style_offsets);
    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(partitions[0].error_code)?
    );
    assert_eq!(Some(-1), partitions[0].timestamp);
    assert_eq!(Some(-1), partitions[0].offset);
    assert_eq!(Some(0), partitions[0].leader_epoch);

    Ok(())
}

/// An invalid timestamp is answered `INVALID_TIMESTAMP` on its own partition: the valid
/// partition beside it still resolves, and both sit under one topic entry.
async fn invalid_timestamp_is_scoped_to_its_partition(
    storage: impl Storage + Clone,
    broker_id: i32,
) -> Result<()> {
    let extensions = Extensions::default();
    let topic = &alphanumeric_string(15)[..];

    let response = CreateTopicsValidationLayer::new()
        .layer(CreateTopicsService {
            storage: storage.clone(),
        })
        .serve(RequestInput {
            request: CreateTopicsRequest::default()
                .validate_only(Some(false))
                .topics(Some(
                    [CreatableTopic::default()
                        .name(topic.into())
                        .num_partitions(2)
                        .replication_factor(1)
                        .assignments(Some([].into()))
                        .configs(Some([].into()))]
                    .into(),
                )),
            extensions: extensions.clone(),
        })
        .await?;

    let created = response.topics.as_deref().unwrap_or_default();
    assert_eq!(ErrorCode::None, ErrorCode::try_from(created[0].error_code)?);

    let response = ListOffsetsService { storage }
        .serve(RequestInput {
            request: ListOffsetsRequest::default()
                .isolation_level(Some(IsolationLevel::ReadUncommitted.into()))
                .replica_id(broker_id)
                .topics(Some(
                    [ListOffsetsTopic::default()
                        .name(topic.into())
                        .partitions(Some(
                            [
                                ListOffsetsPartition::default()
                                    .partition_index(0)
                                    .timestamp(ListOffset::Earliest.try_into()?),
                                ListOffsetsPartition::default()
                                    .partition_index(1)
                                    .timestamp(-100),
                            ]
                            .into(),
                        ))]
                    .into(),
                )),
            extensions,
        })
        .await?;

    let topics = response.topics.as_deref().unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(topic, topics[0].name);

    let mut partitions = topics[0].partitions.clone().unwrap_or_default();
    partitions.sort_by_key(|partition| partition.partition_index);
    assert_eq!(2, partitions.len());

    assert_eq!(0, partitions[0].partition_index);
    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(partitions[0].error_code)?
    );
    assert_eq!(Some(0), partitions[0].offset);

    assert_eq!(1, partitions[1].partition_index);
    assert_eq!(
        ErrorCode::InvalidTimestamp,
        ErrorCode::try_from(partitions[1].error_code)?
    );

    Ok(())
}

/// An out of range `isolation_level` applies to the whole request, so every requested
/// partition is answered `INVALID_REQUEST`.
async fn invalid_isolation_level(storage: impl Storage + Clone, broker_id: i32) -> Result<()> {
    let topic = &alphanumeric_string(15)[..];

    let response = ListOffsetsService { storage }
        .serve(RequestInput {
            request: ListOffsetsRequest::default()
                .isolation_level(Some(7))
                .replica_id(broker_id)
                .topics(Some(
                    [ListOffsetsTopic::default()
                        .name(topic.into())
                        .partitions(Some(
                            [0, 1]
                                .map(|partition_index| {
                                    ListOffsetsPartition::default()
                                        .partition_index(partition_index)
                                        .timestamp(-1)
                                })
                                .into(),
                        ))]
                    .into(),
                )),
            extensions: Extensions::default(),
        })
        .await?;

    let topics = response.topics.as_deref().unwrap_or_default();
    assert_eq!(1, topics.len());

    let partitions = topics[0].partitions.as_deref().unwrap_or_default();
    assert_eq!(2, partitions.len());

    for partition in partitions {
        assert_eq!(
            ErrorCode::InvalidRequest,
            ErrorCode::try_from(partition.error_code)?
        );
    }

    Ok(())
}

/// A request for the Earliest or Latest offset of a partition that does not
/// exist gets UNKNOWN_TOPIC_OR_PARTITION with offset -1, as in Kafka.
async fn unknown_partition(storage: impl Storage + Clone, broker_id: i32) -> Result<()> {
    let extensions = Extensions::default();

    let create_topic = CreateTopicsValidationLayer::new().layer(CreateTopicsService {
        storage: storage.clone(),
    });

    let topic = &alphanumeric_string(15)[..];
    let num_partitions = 3;

    {
        let response = create_topic
            .serve(RequestInput {
                request: CreateTopicsRequest::default()
                    .validate_only(Some(false))
                    .topics(Some(
                        [CreatableTopic::default()
                            .name(topic.into())
                            .num_partitions(num_partitions)
                            .replication_factor(1)
                            .assignments(Some([].into()))
                            .configs(Some([].into()))]
                        .into(),
                    )),
                extensions: extensions.clone(),
            })
            .await?;

        let topics = response.topics.as_deref().unwrap_or_default();
        assert_eq!(1, topics.len());
        assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);
    }

    // The topic's partitions are numbered from 0, so the last is num_partitions - 1.
    let missing_partition = num_partitions;

    let service = ListOffsetsService {
        storage: storage.clone(),
    };

    for which in [ListOffset::Earliest, ListOffset::Latest] {
        let response = service
            .serve(RequestInput {
                request: ListOffsetsRequest::default()
                    .isolation_level(Some(IsolationLevel::ReadUncommitted.into()))
                    .replica_id(broker_id)
                    .topics(Some(
                        [ListOffsetsTopic::default()
                            .name(topic.into())
                            .partitions(Some(
                                [ListOffsetsPartition::default()
                                    .current_leader_epoch(Some(-1))
                                    .max_num_offsets(Some(1))
                                    .partition_index(missing_partition)
                                    .timestamp(which.try_into()?)]
                                .into(),
                            ))]
                        .into(),
                    )),
                extensions: extensions.clone(),
            })
            .await?;

        let topics = response.topics.as_deref().unwrap_or_default();
        assert_eq!(1, topics.len());

        let partitions = topics[0].partitions.as_deref().unwrap_or_default();
        assert_eq!(1, partitions.len());
        assert_eq!(
            ErrorCode::UnknownTopicOrPartition,
            ErrorCode::try_from(partitions[0].error_code)?,
            "{which:?}"
        );
        assert_eq!(Some(-1), partitions[0].offset, "{which:?}");
    }

    Ok(())
}

/// Every partition of a request for many partitions answers within the
/// ListOffsets deadline, in both isolation levels, when each storage call
/// takes 50 to 150ms.
async fn many_partitions_with_latency(storage: impl Storage + Clone, broker_id: i32) -> Result<()> {
    const PARTITIONS: i32 = 64;

    let extensions = Extensions::default();
    let topic = &alphanumeric_string(15)[..];

    let response = CreateTopicsValidationLayer::new()
        .layer(CreateTopicsService {
            storage: storage.clone(),
        })
        .serve(RequestInput {
            request: CreateTopicsRequest::default()
                .validate_only(Some(false))
                .topics(Some(
                    [CreatableTopic::default()
                        .name(topic.into())
                        .num_partitions(PARTITIONS)
                        .replication_factor(1)
                        .assignments(Some([].into()))
                        .configs(Some([].into()))]
                    .into(),
                )),
            extensions: extensions.clone(),
        })
        .await?;

    let topics = response.topics.as_deref().unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);

    let service = ListOffsetsService {
        storage: LatencyIntroducingStorage::new(storage),
    };

    for isolation_level in [
        IsolationLevel::ReadUncommitted,
        IsolationLevel::ReadCommitted,
    ] {
        let partitions = (0..PARTITIONS)
            .map(|partition_index| {
                ListOffset::Latest.try_into().map(|timestamp| {
                    ListOffsetsPartition::default()
                        .current_leader_epoch(Some(-1))
                        .partition_index(partition_index)
                        .timestamp(timestamp)
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        let response = service
            .serve(RequestInput {
                request: ListOffsetsRequest::default()
                    .isolation_level(Some(isolation_level.into()))
                    .replica_id(broker_id)
                    .topics(Some(
                        [ListOffsetsTopic::default()
                            .name(topic.into())
                            .partitions(Some(partitions))]
                        .into(),
                    )),
                extensions: extensions.clone(),
            })
            .await?;

        let topics = response.topics.as_deref().unwrap_or_default();
        assert_eq!(1, topics.len());

        let partitions = topics[0].partitions.as_deref().unwrap_or_default();
        assert_eq!(
            (0..PARTITIONS)
                .map(|partition| (partition, ErrorCode::None, Some(0)))
                .collect::<Vec<_>>(),
            partitions
                .iter()
                .map(
                    |partition| ErrorCode::try_from(partition.error_code).map(|error_code| (
                        partition.partition_index,
                        error_code,
                        partition.offset
                    ))
                )
                .collect::<Result<Vec<_>, _>>()?,
            "{isolation_level:?}"
        );
    }

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
    async fn invalid_timestamp_is_scoped_to_its_partition() -> Result<()> {
        let _guard = init_tracing()?;

        let broker_id = rng().random_range(0..i32::MAX);
        let storage = storage_container(Uuid::now_v7(), broker_id).await?;

        super::invalid_timestamp_is_scoped_to_its_partition(storage, broker_id).await
    }

    #[tokio::test]
    async fn invalid_isolation_level() -> Result<()> {
        let _guard = init_tracing()?;

        let broker_id = rng().random_range(0..i32::MAX);
        let storage = storage_container(Uuid::now_v7(), broker_id).await?;

        super::invalid_isolation_level(storage, broker_id).await
    }

    #[tokio::test]
    async fn simple() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::simple(storage, broker_id).await?;

        Ok(())
    }

    #[tokio::test]
    async fn timestamp_no_match() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::timestamp_no_match(storage, broker_id).await?;

        Ok(())
    }

    #[tokio::test]
    async fn many_partitions_with_latency() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::many_partitions_with_latency(storage, broker_id).await?;

        Ok(())
    }

    #[tokio::test]
    async fn unknown_partition() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::unknown_partition(storage, broker_id).await?;

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
    async fn invalid_timestamp_is_scoped_to_its_partition() -> Result<()> {
        let _guard = init_tracing()?;

        let broker_id = rng().random_range(0..i32::MAX);
        let storage = storage_container(Uuid::now_v7(), broker_id).await?;

        super::invalid_timestamp_is_scoped_to_its_partition(storage, broker_id).await
    }

    #[tokio::test]
    async fn invalid_isolation_level() -> Result<()> {
        let _guard = init_tracing()?;

        let broker_id = rng().random_range(0..i32::MAX);
        let storage = storage_container(Uuid::now_v7(), broker_id).await?;

        super::invalid_isolation_level(storage, broker_id).await
    }

    #[tokio::test]
    async fn simple() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::simple(storage, broker_id).await?;

        Ok(())
    }

    #[tokio::test]
    async fn timestamp_no_match() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::timestamp_no_match(storage, broker_id).await?;

        Ok(())
    }

    #[tokio::test]
    async fn many_partitions_with_latency() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::many_partitions_with_latency(storage, broker_id).await?;

        Ok(())
    }

    #[tokio::test]
    async fn unknown_partition() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::unknown_partition(storage, broker_id).await?;

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
    async fn invalid_timestamp_is_scoped_to_its_partition() -> Result<()> {
        let _guard = init_tracing()?;

        let broker_id = rng().random_range(0..i32::MAX);
        let storage = storage_container(Uuid::now_v7(), broker_id).await?;

        super::invalid_timestamp_is_scoped_to_its_partition(storage, broker_id).await
    }

    #[tokio::test]
    async fn invalid_isolation_level() -> Result<()> {
        let _guard = init_tracing()?;

        let broker_id = rng().random_range(0..i32::MAX);
        let storage = storage_container(Uuid::now_v7(), broker_id).await?;

        super::invalid_isolation_level(storage, broker_id).await
    }

    #[tokio::test]
    async fn simple() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::simple(storage, broker_id).await?;

        Ok(())
    }

    #[tokio::test]
    async fn timestamp_no_match() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::timestamp_no_match(storage, broker_id).await?;

        Ok(())
    }

    #[tokio::test]
    async fn many_partitions_with_latency() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::many_partitions_with_latency(storage, broker_id).await?;

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
    async fn invalid_timestamp_is_scoped_to_its_partition() -> Result<()> {
        let _guard = init_tracing()?;

        let broker_id = rng().random_range(0..i32::MAX);
        let storage = storage_container(Uuid::now_v7(), broker_id).await?;

        super::invalid_timestamp_is_scoped_to_its_partition(storage, broker_id).await
    }

    #[tokio::test]
    async fn invalid_isolation_level() -> Result<()> {
        let _guard = init_tracing()?;

        let broker_id = rng().random_range(0..i32::MAX);
        let storage = storage_container(Uuid::now_v7(), broker_id).await?;

        super::invalid_isolation_level(storage, broker_id).await
    }

    #[tokio::test]
    async fn simple() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::simple(storage, broker_id).await?;

        Ok(())
    }

    #[tokio::test]
    async fn timestamp_no_match() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::timestamp_no_match(storage, broker_id).await?;

        Ok(())
    }

    #[tokio::test]
    async fn many_partitions_with_latency() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::many_partitions_with_latency(storage, broker_id).await?;

        Ok(())
    }

    #[tokio::test]
    async fn unknown_partition() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::unknown_partition(storage, broker_id).await?;

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
    async fn invalid_timestamp_is_scoped_to_its_partition() -> Result<()> {
        let _guard = init_tracing()?;

        let broker_id = rng().random_range(0..i32::MAX);
        let storage = storage_container(Uuid::now_v7(), broker_id).await?;

        super::invalid_timestamp_is_scoped_to_its_partition(storage, broker_id).await
    }

    #[tokio::test]
    async fn invalid_isolation_level() -> Result<()> {
        let _guard = init_tracing()?;

        let broker_id = rng().random_range(0..i32::MAX);
        let storage = storage_container(Uuid::now_v7(), broker_id).await?;

        super::invalid_isolation_level(storage, broker_id).await
    }

    #[tokio::test]
    async fn simple() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::simple(storage, broker_id).await?;

        Ok(())
    }

    #[tokio::test]
    async fn timestamp_no_match() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::timestamp_no_match(storage, broker_id).await?;

        Ok(())
    }

    #[tokio::test]
    async fn many_partitions_with_latency() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::many_partitions_with_latency(storage, broker_id).await?;

        Ok(())
    }

    #[tokio::test]
    async fn unknown_partition() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::unknown_partition(storage, broker_id).await?;

        Ok(())
    }
}
