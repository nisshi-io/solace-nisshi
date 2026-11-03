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

use std::slice;

use crate::common::{self, StorageType, alphanumeric_string, init_tracing, register_broker};
use nisshi_broker::Result;
use nisshi_sans_io::{ErrorCode, create_topics_request::CreatableTopic};
use nisshi_storage::{OffsetCommitRequest, Storage, Topition};
use rand::{prelude::*, rng};
use tracing::debug;
use url::Url;
use uuid::Uuid;

pub async fn offset_commit<G>(cluster_id: impl Into<String>, broker_id: i32, sc: G) -> Result<()>
where
    G: Storage + Clone,
{
    register_broker(cluster_id, broker_id, &sc).await?;

    let topic_name: String = alphanumeric_string(15);
    debug!(?topic_name);

    let num_partitions = 6;
    let replication_factor = 0;
    let assignments = Some([].into());
    let configs = Some([].into());

    let topic_id = sc
        .create_topic(
            CreatableTopic::default()
                .name(topic_name.clone())
                .num_partitions(num_partitions)
                .replication_factor(replication_factor)
                .assignments(assignments.clone())
                .configs(configs.clone()),
            false,
        )
        .await?;
    debug!(?topic_id);

    let partition_index = rng().random_range(0..num_partitions);
    let topition = Topition::new(topic_name.clone(), partition_index);

    let group_id: String = alphanumeric_string(15);

    let offset = rng().random_range(0..i64::MAX);

    let commit = sc
        .offset_commit(
            &group_id,
            None,
            &[(
                topition.clone(),
                OffsetCommitRequest::default().offset(offset),
            )],
        )
        .await?;

    assert_eq!(1, commit.len());
    assert_eq!(ErrorCode::None, commit[0].1);

    let offset_fetch = sc
        .offset_fetch(Some(&group_id), slice::from_ref(&topition), None)
        .await?;
    assert!(offset_fetch.contains_key(&topition));
    assert_eq!(
        Some(offset),
        offset_fetch
            .get(&topition)
            .map(|committed| committed.offset)
    );

    let co_tps = sc.committed_offset_topitions(&group_id).await?;
    assert!(co_tps.contains_key(&topition));
    assert_eq!(
        Some(offset),
        co_tps.get(&topition).map(|committed| committed.offset)
    );

    let groups = sc.list_groups().await?;
    assert_eq!(1, groups.len());
    assert_eq!(group_id, groups[0].name);

    Ok(())
}

pub async fn topic_delete_cascade_to_offset_commit<G>(
    cluster_id: impl Into<String>,
    broker_id: i32,
    sc: G,
) -> Result<()>
where
    G: Storage + Clone,
{
    register_broker(cluster_id, broker_id, &sc).await?;

    let topic_name: String = alphanumeric_string(15);
    debug!(?topic_name);

    let num_partitions = 6;
    let replication_factor = 0;
    let assignments = Some([].into());
    let configs = Some([].into());

    let topic_id = sc
        .create_topic(
            CreatableTopic::default()
                .name(topic_name.clone())
                .num_partitions(num_partitions)
                .replication_factor(replication_factor)
                .assignments(assignments.clone())
                .configs(configs.clone()),
            false,
        )
        .await?;
    debug!(?topic_id);

    let partition_index = rng().random_range(0..num_partitions);
    let topition = Topition::new(topic_name.clone(), partition_index);

    let group_id: String = alphanumeric_string(15);

    let offset = rng().random_range(0..i64::MAX);

    let commit = sc
        .offset_commit(
            &group_id,
            None,
            &[(
                topition.clone(),
                OffsetCommitRequest::default().offset(offset),
            )],
        )
        .await?;

    assert_eq!(1, commit.len());
    assert_eq!(ErrorCode::None, commit[0].1);

    let offset_fetch = sc
        .offset_fetch(Some(&group_id), slice::from_ref(&topition), None)
        .await?;
    assert!(offset_fetch.contains_key(&topition));
    assert_eq!(
        Some(offset),
        offset_fetch
            .get(&topition)
            .map(|committed| committed.offset)
    );

    assert_eq!(ErrorCode::None, sc.delete_topic(&topic_name.into()).await?);

    let offset_fetch = sc
        .offset_fetch(Some(&group_id), slice::from_ref(&topition), None)
        .await?;
    assert!(offset_fetch.contains_key(&topition));
    assert_eq!(
        Some(-1),
        offset_fetch
            .get(&topition)
            .map(|committed| committed.offset)
    );

    Ok(())
}

pub async fn consumer_group_delete_cascade_to_offset_commit<G>(
    cluster_id: impl Into<String>,
    broker_id: i32,
    sc: G,
) -> Result<()>
where
    G: Storage + Clone,
{
    register_broker(cluster_id, broker_id, &sc).await?;

    let topic_name: String = alphanumeric_string(15);
    debug!(?topic_name);

    let num_partitions = 6;
    let replication_factor = 0;
    let assignments = Some([].into());
    let configs = Some([].into());

    let topic_id = sc
        .create_topic(
            CreatableTopic::default()
                .name(topic_name.clone())
                .num_partitions(num_partitions)
                .replication_factor(replication_factor)
                .assignments(assignments.clone())
                .configs(configs.clone()),
            false,
        )
        .await?;
    debug!(?topic_id);

    let partition_index = rng().random_range(0..num_partitions);
    let topition = Topition::new(topic_name.clone(), partition_index);

    let group_id: String = alphanumeric_string(15);

    let offset = rng().random_range(0..i64::MAX);

    let commit = sc
        .offset_commit(
            &group_id,
            None,
            &[(
                topition.clone(),
                OffsetCommitRequest::default().offset(offset),
            )],
        )
        .await?;

    assert_eq!(1, commit.len());
    assert_eq!(ErrorCode::None, commit[0].1);

    let offset_fetch = sc
        .offset_fetch(Some(&group_id), slice::from_ref(&topition), None)
        .await?;
    assert!(offset_fetch.contains_key(&topition));
    assert_eq!(
        Some(offset),
        offset_fetch
            .get(&topition)
            .map(|committed| committed.offset)
    );

    let deleted = sc.delete_groups(Some(slice::from_ref(&group_id))).await?;
    assert_eq!(1, deleted.len());
    assert_eq!(group_id, deleted[0].group_id);
    assert_eq!(ErrorCode::None, ErrorCode::try_from(deleted[0].error_code)?);

    let offset_fetch = sc
        .offset_fetch(Some(&group_id), slice::from_ref(&topition), None)
        .await?;
    assert!(offset_fetch.contains_key(&topition));
    assert_eq!(
        Some(-1),
        offset_fetch
            .get(&topition)
            .map(|committed| committed.offset)
    );

    Ok(())
}

pub async fn delete_unknown_consumer_group<G>(
    cluster_id: impl Into<String>,
    broker_id: i32,
    sc: G,
) -> Result<()>
where
    G: Storage + Clone,
{
    register_broker(cluster_id, broker_id, &sc).await?;

    let group_id: String = alphanumeric_string(15);

    let deleted = sc.delete_groups(Some(slice::from_ref(&group_id))).await?;
    assert_eq!(1, deleted.len());
    assert_eq!(group_id, deleted[0].group_id);
    assert_eq!(
        ErrorCode::GroupIdNotFound,
        ErrorCode::try_from(deleted[0].error_code)?
    );

    Ok(())
}

pub async fn offset_commit_unknown_topition<G>(
    cluster_id: impl Into<String>,
    broker_id: i32,
    sc: G,
) -> Result<()>
where
    G: Storage + Clone,
{
    register_broker(cluster_id, broker_id, &sc).await?;

    let topic_name: String = alphanumeric_string(15);
    debug!(?topic_name);

    let num_partitions = 6;

    let partition_index = rng().random_range(0..num_partitions);
    let topition = Topition::new(topic_name.clone(), partition_index);

    let group_id: String = alphanumeric_string(15);

    let offset = rng().random_range(0..i64::MAX);

    let commit = sc
        .offset_commit(
            &group_id,
            None,
            &[(
                topition.clone(),
                OffsetCommitRequest::default().offset(offset),
            )],
        )
        .await?;

    assert_eq!(1, commit.len());
    assert_eq!(ErrorCode::UnknownTopicOrPartition, commit[0].1);

    let offset_fetch = sc
        .offset_fetch(Some(&group_id), slice::from_ref(&topition), None)
        .await?;
    assert!(offset_fetch.contains_key(&topition));
    assert_eq!(
        Some(-1),
        offset_fetch
            .get(&topition)
            .map(|committed| committed.offset)
    );

    let groups = sc.list_groups().await?;
    assert_eq!(0, groups.len());

    Ok(())
}

pub async fn offset_fetch_unknown_topition<G>(
    cluster_id: impl Into<String>,
    broker_id: i32,
    sc: G,
) -> Result<()>
where
    G: Storage + Clone,
{
    register_broker(cluster_id, broker_id, &sc).await?;

    let topic_name: String = alphanumeric_string(15);
    debug!(?topic_name);

    let num_partitions = 6;

    let partition_index = rng().random_range(0..num_partitions);
    let topition = Topition::new(topic_name.clone(), partition_index);

    let group_id: String = alphanumeric_string(15);

    let offset_fetch = sc
        .offset_fetch(Some(&group_id), slice::from_ref(&topition), None)
        .await?;
    assert!(offset_fetch.contains_key(&topition));
    assert_eq!(
        Some(-1),
        offset_fetch
            .get(&topition)
            .map(|committed| committed.offset)
    );

    let groups = sc.list_groups().await?;
    assert_eq!(0, groups.len());

    Ok(())
}

pub async fn list_groups_none<G>(cluster_id: impl Into<String>, broker_id: i32, sc: G) -> Result<()>
where
    G: Storage + Clone,
{
    register_broker(cluster_id, broker_id, &sc).await?;

    let groups = sc.list_groups().await?;
    assert_eq!(0, groups.len());

    Ok(())
}

#[cfg(feature = "postgres")]
mod pg {
    use nisshi_storage::ArcDynStorage;

    use crate::common::postgres_storage;

    use super::*;

    async fn storage_container(
        cluster: impl Into<String> + Clone,
        node: i32,
    ) -> Result<ArcDynStorage> {
        postgres_storage(cluster, node).await
    }

    #[tokio::test]
    async fn offset_commit() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::offset_commit(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn topic_delete_cascade_to_offset_commit() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::topic_delete_cascade_to_offset_commit(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn consumer_group_delete_cascade_to_offset_commit() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::consumer_group_delete_cascade_to_offset_commit(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn delete_unknown_consumer_group() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::delete_unknown_consumer_group(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn offset_commit_unknown_topition() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::offset_commit_unknown_topition(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn offset_fetch_unknown_topition() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::offset_fetch_unknown_topition(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn list_groups_none() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::list_groups_none(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }
}

#[cfg(feature = "dynostore")]
mod in_memory {
    use nisshi_storage::ArcDynStorage;

    use super::*;

    async fn storage_container(
        cluster: impl Into<String> + Clone,
        node: i32,
    ) -> Result<ArcDynStorage> {
        common::storage_container(
            StorageType::InMemory,
            cluster,
            node,
            Url::parse("tcp://127.0.0.1/")?,
            None,
        )
        .await
    }

    #[tokio::test]
    async fn offset_commit() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::offset_commit(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn topic_delete_cascade_to_offset_commit() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::topic_delete_cascade_to_offset_commit(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn consumer_group_delete_cascade_to_offset_commit() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::consumer_group_delete_cascade_to_offset_commit(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[ignore]
    #[tokio::test]
    async fn delete_unknown_consumer_group() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::delete_unknown_consumer_group(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn offset_commit_unknown_topition() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::offset_commit_unknown_topition(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn offset_fetch_unknown_topition() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::offset_fetch_unknown_topition(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn list_groups_none() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::list_groups_none(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }
}

#[cfg(feature = "libsql")]
mod lite {
    use nisshi_storage::ArcDynStorage;

    use super::*;

    async fn storage_container(
        cluster: impl Into<String> + Clone,
        node: i32,
    ) -> Result<ArcDynStorage> {
        common::storage_container(
            StorageType::Lite,
            cluster,
            node,
            Url::parse("tcp://127.0.0.1/")?,
            None,
        )
        .await
    }

    #[tokio::test]
    async fn offset_commit() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::offset_commit(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn topic_delete_cascade_to_offset_commit() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::topic_delete_cascade_to_offset_commit(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn consumer_group_delete_cascade_to_offset_commit() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::consumer_group_delete_cascade_to_offset_commit(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[ignore]
    #[tokio::test]
    async fn delete_unknown_consumer_group() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::delete_unknown_consumer_group(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn offset_commit_unknown_topition() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::offset_commit_unknown_topition(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn offset_fetch_unknown_topition() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::offset_fetch_unknown_topition(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn list_groups_none() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::list_groups_none(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }
}

#[cfg(feature = "slatedb")]
mod slatedb {
    use nisshi_storage::ArcDynStorage;

    use super::*;

    async fn storage_container(
        cluster: impl Into<String> + Clone,
        node: i32,
    ) -> Result<ArcDynStorage> {
        common::storage_container(
            StorageType::SlateDb,
            cluster,
            node,
            Url::parse("tcp://127.0.0.1/")?,
            None,
        )
        .await
    }

    #[tokio::test]
    async fn offset_commit() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::offset_commit(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn topic_delete_cascade_to_offset_commit() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::topic_delete_cascade_to_offset_commit(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn consumer_group_delete_cascade_to_offset_commit() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::consumer_group_delete_cascade_to_offset_commit(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[ignore]
    #[tokio::test]
    async fn delete_unknown_consumer_group() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::delete_unknown_consumer_group(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn offset_commit_unknown_topition() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::offset_commit_unknown_topition(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn offset_fetch_unknown_topition() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::offset_fetch_unknown_topition(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn list_groups_none() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::list_groups_none(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }
}
