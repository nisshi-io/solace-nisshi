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

use crate::common::{self, alphanumeric_string, register_broker};
use nisshi_broker::Result;
use nisshi_sans_io::{
    ErrorCode, NULL_TOPIC_ID,
    create_topics_request::{CreatableTopic, CreatableTopicConfig},
};
use nisshi_storage::{Storage, TopicId};
use tracing::debug;
use uuid::Uuid;

pub async fn create_delete<G>(cluster_id: impl Into<String>, broker_id: i32, sc: G) -> Result<()>
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

    assert_eq!(
        ErrorCode::None,
        sc.delete_topic(&TopicId::from(topic_id)).await?
    );

    Ok(())
}

pub async fn create_describe_topic_partitions_by_id<G>(
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
    let replication_factor = 3;

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

    let responses = sc
        .describe_topic_partitions(Some(&[topic_id.into()]), 32123, None)
        .await
        .inspect(|responses| debug!(?responses))?;

    assert_eq!(1, responses.len());

    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(responses[0].error_code)?
    );
    assert_eq!(Some(topic_name), responses[0].name);
    assert_eq!(topic_id.as_bytes(), &responses[0].topic_id);
    assert!(!responses[0].is_internal);
    assert_eq!(
        num_partitions as usize,
        responses[0]
            .partitions
            .as_ref()
            .map(|partitions| partitions.len())
            .unwrap_or_default()
    );

    for partition in responses[0].partitions.as_deref().unwrap_or_default() {
        assert_eq!(ErrorCode::None, ErrorCode::try_from(partition.error_code)?);
        assert_eq!(broker_id, partition.leader_id);
        assert_eq!(0, partition.leader_epoch);
        assert_eq!(
            Some(vec![broker_id; replication_factor as usize]),
            partition.replica_nodes
        );
        assert_eq!(
            Some(vec![broker_id; replication_factor as usize]),
            partition.isr_nodes
        );
        assert_eq!(Some(vec![]), partition.eligible_leader_replicas);
        assert_eq!(Some(vec![]), partition.last_known_elr);
        assert_eq!(Some(vec![]), partition.offline_replicas);
    }

    Ok(())
}

pub async fn create_describe_topic_partitions_by_name<G>(
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
    let replication_factor = 3;

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

    let responses = sc
        .describe_topic_partitions(Some(&[topic_name.clone().into()]), 32123, None)
        .await?;

    assert_eq!(1, responses.len());

    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(responses[0].error_code)?
    );
    assert_eq!(Some(topic_name), responses[0].name);
    // assert_eq!(topic_id.as_bytes(), &responses[0].topic_id);
    assert!(!responses[0].is_internal);
    assert_eq!(
        num_partitions as usize,
        responses[0]
            .partitions
            .as_ref()
            .map(|partitions| partitions.len())
            .unwrap_or_default()
    );

    for partition in responses[0].partitions.as_deref().unwrap_or_default() {
        assert_eq!(ErrorCode::None, ErrorCode::try_from(partition.error_code)?);
        assert_eq!(broker_id, partition.leader_id);
        assert_eq!(0, partition.leader_epoch);
        assert_eq!(
            Some(vec![broker_id; replication_factor as usize]),
            partition.replica_nodes
        );
        assert_eq!(
            Some(vec![broker_id; replication_factor as usize]),
            partition.isr_nodes
        );
        assert_eq!(Some(vec![]), partition.eligible_leader_replicas);
        assert_eq!(Some(vec![]), partition.last_known_elr);
        assert_eq!(Some(vec![]), partition.offline_replicas);
    }

    Ok(())
}

pub async fn describe_non_existing_topic_partitions_by_name<G>(
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

    let responses = sc
        .describe_topic_partitions(Some(&[topic_name.clone().into()]), 32123, None)
        .await?;

    assert_eq!(1, responses.len());

    assert_eq!(
        ErrorCode::UnknownTopicOrPartition,
        ErrorCode::try_from(responses[0].error_code)?
    );
    assert_eq!(Some(topic_name), responses[0].name);
    assert_eq!(&NULL_TOPIC_ID, &responses[0].topic_id);
    assert!(!responses[0].is_internal);
    assert_eq!(
        0,
        responses[0]
            .partitions
            .as_ref()
            .map(|partitions| partitions.len())
            .unwrap_or_default()
    );

    Ok(())
}

pub async fn create_with_config_delete<G>(
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
    let configs = Some(
        [CreatableTopicConfig::default()
            .name("xyz".into())
            .value(Some("12321".into()))]
        .into(),
    );

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

    assert_eq!(
        ErrorCode::None,
        sc.delete_topic(&TopicId::from(topic_id)).await?
    );

    Ok(())
}

#[cfg(feature = "postgres")]
mod pg {
    use crate::common::{StorageType, init_tracing};
    use nisshi_storage::ArcDynStorage;
    use rand::{prelude::*, rng};
    use url::Url;

    use super::*;

    async fn storage_container(
        cluster: impl Into<String> + Clone,
        node: i32,
    ) -> Result<ArcDynStorage> {
        common::storage_container(
            StorageType::Postgres,
            cluster,
            node,
            Url::parse("tcp://127.0.0.1/")?,
            None,
        )
        .await
    }

    #[tokio::test]
    async fn create_describe_topic_partitions_by_id() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::create_describe_topic_partitions_by_id(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn create_describe_topic_partitions_by_name() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::create_describe_topic_partitions_by_name(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn describe_non_existing_topic_partitions_by_name() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::describe_non_existing_topic_partitions_by_name(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn create_delete() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::create_delete(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn create_with_config_delete() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::create_with_config_delete(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }
}

#[cfg(feature = "dynostore")]
mod in_memory {
    use crate::common::{StorageType, init_tracing};
    use nisshi_storage::ArcDynStorage;
    use rand::{prelude::*, rng};
    use url::Url;

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
    async fn create_delete() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::create_delete(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn create_describe_topic_partitions_by_id() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::create_describe_topic_partitions_by_id(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn create_describe_topic_partitions_by_name() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::create_describe_topic_partitions_by_name(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn describe_non_existing_topic_partitions_by_name() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::describe_non_existing_topic_partitions_by_name(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn create_with_config_delete() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::create_with_config_delete(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }
}

#[cfg(feature = "libsql")]
mod lite {
    use crate::common::{StorageType, init_tracing};
    use nisshi_storage::ArcDynStorage;
    use rand::{prelude::*, rng};
    use url::Url;

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
    async fn create_describe_topic_partitions_by_id() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::create_describe_topic_partitions_by_id(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn create_describe_topic_partitions_by_name() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::create_describe_topic_partitions_by_name(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn describe_non_existing_topic_partitions_by_name() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::describe_non_existing_topic_partitions_by_name(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn create_delete() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::create_delete(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn create_with_config_delete() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::create_with_config_delete(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }
}

#[cfg(feature = "slatedb")]
mod slatedb {
    use crate::common::{StorageType, init_tracing};
    use nisshi_storage::ArcDynStorage;
    use rand::{prelude::*, rng};
    use url::Url;

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
    async fn create_describe_topic_partitions_by_id() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::create_describe_topic_partitions_by_id(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn create_describe_topic_partitions_by_name() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::create_describe_topic_partitions_by_name(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn describe_non_existing_topic_partitions_by_name() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::describe_non_existing_topic_partitions_by_name(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn create_delete() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::create_delete(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn create_with_config_delete() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::create_with_config_delete(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }
}

#[cfg(feature = "turso")]
mod turso {
    use crate::common::{StorageType, init_tracing};
    use nisshi_storage::ArcDynStorage;
    use rand::{prelude::*, rng};
    use url::Url;

    use super::*;

    async fn storage_container(
        cluster: impl Into<String> + Clone,
        node: i32,
    ) -> Result<ArcDynStorage> {
        common::storage_container(
            StorageType::Turso,
            cluster,
            node,
            Url::parse("tcp://127.0.0.1/")?,
            None,
        )
        .await
    }

    #[tokio::test]
    async fn create_describe_topic_partitions_by_id() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::create_describe_topic_partitions_by_id(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn create_describe_topic_partitions_by_name() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::create_describe_topic_partitions_by_name(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn describe_non_existing_topic_partitions_by_name() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::describe_non_existing_topic_partitions_by_name(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn create_delete() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = format!("cluster-{}", Uuid::now_v7());
        let broker_id = rng().random_range(0..i32::MAX);

        super::create_delete(
            cluster_id.clone(),
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn create_with_config_delete() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::create_with_config_delete(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }
}
