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

use crate::common::{init_tracing, lite_storage, memory_storage, postgres_storage, slate_storage};
use nisshi_broker::Result;
use nisshi_sans_io::{
    ErrorCode, MetadataRequest, NULL_TOPIC_ID, RequestInput, metadata_request::MetadataRequestTopic,
};
use nisshi_storage::{
    ArcDynStorage, AutoCreateRefusals, MetadataService, Storage, TopicCreationConfig,
};
use rama::{Service, extensions::Extensions};
use rand::{prelude::*, rng};
use uuid::Uuid;

async fn simple(storage: impl Storage + Clone, broker_id: i32) -> Result<()> {
    let service = MetadataService {
        storage,
        topic_creation: TopicCreationConfig::default(),
        auto_create_refusals: AutoCreateRefusals::default(),
    };

    let response = service
        .serve(RequestInput {
            request: MetadataRequest::default()
                .allow_auto_topic_creation(Some(false))
                .include_cluster_authorized_operations(Some(false))
                .include_topic_authorized_operations(Some(false))
                .topics(Some([].into())),
            extensions: Extensions::default(),
        })
        .await?;

    let brokers = response.brokers.as_deref().unwrap_or_default();
    assert_eq!(1, brokers.len());
    assert_eq!(broker_id, brokers[0].node_id);
    assert!(brokers[0].rack.is_none());

    Ok(())
}

async fn auto_create_topic(storage: impl Storage + Clone) -> Result<()> {
    let service = MetadataService {
        storage,
        topic_creation: TopicCreationConfig::default(),
        auto_create_refusals: AutoCreateRefusals::default(),
    };

    let name = "auto-created";

    let response = service
        .serve(RequestInput {
            request: MetadataRequest::default()
                .allow_auto_topic_creation(Some(true))
                .topics(Some(vec![
                    MetadataRequestTopic::default().name(Some(name.into())),
                ])),
            extensions: Extensions::default(),
        })
        .await?;

    let topics = response.topics.unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);
    assert_eq!(Some(name.into()), topics[0].name);
    assert_ne!(Some(NULL_TOPIC_ID), topics[0].topic_id);
    assert_eq!(1, topics[0].partitions.as_deref().unwrap_or_default().len());

    Ok(())
}

async fn auto_create_topic_invalid_name(storage: impl Storage + Clone) -> Result<()> {
    let service = MetadataService {
        storage,
        topic_creation: TopicCreationConfig::default(),
        auto_create_refusals: AutoCreateRefusals::default(),
    };

    let name = "not a valid topic name";

    let response = service
        .serve(RequestInput {
            request: MetadataRequest::default()
                .allow_auto_topic_creation(Some(true))
                .topics(Some(vec![
                    MetadataRequestTopic::default().name(Some(name.into())),
                ])),
            extensions: Extensions::default(),
        })
        .await?;

    let topics = response.topics.unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(
        ErrorCode::InvalidTopicException,
        ErrorCode::try_from(topics[0].error_code)?
    );

    Ok(())
}

async fn auto_create_topic_not_allowed(storage: impl Storage + Clone) -> Result<()> {
    let service = MetadataService {
        storage,
        topic_creation: TopicCreationConfig::default(),
        auto_create_refusals: AutoCreateRefusals::default(),
    };

    let name = "not-auto-created";

    let response = service
        .serve(RequestInput {
            request: MetadataRequest::default()
                .allow_auto_topic_creation(Some(false))
                .topics(Some(vec![
                    MetadataRequestTopic::default().name(Some(name.into())),
                ])),
            extensions: Extensions::default(),
        })
        .await?;

    let topics = response.topics.unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(
        ErrorCode::UnknownTopicOrPartition,
        ErrorCode::try_from(topics[0].error_code)?
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
    async fn simple() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::simple(storage, broker_id).await?;

        Ok(())
    }

    #[tokio::test]
    async fn auto_create_topic() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::auto_create_topic(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn auto_create_topic_invalid_name() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::auto_create_topic_invalid_name(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn auto_create_topic_not_allowed() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::auto_create_topic_not_allowed(storage).await?;

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
    async fn simple() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::simple(storage, broker_id).await?;

        Ok(())
    }

    #[tokio::test]
    async fn auto_create_topic() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::auto_create_topic(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn auto_create_topic_invalid_name() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::auto_create_topic_invalid_name(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn auto_create_topic_not_allowed() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::auto_create_topic_not_allowed(storage).await?;

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
        lite_storage(cluster, node).await
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
    async fn auto_create_topic() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::auto_create_topic(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn auto_create_topic_invalid_name() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::auto_create_topic_invalid_name(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn auto_create_topic_not_allowed() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::auto_create_topic_not_allowed(storage).await?;

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
    async fn simple() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::simple(storage, broker_id).await?;

        Ok(())
    }

    #[tokio::test]
    async fn auto_create_topic() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::auto_create_topic(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn auto_create_topic_invalid_name() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::auto_create_topic_invalid_name(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn auto_create_topic_not_allowed() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::auto_create_topic_not_allowed(storage).await?;

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
    async fn simple() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::simple(storage, broker_id).await?;

        Ok(())
    }

    #[tokio::test]
    async fn auto_create_topic() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::auto_create_topic(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn auto_create_topic_invalid_name() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::auto_create_topic_invalid_name(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn auto_create_topic_not_allowed() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::auto_create_topic_not_allowed(storage).await?;

        Ok(())
    }
}
