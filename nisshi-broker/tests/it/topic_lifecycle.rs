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

use crate::common::{
    alphanumeric_string, init_tracing, lite_storage, memory_storage, postgres_storage,
    slate_storage,
};
use nisshi_broker::Result;
use nisshi_sans_io::{
    CreateTopicsRequest, DeleteTopicsRequest, ErrorCode, MetadataRequest, RequestInput,
    create_topics_request::CreatableTopic, metadata_request::MetadataRequestTopic,
};
use nisshi_storage::{
    ArcDynStorage, AutoCreateRefusals, CreateTopicsService, CreateTopicsValidationLayer,
    DeleteTopicsService, MetadataService, Storage, TopicCreationConfig,
};
use rama::{Layer as _, Service, extensions::Extensions};
use rand::{prelude::*, rng};
use uuid::Uuid;

async fn topic_lifecycle(storage: impl Storage + Clone) -> Result<()> {
    let extensions = Extensions::default();

    let create_topic = CreateTopicsValidationLayer::new().layer(CreateTopicsService {
        storage: storage.clone(),
    });

    let delete_topic = DeleteTopicsService {
        storage: storage.clone(),
    };

    let metadata = MetadataService {
        storage: storage.clone(),
        topic_creation: TopicCreationConfig::default(),
        auto_create_refusals: AutoCreateRefusals::default(),
    };

    let name = &alphanumeric_string(15)[..];

    let num_partitions = rng().random_range(1..64);
    let replication_factor = rng().random_range(1..64);

    let response = create_topic
        .serve(RequestInput {
            request: CreateTopicsRequest::default()
                .validate_only(Some(false))
                .topics(Some(
                    [CreatableTopic::default()
                        .name(name.into())
                        .num_partitions(num_partitions)
                        .replication_factor(replication_factor)
                        .assignments(Some([].into()))
                        .configs(Some([].into()))]
                    .into(),
                )),
            extensions: extensions.clone(),
        })
        .await?;

    let topic_id = response.topics.as_deref().unwrap_or_default()[0]
        .topic_id
        .unwrap();

    {
        // metadata via topic uuid
        //

        let response = metadata
            .serve(RequestInput {
                request: MetadataRequest::default()
                    .allow_auto_topic_creation(Some(false))
                    .include_cluster_authorized_operations(Some(false))
                    .include_cluster_authorized_operations(Some(false))
                    .topics(Some(
                        [MetadataRequestTopic::default().topic_id(Some(topic_id))].into(),
                    )),
                extensions: extensions.clone(),
            })
            .await?;

        let topics = response.topics.as_deref().unwrap_or_default();

        assert_eq!(1, topics.len());
        assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);
        assert_eq!(Some(name), topics[0].name.as_deref());
        assert_eq!(Some(topic_id), topics[0].topic_id);
    }

    {
        // metadata via topic name
        //

        let response = metadata
            .serve(RequestInput {
                request: MetadataRequest::default()
                    .allow_auto_topic_creation(Some(false))
                    .include_cluster_authorized_operations(Some(false))
                    .include_cluster_authorized_operations(Some(false))
                    .topics(Some(
                        [MetadataRequestTopic::default().name(Some(name.into()))].into(),
                    )),
                extensions: extensions.clone(),
            })
            .await?;

        let topics = response.topics.as_deref().unwrap_or_default();

        assert_eq!(1, topics.len());
        assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);
        assert_eq!(Some(name), topics[0].name.as_deref());
        assert_eq!(Some(topic_id), topics[0].topic_id);
    }

    {
        // creating a topic with the same name causes an API error: topic already exists
        //
        let response = create_topic
            .serve(RequestInput {
                request: CreateTopicsRequest::default()
                    .validate_only(Some(false))
                    .topics(Some(
                        [CreatableTopic::default()
                            .name(name.into())
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
        assert_eq!(
            ErrorCode::TopicAlreadyExists,
            ErrorCode::try_from(topics[0].error_code)?
        );
    }

    {
        let response = delete_topic
            .serve(RequestInput {
                request: DeleteTopicsRequest::default().topic_names(Some([name.into()].into())),
                extensions: extensions.clone(),
            })
            .await?;

        let results = response.responses.as_deref().unwrap_or_default();
        assert_eq!(1, results.len());
        assert_eq!(ErrorCode::None, ErrorCode::try_from(results[0].error_code)?);
    }

    {
        let response = delete_topic
            .serve(RequestInput {
                request: DeleteTopicsRequest::default().topic_names(Some([name.into()].into())),
                extensions: extensions.clone(),
            })
            .await?;

        let results = response.responses.as_deref().unwrap_or_default();
        assert_eq!(1, results.len());
        assert_eq!(
            ErrorCode::UnknownTopicOrPartition,
            ErrorCode::try_from(results[0].error_code)?
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
    async fn topic_lifecycle() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::topic_lifecycle(storage).await?;

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
    async fn topic_lifecycle() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::topic_lifecycle(storage).await?;

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
    async fn topic_lifecycle() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::topic_lifecycle(storage).await?;

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
    async fn topic_lifecycle() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::topic_lifecycle(storage).await?;

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
    async fn topic_lifecycle() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::topic_lifecycle(storage).await?;

        Ok(())
    }
}
