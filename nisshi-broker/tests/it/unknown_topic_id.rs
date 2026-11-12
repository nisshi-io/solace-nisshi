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

//! A topic ID that the broker does not know is answered `UNKNOWN_TOPIC_ID`, and a topic name that
//! it does not know stays `UNKNOWN_TOPIC_OR_PARTITION`, as in Kafka 3.9.1. The services choose the
//! code, so every storage engine answers the same.

use crate::common::{init_tracing, lite_storage, memory_storage, postgres_storage};
use nisshi_broker::Result;
use nisshi_sans_io::{
    ErrorCode, MetadataRequest, RequestInput, create_topics_request::CreatableTopic,
    metadata_request::MetadataRequestTopic,
};
use nisshi_storage::{
    ArcDynStorage, AutoCreateRefusals, MetadataService, Storage, TopicCreationConfig,
};
use rama::{Service, extensions::Extensions};
use rand::{prelude::*, rng};
use uuid::Uuid;

/// Metadata v12 asked for one unknown ID, one known ID and one unknown name. Nisshi answers a row
/// for each. Kafka 3.9.1 answers only the IDs once a request carries one, so the name row is a
/// known difference that this test does not pin to Kafka.
async fn metadata_by_id(storage: impl Storage + Clone) -> Result<()> {
    let name = crate::common::alphanumeric_string(15);

    let known = storage
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
    let unknown = Uuid::now_v7().into_bytes();
    let unknown_name = crate::common::alphanumeric_string(15);

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
                .topics(Some(vec![
                    MetadataRequestTopic::default().topic_id(Some(unknown)),
                    MetadataRequestTopic::default().topic_id(Some(known.into_bytes())),
                    MetadataRequestTopic::default().name(Some(unknown_name.clone())),
                ])),
            extensions: Extensions::default(),
        })
        .await?;

    let topics = response.topics.unwrap_or_default();
    assert_eq!(3, topics.len(), "{topics:?}");

    let by_id = |id: [u8; 16]| {
        topics
            .iter()
            .find(|topic| topic.topic_id == Some(id))
            .expect("a row for the ID")
    };

    let miss = by_id(unknown);
    assert_eq!(
        ErrorCode::UnknownTopicId,
        ErrorCode::try_from(miss.error_code)?
    );
    assert_eq!(None, miss.name);

    let hit = by_id(known.into_bytes());
    assert_eq!(ErrorCode::None, ErrorCode::try_from(hit.error_code)?);
    assert_eq!(Some(name), hit.name);

    let by_name = topics
        .iter()
        .find(|topic| topic.name.as_deref() == Some(unknown_name.as_str()))
        .expect("a row for the name");
    assert_eq!(
        ErrorCode::UnknownTopicOrPartition,
        ErrorCode::try_from(by_name.error_code)?
    );

    Ok(())
}

macro_rules! backend {
    ($name:ident, $feature:literal, $storage:path) => {
        #[cfg(feature = $feature)]
        mod $name {
            use super::*;

            async fn storage_container(
                cluster: impl Into<String> + Clone,
                node: i32,
            ) -> Result<ArcDynStorage> {
                $storage(cluster, node).await
            }

            #[tokio::test]
            async fn metadata_by_id() -> Result<()> {
                let _guard = init_tracing()?;

                let storage =
                    storage_container(Uuid::now_v7(), rng().random_range(0..i32::MAX)).await?;

                super::metadata_by_id(storage).await
            }
        }
    };
}

backend!(in_memory, "dynostore", memory_storage);
backend!(lite, "libsql", lite_storage);
backend!(pg, "postgres", postgres_storage);
