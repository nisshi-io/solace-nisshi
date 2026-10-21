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

//! A stored replication factor outside what CreateTopics accepts, such as a
//! topic stored before that check existed, must still describe without a
//! panic. Metadata and DescribeTopicPartitions report no replicas for a
//! factor below one, and at most `MAX_REPLICATION_FACTOR` replicas above it.

use crate::common::{alphanumeric_string, init_tracing};
use nisshi_broker::Result;
use nisshi_sans_io::create_topics_request::CreatableTopic;
use nisshi_storage::{Storage, TopicId};
use rand::{RngExt as _, rng};
use uuid::Uuid;

const NUM_PARTITIONS: i32 = 3;

/// The response partition limit that Kafka clients send by default.
const PARTITION_LIMIT: i32 = 2_000;

async fn stored_out_of_range(storage: impl Storage + Clone, broker_id: i32) -> Result<()> {
    for (replication_factor, expected) in [
        (-2, vec![]),
        (3, vec![broker_id; 3]),
        (i16::MAX, vec![broker_id; 64]),
    ] {
        let name = alphanumeric_string(15);
        let context = format!("replication_factor = {replication_factor}");

        let id = storage
            .create_topic(
                CreatableTopic::default()
                    .name(name.clone())
                    .num_partitions(NUM_PARTITIONS)
                    .replication_factor(replication_factor)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            )
            .await?;

        let by_name = storage
            .metadata(Some(&[TopicId::Name(name.clone())]))
            .await?;
        let by_id = storage.metadata(Some(&[TopicId::Id(id)])).await?;
        let all_topics = storage.metadata(None).await?;

        for metadata in [by_name, by_id, all_topics] {
            let topic = metadata
                .topics
                .iter()
                .find(|topic| topic.name.as_deref() == Some(name.as_str()))
                .expect(&context);

            let partitions = topic.partitions.as_deref().unwrap_or_default();
            assert_eq!(NUM_PARTITIONS as usize, partitions.len(), "{context}");

            for partition in partitions {
                assert_eq!(
                    Some(&expected),
                    partition.replica_nodes.as_ref(),
                    "{context}"
                );
                assert_eq!(Some(&expected), partition.isr_nodes.as_ref(), "{context}");
            }
        }

        for topic_id in [TopicId::Name(name.clone()), TopicId::Id(id)] {
            let topics = storage
                .describe_topic_partitions(Some(&[topic_id]), PARTITION_LIMIT, None)
                .await?;

            assert_eq!(1, topics.len(), "{context}");

            let partitions = topics[0].partitions.as_deref().unwrap_or_default();
            assert_eq!(NUM_PARTITIONS as usize, partitions.len(), "{context}");

            for partition in partitions {
                assert_eq!(
                    Some(&expected),
                    partition.replica_nodes.as_ref(),
                    "{context}"
                );
                assert_eq!(Some(&expected), partition.isr_nodes.as_ref(), "{context}");
            }
        }
    }

    Ok(())
}

#[cfg(feature = "dynostore")]
mod in_memory {
    use super::*;
    use crate::common::memory_storage;

    #[tokio::test]
    async fn stored_out_of_range() -> Result<()> {
        let _guard = init_tracing()?;

        let broker_id = rng().random_range(0..i32::MAX);
        let storage = memory_storage(Uuid::now_v7(), broker_id).await?;

        super::stored_out_of_range(storage, broker_id).await
    }
}

#[cfg(feature = "libsql")]
mod lite {
    use super::*;
    use crate::common::lite_storage;

    #[tokio::test]
    async fn stored_out_of_range() -> Result<()> {
        let _guard = init_tracing()?;

        let broker_id = rng().random_range(0..i32::MAX);
        let storage = lite_storage(Uuid::now_v7(), broker_id).await?;

        super::stored_out_of_range(storage, broker_id).await
    }
}

#[cfg(feature = "postgres")]
mod pg {
    use super::*;
    use crate::common::postgres_storage;

    #[tokio::test]
    async fn stored_out_of_range() -> Result<()> {
        let _guard = init_tracing()?;

        let broker_id = rng().random_range(0..i32::MAX);
        let storage = postgres_storage(Uuid::now_v7(), broker_id).await?;

        super::stored_out_of_range(storage, broker_id).await
    }
}

#[cfg(feature = "slatedb")]
mod slatedb {
    use super::*;
    use crate::common::slate_storage;

    #[tokio::test]
    async fn stored_out_of_range() -> Result<()> {
        let _guard = init_tracing()?;

        let broker_id = rng().random_range(0..i32::MAX);
        let storage = slate_storage(Uuid::now_v7(), broker_id).await?;

        super::stored_out_of_range(storage, broker_id).await
    }
}

#[cfg(feature = "turso")]
mod turso {
    use super::*;
    use crate::common::{self, StorageType};
    use url::Url;

    #[ignore = "the Turso engine has no virtual_topic table yet"]
    #[tokio::test]
    async fn stored_out_of_range() -> Result<()> {
        let _guard = init_tracing()?;

        let broker_id = rng().random_range(0..i32::MAX);
        let storage = common::storage_container(
            StorageType::Turso,
            Uuid::now_v7(),
            broker_id,
            Url::parse("tcp://127.0.0.1/")?,
            None,
        )
        .await?;

        super::stored_out_of_range(storage, broker_id).await
    }
}
