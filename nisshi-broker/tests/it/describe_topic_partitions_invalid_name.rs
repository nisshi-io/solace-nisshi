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

//! DescribeTopicPartitions answers `INVALID_TOPIC_EXCEPTION` for an invalid topic name, as
//! Apache Kafka 3.9.1 does, and `UNKNOWN_TOPIC_OR_PARTITION` for a valid name that no topic has.

use std::assert_matches;

use crate::common::{
    alphanumeric_string, init_tracing, lite_storage, memory_storage, postgres_storage,
};
use nisshi_broker::Result;
use nisshi_sans_io::{
    DescribeTopicPartitionsRequest, ErrorCode, NULL_TOPIC_ID, RequestInput,
    describe_topic_partitions_request::TopicRequest,
    describe_topic_partitions_response::DescribeTopicPartitionsResponseTopic,
};
use nisshi_storage::{ArcDynStorage, DescribeTopicPartitionsService, Storage};
use rama::{Service, extensions::Extensions};
use rand::{RngExt as _, rng};
use uuid::Uuid;

/// Describes an unknown valid name and each kind of invalid name in one request, and checks
/// that only the invalid names answer `INVALID_TOPIC_EXCEPTION`.
async fn invalid_names_answer_invalid_topic_exception(storage: impl Storage + Clone) -> Result<()> {
    let service = DescribeTopicPartitionsService { storage };

    let unknown = alphanumeric_string(15);

    // Each name breaks one rule of Kafka's `Topic.validate`: empty, ".", "..", longer than 249,
    // and a character outside `[a-zA-Z0-9._-]`. The NUL byte is a character that PostgreSQL
    // refuses in a text parameter, so the engine fails the lookup instead of missing it.
    let invalid = [
        String::new(),
        ".".into(),
        "..".into(),
        "a".repeat(250),
        "a b".into(),
        "a\0b".into(),
    ];

    let requested = std::iter::once(&unknown).chain(&invalid);

    let response = service
        .serve(RequestInput {
            request: DescribeTopicPartitionsRequest::default().topics(Some(
                requested
                    .clone()
                    .map(|name| TopicRequest::default().name(name.clone()))
                    .collect(),
            )),
            extensions: Extensions::default(),
        })
        .await?;

    let topics = response.topics.unwrap_or_default();
    assert_eq!(1 + invalid.len(), topics.len());

    for (name, topic) in requested.zip(&topics) {
        let expected = if name == &unknown {
            ErrorCode::UnknownTopicOrPartition
        } else {
            ErrorCode::InvalidTopicException
        };

        assert_eq!(
            expected,
            ErrorCode::try_from(topic.error_code)?,
            "name {name:?}: {topic:?}"
        );

        assert_matches!(
            topic,
            DescribeTopicPartitionsResponseTopic {
                name: Some(echoed),
                topic_id: NULL_TOPIC_ID,
                partitions: Some(partitions),
                ..
            } if echoed == name && partitions.is_empty(),
            "name {name:?}"
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
    async fn invalid_names_answer_invalid_topic_exception() -> Result<()> {
        let _guard = init_tracing()?;

        let storage = storage_container(Uuid::now_v7(), rng().random_range(0..i32::MAX)).await?;

        super::invalid_names_answer_invalid_topic_exception(storage).await
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
    async fn invalid_names_answer_invalid_topic_exception() -> Result<()> {
        let _guard = init_tracing()?;

        let storage = storage_container(Uuid::now_v7(), rng().random_range(0..i32::MAX)).await?;

        super::invalid_names_answer_invalid_topic_exception(storage).await
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
    async fn invalid_names_answer_invalid_topic_exception() -> Result<()> {
        let _guard = init_tracing()?;

        let storage = storage_container(Uuid::now_v7(), rng().random_range(0..i32::MAX)).await?;

        super::invalid_names_answer_invalid_topic_exception(storage).await
    }
}
