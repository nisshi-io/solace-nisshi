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

//! A `CreateTopics` dry run answers as a real create would, and changes nothing.

use std::assert_matches;

use crate::common::{alphanumeric_string, init_tracing};
use nisshi_broker::Error;
use nisshi_sans_io::{
    CreateTopicsRequest, DescribeTopicPartitionsRequest, ErrorCode, NULL_TOPIC_ID, RequestInput,
    create_topics_request::CreatableTopic, create_topics_response::CreatableTopicResult,
    describe_topic_partitions_request::TopicRequest,
};
use nisshi_storage::{
    CreateTopicsService, CreateTopicsValidationLayer, DescribeTopicPartitionsService, Storage,
};
use rama::{Layer as _, Service as _, extensions::Extensions};

const PARTITIONS: i32 = 5;
const REPLICATION_FACTOR: i16 = 3;

async fn create_topic(
    storage: &(impl Storage + Clone),
    name: &str,
    num_partitions: i32,
    replication_factor: i16,
    validate_only: bool,
) -> Result<CreatableTopicResult, Error> {
    let service = CreateTopicsValidationLayer::new().layer(CreateTopicsService {
        storage: storage.clone(),
    });

    let response = service
        .serve(RequestInput {
            request: CreateTopicsRequest::default()
                .topics(Some(vec![
                    CreatableTopic::default()
                        .name(name.into())
                        .num_partitions(num_partitions)
                        .replication_factor(replication_factor)
                        .assignments(Some([].into()))
                        .configs(Some([].into())),
                ]))
                .validate_only(Some(validate_only)),
            extensions: Extensions::default(),
        })
        .await?;

    let mut topics = response.topics.unwrap_or_default();
    assert_eq!(1, topics.len());

    Ok(topics.remove(0))
}

/// What `DescribeTopicPartitions` reports for `name`: its error code and partition count.
///
/// `DescribeTopicPartitions` never auto-creates a topic, but `Metadata` does, so this
/// lookup is a safe way to ask whether a topic exists.
async fn describe(
    storage: &(impl Storage + Clone),
    name: &str,
) -> Result<(ErrorCode, usize), Error> {
    let service = DescribeTopicPartitionsService {
        storage: storage.clone(),
    };

    let response = service
        .serve(RequestInput {
            request: DescribeTopicPartitionsRequest::default()
                .topics(Some([TopicRequest::default().name(name.into())].into())),
            extensions: Extensions::default(),
        })
        .await?;

    let topics = response.topics.unwrap_or_default();
    assert_eq!(1, topics.len());

    Ok((
        ErrorCode::try_from(topics[0].error_code)?,
        topics[0].partitions.as_deref().unwrap_or_default().len(),
    ))
}

async fn new_topic_is_not_created(storage: impl Storage + Clone) -> Result<(), Error> {
    let name = alphanumeric_string(15);

    let result = create_topic(&storage, &name, PARTITIONS, REPLICATION_FACTOR, true).await?;

    assert_eq!(name, result.name);
    assert_ne!(Some(NULL_TOPIC_ID), result.topic_id);
    assert_eq!(Some(PARTITIONS), result.num_partitions);
    assert_eq!(Some(REPLICATION_FACTOR), result.replication_factor);
    assert_eq!(ErrorCode::None, ErrorCode::try_from(result.error_code)?);

    assert_eq!(
        (ErrorCode::UnknownTopicOrPartition, 0),
        describe(&storage, &name).await?
    );

    Ok(())
}

async fn existing_topic_is_reported_and_unchanged(
    storage: impl Storage + Clone,
) -> Result<(), Error> {
    let name = alphanumeric_string(15);

    let created = create_topic(&storage, &name, PARTITIONS, REPLICATION_FACTOR, false).await?;
    assert_eq!(ErrorCode::None, ErrorCode::try_from(created.error_code)?);

    let dry_run = create_topic(&storage, &name, 2, 1, true).await?;

    assert_eq!(name, dry_run.name);
    assert_eq!(Some(NULL_TOPIC_ID), dry_run.topic_id);
    assert_eq!(
        ErrorCode::TopicAlreadyExists,
        ErrorCode::try_from(dry_run.error_code)?
    );

    assert_eq!(
        (ErrorCode::None, usize::try_from(PARTITIONS)?),
        describe(&storage, &name).await?
    );

    Ok(())
}

async fn invalid_topic_is_rejected_as_in_a_real_create(
    storage: impl Storage + Clone,
) -> Result<(), Error> {
    let too_long = "a".repeat(250);

    let cases: [(&str, i32, i16); 4] = [
        ("a/b", 1, 1),
        (too_long.as_str(), 1, 1),
        ("valid-name", 0, 1),
        ("valid-name", 1, 0),
    ];

    for (name, num_partitions, replication_factor) in cases {
        let real = create_topic(&storage, name, num_partitions, replication_factor, false).await?;
        let dry_run =
            create_topic(&storage, name, num_partitions, replication_factor, true).await?;

        assert_ne!(ErrorCode::None, ErrorCode::try_from(real.error_code)?);
        assert_eq!(real.error_code, dry_run.error_code);
        assert_eq!(Some(NULL_TOPIC_ID), dry_run.topic_id);
    }

    Ok(())
}

async fn real_create_after_a_dry_run_succeeds(storage: impl Storage + Clone) -> Result<(), Error> {
    let name = alphanumeric_string(15);

    let dry_run = create_topic(&storage, &name, PARTITIONS, REPLICATION_FACTOR, true).await?;
    assert_eq!(ErrorCode::None, ErrorCode::try_from(dry_run.error_code)?);

    let created = create_topic(&storage, &name, PARTITIONS, REPLICATION_FACTOR, false).await?;

    assert_eq!(ErrorCode::None, ErrorCode::try_from(created.error_code)?);
    assert_ne!(Some(NULL_TOPIC_ID), created.topic_id);
    assert_ne!(dry_run.topic_id, created.topic_id);
    assert_matches!(
        describe(&storage, &name).await?,
        (ErrorCode::None, partitions) if partitions == usize::try_from(PARTITIONS)?
    );

    Ok(())
}

macro_rules! backend_tests {
    ($module:ident, $feature:literal, $storage:path) => {
        #[cfg(feature = $feature)]
        mod $module {
            use nisshi_broker::Result;
            use rand::{RngExt as _, rng};
            use uuid::Uuid;

            use super::*;

            async fn storage() -> Result<impl Storage + Clone> {
                $storage(Uuid::now_v7(), rng().random_range(0..i32::MAX)).await
            }

            #[tokio::test]
            async fn new_topic_is_not_created() -> Result<()> {
                let _guard = init_tracing()?;
                super::new_topic_is_not_created(storage().await?).await?;
                Ok(())
            }

            #[tokio::test]
            async fn existing_topic_is_reported_and_unchanged() -> Result<()> {
                let _guard = init_tracing()?;
                super::existing_topic_is_reported_and_unchanged(storage().await?).await?;
                Ok(())
            }

            #[tokio::test]
            async fn invalid_topic_is_rejected_as_in_a_real_create() -> Result<()> {
                let _guard = init_tracing()?;
                super::invalid_topic_is_rejected_as_in_a_real_create(storage().await?).await?;
                Ok(())
            }

            #[tokio::test]
            async fn real_create_after_a_dry_run_succeeds() -> Result<()> {
                let _guard = init_tracing()?;
                super::real_create_after_a_dry_run_succeeds(storage().await?).await?;
                Ok(())
            }
        }
    };
}

backend_tests!(in_memory, "dynostore", crate::common::memory_storage);
backend_tests!(lite, "libsql", crate::common::lite_storage);
backend_tests!(pg, "postgres", crate::common::postgres_storage);
