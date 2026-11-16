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

//! The names of Kafka's internal topics on `CreateTopics` and on `Metadata` auto-creation:
//! `__cluster_metadata`, which no request creates, and `__consumer_offsets`,
//! `__transaction_state` and `__share_group_state`, which `Metadata` does not auto-create.

use std::collections::BTreeMap;

use nisshi_broker::Result;
use nisshi_sans_io::{
    CreateTopicsRequest, ErrorCode, MetadataRequest, RequestInput,
    create_topics_request::CreatableTopic, create_topics_response::CreatableTopicResult,
    metadata_request::MetadataRequestTopic, metadata_response::MetadataResponseTopic,
};
use nisshi_storage::{
    AutoCreateRefusals, CreateTopicsService, CreateTopicsValidationLayer, MetadataService, Storage,
    TopicCreationConfig,
};
use rama::{Layer as _, Service as _, extensions::Extensions};

const CLUSTER_METADATA: &str = "__cluster_metadata";

const INTERNAL_NAMES: [&str; 3] = [
    "__consumer_offsets",
    "__transaction_state",
    "__share_group_state",
];

const PROHIBITED: &str = "Creation of internal topic __cluster_metadata is prohibited.";

/// Sends one `CreateTopics` for `names`, each with the broker defaults, and returns the result
/// of each topic in the order of the request.
async fn create_topics(
    storage: impl Storage + Clone,
    names: &[&str],
    validate_only: bool,
) -> Result<Vec<CreatableTopicResult>> {
    let service = CreateTopicsValidationLayer::new().layer(CreateTopicsService { storage });

    let response = service
        .serve(RequestInput {
            request: CreateTopicsRequest::default()
                .timeout_ms(5_000)
                .validate_only(Some(validate_only))
                .topics(Some(
                    names
                        .iter()
                        .map(|name| {
                            CreatableTopic::default()
                                .name((*name).into())
                                .num_partitions(-1)
                                .replication_factor(-1)
                                .assignments(Some([].into()))
                                .configs(Some([].into()))
                        })
                        .collect(),
                )),
            extensions: Extensions::default(),
        })
        .await?;

    Ok(response.topics.unwrap_or_default())
}

/// Sends one `Metadata` for `names`, with the broker's default topic creation settings, and
/// returns the topics of the response.
async fn metadata(
    storage: impl Storage + Clone,
    names: &[&str],
    allow_auto_topic_creation: bool,
) -> Result<Vec<MetadataResponseTopic>> {
    let service = MetadataService {
        storage,
        topic_creation: TopicCreationConfig::default(),
        auto_create_refusals: AutoCreateRefusals::default(),
    };

    let response = service
        .serve(RequestInput {
            request: MetadataRequest::default()
                .allow_auto_topic_creation(Some(allow_auto_topic_creation))
                .topics(Some(
                    names
                        .iter()
                        .map(|name| MetadataRequestTopic::default().name(Some((*name).into())))
                        .collect(),
                )),
            extensions: Extensions::default(),
        })
        .await?;

    Ok(response.topics.unwrap_or_default())
}

/// Returns the error code of each topic of a `Metadata` response, by name.
fn error_codes(topics: &[MetadataResponseTopic]) -> Result<BTreeMap<String, ErrorCode>> {
    topics
        .iter()
        .map(|topic| {
            Ok((
                topic.name.clone().unwrap_or_default(),
                ErrorCode::try_from(topic.error_code)?,
            ))
        })
        .collect()
}

#[cfg(feature = "dynostore")]
mod in_memory {
    use std::assert_matches;

    use uuid::Uuid;

    use super::*;
    use crate::common::{alphanumeric_string, init_tracing, memory_storage};
    use nisshi_storage::ArcDynStorage;

    async fn storage() -> Result<ArcDynStorage> {
        memory_storage(Uuid::now_v7(), 111).await
    }

    /// Kafka answers `__cluster_metadata` with `INVALID_REQUEST` and this message, and creates
    /// the other topics of the request.
    #[tokio::test]
    async fn create_topics_refuses_cluster_metadata_and_creates_the_rest() -> Result<()> {
        let _guard = init_tracing()?;
        let storage = storage().await?;
        let other = alphanumeric_string(15);

        let results = create_topics(storage.clone(), &[CLUSTER_METADATA, &other], false).await?;

        assert_eq!(2, results.len());
        assert_eq!(CLUSTER_METADATA, results[0].name);
        assert_matches!(
            ErrorCode::try_from(results[0].error_code),
            Ok(ErrorCode::InvalidRequest)
        );
        assert_eq!(Some(PROHIBITED), results[0].error_message.as_deref());
        assert_eq!(other, results[1].name);
        assert_matches!(
            ErrorCode::try_from(results[1].error_code),
            Ok(ErrorCode::None)
        );

        let codes = error_codes(&metadata(storage, &[CLUSTER_METADATA, &other], false).await?)?;

        assert_eq!(
            Some(&ErrorCode::UnknownTopicOrPartition),
            codes.get(CLUSTER_METADATA)
        );
        assert_eq!(Some(&ErrorCode::None), codes.get(&other));
        Ok(())
    }

    /// Kafka removes `__cluster_metadata` from the request before its controller sees
    /// `validate_only`, so a dry run is refused the same way.
    #[tokio::test]
    async fn validate_only_refuses_cluster_metadata() -> Result<()> {
        let _guard = init_tracing()?;
        let storage = storage().await?;

        let results = create_topics(storage.clone(), &[CLUSTER_METADATA], true).await?;

        assert_eq!(1, results.len());
        assert_matches!(
            ErrorCode::try_from(results[0].error_code),
            Ok(ErrorCode::InvalidRequest)
        );
        assert_eq!(Some(PROHIBITED), results[0].error_message.as_deref());

        let codes = error_codes(&metadata(storage, &[CLUSTER_METADATA], false).await?)?;

        assert_eq!(
            Some(&ErrorCode::UnknownTopicOrPartition),
            codes.get(CLUSTER_METADATA)
        );
        Ok(())
    }

    /// A `Metadata` that allows auto-creation answers `UNKNOWN_TOPIC_OR_PARTITION` for
    /// `__cluster_metadata` and for each internal name, and creates none of them.
    #[tokio::test]
    async fn metadata_does_not_auto_create_cluster_metadata_or_an_internal_name() -> Result<()> {
        let _guard = init_tracing()?;
        let storage = storage().await?;

        let mut names = vec![CLUSTER_METADATA];
        names.extend(INTERNAL_NAMES);

        for allow_auto_topic_creation in [true, false] {
            let codes =
                error_codes(&metadata(storage.clone(), &names, allow_auto_topic_creation).await?)?;

            assert_eq!(names.len(), codes.len(), "{allow_auto_topic_creation}");

            for name in &names {
                assert_eq!(
                    Some(&ErrorCode::UnknownTopicOrPartition),
                    codes.get(*name),
                    "{name} with allow_auto_topic_creation {allow_auto_topic_creation}"
                );
            }
        }

        Ok(())
    }

    /// `CreateTopics` creates a topic with an internal name, as Kafka does, and the topic is an
    /// ordinary topic with the broker's default partitions.
    #[tokio::test]
    async fn create_topics_creates_an_internal_name() -> Result<()> {
        let _guard = init_tracing()?;
        let storage = storage().await?;

        let results = create_topics(storage.clone(), &INTERNAL_NAMES, false).await?;

        assert_eq!(INTERNAL_NAMES.len(), results.len());

        for result in &results {
            assert_matches!(
                ErrorCode::try_from(result.error_code),
                Ok(ErrorCode::None),
                "{}",
                result.name
            );
        }

        let topics = metadata(storage, &INTERNAL_NAMES, false).await?;

        assert_eq!(INTERNAL_NAMES.len(), topics.len());

        for topic in &topics {
            assert_matches!(
                ErrorCode::try_from(topic.error_code),
                Ok(ErrorCode::None),
                "{:?}",
                topic.name
            );
            assert_eq!(
                1,
                topic.partitions.as_deref().unwrap_or_default().len(),
                "{:?}",
                topic.name
            );
        }

        Ok(())
    }
}
