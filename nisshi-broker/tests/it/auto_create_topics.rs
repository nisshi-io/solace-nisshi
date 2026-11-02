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

//! Topic auto-creation by `Metadata`: the `auto.create.topics.enable` switch, the
//! `num.partitions` default, and the cap on the partitions that one request may create.

use crate::common::{alphanumeric_string, init_tracing};
use nisshi_broker::Result;
use nisshi_sans_io::{
    ErrorCode, MetadataRequest, MetadataResponse, RequestInput,
    metadata_request::MetadataRequestTopic,
};
use nisshi_storage::{AutoCreateRefusals, MetadataService, Storage, TopicCreationConfig};
use rama::{Service as _, extensions::Extensions};

/// Sends one `Metadata` request for `names`, with `allow_auto_topic_creation` as given.
async fn metadata(
    service: &MetadataService<impl Storage + Clone>,
    names: &[&str],
    allow_auto_topic_creation: Option<bool>,
) -> Result<MetadataResponse> {
    service
        .serve(RequestInput {
            request: MetadataRequest::default()
                .allow_auto_topic_creation(allow_auto_topic_creation)
                .topics(Some(
                    names
                        .iter()
                        .map(|name| MetadataRequestTopic::default().name(Some((*name).into())))
                        .collect(),
                )),
            extensions: Extensions::default(),
        })
        .await
        .map_err(Into::into)
}

/// Asserts that the response answers each topic with `expected`.
fn assert_each_topic(response: &MetadataResponse, expected: ErrorCode) -> Result<()> {
    let topics = response.topics.as_deref().unwrap_or_default();
    assert!(!topics.is_empty());

    for topic in topics {
        assert_eq!(
            expected,
            ErrorCode::try_from(topic.error_code)?,
            "{:?}",
            topic.name
        );
    }

    Ok(())
}

/// With `auto.create.topics.enable` off, a client that allows auto-creation (v4+) or a
/// client too old to say (v0-3, `None`) gets `UNKNOWN_TOPIC_OR_PARTITION`, and the topic
/// is not created. An invalid name still gets `INVALID_TOPIC_EXCEPTION`, as in Kafka.
async fn switched_off_creates_nothing(storage: impl Storage + Clone) -> Result<()> {
    let service = MetadataService {
        storage,
        topic_creation: TopicCreationConfig {
            auto_create_topics_enable: false,
            ..TopicCreationConfig::default()
        },
        auto_create_refusals: AutoCreateRefusals::default(),
    };

    for allow_auto_topic_creation in [Some(true), None] {
        let name = alphanumeric_string(15);

        let response = metadata(&service, &[&name], allow_auto_topic_creation).await?;
        assert_each_topic(&response, ErrorCode::UnknownTopicOrPartition)?;

        let response = metadata(&service, &[&name], Some(false)).await?;
        assert_each_topic(&response, ErrorCode::UnknownTopicOrPartition)?;
    }

    let response = metadata(&service, &["not a valid topic name"], Some(true)).await?;
    assert_each_topic(&response, ErrorCode::InvalidTopicException)?;

    Ok(())
}

/// Kafka sends every unknown name of one request to the controller as one `CreateTopics`,
/// and the controller rejects all of it when it asks for more than 10,000 partitions. So
/// none of the topics is created.
async fn over_the_cap_creates_nothing(storage: impl Storage + Clone) -> Result<()> {
    let service = MetadataService {
        storage,
        topic_creation: TopicCreationConfig {
            num_partitions: 5_000,
            ..TopicCreationConfig::default()
        },
        auto_create_refusals: AutoCreateRefusals::default(),
    };

    let names = [
        alphanumeric_string(15),
        alphanumeric_string(15),
        alphanumeric_string(15),
    ];
    let names = names.iter().map(String::as_str).collect::<Vec<_>>();

    let response = metadata(&service, &names, Some(true)).await?;
    assert_each_topic(&response, ErrorCode::UnknownTopicOrPartition)?;

    let response = metadata(&service, &names, Some(false)).await?;
    assert_each_topic(&response, ErrorCode::UnknownTopicOrPartition)?;

    Ok(())
}

/// The cap allows exactly 10,000 partitions, here across two topics.
async fn at_the_cap_creates_each_topic(storage: impl Storage + Clone) -> Result<()> {
    let service = MetadataService {
        storage,
        topic_creation: TopicCreationConfig {
            num_partitions: 5_000,
            ..TopicCreationConfig::default()
        },
        auto_create_refusals: AutoCreateRefusals::default(),
    };

    let names = [alphanumeric_string(15), alphanumeric_string(15)];
    let names = names.iter().map(String::as_str).collect::<Vec<_>>();

    let response = metadata(&service, &names, Some(true)).await?;
    assert_each_topic(&response, ErrorCode::None)?;

    for topic in response.topics.as_deref().unwrap_or_default() {
        assert_eq!(5_000, topic.partitions.as_deref().unwrap_or_default().len());
    }

    Ok(())
}

/// A name requested twice is created once, so the cap counts it once, as Kafka does with
/// its set of names.
async fn duplicate_name_counts_once(storage: impl Storage + Clone) -> Result<()> {
    let service = MetadataService {
        storage,
        topic_creation: TopicCreationConfig {
            num_partitions: 5_001,
            ..TopicCreationConfig::default()
        },
        auto_create_refusals: AutoCreateRefusals::default(),
    };

    let name = alphanumeric_string(15);

    let response = metadata(&service, &[&name, &name], Some(true)).await?;
    assert_each_topic(&response, ErrorCode::None)?;

    Ok(())
}

/// `num.partitions` sets the partition count of an auto-created topic.
async fn num_partitions_sets_the_partition_count(storage: impl Storage + Clone) -> Result<()> {
    let service = MetadataService {
        storage,
        topic_creation: TopicCreationConfig {
            num_partitions: 7,
            ..TopicCreationConfig::default()
        },
        auto_create_refusals: AutoCreateRefusals::default(),
    };

    let name = alphanumeric_string(15);

    let response = metadata(&service, &[&name], Some(true)).await?;
    assert_each_topic(&response, ErrorCode::None)?;

    let topics = response.topics.as_deref().unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(7, topics[0].partitions.as_deref().unwrap_or_default().len());

    Ok(())
}

/// A `num_partitions` below 1 creates nothing. Only a library user can set one, because the
/// broker's `--num-partitions` must be at least 1.
async fn num_partitions_below_one_creates_nothing(storage: impl Storage + Clone) -> Result<()> {
    for num_partitions in [0, -1_000_000] {
        let service = MetadataService {
            storage: storage.clone(),
            topic_creation: TopicCreationConfig {
                num_partitions,
                ..TopicCreationConfig::default()
            },
            auto_create_refusals: AutoCreateRefusals::default(),
        };

        let name = alphanumeric_string(15);

        let response = metadata(&service, &[&name], Some(true)).await?;
        assert_each_topic(&response, ErrorCode::UnknownTopicOrPartition)?;

        let response = metadata(&service, &[&name], Some(false)).await?;
        assert_each_topic(&response, ErrorCode::UnknownTopicOrPartition)?;
    }

    Ok(())
}

/// Auto-creation refuses a name that collides with an existing topic's name, because Kafka's
/// auto-creation sends a `CreateTopics`, which refuses it.
async fn colliding_name_is_not_created(storage: impl Storage + Clone) -> Result<()> {
    let service = MetadataService {
        storage,
        topic_creation: TopicCreationConfig::default(),
        auto_create_refusals: AutoCreateRefusals::default(),
    };

    let suffix = alphanumeric_string(15);
    let existing = format!("a_{suffix}");
    let colliding = format!("a.{suffix}");

    let response = metadata(&service, &[&existing], Some(true)).await?;
    assert_each_topic(&response, ErrorCode::None)?;

    // A producer retries, and each retry after the first takes the remembered refusal.
    for _ in 0..3 {
        let response = metadata(&service, &[&colliding], Some(true)).await?;
        assert_each_topic(&response, ErrorCode::UnknownTopicOrPartition)?;
    }

    let response = metadata(&service, &[&colliding], Some(false)).await?;
    assert_each_topic(&response, ErrorCode::UnknownTopicOrPartition)?;

    Ok(())
}

#[cfg(feature = "dynostore")]
mod in_memory {
    use super::*;
    use crate::common::memory_storage;
    use nisshi_broker::{Error, service::storage};
    use nisshi_sans_io::{CreateTopicsRequest, create_topics_request::CreatableTopic};
    use nisshi_service::{
        BytesFrameLayer, BytesFrameService, BytesLayer, BytesService, FrameBytesLayer,
        FrameBytesService, FrameRouteService, RequestFrameLayer, RequestFrameService,
    };
    use nisshi_storage::ArcDynStorage;
    use rama::Layer as _;
    use uuid::Uuid;

    async fn storage() -> Result<ArcDynStorage> {
        memory_storage(Uuid::now_v7(), 111).await
    }

    type Broker = RequestFrameService<
        FrameBytesService<BytesService<BytesFrameService<FrameRouteService<Error>>>>,
    >;

    /// The broker's routes, encoding each request and decoding each response, as a client sees
    /// them.
    fn broker(storage: ArcDynStorage, topic_creation: TopicCreationConfig) -> Result<Broker> {
        storage::services(
            FrameRouteService::<Error>::builder(),
            storage,
            topic_creation,
            AutoCreateRefusals::default(),
        )
        .and_then(|builder| builder.build().map_err(Into::into))
        .map(|frame_route| {
            (
                RequestFrameLayer,
                FrameBytesLayer,
                BytesLayer,
                BytesFrameLayer::default(),
            )
                .into_layer(frame_route)
        })
    }

    /// The broker's routes pass the configured settings to both `Metadata` and `CreateTopics`.
    #[tokio::test]
    async fn routes_use_the_configured_topic_creation() -> Result<()> {
        let _guard = init_tracing()?;

        let broker = broker(
            storage().await?,
            TopicCreationConfig {
                auto_create_topics_enable: false,
                num_partitions: 7,
            },
        )?;

        let name = alphanumeric_string(15);

        let response = broker
            .serve(RequestInput {
                request: MetadataRequest::default()
                    .allow_auto_topic_creation(Some(true))
                    .include_cluster_authorized_operations(Some(false))
                    .include_topic_authorized_operations(Some(false))
                    .topics(Some(vec![
                        MetadataRequestTopic::default()
                            .topic_id(Some([0; 16]))
                            .name(Some(name.clone())),
                    ])),
                extensions: Extensions::default(),
            })
            .await?;
        assert_each_topic(&response, ErrorCode::UnknownTopicOrPartition)?;

        let response = broker
            .serve(RequestInput {
                request: CreateTopicsRequest::default()
                    .timeout_ms(5_000)
                    .validate_only(Some(false))
                    .topics(Some(vec![
                        CreatableTopic::default()
                            .name(name)
                            .num_partitions(-1)
                            .replication_factor(-1)
                            .assignments(Some([].into()))
                            .configs(Some([].into())),
                    ])),
                extensions: Extensions::default(),
            })
            .await?;

        let topics = response.topics.as_deref().unwrap_or_default();
        assert_eq!(1, topics.len());
        assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);
        assert_eq!(Some(7), topics[0].num_partitions);

        Ok(())
    }

    #[tokio::test]
    async fn colliding_name_is_not_created() -> Result<()> {
        let _guard = init_tracing()?;
        super::colliding_name_is_not_created(storage().await?).await
    }

    #[tokio::test]
    async fn num_partitions_below_one_creates_nothing() -> Result<()> {
        let _guard = init_tracing()?;
        super::num_partitions_below_one_creates_nothing(storage().await?).await
    }

    #[tokio::test]
    async fn switched_off_creates_nothing() -> Result<()> {
        let _guard = init_tracing()?;
        super::switched_off_creates_nothing(storage().await?).await
    }

    #[tokio::test]
    async fn over_the_cap_creates_nothing() -> Result<()> {
        let _guard = init_tracing()?;
        super::over_the_cap_creates_nothing(storage().await?).await
    }

    #[tokio::test]
    async fn at_the_cap_creates_each_topic() -> Result<()> {
        let _guard = init_tracing()?;
        super::at_the_cap_creates_each_topic(storage().await?).await
    }

    #[tokio::test]
    async fn duplicate_name_counts_once() -> Result<()> {
        let _guard = init_tracing()?;
        super::duplicate_name_counts_once(storage().await?).await
    }

    #[tokio::test]
    async fn num_partitions_sets_the_partition_count() -> Result<()> {
        let _guard = init_tracing()?;
        super::num_partitions_sets_the_partition_count(storage().await?).await
    }
}

#[cfg(feature = "libsql")]
mod lite {
    use super::*;
    use crate::common::lite_storage;
    use nisshi_storage::ArcDynStorage;
    use uuid::Uuid;

    async fn storage() -> Result<ArcDynStorage> {
        lite_storage(Uuid::now_v7(), 111).await
    }

    #[tokio::test]
    async fn switched_off_creates_nothing() -> Result<()> {
        let _guard = init_tracing()?;
        super::switched_off_creates_nothing(storage().await?).await
    }

    #[tokio::test]
    async fn over_the_cap_creates_nothing() -> Result<()> {
        let _guard = init_tracing()?;
        super::over_the_cap_creates_nothing(storage().await?).await
    }

    #[tokio::test]
    async fn num_partitions_sets_the_partition_count() -> Result<()> {
        let _guard = init_tracing()?;
        super::num_partitions_sets_the_partition_count(storage().await?).await
    }

    #[tokio::test]
    async fn colliding_name_is_not_created() -> Result<()> {
        let _guard = init_tracing()?;
        super::colliding_name_is_not_created(storage().await?).await
    }
}

#[cfg(feature = "postgres")]
mod pg {
    use super::*;
    use crate::common::postgres_storage;
    use nisshi_storage::ArcDynStorage;
    use uuid::Uuid;

    async fn storage() -> Result<ArcDynStorage> {
        postgres_storage(Uuid::now_v7(), 111).await
    }

    #[tokio::test]
    async fn switched_off_creates_nothing() -> Result<()> {
        let _guard = init_tracing()?;
        super::switched_off_creates_nothing(storage().await?).await
    }

    #[tokio::test]
    async fn over_the_cap_creates_nothing() -> Result<()> {
        let _guard = init_tracing()?;
        super::over_the_cap_creates_nothing(storage().await?).await
    }

    #[tokio::test]
    async fn num_partitions_sets_the_partition_count() -> Result<()> {
        let _guard = init_tracing()?;
        super::num_partitions_sets_the_partition_count(storage().await?).await
    }

    #[tokio::test]
    async fn colliding_name_is_not_created() -> Result<()> {
        let _guard = init_tracing()?;
        super::colliding_name_is_not_created(storage().await?).await
    }
}
