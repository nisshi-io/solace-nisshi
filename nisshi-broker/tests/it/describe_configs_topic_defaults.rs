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

//! `DescribeConfigs` for a topic lists every topic config, as Kafka 3.9.1 does: the configs that
//! the topic sets as `DYNAMIC_TOPIC_CONFIG`, and every other config with its default value as
//! `DEFAULT_CONFIG` ([`ConfigHelper.scala`]).
//!
//! [`ConfigHelper.scala`]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/ConfigHelper.scala#L174-L197

use crate::common::{self, alphanumeric_string, register_broker};
use nisshi_broker::Result;
use nisshi_sans_io::{
    ConfigResource, ConfigSource, ConfigType, DescribeConfigsRequest, ErrorCode, RequestInput,
    create_topics_request::{CreatableTopic, CreatableTopicConfig},
    describe_configs_request::DescribeConfigsResource,
    describe_configs_response::{DescribeConfigsResourceResult, DescribeConfigsResult},
};
use nisshi_storage::{DescribeConfigsService, Storage};
use rama::{Service, extensions::Extensions};
use rand::{prelude::*, rng};
use uuid::Uuid;

/// A config that a Kafka 3.9.1 broker lists for a topic that doesn't set it.
struct KafkaDefault {
    name: &'static str,
    value: &'static str,
    config_type: ConfigType,
}

impl KafkaDefault {
    const fn new(name: &'static str, value: &'static str, config_type: ConfigType) -> Self {
        Self {
            name,
            value,
            config_type,
        }
    }

    /// Returns the entry that Nisshi lists for this config.
    fn entry(&self) -> DescribeConfigsResourceResult {
        DescribeConfigsResourceResult::default()
            .name(self.name.into())
            .value(Some(self.value.into()))
            .read_only(false)
            .is_default(Some(true))
            .config_source(Some(ConfigSource::DefaultConfig.into()))
            .is_sensitive(false)
            .synonyms(Some([].into()))
            .config_type(Some(self.config_type.into()))
            .documentation(Some("".into()))
    }
}

/// The configs that a Kafka 3.9.1 broker lists for a topic created without any, with their values
/// from `kafka-configs --describe --all` against the `apache/kafka:3.9.1` image, and their types
/// from [`LogConfig`].
///
/// [`LogConfig`]: https://github.com/apache/kafka/blob/3.9.1/storage/src/main/java/org/apache/kafka/storage/internals/log/LogConfig.java#L262-L333
const KAFKA_TOPIC_CONFIGS: [KafkaDefault; 36] = [
    KafkaDefault::new("cleanup.policy", "delete", ConfigType::List),
    KafkaDefault::new("compression.gzip.level", "-1", ConfigType::Int),
    KafkaDefault::new("compression.lz4.level", "9", ConfigType::Int),
    KafkaDefault::new("compression.type", "producer", ConfigType::String),
    KafkaDefault::new("compression.zstd.level", "3", ConfigType::Int),
    KafkaDefault::new("delete.retention.ms", "86400000", ConfigType::Long),
    KafkaDefault::new("file.delete.delay.ms", "60000", ConfigType::Long),
    KafkaDefault::new("flush.messages", "9223372036854775807", ConfigType::Long),
    KafkaDefault::new("flush.ms", "9223372036854775807", ConfigType::Long),
    KafkaDefault::new(
        "follower.replication.throttled.replicas",
        "",
        ConfigType::List,
    ),
    KafkaDefault::new("index.interval.bytes", "4096", ConfigType::Int),
    KafkaDefault::new(
        "leader.replication.throttled.replicas",
        "",
        ConfigType::List,
    ),
    KafkaDefault::new("local.retention.bytes", "-2", ConfigType::Long),
    KafkaDefault::new("local.retention.ms", "-2", ConfigType::Long),
    KafkaDefault::new(
        "max.compaction.lag.ms",
        "9223372036854775807",
        ConfigType::Long,
    ),
    KafkaDefault::new("max.message.bytes", "1048588", ConfigType::Int),
    KafkaDefault::new("message.downconversion.enable", "true", ConfigType::Boolean),
    KafkaDefault::new("message.format.version", "3.0-IV1", ConfigType::String),
    KafkaDefault::new(
        "message.timestamp.after.max.ms",
        "9223372036854775807",
        ConfigType::Long,
    ),
    KafkaDefault::new(
        "message.timestamp.before.max.ms",
        "9223372036854775807",
        ConfigType::Long,
    ),
    KafkaDefault::new(
        "message.timestamp.difference.max.ms",
        "9223372036854775807",
        ConfigType::Long,
    ),
    KafkaDefault::new("message.timestamp.type", "CreateTime", ConfigType::String),
    KafkaDefault::new("min.cleanable.dirty.ratio", "0.5", ConfigType::Double),
    KafkaDefault::new("min.compaction.lag.ms", "0", ConfigType::Long),
    KafkaDefault::new("min.insync.replicas", "1", ConfigType::Int),
    KafkaDefault::new("preallocate", "false", ConfigType::Boolean),
    KafkaDefault::new("remote.log.copy.disable", "false", ConfigType::Boolean),
    KafkaDefault::new("remote.log.delete.on.disable", "false", ConfigType::Boolean),
    KafkaDefault::new("remote.storage.enable", "false", ConfigType::Boolean),
    KafkaDefault::new("retention.bytes", "-1", ConfigType::Long),
    KafkaDefault::new("retention.ms", "604800000", ConfigType::Long),
    KafkaDefault::new("segment.bytes", "1073741824", ConfigType::Int),
    KafkaDefault::new("segment.index.bytes", "10485760", ConfigType::Int),
    KafkaDefault::new("segment.jitter.ms", "0", ConfigType::Long),
    KafkaDefault::new("segment.ms", "604800000", ConfigType::Long),
    KafkaDefault::new(
        "unclean.leader.election.enable",
        "false",
        ConfigType::Boolean,
    ),
];

/// Returns the names of [`KAFKA_TOPIC_CONFIGS`].
fn kafka_names() -> Vec<&'static str> {
    KAFKA_TOPIC_CONFIGS
        .iter()
        .map(|config| config.name)
        .collect()
}

/// Creates a topic with `configs`, and returns its name.
async fn create_topic<G>(storage: &G, configs: &[(&str, &str)]) -> Result<String>
where
    G: Storage,
{
    let name = alphanumeric_string(15);

    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(name.clone())
                .num_partitions(1)
                .replication_factor(0)
                .assignments(Some([].into()))
                .configs(Some(
                    configs
                        .iter()
                        .map(|(name, value)| {
                            CreatableTopicConfig::default()
                                .name((*name).into())
                                .value(Some((*value).into()))
                        })
                        .collect(),
                )),
            false,
        )
        .await?;

    Ok(name)
}

/// Describes the topic `name` through [`DescribeConfigsService`], selecting `keys`.
async fn describe_topic<G>(
    storage: G,
    name: &str,
    keys: Option<Vec<String>>,
) -> Result<DescribeConfigsResult>
where
    G: Storage,
{
    let response = DescribeConfigsService { storage }
        .serve(RequestInput {
            request: DescribeConfigsRequest::default()
                .include_documentation(Some(false))
                .include_synonyms(Some(false))
                .resources(Some(
                    [DescribeConfigsResource::default()
                        .resource_type(ConfigResource::Topic.into())
                        .resource_name(name.into())
                        .configuration_keys(keys)]
                    .into(),
                )),
            extensions: Extensions::default(),
        })
        .await?;

    let mut results = response.results.unwrap_or_default();
    assert_eq!(1, results.len());
    Ok(results.remove(0))
}

/// Returns the entry for `name` in `result`, and checks that `result` lists it once.
fn config<'a>(result: &'a DescribeConfigsResult, name: &str) -> &'a DescribeConfigsResourceResult {
    let mut matching = result
        .configs
        .as_deref()
        .unwrap_or_default()
        .iter()
        .filter(|config| config.name == name);

    let config = matching
        .next()
        .unwrap_or_else(|| panic!("{name} is not listed"));
    assert!(matching.next().is_none(), "{name} is listed more than once");
    config
}

fn names(result: &DescribeConfigsResult) -> Vec<&str> {
    result
        .configs
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(|config| config.name.as_str())
        .collect()
}

pub async fn topic_without_configs_lists_kafka_defaults<G>(
    cluster_id: impl Into<String>,
    broker_id: i32,
    storage: G,
) -> Result<()>
where
    G: Storage + Clone,
{
    register_broker(cluster_id, broker_id, &storage).await?;
    let topic = create_topic(&storage, &[]).await?;

    let result = describe_topic(storage, &topic, None).await?;

    assert_eq!(i16::from(ErrorCode::None), result.error_code);
    assert_eq!(
        Some(
            KAFKA_TOPIC_CONFIGS
                .iter()
                .map(KafkaDefault::entry)
                .collect::<Vec<_>>()
        ),
        result.configs
    );

    Ok(())
}

/// An empty `configuration_keys` selects every config, as an absent one does in Kafka.
pub async fn empty_configuration_keys_select_every_config<G>(
    cluster_id: impl Into<String>,
    broker_id: i32,
    storage: G,
) -> Result<()>
where
    G: Storage + Clone,
{
    register_broker(cluster_id, broker_id, &storage).await?;
    let batch = "nisshi.batch";
    let topic = create_topic(&storage, &[(batch, "true")]).await?;

    let result = describe_topic(storage, &topic, Some(vec![])).await?;

    assert_eq!(i16::from(ErrorCode::None), result.error_code);

    let mut expected = kafka_names();
    expected.push(batch);
    expected.sort_unstable();
    assert_eq!(expected, names(&result));

    assert_eq!(Some("true"), config(&result, batch).value.as_deref());

    Ok(())
}

pub async fn stored_config_is_listed_once_as_dynamic_with_its_kafka_type<G>(
    cluster_id: impl Into<String>,
    broker_id: i32,
    storage: G,
) -> Result<()>
where
    G: Storage + Clone,
{
    register_broker(cluster_id, broker_id, &storage).await?;
    let topic = create_topic(&storage, &[("retention.ms", "123456")]).await?;

    let result = describe_topic(storage, &topic, None).await?;

    assert_eq!(i16::from(ErrorCode::None), result.error_code);
    assert_eq!(kafka_names(), names(&result));

    let retention_ms = config(&result, "retention.ms");
    assert_eq!(Some("123456"), retention_ms.value.as_deref());
    assert_eq!(
        Some(i8::from(ConfigSource::DynamicTopicConfig)),
        retention_ms.config_source
    );
    assert_eq!(Some(i8::from(ConfigType::Long)), retention_ms.config_type);
    assert_ne!(Some(true), retention_ms.is_default);

    Ok(())
}

pub async fn unknown_topic_is_unknown_topic_or_partition<G>(
    cluster_id: impl Into<String>,
    broker_id: i32,
    storage: G,
) -> Result<()>
where
    G: Storage + Clone,
{
    register_broker(cluster_id, broker_id, &storage).await?;
    let topic = alphanumeric_string(15);

    let result = describe_topic(storage, &topic, None).await?;

    assert_eq!(
        i16::from(ErrorCode::UnknownTopicOrPartition),
        result.error_code
    );
    assert_eq!(topic, result.resource_name);
    assert!(result.configs.as_deref().unwrap_or_default().is_empty());

    Ok(())
}

pub async fn configuration_keys_select_only_those_keys<G>(
    cluster_id: impl Into<String>,
    broker_id: i32,
    storage: G,
) -> Result<()>
where
    G: Storage + Clone,
{
    register_broker(cluster_id, broker_id, &storage).await?;
    let topic = create_topic(
        &storage,
        &[("retention.ms", "123456"), ("cleanup.policy", "compact")],
    )
    .await?;

    let result = describe_topic(
        storage,
        &topic,
        Some(vec!["segment.bytes".into(), "retention.ms".into()]),
    )
    .await?;

    assert_eq!(i16::from(ErrorCode::None), result.error_code);
    assert_eq!(vec!["retention.ms", "segment.bytes"], names(&result));

    assert_eq!(
        Some(i8::from(ConfigSource::DynamicTopicConfig)),
        config(&result, "retention.ms").config_source
    );

    let segment_bytes = config(&result, "segment.bytes");
    assert_eq!(Some("1073741824"), segment_bytes.value.as_deref());
    assert_eq!(
        Some(i8::from(ConfigSource::DefaultConfig)),
        segment_bytes.config_source
    );
    assert_eq!(Some(i8::from(ConfigType::Int)), segment_bytes.config_type);

    Ok(())
}

pub async fn config_outside_kafka_is_still_listed<G>(
    cluster_id: impl Into<String>,
    broker_id: i32,
    storage: G,
) -> Result<()>
where
    G: Storage + Clone,
{
    register_broker(cluster_id, broker_id, &storage).await?;
    let lake_partition = "nisshi.lake.partition";
    let topic = create_topic(&storage, &[(lake_partition, "year")]).await?;

    let result = describe_topic(storage, &topic, None).await?;

    assert_eq!(i16::from(ErrorCode::None), result.error_code);
    assert_eq!(KAFKA_TOPIC_CONFIGS.len() + 1, names(&result).len());

    let config = config(&result, lake_partition);
    assert_eq!(Some("year"), config.value.as_deref());
    assert_eq!(
        Some(i8::from(ConfigSource::DynamicTopicConfig)),
        config.config_source
    );
    assert_eq!(Some(i8::from(ConfigType::String)), config.config_type);

    Ok(())
}

/// Runs `$check` against a new storage container of `$storage_type`.
macro_rules! check {
    ($check:ident, $storage_type:expr) => {{
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::$check(
            cluster_id,
            broker_id,
            common::storage_container(
                $storage_type,
                cluster_id,
                broker_id,
                Url::parse("tcp://127.0.0.1/")?,
                None,
            )
            .await?,
        )
        .await
    }};
}

macro_rules! engine {
    ($module:ident, $feature:literal, $storage_type:expr $(, #[$attribute:meta])?) => {
        #[cfg(feature = $feature)]
        mod $module {
            use crate::common::{StorageType, init_tracing};
            use url::Url;

            use super::*;

            #[tokio::test]
            $(#[$attribute])?
            async fn topic_without_configs_lists_kafka_defaults() -> Result<()> {
                check!(topic_without_configs_lists_kafka_defaults, $storage_type)
            }

            #[tokio::test]
            $(#[$attribute])?
            async fn stored_config_is_listed_once_as_dynamic_with_its_kafka_type() -> Result<()> {
                check!(
                    stored_config_is_listed_once_as_dynamic_with_its_kafka_type,
                    $storage_type
                )
            }

            #[tokio::test]
            $(#[$attribute])?
            async fn unknown_topic_is_unknown_topic_or_partition() -> Result<()> {
                check!(unknown_topic_is_unknown_topic_or_partition, $storage_type)
            }

            #[tokio::test]
            $(#[$attribute])?
            async fn configuration_keys_select_only_those_keys() -> Result<()> {
                check!(configuration_keys_select_only_those_keys, $storage_type)
            }

            #[tokio::test]
            $(#[$attribute])?
            async fn empty_configuration_keys_select_every_config() -> Result<()> {
                check!(empty_configuration_keys_select_every_config, $storage_type)
            }

            #[tokio::test]
            $(#[$attribute])?
            async fn config_outside_kafka_is_still_listed() -> Result<()> {
                check!(config_outside_kafka_is_still_listed, $storage_type)
            }
        }
    };
}

engine!(pg, "postgres", StorageType::Postgres);
engine!(in_memory, "dynostore", StorageType::InMemory);
engine!(lite, "libsql", StorageType::Lite);
engine!(
    turso,
    "turso",
    StorageType::Turso,
    #[ignore = "the Turso engine does not start in the broker tests yet, see #866"]
);
