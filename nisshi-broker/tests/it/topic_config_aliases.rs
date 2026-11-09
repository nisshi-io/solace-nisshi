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

//! Checks that a `tansu.*` topic configuration key, which the previous release stored, behaves
//! like its `nisshi.*` key on the storage engines that read it, that DescribeConfigs returns the
//! name that the client set, and that the `nisshi.*` key wins when a topic sets both.
//!
//! The `tansu.*` alias warning is process-wide and a test checks the log of the whole process,
//! so each test needs a process of its own, as `cargo nextest` gives it.

use std::{
    io,
    sync::{Arc, Mutex, OnceLock},
};
use tokio::time::Instant;

use bytes::Bytes;
use nisshi_broker::{Error, Result};
use nisshi_sans_io::{
    ConfigResource, ErrorCode,
    create_topics_request::{CreatableTopic, CreatableTopicConfig},
    record::{Record, inflated},
    topic_config,
};
use nisshi_schema::Registry;
use nisshi_storage::{ArcDynStorage, Storage, Topition};
use rand::{prelude::*, rng};
use serde_json::json;
use tracing::Level;
use url::Url;
use uuid::Uuid;

use crate::common::{
    self, StorageType, alphanumeric_string, lite_storage, memory_storage, postgres_storage,
    register_broker, slate_storage,
};
use crate::produce::produce_batches_with_schemas;

const DEPRECATION: &str = "deprecated topic configuration key";

#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Vec<u8>>>);

impl io::Write for Capture {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0
            .lock()
            .map_err(|err| io::Error::other(err.to_string()))?
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Capture {
    fn text(&self) -> String {
        let written = self
            .0
            .lock()
            .map(|buffer| buffer.clone())
            .unwrap_or_default();
        String::from_utf8_lossy(&written).into_owned()
    }
}

/// Returns the buffer that the process-wide subscriber writes each warning to.
///
/// The subscriber is global, not a thread-local default, because a storage engine can run a
/// query on a thread other than the test's.
fn capture() -> &'static Capture {
    static CAPTURE: OnceLock<Capture> = OnceLock::new();

    CAPTURE.get_or_init(|| {
        let capture = Capture::default();
        let writer = capture.clone();

        tracing::subscriber::set_global_default(
            tracing_subscriber::fmt()
                .with_max_level(Level::WARN)
                .with_ansi(false)
                .with_writer(move || writer.clone())
                .finish(),
        )
        .expect("no other global subscriber in this test process");

        capture
    })
}

fn config(name: &str, value: &str) -> CreatableTopicConfig {
    CreatableTopicConfig::default()
        .name(name.into())
        .value(Some(value.into()))
}

async fn create_topic(
    storage: &impl Storage,
    name: &str,
    configs: Vec<CreatableTopicConfig>,
) -> Result<()> {
    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(name.into())
                .num_partitions(1)
                .replication_factor(0)
                .assignments(Some([].into()))
                .configs(Some(configs)),
            false,
        )
        .await?;

    Ok(())
}

async fn config_names(storage: &impl Storage, topic: &str) -> Result<Vec<String>> {
    Ok(storage
        .describe_config(topic, ConfigResource::Topic, None)
        .await?
        .configs
        .unwrap_or_default()
        .into_iter()
        .map(|config| config.name)
        .collect())
}

async fn person_storage(
    storage_type: StorageType,
    cluster: Uuid,
    node: i32,
) -> Result<ArcDynStorage> {
    common::storage_container(
        storage_type,
        cluster,
        node,
        Url::parse("tcp://127.0.0.1/")?,
        Some(person_schemas()?),
    )
    .await
}

/// The registry that holds the `person` schema.
fn person_schemas() -> Result<Registry> {
    Url::parse("file://../etc/schema")
        .map_err(Error::from)
        .and_then(|url| {
            Registry::builder_try_from_url(&url)
                .map(|builder| builder.build())
                .map_err(Into::into)
        })
}

/// Produces a person with a negative age, which the person schema rejects, and returns the error
/// code that the partition answers with.
async fn produce_invalid_person(storage: &ArcDynStorage, topic: &str) -> Result<ErrorCode> {
    let batch = inflated::Batch::builder()
        .record(
            Record::builder()
                .key(Some(Bytes::from_static(b"\"345-67-6543\"")))
                .value(Some(Bytes::from(serde_json::to_vec(&json!({
                    "firstName": "John",
                    "lastName": "Doe",
                    "age": -1,
                }))?))),
        )
        .build()
        .and_then(TryInto::try_into)?;

    let response = produce_batches_with_schemas(
        storage.clone(),
        Some(person_schemas()?),
        topic,
        0,
        vec![batch],
    )
    .await?;

    ErrorCode::try_from(response.error_code).map_err(Into::into)
}

/// Creates the `person` topic with `configs`, produces an invalid person to it twice, and returns
/// whether the storage engine accepts it. The second produce checks that a deprecation warning
/// is logged once, not for each produce.
async fn accepts_invalid_person(
    storage_type: StorageType,
    configs: Vec<CreatableTopicConfig>,
) -> Result<bool> {
    let cluster = Uuid::now_v7();
    let node = rng().random_range(0..i32::MAX);
    let storage = person_storage(storage_type, cluster, node).await?;
    register_broker(cluster, node, &storage).await?;

    create_topic(&storage, "person", configs).await?;

    let first = produce_invalid_person(&storage, "person").await?;
    let second = produce_invalid_person(&storage, "person").await?;
    assert_eq!(first, second);

    match first {
        ErrorCode::None => Ok(true),
        ErrorCode::InvalidRecord => Ok(false),
        error_code => Err(Error::Api(error_code)),
    }
}

/// Creates a topic with `configs`, produces a record for each of two keys, and returns the number
/// of records that a fetch of `topic/key-a` returns, or `None` when the fetch fails. The fetch
/// fails when the topic is not virtual, because then no topic has that name.
async fn records_of_virtual_topic(
    storage_type: StorageType,
    configs: Vec<CreatableTopicConfig>,
) -> Result<Option<usize>> {
    use nisshi_sans_io::IsolationLevel;
    use std::time::Duration;

    let cluster = Uuid::now_v7();
    let node = rng().random_range(0..i32::MAX);
    let storage = common::storage_container(
        storage_type,
        cluster,
        node,
        Url::parse("tcp://127.0.0.1/")?,
        None,
    )
    .await?;
    register_broker(cluster, node, &storage).await?;

    let topic = alphanumeric_string(15);
    create_topic(&storage, &topic, configs).await?;

    for key in ["key-a", "key-b", "key-a"] {
        let producer = storage
            .init_producer(None, 10_000, Some(-1), Some(-1))
            .await?;

        let batch = inflated::Batch::builder()
            .record(
                Record::builder()
                    .key(Some(Bytes::copy_from_slice(key.as_bytes())))
                    .value(Some(Bytes::from_static(b"value"))),
            )
            .producer_id(producer.id)
            .producer_epoch(producer.epoch)
            .build()
            .and_then(TryInto::try_into)?;

        _ = storage
            .produce(None, &Topition::new(topic.clone(), 0), batch)
            .await?;
    }

    let Ok(batches) = storage
        .fetch(
            &Topition::new(format!("{topic}/key-a"), 0),
            0,
            1,
            50 * 1_024,
            IsolationLevel::ReadUncommitted,
            Instant::now() + Duration::from_millis(500),
        )
        .await
    else {
        return Ok(None);
    };

    Ok(Some(
        batches
            .into_iter()
            .map(|batch| inflated::Batch::try_from(batch).map(|batch| batch.records.len()))
            .collect::<std::result::Result<Vec<_>, _>>()?
            .into_iter()
            .sum(),
    ))
}

mod schema_validation {
    use super::*;

    async fn accepts(
        storage_type: impl Fn() -> StorageType,
        configs: &[(&str, &str)],
    ) -> Result<bool> {
        accepts_invalid_person(
            storage_type(),
            configs
                .iter()
                .map(|(name, value)| config(name, value))
                .collect(),
        )
        .await
    }

    macro_rules! engine {
        ($module:ident, $feature:literal, $storage_type:expr) => {
            #[cfg(feature = $feature)]
            mod $module {
                use super::*;

                #[tokio::test]
                async fn validation_is_on_by_default() -> Result<()> {
                    assert!(!accepts(|| $storage_type, &[]).await?);
                    Ok(())
                }

                #[tokio::test]
                async fn nisshi_key_turns_validation_off() -> Result<()> {
                    assert!(
                        accepts(
                            || $storage_type,
                            &[(topic_config::SCHEMA_VALIDATION, "false")]
                        )
                        .await?
                    );
                    Ok(())
                }

                #[tokio::test]
                async fn alias_turns_validation_off_like_its_key() -> Result<()> {
                    assert!(
                        accepts(|| $storage_type, &[("tansu.schema.validation", "false")]).await?
                    );
                    Ok(())
                }

                #[tokio::test]
                async fn key_wins_over_alias_that_turns_validation_off() -> Result<()> {
                    assert!(
                        !accepts(
                            || $storage_type,
                            &[
                                ("tansu.schema.validation", "false"),
                                (topic_config::SCHEMA_VALIDATION, "true"),
                            ]
                        )
                        .await?
                    );
                    Ok(())
                }

                #[tokio::test]
                async fn key_wins_over_alias_that_turns_validation_on() -> Result<()> {
                    assert!(
                        accepts(
                            || $storage_type,
                            &[
                                (topic_config::SCHEMA_VALIDATION, "false"),
                                ("tansu.schema.validation", "true"),
                            ]
                        )
                        .await?
                    );
                    Ok(())
                }
            }
        };
    }

    engine!(lite, "libsql", StorageType::Lite);
    engine!(pg, "postgres", StorageType::Postgres);
}

mod virtual_topic {
    use super::*;

    macro_rules! engine {
        ($module:ident, $feature:literal, $storage_type:expr) => {
            #[cfg(feature = $feature)]
            mod $module {
                use super::*;

                #[tokio::test]
                async fn nisshi_key_makes_a_virtual_topic() -> Result<()> {
                    assert_eq!(
                        Some(2),
                        records_of_virtual_topic(
                            $storage_type,
                            vec![config(topic_config::VIRTUAL, "true")]
                        )
                        .await?
                    );
                    Ok(())
                }

                #[tokio::test]
                async fn alias_makes_a_virtual_topic_like_its_key() -> Result<()> {
                    assert_eq!(
                        Some(2),
                        records_of_virtual_topic(
                            $storage_type,
                            vec![config("tansu.virtual", "true")]
                        )
                        .await?
                    );
                    Ok(())
                }

                #[tokio::test]
                async fn key_wins_over_alias() -> Result<()> {
                    assert_eq!(
                        None,
                        records_of_virtual_topic(
                            $storage_type,
                            vec![
                                config("tansu.virtual", "true"),
                                config(topic_config::VIRTUAL, "false"),
                            ]
                        )
                        .await?
                    );
                    Ok(())
                }
            }
        };
    }

    engine!(lite, "libsql", StorageType::Lite);
    engine!(pg, "postgres", StorageType::Postgres);
}

mod deprecation_warning {
    use super::*;

    /// Produces invalid persons to a topic that sets `tansu.schema.validation`, and returns the
    /// log of the process.
    #[cfg(feature = "libsql")]
    async fn log_of_alias_use() -> Result<String> {
        let capture = capture();

        _ = accepts_invalid_person(
            StorageType::Lite,
            vec![config("tansu.schema.validation", "false")],
        )
        .await?;

        Ok(capture.text())
    }

    #[cfg(feature = "libsql")]
    #[tokio::test]
    async fn alias_is_warned_about_once_with_its_key_and_never_its_value() -> Result<()> {
        let log = log_of_alias_use().await?;

        assert_eq!(1, log.matches(DEPRECATION).count(), "{log}");
        assert!(log.contains("tansu.schema.validation"), "{log}");
        assert!(log.contains(topic_config::SCHEMA_VALIDATION), "{log}");
        Ok(())
    }

    #[cfg(feature = "libsql")]
    #[tokio::test]
    async fn key_is_not_warned_about() -> Result<()> {
        let capture = capture();

        _ = accepts_invalid_person(
            StorageType::Lite,
            vec![config(topic_config::SCHEMA_VALIDATION, "false")],
        )
        .await?;

        let log = capture.text();
        assert!(!log.contains(DEPRECATION), "{log}");
        Ok(())
    }
}

mod describe_configs {
    use super::*;

    async fn names_are_returned_as_set(storage: ArcDynStorage) -> Result<()> {
        let topic = alphanumeric_string(15);
        create_topic(
            &storage,
            &topic,
            vec![
                config("tansu.lake.sink", "true"),
                config("nisshi.batch", "true"),
            ],
        )
        .await?;

        let names = config_names(&storage, &topic).await?;

        assert!(names.contains(&"tansu.lake.sink".to_owned()), "{names:?}");
        assert!(!names.contains(&"nisshi.lake.sink".to_owned()), "{names:?}");
        assert!(names.contains(&"nisshi.batch".to_owned()), "{names:?}");
        Ok(())
    }

    #[tokio::test]
    async fn memory() -> Result<()> {
        names_are_returned_as_set(memory_storage(Uuid::now_v7(), 111).await?).await
    }

    #[cfg(feature = "libsql")]
    #[tokio::test]
    async fn lite() -> Result<()> {
        names_are_returned_as_set(lite_storage(Uuid::now_v7(), 111).await?).await
    }

    #[cfg(feature = "slatedb")]
    #[tokio::test]
    async fn slatedb() -> Result<()> {
        names_are_returned_as_set(slate_storage(Uuid::now_v7(), 111).await?).await
    }

    #[cfg(feature = "postgres")]
    #[tokio::test]
    async fn pg() -> Result<()> {
        names_are_returned_as_set(postgres_storage(Uuid::now_v7(), 111).await?).await
    }
}
