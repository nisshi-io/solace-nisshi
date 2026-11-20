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

use crate::common::{alphanumeric_string, init_tracing};
use nisshi_broker::Result;
use nisshi_sans_io::{
    ConfigResource, ErrorCode,
    create_topics_request::{CreatableTopic, CreatableTopicConfig},
};
use nisshi_storage::Storage;
use rand::{prelude::*, rng};
use uuid::Uuid;

async fn topic_with_configs(storage: &impl Storage) -> Result<String> {
    let topic = alphanumeric_string(15);

    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(topic.clone())
                .num_partitions(1)
                .replication_factor(0)
                .assignments(Some([].into()))
                .configs(Some(
                    [
                        ("cleanup.policy", "compact"),
                        ("message.timestamp.type", "LogAppendTime"),
                        ("min.insync.replicas", "2"),
                    ]
                    .into_iter()
                    .map(|(name, value)| {
                        CreatableTopicConfig::default()
                            .name(name.into())
                            .value(Some(value.into()))
                    })
                    .collect(),
                )),
            false,
        )
        .await?;

    Ok(topic)
}

async fn describe(
    storage: &impl Storage,
    topic: &str,
    keys: Option<&[&str]>,
) -> Result<(ErrorCode, Vec<(String, Option<String>)>)> {
    let keys = keys.map(|keys| keys.iter().map(|key| key.to_string()).collect::<Vec<_>>());

    let described = storage
        .describe_config(topic, ConfigResource::Topic, keys.as_deref())
        .await?;

    let mut configs = described
        .configs
        .unwrap_or_default()
        .into_iter()
        .map(|config| (config.name, config.value))
        .collect::<Vec<_>>();
    configs.sort();

    Ok((ErrorCode::try_from(described.error_code)?, configs))
}

fn config(name: &str, value: &str) -> (String, Option<String>) {
    (name.into(), Some(value.into()))
}

fn all_configs() -> Vec<(String, Option<String>)> {
    vec![
        config("cleanup.policy", "compact"),
        config("message.timestamp.type", "LogAppendTime"),
        config("min.insync.replicas", "2"),
    ]
}

async fn one_stored_name(storage: impl Storage) -> Result<()> {
    let topic = topic_with_configs(&storage).await?;

    assert_eq!(
        (ErrorCode::None, vec![config("cleanup.policy", "compact")]),
        describe(&storage, &topic, Some(&["cleanup.policy"])).await?
    );

    Ok(())
}

async fn two_stored_names(storage: impl Storage) -> Result<()> {
    let topic = topic_with_configs(&storage).await?;

    assert_eq!(
        (
            ErrorCode::None,
            vec![
                config("cleanup.policy", "compact"),
                config("min.insync.replicas", "2")
            ]
        ),
        describe(
            &storage,
            &topic,
            Some(&["min.insync.replicas", "cleanup.policy"])
        )
        .await?
    );

    Ok(())
}

async fn stored_and_unset_name(storage: impl Storage) -> Result<()> {
    let topic = topic_with_configs(&storage).await?;

    assert_eq!(
        (
            ErrorCode::None,
            vec![config("message.timestamp.type", "LogAppendTime")]
        ),
        describe(
            &storage,
            &topic,
            Some(&["message.timestamp.type", "retention.ms"])
        )
        .await?
    );

    Ok(())
}

async fn only_unset_name(storage: impl Storage) -> Result<()> {
    let topic = topic_with_configs(&storage).await?;

    assert_eq!(
        (ErrorCode::None, vec![]),
        describe(&storage, &topic, Some(&["retention.ms"])).await?
    );

    Ok(())
}

async fn no_keys_or_empty_keys_return_every_config(storage: impl Storage) -> Result<()> {
    let topic = topic_with_configs(&storage).await?;

    assert_eq!(
        (ErrorCode::None, all_configs()),
        describe(&storage, &topic, None).await?
    );
    assert_eq!(
        (ErrorCode::None, all_configs()),
        describe(&storage, &topic, Some(&[])).await?
    );

    Ok(())
}

async fn duplicate_keys_return_one_entry(storage: impl Storage) -> Result<()> {
    let topic = topic_with_configs(&storage).await?;

    assert_eq!(
        (ErrorCode::None, vec![config("cleanup.policy", "compact")]),
        describe(
            &storage,
            &topic,
            Some(&["cleanup.policy", "cleanup.policy"])
        )
        .await?
    );

    Ok(())
}

async fn part_of_a_name_selects_nothing(storage: impl Storage) -> Result<()> {
    let topic = topic_with_configs(&storage).await?;

    assert_eq!(
        (ErrorCode::None, vec![]),
        describe(
            &storage,
            &topic,
            Some(&["cleanup", "policy", "cleanup.policy.x"])
        )
        .await?
    );

    Ok(())
}

async fn name_in_another_case_selects_nothing(storage: impl Storage) -> Result<()> {
    let topic = topic_with_configs(&storage).await?;

    assert_eq!(
        (ErrorCode::None, vec![]),
        describe(
            &storage,
            &topic,
            Some(&["Cleanup.Policy", "CLEANUP.POLICY"])
        )
        .await?
    );

    Ok(())
}

async fn unknown_topic_with_keys(storage: impl Storage) -> Result<()> {
    assert_eq!(
        (ErrorCode::UnknownTopicOrPartition, vec![]),
        describe(
            &storage,
            &alphanumeric_string(15),
            Some(&["cleanup.policy"])
        )
        .await?
    );

    Ok(())
}

macro_rules! engine_tests {
    ($storage:ident) => {
        use super::*;

        engine_tests!(
            $storage,
            one_stored_name,
            two_stored_names,
            stored_and_unset_name,
            only_unset_name,
            no_keys_or_empty_keys_return_every_config,
            duplicate_keys_return_one_entry,
            part_of_a_name_selects_nothing,
            name_in_another_case_selects_nothing,
            unknown_topic_with_keys,
        );
    };
    ($storage:ident, $($case:ident,)+) => {
        $(
            #[tokio::test]
            async fn $case() -> Result<()> {
                let _guard = init_tracing()?;

                let cluster_id = Uuid::now_v7();
                let broker_id = rng().random_range(0..i32::MAX);

                super::$case($storage(cluster_id, broker_id).await?).await
            }
        )+
    };
}

#[cfg(feature = "dynostore")]
mod memory {
    use crate::common::memory_storage;

    engine_tests!(memory_storage);
}

#[cfg(feature = "libsql")]
mod lite {
    use crate::common::lite_storage;

    engine_tests!(lite_storage);
}

#[cfg(feature = "turso")]
mod turso {
    use crate::common::{StorageType, storage_container};
    use nisshi_storage::ArcDynStorage;
    use url::Url;

    async fn turso_storage(cluster: impl Into<String> + Clone, node: i32) -> Result<ArcDynStorage> {
        storage_container(
            StorageType::Turso,
            cluster,
            node,
            Url::parse("tcp://127.0.0.1/")?,
            None,
        )
        .await
    }

    engine_tests!(turso_storage);
}

#[cfg(feature = "postgres")]
mod pg {
    use crate::common::postgres_storage;

    engine_tests!(postgres_storage);
}
