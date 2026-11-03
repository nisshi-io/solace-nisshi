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

//! `group_with_version` reads a group's stored detail and the version that a
//! later `update_group` must pass, without writing. Every backend must return
//! the version that its conditional write compares against, and the read must
//! leave that version as it was.

use std::assert_matches;

use crate::common::{self, StorageType, alphanumeric_string, init_tracing};
use nisshi_broker::{Error, Result};
use nisshi_sans_io::{ErrorCode, create_topics_request::CreatableTopic};
use nisshi_storage::{GroupDetail, OffsetCommitRequest, Storage, Topition, UpdateError, Version};
use rand::{prelude::*, rng};
use url::Url;
use uuid::Uuid;

fn generation(generation_id: i32) -> GroupDetail {
    GroupDetail {
        generation_id,
        session_timeout_ms: 30_000,
        ..Default::default()
    }
}

async fn update(
    storage: &impl Storage,
    group_id: &str,
    detail: GroupDetail,
    version: Option<Version>,
) -> Result<Version, Error> {
    storage
        .update_group(group_id, detail, version)
        .await
        .map_err(|err| Error::Message(format!("update: {err:?}")))
}

async fn read(
    storage: &impl Storage,
    group_id: &str,
) -> Result<Option<(GroupDetail, Version)>, Error> {
    storage
        .group_with_version(group_id)
        .await
        .map_err(|err| Error::Message(format!("group_with_version: {err:?}")))
}

async fn read_stored(storage: &impl Storage, group_id: &str) -> Result<(GroupDetail, Version)> {
    read(storage, group_id)
        .await?
        .ok_or(Error::Message(format!("no stored state for {group_id}")))
}

async fn absent_group_reads_as_none(storage: impl Storage) -> Result<()> {
    let group_id = &alphanumeric_string(15)[..];

    assert_eq!(None, read(&storage, group_id).await?);

    Ok(())
}

async fn read_returns_detail_and_accepted_version(storage: impl Storage) -> Result<()> {
    let group_id = &alphanumeric_string(15)[..];

    let first_detail = generation(1);
    let first = update(&storage, group_id, first_detail.clone(), None).await?;

    let (detail, version) = read_stored(&storage, group_id).await?;
    assert_eq!(first_detail, detail);
    assert_eq!(first, version);

    let second_detail = generation(2);
    let second = update(&storage, group_id, second_detail.clone(), Some(version)).await?;

    let (detail, version) = read_stored(&storage, group_id).await?;
    assert_eq!(second_detail, detail);
    assert_eq!(second, version);

    Ok(())
}

async fn stale_version_is_outdated_after_read(storage: impl Storage) -> Result<()> {
    let group_id = &alphanumeric_string(15)[..];

    let first = update(&storage, group_id, generation(1), None).await?;
    let second_detail = generation(2);
    let second = update(
        &storage,
        group_id,
        second_detail.clone(),
        Some(first.clone()),
    )
    .await?;

    let (detail, version) = read_stored(&storage, group_id).await?;
    assert_eq!(second_detail, detail);
    assert_eq!(second, version);

    let outdated = storage
        .update_group(group_id, generation(3), Some(first))
        .await;
    assert_matches!(
        outdated,
        Err(UpdateError::Outdated { ref current, version: ref current_version })
            if current.generation_id == 2 && *current_version == second
    );

    let (detail, _) = read_stored(&storage, group_id).await?;
    assert_eq!(second_detail, detail);

    Ok(())
}

async fn read_leaves_version_unchanged(storage: impl Storage) -> Result<()> {
    let group_id = &alphanumeric_string(15)[..];

    _ = update(&storage, group_id, generation(1), None).await?;

    let (_, first_read) = read_stored(&storage, group_id).await?;
    let (_, second_read) = read_stored(&storage, group_id).await?;
    assert_eq!(first_read, second_read);

    let second_detail = generation(2);
    _ = update(&storage, group_id, second_detail.clone(), Some(first_read)).await?;

    let (detail, _) = read_stored(&storage, group_id).await?;
    assert_eq!(second_detail, detail);

    Ok(())
}

/// A group that only has committed offsets has no stored state, on every backend, and the first
/// `update_group` of its state passes no version.
async fn group_with_only_committed_offsets_reads_as_none(storage: impl Storage) -> Result<()> {
    let topic = alphanumeric_string(15);
    let group_id = &alphanumeric_string(15)[..];

    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(topic.clone())
                .num_partitions(1)
                .replication_factor(0)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    let committed = storage
        .offset_commit(
            group_id,
            None,
            &[(
                Topition::new(topic, 0),
                OffsetCommitRequest::default().offset(5),
            )],
        )
        .await?;
    assert_eq!(ErrorCode::None, committed[0].1);

    assert_eq!(None, read(&storage, group_id).await?);

    let detail = generation(1);
    let version = update(&storage, group_id, detail.clone(), None).await?;
    assert_eq!((detail, version), read_stored(&storage, group_id).await?);

    Ok(())
}

macro_rules! backend {
    ($(#[$test_attribute:meta])* $name:ident, $feature:literal, $storage_type:expr) => {
        #[cfg(feature = $feature)]
        mod $name {
            use nisshi_storage::ArcDynStorage;

            use super::*;

            async fn storage_container() -> Result<ArcDynStorage> {
                common::storage_container(
                    $storage_type,
                    Uuid::now_v7(),
                    rng().random_range(0..i32::MAX),
                    Url::parse("tcp://127.0.0.1/")?,
                    None,
                )
                .await
            }

            $(#[$test_attribute])*
            #[tokio::test]
            async fn absent_group_reads_as_none() -> Result<()> {
                let _guard = init_tracing()?;
                super::absent_group_reads_as_none(storage_container().await?).await
            }

            $(#[$test_attribute])*
            #[tokio::test]
            async fn read_returns_detail_and_accepted_version() -> Result<()> {
                let _guard = init_tracing()?;
                super::read_returns_detail_and_accepted_version(storage_container().await?).await
            }

            $(#[$test_attribute])*
            #[tokio::test]
            async fn stale_version_is_outdated_after_read() -> Result<()> {
                let _guard = init_tracing()?;
                super::stale_version_is_outdated_after_read(storage_container().await?).await
            }

            $(#[$test_attribute])*
            #[tokio::test]
            async fn read_leaves_version_unchanged() -> Result<()> {
                let _guard = init_tracing()?;
                super::read_leaves_version_unchanged(storage_container().await?).await
            }

            $(#[$test_attribute])*
            #[tokio::test]
            async fn group_with_only_committed_offsets_reads_as_none() -> Result<()> {
                let _guard = init_tracing()?;
                super::group_with_only_committed_offsets_reads_as_none(storage_container().await?)
                    .await
            }
        }
    };
}

backend!(in_memory, "dynostore", StorageType::InMemory);
backend!(lite, "libsql", StorageType::Lite);
backend!(pg, "postgres", StorageType::Postgres);
backend!(slatedb, "slatedb", StorageType::SlateDb);
backend!(turso, "turso", StorageType::Turso);
