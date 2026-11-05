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

//! `update_group` is a conditional write on the version of the group:
//! `version: None` means "create only", and a version must match the stored
//! one. A rejected write reports the group as outdated, carrying the stored
//! group and its version, and leaves the stored group as it was.
//!
//! The group coordinator relies on this. It caches each group's state in
//! memory, and a request that arrives while another request for the same
//! group is in flight finds no cached entry, so it starts from an empty
//! group with no version. The storage must reject that write as outdated
//! and hand back the stored group. SlateDB used to skip the check when no
//! version was given and overwrote the group, dropping every member.

use crate::common::{
    alphanumeric_string, init_tracing, lite_storage, memory_storage, postgres_storage,
    slate_storage,
};
use nisshi_broker::{Error, Result};
use nisshi_storage::{
    ArcDynStorage, GroupDetail, GroupDetailResponse, NamedGroupDetail, Storage, UpdateError,
    Version,
};
use rand::{prelude::*, rng};
use uuid::Uuid;

fn generation(generation_id: i32) -> GroupDetail {
    GroupDetail {
        generation_id,
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

/// The generation of the group as stored, read independently of
/// `update_group`.
async fn stored_generation(storage: &impl Storage, group_id: &str) -> Result<i32, Error> {
    let described = storage
        .describe_groups(Some(&[group_id.to_owned()]), false)
        .await
        .map_err(|err| Error::Message(format!("describe: {err:?}")))?;

    match &described[..] {
        [
            NamedGroupDetail {
                response: GroupDetailResponse::Found(detail),
                ..
            },
        ] => Ok(detail.generation_id),

        otherwise => Err(Error::Message(format!(
            "expecting one described group: {otherwise:?}"
        ))),
    }
}

async fn create_only_without_version(storage: impl Storage + Clone) -> Result<(), Error> {
    let group_id = &alphanumeric_string(15)[..];

    let version = update(&storage, group_id, generation(1), None).await?;

    match storage.update_group(group_id, generation(2), None).await {
        Err(UpdateError::Outdated {
            current,
            version: current_version,
        }) => {
            assert_eq!(1, current.generation_id);
            assert_eq!(version, current_version);
        }

        otherwise => panic!("expected outdated, got: {otherwise:?}"),
    }

    assert_eq!(1, stored_generation(&storage, group_id).await?);

    Ok(())
}

async fn stale_version(storage: impl Storage + Clone) -> Result<(), Error> {
    let group_id = &alphanumeric_string(15)[..];

    let first = update(&storage, group_id, generation(1), None).await?;
    let second = update(&storage, group_id, generation(2), Some(first.clone())).await?;

    match storage
        .update_group(group_id, generation(3), Some(first))
        .await
    {
        Err(UpdateError::Outdated { current, version }) => {
            assert_eq!(2, current.generation_id);
            assert_eq!(second, version);
        }

        otherwise => panic!("expected outdated, got: {otherwise:?}"),
    }

    assert_eq!(2, stored_generation(&storage, group_id).await?);

    Ok(())
}

/// Two updates from the same version race: exactly one wins, and the other
/// is told the group is outdated, with the winner's group and version. On
/// SlateDB the loser can pass the version check and then fail to commit with
/// a transaction conflict, which must also be reported as outdated.
async fn concurrent_same_version(storage: impl Storage + Clone) -> Result<(), Error> {
    let group_id = &alphanumeric_string(15)[..];

    let version = update(&storage, group_id, generation(1), None).await?;

    let (second, third) = tokio::join!(
        storage.update_group(group_id, generation(2), Some(version.clone())),
        storage.update_group(group_id, generation(3), Some(version)),
    );

    let (winner, won, lost) = match (second, third) {
        (Ok(won), Err(lost)) => (2, won, lost),
        (Err(lost), Ok(won)) => (3, won, lost),
        otherwise => panic!("expected exactly one update to win, got: {otherwise:?}"),
    };

    match lost {
        UpdateError::Outdated { current, version } => {
            assert_eq!(winner, current.generation_id);
            assert_eq!(won, version);
        }

        otherwise => panic!("expected outdated, got: {otherwise:?}"),
    }

    assert_eq!(winner, stored_generation(&storage, group_id).await?);

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
    async fn create_only_without_version() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::create_only_without_version(storage).await
    }

    #[tokio::test]
    async fn stale_version() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::stale_version(storage).await
    }

    #[tokio::test]
    async fn concurrent_same_version() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::concurrent_same_version(storage).await
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
    async fn create_only_without_version() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::create_only_without_version(storage).await
    }

    #[tokio::test]
    async fn stale_version() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::stale_version(storage).await
    }

    #[tokio::test]
    async fn concurrent_same_version() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::concurrent_same_version(storage).await
    }
}

#[cfg(feature = "turso")]
mod turso {
    use super::*;

    async fn storage_container(
        cluster: impl Into<String> + Clone,
        node: i32,
    ) -> Result<ArcDynStorage> {
        crate::common::turso_storage(cluster, node).await
    }

    #[tokio::test]
    async fn create_only_without_version() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::create_only_without_version(storage).await
    }

    #[tokio::test]
    async fn stale_version() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::stale_version(storage).await
    }

    #[tokio::test]
    async fn concurrent_same_version() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::concurrent_same_version(storage).await
    }
}

#[cfg(feature = "slatedb")]
mod slatedb {
    use super::*;

    async fn storage_container(
        cluster: impl Into<String> + Clone,
        node: i32,
    ) -> Result<ArcDynStorage> {
        slate_storage(cluster, node).await
    }

    #[tokio::test]
    async fn create_only_without_version() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::create_only_without_version(storage).await
    }

    #[tokio::test]
    async fn stale_version() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::stale_version(storage).await
    }

    #[tokio::test]
    async fn concurrent_same_version() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::concurrent_same_version(storage).await
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
    async fn create_only_without_version() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::create_only_without_version(storage).await
    }

    #[tokio::test]
    async fn stale_version() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::stale_version(storage).await
    }

    #[tokio::test]
    async fn concurrent_same_version() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::concurrent_same_version(storage).await
    }
}
