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

//! Regression test for `describe_groups` on the storage backends that
//! persist `GroupDetail` as JSON text.
//!
//! Both the libsql (`lite.rs`) and turso (`limbo.rs`) backends used to
//! call `serde_json::Value::from(&str)` on the JSON column, which wraps
//! the raw JSON text as a `Value::String` instead of parsing it. The
//! subsequent `serde_json::from_value::<GroupDetail>` then always
//! failed with `invalid type: string "...", expected struct
//! GroupDetail`. The native-JSON Postgres backend was not affected.

use crate::common::{
    alphanumeric_string, init_tracing, lite_storage, memory_storage, postgres_storage,
    slate_storage,
};
use nisshi_broker::{Error, Result};
use nisshi_storage::{ArcDynStorage, GroupDetail, Storage};
use rand::{prelude::*, rng};
use tracing::debug;
use uuid::Uuid;

async fn round_trip(storage: impl Storage + Clone) -> Result<(), Error> {
    let group_id = &alphanumeric_string(15)[..];

    let detail = GroupDetail::default();

    let _version = storage
        .update_group(group_id, detail.clone(), None)
        .await
        .inspect(|version| debug!(?version))
        .map_err(|err| Error::Message(format!("update_group: {err:?}")))?;

    let described = storage
        .describe_groups(Some(&[group_id.into()]), false)
        .await?;

    assert_eq!(1, described.len(), "describe_groups must return one entry");

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
    async fn round_trip() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::round_trip(storage).await?;

        Ok(())
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
    async fn round_trip() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::round_trip(storage).await?;

        Ok(())
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
    async fn round_trip() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::round_trip(storage).await?;

        Ok(())
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
    async fn round_trip() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::round_trip(storage).await?;

        Ok(())
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
    async fn round_trip() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::round_trip(storage).await?;

        Ok(())
    }
}
