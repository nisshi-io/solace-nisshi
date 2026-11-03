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

//! A broker with `--authentication` logs clients in with the SCRAM users in its storage, so it
//! refuses to start on a storage engine that can't store them, instead of starting and then
//! rejecting every login.

use std::assert_matches;

use nisshi_broker::{
    Error, NODE_ID, Result, broker::Broker, coordinator::group::administrator::Controller,
};
use nisshi_storage::ArcDynStorage;
use url::Url;
use uuid::Uuid;

use crate::common::init_tracing;

async fn build(storage: Url, authentication: bool) -> Result<()> {
    let listener = Url::parse("tcp://127.0.0.1:0")?;

    Broker::<Controller<ArcDynStorage>, ArcDynStorage>::builder()
        .node_id(NODE_ID)
        .cluster_id(format!("authentication-storage-{}", Uuid::now_v7()))
        .incarnation_id(Uuid::now_v7())
        .advertised_listener(listener.clone())
        .storage(storage)
        .listener(listener)
        .authentication(authentication)
        .silent(true)
        .build()
        .await
        .map(|_| ())
}

/// Asserts that a broker requiring authentication on `storage` is refused before it starts,
/// naming `engine`, and offering the engines of this build that can store SCRAM users.
async fn assert_refused(storage: &str, engine: &str) -> Result<()> {
    let _guard = init_tracing()?;

    let refused = build(Url::parse(storage)?, true).await;

    assert_matches!(
        &refused,
        Err(Error::Storage(nisshi_storage::Error::AuthenticationUnsupported {
            engine: refused_engine,
            supported,
        })) if *refused_engine == engine
            && (!cfg!(feature = "libsql")
                || supported.iter().any(|name| name == "SQLite (sqlite://)"))
            && (!cfg!(feature = "postgres")
                || supported.iter().any(|name| name == "PostgreSQL (postgres://)"))
            && !supported.iter().any(|name| name == engine),
        "{refused:?}"
    );

    Ok(())
}

#[cfg(feature = "dynostore")]
mod dynostore {
    use super::*;

    #[tokio::test]
    async fn memory_is_refused() -> Result<()> {
        assert_refused("memory://nisshi/", "in-memory (memory://)").await
    }

    /// No S3 service runs here: the broker refuses before it connects to the bucket.
    #[tokio::test]
    async fn s3_is_refused_before_connecting() -> Result<()> {
        assert_refused("s3://nisshi/", "S3 (s3://)").await
    }

    /// No Google Cloud service or credentials exist here.
    #[tokio::test]
    async fn gcs_is_refused() -> Result<()> {
        assert_refused("gs://nisshi/", "Google Cloud Storage (gs://)").await
    }

    /// Without `--authentication`, memory storage starts as before.
    #[tokio::test]
    async fn memory_without_authentication_starts() -> Result<()> {
        let _guard = init_tracing()?;
        build(Url::parse("memory://nisshi/")?, false).await
    }
}

/// Turso's SCRAM methods refuse the change, so it is refused at startup like the other engines
/// that keep no credentials. The broker refuses before it opens the database.
#[cfg(feature = "turso")]
#[tokio::test]
async fn turso_is_refused() -> Result<()> {
    assert_refused("turso://nisshi/", "Turso (turso://)").await
}

#[tokio::test]
async fn null_is_refused() -> Result<()> {
    assert_refused("null://", "null (null://)").await
}

#[cfg(feature = "libsql")]
#[tokio::test]
async fn sqlite_starts() -> Result<()> {
    let _guard = init_tracing()?;
    build(crate::common::lite_url().await?, true).await
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_starts() -> Result<()> {
    let _guard = init_tracing()?;
    build(Url::parse(&crate::common::postgres_url())?, true).await
}
