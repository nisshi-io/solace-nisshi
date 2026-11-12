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

//! An engine's `StorageCapabilities` must match what its storage does. The startup check that
//! refuses `--authentication` trusts the flag, so a flag that says `true` for an engine that drops
//! credentials brings the lockout back, and one that says `false` for an engine that stores them
//! refuses a broker that would have worked.

use std::{assert_matches, sync::Arc};

use bytes::Bytes;
use nisshi_broker::{NODE_ID, Result};
use nisshi_sans_io::ScramMechanism;
use nisshi_storage::{
    ArcDynStorageFactory, Error, ScramCredential, Storage, StorageFactoryConfiguration,
};
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

use crate::common::{init_tracing, register_broker};

const USER: &str = "alice";

fn credential() -> ScramCredential {
    ScramCredential {
        salt: Bytes::from_static(b"salt"),
        iterations: 4096,
        stored_key: Bytes::from_static(b"stored"),
        server_key: Bytes::from_static(b"server"),
    }
}

/// Builds `factory` on `url` and checks that its storage keeps SCRAM credentials exactly when
/// `capabilities().user_credentials` says so.
async fn assert_flag_matches_storage(factory: ArcDynStorageFactory, url: Url) -> Result<()> {
    let expected = factory.capabilities().user_credentials;

    let cluster = format!("storage-capabilities-{}", Uuid::now_v7());

    // The factory directly, not the builder: the builder also runs the startup check, which a
    // Turso database doesn't pass yet, see #866.
    let storage = factory
        .build(StorageFactoryConfiguration {
            node_id: NODE_ID,
            cluster: cluster.clone(),
            advertised_listener: Url::parse("tcp://127.0.0.1/")?,
            storage: url,
            schema_registry: None,
            lake_house: None,
            cancellation: CancellationToken::new(),
        })
        .await?;

    // As the broker does at startup, before a client can alter a credential. An engine that keeps
    // no credentials needs no registered broker to refuse one.
    if expected {
        register_broker(cluster, NODE_ID, &storage).await?;
    }

    let upserted = storage
        .upsert_user_scram_credential(USER, ScramMechanism::Scram256, credential())
        .await;

    // An engine that keeps no credentials may answer a lookup with an error or with nothing.
    let stored = matches!(
        storage
            .user_scram_credential(USER, ScramMechanism::Scram256)
            .await,
        Ok(Some(_))
    );

    if expected {
        assert_matches!(upserted, Ok(()), "flag says it stores credentials");
        assert!(stored, "flag says it stores credentials, and none was kept");
    } else {
        assert_matches!(
            upserted,
            Err(Error::UserCredentialsUnsupported { .. }),
            "flag says it stores nothing"
        );
        assert!(
            !stored,
            "flag says it stores nothing, and a credential was kept"
        );

        assert_matches!(
            storage
                .delete_user_scram_credential(USER, ScramMechanism::Scram256)
                .await,
            Err(Error::UserCredentialsUnsupported { .. })
        );
    }

    Ok(())
}

#[cfg(feature = "dynostore")]
#[tokio::test]
async fn memory() -> Result<()> {
    let _guard = init_tracing()?;

    assert_flag_matches_storage(
        Arc::new(nisshi_storage_dynostore::MemoryEngineFactory),
        Url::parse("memory://")?,
    )
    .await
}

#[tokio::test]
async fn null() -> Result<()> {
    let _guard = init_tracing()?;

    assert_flag_matches_storage(
        Arc::new(nisshi_storage_null::EngineFactory),
        Url::parse("null://")?,
    )
    .await
}

#[cfg(feature = "libsql")]
#[tokio::test]
async fn sqlite() -> Result<()> {
    let _guard = init_tracing()?;

    assert_flag_matches_storage(
        Arc::new(nisshi_storage_sql::LiteEngineFactory),
        crate::common::lite_url().await?,
    )
    .await
}

#[cfg(feature = "turso")]
#[tokio::test]
async fn turso() -> Result<()> {
    let _guard = init_tracing()?;

    let url = Url::parse(&format!(
        "turso://../logs/{}/{}.db",
        env!("CARGO_PKG_NAME"),
        std::thread::current().name().unwrap_or("turso")
    ))?;

    assert_flag_matches_storage(Arc::new(nisshi_storage_sql::LimboEngineFactory), url).await
}
