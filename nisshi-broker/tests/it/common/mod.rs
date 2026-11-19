// Copyright (c) Solace Corporation
//
// Contributors outside Solace:
//   Enwei Jiao
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

#![allow(dead_code)]

pub(crate) mod wire;

pub(crate) use nisshi_storage_integration_test::{broker::*, group::*, random::*};

use glob::glob;
use nisshi_broker::{
    Error, Result,
    coordinator::group::{Coordinator, administrator::Controller},
};
use nisshi_sans_io::{OffsetFetchResponse, offset_fetch_request::OffsetFetchRequestTopic};
use nisshi_schema::Registry;
use nisshi_storage::{ArcDynStorage, Storage, StorageContainer};
use std::{env, future::Future, io::ErrorKind, sync::Arc, thread};
use tokio::fs::remove_file;
use tracing::{debug, subscriber::DefaultGuard};
use tracing_subscriber::EnvFilter;
use url::Url;

pub(crate) fn init_tracing() -> Result<DefaultGuard> {
    use std::{fs::File, sync::Arc};

    Ok(tracing::subscriber::set_default(
        tracing_subscriber::fmt()
            .with_level(true)
            .with_line_number(true)
            .with_thread_names(false)
            .with_env_filter(
                EnvFilter::from_default_env()
                    .add_directive(format!("{}=debug", env!("CARGO_CRATE_NAME")).parse()?),
            )
            .with_writer(
                thread::current()
                    .name()
                    .ok_or(Error::Message(String::from("unnamed thread")))
                    .and_then(|name| {
                        File::create(format!("../logs/{}/{name}.log", env!("CARGO_PKG_NAME")))
                            .map_err(Into::into)
                    })
                    .map(Arc::new)?,
            )
            .finish(),
    ))
}

/// Returns the URL of the PostgreSQL database that [`StorageType::Postgres`]
/// stores to: `NISSHI_TEST_DATABASE_URL`, or else the `db` service of
/// compose.yaml on its default port.
pub(crate) fn postgres_url() -> String {
    env::var("NISSHI_TEST_DATABASE_URL")
        .unwrap_or_else(|_| String::from("postgres://postgres:postgres@localhost"))
}

pub(crate) enum StorageType {
    InMemory,
    Lite,
    Postgres,
    SlateDb,
    Turso,
}

pub(crate) async fn storage_container(
    storage_type: StorageType,
    cluster: impl Into<String> + Clone,
    node: i32,
    advertised_listener: Url,
    schemas: Option<Registry>,
) -> Result<ArcDynStorage> {
    let builder = {
        let mut builder = StorageContainer::builder();

        #[cfg(feature = "dynostore")]
        builder.with_factory(Arc::new(nisshi_storage_dynostore::MemoryEngineFactory));

        #[cfg(feature = "dynostore")]
        builder.with_factory(Arc::new(
            nisshi_storage_dynostore::S3OptimisticConcurrencyEngineFactory,
        ));

        #[cfg(feature = "dynostore")]
        builder.with_factory(Arc::new(
            nisshi_storage_dynostore::GoogleCloudStorageEngineFactory,
        ));

        #[cfg(feature = "libsql")]
        builder.with_factory(Arc::new(nisshi_storage_sql::LiteEngineFactory));

        #[cfg(feature = "postgres")]
        builder.with_factory(Arc::new(nisshi_storage_sql::PostgresEngineFactory));

        #[cfg(feature = "slatedb")]
        builder.with_factory(Arc::new(nisshi_storage_slatedb::EngineFactory));

        #[cfg(feature = "turso")]
        builder.with_factory(Arc::new(nisshi_storage_sql::LimboEngineFactory));

        builder
    };

    let storage = match storage_type {
        StorageType::Postgres => {
            builder
                .cluster_id(cluster.clone())
                .node_id(node)
                .advertised_listener(advertised_listener)
                .schema_registry(schemas)
                .storage(Url::parse(&postgres_url())?)
                .build()
                .await
        }

        StorageType::InMemory => {
            builder
                .cluster_id(cluster.clone())
                .node_id(node)
                .advertised_listener(advertised_listener)
                .schema_registry(schemas)
                .storage(Url::parse("memory://")?)
                .build()
                .await
        }

        StorageType::Lite => {
            builder
                .cluster_id(cluster.clone())
                .node_id(node)
                .advertised_listener(advertised_listener)
                .schema_registry(schemas)
                .storage(lite_url().await?)
                .build()
                .await
        }

        StorageType::Turso => {
            let relative = thread::current()
                .name()
                .ok_or(Error::Message(String::from("unnamed thread")))
                .map(|name| format!("../logs/{}/{name}.db*", env!("CARGO_PKG_NAME")))?;

            let mut path = env::current_dir()?;
            path.push(relative);
            debug!(?path);

            if let Some(pattern) = path.to_str() {
                debug!(pattern);

                let paths = glob(pattern)?;

                for path in paths.flatten() {
                    debug!(?path);

                    remove_file(path).await?;
                }
            }

            builder
                .cluster_id(cluster.clone())
                .node_id(node)
                .advertised_listener(advertised_listener)
                .schema_registry(schemas)
                .storage(
                    thread::current()
                        .name()
                        .ok_or(Error::Message(String::from("unnamed thread")))
                        .map(|name| format!("turso://../logs/{}/{name}.db", env!("CARGO_PKG_NAME")))
                        .inspect(|url| debug!(url))
                        .and_then(|url| Url::parse(&url).map_err(Into::into))?,
                )
                .build()
                .await
        }

        // Uses slatedb://memory for in-memory testing, no external S3 needed
        StorageType::SlateDb => {
            builder
                .cluster_id(cluster.clone())
                .node_id(node)
                .advertised_listener(advertised_listener)
                .schema_registry(schemas)
                .storage(Url::parse("slatedb://memory")?)
                .build()
                .await
        }
    }?;

    register_broker(cluster, node, &storage).await?;

    Ok(storage)
}

/// A `sqlite://` URL for this test thread's own database file, which is removed first, so
/// that each test starts with an empty database.
pub(crate) async fn lite_url() -> Result<Url> {
    let name = thread::current()
        .name()
        .map(String::from)
        .ok_or(Error::Message(String::from("unnamed thread")))?;

    let mut path = env::current_dir()?;
    path.push(format!("../logs/{}/{name}.db", env!("CARGO_PKG_NAME")));
    debug!(?path);

    match remove_file(path).await {
        Ok(_) => Ok(()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        otherwise @ Err(_) => otherwise,
    }?;

    let url = format!("sqlite://../logs/{}/{name}.db", env!("CARGO_PKG_NAME"));
    debug!(url);
    Url::parse(&url).map_err(Into::into)
}

pub(crate) async fn memory_storage(
    cluster: impl Into<String> + Clone,
    node: i32,
) -> Result<ArcDynStorage> {
    storage_container(
        StorageType::InMemory,
        cluster,
        node,
        Url::parse("tcp://127.0.0.1/")?,
        None,
    )
    .await
}

pub(crate) async fn lite_storage(
    cluster: impl Into<String> + Clone,
    node: i32,
) -> Result<ArcDynStorage> {
    storage_container(
        StorageType::Lite,
        cluster,
        node,
        Url::parse("tcp://127.0.0.1/")?,
        None,
    )
    .await
}

pub(crate) async fn slate_storage(
    cluster: impl Into<String> + Clone,
    node: i32,
) -> Result<ArcDynStorage> {
    storage_container(
        StorageType::SlateDb,
        cluster,
        node,
        Url::parse("tcp://127.0.0.1/")?,
        None,
    )
    .await
}

pub(crate) async fn postgres_storage(
    cluster: impl Into<String> + Clone,
    node: i32,
) -> Result<ArcDynStorage> {
    storage_container(
        StorageType::Postgres,
        cluster,
        node,
        Url::parse("tcp://127.0.0.1/")?,
        None,
    )
    .await
}

/// Runs `future` to its end on a single-threaded Tokio runtime of its own, for code that needs a
/// Tokio runtime, such as a storage read with a deadline, called from outside one.
pub(crate) fn block_on_tokio<F: Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("a Tokio runtime")
        .block_on(future)
}

pub(crate) async fn offset_fetch<G>(
    controller: &mut Controller<G>,
    group_id: &str,
    topics: &[OffsetFetchRequestTopic],
) -> Result<OffsetFetchResponse>
where
    G: Storage + Clone,
{
    controller
        .offset_fetch(Some(group_id), Some(topics), None, Some(false))
        .await
        .and_then(|body| TryInto::try_into(body).map_err(Into::into))
}
