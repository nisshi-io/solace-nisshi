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

use std::{
    fmt::{self, Display, Formatter},
    sync::Arc,
};

use async_trait::async_trait;
use nisshi_storage::{
    ArcDynStorage, Error, Result, StorageCapabilities, StorageFactory, StorageFactoryConfiguration,
    reject_unrecognized_options,
};
use regex::Regex;
use slatedb::{
    Db, Settings,
    object_store::{
        ObjectStore,
        aws::{AmazonS3Builder, S3ConditionalPut},
        memory::InMemory,
    },
};

use crate::Engine;

/// slatedb 0.14 runs a background garbage collector by default (0.10 had
/// it off). Keep it off until enabling it is a deliberate, separately
/// tested change.
fn db_settings() -> Settings {
    Settings {
        garbage_collector_options: None,
        ..Settings::default()
    }
}

#[derive(Clone, Copy, Debug)]
pub struct EngineFactory;

impl Display for EngineFactory {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("SlateDB (slatedb://)")
    }
}

#[async_trait]
impl StorageFactory for EngineFactory {
    fn scheme(&self) -> Result<Regex> {
        Regex::new(r"slatedb").map_err(Into::into)
    }

    fn capabilities(&self) -> StorageCapabilities {
        StorageCapabilities {
            user_credentials: true,
        }
    }

    fn deprecation(&self) -> Option<&'static str> {
        Some(
            "the slatedb:// storage engine is deprecated and may be removed in a future release; \
             it is not recommended for new deployments; \
             use postgres:// for production, or sqlite:// or memory:// for development",
        )
    }

    async fn build(&self, configuration: StorageFactoryConfiguration) -> Result<ArcDynStorage> {
        reject_unrecognized_options(&configuration.storage, &[])?;

        let host = configuration.storage.host_str().unwrap_or("nisshi");
        let db_path = format!("nisshi-{}.slatedb", configuration.cluster);

        // Support memory backend for testing: slatedb://memory
        let object_store: Arc<dyn ObjectStore> = if host == "memory" {
            Arc::new(InMemory::new())
        } else {
            // Use S3 backend with host as bucket name
            AmazonS3Builder::from_env()
                .with_bucket_name(host)
                .with_conditional_put(S3ConditionalPut::ETagMatch)
                .build()
                .map(Arc::new)
                .map_err(|e| Error::Message(e.to_string()))?
        };

        Db::builder(db_path, object_store)
            .with_settings(db_settings())
            .build()
            .await
            .map(Arc::new)
            .map_err(Into::into)
            .and_then(|db| {
                Engine::builder()
                    .cluster(configuration.cluster)
                    .node(configuration.node_id)
                    .advertised_listener(configuration.advertised_listener)
                    .db(db)
                    .schemas(configuration.schema_registry)
                    .lake(configuration.lake_house)
                    .build()
            })
            .map(Box::new)
            .map(|storage| Arc::new(storage) as ArcDynStorage)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn garbage_collector_is_off() {
        assert!(Settings::default().garbage_collector_options.is_some());
        assert!(db_settings().garbage_collector_options.is_none());
    }
}
