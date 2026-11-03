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

use std::fmt::{self, Display, Formatter};

use async_trait::async_trait;
use nisshi_storage::{
    ArcDynStorage, Result, StorageCapabilities, StorageFactory, StorageFactoryConfiguration,
    reject_unrecognized_options,
};
use regex::Regex;

use super::{BUSY_TIMEOUT, COMPACTION, Engine, MODE};

#[derive(Clone, Copy, Debug)]
pub struct LiteFactory;

impl Display for LiteFactory {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("SQLite (sqlite://)")
    }
}

#[async_trait]
impl StorageFactory for LiteFactory {
    fn scheme(&self) -> Result<Regex> {
        Regex::new(r"sqlite").map_err(Into::into)
    }

    fn capabilities(&self) -> StorageCapabilities {
        StorageCapabilities {
            user_credentials: true,
        }
    }

    async fn build(&self, configuration: StorageFactoryConfiguration) -> Result<ArcDynStorage> {
        reject_unrecognized_options(
            &configuration.storage,
            &["vacuum_into", BUSY_TIMEOUT, COMPACTION, MODE],
        )?;

        Engine::builder()
            .storage(configuration.storage)
            .node(configuration.node_id)
            .cluster(configuration.cluster)
            .advertised_listener(configuration.advertised_listener)
            .schemas(configuration.schema_registry)
            .lake(configuration.lake_house)
            .cancellation(configuration.cancellation)
            .build()
            .await
    }
}
