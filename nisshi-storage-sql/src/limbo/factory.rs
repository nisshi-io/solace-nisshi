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

use std::{
    fmt::{self, Display, Formatter},
    sync::Arc,
};

use async_trait::async_trait;
use nisshi_storage::{
    ArcDynStorage, Result, SemaphoreProxy, StorageCapabilities, StorageFactory,
    StorageFactoryConfiguration, reject_unrecognized_options,
};
use regex::Regex;

use super::Engine;

/// The engine's name, as the broker reports it to an operator.
pub(super) const NAME: &str = "Turso (turso://)";

#[derive(Clone, Copy, Debug)]
pub struct LimboFactory;

impl Display for LimboFactory {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(NAME)
    }
}

#[async_trait]
impl StorageFactory for LimboFactory {
    fn scheme(&self) -> Result<Regex> {
        Regex::new(r"turso").map_err(Into::into)
    }

    /// No SCRAM credentials yet: the Turso engine refuses to store them, with
    /// [`nisshi_storage::Error::UserCredentialsUnsupported`].
    fn capabilities(&self) -> StorageCapabilities {
        StorageCapabilities::default()
    }

    async fn build(&self, configuration: StorageFactoryConfiguration) -> Result<ArcDynStorage> {
        reject_unrecognized_options(&configuration.storage, &[])?;

        Engine::builder()
            .storage(configuration.storage)
            .node(configuration.node_id)
            .cluster(configuration.cluster)
            .advertised_listener(configuration.advertised_listener)
            .lake(configuration.lake_house)
            .build()
            .await
            // A Turso connection has no busy handler, and one that finds the
            // write lock held fails at once with `Busy`. The proxy lets one
            // call at a time reach the engine, as it does for libSQL.
            .map(SemaphoreProxy::new)
            .map(Box::new)
            .map(|storage| Arc::new(storage) as ArcDynStorage)
    }
}
