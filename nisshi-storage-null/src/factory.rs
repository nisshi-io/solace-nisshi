// Copyright ⓒ Solace Corporation
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

use crate::Engine;
use async_trait::async_trait;
use nisshi_storage::{
    ArcDynStorage, Result, StorageCapabilities, StorageFactory, StorageFactoryConfiguration,
    reject_unrecognized_options,
};
use regex::Regex;
use std::{
    fmt::{self, Display, Formatter},
    sync::Arc,
};

#[derive(Clone, Copy, Debug)]
pub struct EngineFactory;

impl Display for EngineFactory {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(crate::NAME)
    }
}

#[async_trait]
impl StorageFactory for EngineFactory {
    fn scheme(&self) -> Result<Regex> {
        Regex::new(r"null").map_err(Into::into)
    }

    /// The null engine stores nothing, so it has no SCRAM credentials.
    fn capabilities(&self) -> StorageCapabilities {
        StorageCapabilities::default()
    }

    async fn build(&self, configuration: StorageFactoryConfiguration) -> Result<ArcDynStorage> {
        reject_unrecognized_options(&configuration.storage, &[])?;

        Ok(Arc::new(Box::new(Engine::new(
            configuration.cluster,
            configuration.node_id,
            configuration.advertised_listener,
        ))) as ArcDynStorage)
    }
}
