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
    num::NonZeroU32,
    str::FromStr as _,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use nisshi_storage::{
    ArcDynStorage, ProduceRequestBatcher, Result, StorageCapabilities, StorageFactory,
    StorageFactoryConfiguration, parse_storage_option, reject_unrecognized_options,
};
use object_store::{
    aws::{AmazonS3Builder, AmazonS3ConfigKey, S3ConditionalPut},
    gcp::GoogleCloudStorageBuilder,
    memory::InMemory,
};
use regex::Regex;
use tracing::debug;
use url::Url;

use crate::{dynostore::DynoStore, gcs::limit::PutRateLimiter};

const MEMORY: &str = "in-memory (memory://)";
const S3: &str = "S3 (s3://)";
const GCS: &str = "Google Cloud Storage (gs://)";

#[derive(Clone, Copy, Debug)]
pub struct MemoryEngineFactory;

impl Display for MemoryEngineFactory {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(MEMORY)
    }
}

#[async_trait]
impl StorageFactory for MemoryEngineFactory {
    fn scheme(&self) -> Result<Regex> {
        Regex::new(r"memory").map_err(Into::into)
    }

    /// No SCRAM credentials yet: DynoStore keeps none.
    fn capabilities(&self) -> StorageCapabilities {
        StorageCapabilities::default()
    }

    async fn build(&self, configuration: StorageFactoryConfiguration) -> Result<ArcDynStorage> {
        reject_unrecognized_options(&configuration.storage, &[])?;

        Ok(Arc::new(Box::new(
            DynoStore::new(
                configuration.cluster.as_str(),
                configuration.node_id,
                InMemory::new(),
            )?
            .engine(MEMORY)
            .advertised_listener(configuration.advertised_listener.clone())
            .schemas(configuration.schema_registry)
            .lake(configuration.lake_house.clone()),
        )) as ArcDynStorage)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct S3OptimisticConcurrencyEngineFactory;

impl Display for S3OptimisticConcurrencyEngineFactory {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(S3)
    }
}

#[async_trait]
impl StorageFactory for S3OptimisticConcurrencyEngineFactory {
    fn scheme(&self) -> Result<Regex> {
        Regex::new(r"s3").map_err(Into::into)
    }

    /// No SCRAM credentials yet: DynoStore keeps none.
    fn capabilities(&self) -> StorageCapabilities {
        StorageCapabilities::default()
    }

    async fn build(&self, configuration: StorageFactoryConfiguration) -> Result<ArcDynStorage> {
        reject_unrecognized_options(&configuration.storage, BatchOptions::OPTIONS)?;

        let bucket_name = configuration.storage.host_str().unwrap_or("nisshi");

        let BatchOptions {
            minimum_size,
            maximum_delay,
        } = BatchOptions::parse(&configuration.storage)?;

        debug!(?minimum_size, ?maximum_delay);

        let builder = AmazonS3Builder::from_env()
            .with_bucket_name(bucket_name)
            .with_conditional_put(S3ConditionalPut::ETagMatch);

        // With `AWS_SKIP_SIGNATURE` on, object_store sends unsigned requests
        // and never asks the credential provider for a credential. The provider
        // still exists and, with no keys set, falls back to the instance
        // metadata service, so this check would fail a setup that works.
        let skip_signature = builder
            .get_config_value(&AmazonS3ConfigKey::SkipSignature)
            .is_some_and(|value| skip_signature(&value));

        let object_store = builder.build().map_err(nisshi_storage::Error::from)?;

        // We get a credential from the configured provider now, so that a
        // provider failure is reported as `NoCredentials` and not as a failed
        // request. The provider caches the credential, so the `ping()` startup
        // check that follows does not fetch it again.
        if !skip_signature {
            let _ = object_store
                .credentials()
                .get_credential()
                .await
                .map_err(|source| nisshi_storage::Error::NoCredentials(Arc::new(source)))?;
        }

        let storage = DynoStore::new(
            configuration.cluster.as_str(),
            configuration.node_id,
            object_store,
        )?
        .engine(S3)
        .advertised_listener(configuration.advertised_listener.clone())
        .schemas(configuration.schema_registry)
        .lake(configuration.lake_house.clone());

        let storage = ProduceRequestBatcher::new(storage)
            .with_minimum_size(minimum_size)
            .with_maximum_delay(maximum_delay);

        Ok(Arc::new(Box::new(storage)) as ArcDynStorage)
    }
}

/// Returns whether `object_store` reads `value`, the raw `AWS_SKIP_SIGNATURE`
/// setting, as true.
///
/// [`AmazonS3Builder::get_config_value`] returns the unparsed string, and
/// `object_store` parses it only in `build()`, with a crate-private parser
/// that this function must keep matching: `1`, `true`, `on`, `yes` and `y`
/// in any case are true. Every other value is either false or rejected by
/// `build()` before the credential check runs.
fn skip_signature(value: &str) -> bool {
    matches!(
        value.to_ascii_lowercase().as_str(),
        "1" | "true" | "on" | "yes" | "y"
    )
}

#[derive(Clone, Copy, Debug)]
pub struct GoogleCloudStorageEngineFactory;

impl Display for GoogleCloudStorageEngineFactory {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(GCS)
    }
}

#[async_trait]
impl StorageFactory for GoogleCloudStorageEngineFactory {
    fn scheme(&self) -> Result<Regex> {
        Regex::new(r"gs").map_err(Into::into)
    }

    /// No SCRAM credentials yet: DynoStore keeps none.
    fn capabilities(&self) -> StorageCapabilities {
        StorageCapabilities::default()
    }

    async fn build(&self, configuration: StorageFactoryConfiguration) -> Result<ArcDynStorage> {
        reject_unrecognized_options(&configuration.storage, BatchOptions::OPTIONS)?;

        let bucket_name = configuration.storage.host_str().unwrap_or("nisshi");

        let BatchOptions {
            minimum_size,
            maximum_delay,
        } = BatchOptions::parse(&configuration.storage)?;

        GoogleCloudStorageBuilder::from_env()
            .with_bucket_name(bucket_name)
            .build()
            .map_err(Into::into)
            .and_then(|object_store| {
                PutRateLimiter::new(object_store, Duration::from_mins(5)).map(|object_store| {
                    object_store
                        .with_rate_per_second(NonZeroU32::new(1))
                        .with_jitter(Some(Duration::from_millis(50)))
                })
            })
            .and_then(|object_store| {
                DynoStore::new(
                    configuration.cluster.as_str(),
                    configuration.node_id,
                    object_store,
                )
                .map(|storage| {
                    storage
                        .engine(GCS)
                        .advertised_listener(configuration.advertised_listener.clone())
                        .schemas(configuration.schema_registry)
                        .lake(configuration.lake_house.clone())
                })
            })
            .map(|storage| {
                ProduceRequestBatcher::new(storage)
                    .with_minimum_size(minimum_size)
                    .with_maximum_delay(maximum_delay)
            })
            .map(Box::new)
            .map(|storage| Arc::new(storage) as ArcDynStorage)
    }
}

/// The produce batching options that the S3 and Google Cloud Storage engines read from
/// the storage URL.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct BatchOptions {
    minimum_size: Option<usize>,
    maximum_delay: Option<Duration>,
}

impl BatchOptions {
    const OPTIONS: &[&str] = &[BATCH_MIN_SIZE, BATCH_MAX_DELAY];

    /// Reads the options from `storage`. An option that the URL doesn't give is `None`.
    ///
    /// # Errors
    ///
    /// [`nisshi_storage::Error::InvalidStorageOptionValue`] when an option's value
    /// doesn't parse, or is a size too large for this platform's `usize`.
    fn parse(storage: &Url) -> Result<Self> {
        Ok(Self {
            minimum_size: parse_storage_option(storage, BATCH_MIN_SIZE, SIZE_HINT, |value| {
                human_units::Size::from_str(value)
                    .ok()
                    .and_then(|size| usize::try_from(size.0).ok())
            })?,
            maximum_delay: parse_storage_option(
                storage,
                BATCH_MAX_DELAY,
                DURATION_HINT,
                |value| {
                    human_units::Duration::from_str(value)
                        .ok()
                        .map(|duration| duration.0)
                },
            )?,
        })
    }
}

const BATCH_MIN_SIZE: &str = "batch_min_size";
const BATCH_MAX_DELAY: &str = "batch_max_delay";

/// What `batch_min_size` takes. Every example here must parse (see the tests).
const SIZE_HINT: &str = "a size in bytes, such as 512, 64k or 1m";

/// What `batch_max_delay` takes. Every example here must parse (see the tests).
const DURATION_HINT: &str = "a duration, such as 5s or 500ms";

#[cfg(test)]
mod tests;

#[cfg(test)]
mod batch_options_tests;
