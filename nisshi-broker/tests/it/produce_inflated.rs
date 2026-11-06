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

//! `Storage::produce_inflated` stores the batch that the caller decoded, and a backend does not
//! decode the batch's bytes again.
//!
//! Each test sends record bytes that do not decode next to the batch that does. `produce` decodes
//! the bytes and answers `INVALID_RECORD`. `produce_inflated` stores the decoded records and never
//! reads the bytes. The tests run against the backends that decode a produced batch: Postgres,
//! libSQL, and the object store when a schema registry is configured.

use nisshi_storage::fetched::FetchUntilDeadline as _;
use std::{assert_matches, time::Duration};

use crate::common::{StorageType, alphanumeric_string, init_tracing};
use bytes::Bytes;
use nisshi_broker::{Error, Result};
use nisshi_sans_io::{
    ErrorCode, IsolationLevel,
    create_topics_request::CreatableTopic,
    record::{Record, deflated, inflated},
};
use nisshi_schema::Registry;
use nisshi_storage::{ArcDynStorage, Storage, Topition};
use rand::{RngExt as _, rng};
use tokio::time::Instant;
use url::Url;
use uuid::Uuid;

async fn storage(storage_type: StorageType, with_registry: bool) -> Result<ArcDynStorage> {
    let schemas = if with_registry {
        Url::parse("file://../etc/schema")
            .map_err(Error::from)
            .and_then(|url| {
                Registry::builder_try_from_url(&url)
                    .map(|builder| builder.build())
                    .map_err(Into::into)
            })
            .map(Some)?
    } else {
        None
    };

    crate::common::storage_container(
        storage_type,
        Uuid::now_v7(),
        rng().random_range(0..i32::MAX),
        Url::parse("tcp://127.0.0.1/")?,
        schemas,
    )
    .await
}

/// `reads_back` is whether the backend stores the decoded records. The object store keeps the bytes
/// as sent, so a fetch of the undecodable bytes fails there, and the test stops after the produce.
async fn assert_stores_the_decoded_batch(storage: impl Storage, reads_back: bool) -> Result<()> {
    let name = &alphanumeric_string(15)[..];

    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(name.into())
                .num_partitions(1)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    let inflated = inflated::Batch::builder()
        .record(Record::builder().value(Bytes::from_static(b"decoded once").into()))
        .build()?;

    let mut undecodable: deflated::Batch = inflated.clone().try_into()?;
    undecodable.record_data = Bytes::from_static(b"\xff\xff\xff");

    let topition = Topition::new(name, 0);

    assert_matches!(
        storage.produce(None, &topition, undecodable.clone()).await,
        Err(nisshi_storage::Error::Api(ErrorCode::InvalidRecord))
    );

    assert_eq!(
        0,
        storage
            .produce_inflated(None, &topition, undecodable, inflated)
            .await?
    );

    if !reads_back {
        return Ok(());
    }

    let records = storage
        .fetch_until_deadline(
            &topition,
            0,
            1,
            1_000_000,
            IsolationLevel::ReadUncommitted,
            Instant::now() + Duration::from_secs(1),
        )
        .await
        .map(|read| read.batches)?
        .into_iter()
        .map(inflated::Batch::try_from)
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .flat_map(|batch| batch.records)
        .collect::<Vec<_>>();

    assert_eq!(1, records.len());
    assert_eq!(
        Some(Bytes::from_static(b"decoded once")),
        records[0].value.clone()
    );

    Ok(())
}

#[cfg(feature = "dynostore")]
mod in_memory {
    use super::*;

    #[tokio::test]
    async fn stores_the_decoded_batch_with_registry() -> Result<()> {
        let _guard = init_tracing()?;
        assert_stores_the_decoded_batch(storage(StorageType::InMemory, true).await?, false).await
    }
}

#[cfg(feature = "libsql")]
mod lite {
    use super::*;

    #[tokio::test]
    async fn stores_the_decoded_batch() -> Result<()> {
        let _guard = init_tracing()?;
        assert_stores_the_decoded_batch(storage(StorageType::Lite, false).await?, true).await
    }
}

#[cfg(feature = "postgres")]
mod pg {
    use super::*;

    #[tokio::test]
    async fn stores_the_decoded_batch() -> Result<()> {
        let _guard = init_tracing()?;
        assert_stores_the_decoded_batch(storage(StorageType::Postgres, false).await?, true).await
    }
}
