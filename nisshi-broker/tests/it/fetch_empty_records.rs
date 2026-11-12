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

//! A Fetch over records with no key, no value and no headers still spends `max_bytes`. Kafka counts
//! the whole record batch toward `max_bytes`, at least 7 bytes for a record and 61 for the batch
//! header ([`DefaultRecordBatch.java#L129`]). The SQL engines used to count such a record as 0
//! bytes, so a read returned every one of them up to the row limit, however small `max_bytes` was.
//!
//! [`DefaultRecordBatch.java#L129`]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/common/record/DefaultRecordBatch.java#L129

use crate::{
    common::{self, StorageType, init_tracing, register_broker},
    fetch_header_bytes::{create_topic, fetched_offsets},
};
use bytes::Bytes;
use nisshi_broker::Result;
use nisshi_sans_io::record::{Record, inflated};
use nisshi_storage::{Storage, Topition};
use rand::{prelude::*, rng};
use url::Url;
use uuid::Uuid;

/// Produces one batch of `count` records with no value and no headers, and the key `k` when
/// `keyed`.
async fn produce_empty_records<G>(
    sc: &G,
    topition: &Topition,
    keyed: bool,
    count: i64,
) -> Result<()>
where
    G: Storage,
{
    let batch = (0..count)
        .fold(inflated::Batch::builder(), |builder, delta| {
            builder.record(
                Record::builder()
                    .key(keyed.then(|| Bytes::from_static(b"k")))
                    .offset_delta(i32::try_from(delta).unwrap_or(i32::MAX)),
            )
        })
        .last_offset_delta(i32::try_from(count - 1)?)
        .base_sequence(0)
        .build()
        .and_then(TryInto::try_into)?;

    _ = sc.produce(None, topition, batch).await?;

    Ok(())
}

/// A record with no value and no headers costs 7 bytes, 8 with the 1 byte key of a virtual
/// topic's record, so a `max_bytes` of 70 returns the 9 or 8 records whose running total stays
/// under it. A `max_bytes` of 1 still returns the first record (KIP-74). The test reads from the
/// engine, not through `FetchService`, which reads again until the response's `max_bytes` is spent.
pub async fn empty_records_count_toward_max_bytes<C, G>(
    cluster_id: C,
    broker_id: i32,
    sc: G,
) -> Result<()>
where
    C: Into<String>,
    G: Storage + Clone,
{
    register_broker(cluster_id, broker_id, &sc).await?;

    // a keyed fetch of a virtual topic reads through its own statement
    for (keyed, returned) in [(false, 9), (true, 8)] {
        let (topition, fetched) = create_topic(&sc, keyed).await?;

        produce_empty_records(&sc, &topition, keyed, 100).await?;

        assert_eq!(
            (0..returned).collect::<Vec<_>>(),
            fetched_offsets(&sc, &fetched, 0, 70).await?,
            "keyed: {keyed}"
        );

        assert_eq!(
            vec![0],
            fetched_offsets(&sc, &fetched, 0, 1).await?,
            "keyed: {keyed}"
        );
    }

    Ok(())
}

macro_rules! backend {
    ($module:ident, $storage_type:expr, $feature:literal $(, #[$attribute:meta])*) => {
        #[cfg(feature = $feature)]
        mod $module {
            use super::*;
            use nisshi_storage::ArcDynStorage;

            async fn storage_container(
                cluster: impl Into<String> + Clone,
                node: i32,
            ) -> Result<ArcDynStorage> {
                common::storage_container(
                    $storage_type,
                    cluster,
                    node,
                    Url::parse("tcp://127.0.0.1/")?,
                    None,
                )
                .await
            }

            $(#[$attribute])*
            #[tokio::test]
            async fn empty_records_count_toward_max_bytes() -> Result<()> {
                let _guard = init_tracing()?;

                let cluster_id = Uuid::now_v7();
                let broker_id = rng().random_range(0..i32::MAX);

                super::empty_records_count_toward_max_bytes(
                    cluster_id,
                    broker_id,
                    storage_container(cluster_id, broker_id).await?,
                )
                .await
            }
        }
    };
}

backend!(pg, StorageType::Postgres, "postgres");
backend!(lite, StorageType::Lite, "libsql");
backend!(
    turso,
    StorageType::Turso,
    "turso",
    #[ignore = "the Turso engine does not start in the broker tests yet, see #866"]
);
