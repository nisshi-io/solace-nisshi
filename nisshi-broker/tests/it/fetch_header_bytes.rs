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

//! A Fetch counts a record's headers toward `max_bytes`, as Kafka counts the whole record batch
//! ([`ReplicaManager.scala#L1733-L1739`]). The SQL engines used to add up only keys and values, so
//! a partition of records with no key, no value and large headers cost a read nothing against
//! the budget, and one Fetch loaded every one of those headers into memory.
//!
//! [`ReplicaManager.scala#L1733-L1739`]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/ReplicaManager.scala#L1733-L1739

use std::time::Duration;

use crate::common::{self, StorageType, alphanumeric_string, init_tracing, register_broker};
use bytes::Bytes;
use nisshi_broker::Result;
use nisshi_sans_io::{
    IsolationLevel,
    create_topics_request::{CreatableTopic, CreatableTopicConfig},
    record::{Header, Record, inflated},
};
use nisshi_storage::{Storage, Topition};
use rand::{prelude::*, rng};
use url::Url;
use uuid::Uuid;

const HEADER_BYTES: usize = 100 * 1024;
const RECORDS: i64 = 6;

/// Records with no key and no value, and a header of 100 KiB each, are read two at a time by a
/// `max_bytes` of 250 KiB, and one at a time, the first record being exempt from the limit
/// (KIP-74), by a `max_bytes` below the size of one. The test reads from the engine, not through
/// `FetchService`, which reads again until the response's `max_bytes` is spent.
pub async fn headers_count_toward_max_bytes<C, G>(
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
    for keyed in [false, true] {
        let topic_name: String = alphanumeric_string(15);

        _ = sc
            .create_topic(
                CreatableTopic::default()
                    .name(topic_name.clone())
                    .num_partitions(1)
                    .replication_factor(0)
                    .assignments(Some([].into()))
                    .configs(Some(
                        keyed
                            .then(|| {
                                CreatableTopicConfig::default()
                                    .name("tansu.virtual".into())
                                    .value(Some("true".into()))
                            })
                            .into_iter()
                            .collect(),
                    )),
                false,
            )
            .await?;

        let topition = Topition::new(topic_name.clone(), 0);
        let fetched = if keyed {
            Topition::new(format!("{topic_name}/k"), 0)
        } else {
            topition.clone()
        };

        for _ in 0..RECORDS {
            let batch = inflated::Batch::builder()
                .record(
                    Record::builder()
                        .key(keyed.then(|| Bytes::from_static(b"k")))
                        .header(
                            Header::builder()
                                .key(Bytes::from_static(b"h"))
                                .value(Bytes::from(vec![b'x'; HEADER_BYTES])),
                        ),
                )
                .build()
                .and_then(TryInto::try_into)?;

            _ = sc.produce(None, &topition, batch).await?;
        }

        for (fetch_offset, max_bytes, expected) in [
            (0, 250 * 1024, vec![0, 1]),
            (2, 250 * 1024, vec![2, 3]),
            (4, 250 * 1024, vec![4, 5]),
            (0, 50 * 1024, vec![0]),
            (3, 50 * 1024, vec![3]),
        ] {
            let mut offsets = vec![];

            for batch in sc
                .fetch(
                    &fetched,
                    fetch_offset,
                    1,
                    max_bytes,
                    IsolationLevel::ReadUncommitted,
                    Duration::from_secs(5),
                )
                .await?
            {
                let batch = inflated::Batch::try_from(batch)?;

                offsets.extend(
                    batch
                        .records
                        .iter()
                        .map(|record| batch.base_offset + i64::from(record.offset_delta)),
                );
            }

            assert_eq!(
                expected, offsets,
                "fetch at {fetch_offset} within {max_bytes}, keyed: {keyed}"
            );
        }
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
            async fn headers_count_toward_max_bytes() -> Result<()> {
                let _guard = init_tracing()?;

                let cluster_id = Uuid::now_v7();
                let broker_id = rng().random_range(0..i32::MAX);

                super::headers_count_toward_max_bytes(
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
