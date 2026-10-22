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

//! `fetch` stops adding records to its answer once `max_wait` has elapsed since the fetch began,
//! and always answers the first record. The wait for new records belongs to the broker, so an
//! engine that ignores `max_wait` answers every record that fits, however long that takes.

use std::time::Duration;

use crate::common::{self, StorageType, alphanumeric_string, init_tracing};
use bytes::Bytes;
use nisshi_broker::Result;
use nisshi_sans_io::record::{Record, deflated, inflated};
use nisshi_sans_io::{IsolationLevel, create_topics_request::CreatableTopic};
use nisshi_storage::{Storage, Topition};
use rand::{prelude::*, rng};
use url::Url;
use uuid::Uuid;

const RECORDS: usize = 5;

/// Creates a topic and produces `RECORDS` records, one at a time, and returns the topic's partition.
async fn topic_with_records(storage: &impl Storage) -> Result<Topition> {
    let topic = alphanumeric_string(15);

    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(topic.clone())
                .num_partitions(1)
                .replication_factor(0)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    let topition = Topition::new(topic, 0);

    for index in 0..RECORDS {
        let batch = inflated::Batch::builder()
            .record(Record::builder().value(Some(Bytes::from(format!("value-{index}")))))
            .producer_id(-1)
            .producer_epoch(-1)
            .build()
            .and_then(deflated::Batch::try_from)?;

        _ = storage.produce(None, &topition, batch).await?;
    }

    Ok(topition)
}

/// Fetches from the start of `topition` and returns how many records the answer holds.
async fn fetched(storage: &impl Storage, topition: &Topition, max_wait: Duration) -> Result<usize> {
    let batches = storage
        .fetch(
            topition,
            0,
            1,
            1024 * 1024,
            IsolationLevel::ReadUncommitted,
            max_wait,
        )
        .await?;

    batches
        .into_iter()
        .map(|batch| inflated::Batch::try_from(batch).map(|batch| batch.records.len()))
        .sum::<Result<usize, _>>()
        .map_err(Into::into)
}

/// A `max_wait` that has already elapsed leaves only the first record in the answer.
async fn elapsed_max_wait_answers_the_first_record(storage: impl Storage) -> Result<()> {
    let topition = topic_with_records(&storage).await?;

    assert_eq!(1, fetched(&storage, &topition, Duration::ZERO).await?);

    Ok(())
}

/// A `max_wait` that doesn't elapse answers every record.
async fn long_max_wait_answers_every_record(storage: impl Storage) -> Result<()> {
    let topition = topic_with_records(&storage).await?;

    assert_eq!(
        RECORDS,
        fetched(&storage, &topition, Duration::from_secs(60)).await?
    );

    Ok(())
}

macro_rules! backend {
    ($name:ident, $feature:literal, $storage_type:expr) => {
        #[cfg(feature = $feature)]
        mod $name {
            use nisshi_storage::ArcDynStorage;

            use super::*;

            async fn storage_container() -> Result<ArcDynStorage> {
                common::storage_container(
                    $storage_type,
                    Uuid::now_v7(),
                    rng().random_range(0..i32::MAX),
                    Url::parse("tcp://127.0.0.1/")?,
                    None,
                )
                .await
            }

            #[tokio::test]
            async fn elapsed_max_wait_answers_the_first_record() -> Result<()> {
                let _guard = init_tracing()?;
                super::elapsed_max_wait_answers_the_first_record(storage_container().await?).await
            }

            #[tokio::test]
            async fn long_max_wait_answers_every_record() -> Result<()> {
                let _guard = init_tracing()?;
                super::long_max_wait_answers_every_record(storage_container().await?).await
            }
        }
    };
}

backend!(lite, "libsql", StorageType::Lite);
backend!(turso, "turso", StorageType::Turso);
