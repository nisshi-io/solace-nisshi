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

//! The error code that `ListOffsets` by timestamp answers when the object store fails during the
//! lookup. The Java consumer retries `KAFKA_STORAGE_ERROR` and `UNKNOWN_SERVER_ERROR` alike. The
//! admin client retries only a retriable error, which `KAFKA_STORAGE_ERROR` is and
//! `UNKNOWN_SERVER_ERROR` is not, so it fails on the second.

use std::sync::Arc;

use bytes::Bytes;
use nisshi_sans_io::{
    ErrorCode, IsolationLevel, ListOffset,
    create_topics_request::CreatableTopic,
    record::{Record, deflated, inflated},
    to_system_time,
};
use nisshi_storage::{Error, Storage as _, Topition};
use object_store::memory::InMemory;

use crate::dynostore::{
    DynoStore,
    tests::{failing_object_store::FailingObjectStore, init_tracing},
    time_index::TimeIndex,
};

const T0: i64 = 1_577_836_800_000;

fn batch() -> Result<deflated::Batch, Error> {
    inflated::Batch::builder()
        .base_timestamp(T0)
        .max_timestamp(T0)
        .record(Record::builder().value(Some(Bytes::from_static(b"v"))))
        .build()
        .and_then(deflated::Batch::try_from)
        .map_err(Into::into)
}

/// A scan for a timestamp lists the batches of the partition. When the listing fails, only that
/// partition answers an error, and the code is retriable.
#[tokio::test]
async fn failed_listing_answers_kafka_storage_error_in_its_own_partition() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let object_store = Arc::new(InMemory::new());
    let setup = DynoStore::new("nisshi", 111, object_store.clone())?;

    _ = setup
        .create_topic(
            CreatableTopic::default()
                .name("abc".into())
                .num_partitions(2)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    let failing = Topition::new("abc", 0);
    let empty = Topition::new("abc", 1);
    assert_eq!(0, setup.produce(None, &failing, batch()?).await?);

    // The same bucket, whose listings now fail.
    let storage = DynoStore::new(
        "nisshi",
        111,
        FailingObjectStore::failing_list(object_store),
    )?;

    let target = ListOffset::Timestamp(to_system_time(T0)?);
    let responses = storage
        .list_offsets(
            IsolationLevel::ReadUncommitted,
            &[(failing.clone(), target), (empty.clone(), target)],
        )
        .await?;

    assert_eq!(2, responses.len());

    assert_eq!(failing, responses[0].0);
    assert_eq!(ErrorCode::KafkaStorageError, responses[0].1.error_code);
    assert_eq!(None, responses[0].1.offset);

    // The other partition has no records, so it answers without a scan.
    assert_eq!(empty, responses[1].0);
    assert_eq!(ErrorCode::None, responses[1].1.error_code);

    Ok(())
}

/// A partition whose time index is not complete (as one written before the index existed) is
/// backfilled by the first lookup, which writes the index. When that write fails, the partition
/// answers a retriable error.
#[tokio::test]
async fn failed_backfill_write_answers_kafka_storage_error() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let inner = Arc::new(InMemory::new());
    let setup = DynoStore::new("nisshi", 111, inner.clone())?;

    _ = setup
        .create_topic(
            CreatableTopic::default()
                .name("abc".into())
                .num_partitions(1)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    let topition = Topition::new("abc", 0);
    assert_eq!(0, setup.produce(None, &topition, batch()?).await?);

    setup
        .watermark_for(&topition)?
        .with_mut(&setup.object_store, |watermark| {
            watermark.time_index = TimeIndex::default();
            Ok(())
        })
        .await?;

    let storage = DynoStore::new("nisshi", 111, FailingObjectStore::failing_put(inner))?;

    let responses = storage
        .list_offsets(
            IsolationLevel::ReadUncommitted,
            &[(topition.clone(), ListOffset::Timestamp(to_system_time(T0)?))],
        )
        .await?;

    assert_eq!(1, responses.len());
    assert_eq!(ErrorCode::KafkaStorageError, responses[0].1.error_code);
    assert_eq!(None, responses[0].1.offset);

    Ok(())
}
