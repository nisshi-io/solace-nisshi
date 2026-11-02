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

//! A produce advances an idempotent producer's sequence only after every other write of the
//! batch succeeds. When a write before the advance fails, the client's retry of the same batch
//! must be stored. If the sequence had already moved, the retry would get
//! `DUPLICATE_SEQUENCE_NUMBER`, which a client reports as success, and the batch would be lost.
//!
//! Kafka commits producer state after the log append, in
//! [`UnifiedLog.append`](https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/log/UnifiedLog.scala#L895-L915).
//!
//! `traceforge_produce_object_store_fault` in `nisshi-broker` fails the batch write. These tests
//! fail the writes before it: the watermark, and the transaction's record of the batch.

use std::{assert_matches, time::Duration};

use bytes::Bytes;
use nisshi_sans_io::{
    BatchAttribute, IsolationLevel,
    add_partitions_to_txn_request::AddPartitionsToTxnTopic,
    create_topics_request::{CreatableTopic, CreatableTopicConfig},
    record::{Record, deflated, inflated},
    topic_config,
};
use nisshi_schema::lake::House;
use nisshi_storage::{Error, Storage as _, Topition, TxnAddPartitionsRequest};
use object_store::{PutPayload, memory::InMemory, path::Path};

use crate::dynostore::{
    DynoStore, Meta,
    tests::{failing_object_store::FailingObjectStore, init_tracing},
};

const TOPIC: &str = "abc";
const TRANSACTION: &str = "txn-1";

async fn create_topic(
    storage: &DynoStore,
    configs: Vec<CreatableTopicConfig>,
) -> Result<(), Error> {
    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(TOPIC.into())
                .num_partitions(1)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some(configs)),
            false,
        )
        .await?;

    Ok(())
}

fn idempotent_batch(
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
    transactional: bool,
) -> Result<deflated::Batch, Error> {
    inflated::Batch::builder()
        .attributes(BatchAttribute::default().transaction(transactional).into())
        .producer_id(producer_id)
        .producer_epoch(producer_epoch)
        .base_sequence(base_sequence)
        .record(Record::builder().value(Some(Bytes::from_static(b"value"))))
        .build()
        .and_then(deflated::Batch::try_from)
        .map_err(Into::into)
}

async fn fetched_base_offsets(storage: &DynoStore, topition: &Topition) -> Result<Vec<i64>, Error> {
    Ok(storage
        .fetch(
            topition,
            0,
            1,
            1_048_576,
            IsolationLevel::ReadUncommitted,
            Duration::from_millis(10),
        )
        .await?
        .iter()
        .map(|batch| batch.base_offset)
        .collect())
}

fn is_watermark_write(location: &Path, _payload: &PutPayload) -> bool {
    location.as_ref().ends_with("/watermark.json")
}

/// Picks the `meta.json` write that records a batch's offsets in its transaction. The sequence
/// advance writes `meta.json` too, so the path alone cannot tell the two apart.
fn records_produce_in_transaction(location: &Path, payload: &PutPayload) -> bool {
    location.as_ref().ends_with("/meta.json")
        && serde_json::from_slice::<Meta>(&payload.iter().flatten().copied().collect::<Vec<u8>>())
            .is_ok_and(|meta| {
                meta.transactions
                    .values()
                    .flat_map(|txn| txn.epochs.values())
                    .flat_map(|txn_detail| txn_detail.produces.values())
                    .flat_map(|partitions| partitions.values())
                    .any(Option::is_some)
            })
}

#[tokio::test]
async fn retry_after_failed_watermark_write_stores_the_batch() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let object_store = FailingObjectStore::failing_put_on_demand(InMemory::new());
    let storage = DynoStore::new("nisshi", 111, object_store.clone())?;

    create_topic(&storage, vec![]).await?;

    let topition = Topition::new(TOPIC, 0);
    let producer = storage.init_producer(None, 0, Some(-1), Some(-1)).await?;

    object_store.fail_next_put(is_watermark_write);

    assert_matches!(
        storage
            .produce(
                None,
                &topition,
                idempotent_batch(producer.id, producer.epoch, 0, false)?
            )
            .await,
        Err(Error::ObjectStore(_))
    );

    let retry = storage
        .produce(
            None,
            &topition,
            idempotent_batch(producer.id, producer.epoch, 0, false)?,
        )
        .await?;

    assert_eq!(0, retry);
    assert_eq!(vec![0], fetched_base_offsets(&storage, &topition).await?);

    assert_eq!(
        1,
        storage
            .produce(
                None,
                &topition,
                idempotent_batch(producer.id, producer.epoch, 1, false)?
            )
            .await?
    );

    Ok(())
}

#[tokio::test]
async fn retry_after_failed_transaction_write_stores_the_batch() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let object_store = FailingObjectStore::failing_put_on_demand(InMemory::new());
    let storage = DynoStore::new("nisshi", 111, object_store.clone())?;

    create_topic(&storage, vec![]).await?;

    let topition = Topition::new(TOPIC, 0);
    let producer = storage
        .init_producer(Some(TRANSACTION), 10_000, Some(-1), Some(-1))
        .await?;

    _ = storage
        .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
            transaction_id: TRANSACTION.into(),
            producer_id: producer.id,
            producer_epoch: producer.epoch,
            topics: vec![
                AddPartitionsToTxnTopic::default()
                    .name(TOPIC.into())
                    .partitions(Some(vec![0])),
            ],
        })
        .await?;

    object_store.fail_next_put(records_produce_in_transaction);

    assert_matches!(
        storage
            .produce(
                Some(TRANSACTION),
                &topition,
                idempotent_batch(producer.id, producer.epoch, 0, true)?
            )
            .await,
        Err(Error::ObjectStore(_))
    );

    let retry = storage
        .produce(
            Some(TRANSACTION),
            &topition,
            idempotent_batch(producer.id, producer.epoch, 0, true)?,
        )
        .await?;

    assert_eq!(
        vec![retry],
        fetched_base_offsets(&storage, &topition).await?
    );

    assert_eq!(
        retry + 1,
        storage
            .produce(
                Some(TRANSACTION),
                &topition,
                idempotent_batch(producer.id, producer.epoch, 1, true)?
            )
            .await?
    );

    Ok(())
}

/// A topic with `nisshi.lake.sink=true` takes its own branch of the produce, which orders the
/// watermark write, the lake write and the sequence advance separately from the other branch.
#[tokio::test]
async fn retry_after_failed_watermark_write_to_lake_sink_stores_the_batch() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let object_store = FailingObjectStore::failing_put_on_demand(InMemory::new());

    // `House::None` sends the produce down the lake-sink path, and without a schema registry
    // that path writes nothing to the lake, so the test needs no lake service.
    let storage = DynoStore::new("nisshi", 111, object_store.clone())?.lake(Some(House::None));

    create_topic(
        &storage,
        vec![
            CreatableTopicConfig::default()
                .name(topic_config::LAKE_SINK.into())
                .value(Some("true".into())),
        ],
    )
    .await?;

    let topition = Topition::new(TOPIC, 0);
    let producer = storage.init_producer(None, 0, Some(-1), Some(-1)).await?;

    object_store.fail_next_put(is_watermark_write);

    assert_matches!(
        storage
            .produce(
                None,
                &topition,
                idempotent_batch(producer.id, producer.epoch, 0, false)?
            )
            .await,
        Err(Error::ObjectStore(_))
    );

    // The retry is stored, not answered as a duplicate of a batch that was never stored.
    assert_eq!(
        0,
        storage
            .produce(
                None,
                &topition,
                idempotent_batch(producer.id, producer.epoch, 0, false)?
            )
            .await?
    );

    // The watermark moved with the retry, so the next batch takes the next offset. If the
    // sequence had advanced before the watermark write, this batch would share offset 0.
    assert_eq!(
        1,
        storage
            .produce(
                None,
                &topition,
                idempotent_batch(producer.id, producer.epoch, 1, false)?
            )
            .await?
    );

    assert_eq!(2, storage.offset_stage(&topition).await?.high_watermark);

    Ok(())
}
