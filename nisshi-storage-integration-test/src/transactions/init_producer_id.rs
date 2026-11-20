// Copyright (c) Solace Corporation
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

//! Cases about producer IDs for a transactional ID, and the transactions of those producers.

use crate::{
    broker::register_broker,
    producer_id::init_producer_id::{
        TRANSACTION_TIMEOUT_MS, assert_failed, assert_ok, init_idempotent_producer,
        request_decoded_at_version,
    },
    random::alphanumeric_string,
};
use bytes::Bytes;
use nisshi_broker::Result;
use nisshi_sans_io::{
    BatchAttribute, ControlBatch, ErrorCode, InitProducerIdRequest, InitProducerIdResponse,
    IsolationLevel, ListOffset, RequestInput,
    add_partitions_to_txn_request::AddPartitionsToTxnTopic,
    create_topics_request::CreatableTopic,
    record::{Record, inflated},
};
use nisshi_storage::{
    InitProducerIdService, Storage, Topition, TxnAddPartitionsRequest,
    fetched::FetchUntilDeadline as _,
};
use rama::{Service, extensions::Extensions};
use std::time::Duration;
use tokio::time::Instant;
use uuid::Uuid;

/// Initializes a transactional producer for `transactional_id` in the storage, with this producer
/// ID and epoch.
///
/// Calls the storage directly, because [`InitProducerIdService`] refuses every request that carries
/// a transactional ID.
async fn init_transactional_producer(
    storage: &impl Storage,
    transactional_id: &str,
    producer_id: i64,
    producer_epoch: i16,
) -> Result<InitProducerIdResponse> {
    let response = storage
        .init_producer(
            Some(transactional_id),
            TRANSACTION_TIMEOUT_MS,
            Some(producer_id),
            Some(producer_epoch),
        )
        .await?;

    Ok(InitProducerIdResponse::default()
        .error_code(response.error.into())
        .producer_id(response.id)
        .producer_epoch(response.epoch))
}

/// The broker must answer a transactional ID with `TRANSACTIONAL_ID_AUTHORIZATION_FAILED` at every
/// version, before it calls the storage.
pub async fn transactional_id_is_refused(
    cluster_id: impl Into<String>,
    broker_id: i32,
    storage: impl Storage + Clone,
) -> Result<()> {
    register_broker(cluster_id, broker_id, &storage).await?;

    let service = InitProducerIdService {
        storage: storage.clone(),
    };
    let txn = Uuid::now_v7().to_string();

    for api_version in 0..=5 {
        let request = request_decoded_at_version(
            InitProducerIdRequest::default()
                .transactional_id(Some(txn.clone()))
                .transaction_timeout_ms(TRANSACTION_TIMEOUT_MS)
                .producer_id(Some(-1))
                .producer_epoch(Some(-1)),
            api_version,
        )?;

        let response = service
            .serve(RequestInput {
                request,
                extensions: Extensions::default(),
            })
            .await?;

        assert_failed(ErrorCode::TransactionalIdAuthorizationFailed, &response);
    }

    // This call checks that no refused request reached the storage. The storage still serves a
    // transactional ID when the test calls it directly. The storage registers an ID on its first
    // request and answers it with epoch 0, and answers each later request for the ID with the
    // next epoch. Epoch 0 here shows that the storage had not seen the ID before this call.
    let direct_storage_response = init_transactional_producer(&storage, &txn, -1, -1).await?;
    assert_ok(&direct_storage_response);
    assert_eq!(0, direct_storage_response.producer_epoch);

    Ok(())
}

/// v0-2 have no ProducerId or ProducerEpoch, so a fresh request for a transactional ID decodes
/// with neither set. The broker must then give the request the producer ID that holds the
/// transactional ID, with the next epoch.
pub async fn old_versions_bump_a_transactional_producer(
    cluster_id: impl Into<String>,
    broker_id: i32,
    storage: impl Storage + Clone,
) -> Result<()> {
    register_broker(cluster_id, broker_id, &storage).await?;

    let service = InitProducerIdService { storage };
    let txn = Uuid::now_v7().to_string();

    let mut previous: Option<InitProducerIdResponse> = None;

    for api_version in 0..=5 {
        let request = request_decoded_at_version(
            InitProducerIdRequest::default()
                .transactional_id(Some(txn.clone()))
                .transaction_timeout_ms(TRANSACTION_TIMEOUT_MS)
                .producer_id(Some(-1))
                .producer_epoch(Some(-1)),
            api_version,
        )?;

        let response = service
            .serve(RequestInput {
                request,
                extensions: Extensions::default(),
            })
            .await?;

        assert_ok(&response);

        match previous {
            Some(previous) => {
                assert_eq!(previous.producer_id, response.producer_id);
                assert_eq!(previous.producer_epoch + 1, response.producer_epoch);
            }

            None => assert_eq!(0, response.producer_epoch),
        }

        previous = Some(response);
    }

    Ok(())
}

pub async fn txn_claim_current_epoch_bumps(
    cluster_id: impl Into<String>,
    broker_id: i32,
    storage: impl Storage + Clone,
) -> Result<()> {
    register_broker(cluster_id, broker_id, &storage).await?;

    let txn = Uuid::now_v7().to_string();

    let mut current = init_transactional_producer(&storage, &txn, -1, -1).await?;
    assert_ok(&current);

    for _ in 0..2 {
        let bumped = init_transactional_producer(
            &storage,
            &txn,
            current.producer_id,
            current.producer_epoch,
        )
        .await?;

        assert_ok(&bumped);
        assert_eq!(current.producer_id, bumped.producer_id);
        assert_eq!(current.producer_epoch + 1, bumped.producer_epoch);

        current = bumped;
    }

    Ok(())
}

/// A claim of an older or a newer epoch is fenced, and leaves the stored epoch as it was.
pub async fn txn_claim_stale_epoch_is_fenced(
    cluster_id: impl Into<String>,
    broker_id: i32,
    storage: impl Storage + Clone,
) -> Result<()> {
    register_broker(cluster_id, broker_id, &storage).await?;

    let txn = Uuid::now_v7().to_string();

    let stale = init_transactional_producer(&storage, &txn, -1, -1).await?;
    assert_ok(&stale);

    let current = init_transactional_producer(&storage, &txn, -1, -1).await?;
    assert_ok(&current);
    assert_eq!(stale.producer_id, current.producer_id);
    assert_eq!(stale.producer_epoch + 1, current.producer_epoch);

    for epoch in [stale.producer_epoch, current.producer_epoch + 1] {
        assert_failed(
            ErrorCode::ProducerFenced,
            &init_transactional_producer(&storage, &txn, current.producer_id, epoch).await?,
        );
    }

    let bumped =
        init_transactional_producer(&storage, &txn, current.producer_id, current.producer_epoch)
            .await?;
    assert_ok(&bumped);
    assert_eq!(current.producer_epoch + 1, bumped.producer_epoch);

    Ok(())
}

pub async fn txn_claim_other_id_is_fenced(
    cluster_id: impl Into<String>,
    broker_id: i32,
    storage: impl Storage + Clone,
) -> Result<()> {
    register_broker(cluster_id, broker_id, &storage).await?;

    let txn = Uuid::now_v7().to_string();
    let other_txn = Uuid::now_v7().to_string();

    let producer = init_transactional_producer(&storage, &txn, -1, -1).await?;
    assert_ok(&producer);

    let other = init_transactional_producer(&storage, &other_txn, -1, -1).await?;
    assert_ok(&other);
    assert_ne!(producer.producer_id, other.producer_id);
    assert_eq!(producer.producer_epoch, other.producer_epoch);

    assert_failed(
        ErrorCode::ProducerFenced,
        &init_transactional_producer(&storage, &txn, other.producer_id, other.producer_epoch)
            .await?,
    );

    Ok(())
}

/// A claim for a transactional ID with no producer creates one, as in Kafka.
pub async fn txn_claim_unknown_txn_is_fresh(
    cluster_id: impl Into<String>,
    broker_id: i32,
    storage: impl Storage + Clone,
) -> Result<()> {
    register_broker(cluster_id, broker_id, &storage).await?;

    let txn = Uuid::now_v7().to_string();

    let response = init_transactional_producer(&storage, &txn, 4_242, 3).await?;
    assert_ok(&response);
    assert!(response.producer_id > 0);
    assert_eq!(0, response.producer_epoch);

    Ok(())
}

/// A request with only one of producer ID and epoch set to -1 is invalid, through the
/// service and when a caller uses the storage directly.
pub async fn mixed_shape_is_invalid_request(
    cluster_id: impl Into<String>,
    broker_id: i32,
    storage: impl Storage + Clone,
) -> Result<()> {
    register_broker(cluster_id, broker_id, &storage).await?;

    let txn = Uuid::now_v7().to_string();

    for (producer_id, producer_epoch) in [(-1, 0), (5, -1)] {
        assert_failed(
            ErrorCode::InvalidRequest,
            &init_idempotent_producer(&storage, producer_id, producer_epoch).await?,
        );
        assert_failed(
            ErrorCode::InvalidRequest,
            &init_transactional_producer(&storage, &txn, producer_id, producer_epoch).await?,
        );
    }

    for transactional_id in [None, Some(txn.as_str())] {
        for (producer_id, producer_epoch) in
            [(Some(-1), None), (None, Some(0)), (Some(5), Some(-1))]
        {
            let response = storage
                .init_producer(
                    transactional_id,
                    TRANSACTION_TIMEOUT_MS,
                    producer_id,
                    producer_epoch,
                )
                .await?;

            assert_eq!(ErrorCode::InvalidRequest, response.error, "{response:?}");
            assert_eq!((-1, -1), (response.id, response.epoch), "{response:?}");
        }
    }

    Ok(())
}

const RECORDS: i32 = 3;

/// Creates a topic with one partition, and returns that partition.
async fn create_topition(storage: &impl Storage) -> Result<Topition> {
    let name = alphanumeric_string(15);

    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(name.clone())
                .num_partitions(1)
                .replication_factor(0)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    Ok(Topition::new(name, 0))
}

/// Adds the partition to the producer's transaction, and produces [`RECORDS`] records to it.
async fn produce_in_txn(
    storage: &impl Storage,
    transactional_id: &str,
    producer: &InitProducerIdResponse,
    topition: &Topition,
) -> Result<()> {
    _ = storage
        .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
            transaction_id: transactional_id.into(),
            producer_id: producer.producer_id,
            producer_epoch: producer.producer_epoch,
            topics: vec![
                AddPartitionsToTxnTopic::default()
                    .name(topition.topic().into())
                    .partitions(Some(vec![topition.partition()])),
            ],
        })
        .await?;

    for base_sequence in 0..RECORDS {
        let batch = inflated::Batch::builder()
            .record(Record::builder().value(Bytes::from_static(b"value").into()))
            .attributes(BatchAttribute::default().transaction(true).into())
            .producer_id(producer.producer_id)
            .producer_epoch(producer.producer_epoch)
            .base_sequence(base_sequence)
            .build()
            .and_then(TryInto::try_into)?;

        _ = storage
            .produce(Some(transactional_id), topition, batch)
            .await?;
    }

    Ok(())
}

/// Returns the latest offset of the partition at this isolation level.
async fn latest(
    storage: &impl Storage,
    topition: &Topition,
    isolation: IsolationLevel,
) -> Result<Option<i64>> {
    let offsets = storage
        .list_offsets(isolation, &[(topition.clone(), ListOffset::Latest)])
        .await?;

    assert_eq!(1, offsets.len());
    assert_eq!(ErrorCode::None, offsets[0].1.error_code);

    Ok(offsets[0].1.offset)
}

/// Returns the key of the control record that ends the producer's transaction, at the
/// offset after its records.
async fn end_marker(storage: &impl Storage, topition: &Topition) -> Result<Option<Bytes>> {
    let batches = storage
        .fetch_until_deadline(
            topition,
            i64::from(RECORDS),
            1,
            50 * 1024,
            IsolationLevel::ReadUncommitted,
            Instant::now() + Duration::from_millis(500),
        )
        .await
        .map(|read| read.batches)?;

    assert_eq!(1, batches.len());

    let batch = inflated::Batch::try_from(batches[0].clone())?;
    assert_eq!(1, batch.records.len());

    Ok(batch.records[0].key.clone())
}

/// A claim of the current epoch while a transaction is ongoing aborts that transaction,
/// then bumps the epoch, as a fresh request does.
pub async fn txn_claim_ongoing_txn_aborts_and_bumps(
    cluster_id: impl Into<String>,
    broker_id: i32,
    storage: impl Storage + Clone,
) -> Result<()> {
    register_broker(cluster_id, broker_id, &storage).await?;

    let txn = Uuid::now_v7().to_string();
    let topition = create_topition(&storage).await?;

    let producer = init_transactional_producer(&storage, &txn, -1, -1).await?;
    assert_ok(&producer);

    produce_in_txn(&storage, &txn, &producer, &topition).await?;

    assert_eq!(
        Some(0),
        latest(&storage, &topition, IsolationLevel::ReadCommitted).await?
    );

    let bumped = init_transactional_producer(
        &storage,
        &txn,
        producer.producer_id,
        producer.producer_epoch,
    )
    .await?;
    assert_ok(&bumped);
    assert_eq!(producer.producer_id, bumped.producer_id);
    assert_eq!(producer.producer_epoch + 1, bumped.producer_epoch);

    let after_abort = Some(i64::from(RECORDS) + 1);

    assert_eq!(
        after_abort,
        latest(&storage, &topition, IsolationLevel::ReadUncommitted).await?
    );
    assert_eq!(
        after_abort,
        latest(&storage, &topition, IsolationLevel::ReadCommitted).await?
    );
    assert_eq!(
        Some(ControlBatch::default().abort().try_into()?),
        end_marker(&storage, &topition).await?
    );

    Ok(())
}

/// A fenced claim leaves the transaction of the producer that holds the current epoch
/// open, and that producer can still commit it.
pub async fn txn_fenced_claim_leaves_ongoing_txn(
    cluster_id: impl Into<String>,
    broker_id: i32,
    storage: impl Storage + Clone,
) -> Result<()> {
    register_broker(cluster_id, broker_id, &storage).await?;

    let txn = Uuid::now_v7().to_string();
    let topition = create_topition(&storage).await?;

    let fenced = init_transactional_producer(&storage, &txn, -1, -1).await?;
    assert_ok(&fenced);

    let current = init_transactional_producer(&storage, &txn, -1, -1).await?;
    assert_ok(&current);

    produce_in_txn(&storage, &txn, &current, &topition).await?;

    assert_failed(
        ErrorCode::ProducerFenced,
        &init_transactional_producer(&storage, &txn, fenced.producer_id, fenced.producer_epoch)
            .await?,
    );

    assert_eq!(
        Some(0),
        latest(&storage, &topition, IsolationLevel::ReadCommitted).await?
    );

    assert_eq!(
        ErrorCode::None,
        storage
            .txn_end(&txn, current.producer_id, current.producer_epoch, true)
            .await?
    );

    assert_eq!(
        Some(i64::from(RECORDS) + 1),
        latest(&storage, &topition, IsolationLevel::ReadCommitted).await?
    );
    assert_eq!(
        Some(ControlBatch::default().commit().try_into()?),
        end_marker(&storage, &topition).await?
    );

    Ok(())
}
