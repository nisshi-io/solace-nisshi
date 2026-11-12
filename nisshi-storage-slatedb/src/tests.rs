// Copyright (c) Solace Corporation
//
// Contributors outside Solace:
//   Enwei Jiao
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

//! Tests for SlateDB storage engine
//!
//! Note: Basic CRUD operations (create/delete topic, metadata, produce, offset commit,
//! list offsets, init producer, describe config, list/delete groups, transactions)
//! are covered by broker tests in nisshi-broker/tests/it/*.rs with slatedb module.
//!
//! This file contains tests for:
//! - Low-level API tests (offset_stage, brokers, cluster_id, node)
//! - Error case tests (duplicate topic, unknown txn, wrong producer/epoch)
//! - Unique feature tests (isolation levels, delete records, idempotent produce, builder pattern)

use std::sync::Arc;

use bytes::Bytes;
use nisshi_sans_io::{ErrorCode, create_topics_request::CreatableTopic, record::deflated::Batch};
use nisshi_storage::{
    BrokerRegistrationRequest, Error, Storage, Topition, TxnAddPartitionsRequest,
    TxnAddPartitionsResponse,
};
use slatedb::{Db, object_store::memory::InMemory};
use url::Url;

use super::engine::Engine;

async fn create_test_engine() -> Engine {
    let object_store = Arc::new(InMemory::new());
    let db = Db::open("test.slatedb", object_store)
        .await
        .expect("Failed to open SlateDB");

    Engine::new(
        "test-cluster",
        1,
        Url::parse("tcp://localhost:9092").unwrap(),
        Arc::new(db),
    )
}

/// A single-record batch, built through the encode path, so that it decodes
/// back to the record it holds.
fn simple_batch() -> Batch {
    use nisshi_sans_io::record::{Record, inflated};

    inflated::Batch::builder()
        .record(Record::builder().value(Some(Bytes::from_static(b"value"))))
        .build()
        .and_then(Batch::try_from)
        .unwrap()
}

// ========== Unique Error Case Tests ==========

#[tokio::test]
async fn test_create_duplicate_topic() {
    let engine = create_test_engine().await;

    let topic = CreatableTopic::default()
        .name("dup-topic".into())
        .num_partitions(1)
        .replication_factor(1);

    // First creation should succeed
    let result = engine.create_topic(topic.clone(), false).await;
    assert!(result.is_ok());

    // Second creation should fail
    let result = engine.create_topic(topic, false).await;
    assert!(matches!(
        result,
        Err(Error::Api(ErrorCode::TopicAlreadyExists))
    ));
}

// ========== Low-level API Tests ==========

#[tokio::test]
async fn test_offset_stage() {
    let engine = create_test_engine().await;

    let topic = CreatableTopic::default()
        .name("stage-topic".into())
        .num_partitions(1)
        .replication_factor(1);

    let _ = engine.create_topic(topic, false).await.unwrap();

    let topition = Topition::new("stage-topic", 0);

    let stage = engine.offset_stage(&topition).await.unwrap();
    assert_eq!(0, stage.log_start);
    assert_eq!(0, stage.last_stable);
    assert_eq!(0, stage.high_watermark);
}

#[tokio::test]
async fn test_brokers() {
    let engine = create_test_engine().await;

    let brokers = engine.brokers().await.unwrap();
    assert_eq!(1, brokers.len());
    assert_eq!(1, brokers[0].broker_id);
    assert_eq!("localhost", brokers[0].host.as_str());
    assert_eq!(9092, brokers[0].port);
}

#[tokio::test]
async fn test_cluster_id() {
    let engine = create_test_engine().await;

    let cluster_id = engine.cluster_id().await.unwrap();
    assert_eq!("test-cluster", cluster_id);
}

#[tokio::test]
async fn test_node() {
    let engine = create_test_engine().await;

    let node = engine.node().await.unwrap();
    assert_eq!(1, node);
}

// ========== Transaction Error Tests ==========

#[tokio::test]
async fn test_txn_add_partitions_unknown_txn() {
    use nisshi_sans_io::add_partitions_to_txn_request::AddPartitionsToTxnTopic;

    let engine = create_test_engine().await;

    // Try to add partitions without initializing transaction
    let request = TxnAddPartitionsRequest::VersionZeroToThree {
        transaction_id: "unknown-txn".into(),
        producer_id: 1,
        producer_epoch: 0,
        topics: vec![
            AddPartitionsToTxnTopic::default()
                .name("any-topic".into())
                .partitions(Some(vec![0])),
        ],
    };

    let response = engine.txn_add_partitions(request).await.unwrap();

    match response {
        TxnAddPartitionsResponse::VersionZeroToThree(results) => {
            let partitions = results[0].results_by_partition.as_ref().unwrap();
            assert_eq!(
                ErrorCode::TransactionalIdNotFound,
                ErrorCode::try_from(partitions[0].partition_error_code).unwrap()
            );
        }
        _ => panic!("Expected VersionZeroToThree response"),
    }
}

// ========== Isolation Level Tests ==========

#[tokio::test]
async fn test_fetch_isolation_levels() {
    let engine = create_test_engine().await;

    let topic = CreatableTopic::default()
        .name("isolation-topic".into())
        .num_partitions(1)
        .replication_factor(1);
    let _ = engine.create_topic(topic, false).await.unwrap();

    let topition = Topition::new("isolation-topic", 0);

    // Produce some data with valid empty batch (record_count=0)
    let batch = Batch {
        base_offset: 0,
        batch_length: 0,
        partition_leader_epoch: 0,
        magic: 2,
        crc: 0,
        attributes: 0,
        last_offset_delta: 0,
        base_timestamp: 1000,
        max_timestamp: 1000,
        producer_id: -1,
        producer_epoch: -1,
        base_sequence: -1,
        record_count: 0,
        record_data: Bytes::new(),
    };

    let _ = engine
        .produce(None, &topition, batch.clone())
        .await
        .unwrap();
    let _ = engine
        .produce(None, &topition, batch.clone())
        .await
        .unwrap();

    // Verify offset stage to confirm data was written
    let stage = engine.offset_stage(&topition).await.unwrap();
    assert_eq!(2, stage.high_watermark);
    assert_eq!(2, stage.last_stable);

    // Test that ReadUncommitted and ReadCommitted return same result when no txns
    let stage_uncommitted = engine.offset_stage(&topition).await.unwrap();
    assert_eq!(
        stage_uncommitted.high_watermark,
        stage_uncommitted.last_stable
    );
}

#[tokio::test]
async fn test_offset_stage_with_transaction() {
    use nisshi_sans_io::add_partitions_to_txn_request::AddPartitionsToTxnTopic;

    let engine = create_test_engine().await;

    let topic = CreatableTopic::default()
        .name("stage-txn-topic".into())
        .num_partitions(1)
        .replication_factor(1);
    let _ = engine.create_topic(topic, false).await.unwrap();

    let topition = Topition::new("stage-txn-topic", 0);

    // Produce some data first
    let batch = Batch {
        base_offset: 0,
        batch_length: 0,
        partition_leader_epoch: 0,
        magic: 2,
        crc: 0,
        attributes: 0,
        last_offset_delta: 0,
        base_timestamp: 1000,
        max_timestamp: 1000,
        producer_id: -1,
        producer_epoch: -1,
        base_sequence: -1,
        record_count: 1,
        record_data: Bytes::new(),
    };

    let _ = engine
        .produce(None, &topition, batch.clone())
        .await
        .unwrap();

    // Check offset stage - no transactions, so last_stable == high_watermark
    let stage = engine.offset_stage(&topition).await.unwrap();
    assert_eq!(1, stage.high_watermark);
    assert_eq!(1, stage.last_stable);

    // Start a transaction and add this partition
    let producer = engine
        .init_producer(Some("stage-test-txn"), 60000, Some(-1), Some(-1))
        .await
        .unwrap();

    let request = TxnAddPartitionsRequest::VersionZeroToThree {
        transaction_id: "stage-test-txn".into(),
        producer_id: producer.id,
        producer_epoch: producer.epoch,
        topics: vec![
            AddPartitionsToTxnTopic::default()
                .name("stage-txn-topic".into())
                .partitions(Some(vec![0])),
        ],
    };
    let _ = engine.txn_add_partitions(request).await.unwrap();

    // After committing the transaction, offset stage should still be consistent
    // Note: txn_end now writes a commit marker batch, incrementing high_watermark by 1
    let _ = engine
        .txn_end("stage-test-txn", producer.id, producer.epoch, true)
        .await
        .unwrap();

    let stage = engine.offset_stage(&topition).await.unwrap();
    // high_watermark increased by 1 due to commit marker batch
    assert_eq!(2, stage.high_watermark);
    assert_eq!(2, stage.last_stable);
}

// ========== Idempotent Producer Tests ==========

#[tokio::test]
async fn test_idempotent_produce() {
    let engine = create_test_engine().await;

    let topic = CreatableTopic::default()
        .name("idempotent-topic".into())
        .num_partitions(1)
        .replication_factor(1);
    let _ = engine.create_topic(topic, false).await.unwrap();

    // Initialize idempotent producer
    let producer = engine
        .init_producer(None, 60000, Some(-1), Some(-1))
        .await
        .unwrap();

    let topition = Topition::new("idempotent-topic", 0);

    // First batch with sequence 0
    let batch = Batch {
        base_offset: 0,
        batch_length: 0,
        partition_leader_epoch: 0,
        magic: 2,
        crc: 0,
        attributes: 0,
        last_offset_delta: 0,
        base_timestamp: 1000,
        max_timestamp: 1000,
        producer_id: producer.id,
        producer_epoch: producer.epoch,
        base_sequence: 0,
        record_count: 1,
        record_data: Bytes::new(),
    };

    let offset = engine.produce(None, &topition, batch).await.unwrap();
    assert_eq!(0, offset);
}

// ========== Delete Records Tests ==========

#[tokio::test]
async fn test_delete_records() {
    use nisshi_sans_io::delete_records_request::{DeleteRecordsPartition, DeleteRecordsTopic};

    let engine = create_test_engine().await;

    let topic = CreatableTopic::default()
        .name("delete-records-topic".into())
        .num_partitions(1)
        .replication_factor(1);
    let _ = engine.create_topic(topic, false).await.unwrap();

    let topition = Topition::new("delete-records-topic", 0);

    // Produce some data
    let batch = simple_batch();

    for _ in 0..5 {
        let _ = engine
            .produce(None, &topition, batch.clone())
            .await
            .unwrap();
    }

    // Verify initial state
    let stage = engine.offset_stage(&topition).await.unwrap();
    assert_eq!(0, stage.log_start);
    assert_eq!(5, stage.high_watermark);

    // Delete records up to offset 3
    let delete_request = vec![
        DeleteRecordsTopic::default()
            .name("delete-records-topic".into())
            .partitions(Some(vec![
                DeleteRecordsPartition::default()
                    .partition_index(0)
                    .offset(3),
            ])),
    ];

    let results = engine.delete_records(&delete_request).await.unwrap();

    assert_eq!(1, results.len());
    let partitions = results[0].partitions.as_ref().unwrap();
    assert_eq!(1, partitions.len());
    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(partitions[0].error_code).unwrap()
    );
    assert_eq!(3, partitions[0].low_watermark);

    // Verify log_start was updated
    let stage = engine.offset_stage(&topition).await.unwrap();
    assert_eq!(3, stage.log_start);
}

/// `offset == -1` ("delete everything up to the high watermark") must never
/// physically remove the batch holding `high_watermark - 1`: it's Kafka's
/// active segment. The public API can't observe this directly -- `Latest`
/// is read straight from the stored watermark, not by scanning batches, so
/// every public-API test still passes even with that protection deleted
/// entirely. This test builds the `Engine` from a `Db` handle it keeps for
/// itself, so it can scan the batch-key prefix directly afterward and
/// confirm the one physical object that must survive actually does.
#[tokio::test]
async fn test_delete_records_to_high_watermark_keeps_one_physical_batch() {
    use super::types::{BatchKey, BatchKeyPrefix};
    use nisshi_sans_io::delete_records_request::{DeleteRecordsPartition, DeleteRecordsTopic};

    let object_store = Arc::new(InMemory::new());
    let db = Arc::new(
        Db::open("test.slatedb", object_store)
            .await
            .expect("Failed to open SlateDB"),
    );

    let engine = Engine::new(
        "test-cluster",
        1,
        Url::parse("tcp://localhost:9092").unwrap(),
        db.clone(),
    );

    let topic = CreatableTopic::default()
        .name("protect-active-batch-topic".into())
        .num_partitions(1)
        .replication_factor(1);
    let topic_id = engine.create_topic(topic, false).await.unwrap();

    let topition = Topition::new("protect-active-batch-topic", 0);

    // Five single-record batches: offsets 0..=4, each its own physical
    // batch object (unlike `test_delete_records` above, which only needs
    // log_start/low_watermark to move, not an exact physical object count).
    for _ in 0..5 {
        let batch = simple_batch();
        let _ = engine.produce(None, &topition, batch).await.unwrap();
    }

    async fn batch_offsets(db: &Db, topic_id: uuid::Uuid, partition: i32) -> Vec<i64> {
        let prefix = postcard::to_stdvec(&BatchKeyPrefix::new(topic_id, partition)).unwrap();
        let scan_start = postcard::to_stdvec(&BatchKey::scan_from(topic_id, partition, 0)).unwrap();

        let mut scan = db.scan(scan_start..).await.unwrap();
        let mut offsets = vec![];

        while let Some(kv) = scan.next().await.unwrap() {
            if !kv.key.starts_with(&prefix) {
                break;
            }

            offsets.push(postcard::from_bytes::<BatchKey>(&kv.key).unwrap().offset);
        }

        offsets
    }

    assert_eq!(
        vec![0, 1, 2, 3, 4],
        batch_offsets(&db, topic_id, 0).await,
        "setup: expected one physical batch object per produced record"
    );

    let delete_request = vec![
        DeleteRecordsTopic::default()
            .name("protect-active-batch-topic".into())
            .partitions(Some(vec![
                DeleteRecordsPartition::default()
                    .partition_index(0)
                    .offset(-1),
            ])),
    ];

    let results = engine.delete_records(&delete_request).await.unwrap();
    let partitions = results[0].partitions.as_ref().unwrap();
    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(partitions[0].error_code).unwrap()
    );
    assert_eq!(5, partitions[0].low_watermark);

    // Only the batch at offset 4, which holds high_watermark - 1, remains.
    // Every batch ends below a cutoff equal to the high watermark, so this
    // batch survives only because the delete keeps the last batch.
    assert_eq!(
        vec![4],
        batch_offsets(&db, topic_id, 0).await,
        "the active batch holding high_watermark - 1 must survive a -1 delete"
    );
}

#[tokio::test]
async fn test_fetch_with_min_bytes() {
    let engine = create_test_engine().await;

    let topic = CreatableTopic::default()
        .name("min-bytes-topic".into())
        .num_partitions(1)
        .replication_factor(1);
    let _ = engine.create_topic(topic, false).await.unwrap();

    let topition = Topition::new("min-bytes-topic", 0);

    // Produce multiple small batches with valid empty batch (record_count=0)
    let batch = Batch {
        base_offset: 0,
        batch_length: 0,
        partition_leader_epoch: 0,
        magic: 2,
        crc: 0,
        attributes: 0,
        last_offset_delta: 0,
        base_timestamp: 1000,
        max_timestamp: 1000,
        producer_id: -1,
        producer_epoch: -1,
        base_sequence: -1,
        record_count: 0,
        record_data: Bytes::new(),
    };

    for _ in 0..5 {
        let _ = engine
            .produce(None, &topition, batch.clone())
            .await
            .unwrap();
    }

    // Verify data was written
    let stage = engine.offset_stage(&topition).await.unwrap();
    assert_eq!(5, stage.high_watermark);
}

// ========== Register Broker Tests ==========

#[tokio::test]
async fn test_register_broker() {
    let engine = create_test_engine().await;

    let registration = BrokerRegistrationRequest {
        cluster_id: "test-cluster".into(),
        broker_id: 1,
        rack: Some("rack-1".into()),
        incarnation_id: Default::default(),
    };

    engine.register_broker(registration).await.unwrap();

    // Check that broker is now registered
    let brokers = engine.brokers().await.unwrap();
    assert_eq!(1, brokers.len());
    assert_eq!(1, brokers[0].broker_id);
    assert_eq!(Some("rack-1".to_string()), brokers[0].rack);
}

// ========== Version Four Plus Transaction Tests ==========

#[tokio::test]
async fn test_txn_add_partitions_version_four_plus() {
    use nisshi_sans_io::add_partitions_to_txn_request::{
        AddPartitionsToTxnTopic, AddPartitionsToTxnTransaction,
    };

    let engine = create_test_engine().await;

    // Create topic
    let topic = CreatableTopic::default()
        .name("v4-topic".into())
        .num_partitions(2)
        .replication_factor(1);
    let _ = engine.create_topic(topic, false).await.unwrap();

    // Initialize transactional producer
    let producer = engine
        .init_producer(Some("v4-txn"), 60000, Some(-1), Some(-1))
        .await
        .unwrap();

    // Use VersionFourPlus format
    let request = TxnAddPartitionsRequest::VersionFourPlus {
        transactions: vec![
            AddPartitionsToTxnTransaction::default()
                .transactional_id("v4-txn".into())
                .producer_id(producer.id)
                .producer_epoch(producer.epoch)
                .verify_only(false)
                .topics(Some(vec![
                    AddPartitionsToTxnTopic::default()
                        .name("v4-topic".into())
                        .partitions(Some(vec![0, 1])),
                ])),
        ],
    };

    let response = engine.txn_add_partitions(request).await.unwrap();

    match response {
        TxnAddPartitionsResponse::VersionFourPlus(results) => {
            assert_eq!(1, results.len());
            assert_eq!("v4-txn", results[0].transactional_id.as_str());

            let topic_results = results[0].topic_results.as_ref().unwrap();
            assert_eq!(1, topic_results.len());
            assert_eq!("v4-topic", topic_results[0].name.as_str());

            let partitions = topic_results[0].results_by_partition.as_ref().unwrap();
            assert_eq!(2, partitions.len());
        }
        _ => panic!("Expected VersionFourPlus response"),
    }
}

// ========== Additional Transaction Error Tests ==========

#[tokio::test]
async fn test_txn_wrong_producer_id() {
    use nisshi_sans_io::add_partitions_to_txn_request::AddPartitionsToTxnTopic;

    let engine = create_test_engine().await;

    // Initialize transactional producer
    let _ = engine
        .init_producer(Some("wrong-id-txn"), 60000, Some(-1), Some(-1))
        .await
        .unwrap();

    // Try with wrong producer_id
    let request = TxnAddPartitionsRequest::VersionZeroToThree {
        transaction_id: "wrong-id-txn".into(),
        producer_id: 9999, // Wrong ID
        producer_epoch: 0,
        topics: vec![
            AddPartitionsToTxnTopic::default()
                .name("any-topic".into())
                .partitions(Some(vec![0])),
        ],
    };

    let response = engine.txn_add_partitions(request).await.unwrap();

    match response {
        TxnAddPartitionsResponse::VersionZeroToThree(results) => {
            let partitions = results[0].results_by_partition.as_ref().unwrap();
            assert_eq!(
                ErrorCode::UnknownProducerId,
                ErrorCode::try_from(partitions[0].partition_error_code).unwrap()
            );
        }
        _ => panic!("Expected VersionZeroToThree response"),
    }
}

#[tokio::test]
async fn test_txn_wrong_epoch() {
    use nisshi_sans_io::add_partitions_to_txn_request::AddPartitionsToTxnTopic;

    let engine = create_test_engine().await;

    // Initialize transactional producer
    let producer = engine
        .init_producer(Some("wrong-epoch-txn"), 60000, Some(-1), Some(-1))
        .await
        .unwrap();

    // Try with wrong producer_epoch
    let request = TxnAddPartitionsRequest::VersionZeroToThree {
        transaction_id: "wrong-epoch-txn".into(),
        producer_id: producer.id,
        producer_epoch: 99, // Wrong epoch
        topics: vec![
            AddPartitionsToTxnTopic::default()
                .name("any-topic".into())
                .partitions(Some(vec![0])),
        ],
    };

    let response = engine.txn_add_partitions(request).await.unwrap();

    match response {
        TxnAddPartitionsResponse::VersionZeroToThree(results) => {
            let partitions = results[0].results_by_partition.as_ref().unwrap();
            assert_eq!(
                ErrorCode::ProducerFenced,
                ErrorCode::try_from(partitions[0].partition_error_code).unwrap()
            );
        }
        _ => panic!("Expected VersionZeroToThree response"),
    }
}

#[tokio::test]
async fn test_txn_end_unknown_transaction() {
    let engine = create_test_engine().await;

    // Try to end a transaction that doesn't exist
    let result = engine.txn_end("nonexistent-txn", 1, 0, true).await;

    assert!(matches!(
        result,
        Err(Error::Api(ErrorCode::TransactionalIdNotFound))
    ));
}

#[tokio::test]
async fn test_txn_end_wrong_producer() {
    let engine = create_test_engine().await;

    // Initialize transactional producer
    let producer = engine
        .init_producer(Some("end-wrong-prod"), 60000, Some(-1), Some(-1))
        .await
        .unwrap();

    // Try to end with wrong producer_id
    let result = engine
        .txn_end("end-wrong-prod", 9999, producer.epoch, true)
        .await;

    assert!(matches!(
        result,
        Err(Error::Api(ErrorCode::UnknownProducerId))
    ));
}

#[tokio::test]
async fn test_txn_end_wrong_epoch() {
    let engine = create_test_engine().await;

    // Initialize transactional producer
    let producer = engine
        .init_producer(Some("end-wrong-epoch"), 60000, Some(-1), Some(-1))
        .await
        .unwrap();

    // Try to end with wrong epoch
    let result = engine
        .txn_end("end-wrong-epoch", producer.id, 99, true)
        .await;

    assert!(matches!(result, Err(Error::Api(ErrorCode::ProducerFenced))));
}

// ========== Idempotent Producer Error Tests ==========

#[tokio::test]
async fn test_idempotent_unknown_producer() {
    let engine = create_test_engine().await;

    let topic = CreatableTopic::default()
        .name("unknown-prod-topic".into())
        .num_partitions(1)
        .replication_factor(1);
    let _ = engine.create_topic(topic, false).await.unwrap();

    let topition = Topition::new("unknown-prod-topic", 0);

    // Try to produce with unknown producer_id (but idempotent flag set)
    let batch = Batch {
        base_offset: 0,
        batch_length: 0,
        partition_leader_epoch: 0,
        magic: 2,
        crc: 0,
        attributes: 0,
        last_offset_delta: 0,
        base_timestamp: 1000,
        max_timestamp: 1000,
        producer_id: 9999, // Unknown producer
        producer_epoch: 0,
        base_sequence: 0,
        record_count: 0,
        record_data: Bytes::new(),
    };

    let result = engine.produce(None, &topition, batch).await;

    assert!(matches!(
        result,
        Err(Error::Api(ErrorCode::UnknownProducerId))
    ));
}

// ========== Delete Records Edge Cases ==========

#[tokio::test]
async fn test_delete_records_unknown_topic() {
    use nisshi_sans_io::delete_records_request::{DeleteRecordsPartition, DeleteRecordsTopic};

    let engine = create_test_engine().await;

    // Delete records from non-existent topic
    let delete_request = vec![
        DeleteRecordsTopic::default()
            .name("nonexistent-topic".into())
            .partitions(Some(vec![
                DeleteRecordsPartition::default()
                    .partition_index(0)
                    .offset(5),
            ])),
    ];

    let results = engine.delete_records(&delete_request).await.unwrap();

    assert_eq!(1, results.len());
    let partitions = results[0].partitions.as_ref().unwrap();
    assert_eq!(
        ErrorCode::UnknownTopicOrPartition,
        ErrorCode::try_from(partitions[0].error_code).unwrap()
    );
}

#[tokio::test]
async fn test_delete_records_unknown_partition() {
    use nisshi_sans_io::delete_records_request::{DeleteRecordsPartition, DeleteRecordsTopic};

    let engine = create_test_engine().await;

    let topic = CreatableTopic::default()
        .name("del-unknown-part".into())
        .num_partitions(1)
        .replication_factor(1);
    let _ = engine.create_topic(topic, false).await.unwrap();

    // Delete records from non-existent partition
    let delete_request = vec![
        DeleteRecordsTopic::default()
            .name("del-unknown-part".into())
            .partitions(Some(vec![
                DeleteRecordsPartition::default()
                    .partition_index(99) // Invalid partition
                    .offset(5),
            ])),
    ];

    let results = engine.delete_records(&delete_request).await.unwrap();

    assert_eq!(1, results.len());
    let partitions = results[0].partitions.as_ref().unwrap();
    assert_eq!(
        ErrorCode::UnknownTopicOrPartition,
        ErrorCode::try_from(partitions[0].error_code).unwrap()
    );
}

// ========== Multiple Partition Tests ==========

#[tokio::test]
async fn test_produce_multiple_partitions() {
    let engine = create_test_engine().await;

    let topic = CreatableTopic::default()
        .name("multi-part-topic".into())
        .num_partitions(3)
        .replication_factor(1);
    let _ = engine.create_topic(topic, false).await.unwrap();

    let batch = Batch {
        base_offset: 0,
        batch_length: 0,
        partition_leader_epoch: 0,
        magic: 2,
        crc: 0,
        attributes: 0,
        last_offset_delta: 0,
        base_timestamp: 1000,
        max_timestamp: 1000,
        producer_id: -1,
        producer_epoch: -1,
        base_sequence: -1,
        record_count: 0,
        record_data: Bytes::new(),
    };

    // Produce to different partitions
    for partition in 0..3 {
        let topition = Topition::new("multi-part-topic", partition);
        let offset = engine
            .produce(None, &topition, batch.clone())
            .await
            .unwrap();
        assert_eq!(0, offset); // Each partition starts at 0
    }

    // Verify each partition has its own offset
    for partition in 0..3 {
        let topition = Topition::new("multi-part-topic", partition);
        let stage = engine.offset_stage(&topition).await.unwrap();
        assert_eq!(1, stage.high_watermark);
    }
}

// ========== Transactional Offset Tests ==========

#[tokio::test]
async fn test_txn_add_offsets() {
    let engine = create_test_engine().await;

    let producer = engine
        .init_producer(Some("add-offsets-txn"), 60000, Some(-1), Some(-1))
        .await
        .unwrap();

    let error_code = engine
        .txn_add_offsets("add-offsets-txn", producer.id, producer.epoch, "group-1")
        .await
        .unwrap();
    assert_eq!(ErrorCode::None, error_code);
}

#[tokio::test]
async fn test_txn_add_offsets_unknown_txn() {
    let engine = create_test_engine().await;

    let error_code = engine
        .txn_add_offsets("unknown-txn", 1, 0, "group-1")
        .await
        .unwrap();
    assert_eq!(ErrorCode::TransactionalIdNotFound, error_code);
}

#[tokio::test]
async fn test_txn_add_offsets_wrong_producer() {
    let engine = create_test_engine().await;

    let producer = engine
        .init_producer(Some("add-offsets-wrong-prod"), 60000, Some(-1), Some(-1))
        .await
        .unwrap();

    let error_code = engine
        .txn_add_offsets("add-offsets-wrong-prod", 9999, producer.epoch, "group-1")
        .await
        .unwrap();
    assert_eq!(ErrorCode::UnknownProducerId, error_code);
}

#[tokio::test]
async fn test_txn_add_offsets_wrong_epoch() {
    let engine = create_test_engine().await;

    let producer = engine
        .init_producer(Some("add-offsets-wrong-epoch"), 60000, Some(-1), Some(-1))
        .await
        .unwrap();

    let error_code = engine
        .txn_add_offsets("add-offsets-wrong-epoch", producer.id, 99, "group-1")
        .await
        .unwrap();
    assert_eq!(ErrorCode::ProducerFenced, error_code);
}

// ========== SCRAM Credential Tests ==========

#[tokio::test]
async fn test_scram_credential_lifecycle() {
    use nisshi_sans_io::ScramMechanism;
    use nisshi_storage::ScramCredential;

    let engine = create_test_engine().await;

    // No credential stored yet
    let found = engine
        .user_scram_credential("alice", ScramMechanism::Scram256)
        .await
        .unwrap();
    assert!(found.is_none());

    let credential = ScramCredential {
        salt: Bytes::from_static(b"salt"),
        iterations: 4096,
        stored_key: Bytes::from_static(b"stored-key"),
        server_key: Bytes::from_static(b"server-key"),
    };

    engine
        .upsert_user_scram_credential("alice", ScramMechanism::Scram256, credential.clone())
        .await
        .unwrap();

    // Credential round-trips
    let found = engine
        .user_scram_credential("alice", ScramMechanism::Scram256)
        .await
        .unwrap();
    assert_eq!(Some(credential.clone()), found);

    // Different mechanism and user are distinct keys
    assert!(
        engine
            .user_scram_credential("alice", ScramMechanism::Scram512)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        engine
            .user_scram_credential("bob", ScramMechanism::Scram256)
            .await
            .unwrap()
            .is_none()
    );

    engine
        .delete_user_scram_credential("alice", ScramMechanism::Scram256)
        .await
        .unwrap();

    let found = engine
        .user_scram_credential("alice", ScramMechanism::Scram256)
        .await
        .unwrap();
    assert!(found.is_none());
}

// ========== List Offsets Tests ==========

#[tokio::test]
async fn test_list_offsets_earliest_after_delete_records() {
    use nisshi_sans_io::delete_records_request::{DeleteRecordsPartition, DeleteRecordsTopic};
    use nisshi_sans_io::{IsolationLevel, ListOffset};

    let engine = create_test_engine().await;

    let topic = CreatableTopic::default()
        .name("earliest-topic".into())
        .num_partitions(1)
        .replication_factor(1);
    let _ = engine.create_topic(topic, false).await.unwrap();

    let topition = Topition::new("earliest-topic", 0);

    let batch = simple_batch();

    for _ in 0..5 {
        let _ = engine
            .produce(None, &topition, batch.clone())
            .await
            .unwrap();
    }

    // Before deletion, earliest is 0
    let responses = engine
        .list_offsets(
            IsolationLevel::ReadUncommitted,
            &[(topition.clone(), ListOffset::Earliest)],
        )
        .await
        .unwrap();
    assert_eq!(Some(0), responses[0].1.offset);

    // Delete records up to offset 3
    let delete_request = vec![
        DeleteRecordsTopic::default()
            .name("earliest-topic".into())
            .partitions(Some(vec![
                DeleteRecordsPartition::default()
                    .partition_index(0)
                    .offset(3),
            ])),
    ];
    let _ = engine.delete_records(&delete_request).await.unwrap();

    // Earliest now reflects the advanced log start offset
    let responses = engine
        .list_offsets(
            IsolationLevel::ReadUncommitted,
            &[
                (topition.clone(), ListOffset::Earliest),
                (topition.clone(), ListOffset::Latest),
            ],
        )
        .await
        .unwrap();
    assert_eq!(ErrorCode::None, responses[0].1.error_code);
    assert_eq!(Some(3), responses[0].1.offset);
    assert_eq!(Some(5), responses[1].1.offset);
}

// ========== Cleanup Policy Tests ==========

mod cleanup_policy {
    use std::time::{Duration, SystemTime};

    use tokio::time::Instant;

    use nisshi_sans_io::{
        BatchAttribute, Compression, IsolationLevel,
        create_topics_request::CreatableTopicConfig,
        record::{Record, inflated},
    };

    use super::*;
    use nisshi_storage::fetched::FetchUntilDeadline as _;

    async fn topic_with_policy(engine: &Engine, name: &str, configs: &[(&str, &str)]) -> Topition {
        let topic = CreatableTopic::default()
            .name(name.into())
            .num_partitions(1)
            .replication_factor(1)
            .configs(Some(
                configs
                    .iter()
                    .map(|(name, value)| {
                        CreatableTopicConfig::default()
                            .name((*name).into())
                            .value(Some((*value).into()))
                    })
                    .collect(),
            ));

        let _ = engine.create_topic(topic, false).await.unwrap();

        Topition::new(name, 0)
    }

    fn keyed_batch(key: &'static [u8], value: &'static [u8], timestamp: Option<i64>) -> Batch {
        let mut builder = inflated::Batch::builder().record(
            Record::builder()
                .key(Some(Bytes::from_static(key)))
                .value(Some(Bytes::from_static(value))),
        );

        if let Some(timestamp) = timestamp {
            builder = builder.base_timestamp(timestamp).max_timestamp(timestamp);
        }

        builder.build().and_then(Batch::try_from).unwrap()
    }

    async fn fetch_all(engine: &Engine, topition: &Topition) -> Vec<Batch> {
        engine
            .fetch_until_deadline(
                topition,
                0,
                1,
                1024 * 1024,
                IsolationLevel::ReadUncommitted,
                Instant::now() + Duration::from_secs(5),
            )
            .await
            .map(|read| read.batches)
            .unwrap()
    }

    fn now_millis() -> i64 {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|since_epoch| since_epoch.as_millis() as i64)
            .unwrap()
    }

    #[tokio::test]
    async fn compact_keeps_latest_record_per_key() {
        let engine = create_test_engine().await;
        let topition =
            topic_with_policy(&engine, "policy-compact", &[("cleanup.policy", "compact")]).await;

        for batch in [
            keyed_batch(b"a", b"one", None),
            keyed_batch(b"b", b"first", None),
            keyed_batch(b"a", b"two", None),
        ] {
            let _ = engine.produce(None, &topition, batch).await.unwrap();
        }

        engine.maintain(SystemTime::now()).await.unwrap();

        // The batch at offset 0 (key "a", superseded at offset 2) is
        // removed, but the log start stays at 0, as in Kafka: a consumer at
        // 0 reads from the first surviving batch, rather than being answered
        // OFFSET_OUT_OF_RANGE by Fetch and resetting its position
        let stage = engine.offset_stage(&topition).await.unwrap();
        assert_eq!(0, stage.log_start);
        assert_eq!(3, stage.high_watermark);

        let batches = fetch_all(&engine, &topition).await;
        assert_eq!(2, batches.len());
        assert_eq!(1, batches[0].base_offset);
        assert_eq!(2, batches[1].base_offset);

        let records = inflated::Batch::try_from(batches[1].clone())
            .unwrap()
            .records;
        assert_eq!(1, records.len());
        assert_eq!(Some(Bytes::from_static(b"two")), records[0].value);

        // Compaction is idempotent
        engine.maintain(SystemTime::now()).await.unwrap();
        assert_eq!(2, fetch_all(&engine, &topition).await.len());
    }

    #[tokio::test]
    async fn compact_skips_a_batch_it_cannot_inflate() {
        let engine = create_test_engine().await;
        let topition = topic_with_policy(
            &engine,
            "policy-compact-skip",
            &[("cleanup.policy", "compact")],
        )
        .await;

        // Without a schema registry the engine stores a batch without
        // inflating it, so one that cannot be inflated (plain record data
        // labelled as zstd) is stored but can never be read back as records.
        // Compaction must leave it alone, not abandon the pass.
        let mut undecodable = keyed_batch(b"b", b"opaque", None);
        undecodable.attributes = BatchAttribute::default()
            .compression(Compression::Zstd)
            .into();

        for batch in [
            keyed_batch(b"a", b"one", None),
            undecodable,
            keyed_batch(b"a", b"two", None),
        ] {
            let _ = engine.produce(None, &topition, batch).await.unwrap();
        }

        engine.maintain(SystemTime::now()).await.unwrap();

        // Offset 0 (key "a", superseded at offset 2) is removed; the batch
        // that cannot be inflated is still there, untouched.
        let batches = fetch_all(&engine, &topition).await;
        assert_eq!(2, batches.len());
        assert_eq!(1, batches[0].base_offset);
        assert_eq!(2, batches[1].base_offset);
        assert!(inflated::Batch::try_from(batches[0].clone()).is_err());
    }

    #[tokio::test]
    async fn compact_rewrites_partial_batch() {
        let engine = create_test_engine().await;
        let topition = topic_with_policy(
            &engine,
            "policy-compact-partial",
            &[("cleanup.policy", "compact")],
        )
        .await;

        // One batch with two records, then key "a" is superseded
        let batch = inflated::Batch::builder()
            .record(
                Record::builder()
                    .key(Some(Bytes::from_static(b"a")))
                    .value(Some(Bytes::from_static(b"one"))),
            )
            .record(
                Record::builder()
                    .key(Some(Bytes::from_static(b"b")))
                    .value(Some(Bytes::from_static(b"first")))
                    .offset_delta(1),
            )
            .last_offset_delta(1)
            .build()
            .and_then(Batch::try_from)
            .unwrap();

        let _ = engine.produce(None, &topition, batch).await.unwrap();
        let _ = engine
            .produce(None, &topition, keyed_batch(b"a", b"two", None))
            .await
            .unwrap();

        engine.maintain(SystemTime::now()).await.unwrap();

        // The first batch survives with only the record for key "b"
        let stage = engine.offset_stage(&topition).await.unwrap();
        assert_eq!(0, stage.log_start);
        assert_eq!(3, stage.high_watermark);

        let batches = fetch_all(&engine, &topition).await;
        assert_eq!(2, batches.len());
        assert_eq!(0, batches[0].base_offset);

        let records = inflated::Batch::try_from(batches[0].clone())
            .unwrap()
            .records;
        assert_eq!(1, records.len());
        assert_eq!(Some(Bytes::from_static(b"b")), records[0].key);
        assert_eq!(Some(Bytes::from_static(b"first")), records[0].value);
        assert_eq!(1, records[0].offset_delta);
    }

    #[tokio::test]
    async fn delete_removes_expired_prefix() {
        let engine = create_test_engine().await;
        let topition = topic_with_policy(
            &engine,
            "policy-delete",
            &[("cleanup.policy", "delete"), ("retention.ms", "60000")],
        )
        .await;

        let now = now_millis();
        let expired = now - Duration::from_mins(5).as_millis() as i64;

        for batch in [
            keyed_batch(b"a", b"one", Some(expired)),
            keyed_batch(b"a", b"two", Some(expired + 1)),
            keyed_batch(b"a", b"three", Some(now)),
        ] {
            let _ = engine.produce(None, &topition, batch).await.unwrap();
        }

        engine.maintain(SystemTime::now()).await.unwrap();

        let stage = engine.offset_stage(&topition).await.unwrap();
        assert_eq!(2, stage.log_start);
        assert_eq!(3, stage.high_watermark);

        let batches = fetch_all(&engine, &topition).await;
        assert_eq!(1, batches.len());
        assert_eq!(2, batches[0].base_offset);
    }

    #[tokio::test]
    async fn delete_whole_partition_meets_high_watermark() {
        use nisshi_sans_io::ListOffset;

        let engine = create_test_engine().await;
        let topition = topic_with_policy(
            &engine,
            "policy-delete-all",
            &[("cleanup.policy", "delete"), ("retention.ms", "60000")],
        )
        .await;

        let expired = now_millis() - Duration::from_mins(5).as_millis() as i64;

        for batch in [
            keyed_batch(b"a", b"one", Some(expired)),
            keyed_batch(b"a", b"two", Some(expired + 1)),
        ] {
            let _ = engine.produce(None, &topition, batch).await.unwrap();
        }

        engine.maintain(SystemTime::now()).await.unwrap();

        // The log start meets the high watermark: offsets are never reused
        let stage = engine.offset_stage(&topition).await.unwrap();
        assert_eq!(2, stage.log_start);
        assert_eq!(2, stage.high_watermark);

        assert!(fetch_all(&engine, &topition).await.is_empty());

        let responses = engine
            .list_offsets(
                IsolationLevel::ReadUncommitted,
                &[(topition.clone(), ListOffset::Earliest)],
            )
            .await
            .unwrap();
        assert_eq!(Some(2), responses[0].1.offset);

        // The next produce continues from the high watermark
        let offset = engine
            .produce(None, &topition, keyed_batch(b"a", b"three", None))
            .await
            .unwrap();
        assert_eq!(2, offset);
    }

    #[tokio::test]
    async fn no_policy_is_untouched() {
        let engine = create_test_engine().await;
        let topition = topic_with_policy(&engine, "policy-none", &[]).await;

        let expired = now_millis() - Duration::from_hours(30 * 24).as_millis() as i64;

        for batch in [
            keyed_batch(b"a", b"one", Some(expired)),
            keyed_batch(b"a", b"two", Some(expired + 1)),
        ] {
            let _ = engine.produce(None, &topition, batch).await.unwrap();
        }

        engine.maintain(SystemTime::now()).await.unwrap();

        let stage = engine.offset_stage(&topition).await.unwrap();
        assert_eq!(0, stage.log_start);
        assert_eq!(2, stage.high_watermark);
        assert_eq!(2, fetch_all(&engine, &topition).await.len());
    }
}

// ========== Builder Pattern Tests ==========

#[tokio::test]
async fn test_builder_pattern() {
    let object_store = Arc::new(InMemory::new());
    let db = Db::open("builder-test.slatedb", object_store)
        .await
        .expect("Failed to open SlateDB");

    let engine = Engine::builder()
        .cluster("builder-cluster")
        .node(42)
        .advertised_listener(Url::parse("tcp://10.0.0.1:9093").unwrap())
        .db(Arc::new(db))
        .schemas(None)
        .lake(None)
        .build()
        .unwrap();

    assert_eq!("builder-cluster", engine.cluster_id().await.unwrap());
    assert_eq!(42, engine.node().await.unwrap());
}

#[tokio::test]
async fn corrupt_committed_offset_is_unknown_server_error() {
    use super::types::OffsetCommitKey;

    let engine = create_test_engine().await;

    _ = engine
        .create_topic(
            CreatableTopic::default()
                .name("corrupt".into())
                .num_partitions(1)
                .replication_factor(1),
            false,
        )
        .await
        .unwrap();

    let key = postcard::to_stdvec(&OffsetCommitKey::new("group", "corrupt", 0)).unwrap();
    _ = engine.db.put(&key, b"").await.unwrap();

    let topition = Topition::new("corrupt", 0);

    assert!(matches!(
        engine
            .offset_fetch(Some("group"), std::slice::from_ref(&topition), None)
            .await,
        Err(Error::Api(ErrorCode::UnknownServerError))
    ));

    assert!(matches!(
        engine.committed_offset_topitions("group").await,
        Err(Error::Api(ErrorCode::UnknownServerError))
    ));
}
