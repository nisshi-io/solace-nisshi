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

//! Tests of the default [`Storage::produce_all`].

use nisshi_sans_io::{
    ErrorCode,
    record::{deflated, inflated},
};
use std::assert_matches;

use crate::{
    Error, ProduceEntry, Result, Storage, Topition,
    service::counting_storage::{CountingStorage, Listing},
};

/// An entry for `partition` of the topic `abc`. The `producer_id` of its batch tells it apart from
/// the other entries.
fn entry(partition: i32, producer_id: i64, inflated: Option<inflated::Batch>) -> ProduceEntry {
    ProduceEntry {
        topition: Topition::new("abc", partition),
        deflated: deflated::Batch {
            producer_id,
            ..deflated::Batch::default()
        },
        inflated,
    }
}

fn storage() -> CountingStorage {
    CountingStorage::new(Listing::default(), &["abc"])
}

#[tokio::test]
async fn a_decoded_batch_selects_produce_inflated_and_none_selects_produce() {
    let storage = storage();

    let results = storage
        .produce_all(
            Some("transaction"),
            vec![
                entry(0, 10, Some(inflated::Batch::default())),
                entry(1, 11, None),
            ],
        )
        .await;

    assert_eq!(
        vec![0, 1],
        results.into_iter().collect::<Result<Vec<_>>>().unwrap()
    );

    let produced = storage.produced();
    assert_eq!(2, produced.len());
    assert_matches!(produced[0].inflated, Some(_));
    assert_matches!(produced[1].inflated, None);
}

#[tokio::test]
async fn every_entry_is_produced_with_the_transaction_id_of_the_call() {
    let in_transaction = storage();
    _ = in_transaction
        .produce_all(
            Some("transaction"),
            vec![entry(0, 10, None), entry(1, 11, None)],
        )
        .await;

    let outside_transaction = storage();
    _ = outside_transaction
        .produce_all(None, vec![entry(0, 10, None), entry(1, 11, None)])
        .await;

    assert_eq!(
        vec![Some("transaction".to_owned()); 2],
        in_transaction
            .produced()
            .into_iter()
            .map(|produced| produced.transaction_id)
            .collect::<Vec<_>>()
    );
    assert_eq!(
        vec![None; 2],
        outside_transaction
            .produced()
            .into_iter()
            .map(|produced| produced.transaction_id)
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn the_results_and_the_writes_follow_the_order_of_the_entries() {
    let storage = storage();

    // The partitions are not in ascending order, so that sorting or reversing the entries
    // changes the order of the results.
    let results = storage
        .produce_all(
            None,
            vec![entry(2, 10, None), entry(0, 11, None), entry(1, 12, None)],
        )
        .await;

    // The storage answers a produce to a partition with that partition's number.
    assert_eq!(
        vec![2, 0, 1],
        results.into_iter().collect::<Result<Vec<_>>>().unwrap()
    );

    assert_eq!(
        vec![
            (Topition::new("abc", 2), 10),
            (Topition::new("abc", 0), 11),
            (Topition::new("abc", 1), 12),
        ],
        storage
            .produced()
            .into_iter()
            .map(|produced| (produced.topition, produced.deflated.producer_id))
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn two_entries_for_one_partition_are_written_in_the_order_of_the_entries() {
    let storage = storage();

    let results = storage
        .produce_all(None, vec![entry(0, 10, None), entry(0, 11, None)])
        .await;

    assert_eq!(2, results.len());
    assert_eq!(
        vec![10, 11],
        storage
            .produced()
            .into_iter()
            .map(|produced| produced.deflated.producer_id)
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn a_failed_entry_fails_alone_wherever_it_is() {
    // The failing entry is the first, the middle and the last in turn, so that a result list that
    // stops at the failure, or that fails the entries after it, fails one of the three runs.
    for failing in 0..3 {
        let storage = storage().with_failing_produce(
            Topition::new("abc", failing),
            Error::Api(ErrorCode::KafkaStorageError),
        );

        let results = storage
            .produce_all(
                None,
                vec![entry(0, 10, None), entry(1, 11, None), entry(2, 12, None)],
            )
            .await;

        assert_eq!(3, results.len());

        for (partition, result) in (0..3).zip(&results) {
            if partition == failing {
                assert_matches!(result, Err(Error::Api(ErrorCode::KafkaStorageError)));
            } else {
                assert_matches!(result, Ok(offset) if *offset == i64::from(partition));
            }
        }

        assert_eq!(
            (0..3)
                .filter(|partition| *partition != failing)
                .map(|partition| Topition::new("abc", partition))
                .collect::<Vec<_>>(),
            storage
                .produced()
                .into_iter()
                .map(|produced| produced.topition)
                .collect::<Vec<_>>()
        );
    }
}

#[tokio::test]
async fn no_entries_produce_nothing() {
    let storage = storage();

    let results = storage.produce_all(None, Vec::new()).await;

    assert!(results.is_empty());
    assert!(storage.produced().is_empty());
}
