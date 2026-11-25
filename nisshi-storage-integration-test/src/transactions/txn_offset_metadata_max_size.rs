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

//! `TxnOffsetCommit` rejects the commit of a partition whose metadata is longer than 4096
//! characters with `OFFSET_METADATA_TOO_LARGE`, and stores the other partitions of the request.
//!
//! Kafka counts the metadata's length in UTF-16 code units, against `offset.metadata.max.bytes`
//! (4096 by default).

use crate::metadata_max_size::{self, MAX_METADATA_LENGTH, OFFSET};
use crate::random;
use nisshi_broker::Result;
use nisshi_sans_io::{
    ErrorCode, RequestInput, TxnOffsetCommitRequest,
    txn_offset_commit_request::{TxnOffsetCommitRequestPartition, TxnOffsetCommitRequestTopic},
};
use nisshi_storage::{Storage, TxnOffsetCommitService};
use rama::{Service as _, extensions::Extensions};

pub async fn txn_offset_commit_rejects_metadata_over_the_limit<G>(
    cluster_id: impl Into<String>,
    broker_id: i32,
    storage: G,
) -> Result<()>
where
    G: Storage + Clone,
{
    let topic = metadata_max_size::create_topic(cluster_id, broker_id, &storage).await?;
    let group_id = random::alphanumeric_string(15);
    let transaction_id = random::alphanumeric_string(15);

    let producer = storage
        .init_producer(Some(transaction_id.as_str()), 10_000, Some(-1), Some(-1))
        .await?;

    let response = TxnOffsetCommitService {
        storage: storage.clone(),
    }
    .serve(RequestInput {
        request: TxnOffsetCommitRequest::default()
            .transactional_id(transaction_id.clone())
            .group_id(group_id.clone())
            .producer_id(producer.id)
            .producer_epoch(producer.epoch)
            .generation_id(Some(-1))
            .member_id(Some(String::new()))
            .group_instance_id(None)
            .topics(Some(vec![
                TxnOffsetCommitRequestTopic::default()
                    .name(topic.clone())
                    .partitions(Some(vec![
                        TxnOffsetCommitRequestPartition::default()
                            .partition_index(0)
                            .committed_offset(OFFSET)
                            .committed_leader_epoch(Some(-1))
                            .committed_metadata(Some(metadata_max_size::metadata_of_length(
                                MAX_METADATA_LENGTH + 1,
                            ))),
                        TxnOffsetCommitRequestPartition::default()
                            .partition_index(1)
                            .committed_offset(OFFSET)
                            .committed_leader_epoch(Some(-1))
                            .committed_metadata(Some(metadata_max_size::metadata_of_length(
                                MAX_METADATA_LENGTH,
                            ))),
                    ])),
            ])),
        extensions: Extensions::default(),
    })
    .await?;

    let mut outcome = response
        .topics
        .unwrap_or_default()
        .iter()
        .flat_map(|topic| topic.partitions.iter().flatten())
        .map(|partition| {
            ErrorCode::try_from(partition.error_code)
                .map(|error_code| (partition.partition_index, error_code))
        })
        .collect::<Result<Vec<_>, _>>()?;
    outcome.sort();

    assert_eq!(
        vec![(0, ErrorCode::OffsetMetadataTooLarge), (1, ErrorCode::None)],
        outcome
    );

    assert_eq!(
        ErrorCode::None,
        storage
            .txn_end(&transaction_id, producer.id, producer.epoch, true)
            .await?
    );

    assert_eq!(
        -1,
        metadata_max_size::committed_offset(&storage, &group_id, &topic, 0).await?
    );
    assert_eq!(
        OFFSET,
        metadata_max_size::committed_offset(&storage, &group_id, &topic, 1).await?
    );

    Ok(())
}
