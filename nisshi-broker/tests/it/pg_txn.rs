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

#[cfg(feature = "postgres")]
use bytes::Bytes;

#[cfg(feature = "postgres")]
use crate::common::{
    CLIENT_ID, COOPERATIVE_STICKY, PROTOCOL_TYPE, RANGE, StorageType, alphanumeric_string,
    heartbeat, init_tracing, join_group, offset_fetch, register_broker, storage_container,
    sync_group,
};

#[cfg(feature = "postgres")]
use rand::{prelude::*, rng};

#[cfg(feature = "postgres")]
use nisshi_broker::{Result, coordinator::group::administrator::Controller};

#[cfg(feature = "postgres")]
use nisshi_sans_io::{
    BatchAttribute, ErrorCode, HeartbeatResponse, IsolationLevel, OffsetFetchResponse,
    add_partitions_to_txn_request::AddPartitionsToTxnTopic,
    create_topics_request::CreatableTopic,
    join_group_request::JoinGroupRequestProtocol,
    join_group_response::JoinGroupResponseMember,
    offset_fetch_request::OffsetFetchRequestTopic,
    offset_fetch_response::{OffsetFetchResponsePartition, OffsetFetchResponseTopic},
    record::{Record, inflated},
    sync_group_request::SyncGroupRequestAssignment,
    txn_offset_commit_request::{TxnOffsetCommitRequestPartition, TxnOffsetCommitRequestTopic},
    txn_offset_commit_response::{TxnOffsetCommitResponsePartition, TxnOffsetCommitResponseTopic},
};

#[cfg(feature = "postgres")]
use nisshi_storage::{Storage, Topition, TxnAddPartitionsRequest, TxnOffsetCommitRequest};

#[cfg(feature = "postgres")]
use tracing::debug;

#[cfg(feature = "postgres")]
use url::Url;

#[cfg(feature = "postgres")]
use uuid::Uuid;

#[tokio::test]
#[cfg(feature = "postgres")]
async fn simple_txn_commit() -> Result<()> {
    use nisshi_sans_io::ListOffset;

    let _guard = init_tracing()?;

    let mut rng = rng();

    let cluster_id = Uuid::now_v7();
    let broker_id = rng.random_range(0..i32::MAX);

    let sc = storage_container(
        StorageType::Postgres,
        cluster_id,
        broker_id,
        Url::parse("tcp://127.0.0.1/")?,
        None,
    )
    .await?;

    register_broker(cluster_id, broker_id, &sc).await?;

    let input_topic_name: String = alphanumeric_string(15);
    debug!(?input_topic_name);

    let num_partitions = 6;
    let replication_factor = 0;
    let assignments = Some([].into());
    let configs = Some([].into());

    // create input topic
    //
    let input_topic_id = sc
        .create_topic(
            CreatableTopic::default()
                .name(input_topic_name.clone())
                .num_partitions(num_partitions)
                .replication_factor(replication_factor)
                .assignments(assignments.clone())
                .configs(configs.clone()),
            false,
        )
        .await?;
    debug!(?input_topic_id);

    let input_partition_index = rng.random_range(0..num_partitions);
    let input_topition = Topition::new(input_topic_name.clone(), input_partition_index);
    let records = 6;

    // populate the input topic with some records
    //
    for n in 0..records {
        let value = format!("Lorem ipsum dolor sit amet: {n}");

        let batch = inflated::Batch::builder()
            .record(Record::builder().value(Bytes::copy_from_slice(value.as_bytes()).into()))
            .build()
            .and_then(TryInto::try_into)
            .inspect(|deflated| debug!(?deflated))?;

        _ = sc
            .produce(None, &input_topition, batch)
            .await
            .inspect(|offset| debug!(?offset))?;
    }

    let output_topic_name: String = alphanumeric_string(15);
    debug!(?output_topic_name);

    // create output topic
    //
    let output_topic_id = sc
        .create_topic(
            CreatableTopic::default()
                .name(output_topic_name.clone())
                .num_partitions(num_partitions)
                .replication_factor(replication_factor)
                .assignments(assignments)
                .configs(configs),
            false,
        )
        .await?;
    debug!(?output_topic_id);

    // consumer group controller
    //
    let mut controller = Controller::with_storage(sc.clone())?;

    let session_timeout_ms = 45_000;
    let rebalance_timeout_ms = Some(300_000);
    let group_instance_id = None;
    let reason = None;

    let group_id: String = alphanumeric_string(15);
    debug!(?group_id);

    let first_member_range_meta = Bytes::from_static(b"first_member_range_meta_01");
    let first_member_sticky_meta = Bytes::from_static(b"first_member_sticky_meta_01");

    let protocols = [
        JoinGroupRequestProtocol::default()
            .name(RANGE.into())
            .metadata(first_member_range_meta.clone()),
        JoinGroupRequestProtocol::default()
            .name(COOPERATIVE_STICKY.into())
            .metadata(first_member_sticky_meta),
    ];

    // join group without a member id
    //
    let member_id_required = join_group(
        &mut controller,
        Some(CLIENT_ID),
        group_id.as_str(),
        session_timeout_ms,
        rebalance_timeout_ms,
        "",
        group_instance_id,
        PROTOCOL_TYPE,
        Some(&protocols[..]),
        reason,
    )
    .await?;

    // join rejected as member id is required
    //
    assert_eq!(
        ErrorCode::MemberIdRequired,
        ErrorCode::try_from(member_id_required.error_code)?
    );
    // Kafka leaves the protocol type null in this response.
    assert_eq!(None, member_id_required.protocol_type);
    assert_eq!(Some("".into()), member_id_required.protocol_name);
    assert!(member_id_required.leader.is_empty());
    assert!(member_id_required.member_id.starts_with(CLIENT_ID));
    assert_eq!(0, member_id_required.members.unwrap().len());

    // join with the supplied member id
    //
    let join_response = join_group(
        &mut controller,
        Some(CLIENT_ID),
        group_id.as_str(),
        session_timeout_ms,
        rebalance_timeout_ms,
        member_id_required.member_id.as_str(),
        group_instance_id,
        PROTOCOL_TYPE,
        Some(&protocols[..]),
        reason,
    )
    .await?;

    // join accepted as leader
    //
    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(join_response.error_code)?
    );
    assert_eq!(1, join_response.generation_id);
    assert_eq!(Some(PROTOCOL_TYPE.into()), join_response.protocol_type);
    assert_eq!(Some(RANGE.into()), join_response.protocol_name);
    assert_eq!(member_id_required.member_id.as_str(), join_response.leader);
    assert_eq!(
        Some(vec![
            JoinGroupResponseMember::default()
                .member_id(member_id_required.member_id.clone())
                .group_instance_id(None)
                .metadata(first_member_range_meta.clone())
        ]),
        join_response.members
    );

    let member_id = member_id_required.member_id.clone();
    debug!(?member_id);

    let first_member_assignment_01 = Bytes::from_static(b"assignment_01");

    let assignments = [SyncGroupRequestAssignment::default()
        .member_id(member_id.clone())
        .assignment(first_member_assignment_01.clone())];

    // sync to form the group
    //
    let sync_response = sync_group(
        &mut controller,
        group_id.as_str(),
        join_response.generation_id,
        member_id.as_str(),
        group_instance_id,
        PROTOCOL_TYPE,
        RANGE,
        &assignments,
    )
    .await?;
    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(sync_response.error_code)?
    );
    assert_eq!(PROTOCOL_TYPE, sync_response.protocol_type.unwrap());
    assert_eq!(RANGE, sync_response.protocol_name.unwrap());
    assert_eq!(first_member_assignment_01, sync_response.assignment);

    // heartbeat establishing leadership of current generation
    //
    let HeartbeatResponse { error_code, .. } = heartbeat(
        &mut controller,
        group_id.as_str(),
        join_response.generation_id,
        &member_id,
        group_instance_id,
    )
    .await?;
    assert_eq!(ErrorCode::None, ErrorCode::try_from(error_code)?);

    let transaction_id: String = alphanumeric_string(10);
    debug!(?transaction_id);

    let transaction_timeout_ms = 10_000;

    // initialise producer with a transaction
    //
    let txn_producer = sc
        .init_producer(
            Some(transaction_id.as_str()),
            transaction_timeout_ms,
            Some(-1),
            Some(-1),
        )
        .await?;
    debug!(?txn_producer);

    // add all output topic partitions to the transaction
    //
    let txn_add_partitions = TxnAddPartitionsRequest::VersionZeroToThree {
        transaction_id: transaction_id.clone(),
        producer_id: txn_producer.id,
        producer_epoch: txn_producer.epoch,
        topics: vec![
            AddPartitionsToTxnTopic::default()
                .name(output_topic_name.clone())
                .partitions(Some((0..num_partitions).collect())),
        ],
    };

    let txn_add_partitions_response = sc.txn_add_partitions(txn_add_partitions).await?;
    debug!(?txn_add_partitions_response);
    assert_eq!(1, txn_add_partitions_response.zero_to_three().len());
    assert_eq!(
        output_topic_name,
        txn_add_partitions_response.zero_to_three()[0].name
    );

    // adding offsets
    //
    assert_eq!(
        ErrorCode::None,
        sc.txn_add_offsets(
            transaction_id.as_str(),
            txn_producer.id,
            txn_producer.epoch,
            group_id.as_str(),
        )
        .await?
    );

    const COMMITTED_OFFSET: i64 = 6543456;

    // commit an offset for the input topic to the consumer group
    //
    assert_eq!(
        vec![
            TxnOffsetCommitResponseTopic::default()
                .name(input_topic_name.clone())
                .partitions(Some(
                    [TxnOffsetCommitResponsePartition::default()
                        .partition_index(input_partition_index)
                        .error_code(ErrorCode::None.into())]
                    .into(),
                ))
        ],
        sc.txn_offset_commit(TxnOffsetCommitRequest {
            transaction_id: transaction_id.clone(),
            group_id: group_id.clone(),
            producer_id: txn_producer.id,
            producer_epoch: txn_producer.epoch,
            generation_id: Some(join_response.generation_id),
            member_id: Some(member_id),
            group_instance_id: group_instance_id
                .map(|group_instance_id| group_instance_id.to_owned()),
            topics: [TxnOffsetCommitRequestTopic::default()
                .name(input_topic_name.clone())
                .partitions(Some(
                    [TxnOffsetCommitRequestPartition::default()
                        .partition_index(input_partition_index)
                        .committed_offset(COMMITTED_OFFSET)
                        .committed_leader_epoch(Some(-1))
                        .committed_metadata(None)]
                    .into(),
                ))]
            .into(),
        })
        .await?
    );

    // verify that the committed offset is not visible as the transaction remains in progress
    //
    assert_eq!(
        OffsetFetchResponse::default()
            .topics(Some(
                [OffsetFetchResponseTopic::default()
                    .name(input_topic_name.clone())
                    .partitions(Some(
                        [OffsetFetchResponsePartition::default()
                            .partition_index(input_partition_index)
                            .committed_offset(-1)
                            .committed_leader_epoch(Some(-1))
                            .metadata(Some("".into()))
                            .error_code(0)]
                        .into()
                    ))]
                .into()
            ))
            .error_code(Some(ErrorCode::None.into()))
            .throttle_time_ms(Some(0)),
        offset_fetch(
            &mut controller,
            group_id.as_str(),
            &[OffsetFetchRequestTopic::default()
                .name(input_topic_name.clone())
                .partition_indexes(Some([input_partition_index].into()))],
        )
        .await?
    );

    let output_partition_index = rng.random_range(0..num_partitions);
    let output_topition = Topition::new(output_topic_name.clone(), output_partition_index);

    // verify the high watermark of the output topic is 0
    // prior to producing records
    //
    let list_offsets_before_produce = sc
        .list_offsets(
            IsolationLevel::ReadUncommitted,
            &[(output_topition.clone(), ListOffset::Latest)],
        )
        .await?;
    assert_eq!(1, list_offsets_before_produce.len());
    assert_eq!(output_topic_name, list_offsets_before_produce[0].0.topic());
    assert_eq!(
        output_partition_index,
        list_offsets_before_produce[0].0.partition()
    );
    assert_eq!(ErrorCode::None, list_offsets_before_produce[0].1.error_code);
    assert_eq!(Some(0), list_offsets_before_produce[0].1.offset);

    // produce records to output topic while in a transaction
    //
    for n in 0..records {
        let value = format!("Consectetur adipiscing elit: {n}");
        let batch = inflated::Batch::builder()
            .record(Record::builder().value(Bytes::copy_from_slice(value.as_bytes()).into()))
            .attributes(BatchAttribute::default().transaction(true).into())
            .producer_id(txn_producer.id)
            .producer_epoch(txn_producer.epoch)
            .base_sequence(n as i32)
            .build()
            .and_then(TryInto::try_into)
            .inspect(|deflated| debug!(?deflated))?;

        _ = sc
            .produce(Some(transaction_id.as_str()), &output_topition, batch)
            .await
            .inspect(|offset| debug!(?offset))?;
    }

    // read uncommitted latest offset has updated
    //
    let list_offsets_after_produce = sc
        .list_offsets(
            IsolationLevel::ReadUncommitted,
            &[(output_topition.clone(), ListOffset::Latest)],
        )
        .await?;
    assert_eq!(1, list_offsets_after_produce.len());
    assert_eq!(output_topic_name, list_offsets_after_produce[0].0.topic());
    assert_eq!(
        output_partition_index,
        list_offsets_after_produce[0].0.partition()
    );
    assert_eq!(ErrorCode::None, list_offsets_after_produce[0].1.error_code);
    assert_eq!(Some(records), list_offsets_after_produce[0].1.offset);

    // read committed offset is at 0
    //
    let list_offsets_after_produce = sc
        .list_offsets(
            IsolationLevel::ReadCommitted,
            &[(output_topition.clone(), ListOffset::Latest)],
        )
        .await?;
    assert_eq!(1, list_offsets_after_produce.len());
    assert_eq!(output_topic_name, list_offsets_after_produce[0].0.topic());
    assert_eq!(
        output_partition_index,
        list_offsets_after_produce[0].0.partition()
    );
    assert_eq!(ErrorCode::None, list_offsets_after_produce[0].1.error_code);
    assert_eq!(Some(0), list_offsets_after_produce[0].1.offset);

    // commit the transaction
    //
    assert_eq!(
        ErrorCode::None,
        sc.txn_end(
            transaction_id.as_str(),
            txn_producer.id,
            txn_producer.epoch,
            true
        )
        .await?
    );

    // committed offset is now visible
    assert_eq!(
        OffsetFetchResponse::default()
            .topics(Some(
                [OffsetFetchResponseTopic::default()
                    .name(input_topic_name.clone())
                    .partitions(Some(
                        [OffsetFetchResponsePartition::default()
                            .partition_index(input_partition_index)
                            .committed_offset(COMMITTED_OFFSET)
                            .committed_leader_epoch(Some(-1))
                            .metadata(Some("".into()))
                            .error_code(0)]
                        .into()
                    ))]
                .into()
            ))
            .throttle_time_ms(Some(0))
            .error_code(Some(ErrorCode::None.into())),
        offset_fetch(
            &mut controller,
            group_id.as_str(),
            &[OffsetFetchRequestTopic::default()
                .name(input_topic_name.clone())
                .partition_indexes(Some([input_partition_index].into()))],
        )
        .await?
    );

    // read uncommitted
    //
    let list_offsets_after_produce = sc
        .list_offsets(
            IsolationLevel::ReadUncommitted,
            &[(output_topition.clone(), ListOffset::Latest)],
        )
        .await?;
    assert_eq!(1, list_offsets_after_produce.len());
    assert_eq!(output_topic_name, list_offsets_after_produce[0].0.topic());
    assert_eq!(
        output_partition_index,
        list_offsets_after_produce[0].0.partition()
    );
    assert_eq!(ErrorCode::None, list_offsets_after_produce[0].1.error_code);
    // includes the produced end txn marker as part of the transaction
    assert_eq!(Some(records + 1), list_offsets_after_produce[0].1.offset);

    // read committed offset has updated to high watermark
    //
    let list_offsets_after_produce = sc
        .list_offsets(
            IsolationLevel::ReadCommitted,
            &[(output_topition.clone(), ListOffset::Latest)],
        )
        .await?;
    assert_eq!(1, list_offsets_after_produce.len());
    assert_eq!(output_topic_name, list_offsets_after_produce[0].0.topic());
    assert_eq!(
        output_partition_index,
        list_offsets_after_produce[0].0.partition()
    );
    assert_eq!(ErrorCode::None, list_offsets_after_produce[0].1.error_code);
    assert_eq!(Some(records + 1), list_offsets_after_produce[0].1.offset);

    // assert_eq!(
    //     ErrorCode::None,
    //     sc.delete_topic(&TopicId::from(input_topic_id)).await?
    // );

    // assert_eq!(
    //     ErrorCode::None,
    //     sc.delete_topic(&TopicId::from(output_topic_id)).await?
    // );

    Ok(())
}
