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

//! A `JoinGroup`, `SyncGroup`, `Heartbeat`, `LeaveGroup`, `OffsetCommit` or `DeleteGroups` whose
//! storage fails answers an error code in its response, as Kafka's group coordinator does, and
//! the same request succeeds once storage answers again. A transient
//! failure answers `COORDINATOR_NOT_AVAILABLE`, which clients retry, and a permanent one answers
//! `UNKNOWN_SERVER_ERROR`. The coordinator returns a response and not an error, so the broker
//! keeps the client's connection open.

#![cfg(feature = "dynostore")]

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use bytes::Bytes;
use nisshi_broker::coordinator::group::{
    Coordinator as _, OffsetCommit, administrator::Controller,
};
use nisshi_sans_io::{
    ApiKey as _, Body, ConfigResource, DeleteGroupsRequest, DeleteGroupsResponse, ErrorCode, Frame,
    Header, HeartbeatRequest, HeartbeatResponse, IsolationLevel, JoinGroupRequest,
    JoinGroupResponse, LeaveGroupRequest, LeaveGroupResponse, ListOffset, OffsetCommitResponse,
    ScramMechanism, SyncGroupRequest, SyncGroupResponse,
    create_topics_request::CreatableTopic,
    delete_groups_response::DeletableGroupResult,
    delete_records_request::DeleteRecordsTopic,
    delete_records_response::DeleteRecordsTopicResult,
    describe_cluster_response::DescribeClusterBroker,
    describe_configs_response::DescribeConfigsResult,
    describe_topic_partitions_response::DescribeTopicPartitionsResponseTopic,
    fetch_response::AbortedTransaction,
    incremental_alter_configs_request::AlterConfigsResource,
    incremental_alter_configs_response::AlterConfigsResourceResponse,
    join_group_request::JoinGroupRequestProtocol,
    leave_group_request::MemberIdentity,
    offset_commit_request::{OffsetCommitRequestPartition, OffsetCommitRequestTopic},
    record::deflated,
    sync_group_request::SyncGroupRequestAssignment,
    txn_offset_commit_response::TxnOffsetCommitResponseTopic,
};
use nisshi_service::routable_range;
use nisshi_storage::{
    ArcDynStorage, BrokerRegistrationRequest, ByteLimit, CommittedOffset, Error, FetchedSender,
    GroupDetail, ListOffsetResponse, MetadataResponse, NamedGroupDetail, OffsetCommitRequest,
    OffsetStage, ProducerIdResponse, ReadEnd, Result as StorageResult, ScramCredential, Storage,
    TopicId, Topition, TxnAddPartitionsRequest, TxnAddPartitionsResponse, TxnOffsetCommitRequest,
    UpdateError, Version,
};
use tokio::time::{sleep, timeout};
use url::Url;

use crate::common::{
    CLIENT_ID, Formed, JOIN_GROUP_VERSION, PROTOCOL_TYPE, RANGE, alphanumeric_string, form_group,
    init_tracing, memory_storage,
};

/// The topic that [`form_group`] subscribes to.
const TOPIC: &str = "t";

/// The storage calls that an outage fails.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Failing {
    /// Every read and write of a group's state and of its committed offsets.
    GroupsAndOffsets,
    /// The writes of a group's state and of its committed offsets, and the delete of a group.
    Writes,
    /// The writes of committed offsets.
    OffsetCommits,
}

/// An outage of the storage calls in `failing`, each of which fails with `error`.
#[derive(Clone, Debug)]
struct Outage {
    failing: Failing,
    error: Error,
}

/// A storage whose group and offset calls fail while an outage of them is in progress, and pass
/// to `inner` otherwise. Every other call passes to `inner`.
#[derive(Clone, Debug)]
struct OutageStorage {
    inner: ArcDynStorage,
    outage: Arc<Mutex<Option<Outage>>>,
}

impl OutageStorage {
    async fn new() -> Result<Self> {
        let inner = memory_storage(alphanumeric_string(15), 111).await?;

        _ = inner
            .create_topic(
                CreatableTopic::default()
                    .name(TOPIC.into())
                    .num_partitions(3)
                    .replication_factor(1)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            )
            .await?;

        Ok(Self {
            inner,
            outage: Arc::default(),
        })
    }

    /// Starts an outage in which each call in `failing` fails with `error`.
    fn start(&self, failing: Failing, error: Error) {
        _ = self
            .outage
            .lock()
            .expect("outage lock")
            .replace(Outage { failing, error });
    }

    fn end(&self) {
        _ = self.outage.lock().expect("outage lock").take();
    }

    /// Returns the error of the outage in progress if it fails any call in `failing`.
    fn available(&self, failing: &[Failing]) -> StorageResult<()> {
        match self.outage.lock().expect("outage lock").as_ref() {
            Some(outage) if failing.contains(&outage.failing) => Err(outage.error.clone()),
            _ => Ok(()),
        }
    }

    fn read_available(&self) -> StorageResult<()> {
        self.available(&[Failing::GroupsAndOffsets])
    }

    fn write_available(&self) -> StorageResult<()> {
        self.available(&[Failing::GroupsAndOffsets, Failing::Writes])
    }

    fn offset_commit_available(&self) -> StorageResult<()> {
        self.available(&[
            Failing::GroupsAndOffsets,
            Failing::Writes,
            Failing::OffsetCommits,
        ])
    }
}

/// An error that a retry may clear, such as a lost connection to the storage.
fn transient() -> Error {
    Error::from(std::io::Error::other("storage is unreachable"))
}

/// An error that a retry can't clear.
fn permanent() -> Error {
    Error::Message(String::from("the stored group does not parse"))
}

fn protocols() -> Vec<JoinGroupRequestProtocol> {
    vec![
        JoinGroupRequestProtocol::default()
            .name(RANGE.into())
            .metadata(Bytes::from_static(b"range-meta")),
    ]
}

fn offset_commit_topics() -> Vec<OffsetCommitRequestTopic> {
    vec![
        OffsetCommitRequestTopic::default()
            .name(TOPIC.into())
            .partitions(Some(
                (0..2)
                    .map(|partition_index| {
                        OffsetCommitRequestPartition::default()
                            .partition_index(partition_index)
                            .committed_offset(42)
                            .committed_leader_epoch(Some(-1))
                            .committed_metadata(None)
                    })
                    .collect(),
            )),
    ]
}

async fn join_body(
    controller: &Controller<OutageStorage>,
    group_id: &str,
    member_id: &str,
) -> Result<Body> {
    let protocols = protocols();

    controller
        .join(
            JOIN_GROUP_VERSION,
            Some(CLIENT_ID),
            None,
            group_id,
            10_000,
            Some(30_000),
            member_id,
            None,
            PROTOCOL_TYPE,
            Some(&protocols),
            None,
        )
        .await
        .map_err(Into::into)
}

/// Returns the error code and the member id of a `JoinGroup` response.
fn join_error_code_and_member_id(body: Body) -> Result<(ErrorCode, String)> {
    let Body::JoinGroupResponse(JoinGroupResponse {
        error_code,
        member_id,
        ..
    }) = body
    else {
        return Err(anyhow!("expected a join group response: {body:?}"));
    };

    Ok((ErrorCode::try_from(error_code)?, member_id))
}

async fn join_error_code(
    controller: &Controller<OutageStorage>,
    group_id: &str,
    member_id: &str,
) -> Result<(ErrorCode, String)> {
    join_body(controller, group_id, member_id)
        .await
        .and_then(join_error_code_and_member_id)
}

/// Returns the body of a `SyncGroup` from the member of `formed`, with the assignment of each
/// partition of [`TOPIC`] to that member.
async fn sync_body(controller: &Controller<OutageStorage>, formed: &Formed) -> Result<Body> {
    let assignments = [SyncGroupRequestAssignment::default()
        .member_id(formed.member_id.clone())
        .assignment(Bytes::new())];

    controller
        .sync(
            &formed.group,
            formed.generation_id,
            &formed.member_id,
            None,
            None,
            None,
            Some(&assignments),
        )
        .await
        .map_err(Into::into)
}

async fn sync_error_code(
    controller: &Controller<OutageStorage>,
    formed: &Formed,
) -> Result<ErrorCode> {
    let body = sync_body(controller, formed).await?;

    let Body::SyncGroupResponse(SyncGroupResponse { error_code, .. }) = body else {
        return Err(anyhow!("expected a sync group response: {body:?}"));
    };

    ErrorCode::try_from(error_code).map_err(Into::into)
}

async fn heartbeat_body(controller: &Controller<OutageStorage>, formed: &Formed) -> Result<Body> {
    controller
        .heartbeat(&formed.group, formed.generation_id, &formed.member_id, None)
        .await
        .map_err(Into::into)
}

async fn heartbeat_error_code(
    controller: &Controller<OutageStorage>,
    formed: &Formed,
) -> Result<ErrorCode> {
    let body = heartbeat_body(controller, formed).await?;

    let Body::HeartbeatResponse(HeartbeatResponse { error_code, .. }) = body else {
        return Err(anyhow!("expected a heartbeat response: {body:?}"));
    };

    ErrorCode::try_from(error_code).map_err(Into::into)
}

async fn leave_body(controller: &Controller<OutageStorage>, formed: &Formed) -> Result<Body> {
    let members = [MemberIdentity::default()
        .member_id(formed.member_id.clone())
        .group_instance_id(None)
        .reason(None)];

    controller
        .leave(&formed.group, None, Some(&members))
        .await
        .map_err(Into::into)
}

async fn leave_error_code(
    controller: &Controller<OutageStorage>,
    formed: &Formed,
) -> Result<ErrorCode> {
    let body = leave_body(controller, formed).await?;

    let Body::LeaveGroupResponse(LeaveGroupResponse { error_code, .. }) = body else {
        return Err(anyhow!("expected a leave group response: {body:?}"));
    };

    ErrorCode::try_from(error_code).map_err(Into::into)
}

async fn offset_commit_body(
    controller: &Controller<OutageStorage>,
    formed: &Formed,
) -> Result<Body> {
    let topics = offset_commit_topics();

    controller
        .offset_commit(OffsetCommit {
            group_id: &formed.group,
            generation_id_or_member_epoch: Some(formed.generation_id),
            member_id: Some(&formed.member_id),
            group_instance_id: None,
            retention_time_ms: None,
            topics: Some(&topics),
            is_transactional: false,
        })
        .await
        .map_err(Into::into)
}

/// Returns the error code of each partition in an `OffsetCommit` from the member of `formed`.
async fn offset_commit_error_codes(
    controller: &Controller<OutageStorage>,
    formed: &Formed,
) -> Result<Vec<(i32, ErrorCode)>> {
    let body = offset_commit_body(controller, formed).await?;

    let Body::OffsetCommitResponse(OffsetCommitResponse {
        topics: Some(topics),
        ..
    }) = body
    else {
        return Err(anyhow!("expected an offset commit response: {body:?}"));
    };

    topics
        .iter()
        .flat_map(|topic| topic.partitions.iter().flatten())
        .map(|partition| {
            ErrorCode::try_from(partition.error_code)
                .map(|error_code| (partition.partition_index, error_code))
                .map_err(Into::into)
        })
        .collect()
}

/// Returns the error code of each group in a `DeleteGroups` of `group_ids`.
async fn delete_groups_error_codes(
    controller: &Controller<OutageStorage>,
    group_ids: &[String],
) -> Result<Vec<(String, ErrorCode)>> {
    let body = controller.delete_groups(group_ids).await?;

    let Body::DeleteGroupsResponse(DeleteGroupsResponse {
        results: Some(results),
        ..
    }) = body
    else {
        return Err(anyhow!("expected a delete groups response: {body:?}"));
    };

    results
        .into_iter()
        .map(|result| {
            ErrorCode::try_from(result.error_code)
                .map(|error_code| (result.group_id, error_code))
                .map_err(Into::into)
        })
        .collect()
}

/// Encodes `body`, the response to a request of `api`, at each version of `api` that the broker
/// serves, as the broker encodes a response before it writes it to the connection.
fn encode_at_every_served_version(api: i16, body: &Body) -> Result<()> {
    let versions = routable_range(api).ok_or_else(|| anyhow!("api key {api} is not served"))?;

    for api_version in versions {
        _ = Frame::response(
            Header::Response { correlation_id: 0 },
            body.clone(),
            api,
            api_version,
        )
        .map_err(|error| anyhow!("api key {api} v{api_version}: {error:?}"))?;
    }

    Ok(())
}

/// Returns the code at the top level of `body`, or the code of each partition or group that it
/// answers, which are all the same for a request that failed.
fn error_codes(body: &Body) -> Result<BTreeSet<ErrorCode>> {
    let codes = match body {
        Body::JoinGroupResponse(response) => vec![response.error_code],
        Body::SyncGroupResponse(response) => vec![response.error_code],
        Body::HeartbeatResponse(response) => vec![response.error_code],
        Body::LeaveGroupResponse(response) => vec![response.error_code],
        Body::OffsetCommitResponse(response) => response
            .topics
            .iter()
            .flatten()
            .flat_map(|topic| topic.partitions.iter().flatten())
            .map(|partition| partition.error_code)
            .collect(),
        Body::DeleteGroupsResponse(response) => response
            .results
            .iter()
            .flatten()
            .map(|result| result.error_code)
            .collect(),
        otherwise => return Err(anyhow!("unexpected response: {otherwise:?}")),
    };

    codes
        .into_iter()
        .map(|code| ErrorCode::try_from(code).map_err(Into::into))
        .collect()
}

#[tokio::test]
async fn join_answers_coordinator_not_available_until_storage_answers() -> Result<()> {
    let _guard = init_tracing()?;

    let storage = OutageStorage::new().await?;
    let controller = Controller::with_storage(storage.clone())?;
    let group_id = alphanumeric_string(15);

    storage.start(Failing::GroupsAndOffsets, transient());

    assert_eq!(
        (ErrorCode::CoordinatorNotAvailable, String::new()),
        join_error_code(&controller, &group_id, "").await?
    );

    storage.end();

    // A member without a member id gets one with `MEMBER_ID_REQUIRED`, after the coordinator
    // has read the group from storage.
    let (error_code, member_id) = join_error_code(&controller, &group_id, "").await?;
    assert_eq!(ErrorCode::MemberIdRequired, error_code);
    assert!(!member_id.is_empty());

    Ok(())
}

#[tokio::test]
async fn sync_answers_coordinator_not_available_until_storage_answers() -> Result<()> {
    let _guard = init_tracing()?;

    let storage = OutageStorage::new().await?;
    let controller = Controller::with_storage(storage.clone())?;
    let formed = form_group(&controller).await?;

    storage.start(Failing::GroupsAndOffsets, transient());

    assert_eq!(
        ErrorCode::CoordinatorNotAvailable,
        sync_error_code(&controller, &formed).await?
    );

    storage.end();

    assert_eq!(
        ErrorCode::None,
        sync_error_code(&controller, &formed).await?
    );

    Ok(())
}

#[tokio::test]
async fn heartbeat_answers_coordinator_not_available_until_storage_answers() -> Result<()> {
    let _guard = init_tracing()?;

    let storage = OutageStorage::new().await?;
    let controller = Controller::with_storage(storage.clone())?;
    let formed = form_group(&controller).await?;

    storage.start(Failing::GroupsAndOffsets, transient());

    assert_eq!(
        ErrorCode::CoordinatorNotAvailable,
        heartbeat_error_code(&controller, &formed).await?
    );

    storage.end();

    assert_eq!(
        ErrorCode::None,
        heartbeat_error_code(&controller, &formed).await?
    );

    Ok(())
}

/// Each request answers `UNKNOWN_SERVER_ERROR` for a failure that a retry can't clear, in a
/// response that the broker can encode at every version that it serves. A response that leaves a
/// field of a version unset fails to encode, and the broker then closes the connection.
#[tokio::test]
async fn each_request_answers_unknown_server_error_for_a_permanent_failure() -> Result<()> {
    let _guard = init_tracing()?;

    let storage = OutageStorage::new().await?;
    let controller = Controller::with_storage(storage.clone())?;
    let formed = form_group(&controller).await?;

    storage.start(Failing::GroupsAndOffsets, permanent());

    let join = join_body(&controller, &formed.group, &formed.member_id).await?;

    // The response echoes the member id of the request.
    assert_eq!(
        (ErrorCode::UnknownServerError, formed.member_id.clone()),
        join_error_code_and_member_id(join.clone())?
    );

    let answered = [
        (JoinGroupRequest::KEY, join),
        (
            SyncGroupRequest::KEY,
            sync_body(&controller, &formed).await?,
        ),
        (
            HeartbeatRequest::KEY,
            heartbeat_body(&controller, &formed).await?,
        ),
        (
            LeaveGroupRequest::KEY,
            leave_body(&controller, &formed).await?,
        ),
        (
            nisshi_sans_io::OffsetCommitRequest::KEY,
            offset_commit_body(&controller, &formed).await?,
        ),
        (
            DeleteGroupsRequest::KEY,
            controller
                .delete_groups(std::slice::from_ref(&formed.group))
                .await?,
        ),
    ];

    for (api, body) in &answered {
        assert_eq!(
            BTreeSet::from([ErrorCode::UnknownServerError]),
            error_codes(body)?,
            "api key {api}"
        );

        encode_at_every_served_version(*api, body)?;
    }

    Ok(())
}

/// The responses that answer `COORDINATOR_NOT_AVAILABLE` encode at every version that the broker
/// serves, as those for a permanent failure do.
#[tokio::test]
async fn each_request_encodes_coordinator_not_available_at_every_served_version() -> Result<()> {
    let _guard = init_tracing()?;

    let storage = OutageStorage::new().await?;
    let controller = Controller::with_storage(storage.clone())?;
    let formed = form_group(&controller).await?;

    storage.start(Failing::GroupsAndOffsets, transient());

    let answered = [
        (
            JoinGroupRequest::KEY,
            join_body(&controller, &formed.group, &formed.member_id).await?,
        ),
        (
            SyncGroupRequest::KEY,
            sync_body(&controller, &formed).await?,
        ),
        (
            HeartbeatRequest::KEY,
            heartbeat_body(&controller, &formed).await?,
        ),
        (
            LeaveGroupRequest::KEY,
            leave_body(&controller, &formed).await?,
        ),
        (
            nisshi_sans_io::OffsetCommitRequest::KEY,
            offset_commit_body(&controller, &formed).await?,
        ),
        (
            DeleteGroupsRequest::KEY,
            controller
                .delete_groups(std::slice::from_ref(&formed.group))
                .await?,
        ),
    ];

    for (api, body) in &answered {
        assert_eq!(
            BTreeSet::from([ErrorCode::CoordinatorNotAvailable]),
            error_codes(body)?,
            "api key {api}"
        );

        encode_at_every_served_version(*api, body)?;
    }

    Ok(())
}

#[tokio::test]
async fn leave_answers_coordinator_not_available_until_storage_answers() -> Result<()> {
    let _guard = init_tracing()?;

    let storage = OutageStorage::new().await?;
    let controller = Controller::with_storage(storage.clone())?;
    let formed = form_group(&controller).await?;

    storage.start(Failing::GroupsAndOffsets, transient());

    assert_eq!(
        ErrorCode::CoordinatorNotAvailable,
        leave_error_code(&controller, &formed).await?
    );

    storage.end();

    assert_eq!(
        ErrorCode::None,
        leave_error_code(&controller, &formed).await?
    );

    Ok(())
}

#[tokio::test]
async fn offset_commit_answers_coordinator_not_available_until_storage_answers() -> Result<()> {
    let _guard = init_tracing()?;

    let storage = OutageStorage::new().await?;
    let controller = Controller::with_storage(storage.clone())?;
    let formed = form_group(&controller).await?;

    storage.start(Failing::GroupsAndOffsets, transient());

    assert_eq!(
        vec![
            (0, ErrorCode::CoordinatorNotAvailable),
            (1, ErrorCode::CoordinatorNotAvailable)
        ],
        offset_commit_error_codes(&controller, &formed).await?
    );

    storage.end();

    assert_eq!(
        vec![(0, ErrorCode::None), (1, ErrorCode::None)],
        offset_commit_error_codes(&controller, &formed).await?
    );

    Ok(())
}

#[tokio::test]
async fn delete_groups_answers_coordinator_not_available_until_storage_answers() -> Result<()> {
    let _guard = init_tracing()?;

    let storage = OutageStorage::new().await?;
    let controller = Controller::with_storage(storage.clone())?;
    let formed = form_group(&controller).await?;

    // A group with a member can't be deleted, so its only member leaves first.
    assert_eq!(
        ErrorCode::None,
        leave_error_code(&controller, &formed).await?
    );

    let other = alphanumeric_string(15);

    storage.start(Failing::GroupsAndOffsets, transient());

    assert_eq!(
        vec![
            (formed.group.clone(), ErrorCode::CoordinatorNotAvailable),
            (other.clone(), ErrorCode::CoordinatorNotAvailable),
        ],
        delete_groups_error_codes(&controller, &[formed.group.clone(), other]).await?
    );

    storage.end();

    assert_eq!(
        vec![(formed.group.clone(), ErrorCode::None)],
        delete_groups_error_codes(&controller, std::slice::from_ref(&formed.group)).await?
    );

    Ok(())
}

#[tokio::test]
async fn leave_answers_coordinator_not_available_when_the_group_write_fails() -> Result<()> {
    let _guard = init_tracing()?;

    let storage = OutageStorage::new().await?;
    let controller = Controller::with_storage(storage.clone())?;
    let formed = form_group(&controller).await?;

    storage.start(Failing::Writes, transient());

    assert_eq!(
        ErrorCode::CoordinatorNotAvailable,
        leave_error_code(&controller, &formed).await?
    );

    storage.end();

    // The member is still in the group, because the coordinator did not store its leave.
    assert_eq!(
        ErrorCode::None,
        heartbeat_error_code(&controller, &formed).await?
    );

    Ok(())
}

#[tokio::test]
async fn offset_commit_answers_coordinator_not_available_when_the_offsets_write_fails() -> Result<()>
{
    let _guard = init_tracing()?;

    let storage = OutageStorage::new().await?;
    let controller = Controller::with_storage(storage.clone())?;
    let formed = form_group(&controller).await?;

    // The coordinator reads and stores the group, and then fails to store the offsets.
    storage.start(Failing::OffsetCommits, transient());

    assert_eq!(
        vec![
            (0, ErrorCode::CoordinatorNotAvailable),
            (1, ErrorCode::CoordinatorNotAvailable)
        ],
        offset_commit_error_codes(&controller, &formed).await?
    );

    storage.end();

    assert_eq!(
        vec![(0, ErrorCode::None), (1, ErrorCode::None)],
        offset_commit_error_codes(&controller, &formed).await?
    );

    Ok(())
}

#[tokio::test]
async fn delete_groups_answers_coordinator_not_available_when_the_delete_fails() -> Result<()> {
    let _guard = init_tracing()?;

    let storage = OutageStorage::new().await?;
    let controller = Controller::with_storage(storage.clone())?;
    let formed = form_group(&controller).await?;

    assert_eq!(
        ErrorCode::None,
        leave_error_code(&controller, &formed).await?
    );

    // The coordinator reads the empty group, and then fails to delete it.
    storage.start(Failing::Writes, transient());

    assert_eq!(
        vec![(formed.group.clone(), ErrorCode::CoordinatorNotAvailable)],
        delete_groups_error_codes(&controller, std::slice::from_ref(&formed.group)).await?
    );

    storage.end();

    assert_eq!(
        vec![(formed.group.clone(), ErrorCode::None)],
        delete_groups_error_codes(&controller, std::slice::from_ref(&formed.group)).await?
    );

    Ok(())
}

/// A `JoinGroup` held for the end of the join phase answers `COORDINATOR_NOT_AVAILABLE` when a
/// later read of the group fails, and echoes the member id of the request.
#[tokio::test]
async fn held_join_answers_coordinator_not_available_with_its_member_id() -> Result<()> {
    let _guard = init_tracing()?;

    let storage = OutageStorage::new().await?;
    let controller = Controller::with_storage(storage.clone())?;
    let group_id = alphanumeric_string(15);

    let (error_code, member_id) = join_error_code(&controller, &group_id, "").await?;
    assert_eq!(ErrorCode::MemberIdRequired, error_code);

    // The first join of a new group waits for the end of the initial rebalance delay.
    let held = tokio::spawn({
        let controller = controller.clone();
        let group_id = group_id.clone();
        let member_id = member_id.clone();
        async move { join_error_code(&controller, &group_id, &member_id).await }
    });

    // The join has stored the member in the group, and waits. The `MEMBER_ID_REQUIRED` answer
    // stored the group with the member pending, not in its members.
    timeout(Duration::from_secs(10), async {
        while !storage
            .inner
            .group_with_version(&group_id)
            .await
            .ok()
            .flatten()
            .is_some_and(|(detail, _)| detail.members.contains_key(&member_id))
        {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    assert!(!held.is_finished());

    storage.start(Failing::GroupsAndOffsets, transient());

    assert_eq!(
        (ErrorCode::CoordinatorNotAvailable, member_id),
        held.await??
    );

    Ok(())
}

/// A `SyncGroup` from the leader answers `COORDINATOR_NOT_AVAILABLE` when it reads the group and
/// then fails to store the leader's assignment.
#[tokio::test]
async fn sync_answers_coordinator_not_available_when_the_group_write_fails() -> Result<()> {
    let _guard = init_tracing()?;

    let storage = OutageStorage::new().await?;
    let controller = Controller::with_storage(storage.clone())?;
    let group_id = alphanumeric_string(15);

    let (_, member_id) = join_error_code(&controller, &group_id, "").await?;

    let Body::JoinGroupResponse(JoinGroupResponse {
        error_code,
        generation_id,
        leader,
        ..
    }) = join_body(&controller, &group_id, &member_id).await?
    else {
        return Err(anyhow!("expected a join group response"));
    };

    // The only member leads the group, which waits for its assignment.
    assert_eq!(i16::from(ErrorCode::None), error_code);
    assert_eq!(member_id, leader);

    let formed = Formed {
        group: group_id,
        generation_id,
        member_id,
    };

    storage.start(Failing::Writes, transient());

    assert_eq!(
        ErrorCode::CoordinatorNotAvailable,
        sync_error_code(&controller, &formed).await?
    );

    storage.end();

    assert_eq!(
        ErrorCode::None,
        sync_error_code(&controller, &formed).await?
    );

    Ok(())
}

#[tokio::test]
async fn heartbeat_answers_coordinator_not_available_when_the_group_write_fails() -> Result<()> {
    let _guard = init_tracing()?;

    let storage = OutageStorage::new().await?;
    let controller = Controller::with_storage(storage.clone())?;
    let formed = form_group(&controller).await?;

    // The heartbeat reads the group, and then fails to store the member's contact.
    storage.start(Failing::Writes, transient());

    assert_eq!(
        ErrorCode::CoordinatorNotAvailable,
        heartbeat_error_code(&controller, &formed).await?
    );

    storage.end();

    assert_eq!(
        ErrorCode::None,
        heartbeat_error_code(&controller, &formed).await?
    );

    Ok(())
}

/// A `DeleteGroups` whose delete fails keeps the answer that it decided for a group before the
/// delete, and answers `COORDINATOR_NOT_AVAILABLE` only for each group that it tried to delete.
#[tokio::test]
async fn delete_groups_keeps_its_refusals_when_the_delete_fails() -> Result<()> {
    let _guard = init_tracing()?;

    let storage = OutageStorage::new().await?;
    let controller = Controller::with_storage(storage.clone())?;
    let with_member = form_group(&controller).await?;
    let empty = form_group(&controller).await?;

    assert_eq!(
        ErrorCode::None,
        leave_error_code(&controller, &empty).await?
    );

    storage.start(Failing::Writes, transient());

    assert_eq!(
        vec![
            (with_member.group.clone(), ErrorCode::NonEmptyGroup),
            (empty.group.clone(), ErrorCode::CoordinatorNotAvailable),
        ],
        delete_groups_error_codes(
            &controller,
            &[with_member.group.clone(), empty.group.clone()]
        )
        .await?
    );

    Ok(())
}

/// A failure that a retry may clear logs at `warn` once in each interval for each request, with
/// the group id and the member id, and at debug after that.
#[tokio::test]
async fn a_retriable_failure_warns_once_in_each_interval() -> Result<()> {
    let storage = OutageStorage::new().await?;
    let controller = Controller::with_storage(storage.clone())?;
    let formed = form_group(&controller).await?;

    let log = Log::default();
    let writer = log.clone();

    // Each line holds only the fields of its own event, not those of the request's span, so the
    // ids that the test finds come from the event.
    let _guard = tracing::subscriber::set_default(
        tracing_subscriber::fmt()
            .json()
            .with_span_list(false)
            .with_current_span(false)
            .with_max_level(tracing::Level::DEBUG)
            .with_writer(move || writer.clone())
            .finish(),
    );

    storage.start(Failing::GroupsAndOffsets, transient());

    for _ in 0..3 {
        assert_eq!(
            ErrorCode::CoordinatorNotAvailable,
            heartbeat_error_code(&controller, &formed).await?
        );
    }

    let failures = log.events_with_message("the group request failed")?;
    assert_eq!(3, failures.len(), "{failures:#?}");

    let warned = failures
        .iter()
        .filter(|event| event["level"] == "WARN")
        .collect::<Vec<_>>();
    assert_eq!(1, warned.len(), "{failures:#?}");
    assert_eq!(
        formed.group, warned[0]["fields"]["group_id"],
        "{}",
        warned[0]
    );
    assert_eq!(
        formed.member_id, warned[0]["fields"]["member_id"],
        "{}",
        warned[0]
    );

    Ok(())
}

/// Collects what the `tracing` subscriber of a test writes.
#[derive(Clone, Default)]
struct Log(Arc<Mutex<Vec<u8>>>);

impl Log {
    /// Returns each event of the log, written as a line of JSON, whose message is `message`.
    fn events_with_message(&self, message: &str) -> Result<Vec<serde_json::Value>> {
        String::from_utf8_lossy(&self.0.lock().expect("log"))
            .lines()
            .map(serde_json::from_str::<serde_json::Value>)
            .filter(|event| {
                event
                    .as_ref()
                    .map_or(true, |event| event["fields"]["message"] == message)
            })
            .collect::<Result<_, _>>()
            .map_err(Into::into)
    }
}

impl std::io::Write for Log {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().expect("log").extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[async_trait]
impl Storage for OutageStorage {
    async fn register_broker(
        &self,
        broker_registration: BrokerRegistrationRequest,
    ) -> StorageResult<()> {
        self.inner.register_broker(broker_registration).await
    }

    async fn create_topic(
        &self,
        topic: CreatableTopic,
        validate_only: bool,
    ) -> StorageResult<uuid::Uuid> {
        self.inner.create_topic(topic, validate_only).await
    }

    async fn incremental_alter_resource(
        &self,
        resource: AlterConfigsResource,
    ) -> StorageResult<AlterConfigsResourceResponse> {
        self.inner.incremental_alter_resource(resource).await
    }

    async fn delete_records(
        &self,
        topics: &[DeleteRecordsTopic],
    ) -> StorageResult<Vec<DeleteRecordsTopicResult>> {
        self.inner.delete_records(topics).await
    }

    async fn delete_topic(&self, topic: &TopicId) -> StorageResult<ErrorCode> {
        self.inner.delete_topic(topic).await
    }

    async fn brokers(&self) -> StorageResult<Vec<DescribeClusterBroker>> {
        self.inner.brokers().await
    }

    async fn produce(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        batch: deflated::Batch,
    ) -> StorageResult<i64> {
        self.inner.produce(transaction_id, topition, batch).await
    }

    async fn fetch(
        &self,
        topition: &'_ Topition,
        offset: i64,
        min_bytes: u32,
        limit: ByteLimit,
        isolation: IsolationLevel,
        fetched_sender: FetchedSender,
    ) -> StorageResult<ReadEnd> {
        self.inner
            .fetch(
                topition,
                offset,
                min_bytes,
                limit,
                isolation,
                fetched_sender,
            )
            .await
    }

    async fn offset_stage(&self, topition: &Topition) -> StorageResult<OffsetStage> {
        self.inner.offset_stage(topition).await
    }

    async fn list_offsets(
        &self,
        isolation_level: IsolationLevel,
        offsets: &[(Topition, ListOffset)],
    ) -> StorageResult<Vec<(Topition, ListOffsetResponse)>> {
        self.inner.list_offsets(isolation_level, offsets).await
    }

    async fn offset_commit(
        &self,
        group_id: &str,
        retention_time_ms: Option<Duration>,
        offsets: &[(Topition, OffsetCommitRequest)],
    ) -> StorageResult<Vec<(Topition, ErrorCode)>> {
        self.offset_commit_available()?;
        self.inner
            .offset_commit(group_id, retention_time_ms, offsets)
            .await
    }

    async fn offset_fetch(
        &self,
        group_id: Option<&str>,
        topics: &[Topition],
        require_stable: Option<bool>,
    ) -> StorageResult<BTreeMap<Topition, CommittedOffset>> {
        self.read_available()?;
        self.inner
            .offset_fetch(group_id, topics, require_stable)
            .await
    }

    async fn committed_offset_topitions(
        &self,
        group_id: &str,
    ) -> StorageResult<BTreeMap<Topition, CommittedOffset>> {
        self.read_available()?;
        self.inner.committed_offset_topitions(group_id).await
    }

    async fn pending_txn_offsets(&self, group_id: &str) -> StorageResult<BTreeSet<Topition>> {
        self.read_available()?;
        self.inner.pending_txn_offsets(group_id).await
    }

    async fn metadata(&self, topics: Option<&[TopicId]>) -> StorageResult<MetadataResponse> {
        self.inner.metadata(topics).await
    }

    async fn upsert_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
        credential: ScramCredential,
    ) -> StorageResult<()> {
        self.inner
            .upsert_user_scram_credential(user, mechanism, credential)
            .await
    }

    async fn delete_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> StorageResult<()> {
        self.inner
            .delete_user_scram_credential(user, mechanism)
            .await
    }

    async fn user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> StorageResult<Option<ScramCredential>> {
        self.inner.user_scram_credential(user, mechanism).await
    }

    async fn describe_config(
        &self,
        name: &str,
        resource: ConfigResource,
        keys: Option<&[String]>,
    ) -> StorageResult<DescribeConfigsResult> {
        self.inner.describe_config(name, resource, keys).await
    }

    async fn list_groups(&self) -> StorageResult<Vec<NamedGroupDetail>> {
        self.read_available()?;
        self.inner.list_groups().await
    }

    async fn delete_groups(
        &self,
        group_ids: Option<&[String]>,
    ) -> StorageResult<Vec<DeletableGroupResult>> {
        self.write_available()?;
        self.inner.delete_groups(group_ids).await
    }

    async fn describe_groups(
        &self,
        group_ids: Option<&[String]>,
        include_authorized_operations: bool,
    ) -> StorageResult<Vec<NamedGroupDetail>> {
        self.read_available()?;
        self.inner
            .describe_groups(group_ids, include_authorized_operations)
            .await
    }

    async fn describe_topic_partitions(
        &self,
        topics: Option<&[TopicId]>,
        partition_limit: i32,
        cursor: Option<Topition>,
    ) -> StorageResult<Vec<DescribeTopicPartitionsResponseTopic>> {
        self.inner
            .describe_topic_partitions(topics, partition_limit, cursor)
            .await
    }

    async fn group_with_version(
        &self,
        group_id: &str,
    ) -> StorageResult<Option<(GroupDetail, Version)>> {
        self.read_available()?;
        self.inner.group_with_version(group_id).await
    }

    async fn update_group(
        &self,
        group_id: &str,
        detail: GroupDetail,
        version: Option<Version>,
    ) -> StorageResult<Version, UpdateError<GroupDetail>> {
        self.write_available()?;
        self.inner.update_group(group_id, detail, version).await
    }

    async fn init_producer(
        &self,
        transaction_id: Option<&str>,
        transaction_timeout_ms: i32,
        producer_id: Option<i64>,
        producer_epoch: Option<i16>,
    ) -> StorageResult<ProducerIdResponse> {
        self.inner
            .init_producer(
                transaction_id,
                transaction_timeout_ms,
                producer_id,
                producer_epoch,
            )
            .await
    }

    async fn txn_add_offsets(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        group_id: &str,
    ) -> StorageResult<ErrorCode> {
        self.inner
            .txn_add_offsets(transaction_id, producer_id, producer_epoch, group_id)
            .await
    }

    async fn txn_add_partitions(
        &self,
        partitions: TxnAddPartitionsRequest,
    ) -> StorageResult<TxnAddPartitionsResponse> {
        self.inner.txn_add_partitions(partitions).await
    }

    async fn txn_offset_commit(
        &self,
        offsets: TxnOffsetCommitRequest,
    ) -> StorageResult<Vec<TxnOffsetCommitResponseTopic>> {
        self.inner.txn_offset_commit(offsets).await
    }

    async fn txn_end(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
    ) -> StorageResult<ErrorCode> {
        self.inner
            .txn_end(transaction_id, producer_id, producer_epoch, committed)
            .await
    }

    async fn maintain(&self, now: SystemTime) -> StorageResult<()> {
        self.inner.maintain(now).await
    }

    async fn maintain_transactions(&self, now: SystemTime) -> StorageResult<()> {
        self.inner.maintain_transactions(now).await
    }

    async fn aborted_transactions(
        &self,
        topition: &Topition,
        offset: i64,
        last_stable_offset: i64,
    ) -> StorageResult<Vec<AbortedTransaction>> {
        self.inner
            .aborted_transactions(topition, offset, last_stable_offset)
            .await
    }

    async fn cluster_id(&self) -> StorageResult<String> {
        self.inner.cluster_id().await
    }

    async fn node(&self) -> StorageResult<i32> {
        self.inner.node().await
    }

    async fn advertised_listener(&self) -> StorageResult<Url> {
        self.inner.advertised_listener().await
    }

    async fn ping(&self) -> StorageResult<()> {
        self.inner.ping().await
    }
}
