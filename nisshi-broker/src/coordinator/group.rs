// Copyright ⓒ 2024-2025 Peter Morgan <peter.james.morgan@gmail.com>
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

pub mod administrator;

use crate::Result;
use async_trait::async_trait;
use nisshi_sans_io::{
    Body,
    join_group_request::JoinGroupRequestProtocol,
    leave_group_request::MemberIdentity,
    offset_commit_request::OffsetCommitRequestTopic,
    offset_fetch_request::{OffsetFetchRequestGroup, OffsetFetchRequestTopic},
    sync_group_request::SyncGroupRequestAssignment,
};
use std::fmt::Debug;

#[derive(Debug)]
pub struct OffsetCommit<'a> {
    pub group_id: &'a str,
    pub generation_id_or_member_epoch: Option<i32>,
    pub member_id: Option<&'a str>,
    pub group_instance_id: Option<&'a str>,
    pub retention_time_ms: Option<i64>,
    pub topics: Option<&'a [OffsetCommitRequestTopic]>,
    /// Whether this is the group check of a `TxnOffsetCommit`, which Kafka does not hold to the
    /// group's state: a group that waits for its leader's assignment does not refuse it, and
    /// it is not a contact from the member.
    pub is_transactional: bool,
}

#[async_trait]
pub trait Coordinator: Clone + Debug + Send + Sync + 'static {
    /// Handles a `JoinGroup` of `api_version`, which decides whether a dynamic member without a
    /// member id gets `MEMBER_ID_REQUIRED`. A `rebalance_timeout_ms` of `None`, from version 0,
    /// means the session timeout. `client_host` is the client's address as `DescribeGroups`
    /// reports it, or `None` for a connection that the broker did not accept itself.
    #[allow(clippy::too_many_arguments)]
    async fn join(
        &self,
        api_version: i16,
        client_id: Option<&str>,
        client_host: Option<&str>,
        group_id: &str,
        session_timeout_ms: i32,
        rebalance_timeout_ms: Option<i32>,
        member_id: &str,
        group_instance_id: Option<&str>,
        protocol_type: &str,
        protocols: Option<&[JoinGroupRequestProtocol]>,
        reason: Option<&str>,
    ) -> Result<Body>;

    #[allow(clippy::too_many_arguments)]
    async fn sync(
        &self,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
        protocol_type: Option<&str>,
        protocol_name: Option<&str>,
        assignments: Option<&[SyncGroupRequestAssignment]>,
    ) -> Result<Body>;

    async fn heartbeat(
        &self,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
    ) -> Result<Body>;

    async fn leave(
        &self,
        group_id: &str,
        member_id: Option<&str>,
        members: Option<&[MemberIdentity]>,
    ) -> Result<Body>;

    async fn offset_commit(&self, detail: OffsetCommit<'_>) -> Result<Body>;

    /// Fences a transactional offset commit as an `OffsetCommit` is fenced: the member, its
    /// generation and its group instance ID must match the group. Returns the error code that
    /// refuses the commit, or `None` when it may go ahead. Stores no offsets.
    async fn txn_offset_commit_fence(
        &self,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
    ) -> Result<Option<nisshi_sans_io::ErrorCode>>;

    async fn offset_fetch(
        &self,
        group_id: Option<&str>,
        topics: Option<&[OffsetFetchRequestTopic]>,
        groups: Option<&[OffsetFetchRequestGroup]>,
        require_stable: Option<bool>,
    ) -> Result<Body>;

    /// Deletes one or more groups, refusing any that still has members
    /// (`NON_EMPTY_GROUP`), and forgetting the in-memory state that this
    /// coordinator held for each group it deletes.
    async fn delete_groups(&self, group_ids: &[String]) -> Result<Body>;

    /// Lists every group whose state matches `states_filter`, with its state at the current time.
    async fn list_groups(&self, states_filter: Option<&[String]>) -> Result<Body>;

    /// Describes each group in `group_ids` as it is at the current time.
    async fn describe_groups(
        &self,
        group_ids: Option<&[String]>,
        include_authorized_operations: bool,
    ) -> Result<Body>;
}
