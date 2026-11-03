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

//! An `OffsetFetch` whose storage fails reports `COORDINATOR_NOT_AVAILABLE`, a
//! retriable error, in the fields that each request version reads.

use std::{
    collections::{BTreeMap, BTreeSet},
    time::{Duration, SystemTime},
};

use crate::common::init_tracing;
use async_trait::async_trait;
use nisshi_broker::{
    Result,
    coordinator::group::{Coordinator as _, administrator::Controller},
};
use nisshi_sans_io::{
    ConfigResource, ErrorCode, IsolationLevel, ListOffset, OffsetFetchResponse, ScramMechanism,
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
    offset_fetch_request::{
        OffsetFetchRequestGroup, OffsetFetchRequestTopic, OffsetFetchRequestTopics,
    },
    record::deflated::Batch,
    txn_offset_commit_response::TxnOffsetCommitResponseTopic,
};
use nisshi_storage::{
    BrokerRegistrationRequest, CommittedOffset, Error, GroupDetail, ListOffsetResponse,
    MetadataResponse, NamedGroupDetail, OffsetCommitRequest, OffsetStage, ProducerIdResponse,
    ScramCredential, Storage, TopicId, Topition, TxnAddPartitionsRequest, TxnAddPartitionsResponse,
    TxnOffsetCommitRequest, UpdateError, Version,
};
use tracing::instrument;
use url::Url;
use uuid::Uuid;

const TOPIC: &str = "t";
const OFFSET: i64 = 42;

const FAILING: &str = "failing";
const CORRUPT: &str = "corrupt";
const HEALTHY: &str = "healthy";

/// A storage that fails to read the committed offsets of each group in
/// `failing`, returns `UNKNOWN_SERVER_ERROR` for [`CORRUPT`], and returns
/// [`OFFSET`] for every partition of any other group.
#[derive(Clone, Debug, Default, Hash)]
struct FailingOffsets {
    failing: BTreeSet<String>,
}

impl FailingOffsets {
    fn new(failing: &[&str]) -> Self {
        Self {
            failing: failing.iter().map(|group| (*group).to_owned()).collect(),
        }
    }

    fn offsets(
        &self,
        group_id: &str,
        topics: &[Topition],
    ) -> nisshi_storage::Result<BTreeMap<Topition, CommittedOffset>> {
        if self.failing.contains(group_id) {
            Err(Error::Message(format!(
                "offsets of {group_id} are unavailable"
            )))
        } else if group_id == CORRUPT {
            Err(Error::Api(ErrorCode::UnknownServerError))
        } else {
            Ok(topics
                .iter()
                .map(|topition| {
                    (
                        topition.clone(),
                        CommittedOffset {
                            offset: OFFSET,
                            leader_epoch: None,
                            metadata: None,
                        },
                    )
                })
                .collect())
        }
    }
}

fn request_topics() -> Vec<OffsetFetchRequestTopic> {
    vec![
        OffsetFetchRequestTopic::default()
            .name(TOPIC.into())
            .partition_indexes(Some(vec![0, 1])),
    ]
}

fn request_group(group_id: &str) -> OffsetFetchRequestGroup {
    OffsetFetchRequestGroup::default()
        .group_id(group_id.into())
        .member_id(None)
        .member_epoch(Some(-1))
        .topics(Some(vec![
            OffsetFetchRequestTopics::default()
                .name(TOPIC.into())
                .partition_indexes(Some(vec![0])),
        ]))
}

#[tokio::test]
async fn single_group_reports_error_at_top_level_and_on_each_partition() -> Result<()> {
    let _guard = init_tracing()?;

    let controller = Controller::with_storage(FailingOffsets::new(&[FAILING]))?;

    let topics = request_topics();

    let response = controller
        .offset_fetch(Some(FAILING), Some(&topics), None, Some(false))
        .await
        .and_then(|body| OffsetFetchResponse::try_from(body).map_err(Into::into))?;

    assert_eq!(
        Some(i16::from(ErrorCode::CoordinatorNotAvailable)),
        response.error_code
    );

    let topics = response.topics.unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(TOPIC, topics[0].name);

    let partitions = topics[0].partitions.as_deref().unwrap_or_default();
    assert_eq!(
        vec![0, 1],
        partitions
            .iter()
            .map(|partition| partition.partition_index)
            .collect::<Vec<_>>()
    );

    for partition in partitions {
        assert_eq!(-1, partition.committed_offset);
        assert_eq!(
            i16::from(ErrorCode::CoordinatorNotAvailable),
            partition.error_code
        );
    }

    Ok(())
}

#[tokio::test]
async fn single_group_without_error() -> Result<()> {
    let _guard = init_tracing()?;

    let controller = Controller::with_storage(FailingOffsets::new(&[FAILING]))?;

    let topics = request_topics();

    let response = controller
        .offset_fetch(Some(HEALTHY), Some(&topics), None, Some(false))
        .await
        .and_then(|body| OffsetFetchResponse::try_from(body).map_err(Into::into))?;

    assert_eq!(Some(i16::from(ErrorCode::None)), response.error_code);

    let topics = response.topics.unwrap_or_default();
    let partitions = topics[0].partitions.as_deref().unwrap_or_default();
    assert_eq!(2, partitions.len());
    assert!(
        partitions
            .iter()
            .all(|partition| partition.committed_offset == OFFSET)
    );

    Ok(())
}

#[tokio::test]
async fn storage_error_code_is_kept() -> Result<()> {
    let _guard = init_tracing()?;

    let controller = Controller::with_storage(FailingOffsets::new(&[FAILING]))?;

    let topics = request_topics();

    let response = controller
        .offset_fetch(Some(CORRUPT), Some(&topics), None, Some(false))
        .await
        .and_then(|body| OffsetFetchResponse::try_from(body).map_err(Into::into))?;

    assert_eq!(
        Some(i16::from(ErrorCode::UnknownServerError)),
        response.error_code
    );

    let groups = [request_group(CORRUPT)];

    let response = controller
        .offset_fetch(None, None, Some(&groups), Some(false))
        .await
        .and_then(|body| OffsetFetchResponse::try_from(body).map_err(Into::into))?;

    let groups = response.groups.unwrap_or_default();
    assert_eq!(1, groups.len());
    assert_eq!(
        i16::from(ErrorCode::UnknownServerError),
        groups[0].error_code
    );

    Ok(())
}

#[tokio::test]
async fn each_group_reports_its_own_error() -> Result<()> {
    let _guard = init_tracing()?;

    let controller = Controller::with_storage(FailingOffsets::new(&[FAILING]))?;

    let groups = [
        request_group(FAILING),
        request_group(HEALTHY),
        request_group(FAILING).topics(None),
        request_group(HEALTHY).topics(None),
    ];

    let response = controller
        .offset_fetch(None, None, Some(&groups), Some(false))
        .await
        .and_then(|body| OffsetFetchResponse::try_from(body).map_err(Into::into))?;

    let groups = response.groups.unwrap_or_default();
    assert_eq!(4, groups.len());

    for group in &groups {
        let topics = group.topics.as_deref().unwrap_or_default();

        if group.group_id == FAILING {
            assert_eq!(
                i16::from(ErrorCode::CoordinatorNotAvailable),
                group.error_code
            );
            assert!(topics.is_empty());
        } else {
            assert_eq!(HEALTHY, group.group_id);
            assert_eq!(i16::from(ErrorCode::None), group.error_code);
            assert_eq!(1, topics.len());

            let partitions = topics[0].partitions.as_deref().unwrap_or_default();
            assert_eq!(1, partitions.len());
            assert_eq!(OFFSET, partitions[0].committed_offset);
        }
    }

    Ok(())
}

#[async_trait]
impl Storage for FailingOffsets {
    #[instrument(skip_all)]
    async fn register_broker(
        &self,
        _broker_registration: BrokerRegistrationRequest,
    ) -> nisshi_storage::Result<()> {
        unimplemented!()
    }

    #[instrument(skip_all)]
    async fn brokers(&self) -> nisshi_storage::Result<Vec<DescribeClusterBroker>> {
        unimplemented!()
    }

    #[instrument(skip_all)]
    async fn create_topic(
        &self,
        _topic: CreatableTopic,
        _validate_only: bool,
    ) -> nisshi_storage::Result<Uuid> {
        unimplemented!()
    }

    #[instrument(skip_all)]
    async fn delete_records(
        &self,
        _topics: &[DeleteRecordsTopic],
    ) -> nisshi_storage::Result<Vec<DeleteRecordsTopicResult>> {
        unimplemented!()
    }

    #[instrument(skip_all)]
    async fn delete_topic(&self, _topic: &TopicId) -> nisshi_storage::Result<ErrorCode> {
        unimplemented!()
    }

    #[instrument(skip_all)]
    async fn incremental_alter_resource(
        &self,
        _resource: AlterConfigsResource,
    ) -> nisshi_storage::Result<AlterConfigsResourceResponse> {
        unimplemented!()
    }

    #[instrument(skip_all)]
    async fn produce(
        &self,
        _transaction_id: Option<&str>,
        _topition: &Topition,
        _deflated: Batch,
    ) -> nisshi_storage::Result<i64> {
        unimplemented!()
    }

    #[instrument(skip_all)]
    async fn fetch(
        &self,
        _topition: &Topition,
        _offset: i64,
        _min_bytes: u32,
        _max_bytes: u32,
        _isolation_level: IsolationLevel,
        _max_wait: Duration,
    ) -> nisshi_storage::Result<Vec<Batch>> {
        unimplemented!()
    }

    #[instrument(skip_all)]
    async fn offset_stage(&self, _topition: &Topition) -> nisshi_storage::Result<OffsetStage> {
        unimplemented!()
    }

    #[instrument(skip_all)]
    async fn offset_commit(
        &self,
        _group: &str,
        _retention: Option<Duration>,
        _offsets: &[(Topition, OffsetCommitRequest)],
    ) -> nisshi_storage::Result<Vec<(Topition, ErrorCode)>> {
        unimplemented!()
    }

    #[instrument(skip_all)]
    async fn committed_offset_topitions(
        &self,
        group_id: &str,
    ) -> nisshi_storage::Result<BTreeMap<Topition, CommittedOffset>> {
        self.offsets(group_id, &[Topition::new(TOPIC, 0)])
    }

    #[instrument(skip_all)]
    async fn pending_txn_offsets(
        &self,
        _group_id: &str,
    ) -> nisshi_storage::Result<BTreeSet<Topition>> {
        Ok(BTreeSet::new())
    }

    #[instrument(skip_all)]
    async fn offset_fetch(
        &self,
        group_id: Option<&str>,
        topics: &[Topition],
        _require_stable: Option<bool>,
    ) -> nisshi_storage::Result<BTreeMap<Topition, CommittedOffset>> {
        self.offsets(group_id.unwrap_or_default(), topics)
    }

    #[instrument(skip_all)]
    async fn list_offsets(
        &self,
        _isolation_level: IsolationLevel,
        _offsets: &[(Topition, ListOffset)],
    ) -> nisshi_storage::Result<Vec<(Topition, ListOffsetResponse)>> {
        unimplemented!()
    }

    #[instrument(skip_all)]
    async fn metadata(
        &self,
        _topics: Option<&[TopicId]>,
    ) -> nisshi_storage::Result<MetadataResponse> {
        unimplemented!()
    }

    #[instrument(skip_all)]
    async fn describe_config(
        &self,
        _name: &str,
        _resource: ConfigResource,
        _keys: Option<&[String]>,
    ) -> nisshi_storage::Result<DescribeConfigsResult> {
        unimplemented!()
    }

    #[instrument(skip_all)]
    async fn describe_topic_partitions(
        &self,
        _topics: Option<&[TopicId]>,
        _partition_limit: i32,
        _cursor: Option<Topition>,
    ) -> nisshi_storage::Result<Vec<DescribeTopicPartitionsResponseTopic>> {
        unimplemented!()
    }

    #[instrument(skip_all)]
    async fn list_groups(&self) -> nisshi_storage::Result<Vec<NamedGroupDetail>> {
        unimplemented!()
    }

    #[instrument(skip_all)]
    async fn delete_groups(
        &self,
        _group_ids: Option<&[String]>,
    ) -> nisshi_storage::Result<Vec<DeletableGroupResult>> {
        unimplemented!()
    }

    #[instrument(skip_all)]
    async fn describe_groups(
        &self,
        _group_ids: Option<&[String]>,
        _include_authorized_operations: bool,
    ) -> nisshi_storage::Result<Vec<NamedGroupDetail>> {
        unimplemented!()
    }

    #[instrument(skip_all)]
    async fn group_with_version(
        &self,
        _group_id: &str,
    ) -> nisshi_storage::Result<Option<(GroupDetail, Version)>> {
        unimplemented!()
    }

    #[instrument(skip_all)]
    async fn update_group(
        &self,
        _group_id: &str,
        _detail: GroupDetail,
        _version: Option<Version>,
    ) -> nisshi_storage::Result<Version, UpdateError<GroupDetail>> {
        unimplemented!()
    }

    #[instrument(skip_all)]
    async fn init_producer(
        &self,
        _transaction_id: Option<&str>,
        _transaction_timeout_ms: i32,
        _producer_id: Option<i64>,
        _producer_epoch: Option<i16>,
    ) -> nisshi_storage::Result<ProducerIdResponse> {
        unimplemented!()
    }

    #[instrument(skip_all)]
    async fn txn_add_offsets(
        &self,
        _transaction_id: &str,
        _producer_id: i64,
        _producer_epoch: i16,
        _group_id: &str,
    ) -> nisshi_storage::Result<ErrorCode> {
        unimplemented!()
    }

    #[instrument(skip_all)]
    async fn txn_add_partitions(
        &self,
        _partitions: TxnAddPartitionsRequest,
    ) -> nisshi_storage::Result<TxnAddPartitionsResponse> {
        unimplemented!()
    }

    #[instrument(skip_all)]
    async fn txn_offset_commit(
        &self,
        _offsets: TxnOffsetCommitRequest,
    ) -> nisshi_storage::Result<Vec<TxnOffsetCommitResponseTopic>> {
        unimplemented!()
    }

    #[instrument(skip_all)]
    async fn txn_end(
        &self,
        _transaction_id: &str,
        _producer_id: i64,
        _producer_epoch: i16,
        _committed: bool,
    ) -> nisshi_storage::Result<ErrorCode> {
        unimplemented!()
    }

    #[instrument(skip_all)]
    async fn maintain(&self, _now: SystemTime) -> nisshi_storage::Result<()> {
        unimplemented!()
    }

    #[instrument(skip_all)]
    async fn maintain_transactions(&self, _now: SystemTime) -> nisshi_storage::Result<()> {
        unimplemented!()
    }

    #[instrument(skip_all)]
    async fn aborted_transactions(
        &self,
        _topition: &Topition,
        _offset: i64,
        _last_stable_offset: i64,
    ) -> nisshi_storage::Result<Vec<AbortedTransaction>> {
        unimplemented!()
    }

    #[instrument(skip_all)]
    async fn cluster_id(&self) -> nisshi_storage::Result<String> {
        unimplemented!()
    }

    #[instrument(skip_all)]
    async fn node(&self) -> nisshi_storage::Result<i32> {
        unimplemented!()
    }

    #[instrument(skip_all)]
    async fn advertised_listener(&self) -> nisshi_storage::Result<Url> {
        unimplemented!()
    }

    #[instrument(skip_all)]
    async fn ping(&self) -> nisshi_storage::Result<()> {
        unimplemented!()
    }

    #[instrument(skip_all)]
    async fn delete_user_scram_credential(
        &self,
        _user: &str,
        _mechanism: ScramMechanism,
    ) -> nisshi_storage::Result<()> {
        unimplemented!()
    }

    #[instrument(skip_all)]
    async fn upsert_user_scram_credential(
        &self,
        _user: &str,
        _mechanism: ScramMechanism,
        _credential: ScramCredential,
    ) -> nisshi_storage::Result<()> {
        unimplemented!()
    }

    #[instrument(skip_all)]
    async fn user_scram_credential(
        &self,
        _user: &str,
        _mechanism: ScramMechanism,
    ) -> nisshi_storage::Result<Option<ScramCredential>> {
        unimplemented!()
    }
}
