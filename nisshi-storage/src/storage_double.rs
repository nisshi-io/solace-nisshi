// Copyright ⓒ Solace Corporation
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

//! The [`storage_double!`] macro, which writes the [`Storage`](crate::Storage) impl of a unit
//! test's double.
//!
//! This module holds only the macro. A double belongs in the `tests` module that uses it.

/// Implements [`Storage`](crate::Storage) for a double with the methods given, and leaves every
/// other method unimplemented.
///
/// The macro has two forms:
///
/// - `storage_double!(Double, async fn fetch(&self, fetched_sender) { ... })` gives only the
///   body of [`Storage::fetch`](crate::Storage::fetch), and leaves every other method
///   unimplemented.
/// - `storage_double!(Double { ... })` takes whole methods. The double gives `fetch`,
///   `offset_stage` and `metadata` itself, because this form leaves them out.
///
/// The macro puts the impl in a `const _` block with its own imports of the types in the
/// signatures of [`Storage`](crate::Storage), so that a file that uses the macro does not import
/// them. The methods that a double gives still see the imports of the file that uses the macro.
macro_rules! storage_double {
    // The caller names `self` and the sender, because a body that the caller writes can't see a
    // binding that the macro writes.
    ($double:ty, async fn fetch(&$self:ident, $fetched_sender:ident) $body:block) => {
        $crate::storage_double::storage_double!($double {
            async fn fetch(
                &$self,
                _topition: &Topition,
                _offset: i64,
                _min_bytes: u32,
                _max_bytes: u32,
                _isolation_level: IsolationLevel,
                $fetched_sender: FetchedSender,
            ) -> Result<()> $body

            async fn offset_stage(&self, _topition: &Topition) -> Result<OffsetStage> {
                unimplemented!()
            }

            async fn metadata(&self, _topics: Option<&[TopicId]>) -> Result<MetadataResponse> {
                unimplemented!()
            }
        });
    };

    ($double:ty { $($method:item)* }) => {
        const _: () = {
            use std::{
                collections::{BTreeMap, BTreeSet},
                time::{Duration, SystemTime},
            };

            use async_trait::async_trait;
            use nisshi_sans_io::{
                ConfigResource, ErrorCode, IsolationLevel, ListOffset, ScramMechanism,
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
                record::deflated,
                txn_offset_commit_response::TxnOffsetCommitResponseTopic,
            };
            use url::Url;
            use uuid::Uuid;

            use $crate::{
                BrokerRegistrationRequest, CommittedOffset, GroupDetail, ListOffsetResponse,
                MetadataResponse, NamedGroupDetail, OffsetCommitRequest, OffsetStage,
                ProducerIdResponse, Result, ScramCredential, Storage, TopicId, Topition,
                TxnAddPartitionsRequest, TxnAddPartitionsResponse, TxnOffsetCommitRequest,
                UpdateError, Version, fetched::FetchedSender,
            };

            #[async_trait]
            impl Storage for $double {
                $($method)*

                async fn register_broker(
                    &self,
                    _broker_registration: BrokerRegistrationRequest,
                ) -> Result<()> {
                    unimplemented!()
                }

                async fn brokers(&self) -> Result<Vec<DescribeClusterBroker>> {
                    unimplemented!()
                }

                async fn create_topic(
                    &self,
                    _topic: CreatableTopic,
                    _validate_only: bool,
                ) -> Result<Uuid> {
                    unimplemented!()
                }

                async fn delete_records(
                    &self,
                    _topics: &[DeleteRecordsTopic],
                ) -> Result<Vec<DeleteRecordsTopicResult>> {
                    unimplemented!()
                }

                async fn delete_topic(&self, _topic: &TopicId) -> Result<ErrorCode> {
                    unimplemented!()
                }

                async fn incremental_alter_resource(
                    &self,
                    _resource: AlterConfigsResource,
                ) -> Result<AlterConfigsResourceResponse> {
                    unimplemented!()
                }

                async fn produce(
                    &self,
                    _transaction_id: Option<&str>,
                    _topition: &Topition,
                    _deflated: deflated::Batch,
                ) -> Result<i64> {
                    unimplemented!()
                }

                async fn offset_commit(
                    &self,
                    _group: &str,
                    _retention: Option<Duration>,
                    _offsets: &[(Topition, OffsetCommitRequest)],
                ) -> Result<Vec<(Topition, ErrorCode)>> {
                    unimplemented!()
                }

                async fn committed_offset_topitions(
                    &self,
                    _group_id: &str,
                ) -> Result<BTreeMap<Topition, CommittedOffset>> {
                    unimplemented!()
                }

                async fn pending_txn_offsets(&self, _group_id: &str) -> Result<BTreeSet<Topition>> {
                    unimplemented!()
                }

                async fn offset_fetch(
                    &self,
                    _group_id: Option<&str>,
                    _topics: &[Topition],
                    _require_stable: Option<bool>,
                ) -> Result<BTreeMap<Topition, CommittedOffset>> {
                    unimplemented!()
                }

                async fn list_offsets(
                    &self,
                    _isolation_level: IsolationLevel,
                    _offsets: &[(Topition, ListOffset)],
                ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
                    unimplemented!()
                }

                async fn describe_config(
                    &self,
                    _name: &str,
                    _resource: ConfigResource,
                    _keys: Option<&[String]>,
                ) -> Result<DescribeConfigsResult> {
                    unimplemented!()
                }

                async fn describe_topic_partitions(
                    &self,
                    _topics: Option<&[TopicId]>,
                    _partition_limit: i32,
                    _cursor: Option<Topition>,
                ) -> Result<Vec<DescribeTopicPartitionsResponseTopic>> {
                    unimplemented!()
                }

                async fn list_groups(&self) -> Result<Vec<NamedGroupDetail>> {
                    unimplemented!()
                }

                async fn delete_groups(
                    &self,
                    _group_ids: Option<&[String]>,
                ) -> Result<Vec<DeletableGroupResult>> {
                    unimplemented!()
                }

                async fn describe_groups(
                    &self,
                    _group_ids: Option<&[String]>,
                    _include_authorized_operations: bool,
                ) -> Result<Vec<NamedGroupDetail>> {
                    unimplemented!()
                }

                async fn group_with_version(
                    &self,
                    _group_id: &str,
                ) -> Result<Option<(GroupDetail, Version)>> {
                    unimplemented!()
                }

                async fn update_group(
                    &self,
                    _group_id: &str,
                    _detail: GroupDetail,
                    _version: Option<Version>,
                ) -> Result<Version, UpdateError<GroupDetail>> {
                    unimplemented!()
                }

                async fn init_producer(
                    &self,
                    _transaction_id: Option<&str>,
                    _transaction_timeout_ms: i32,
                    _producer_id: Option<i64>,
                    _producer_epoch: Option<i16>,
                ) -> Result<ProducerIdResponse> {
                    unimplemented!()
                }

                async fn txn_add_offsets(
                    &self,
                    _transaction_id: &str,
                    _producer_id: i64,
                    _producer_epoch: i16,
                    _group_id: &str,
                ) -> Result<ErrorCode> {
                    unimplemented!()
                }

                async fn txn_add_partitions(
                    &self,
                    _partitions: TxnAddPartitionsRequest,
                ) -> Result<TxnAddPartitionsResponse> {
                    unimplemented!()
                }

                async fn txn_offset_commit(
                    &self,
                    _offsets: TxnOffsetCommitRequest,
                ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
                    unimplemented!()
                }

                async fn txn_end(
                    &self,
                    _transaction_id: &str,
                    _producer_id: i64,
                    _producer_epoch: i16,
                    _committed: bool,
                ) -> Result<ErrorCode> {
                    unimplemented!()
                }

                async fn maintain(&self, _now: SystemTime) -> Result<()> {
                    unimplemented!()
                }

                async fn maintain_transactions(&self, _now: SystemTime) -> Result<()> {
                    unimplemented!()
                }

                async fn aborted_transactions(
                    &self,
                    _topition: &Topition,
                    _offset: i64,
                    _last_stable_offset: i64,
                ) -> Result<Vec<AbortedTransaction>> {
                    unimplemented!()
                }

                async fn cluster_id(&self) -> Result<String> {
                    unimplemented!()
                }

                async fn node(&self) -> Result<i32> {
                    unimplemented!()
                }

                async fn advertised_listener(&self) -> Result<Url> {
                    unimplemented!()
                }

                async fn ping(&self) -> Result<()> {
                    unimplemented!()
                }

                async fn delete_user_scram_credential(
                    &self,
                    _user: &str,
                    _mechanism: ScramMechanism,
                ) -> Result<()> {
                    unimplemented!()
                }

                async fn upsert_user_scram_credential(
                    &self,
                    _user: &str,
                    _mechanism: ScramMechanism,
                    _credential: ScramCredential,
                ) -> Result<()> {
                    unimplemented!()
                }

                async fn user_scram_credential(
                    &self,
                    _user: &str,
                    _mechanism: ScramMechanism,
                ) -> Result<Option<ScramCredential>> {
                    unimplemented!()
                }
            }
        };
    };
}

pub(crate) use storage_double;
