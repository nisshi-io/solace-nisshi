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

#![deny(clippy::todo, clippy::unimplemented)]
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::todo,
        clippy::unimplemented
    )
)]
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use nisshi_sans_io::{
    ConfigResource, ErrorCode, IsolationLevel, ListOffset, NULL_TOPIC_ID, ScramMechanism,
    add_partitions_to_txn_response::{AddPartitionsToTxnResult, AddPartitionsToTxnTopicResult},
    create_topics_request::CreatableTopic,
    delete_groups_response::DeletableGroupResult,
    delete_records_request::DeleteRecordsTopic,
    delete_records_response::DeleteRecordsTopicResult,
    describe_cluster_response::DescribeClusterBroker,
    describe_configs_response::DescribeConfigsResult,
    describe_topic_partitions_response::{
        DescribeTopicPartitionsResponsePartition, DescribeTopicPartitionsResponseTopic,
    },
    fetch_response::AbortedTransaction,
    incremental_alter_configs_request::AlterConfigsResource,
    incremental_alter_configs_response::AlterConfigsResourceResponse,
    metadata_response::{MetadataResponseBroker, MetadataResponseTopic},
    record::deflated::Batch,
    txn_offset_commit_response::TxnOffsetCommitResponseTopic,
};
use nisshi_storage::{
    BrokerRegistrationRequest, CommittedOffset, Error, GroupDetail, GroupDetailResponse,
    ListOffsetResponse, MetadataResponse, NamedGroupDetail, OffsetCommitRequest, OffsetStage,
    ProducerIdResponse, Result, ScramCredential, Storage, TopicId, Topition,
    TxnAddPartitionsRequest, TxnAddPartitionsResponse, TxnOffsetCommitRequest, UpdateError,
    Version,
};
use tracing::instrument;
use url::Url;
use uuid::Uuid;

mod factory;

pub use factory::EngineFactory;

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct Group {
    detail: GroupDetail,
    version: Option<Version>,
}

#[derive(Clone, Debug)]
struct Engine {
    cluster: String,
    node: i32,
    advertised_listener: Url,

    topics: Arc<Mutex<Vec<CreatableTopic>>>,
    groups: Arc<Mutex<BTreeMap<String, Group>>>,
}

impl Engine {
    fn new(cluster: String, node: i32, advertised_listener: Url) -> Self {
        Self {
            cluster,
            node,
            advertised_listener,
            topics: Arc::new(Mutex::new(Vec::new())),
            groups: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }
}

/// The engine's name for people.
pub(crate) const NAME: &str = "null (null://)";

const FEATURE: &str = "storage";
const MESSAGE: &str = "storage has not been defined";

#[async_trait]
impl Storage for Engine {
    #[instrument(skip_all)]
    async fn register_broker(&self, _broker_registration: BrokerRegistrationRequest) -> Result<()> {
        Ok(())
    }

    #[instrument(skip_all)]
    async fn brokers(&self) -> Result<Vec<DescribeClusterBroker>> {
        let broker_id = self.node;
        let host = self
            .advertised_listener
            .host_str()
            .unwrap_or("0.0.0.0")
            .into();
        let port = self.advertised_listener.port().unwrap_or(9092).into();
        let rack = None;

        Ok(vec![
            DescribeClusterBroker::default()
                .broker_id(broker_id)
                .host(host)
                .port(port)
                .rack(rack),
        ])
    }

    #[instrument(skip_all)]
    async fn create_topic(&self, topic: CreatableTopic, _validate_only: bool) -> Result<Uuid> {
        self.topics
            .lock()
            .map_err(Into::into)
            .and_then(|mut topics| {
                if topics.iter().any(|existing| existing.name == topic.name) {
                    Err(Error::Api(ErrorCode::TopicAlreadyExists))
                } else {
                    topics.push(topic);
                    Ok(Uuid::now_v7())
                }
            })
    }

    #[instrument(skip_all)]
    async fn delete_records(
        &self,
        _topics: &[DeleteRecordsTopic],
    ) -> Result<Vec<DeleteRecordsTopicResult>> {
        Err(Error::FeatureNotEnabled {
            feature: FEATURE.into(),
            message: MESSAGE.into(),
        })
    }

    #[instrument(skip_all)]
    async fn delete_topic(&self, _topic: &TopicId) -> Result<ErrorCode> {
        Ok(ErrorCode::None)
    }

    #[instrument(skip_all)]
    async fn incremental_alter_resource(
        &self,
        resource: AlterConfigsResource,
    ) -> Result<AlterConfigsResourceResponse> {
        Ok(AlterConfigsResourceResponse::default()
            .error_code(ErrorCode::None.into())
            .error_message(Some(ErrorCode::None.to_string()))
            .resource_name(resource.resource_name)
            .resource_type(resource.resource_type))
    }

    #[instrument(skip_all)]
    async fn produce(
        &self,
        _transaction_id: Option<&str>,
        _topition: &Topition,
        _deflated: Batch,
    ) -> Result<i64> {
        Ok(6)
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
    ) -> Result<Vec<Batch>> {
        Ok([].into())
    }

    #[instrument(skip_all)]
    async fn offset_stage(&self, _topition: &Topition) -> Result<OffsetStage> {
        Ok(OffsetStage::default())
    }

    #[instrument(skip_all)]
    async fn offset_commit(
        &self,
        _group: &str,
        _retention: Option<Duration>,
        offsets: &[(Topition, OffsetCommitRequest)],
    ) -> Result<Vec<(Topition, ErrorCode)>> {
        Ok(offsets
            .iter()
            .map(|(topition, _)| (topition.to_owned(), ErrorCode::None))
            .collect())
    }

    #[instrument(skip_all)]
    async fn committed_offset_topitions(
        &self,
        _group_id: &str,
    ) -> Result<BTreeMap<Topition, CommittedOffset>> {
        Ok(BTreeMap::new())
    }

    /// The null engine keeps no transactional offsets, so none is pending.
    #[instrument(skip_all)]
    async fn pending_txn_offsets(&self, _group_id: &str) -> Result<BTreeSet<Topition>> {
        Ok(BTreeSet::new())
    }

    #[instrument(skip_all)]
    async fn offset_fetch(
        &self,
        _group_id: Option<&str>,
        topics: &[Topition],
        _require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, CommittedOffset>> {
        Ok(topics
            .iter()
            .map(|topition| {
                (
                    topition.to_owned(),
                    CommittedOffset {
                        offset: 0,
                        leader_epoch: None,
                        metadata: None,
                    },
                )
            })
            .collect())
    }

    #[instrument(skip_all)]
    async fn list_offsets(
        &self,
        _isolation_level: IsolationLevel,
        offsets: &[(Topition, ListOffset)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
        Ok(offsets
            .iter()
            .map(|(topition, list_offset)| {
                // No records are stored: Earliest/Latest answer 0, a Timestamp
                // lookup answers no offset (see `ListOffsetResponse::offset`).
                let offset = match list_offset {
                    ListOffset::Earliest | ListOffset::Latest => Some(0),
                    ListOffset::Timestamp(_) => None,
                };

                (
                    topition.to_owned(),
                    ListOffsetResponse {
                        error_code: ErrorCode::None,
                        timestamp: None,
                        offset,
                    },
                )
            })
            .collect())
    }

    #[instrument(skip_all)]
    async fn metadata(&self, _topics: Option<&[TopicId]>) -> Result<MetadataResponse> {
        let node_id = self.node;
        let host = self
            .advertised_listener
            .host_str()
            .unwrap_or("0.0.0.0")
            .into();
        let port = self.advertised_listener.port().unwrap_or(9092).into();
        let rack = None;

        let topics = self
            .topics
            .lock()?
            .iter()
            .map(|topic| {
                nisshi_storage::assign_replicas(
                    &[self.node],
                    topic.num_partitions,
                    topic.replication_factor,
                    ErrorCode::None.into(),
                    -1,
                )
                .map(|partitions| {
                    MetadataResponseTopic::default()
                        .error_code(ErrorCode::None.into())
                        .is_internal(Some(false))
                        .name(Some(topic.name.clone()))
                        .partitions(Some(partitions))
                        .topic_id(Some(NULL_TOPIC_ID))
                        .topic_authorized_operations(Some(i32::MIN))
                })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(MetadataResponse {
            cluster: Some(self.cluster.clone()),
            controller: Some(self.node),
            brokers: [MetadataResponseBroker::default()
                .node_id(node_id)
                .host(host)
                .port(port)
                .rack(rack)]
            .into(),
            topics,
        })
    }

    #[instrument(skip_all)]
    async fn describe_config(
        &self,
        name: &str,
        resource: ConfigResource,
        _keys: Option<&[String]>,
    ) -> Result<DescribeConfigsResult> {
        let error_code = if resource == ConfigResource::Topic
            && !self
                .topics
                .lock()
                .map_err(Error::from)?
                .iter()
                .any(|topic| topic.name == name)
        {
            ErrorCode::UnknownTopicOrPartition
        } else {
            ErrorCode::None
        };

        Ok(DescribeConfigsResult::default()
            .configs(Some([].into()))
            .resource_name(name.to_string())
            .resource_type(resource.into())
            .error_code(error_code.into())
            .error_message(Some(error_code.to_string())))
    }

    #[instrument(skip_all)]
    async fn describe_topic_partitions(
        &self,
        _topics: Option<&[TopicId]>,
        _partition_limit: i32,
        _cursor: Option<Topition>,
    ) -> Result<Vec<DescribeTopicPartitionsResponseTopic>> {
        self.topics.lock().map_err(Into::into).map(|existing| {
            existing
                .iter()
                .map(|existing| {
                    DescribeTopicPartitionsResponseTopic::default()
                        .error_code(ErrorCode::None.into())
                        .name(Some(existing.name.clone()))
                        .partitions(Some(
                            (0..existing.num_partitions)
                                .map(|partition_index| {
                                    DescribeTopicPartitionsResponsePartition::default()
                                        .leader_id(self.node)
                                        .partition_index(partition_index)
                                        .isr_nodes(Some(nisshi_storage::single_node_replicas(
                                            self.node,
                                            existing.replication_factor,
                                        )))
                                })
                                .collect(),
                        ))
                })
                .collect()
        })
    }

    #[instrument(skip_all)]
    async fn list_groups(&self) -> Result<Vec<NamedGroupDetail>> {
        Ok([].into())
    }

    #[instrument(skip_all)]
    async fn delete_groups(
        &self,
        group_ids: Option<&[String]>,
    ) -> Result<Vec<DeletableGroupResult>> {
        Ok(group_ids
            .unwrap_or_default()
            .iter()
            .map(|group_id| {
                DeletableGroupResult::default()
                    .error_code(ErrorCode::None.into())
                    .group_id(group_id.to_owned())
            })
            .collect())
    }

    #[instrument(skip_all)]
    async fn describe_groups(
        &self,
        group_ids: Option<&[String]>,
        _include_authorized_operations: bool,
    ) -> Result<Vec<NamedGroupDetail>> {
        Ok(group_ids
            .unwrap_or_default()
            .iter()
            .map(|name| NamedGroupDetail {
                name: name.to_owned(),
                response: GroupDetailResponse::ErrorCode(ErrorCode::GroupIdNotFound),
            })
            .collect())
    }

    #[instrument(skip_all)]
    async fn group_with_version(&self, group_id: &str) -> Result<Option<(GroupDetail, Version)>> {
        self.groups.lock().map_err(Into::into).map(|groups| {
            groups.get(group_id).map(|group| {
                (
                    group.detail.clone(),
                    group.version.clone().unwrap_or_default(),
                )
            })
        })
    }

    #[instrument(skip_all)]
    async fn update_group(
        &self,
        group_id: &str,
        detail: GroupDetail,
        version: Option<Version>,
    ) -> Result<Version, UpdateError<GroupDetail>> {
        self.groups
            .lock()
            .map_err(|err| UpdateError::Error(err.into()))
            .and_then(|mut groups| {
                let group = groups.entry(group_id.to_string()).or_insert(Group {
                    detail: detail.clone(),
                    version: version.clone(),
                });

                if group.version == version {
                    let id = Uuid::now_v7();
                    let version = Version {
                        e_tag: Some(id.to_string()),
                        version: Some(id.to_string()),
                    };

                    group.detail = detail;
                    group.version = Some(version.clone());

                    Ok(version)
                } else {
                    Err(UpdateError::Outdated {
                        current: Box::new(group.detail.clone()),
                        version: group.version.clone().unwrap_or_default(),
                    })
                }
            })
    }

    #[instrument(skip_all)]
    async fn init_producer(
        &self,
        _transaction_id: Option<&str>,
        _transaction_timeout_ms: i32,
        _producer_id: Option<i64>,
        _producer_epoch: Option<i16>,
    ) -> Result<ProducerIdResponse> {
        Ok(ProducerIdResponse {
            error: ErrorCode::None,
            id: 6,
            epoch: 6,
        })
    }

    #[instrument(skip_all)]
    async fn txn_add_offsets(
        &self,
        _transaction_id: &str,
        _producer_id: i64,
        _producer_epoch: i16,
        _group_id: &str,
    ) -> Result<ErrorCode> {
        Ok(ErrorCode::None)
    }

    #[instrument(skip_all)]
    async fn txn_add_partitions(
        &self,
        partitions: TxnAddPartitionsRequest,
    ) -> Result<TxnAddPartitionsResponse> {
        Ok(match partitions {
            TxnAddPartitionsRequest::VersionZeroToThree { topics, .. } => {
                TxnAddPartitionsResponse::VersionZeroToThree(
                    topics
                        .iter()
                        .map(|topic| {
                            AddPartitionsToTxnTopicResult::default()
                                .name(topic.name.clone())
                                .results_by_partition(Some([].into()))
                        })
                        .collect(),
                )
            }
            TxnAddPartitionsRequest::VersionFourPlus { transactions } => {
                TxnAddPartitionsResponse::VersionFourPlus(
                    transactions
                        .iter()
                        .map(|_| AddPartitionsToTxnResult::default())
                        .collect(),
                )
            }
        })
    }

    #[instrument(skip_all)]
    async fn txn_offset_commit(
        &self,
        offsets: TxnOffsetCommitRequest,
    ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
        Ok(offsets
            .topics
            .iter()
            .map(|topic| TxnOffsetCommitResponseTopic::default().name(topic.name.clone()))
            .collect())
    }

    #[instrument(skip_all)]
    async fn txn_end(
        &self,
        _transaction_id: &str,
        _producer_id: i64,
        _producer_epoch: i16,
        _committed: bool,
    ) -> Result<ErrorCode> {
        Ok(ErrorCode::None)
    }

    #[instrument(skip_all)]
    async fn maintain(&self, _now: SystemTime) -> Result<()> {
        Ok(())
    }

    #[instrument(skip_all)]
    async fn maintain_transactions(&self, _now: SystemTime) -> Result<()> {
        Ok(())
    }

    #[instrument(skip_all)]
    async fn aborted_transactions(
        &self,
        _topition: &Topition,
        _offset: i64,
        _last_stable_offset: i64,
    ) -> Result<Vec<AbortedTransaction>> {
        Ok(vec![])
    }

    #[instrument(skip_all)]
    async fn cluster_id(&self) -> Result<String> {
        Ok(self.cluster.clone())
    }

    #[instrument(skip_all)]
    async fn node(&self) -> Result<i32> {
        Ok(self.node)
    }

    #[instrument(skip_all)]
    async fn advertised_listener(&self) -> Result<Url> {
        Ok(self.advertised_listener.clone())
    }

    #[instrument(skip_all)]
    async fn ping(&self) -> Result<()> {
        Ok(())
    }

    #[instrument(skip_all)]
    async fn delete_user_scram_credential(
        &self,
        _user: &str,
        _mechanism: ScramMechanism,
    ) -> Result<()> {
        Err(Error::UserCredentialsUnsupported {
            engine: NAME.into(),
        })
    }

    #[instrument(skip(_credential), ret)]
    async fn upsert_user_scram_credential(
        &self,
        user: &str,
        _mechanism: ScramMechanism,
        _credential: ScramCredential,
    ) -> Result<()> {
        Err(Error::UserCredentialsUnsupported {
            engine: NAME.into(),
        })
    }

    #[instrument(ret)]
    async fn user_scram_credential(
        &self,
        _user: &str,
        _mechanism: ScramMechanism,
    ) -> Result<Option<ScramCredential>> {
        Err(Error::FeatureNotEnabled {
            feature: FEATURE.into(),
            message: MESSAGE.into(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn list_offsets_timestamp_has_no_offset() -> Result<()> {
        let engine = Engine::new(
            "cluster".into(),
            111,
            Url::parse("tcp://127.0.0.1:9092/").expect("valid url"),
        );
        let topition = Topition::new("t", 0);

        let responses = engine
            .list_offsets(
                IsolationLevel::ReadUncommitted,
                &[
                    (topition.clone(), ListOffset::Earliest),
                    (topition.clone(), ListOffset::Latest),
                    (topition.clone(), ListOffset::Timestamp(SystemTime::now())),
                ],
            )
            .await?;

        let offsets = responses
            .iter()
            .map(|(_, response)| (response.error_code, response.offset))
            .collect::<Vec<_>>();

        assert_eq!(
            vec![
                (ErrorCode::None, Some(0)),
                (ErrorCode::None, Some(0)),
                (ErrorCode::None, None),
            ],
            offsets
        );

        Ok(())
    }

    /// A topic that the engine doesn't track is unknown, as on the other engines, so that
    /// `DescribeConfigsService` doesn't list Kafka's defaults for it.
    #[tokio::test]
    async fn describe_config_of_unknown_topic_is_unknown_topic_or_partition() -> Result<()> {
        let engine = Engine::new(
            "cluster".into(),
            111,
            Url::parse("tcp://127.0.0.1:9092/").expect("valid url"),
        );

        _ = engine
            .create_topic(CreatableTopic::default().name("known".into()), false)
            .await?;

        let known = engine
            .describe_config("known", ConfigResource::Topic, None)
            .await?;
        assert_eq!(i16::from(ErrorCode::None), known.error_code);

        let unknown = engine
            .describe_config("unknown", ConfigResource::Topic, None)
            .await?;
        assert_eq!(
            i16::from(ErrorCode::UnknownTopicOrPartition),
            unknown.error_code
        );
        assert_eq!("unknown", unknown.resource_name);

        Ok(())
    }

    /// A stored replication factor outside what CreateTopics accepts must
    /// describe without a panic: no replicas below one, and at most
    /// `MAX_REPLICATION_FACTOR` replicas above it.
    #[tokio::test]
    async fn stored_out_of_range_replication_factor() -> Result<()> {
        let node = 111;

        for (replication_factor, expected) in [(-2, vec![]), (i16::MAX, vec![node; 64])] {
            let engine = Engine::new(
                "cluster".into(),
                node,
                Url::parse("tcp://127.0.0.1:9092/").expect("valid url"),
            );

            _ = engine
                .create_topic(
                    CreatableTopic::default()
                        .name("t".into())
                        .num_partitions(3)
                        .replication_factor(replication_factor),
                    false,
                )
                .await?;

            let metadata = engine.metadata(None).await?;
            let partitions = metadata.topics[0].partitions.as_deref().unwrap_or_default();
            assert_eq!(3, partitions.len());

            for partition in partitions {
                assert_eq!(Some(&expected), partition.replica_nodes.as_ref());
                assert_eq!(Some(&expected), partition.isr_nodes.as_ref());
            }

            let topics = engine.describe_topic_partitions(None, 2_000, None).await?;
            let partitions = topics[0].partitions.as_deref().unwrap_or_default();
            assert_eq!(3, partitions.len());

            for partition in partitions {
                assert_eq!(Some(&expected), partition.isr_nodes.as_ref());
            }
        }

        Ok(())
    }
}
