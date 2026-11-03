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

use nisshi_sans_io::{
    AddOffsetsToTxnRequest, AddPartitionsToTxnRequest, AlterUserScramCredentialsRequest,
    ApiKey as _, ConsumerGroupDescribeRequest, CreateAclsRequest, CreateTopicsRequest,
    DeleteAclsRequest, DeleteRecordsRequest, DeleteTopicsRequest, DescribeAclsRequest,
    DescribeClusterRequest, DescribeConfigsRequest, DescribeTopicPartitionsRequest,
    DescribeUserScramCredentialsRequest, EndTxnRequest, FetchRequest, FindCoordinatorRequest,
    IncrementalAlterConfigsRequest, InitProducerIdRequest, ListOffsetsRequest,
    ListPartitionReassignmentsRequest, MetadataRequest, ProduceRequest, TxnOffsetCommitRequest,
};
use nisshi_service::{FrameRequestLayer, FrameRouteBuilder, TransactionAbortableDowngradeLayer};
use nisshi_storage::{
    AlterUserScramCredentialsService, AutoCreateRefusals, ConsumerGroupDescribeService,
    CreateAclsService, CreateTopicsService, CreateTopicsValidationLayer, DeleteAclsService,
    DeleteRecordsService, DeleteTopicsService, DescribeAclsService, DescribeClusterService,
    DescribeConfigsService, DescribeTopicPartitionsService, DescribeUserScramCredentialsService,
    FetchService, FetchValidationLayer, FindCoordinatorService, IncrementalAlterConfigsService,
    InitProducerIdService, ListOffsetsService, ListPartitionReassignmentsService, MetadataService,
    ProduceService, Storage, TopicCreationConfig, TxnAddOffsetsService, TxnAddPartitionService,
    TxnEndService, TxnOffsetCommitService,
};
use rama::{Layer as _, Service as _, layer::MapErrLayer};

use crate::Error;

pub fn services<S>(
    builder: FrameRouteBuilder<Error>,
    storage: S,
    topic_creation: TopicCreationConfig,
    auto_create_refusals: AutoCreateRefusals,
) -> Result<FrameRouteBuilder<Error>, Error>
where
    S: Storage + Clone,
{
    [
        add_offsets_to_txn,
        add_partitions_to_txn,
        alter_user_scram_credentials,
        consumer_group_describe,
        create_acls,
        delete_acls,
        delete_records,
        delete_topics,
        describe_acls,
        describe_cluster,
        describe_configs,
        describe_topic_partitions,
        describe_user_scram_credentials,
        fetch,
        find_coordinator,
        incremental_alter_configs,
        init_producer_id,
        list_offsets,
        list_partition_reassignments,
        produce,
        end_txn,
        txn_offset_commit,
    ]
    .iter()
    .try_fold(builder, |builder, service| {
        service(builder, storage.clone())
    })
    .and_then(|builder| {
        metadata(
            builder,
            storage.clone(),
            topic_creation,
            auto_create_refusals,
        )
    })
    .and_then(|builder| create_topics(builder, storage, topic_creation))
}

pub fn alter_user_scram_credentials<S>(
    builder: FrameRouteBuilder<Error>,
    storage: S,
) -> Result<FrameRouteBuilder<Error>, Error>
where
    S: Storage,
{
    builder
        .with_route(
            AlterUserScramCredentialsRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                FrameRequestLayer::<AlterUserScramCredentialsRequest>::new(),
            )
                .into_layer(AlterUserScramCredentialsService { storage })
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn describe_user_scram_credentials<S>(
    builder: FrameRouteBuilder<Error>,
    storage: S,
) -> Result<FrameRouteBuilder<Error>, Error>
where
    S: Storage,
{
    builder
        .with_route(
            DescribeUserScramCredentialsRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                FrameRequestLayer::<DescribeUserScramCredentialsRequest>::new(),
            )
                .into_layer(DescribeUserScramCredentialsService { storage })
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn create_acls<S>(
    builder: FrameRouteBuilder<Error>,
    storage: S,
) -> Result<FrameRouteBuilder<Error>, Error>
where
    S: Storage,
{
    builder
        .with_route(
            CreateAclsRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                FrameRequestLayer::<CreateAclsRequest>::new(),
            )
                .into_layer(CreateAclsService { storage })
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn delete_acls<S>(
    builder: FrameRouteBuilder<Error>,
    storage: S,
) -> Result<FrameRouteBuilder<Error>, Error>
where
    S: Storage,
{
    builder
        .with_route(
            DeleteAclsRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                FrameRequestLayer::<DeleteAclsRequest>::new(),
            )
                .into_layer(DeleteAclsService { storage })
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn describe_acls<S>(
    builder: FrameRouteBuilder<Error>,
    storage: S,
) -> Result<FrameRouteBuilder<Error>, Error>
where
    S: Storage,
{
    builder
        .with_route(
            DescribeAclsRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                FrameRequestLayer::<DescribeAclsRequest>::new(),
            )
                .into_layer(DescribeAclsService { storage })
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn consumer_group_describe<S>(
    builder: FrameRouteBuilder<Error>,
    storage: S,
) -> Result<FrameRouteBuilder<Error>, Error>
where
    S: Storage,
{
    builder
        .with_route(
            ConsumerGroupDescribeRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                FrameRequestLayer::<ConsumerGroupDescribeRequest>::new(),
            )
                .into_layer(ConsumerGroupDescribeService { storage })
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn create_topics<S>(
    builder: FrameRouteBuilder<Error>,
    storage: S,
    topic_creation: TopicCreationConfig,
) -> Result<FrameRouteBuilder<Error>, Error>
where
    S: Storage,
{
    builder
        .with_route(
            CreateTopicsRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                FrameRequestLayer::<CreateTopicsRequest>::new(),
                CreateTopicsValidationLayer::new().num_partitions(topic_creation.num_partitions),
            )
                .into_layer(CreateTopicsService { storage })
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn delete_records<S>(
    builder: FrameRouteBuilder<Error>,
    storage: S,
) -> Result<FrameRouteBuilder<Error>, Error>
where
    S: Storage,
{
    builder
        .with_route(
            DeleteRecordsRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                FrameRequestLayer::<DeleteRecordsRequest>::new(),
            )
                .into_layer(DeleteRecordsService { storage })
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn delete_topics<S>(
    builder: FrameRouteBuilder<Error>,
    storage: S,
) -> Result<FrameRouteBuilder<Error>, Error>
where
    S: Storage,
{
    builder
        .with_route(
            DeleteTopicsRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                FrameRequestLayer::<DeleteTopicsRequest>::new(),
            )
                .into_layer(DeleteTopicsService { storage })
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn describe_cluster<S>(
    builder: FrameRouteBuilder<Error>,
    storage: S,
) -> Result<FrameRouteBuilder<Error>, Error>
where
    S: Storage,
{
    builder
        .with_route(
            DescribeClusterRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                FrameRequestLayer::<DescribeClusterRequest>::new(),
            )
                .into_layer(DescribeClusterService { storage })
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn describe_configs<S>(
    builder: FrameRouteBuilder<Error>,
    storage: S,
) -> Result<FrameRouteBuilder<Error>, Error>
where
    S: Storage,
{
    builder
        .with_route(
            DescribeConfigsRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                FrameRequestLayer::<DescribeConfigsRequest>::new(),
            )
                .into_layer(DescribeConfigsService { storage })
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn describe_topic_partitions<S>(
    builder: FrameRouteBuilder<Error>,
    storage: S,
) -> Result<FrameRouteBuilder<Error>, Error>
where
    S: Storage,
{
    builder
        .with_route(
            DescribeTopicPartitionsRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                FrameRequestLayer::<DescribeTopicPartitionsRequest>::new(),
            )
                .into_layer(DescribeTopicPartitionsService { storage })
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn fetch<S>(
    builder: FrameRouteBuilder<Error>,
    storage: S,
) -> Result<FrameRouteBuilder<Error>, Error>
where
    S: Storage,
{
    builder
        .with_route(
            FetchRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                FrameRequestLayer::<FetchRequest>::new(),
                FetchValidationLayer::new(),
            )
                .into_layer(FetchService { storage })
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn find_coordinator<S>(
    builder: FrameRouteBuilder<Error>,
    storage: S,
) -> Result<FrameRouteBuilder<Error>, Error>
where
    S: Storage,
{
    builder
        .with_route(
            FindCoordinatorRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                FrameRequestLayer::<FindCoordinatorRequest>::new(),
            )
                .into_layer(FindCoordinatorService { storage })
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn incremental_alter_configs<S>(
    builder: FrameRouteBuilder<Error>,
    storage: S,
) -> Result<FrameRouteBuilder<Error>, Error>
where
    S: Storage,
{
    builder
        .with_route(
            IncrementalAlterConfigsRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                FrameRequestLayer::<IncrementalAlterConfigsRequest>::new(),
            )
                .into_layer(IncrementalAlterConfigsService { storage })
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn init_producer_id<S>(
    builder: FrameRouteBuilder<Error>,
    storage: S,
) -> Result<FrameRouteBuilder<Error>, Error>
where
    S: Storage,
{
    builder
        .with_route(
            InitProducerIdRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                FrameRequestLayer::<InitProducerIdRequest>::new(),
            )
                .into_layer(InitProducerIdService { storage })
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn list_offsets<S>(
    builder: FrameRouteBuilder<Error>,
    storage: S,
) -> Result<FrameRouteBuilder<Error>, Error>
where
    S: Storage,
{
    builder
        .with_capped_route::<ListOffsetsRequest, _>(ListOffsetsService { storage })
        .map_err(Into::into)
}

pub fn list_partition_reassignments<S>(
    builder: FrameRouteBuilder<Error>,
    storage: S,
) -> Result<FrameRouteBuilder<Error>, Error>
where
    S: Storage,
{
    builder
        .with_route(
            ListPartitionReassignmentsRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                FrameRequestLayer::<ListPartitionReassignmentsRequest>::new(),
            )
                .into_layer(ListPartitionReassignmentsService { storage })
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn metadata<S>(
    builder: FrameRouteBuilder<Error>,
    storage: S,
    topic_creation: TopicCreationConfig,
    auto_create_refusals: AutoCreateRefusals,
) -> Result<FrameRouteBuilder<Error>, Error>
where
    S: Storage + Clone,
{
    builder
        .with_route(
            MetadataRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                FrameRequestLayer::<MetadataRequest>::new(),
            )
                .into_layer(MetadataService {
                    storage,
                    topic_creation,
                    auto_create_refusals,
                })
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn produce<S>(
    builder: FrameRouteBuilder<Error>,
    storage: S,
) -> Result<FrameRouteBuilder<Error>, Error>
where
    S: Storage,
{
    builder
        .with_capped_route_layer::<ProduceRequest, _, _>(
            TransactionAbortableDowngradeLayer,
            ProduceService { storage },
        )
        .map_err(Into::into)
}

pub fn add_offsets_to_txn<S>(
    builder: FrameRouteBuilder<Error>,
    storage: S,
) -> Result<FrameRouteBuilder<Error>, Error>
where
    S: Storage,
{
    builder
        .with_route(
            AddOffsetsToTxnRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                FrameRequestLayer::<AddOffsetsToTxnRequest>::new(),
            )
                .into_layer(TxnAddOffsetsService { storage })
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn add_partitions_to_txn<S>(
    builder: FrameRouteBuilder<Error>,
    storage: S,
) -> Result<FrameRouteBuilder<Error>, Error>
where
    S: Storage,
{
    builder
        .with_capped_route::<AddPartitionsToTxnRequest, _>(TxnAddPartitionService { storage })
        .map_err(Into::into)
}

pub fn end_txn<S>(
    builder: FrameRouteBuilder<Error>,
    storage: S,
) -> Result<FrameRouteBuilder<Error>, Error>
where
    S: Storage,
{
    builder
        .with_route(
            EndTxnRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                FrameRequestLayer::<EndTxnRequest>::new(),
            )
                .into_layer(TxnEndService { storage })
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn txn_offset_commit<S>(
    builder: FrameRouteBuilder<Error>,
    storage: S,
) -> Result<FrameRouteBuilder<Error>, Error>
where
    S: Storage,
{
    builder
        .with_route(
            TxnOffsetCommitRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                FrameRequestLayer::<TxnOffsetCommitRequest>::new(),
            )
                .into_layer(TxnOffsetCommitService { storage })
                .boxed(),
        )
        .map_err(Into::into)
}
