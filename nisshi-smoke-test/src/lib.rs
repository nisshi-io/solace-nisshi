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

//! Harness for the smoke suite: run the real Kafka CLI tools against a
//! broker on a chosen storage engine, the way a user would.
//!
//! `just smoke <engine>` passes the suite its configuration through the environment, which
//! [`settings`] reads.

// The whole crate is test harness: a failed setup step should panic the test that hit it.
#![allow(clippy::unwrap_used, clippy::expect_used)]

pub mod broker;
mod broker_state_reader;
mod common;
mod kafka_cli;
mod postgres_server;
pub mod settings;
mod storage_url;
mod tls_certificate;

pub use broker::{Broker, FREQUENT_MAINTENANCE, LaunchOptions};
pub use broker_state_reader::BrokerStateReader;
pub use common::{ReservedPort, polling, unique};
pub use kafka_cli::{
    Acks, AnyCommand, ApiVersions, BackgroundTool, ClusterId, CommittedOffset, Consume,
    ConsumeMatching, ConsumedOffsets, ConsumedRecord, ConsumerEvent, CreateTopic, DeleteRecords,
    DescribeConfigs, DescribeGroup, DescribeGroupMembers, DescribeGroupState, DescribeTopic,
    DescribeUsers, DescribedMember, GROUP_NOT_EMPTY_EXCEPTION, GetOffsets, GroupOffsets,
    GroupState, KafkaAcls, KafkaBrokerApiVersions, KafkaCli, KafkaCluster, KafkaConfigs,
    KafkaConsoleConsumer, KafkaConsoleProducer, KafkaConsumerGroups, KafkaDeleteRecords,
    KafkaGetOffsets, KafkaProducerPerfTest, KafkaReassignPartitions, KafkaTopics,
    KafkaVerifiableConsumer, KafkaVerifiableProducer, LOG_APPEND_TIME, ListGroups,
    ListGroupsInState, ListTopics, NO_REASSIGNMENTS, Output, PRINTED_NULL, PartitionAssignor,
    PartitionReplicas, PrintedLine, PrintsPartitionLines, RECORD_TOO_LARGE_EXCEPTION, Record,
    SASL_AUTHENTICATION_EXCEPTION, SASL_MECHANISM_NOT_ENABLED, SECURITY_DISABLED_EXCEPTION,
    ScramLogin, ScramMechanism, SendResult, StdoutLines, StoredRecord, TOPIC_EXISTS_EXCEPTION,
    TRANSACTIONAL_ID_AUTHORIZATION_EXCEPTION, TopicPartition, UNSUPPORTED_VERSION_EXCEPTION,
    VerifiableConsume, VerifiableProduce,
};
pub use postgres_server::PostgresServer;
pub use storage_url::StorageUrl;
pub use tls_certificate::TlsCertificate;

/// The cluster id of the broker that all tests share.
pub const SHARED_CLUSTER_ID: &str = "nisshi-smoke";
