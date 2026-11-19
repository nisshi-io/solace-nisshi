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

//! A read of named topics that fails with an error that a retry may clear answers each topic
//! that the service answered before with its partitions and no leader, and returns the error
//! for a topic that it never answered.

use std::{assert_matches, io};

use nisshi_sans_io::{
    ErrorCode, MetadataRequest, MetadataResponse,
    metadata_request::MetadataRequestTopic,
    metadata_response::{MetadataResponsePartition, MetadataResponseTopic},
};
use rama::Service as _;

use crate::{
    AutoCreateRefusals, Error, KnownTopics, MetadataService, Result, TopicCreationConfig,
    service::counting_storage::{CountingStorage, Listing, NODE_ID},
};

const TOPIC: &str = "orders";

fn service(
    storage: &CountingStorage,
    known_topics: &KnownTopics,
) -> MetadataService<CountingStorage> {
    MetadataService {
        storage: storage.clone(),
        topic_creation: TopicCreationConfig::default(),
        auto_create_refusals: AutoCreateRefusals::default(),
        known_topics: known_topics.clone(),
    }
}

/// Sends a Metadata request for `names` that does not allow auto-creation.
async fn metadata(
    service: &MetadataService<CountingStorage>,
    names: &[&str],
) -> Result<MetadataResponse> {
    service
        .serve(
            MetadataRequest::default()
                .allow_auto_topic_creation(Some(false))
                .topics(Some(
                    names
                        .iter()
                        .map(|name| MetadataRequestTopic::default().name(Some((*name).into())))
                        .collect(),
                )),
        )
        .await
}

/// An error that a retry may clear, as a lost connection to the storage is.
fn transient() -> Error {
    Error::from(io::Error::other("storage unreachable"))
}

fn topics(response: MetadataResponse) -> Vec<MetadataResponseTopic> {
    response.topics.unwrap_or_default()
}

#[tokio::test]
async fn a_failed_read_answers_the_partitions_last_read_without_a_leader() -> Result<()> {
    let storage = CountingStorage::new(Listing::Topics, &[TOPIC]);
    let service = service(&storage, &KnownTopics::default());

    _ = metadata(&service, &[TOPIC]).await?;
    storage.fail_named_lookups(Some(transient()));

    let topics = topics(metadata(&service, &[TOPIC]).await?);

    assert_eq!(1, topics.len());
    assert_eq!(Some(TOPIC), topics[0].name.as_deref());
    assert_eq!(i16::from(ErrorCode::None), topics[0].error_code);
    assert_eq!(
        Some(vec![
            MetadataResponsePartition::default()
                .error_code(ErrorCode::LeaderNotAvailable.into())
                .partition_index(0)
                .leader_id(-1)
                .leader_epoch(Some(0))
                .replica_nodes(Some(vec![NODE_ID]))
                .isr_nodes(Some(vec![NODE_ID]))
                .offline_replicas(Some(vec![]))
        ]),
        topics[0].partitions
    );

    Ok(())
}

/// A pool that has no free connection in time fails with `REQUEST_TIMED_OUT`, which a retry
/// may clear too.
#[tokio::test]
async fn a_timed_out_read_answers_the_partitions_last_read_without_a_leader() -> Result<()> {
    let storage = CountingStorage::new(Listing::Topics, &[TOPIC]);
    let service = service(&storage, &KnownTopics::default());

    _ = metadata(&service, &[TOPIC]).await?;
    storage.fail_named_lookups(Some(Error::Api(ErrorCode::RequestTimedOut)));

    let topics = topics(metadata(&service, &[TOPIC]).await?);

    assert_eq!(1, topics.len());
    assert_eq!(i16::from(ErrorCode::None), topics[0].error_code);
    Ok(())
}

/// A SQL engine reads the brokers through the connection that failed, so the read of no topics
/// fails too, and the service answers the brokers of its last read.
#[tokio::test]
async fn a_failed_read_of_the_brokers_answers_the_brokers_last_read() -> Result<()> {
    let storage = CountingStorage::new(Listing::Topics, &[TOPIC]);
    let service = service(&storage, &KnownTopics::default());

    let read = metadata(&service, &[TOPIC]).await?;
    storage.fail_lookups(Some(transient()));

    let response = metadata(&service, &[TOPIC]).await?;

    assert_eq!(read.brokers, response.brokers);
    assert_eq!(
        vec![i16::from(ErrorCode::None)],
        topics(response)
            .iter()
            .map(|topic| topic.error_code)
            .collect::<Vec<_>>()
    );
    Ok(())
}

/// The broker gives the service of each connection a clone of one store, so a topic read on one
/// connection is answered without a leader on another.
#[tokio::test]
async fn a_topic_read_on_one_connection_is_known_on_another() -> Result<()> {
    let storage = CountingStorage::new(Listing::Topics, &[TOPIC]);
    let known_topics = KnownTopics::default();

    _ = metadata(&service(&storage, &known_topics), &[TOPIC]).await?;
    storage.fail_named_lookups(Some(transient()));

    let topics = topics(metadata(&service(&storage, &known_topics), &[TOPIC]).await?);

    assert_eq!(
        vec![(Some(TOPIC.to_owned()), i16::from(ErrorCode::None))],
        topics
            .iter()
            .map(|topic| (topic.name.clone(), topic.error_code))
            .collect::<Vec<_>>()
    );

    Ok(())
}

/// The service has no partitions to answer for a topic that it never read, so it returns the
/// error, and the broker closes the connection.
#[tokio::test]
async fn a_failed_read_of_a_topic_never_answered_returns_the_error() -> Result<()> {
    let storage = CountingStorage::new(Listing::Topics, &[TOPIC]);
    let service = service(&storage, &KnownTopics::default());

    storage.fail_named_lookups(Some(transient()));

    assert_matches!(metadata(&service, &[TOPIC]).await, Err(Error::Io(_)));
    Ok(())
}

/// One topic that the service never read makes it return the error for the whole request.
#[tokio::test]
async fn a_failed_read_naming_one_topic_never_answered_returns_the_error() -> Result<()> {
    let storage = CountingStorage::new(Listing::Topics, &[TOPIC, "payments"]);
    let service = service(&storage, &KnownTopics::default());

    _ = metadata(&service, &[TOPIC]).await?;
    storage.fail_named_lookups(Some(transient()));

    assert_matches!(
        metadata(&service, &[TOPIC, "payments"]).await,
        Err(Error::Io(_))
    );
    Ok(())
}

#[tokio::test]
async fn a_topic_answered_as_unknown_is_forgotten() -> Result<()> {
    let storage = CountingStorage::new(Listing::Topics, &[TOPIC]);
    let service = service(&storage, &KnownTopics::default());

    _ = metadata(&service, &[TOPIC]).await?;
    storage.delete(TOPIC);

    assert_eq!(
        vec![i16::from(ErrorCode::UnknownTopicOrPartition)],
        topics(metadata(&service, &[TOPIC]).await?)
            .iter()
            .map(|topic| topic.error_code)
            .collect::<Vec<_>>()
    );

    storage.fail_named_lookups(Some(transient()));

    assert_matches!(metadata(&service, &[TOPIC]).await, Err(Error::Io(_)));
    Ok(())
}

/// An error that a retry does not clear would leave the partitions without a leader for good, so
/// the service answers the topic `UNKNOWN_SERVER_ERROR`, which a client reports at once.
#[tokio::test]
async fn a_permanent_failure_answers_unknown_server_error() -> Result<()> {
    let storage = CountingStorage::new(Listing::Topics, &[TOPIC]);
    let service = service(&storage, &KnownTopics::default());

    _ = metadata(&service, &[TOPIC]).await?;
    storage.fail_named_lookups(Some(Error::Message("permanent".into())));

    let topics = topics(metadata(&service, &[TOPIC]).await?);

    assert_eq!(1, topics.len());
    assert_eq!(
        i16::from(ErrorCode::UnknownServerError),
        topics[0].error_code
    );
    assert_eq!(Some(vec![]), topics[0].partitions);
    Ok(())
}

#[tokio::test]
async fn a_read_that_works_again_answers_the_leader() -> Result<()> {
    let storage = CountingStorage::new(Listing::Topics, &[TOPIC]);
    let service = service(&storage, &KnownTopics::default());

    _ = metadata(&service, &[TOPIC]).await?;
    storage.fail_named_lookups(Some(transient()));
    _ = metadata(&service, &[TOPIC]).await?;
    storage.fail_named_lookups(None);

    let topics = topics(metadata(&service, &[TOPIC]).await?);
    let partitions = topics[0].partitions.as_deref().unwrap_or_default();

    assert_eq!(
        vec![(i16::from(ErrorCode::None), NODE_ID)],
        partitions
            .iter()
            .map(|partition| (partition.error_code, partition.leader_id))
            .collect::<Vec<_>>()
    );
    Ok(())
}
