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

use std::{
    io,
    sync::{Arc, Mutex},
};

use nisshi_sans_io::{
    ErrorCode, MetadataRequest, create_topics_response::CreatableTopicResult,
    metadata_request::MetadataRequestTopic,
};
use rama::Service as _;
use tokio::time::advance;
use tracing::Level;

use super::{
    AutoCreateRefusals, MetadataService, auto_create_refusals::REFUSAL_TTL, topic_exists_after,
};
use crate::{
    Result, TopicCreationConfig,
    service::counting_storage::{CountingStorage, Listing, Lookup},
};

fn service(
    storage: &CountingStorage,
    refusals: &AutoCreateRefusals,
) -> MetadataService<CountingStorage> {
    MetadataService {
        storage: storage.clone(),
        topic_creation: TopicCreationConfig::default(),
        auto_create_refusals: refusals.clone(),
    }
}

/// Sends a Metadata request that allows auto-creation for `names`, and returns the error
/// code of each topic in the response.
async fn metadata(
    service: &MetadataService<CountingStorage>,
    names: &[&str],
) -> Result<Vec<(String, ErrorCode)>> {
    let response = service
        .serve(
            MetadataRequest::default()
                .allow_auto_topic_creation(Some(true))
                .topics(Some(
                    names
                        .iter()
                        .map(|name| MetadataRequestTopic::default().name(Some((*name).into())))
                        .collect(),
                )),
        )
        .await?;

    response
        .topics
        .unwrap_or_default()
        .into_iter()
        .map(|topic| {
            Ok((
                topic.name.unwrap_or_default(),
                ErrorCode::try_from(topic.error_code)?,
            ))
        })
        .collect()
}

fn unknown(name: &str) -> (String, ErrorCode) {
    (name.to_owned(), ErrorCode::UnknownTopicOrPartition)
}

/// An empty list asks for no topics, so the response has none and the storage lists none,
/// where a null list asks for all of them.
#[tokio::test(start_paused = true)]
async fn an_empty_list_answers_no_topics() -> Result<()> {
    let storage = CountingStorage::new(Listing::Topics, &["a.b", "c.d"]);
    let service = service(&storage, &AutoCreateRefusals::default());

    assert_eq!(
        Vec::<(String, ErrorCode)>::new(),
        metadata(&service, &[]).await?
    );
    assert_eq!(0, storage.listings());
    assert!(storage.created().is_empty());

    let all = service
        .serve(MetadataRequest::default().topics(None))
        .await?;

    assert_eq!(2, all.topics.unwrap_or_default().len());
    assert_eq!(1, storage.listings());
    Ok(())
}

/// The deprecated slatedb engine lists every topic for an empty slice, so the service itself
/// keeps an empty list from answering topics. [`Lookup::Listed`] stands in for it, since it
/// lists the topics whatever the lookup names.
#[tokio::test(start_paused = true)]
async fn an_empty_list_answers_no_topics_from_a_storage_that_lists_every_topic() -> Result<()> {
    let storage =
        CountingStorage::new(Listing::Topics, &["a.b", "c.d"]).with_lookup(Lookup::Listed);
    let service = service(&storage, &AutoCreateRefusals::default());

    assert_eq!(
        Vec::<(String, ErrorCode)>::new(),
        metadata(&service, &[]).await?
    );
    assert_eq!(1, storage.lookups());
    assert!(storage.created().is_empty());
    Ok(())
}

/// A producer retries Metadata for a name that collides with an existing topic. Only the
/// first request lists every topic for the collision check. Each request still looks up its
/// names once.
#[tokio::test(start_paused = true)]
async fn a_retried_colliding_name_lists_topics_once() -> Result<()> {
    let storage = CountingStorage::new(Listing::Topics, &["a.b"]);
    let service = service(&storage, &AutoCreateRefusals::default());

    for _ in 0..5 {
        assert_eq!(vec![unknown("a_b")], metadata(&service, &["a_b"]).await?);
    }

    assert_eq!(1, storage.listings());
    assert_eq!(5, storage.lookups());
    assert!(storage.created().is_empty());
    Ok(())
}

/// The broker gives the service of each connection a clone of one store, so a refusal on
/// one connection saves the listing on another.
#[tokio::test(start_paused = true)]
async fn a_refusal_is_shared_by_clones() -> Result<()> {
    let storage = CountingStorage::new(Listing::Topics, &["a.b"]);
    let refusals = AutoCreateRefusals::default();

    for service in [service(&storage, &refusals), service(&storage, &refusals)] {
        assert_eq!(vec![unknown("a_b")], metadata(&service, &["a_b"]).await?);
    }

    assert_eq!(1, storage.listings());
    Ok(())
}

/// Once the colliding topic is deleted, the name is created when its refusal expires, and
/// not before.
#[tokio::test(start_paused = true)]
async fn a_refused_name_is_created_once_its_refusal_expires() -> Result<()> {
    let storage = CountingStorage::new(Listing::Topics, &["a.b"]);
    let service = service(&storage, &AutoCreateRefusals::default());

    assert_eq!(vec![unknown("a_b")], metadata(&service, &["a_b"]).await?);
    storage.delete("a.b");

    assert_eq!(vec![unknown("a_b")], metadata(&service, &["a_b"]).await?);
    assert!(storage.created().is_empty());
    assert_eq!(1, storage.listings());

    advance(REFUSAL_TTL).await;

    assert_eq!(
        vec![(String::from("a_b"), ErrorCode::None)],
        metadata(&service, &["a_b"]).await?
    );
    assert_eq!(vec![String::from("a_b")], storage.created());
    assert_eq!(2, storage.listings());
    Ok(())
}

/// A failed listing is the broker's failure, not a refusal, so the next request tries again.
#[tokio::test(start_paused = true)]
async fn a_failed_listing_is_not_remembered() -> Result<()> {
    for listing in [Listing::Error, Listing::NamelessTopic] {
        let storage = CountingStorage::new(listing, &[]);
        let service = service(&storage, &AutoCreateRefusals::default());

        for _ in 0..2 {
            assert_eq!(vec![unknown("a_b")], metadata(&service, &["a_b"]).await?);
        }

        assert_eq!(2, storage.listings(), "{listing:?}");
    }

    Ok(())
}

/// `__cluster_metadata` and the internal names get no `CreateTopics`, so the storage lists
/// nothing for a collision check, and the other name of the request is still created.
#[tokio::test(start_paused = true)]
async fn cluster_metadata_and_the_internal_names_are_not_auto_created() -> Result<()> {
    let storage = CountingStorage::new(Listing::Topics, &[]);
    let service = service(&storage, &AutoCreateRefusals::default());

    let names = [
        "__cluster_metadata",
        "__consumer_offsets",
        "__transaction_state",
        "__share_group_state",
        "ab",
    ];

    let expected = names
        .iter()
        .map(|name| match *name {
            "ab" => (String::from("ab"), ErrorCode::None),
            other => unknown(other),
        })
        .collect::<Vec<_>>();

    assert_eq!(expected, metadata(&service, &names).await?);
    assert_eq!(vec![String::from("ab")], storage.created());
    assert_eq!(0, storage.listings());
    Ok(())
}

/// A refused name is left out of auto-creation, and the other names of the request are
/// still created.
#[tokio::test(start_paused = true)]
async fn a_refused_name_does_not_block_others() -> Result<()> {
    let storage = CountingStorage::new(Listing::Topics, &["a.b"]);
    let service = service(&storage, &AutoCreateRefusals::default());

    assert_eq!(vec![unknown("a_b")], metadata(&service, &["a_b"]).await?);

    assert_eq!(
        vec![unknown("a_b"), (String::from("c_d"), ErrorCode::None)],
        metadata(&service, &["a_b", "c_d"]).await?
    );
    assert_eq!(vec![String::from("c_d")], storage.created());
    Ok(())
}

/// The log lines written at `warn` or above.
#[derive(Clone, Default)]
struct WarnLog(Arc<Mutex<Vec<u8>>>);

impl io::Write for WarnLog {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.lock().expect("log").extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Returns whether the topic exists after `error_code`, and what was logged at `warn` or
/// above.
fn outcome(error_code: ErrorCode) -> (bool, String) {
    let log = WarnLog::default();
    let writer = log.clone();

    let exists = tracing::subscriber::with_default(
        tracing_subscriber::fmt()
            .with_max_level(Level::WARN)
            .with_ansi(false)
            .with_writer(move || writer.clone())
            .finish(),
        || {
            topic_exists_after(
                &CreatableTopicResult::default()
                    .name("orders".into())
                    .error_code(error_code.into()),
            )
        },
    );

    let text = String::from_utf8(log.0.lock().expect("log").clone()).expect("utf-8");
    (exists, text)
}

/// A storage failure is the broker's problem, not the client's, so the operator sees it at
/// the default log level. The producer only sees `UNKNOWN_TOPIC_OR_PARTITION`.
#[test]
fn a_storage_failure_is_logged_at_warn() {
    for error_code in [ErrorCode::UnknownServerError, ErrorCode::KafkaStorageError] {
        let (exists, log) = outcome(error_code);

        assert!(!exists, "{error_code:?}");
        assert!(log.contains("orders"), "{error_code:?}: {log:?}");
    }
}

/// A client sends a colliding name again on each retry, so the refusal stays below `warn`.
#[test]
fn a_refused_name_is_not_logged_at_warn() {
    let (exists, log) = outcome(ErrorCode::InvalidTopicException);

    assert!(!exists);
    assert_eq!("", log);
}

#[test]
fn a_created_or_existing_topic_exists() {
    for error_code in [ErrorCode::None, ErrorCode::TopicAlreadyExists] {
        assert!(outcome(error_code).0, "{error_code:?}");
    }
}
