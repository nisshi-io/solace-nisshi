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

//! The error that `DynoStore` returns when it can't read the cluster's metadata. A producer
//! retries `KAFKA_STORAGE_ERROR`, so an object store error becomes that code.

use std::assert_matches;

use bytes::Bytes;
use nisshi_sans_io::{
    ConfigResource, ErrorCode,
    record::{Record, deflated, inflated},
};
use nisshi_storage::{Error, Storage as _, TopicId, Topition};
use object_store::{ObjectStoreExt as _, PutPayload, memory::InMemory, path::Path};
use uuid::Uuid;

use crate::dynostore::{
    DynoStore,
    tests::{failing_object_store::FailingObjectStore, init_tracing},
};

#[tokio::test]
async fn failed_read_answers_kafka_storage_error_to_describe_config() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = DynoStore::new(
        "nisshi",
        111,
        FailingObjectStore::failing_get(InMemory::new()),
    )?;

    assert_matches!(
        storage
            .describe_config("abc", ConfigResource::Topic, None)
            .await,
        Err(Error::Api(ErrorCode::KafkaStorageError))
    );

    Ok(())
}

/// A produce checks that its partition exists before it writes, which reads the metadata.
#[tokio::test]
async fn failed_read_answers_kafka_storage_error_to_produce() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = DynoStore::new(
        "nisshi",
        111,
        FailingObjectStore::failing_get(InMemory::new()),
    )?;

    let batch = inflated::Batch::builder()
        .record(Record::builder().value(Some(Bytes::from_static(b"abc"))))
        .build()
        .and_then(deflated::Batch::try_from)?;

    assert_matches!(
        storage.produce(None, &Topition::new("abc", 0), batch).await,
        Err(Error::Api(ErrorCode::KafkaStorageError))
    );

    Ok(())
}

/// A metadata read that fails transiently returns the error to the caller, which answers
/// `KAFKA_STORAGE_ERROR`, because an unknown topic ID tells a client that the topic is gone.
#[tokio::test]
async fn failed_read_returns_a_transient_error_from_metadata() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = DynoStore::new(
        "nisshi",
        111,
        FailingObjectStore::failing_get(InMemory::new()),
    )?;

    let error = storage
        .metadata(Some(&[TopicId::from(Uuid::now_v7()), TopicId::from("abc")]))
        .await
        .expect_err("a failed read is an error of the whole call");

    assert_eq!(
        ErrorCode::KafkaStorageError,
        nisshi_storage::topic_id_query_error_code(&error)
    );

    Ok(())
}

/// A retry does not fix metadata that can't be decoded, so its error keeps its type.
#[tokio::test]
async fn undecodable_metadata_is_not_a_kafka_storage_error() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let object_store = InMemory::new();
    let storage = DynoStore::new("nisshi", 111, object_store.clone())?;

    _ = object_store
        .put(
            &Path::from("clusters/nisshi/meta.json"),
            PutPayload::from_static(b"not json"),
        )
        .await?;

    let described = storage
        .describe_config("abc", ConfigResource::Topic, None)
        .await;

    assert!(described.is_err());
    assert!(!matches!(
        described,
        Err(Error::Api(ErrorCode::KafkaStorageError))
    ));

    Ok(())
}

/// A retry does not fix metadata that can't be decoded, so the error that a metadata read returns
/// for it answers `UNKNOWN_SERVER_ERROR` and not the retriable `KAFKA_STORAGE_ERROR`.
#[tokio::test]
async fn undecodable_metadata_returns_a_permanent_error_from_metadata() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let object_store = InMemory::new();
    let storage = DynoStore::new("nisshi", 111, object_store.clone())?;

    _ = object_store
        .put(
            &Path::from("clusters/nisshi/meta.json"),
            PutPayload::from_static(b"not json"),
        )
        .await?;

    let error = storage
        .metadata(Some(&[TopicId::from(Uuid::now_v7()), TopicId::from("abc")]))
        .await
        .expect_err("an undecodable read is an error of the whole call");

    assert_eq!(
        ErrorCode::UnknownServerError,
        nisshi_storage::topic_id_query_error_code(&error)
    );

    Ok(())
}
