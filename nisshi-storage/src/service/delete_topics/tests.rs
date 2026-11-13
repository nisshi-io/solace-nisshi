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

use std::{assert_matches, time::Duration};

use nisshi_sans_io::{
    ApiKey as _, DeleteTopicsRequest, DeleteTopicsResponse, ErrorCode, Frame, Header,
    NULL_TOPIC_ID, RequestInput, delete_topics_request::DeleteTopicState,
    delete_topics_response::DeletableTopicResult,
};
use rama::{Service as _, extensions::Extensions};
use uuid::Uuid;

use super::DeleteTopicsService;
use crate::{Error, Result, TopicId, service::counting_storage::CountingStorage};

/// A timeout that no test reaches.
const LONG: i32 = 600_000;

/// An error that a retry may clear.
fn transient() -> Error {
    Error::from(std::io::Error::other("connection reset"))
}

/// An error that a retry can't clear, with text that must not reach a client.
fn permanent() -> Error {
    Error::Message("password=hunter2".into())
}

async fn serve(
    storage: &CountingStorage,
    request: DeleteTopicsRequest,
) -> Result<DeleteTopicsResponse> {
    DeleteTopicsService {
        storage: storage.clone(),
    }
    .serve(RequestInput {
        request,
        extensions: Extensions::default(),
    })
    .await
}

fn by_name(names: &[&str], timeout_ms: i32) -> DeleteTopicsRequest {
    DeleteTopicsRequest::default()
        .topic_names(Some(names.iter().map(|name| (*name).to_owned()).collect()))
        .timeout_ms(timeout_ms)
}

fn answers(response: DeleteTopicsResponse) -> Vec<DeletableTopicResult> {
    response.responses.unwrap_or_default()
}

fn codes(results: &[DeletableTopicResult]) -> Result<Vec<ErrorCode>> {
    results
        .iter()
        .map(|result| ErrorCode::try_from(result.error_code).map_err(Into::into))
        .collect()
}

fn names(results: &[DeletableTopicResult]) -> Vec<Option<&str>> {
    results
        .iter()
        .map(|result| result.name.as_deref())
        .collect()
}

/// A storage error that leaves `serve` closes the connection, because the stream returns it
/// before it writes a response. The service answers the topic instead.
#[tokio::test]
async fn storage_error_answers_a_response() -> Result<()> {
    let storage = CountingStorage::failing_delete_topic("a".into(), transient());

    let response = serve(&storage, by_name(&["a"], LONG)).await?;

    assert_eq!(vec![Some("a")], names(&answers(response)));

    Ok(())
}

/// An error that a retry may clear answers `REQUEST_TIMED_OUT`, as Kafka answers a deletion
/// that timed out. Any other error answers `UNKNOWN_SERVER_ERROR`, and an error that is a
/// Kafka code answers that code.
#[tokio::test]
async fn the_error_chooses_the_code() -> Result<()> {
    for (error, expected) in [
        (transient(), ErrorCode::RequestTimedOut),
        (permanent(), ErrorCode::UnknownServerError),
        (
            Error::Api(ErrorCode::TopicAuthorizationFailed),
            ErrorCode::TopicAuthorizationFailed,
        ),
    ] {
        let storage = CountingStorage::failing_delete_topic("a".into(), error.clone());

        let response = serve(&storage, by_name(&["a"], LONG)).await?;

        assert_eq!(vec![expected], codes(&answers(response))?, "{error:?}");
    }

    Ok(())
}

/// A failure on one topic does not stop the others. Each topic gets its own result, in the
/// order of the request.
#[tokio::test]
async fn a_failed_topic_does_not_stop_the_others() -> Result<()> {
    let storage = CountingStorage::failing_delete_topic("failed".into(), transient());

    let response = serve(&storage, by_name(&["first", "failed", "last"], LONG)).await?;

    let results = answers(response);
    assert_eq!(
        vec![ErrorCode::None, ErrorCode::RequestTimedOut, ErrorCode::None],
        codes(&results)?
    );
    assert_eq!(
        vec![Some("first"), Some("failed"), Some("last")],
        names(&results)
    );
    assert_eq!(
        vec![TopicId::from("first"), TopicId::from("last")],
        storage.deleted()?
    );

    Ok(())
}

/// The v6 form names a topic by name or by ID. A failed topic keeps the name and the ID that
/// the request gave it, and the topics after it are still deleted.
#[tokio::test]
async fn a_failed_topic_keeps_its_name_and_id_in_a_v6_request() -> Result<()> {
    let failed_id = Uuid::new_v4();
    let last_id = Uuid::new_v4();
    let storage = CountingStorage::failing_delete_topic(failed_id.into(), transient());

    let response = serve(
        &storage,
        DeleteTopicsRequest::default()
            .timeout_ms(LONG)
            .topics(Some(vec![
                DeleteTopicState::default().name(Some("named".into())),
                DeleteTopicState::default().topic_id(failed_id.into_bytes()),
                DeleteTopicState::default().topic_id(last_id.into_bytes()),
            ])),
    )
    .await?;

    let results = answers(response);
    assert_eq!(
        vec![ErrorCode::None, ErrorCode::RequestTimedOut, ErrorCode::None],
        codes(&results)?
    );
    assert_eq!(vec![Some("named"), None, None], names(&results));
    assert_eq!(Some(NULL_TOPIC_ID), results[0].topic_id);
    assert_eq!(Some(failed_id.into_bytes()), results[1].topic_id);
    assert_eq!(Some(last_id.into_bytes()), results[2].topic_id);
    assert_eq!(
        vec![TopicId::from("named"), TopicId::from(last_id)],
        storage.deleted()?
    );

    Ok(())
}

/// Once a topic has failed and the request's `timeout_ms` has passed, the topics after it
/// answer `REQUEST_TIMED_OUT` without a call to storage. A request with no failure still
/// deletes every topic, whatever its timeout.
#[tokio::test]
async fn an_expired_timeout_answers_the_rest_without_a_try() -> Result<()> {
    let storage = CountingStorage::failing_delete_topic("failed".into(), permanent());

    let response = serve(&storage, by_name(&["failed", "a", "b"], 0)).await?;

    let results = answers(response);
    assert_eq!(
        vec![
            ErrorCode::UnknownServerError,
            ErrorCode::RequestTimedOut,
            ErrorCode::RequestTimedOut
        ],
        codes(&results)?
    );
    assert_eq!(vec![Some("failed"), Some("a"), Some("b")], names(&results));
    assert_eq!(Vec::<TopicId>::new(), storage.deleted()?);

    let storage = CountingStorage::default();

    let response = serve(&storage, by_name(&["a", "b"], 0)).await?;

    assert_eq!(
        vec![ErrorCode::None, ErrorCode::None],
        codes(&answers(response))?
    );
    assert_eq!(
        vec![TopicId::from("a"), TopicId::from("b")],
        storage.deleted()?
    );

    Ok(())
}

/// A failing delete that takes longer than the request's `timeout_ms` spends it, and the topic
/// after it answers `REQUEST_TIMED_OUT` without a try. A failing delete that ends within the
/// time leaves it, and the topic after it is still deleted.
#[tokio::test(start_paused = true)]
async fn a_failure_spends_the_timeout_only_when_it_outlasts_it() -> Result<()> {
    for (timeout_ms, expected, deleted) in [
        (
            3_000,
            [ErrorCode::RequestTimedOut, ErrorCode::RequestTimedOut],
            vec![],
        ),
        (
            10_000,
            [ErrorCode::RequestTimedOut, ErrorCode::None],
            vec![TopicId::from("a")],
        ),
    ] {
        let storage = CountingStorage::failing_delete_topic("failed".into(), transient())
            .with_delete_failure_delay(Duration::from_secs(5));

        let response = serve(&storage, by_name(&["failed", "a"], timeout_ms)).await?;

        assert_eq!(
            expected.to_vec(),
            codes(&answers(response))?,
            "{timeout_ms}"
        );
        assert_eq!(deleted, storage.deleted()?, "{timeout_ms}");
    }

    Ok(())
}

/// An error that is a Kafka code is that topic's answer and not an outage, so it does not spend
/// the request's time: the topics after it are still deleted, even with a `timeout_ms` of 0.
#[tokio::test]
async fn an_api_error_does_not_spend_the_timeout() -> Result<()> {
    let storage = CountingStorage::failing_delete_topic(
        "failed".into(),
        Error::Api(ErrorCode::TopicAuthorizationFailed),
    );

    let response = serve(&storage, by_name(&["failed", "a", "b"], 0)).await?;

    assert_eq!(
        vec![
            ErrorCode::TopicAuthorizationFailed,
            ErrorCode::None,
            ErrorCode::None
        ],
        codes(&answers(response))?
    );
    assert_eq!(
        vec![TopicId::from("a"), TopicId::from("b")],
        storage.deleted()?
    );

    Ok(())
}

/// A transient failure carries a fixed message. A permanent failure carries none, as Kafka's
/// `UNKNOWN_SERVER_ERROR` does, so the text of the storage error does not reach the client.
/// The response encodes at versions 0 and 5, where the request names topics, and at version 6,
/// where it can name them by ID.
#[tokio::test]
async fn the_message_is_fixed_and_the_response_encodes() -> Result<()> {
    for (error, message) in [
        (
            transient(),
            Some("The storage is temporarily unavailable.".to_owned()),
        ),
        (permanent(), None),
    ] {
        let storage = CountingStorage::failing_delete_topic("a".into(), error);

        let response = serve(&storage, by_name(&["a"], LONG)).await?;

        assert_eq!(message, answers(response.clone())[0].error_message);

        let v6 = serve(
            &storage,
            DeleteTopicsRequest::default()
                .timeout_ms(LONG)
                .topics(Some(vec![
                    DeleteTopicState::default().name(Some("a".into())),
                ])),
        )
        .await?;

        assert_eq!(message, answers(v6.clone())[0].error_message);

        for (response, version) in [(response.clone(), 0), (response, 5), (v6, 6)] {
            _ = Frame::response(
                Header::Response { correlation_id: 0 },
                response.into(),
                DeleteTopicsRequest::KEY,
                version,
            )?;
        }
    }

    Ok(())
}

/// A storage error that is `UNKNOWN_TOPIC_OR_PARTITION` answers `UNKNOWN_TOPIC_ID` for a topic
/// that the request names by ID, and keeps its code for a name, as an `Ok` answer does.
#[tokio::test]
async fn an_unknown_topic_error_is_remapped_for_an_id() -> Result<()> {
    let id = Uuid::new_v4();
    let error = Error::Api(ErrorCode::UnknownTopicOrPartition);

    for (storage, request, expected) in [
        (
            CountingStorage::failing_delete_topic(id.into(), error.clone()),
            DeleteTopicsRequest::default()
                .timeout_ms(LONG)
                .topics(Some(vec![
                    DeleteTopicState::default().topic_id(id.into_bytes()),
                ])),
            ErrorCode::UnknownTopicId,
        ),
        (
            CountingStorage::failing_delete_topic("a".into(), error),
            by_name(&["a"], LONG),
            ErrorCode::UnknownTopicOrPartition,
        ),
    ] {
        let response = serve(&storage, request).await?;

        let results = answers(response);
        assert_eq!(vec![expected], codes(&results)?);
        assert_matches!(
            results[0].error_message,
            Some(ref message) if *message == expected.to_string()
        );
    }

    Ok(())
}
