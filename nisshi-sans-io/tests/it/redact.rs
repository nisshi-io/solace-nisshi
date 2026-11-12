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

use bytes::Bytes;
use nisshi_sans_io::{
    AlterConfigsRequest, AlterUserScramCredentialsRequest, ApiKey, BatchAttribute, Body,
    Compression, CreateDelegationTokenResponse, DescribeDelegationTokenResponse,
    ExpireDelegationTokenRequest, Frame, Header, IncrementalAlterConfigsRequest, ProduceRequest,
    RenewDelegationTokenRequest, Result, SaslAuthenticateRequest, SaslAuthenticateResponse,
    alter_configs_request,
    alter_user_scram_credentials_request::ScramCredentialUpsertion,
    describe_delegation_token_response::DescribedDelegationToken,
    incremental_alter_configs_request,
    produce_request::{PartitionProduceData, TopicProduceData},
    record::{self, Record, deflated, inflated},
};
use std::{
    fmt::Debug,
    io,
    sync::{Arc, Mutex},
};
use tracing::Level;
use tracing_subscriber::fmt::format::FmtSpan;

/// A SASL/PLAIN message with a marker password.
const MARKER: &[u8] = b"\0alice\0hunter2-marker";

fn marker() -> Bytes {
    Bytes::from_static(MARKER)
}

/// Asserts that `text` doesn't hold the marker password as text (also inside a
/// `b"..."` literal), as a decimal byte list, as hex, or as the decoder's
/// one-line-per-byte form of its `h`.
fn assert_hidden(text: &str) {
    for form in [
        "hunter2",
        "104, 117, 110, 116, 101, 114, 50",
        "68756e74657232",
        "value: 104:u8",
    ] {
        assert!(!text.contains(form), "found {form:?} in: {text}");
    }
}

/// Asserts that the `Debug` output of `message`, of its `Body` and of a `Frame`
/// holding it hides the marker and still shows each of `shown`.
fn assert_debug_hidden<T>(message: T, shown: &[&str])
where
    T: Clone + Debug + Into<Body>,
{
    let body: Body = message.clone().into();
    let frame = Frame {
        size: 0,
        header: Header::Response { correlation_id: 7 },
        body: body.clone(),
    };

    for text in [
        format!("{message:?}"),
        format!("{body:?}"),
        format!("{frame:?}"),
    ] {
        assert_hidden(&text);
        assert!(text.contains("[hidden]"), "{text}");

        for expected in shown {
            assert!(text.contains(expected), "missing {expected:?} in: {text}");
        }
    }
}

#[test]
fn sasl_authenticate_request() {
    assert_debug_hidden(
        SaslAuthenticateRequest::default().auth_bytes(marker()),
        &["auth_bytes: [hidden]"],
    );
}

#[test]
fn sasl_authenticate_response() {
    assert_debug_hidden(
        SaslAuthenticateResponse::default()
            .error_code(58)
            .auth_bytes(marker())
            .session_lifetime_ms(Some(3_600_000)),
        &[
            "error_code: 58",
            "auth_bytes: [hidden]",
            "session_lifetime_ms: Some(3600000)",
        ],
    );
}

#[test]
fn alter_user_scram_credentials_request() {
    assert_debug_hidden(
        AlterUserScramCredentialsRequest::default().upsertions(Some(vec![
            ScramCredentialUpsertion::default()
                .name("alice".into())
                .mechanism(1)
                .iterations(4096)
                .salt(Bytes::from_static(b"salt-marker"))
                .salted_password(marker()),
        ])),
        &[
            "name: \"alice\"",
            "iterations: 4096",
            "salt-marker",
            "salted_password: [hidden]",
        ],
    );
}

#[test]
fn create_delegation_token_response() {
    assert_debug_hidden(
        CreateDelegationTokenResponse::default()
            .error_code(0)
            .token_id("token-marker".into())
            .hmac(marker()),
        &["error_code: 0", "token-marker", "hmac: [hidden]"],
    );
}

#[test]
fn describe_delegation_token_response() {
    assert_debug_hidden(
        DescribeDelegationTokenResponse::default()
            .error_code(0)
            .tokens(Some(vec![
                DescribedDelegationToken::default()
                    .token_id("token-marker".into())
                    .hmac(marker()),
            ])),
        &["error_code: 0", "token-marker", "hmac: [hidden]"],
    );
}

#[test]
fn expire_delegation_token_request() {
    assert_debug_hidden(
        ExpireDelegationTokenRequest::default()
            .hmac(marker())
            .expiry_time_period_ms(86_400_000),
        &["hmac: [hidden]", "expiry_time_period_ms: 86400000"],
    );
}

#[test]
fn renew_delegation_token_request() {
    assert_debug_hidden(
        RenewDelegationTokenRequest::default()
            .hmac(marker())
            .renew_period_ms(86_400_000),
        &["hmac: [hidden]", "renew_period_ms: 86400000"],
    );
}

#[test]
fn alter_configs_request() {
    assert_debug_hidden(
        AlterConfigsRequest::default().resources(Some(vec![
            alter_configs_request::AlterConfigsResource::default()
                .resource_type(4)
                .resource_name("111".into())
                .configs(Some(vec![
                    alter_configs_request::AlterableConfig::default()
                        .name("ssl.keystore.password".into())
                        .value(Some("hunter2-marker".into())),
                ])),
        ])),
        &["ssl.keystore.password", "value: [hidden]"],
    );
}

#[test]
fn incremental_alter_configs_request() {
    assert_debug_hidden(
        IncrementalAlterConfigsRequest::default().resources(Some(vec![
            incremental_alter_configs_request::AlterConfigsResource::default()
                .resource_type(4)
                .resource_name("111".into())
                .configs(Some(vec![
                    incremental_alter_configs_request::AlterableConfig::default()
                        .name("ssl.keystore.password".into())
                        .config_operation(0)
                        .value(Some("hunter2-marker".into())),
                ])),
        ])),
        &["ssl.keystore.password", "value: [hidden]"],
    );
}

#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Vec<u8>>>);

impl io::Write for Capture {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0
            .lock()
            .map_err(|err| io::Error::other(err.to_string()))?
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Runs `f` with every event and span at TRACE written to a buffer, and
/// returns what was written.
fn captured(f: impl FnOnce() -> Result<()>) -> Result<String> {
    let capture = Capture::default();
    let writer = capture.clone();

    let subscriber = tracing_subscriber::fmt()
        .with_max_level(Level::TRACE)
        .with_span_events(FmtSpan::FULL)
        .with_ansi(false)
        .with_writer(move || writer.clone())
        .finish();

    tracing::subscriber::with_default(subscriber, f)?;

    let written = capture
        .0
        .lock()
        .map(|buffer| buffer.clone())
        .unwrap_or_default();
    Ok(String::from_utf8_lossy(&written).into_owned())
}

fn request_round_trip(api_version: i16, body: Body) -> Result<String> {
    captured(|| {
        let encoded = Frame::request(
            Header::Request {
                api_key: body.api_key(),
                api_version,
                correlation_id: 7,
                client_id: Some("redact".into()),
            },
            body.clone(),
        )?;

        let decoded = Frame::request_from_bytes(encoded)?;
        assert_eq!(body, decoded.body);
        Ok(())
    })
}

fn response_round_trip(api_key: i16, api_version: i16, body: Body) -> Result<String> {
    captured(|| {
        let encoded = Frame::response(
            Header::Response { correlation_id: 7 },
            body.clone(),
            api_key,
            api_version,
        )?;

        let decoded = Frame::response_from_bytes(encoded, api_key, api_version)?;
        assert_eq!(body, decoded.body);
        Ok(())
    })
}

#[test]
fn sasl_authenticate_request_logs() -> Result<()> {
    for api_version in 0..=2 {
        let logs = request_round_trip(
            api_version,
            SaslAuthenticateRequest::default()
                .auth_bytes(marker())
                .into(),
        )?;

        // The span that used to record the auth bytes still runs, so the
        // capture covers the encode path that this test guards.
        assert!(logs.contains("serialize_bytes{len=21}"), "{logs}");
        assert_hidden(&logs);
    }

    Ok(())
}

#[test]
fn sasl_authenticate_response_logs() -> Result<()> {
    for api_version in 0..=2 {
        let logs = response_round_trip(
            SaslAuthenticateResponse::KEY,
            api_version,
            SaslAuthenticateResponse::default()
                .error_code(0)
                .error_message(Some("ok".into()))
                .auth_bytes(marker())
                .session_lifetime_ms((api_version >= 1).then_some(0))
                .into(),
        )?;

        assert!(!logs.is_empty());
        assert_hidden(&logs);
    }

    Ok(())
}

#[test]
fn alter_configs_request_logs() -> Result<()> {
    for api_version in 0..=2 {
        let logs = request_round_trip(
            api_version,
            AlterConfigsRequest::default()
                .resources(Some(vec![
                    alter_configs_request::AlterConfigsResource::default()
                        .resource_type(4)
                        .resource_name("111".into())
                        .configs(Some(vec![
                            alter_configs_request::AlterableConfig::default()
                                .name("ssl.keystore.password".into())
                                .value(Some("hunter2-marker".into())),
                        ])),
                ]))
                .validate_only(false)
                .into(),
        )?;

        assert!(logs.contains("value: [hidden]"), "{logs}");
        assert_hidden(&logs);
    }

    Ok(())
}

#[test]
fn incremental_alter_configs_request_logs() -> Result<()> {
    for api_version in 0..=1 {
        let logs = request_round_trip(
            api_version,
            IncrementalAlterConfigsRequest::default()
                .resources(Some(vec![
                    incremental_alter_configs_request::AlterConfigsResource::default()
                        .resource_type(4)
                        .resource_name("111".into())
                        .configs(Some(vec![
                            incremental_alter_configs_request::AlterableConfig::default()
                                .name("ssl.keystore.password".into())
                                .config_operation(0)
                                .value(Some("hunter2-marker".into())),
                        ])),
                ]))
                .validate_only(false)
                .into(),
        )?;

        assert!(logs.contains("value: [hidden]"), "{logs}");
        assert_hidden(&logs);
    }

    Ok(())
}

#[test]
fn alter_user_scram_credentials_request_logs() -> Result<()> {
    let logs = request_round_trip(
        0,
        AlterUserScramCredentialsRequest::default()
            .deletions(Some(vec![]))
            .upsertions(Some(vec![
                ScramCredentialUpsertion::default()
                    .name("alice".into())
                    .mechanism(1)
                    .iterations(4096)
                    .salt(Bytes::from_static(b"salt-marker"))
                    .salted_password(marker()),
            ]))
            .into(),
    )?;

    assert!(!logs.is_empty());
    assert_hidden(&logs);

    Ok(())
}

/// A batch with one record whose key, value and header value are the marker.
fn marker_batch() -> Result<inflated::Batch> {
    compressed_marker_batch(Compression::None)
}

fn compressed_marker_batch(compression: Compression) -> Result<inflated::Batch> {
    inflated::Batch::builder()
        .attributes(BatchAttribute::default().compression(compression).into())
        .record(
            Record::builder()
                .key(Some(marker()))
                .value(Some(marker()))
                .header(
                    record::Header::builder()
                        .key(Bytes::from_static(b"header-marker"))
                        .value(marker()),
                ),
        )
        .producer_id(-1)
        .producer_epoch(-1)
        .build()
}

fn marker_produce_request() -> Result<ProduceRequest> {
    Ok(ProduceRequest::default()
        .transactional_id(None)
        .acks(-1)
        .timeout_ms(1_500)
        .topic_data(Some(vec![
            TopicProduceData::default()
                .name("topic-marker".into())
                .partition_data(Some(vec![
                    PartitionProduceData::default().index(0).records(Some(
                        inflated::Frame {
                            batches: vec![marker_batch()?],
                        }
                        .try_into()?,
                    )),
                ])),
        ])))
}

#[test]
fn record_debug() -> Result<()> {
    let inflated = marker_batch()?;
    let deflated = deflated::Batch::try_from(inflated.clone())?;

    let record = format!("{:?}", inflated.records[0]);
    let inflated = format!("{inflated:?}");
    let deflated = format!("{deflated:?}");

    for text in [&record, &inflated, &deflated] {
        assert_hidden(text);
    }

    assert!(record.contains("key_len: Some(21)"), "{record}");
    assert!(record.contains("value_len: Some(21)"), "{record}");
    assert!(record.contains("header-marker"), "{record}");
    assert!(deflated.contains("record_data_len"), "{deflated}");

    Ok(())
}

#[test]
fn record_inflate_logs() -> Result<()> {
    for compression in [
        Compression::None,
        Compression::Gzip,
        Compression::Snappy,
        Compression::Lz4,
        Compression::Zstd,
    ] {
        let label = format!("{compression:?}");
        let batch = compressed_marker_batch(compression)?;
        let expected = batch.records.clone();

        let logs = captured(|| {
            let deflated = deflated::Batch::try_from(batch)?;
            let records = Vec::<Record>::try_from(deflated)?;
            assert_eq!(expected, records);
            Ok(())
        })?;

        assert!(!logs.is_empty(), "{label}");
        assert_hidden(&logs);
    }

    Ok(())
}

#[test]
fn produce_request_debug() -> Result<()> {
    let request = marker_produce_request()?;
    let body: Body = request.clone().into();

    for text in [format!("{request:?}"), format!("{body:?}")] {
        assert_hidden(&text);
        assert!(text.contains("topic-marker"), "{text}");
    }

    Ok(())
}

#[test]
fn produce_request_logs() -> Result<()> {
    let logs = request_round_trip(9, marker_produce_request()?.into())?;

    assert!(!logs.is_empty());
    assert_hidden(&logs);

    Ok(())
}
