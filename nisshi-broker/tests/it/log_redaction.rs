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

//! Checks that a SCRAM credential upsert, a SASL/SCRAM login, a produce and a
//! fetch through the broker and a storage backend don't write their secrets or
//! record data to the log, at any level.

use std::{
    io,
    sync::{Arc, Mutex, OnceLock},
};

use crate::common::{lite_storage, postgres_storage, slate_storage};
use bytes::{BufMut, Bytes, BytesMut};
use nisshi_broker::{
    Error, Result,
    otel::log_filter_from,
    service::{auth, storage},
};
use nisshi_sans_io::{
    AlterUserScramCredentialsRequest, AlterUserScramCredentialsResponse, ApiKey, BatchAttribute,
    Body, BytesInput, Compression, CreateTopicsRequest, CreateTopicsResponse, ErrorCode,
    FetchRequest, FetchResponse, Frame, Header, ProduceRequest, ProduceResponse,
    SaslAuthenticateRequest, SaslAuthenticateResponse, SaslHandshakeRequest, SaslHandshakeResponse,
    ScramMechanism,
    alter_user_scram_credentials_request::ScramCredentialUpsertion,
    create_topics_request::CreatableTopic,
    fetch_request::{FetchPartition, FetchTopic},
    produce_request::{PartitionProduceData, TopicProduceData},
    record::{self, Record, inflated},
};
use nisshi_service::{BytesFrameLayer, BytesFrameService, FrameRouteService};
use nisshi_storage::{ArcDynStorage, AutoCreateRefusals, Storage, TopicCreationConfig};
use pbkdf2::{hmac::Hmac, pbkdf2};
use rama::{Layer as _, Service as _, extensions::Extensions};
use rand::{RngExt as _, rng};
use rsasl::{
    config::SASLConfig,
    prelude::{Mechname, SASLClient, State},
};
use sha2::Sha256;
use tracing::Level;
use tracing_subscriber::{Layer, fmt::format::FmtSpan, layer::SubscriberExt};
use uuid::Uuid;

type Broker = BytesFrameService<FrameRouteService<Error>>;

const PRINCIPAL: &str = "alice";
const PASSWORD: &str = "e2e-hunter2-password";
const SALT: &[u8] = b"log-redaction-salt";
const ITERATIONS: u32 = 4096;
const TOPIC: &str = "log-redaction";
const RECORD_KEY: &[u8] = b"e2e-record-marker-key";
const RECORD_VALUE: &[u8] = b"e2e-record-marker-value";
const HEADER_VALUE: &[u8] = b"e2e-header-marker-value";
const CLIENT_ID: &str = "log-redaction";

fn broker<S>(storage: S, sasl_config: Option<Arc<SASLConfig>>) -> Result<Broker>
where
    S: Storage + Clone,
{
    storage::services(
        FrameRouteService::<Error>::builder(),
        storage,
        TopicCreationConfig::default(),
        AutoCreateRefusals::default(),
    )
    .and_then(auth::services)
    .and_then(|builder| builder.build().map_err(Into::into))
    .map(|frame_route| {
        (BytesFrameLayer::default().with_sasl_config(sasl_config),).into_layer(frame_route)
    })
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

impl Capture {
    fn text(&self) -> String {
        let written = self
            .0
            .lock()
            .map(|buffer| buffer.clone())
            .unwrap_or_default();
        String::from_utf8_lossy(&written).into_owned()
    }
}

/// Returns the buffer that the process-wide subscriber writes every event and
/// span to, at TRACE, with the caps that the broker's own env filter puts on
/// its dependencies.
///
/// The subscriber is global, not a thread-local default, because the broker
/// decodes frames on blocking threads, and a thread-local default would miss
/// their events.
fn capture() -> &'static Capture {
    static CAPTURE: OnceLock<Capture> = OnceLock::new();

    CAPTURE.get_or_init(|| {
        let capture = Capture::default();
        let writer = capture.clone();

        tracing::subscriber::set_global_default(
            tracing_subscriber::registry().with(
                tracing_subscriber::fmt::layer()
                    .with_span_events(FmtSpan::FULL)
                    .with_ansi(false)
                    .with_writer(move || writer.clone())
                    .with_filter(
                        log_filter_from(Some(Level::TRACE.as_str()))
                            .expect("the broker's log filter accepts a level"),
                    ),
            ),
        )
        .expect("no other global subscriber in this test process");

        capture
    })
}

/// The forms in which a log line could hold `marker`, each with a label: its
/// first bytes as a decimal list and as hex, its `Debug` text (which is the
/// text itself when it is ASCII), and the decoder's one-line-per-byte form of
/// its first byte.
fn forms(marker: &[u8]) -> Vec<(&'static str, String)> {
    let head = &marker[..marker.len().min(8)];
    let escaped = format!("{:?}", Bytes::copy_from_slice(marker));

    [
        (
            "a decimal list",
            format!("{head:?}").trim_matches(['[', ']']).to_owned(),
        ),
        (
            "hex",
            head.iter().map(|byte| format!("{byte:02x}")).collect(),
        ),
        (
            "Debug text",
            escaped
                .trim_start_matches("b\"")
                .trim_end_matches('"')
                .to_owned(),
        ),
    ]
    .into_iter()
    .chain(
        head.first()
            .map(|byte| ("one line per byte", format!("value: {byte}:u8"))),
    )
    .collect()
}

/// Asserts that `logs` doesn't hold `marker` in any of its [`forms`]. The
/// failure message names the form, not its text, so a failure doesn't print
/// the value it found.
fn assert_hidden(logs: &str, name: &str, marker: &[u8]) {
    for (label, form) in forms(marker) {
        assert!(!logs.contains(&form), "the log holds {name} as {label}");
    }
}

fn salted_password() -> Bytes {
    let mut salted = BytesMut::zeroed(32);
    pbkdf2::<Hmac<Sha256>>(PASSWORD.as_bytes(), SALT, ITERATIONS, &mut salted)
        .expect("a 32 byte output is valid for SHA-256");
    salted.freeze()
}

async fn request<Q, R>(
    broker: &Broker,
    extensions: &Extensions,
    correlation_id: &mut i32,
    api_version: i16,
    request: Q,
) -> Result<R>
where
    Q: ApiKey + Into<Body>,
    R: ApiKey + TryFrom<Body, Error = nisshi_sans_io::Error>,
{
    *correlation_id += 1;

    let response = broker
        .serve(BytesInput {
            bytes: Frame::request(
                Header::Request {
                    api_key: Q::KEY,
                    api_version,
                    correlation_id: *correlation_id,
                    client_id: Some(CLIENT_ID.into()),
                },
                request.into(),
            )?,
            extensions: extensions.clone(),
        })
        .await?;

    Frame::response_from_bytes(response, R::KEY, api_version)
        .and_then(|frame| R::try_from(frame.body))
        .map_err(Into::into)
}

/// Runs a SCRAM-SHA-256 login on `broker` and returns every SASL message that
/// the client and the broker exchanged.
async fn scram_login(
    broker: &Broker,
    extensions: &Extensions,
    correlation_id: &mut i32,
) -> Result<Vec<Bytes>> {
    const API_VERSION: i16 = 1;

    let handshake: SaslHandshakeResponse = request(
        broker,
        extensions,
        correlation_id,
        API_VERSION,
        SaslHandshakeRequest::default().mechanism("SCRAM-SHA-256".into()),
    )
    .await?;
    assert_eq!(ErrorCode::None, ErrorCode::try_from(handshake.error_code)?);

    // A Kafka client logs in with the mechanism that it requested, not one that
    // it picks from the list.
    let offered = [Mechname::parse(b"SCRAM-SHA-256").expect("mechanism name")];

    let sasl = SASLClient::new(
        SASLConfig::with_credentials(None, PRINCIPAL.into(), PASSWORD.into())
            .expect("client sasl config"),
    );
    let mut session = sasl
        .start_suggested(&offered)
        .expect("SCRAM-SHA-256 is offered");

    let mut exchanged = Vec::new();
    let mut input: Option<Bytes> = None;

    loop {
        let mut output = BytesMut::new().writer();

        match session
            .step(input.as_deref(), &mut output)
            .expect("the broker accepts the client's proof")
        {
            State::Running => {
                let auth_bytes = output.into_inner().freeze();
                exchanged.push(auth_bytes.clone());

                let response: SaslAuthenticateResponse = request(
                    broker,
                    extensions,
                    correlation_id,
                    API_VERSION,
                    SaslAuthenticateRequest::default().auth_bytes(auth_bytes),
                )
                .await?;
                assert_eq!(ErrorCode::None, ErrorCode::try_from(response.error_code)?);

                exchanged.push(response.auth_bytes.clone());
                input = Some(response.auth_bytes);
            }

            State::Finished(_) => break,
        }
    }

    Ok(exchanged)
}

fn produce_request(compression: Compression) -> Result<ProduceRequest> {
    let batch = inflated::Batch::builder()
        .attributes(BatchAttribute::default().compression(compression).into())
        .record(
            Record::builder()
                .key(Some(Bytes::from_static(RECORD_KEY)))
                .value(Some(Bytes::from_static(RECORD_VALUE)))
                .header(
                    record::Header::builder()
                        .key(Bytes::from_static(b"header-key"))
                        .value(Bytes::from_static(HEADER_VALUE)),
                ),
        )
        .producer_id(-1)
        .producer_epoch(-1)
        .build()?;

    Ok(ProduceRequest::default()
        .transactional_id(None)
        .acks(-1)
        .timeout_ms(5_000)
        .topic_data(Some(vec![
            TopicProduceData::default()
                .name(TOPIC.into())
                .partition_data(Some(vec![
                    PartitionProduceData::default().index(0).records(Some(
                        inflated::Frame {
                            batches: vec![batch],
                        }
                        .try_into()?,
                    )),
                ])),
        ])))
}

async fn secrets_stay_out_of_logs(storage: ArcDynStorage) -> Result<()> {
    let capture = capture();
    let salted_password = salted_password();
    let mut correlation_id = 0;

    // Without SASL the broker accepts AlterUserScramCredentials before a
    // login, so this connection creates the user that the next one logs in as.
    {
        let broker = broker(storage.clone(), None)?;
        let extensions = Extensions::default();

        let response: AlterUserScramCredentialsResponse = request(
            &broker,
            &extensions,
            &mut correlation_id,
            0,
            AlterUserScramCredentialsRequest::default()
                .deletions(Some(vec![]))
                .upsertions(Some(vec![
                    ScramCredentialUpsertion::default()
                        .name(PRINCIPAL.into())
                        .mechanism(ScramMechanism::Scram256.into())
                        .iterations(ITERATIONS as i32)
                        .salt(Bytes::from_static(SALT))
                        .salted_password(salted_password.clone()),
                ])),
        )
        .await?;

        for result in response.results.unwrap_or_default() {
            assert_eq!(ErrorCode::None, ErrorCode::try_from(result.error_code)?);
        }
    }

    let sasl_config = nisshi_auth::configuration(storage.clone())
        .map(Some)
        .map_err(Error::from)?;
    let broker = broker(storage, sasl_config)?;
    let extensions = Extensions::default();

    let exchanged = scram_login(&broker, &extensions, &mut correlation_id).await?;

    let created: CreateTopicsResponse = request(
        &broker,
        &extensions,
        &mut correlation_id,
        7,
        CreateTopicsRequest::default()
            .timeout_ms(30_000)
            .validate_only(Some(false))
            .topics(Some(vec![
                CreatableTopic::default()
                    .assignments(Some(vec![]))
                    .configs(Some(vec![]))
                    .name(TOPIC.into())
                    .num_partitions(1)
                    .replication_factor(1),
            ])),
    )
    .await?;

    for topic in created.topics.unwrap_or_default() {
        assert_eq!(ErrorCode::None, ErrorCode::try_from(topic.error_code)?);
    }

    for compression in [Compression::None, Compression::Gzip] {
        let produced: ProduceResponse = request(
            &broker,
            &extensions,
            &mut correlation_id,
            9,
            produce_request(compression)?,
        )
        .await?;

        for partition in produced
            .responses
            .unwrap_or_default()
            .into_iter()
            .flat_map(|topic| topic.partition_responses.unwrap_or_default())
        {
            assert_eq!(ErrorCode::None, ErrorCode::try_from(partition.error_code)?);
        }
    }

    let fetched: FetchResponse = request(
        &broker,
        &extensions,
        &mut correlation_id,
        12,
        FetchRequest::default()
            .replica_id(Some(-1))
            .max_wait_ms(0)
            .min_bytes(1)
            .max_bytes(Some(1024 * 1024))
            .isolation_level(Some(0))
            .session_id(Some(0))
            .session_epoch(Some(-1))
            .topics(Some(vec![
                FetchTopic::default()
                    .topic(Some(TOPIC.into()))
                    .partitions(Some(vec![
                        FetchPartition::default()
                            .partition(0)
                            .current_leader_epoch(Some(-1))
                            .fetch_offset(0)
                            .last_fetched_epoch(Some(-1))
                            .log_start_offset(Some(-1))
                            .partition_max_bytes(1024 * 1024),
                    ])),
            ]))
            .forgotten_topics_data(Some(vec![]))
            .rack_id(Some(String::new())),
    )
    .await?;

    let batches = fetched
        .responses
        .unwrap_or_default()
        .into_iter()
        .flat_map(|topic| topic.partitions.unwrap_or_default())
        .inspect(|partition| {
            assert!(matches!(
                ErrorCode::try_from(partition.error_code),
                Ok(ErrorCode::None)
            ));
        })
        .filter_map(|partition| partition.records)
        .map(|records| records.batches.len())
        .sum::<usize>();
    assert!(batches > 0, "the fetch returns the produced batches");

    let logs = capture.text();

    // The broker logged the decoded requests, so the capture covers the
    // paths that this test guards.
    assert!(logs.contains("auth_bytes: [hidden]"), "{logs}");
    assert!(logs.contains("salted_password: [hidden]"), "{logs}");
    assert!(logs.contains("record_data_len"), "{logs}");

    assert_hidden(&logs, "the password", PASSWORD.as_bytes());
    assert_hidden(&logs, "the salted password", &salted_password);
    assert_hidden(&logs, "the record key", RECORD_KEY);
    assert_hidden(&logs, "the record value", RECORD_VALUE);
    assert_hidden(&logs, "the record header value", HEADER_VALUE);

    for (index, message) in exchanged.iter().enumerate() {
        assert_hidden(&logs, &format!("SASL message {index}"), message);
    }

    Ok(())
}

// No in-memory (dynostore) leg: DynoStore keeps no SCRAM credentials, so the
// login above can't succeed there.

#[cfg(feature = "libsql")]
mod lite {
    use super::*;

    #[tokio::test]
    async fn secrets_stay_out_of_logs() -> Result<()> {
        let storage = lite_storage(Uuid::now_v7(), rng().random_range(0..i32::MAX)).await?;
        super::secrets_stay_out_of_logs(storage).await
    }
}

#[cfg(feature = "turso")]
mod turso {
    use super::*;

    #[tokio::test]
    async fn secrets_stay_out_of_logs() -> Result<()> {
        let storage =
            crate::common::turso_storage(Uuid::now_v7(), rng().random_range(0..i32::MAX)).await?;
        super::secrets_stay_out_of_logs(storage).await
    }
}

#[cfg(feature = "slatedb")]
mod slatedb {
    use super::*;

    #[tokio::test]
    async fn secrets_stay_out_of_logs() -> Result<()> {
        let storage = slate_storage(Uuid::now_v7(), rng().random_range(0..i32::MAX)).await?;
        super::secrets_stay_out_of_logs(storage).await
    }
}

#[cfg(feature = "postgres")]
mod pg {
    use super::*;

    #[tokio::test]
    async fn secrets_stay_out_of_logs() -> Result<()> {
        let storage = postgres_storage(Uuid::now_v7(), rng().random_range(0..i32::MAX)).await?;
        super::secrets_stay_out_of_logs(storage).await
    }
}
