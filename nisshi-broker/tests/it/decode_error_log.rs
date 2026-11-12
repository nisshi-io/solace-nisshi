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

//! A request that fails, over a real socket, logs a line of the same size
//! however large the request is: the broker never writes the request's bytes
//! to the log. One case sends a string field that is not valid UTF-8, which
//! fails to decode; the other sends an API that the broker doesn't route.
//!
//! Both errors are logged at ERROR, the level of the default filter, by
//! `TcpBytesService` (`stream.rs`) and by the broker's connection task
//! (`broker.rs`).

use std::{
    io,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Result, anyhow};
use bytes::Bytes;
use nisshi_sans_io::{
    ApiKey as _, Frame, Header, MetadataRequest, PushTelemetryRequest,
    metadata_request::MetadataRequestTopic,
};
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::TcpStream,
    time::{Instant, sleep, timeout},
};
use tracing::Level;

use crate::common::wire::spawn_broker;

/// Request sizes: a small one, and one whose bytes, printed one by one,
/// would far exceed [`MAXIMUM_LINE`].
const SMALL: usize = 1024;
const LARGE: usize = 256 * 1024;

/// The longest log line a failed request may write.
const MAXIMUM_LINE: usize = 512;

/// The bytes of the invalid or unrouted field, repeated. They are never
/// valid UTF-8.
const FILL: [u8; 2] = [0xfe, 0xff];

/// Renderings of [`FILL`] that a log holding the request's bytes would
/// contain: a decimal list, `Bytes`-style escapes, and hex.
const FILL_FORMS: [&str; 4] = ["254, 255", r"\xfe\xff", "fe, ff", "feff"];

/// `len` bytes of [`FILL`].
fn filled(len: usize) -> Vec<u8> {
    FILL.iter().copied().cycle().take(len).collect()
}

/// How much the log of the large request may differ in size from the log of
/// the small one: room for the digits of the lengths in the error.
const SLACK: usize = 64;

const DEADLINE: Duration = Duration::from_secs(10);

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

/// The two places that log a failed request.
const SITES: [&str; 2] = [
    "nisshi-service/src/stream.rs",
    "nisshi-broker/src/broker.rs",
];

/// What the error of each case prints, so a request that fails earlier with a
/// different error doesn't pass for the case it was built to reach.
const FROM_UTF8: &str = "FromUtf8 {";
const UNKNOWN_SERVICE_FRAME: &str = "UnknownServiceFrame {";

/// Whether `logs` holds a line from each of the two error sites that reports
/// `variant`.
fn both_sites(logs: &str, variant: &str) -> bool {
    SITES.iter().all(|site| {
        logs.lines()
            .any(|line| line.contains(site) && line.contains(variant))
    })
}

/// The lines of `logs` that report `variant`.
fn variant_lines<'a>(logs: &'a str, variant: &str) -> impl Iterator<Item = &'a str> {
    let variant = variant.to_owned();
    logs.lines().filter(move |line| line.contains(&variant))
}

/// Sends `frame` to a new broker, waits for the broker to close the
/// connection and log the failure at both sites as `variant`, and returns
/// what it logged.
///
/// The subscriber is the thread's default, at ERROR, as with `RUST_LOG`
/// unset. The test runs on a current-thread runtime, so the broker's
/// connection task logs on this thread.
async fn logs_for(frame: Bytes, variant: &str) -> Result<String> {
    let capture = Capture::default();
    let writer = capture.clone();

    let _guard = tracing::subscriber::set_default(
        tracing_subscriber::fmt()
            .with_max_level(Level::ERROR)
            .with_file(true)
            .with_ansi(false)
            .with_writer(move || writer.clone())
            .finish(),
    );

    let broker = spawn_broker(None).await?;

    let mut stream = TcpStream::connect(broker.addr).await?;
    stream.write_all(&frame).await?;
    stream.flush().await?;

    let mut response = Vec::new();
    _ = timeout(DEADLINE, stream.read_to_end(&mut response)).await??;

    let deadline = Instant::now() + DEADLINE;

    while !both_sites(&capture.text(), variant) {
        if Instant::now() > deadline {
            return Err(anyhow!(
                "no {variant} error logged at both sites: {}",
                capture.text()
            ));
        }

        sleep(Duration::from_millis(10)).await;
    }

    drop(broker);

    Ok(capture.text())
}

/// Asserts that the `variant` lines of the small and large request are about
/// the same size, that no line is longer than [`MAXIMUM_LINE`], and that no
/// line holds the request's bytes, even as a short prefix. A failure names
/// sizes and forms only, so it doesn't print the bytes it found.
///
/// Only the `variant` lines are compared in size, so an unrelated ERROR line
/// that one run writes and the other doesn't can't fail the comparison.
fn assert_bounded(small: &str, large: &str, variant: &str) {
    for (name, logs) in [("small", small), ("large", large)] {
        for line in logs.lines() {
            assert!(
                line.len() <= MAXIMUM_LINE,
                "the {name} request logged a line of {} bytes",
                line.len()
            );
        }

        for form in FILL_FORMS {
            assert!(
                !logs.contains(form),
                "the {name} request logged its bytes as {form:?}"
            );
        }
    }

    let small_size: usize = variant_lines(small, variant).map(str::len).sum();
    let large_size: usize = variant_lines(large, variant).map(str::len).sum();

    assert!(
        large_size.abs_diff(small_size) <= SLACK,
        "a {SMALL} byte field logged {small_size} bytes of {variant} lines, \
         a {LARGE} byte field {large_size}"
    );
}

/// A Metadata request whose topic name is `len` bytes of [`FILL`].
fn invalid_topic_name(len: usize) -> Result<Bytes> {
    let frame = Frame::request(
        Header::Request {
            api_key: MetadataRequest::KEY,
            api_version: 12,
            correlation_id: 1,
            client_id: Some(env!("CARGO_PKG_NAME").into()),
        },
        MetadataRequest::default()
            .topics(Some(
                [MetadataRequestTopic::default()
                    .topic_id(Some([0; 16]))
                    .name(Some("a".repeat(len)))]
                .into(),
            ))
            .allow_auto_topic_creation(Some(false))
            .include_topic_authorized_operations(Some(false))
            .into(),
    )?;

    // The encoder only takes a `String`, so write the name valid and replace
    // its bytes, which keeps every length prefix intact.
    let placeholder = vec![b'a'; len];
    let start = frame
        .windows(len)
        .position(|window| window == placeholder.as_slice())
        .ok_or_else(|| anyhow!("the frame holds the topic name"))?;

    let mut patched = frame.to_vec();
    patched[start..start + len].copy_from_slice(&filled(len));
    Ok(Bytes::from(patched))
}

/// A PushTelemetry request, which the broker doesn't route, carrying `len`
/// bytes of [`FILL`] as metrics.
fn unrouted(len: usize) -> Result<Bytes> {
    Frame::request(
        Header::Request {
            api_key: PushTelemetryRequest::KEY,
            api_version: 0,
            correlation_id: 1,
            client_id: Some(env!("CARGO_PKG_NAME").into()),
        },
        PushTelemetryRequest::default()
            .client_instance_id([0; 16])
            .metrics(Bytes::from(filled(len)))
            .into(),
    )
    .map_err(Into::into)
}

#[tokio::test(flavor = "current_thread")]
async fn invalid_utf8() -> Result<()> {
    let small = logs_for(invalid_topic_name(SMALL)?, FROM_UTF8).await?;
    let large = logs_for(invalid_topic_name(LARGE)?, FROM_UTF8).await?;

    assert_bounded(&small, &large, FROM_UTF8);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn unrouted_request() -> Result<()> {
    let small = logs_for(unrouted(SMALL)?, UNKNOWN_SERVICE_FRAME).await?;
    let large = logs_for(unrouted(LARGE)?, UNKNOWN_SERVICE_FRAME).await?;

    assert_bounded(&small, &large, UNKNOWN_SERVICE_FRAME);

    // The line names the API, so an operator can tell what was refused.
    assert!(
        large.contains("api_name: \"PushTelemetryRequest\""),
        "the unrouted request's log doesn't name its API"
    );
    Ok(())
}
