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

//! [`ShutdownExtension`]: when its token is cancelled, a connection stops reading requests,
//! answers the request that it has started to read, and then closes.

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use bytes::Bytes;
use nisshi_sans_io::BytesInput;
use nisshi_service::{Error as ServiceError, ShutdownExtension, TcpBytesLayer};
use rama::{
    Layer as _, Service, error::BoxError, extensions::Extensions, tcp::TcpStream as RamaTcpStream,
};
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::{TcpListener, TcpStream},
    sync::{Notify, Semaphore},
    task::JoinHandle,
    time::{sleep, timeout},
};
use tokio_util::sync::CancellationToken;

use crate::common::init_tracing;

const READ_TIMEOUT: Duration = Duration::from_secs(5);

/// A frame: a 4 byte length, then that many bytes.
const FRAME: &[u8] = b"\x00\x00\x00\x04ping";

/// Answers each frame with itself, once the test releases it.
///
/// The handler counts its calls, and notifies `entered` when a call starts, so a test can
/// cancel the token while a request is in the handler.
#[derive(Clone, Debug)]
struct GatedEcho {
    calls: Arc<AtomicUsize>,
    entered: Arc<Notify>,
    gate: Arc<Semaphore>,
}

/// More calls than any test makes.
const ALL_CALLS: usize = 1024;

impl GatedEcho {
    /// Returns an echo that holds each call until the test releases it.
    fn closed() -> Self {
        Self {
            calls: Arc::default(),
            entered: Arc::default(),
            gate: Arc::new(Semaphore::new(0)),
        }
    }

    /// Returns an echo that answers each call at once.
    fn open() -> Self {
        let echo = Self::closed();
        echo.release(ALL_CALLS);
        echo
    }

    fn release(&self, calls: usize) {
        self.gate.add_permits(calls);
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl Service<BytesInput> for GatedEcho {
    type Output = Bytes;
    type Error = ServiceError;

    async fn serve(&self, input: BytesInput) -> Result<Self::Output, Self::Error> {
        _ = self.calls.fetch_add(1, Ordering::SeqCst);
        self.entered.notify_one();

        if let Ok(permit) = self.gate.acquire().await {
            permit.forget();
        }

        Ok(input.bytes)
    }
}

/// Serves one connection with [`TcpBytesLayer`] over `echo`, with a [`ShutdownExtension`] that
/// holds `token`.
async fn serve_one(
    echo: GatedEcho,
    token: CancellationToken,
) -> Result<(TcpStream, JoinHandle<Result<(), ServiceError>>), BoxError> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let client = TcpStream::connect(listener.local_addr()?).await?;
    let (stream, _) = listener.accept().await?;

    let extensions = Extensions::default();
    _ = extensions.insert(ShutdownExtension::new(token));

    let handle = tokio::spawn(async move {
        TcpBytesLayer
            .into_layer(echo)
            .serve(RamaTcpStream::from_tokio_tcp_stream(stream, extensions))
            .await
    });

    Ok((client, handle))
}

async fn read_response(client: &mut TcpStream) -> Result<(), BoxError> {
    let mut response = [0u8; FRAME.len()];
    _ = timeout(READ_TIMEOUT, client.read_exact(&mut response)).await??;
    assert_eq!(FRAME, &response[..]);
    Ok(())
}

/// Fails unless the next read on `client` is the end of the stream: not data, and not a
/// reset.
async fn assert_end_of_stream(client: &mut TcpStream) -> Result<(), BoxError> {
    let mut buffer = [0u8; 1];
    let read = timeout(READ_TIMEOUT, client.read(&mut buffer)).await?;
    assert_eq!(0, read?, "the connection is still open");
    Ok(())
}

#[tokio::test]
async fn an_idle_connection_closes_when_the_drain_starts() -> Result<(), BoxError> {
    let _guard = init_tracing()?;

    let token = CancellationToken::new();
    let (mut client, handle) = serve_one(GatedEcho::open(), token.clone()).await?;

    // Gives the connection time to wait for the first byte of a request, so that the
    // cancellation wakes that wait.
    sleep(Duration::from_millis(100)).await;
    token.cancel();

    assert_end_of_stream(&mut client).await?;
    assert!(timeout(READ_TIMEOUT, handle).await??.is_ok());

    Ok(())
}

#[tokio::test]
async fn a_request_read_before_the_drain_gets_its_response_then_eof() -> Result<(), BoxError> {
    let _guard = init_tracing()?;

    let echo = GatedEcho::closed();
    let token = CancellationToken::new();
    let (mut client, handle) = serve_one(echo.clone(), token.clone()).await?;

    client.write_all(FRAME).await?;
    timeout(READ_TIMEOUT, echo.entered.notified()).await?;

    token.cancel();
    echo.release(1);

    read_response(&mut client).await?;

    // The broker has stopped reading requests, so it discards this one instead of serving it.
    // The handler is open, so a request that the broker serves gets an answer.
    echo.release(ALL_CALLS);
    client.write_all(FRAME).await?;

    assert_end_of_stream(&mut client).await?;
    assert!(timeout(READ_TIMEOUT, handle).await??.is_ok());
    assert_eq!(1, echo.calls());

    Ok(())
}

#[tokio::test]
async fn a_pipelined_request_is_not_served_and_the_first_response_survives() -> Result<(), BoxError>
{
    let _guard = init_tracing()?;

    let echo = GatedEcho::closed();
    let token = CancellationToken::new();
    let (mut client, handle) = serve_one(echo.clone(), token.clone()).await?;

    // The second frame waits unread in the broker's receive buffer while the handler holds the
    // first.
    client.write_all(&[FRAME, FRAME].concat()).await?;
    timeout(READ_TIMEOUT, echo.entered.notified()).await?;

    token.cancel();
    echo.release(ALL_CALLS);

    // The client reads only after the broker has closed its side. A socket that closes with
    // unread bytes in its receive buffer sends a reset, and a reset that arrives before the
    // client reads can discard the response.
    assert!(timeout(READ_TIMEOUT, handle).await??.is_ok());
    sleep(Duration::from_millis(100)).await;

    read_response(&mut client).await?;
    assert_end_of_stream(&mut client).await?;
    assert_eq!(1, echo.calls());

    Ok(())
}

#[tokio::test]
async fn a_request_that_has_started_arriving_is_finished() -> Result<(), BoxError> {
    let _guard = init_tracing()?;

    let echo = GatedEcho::open();
    let token = CancellationToken::new();
    let (mut client, handle) = serve_one(echo.clone(), token.clone()).await?;

    let (head, tail) = FRAME.split_at(2);
    client.write_all(head).await?;

    // Gives the broker time to read the first byte of the size prefix before the drain starts.
    sleep(Duration::from_millis(100)).await;
    token.cancel();
    sleep(Duration::from_millis(100)).await;

    client.write_all(tail).await?;

    read_response(&mut client).await?;
    assert_end_of_stream(&mut client).await?;
    assert!(timeout(READ_TIMEOUT, handle).await??.is_ok());
    assert_eq!(1, echo.calls());

    Ok(())
}
