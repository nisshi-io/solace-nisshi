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

//! [`CloseAfterResponseExtension`]: a handler that marks it gets its response
//! written, and then the connection closes. Without the mark the connection
//! serves the next request.

use std::time::Duration;

use bytes::Bytes;
use nisshi_sans_io::{BytesInput, CloseAfterResponseExtension};
use nisshi_service::{Error as ServiceError, TcpBytesLayer};
use rama::{
    Layer as _, Service,
    error::BoxError,
    extensions::{Extensions, ExtensionsRef as _},
    tcp::TcpStream as RamaTcpStream,
};
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::{TcpListener, TcpStream},
    task::JoinHandle,
    time::timeout,
};

use crate::common::init_tracing;

const READ_TIMEOUT: Duration = Duration::from_secs(5);

/// A frame: a 4 byte length, then that many bytes.
const FRAME: &[u8] = b"\x00\x00\x00\x04ping";

/// Answers each frame with itself, and marks the connection to close when
/// `close` is set.
#[derive(Clone, Copy, Debug)]
struct Echo {
    close: bool,
}

impl Service<BytesInput> for Echo {
    type Output = Bytes;
    type Error = ServiceError;

    async fn serve(&self, input: BytesInput) -> Result<Self::Output, Self::Error> {
        if self.close {
            CloseAfterResponseExtension::mark(input.extensions());
        }

        Ok(input.bytes)
    }
}

/// Serves one connection with [`TcpBytesLayer`] over `echo`.
async fn serve_one(
    echo: Echo,
) -> Result<(TcpStream, JoinHandle<Result<(), ServiceError>>), BoxError> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let client = TcpStream::connect(listener.local_addr()?).await?;
    let (stream, _) = listener.accept().await?;

    let handle = tokio::spawn(async move {
        TcpBytesLayer
            .into_layer(echo)
            .serve(RamaTcpStream::from_tokio_tcp_stream(
                stream,
                Extensions::default(),
            ))
            .await
    });

    Ok((client, handle))
}

async fn exchange(client: &mut TcpStream) -> Result<(), BoxError> {
    client.write_all(FRAME).await?;

    let mut response = [0u8; FRAME.len()];
    _ = timeout(READ_TIMEOUT, client.read_exact(&mut response)).await??;
    assert_eq!(FRAME, &response[..]);

    Ok(())
}

#[tokio::test]
async fn marked_response_is_written_then_the_connection_closes() -> Result<(), BoxError> {
    let _guard = init_tracing()?;

    let (mut client, handle) = serve_one(Echo { close: true }).await?;

    exchange(&mut client).await?;

    let mut buffer = [0u8; 1];
    let read = timeout(READ_TIMEOUT, client.read(&mut buffer)).await?;
    assert_eq!(0, read?, "the connection is still open");

    // The loop ends the connection without an error.
    assert!(timeout(READ_TIMEOUT, handle).await??.is_ok());

    Ok(())
}

#[tokio::test]
async fn unmarked_connection_serves_the_next_request() -> Result<(), BoxError> {
    let _guard = init_tracing()?;

    let (mut client, _handle) = serve_one(Echo { close: false }).await?;

    exchange(&mut client).await?;
    exchange(&mut client).await?;

    Ok(())
}
