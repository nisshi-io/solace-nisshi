// Copyright ⓒ Solace Corporation
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

use clap::Parser;
use dotenv::dotenv;
use nisshi_client::{
    BytesConnectionService, ConnectionManager, Error, FrameConnectionLayer, FramePoolLayer,
};
use nisshi_service::{
    BytesFrameLayer, TcpBytesLayer, TcpContextLayer, TcpListenerInput, TcpListenerLayer, host_port,
};
use rama::{Layer as _, Service as _, extensions::Extensions};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::{
    EnvFilter, fmt::format::FmtSpan, layer::SubscriberExt, util::SubscriberInitExt,
};
use url::Url;

type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Debug, Parser)]
#[command(
    version,
    about = "Proxy",
    long_about = None,
)]
struct Arg {
    #[arg(long, default_value = "tcp://localhost:9092")]
    listen: Url,

    #[arg(long, default_value = "tcp://localhost:19092")]
    origin: Url,
}

#[tokio::main]
async fn main() -> Result<()> {
    _ = dotenv().ok();

    tracing_subscriber::registry()
        .with(EnvFilter::from_default_env())
        .with(
            tracing_subscriber::fmt::layer()
                .with_level(true)
                .with_line_number(true)
                .with_thread_ids(false)
                .with_span_events(FmtSpan::NONE),
        )
        .init();

    let arg = Arg::parse();

    // forward protocol frames to the origin using a connection pool:
    let origin = ConnectionManager::builder(arg.origin)
        .client_id(Some(env!("CARGO_PKG_NAME").into()))
        .build()
        .await?;

    // a tcp listener used by the proxy
    let listener = TcpListener::bind(host_port(arg.listen).await?).await?;

    // listen for requests until cancelled
    let token = CancellationToken::new();

    let extensions = Extensions::default();

    let stack = (
        // server layers: reading tcp -> bytes -> frames:
        TcpListenerLayer::new(token),
        TcpContextLayer::default(),
        TcpBytesLayer,
        BytesFrameLayer::default(),
        // client layers: writing frames -> connection pool -> bytes -> origin:
        FramePoolLayer::new(origin),
        FrameConnectionLayer,
    )
        .into_layer(BytesConnectionService);

    stack
        .serve(TcpListenerInput {
            listener,
            extensions,
        })
        .await?;

    Ok(())
}
