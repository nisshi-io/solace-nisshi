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

use clap::Parser;
use dotenv::dotenv;
use nisshi_client::{Client, ConnectionManager, Error};
use nisshi_sans_io::{CreateTopicsRequest, create_topics_request::CreatableTopic};
use tracing_subscriber::{
    EnvFilter, fmt::format::FmtSpan, layer::SubscriberExt, util::SubscriberInitExt,
};
use url::Url;

type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Debug, Parser)]
#[command(
    version,
    about = "Create Topic",
    long_about = None,
)]
struct Arg {
    #[arg(long, default_value = "tcp://localhost:9092")]
    broker: Url,

    topic: String,

    #[arg(long, default_value = "3")]
    num_partitions: i32,

    #[arg(long, default_value = "1")]
    replication_factor: i16,

    #[arg(long, default_value = "5000")]
    timeout: i32,
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

    let origin = ConnectionManager::builder(arg.broker)
        .client_id(Some(env!("CARGO_PKG_NAME").into()))
        .build()
        .await
        .map(Client::new)?;

    origin
        .call(
            CreateTopicsRequest::default()
                .timeout_ms(arg.timeout)
                .validate_only(Some(false))
                .topics(Some(
                    [CreatableTopic::default()
                        .num_partitions(arg.num_partitions)
                        .configs(Some([].into()))
                        .assignments(Some([].into()))
                        .replication_factor(arg.replication_factor)
                        .name(arg.topic)]
                    .into(),
                )),
        )
        .await
        .inspect(|response| println!("{response:?}"))
        .and(Ok(()))
}
