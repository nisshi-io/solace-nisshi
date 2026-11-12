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

use std::{result, time::Duration};

use clap::Parser;
use dotenv::dotenv;
use nisshi_client::{
    BytesConnectionService, Client, ConnectionManager, ConsumerGroupLayer, FrameConnectionLayer,
    FramePoolLayer,
};
use nisshi_sans_io::{
    CoordinatorType, FindCoordinatorRequest, MetadataRequest, NULL_TOPIC_ID,
    metadata_request::MetadataRequestTopic,
};
use rama::{Layer as _, Service};
use tracing::debug;
use tracing_subscriber::{
    EnvFilter, fmt::format::FmtSpan, layer::SubscriberExt, util::SubscriberInitExt,
};
use url::Url;

type Result<T> = result::Result<T, anyhow::Error>;

fn parse_duration(s: &str) -> Result<Duration> {
    humantime::parse_duration(s).map_err(anyhow::Error::new)
}

#[derive(Clone, Debug, Parser)]
#[command(
    version,
    about = "Group Consumer",
    long_about = None,
)]
struct Arg {
    /// The URL of the broker
    #[arg(long, default_value = "tcp://localhost:9092")]
    broker: Url,

    /// The topics within the group
    #[arg(long)]
    topics: Vec<String>,

    /// The group name
    #[arg(long, default_value = "test")]
    group: String,

    #[arg(long, default_value = "3s", value_parser = parse_duration)]
    heartbeat_interval: Duration,
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

    let pool = ConnectionManager::builder(arg.broker)
        .client_id(Some(env!("CARGO_PKG_NAME").into()))
        .build()
        .await
        .inspect(|pool| debug!(?pool))?;

    let origin = Client::new(pool.clone());

    let _coordinator = origin
        .call(
            FindCoordinatorRequest::default()
                .coordinator_keys(Some([arg.group.clone()].into()))
                .key(Some(arg.group.clone()))
                .key_type(Some(CoordinatorType::Group.into())),
        )
        .await
        .inspect(|coordinator| debug!(?coordinator))?;

    let metadata = origin
        .call(
            MetadataRequest::default()
                .allow_auto_topic_creation(Some(false))
                .include_cluster_authorized_operations(Some(false))
                .include_topic_authorized_operations(Some(false))
                .topics(Some(
                    arg.topics
                        .iter()
                        .map(|topic| {
                            MetadataRequestTopic::default()
                                .name(Some(topic.to_owned()))
                                .topic_id(Some(NULL_TOPIC_ID))
                        })
                        .collect(),
                )),
        )
        .await?;

    (
        ConsumerGroupLayer::new(arg.group, arg.topics, metadata),
        FramePoolLayer::new(pool.clone()),
        FrameConnectionLayer,
    )
        .into_layer(BytesConnectionService)
        .serve(())
        .await
        .map_err(Into::into)
}
