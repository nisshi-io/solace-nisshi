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
use nisshi_client::{Client, ConnectionManager, Error};
use nisshi_sans_io::{
    FetchRequest, MetadataRequest, NULL_TOPIC_ID,
    fetch_request::{FetchPartition, FetchTopic},
    metadata_request::MetadataRequestTopic,
};
use tracing_subscriber::{
    EnvFilter, fmt::format::FmtSpan, layer::SubscriberExt, util::SubscriberInitExt,
};
use url::Url;

type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Debug, Parser)]
#[command(
    version,
    about = "Fetch",
    long_about = None,
)]
struct Arg {
    /// The URL of the broker to fetch messages from
    #[arg(long, default_value = "tcp://localhost:9092")]
    broker: Url,

    /// The topic to fetch messages from
    topic: String,

    /// The partition to fetch messages from
    #[arg(long, default_value = "0")]
    partition: i32,

    /// The maximum time in milliseconds to wait for a message
    #[arg(long, default_value = "5000")]
    max_wait_time_ms: i32,

    /// The minimum number of bytes to wait for
    #[arg(long, default_value = "1")]
    min_bytes: i32,

    /// The maximum bytes to wait for
    #[arg(long, default_value = "52428800")]
    max_bytes: Option<i32>,

    /// The fetch offset to start from
    #[arg(long, default_value = "0")]
    fetch_offset: i64,

    /// The partition to consume from
    #[arg(long, default_value = "1048576")]
    partition_max_bytes: i32,
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

    let metadata = origin
        .call(
            MetadataRequest::default()
                .allow_auto_topic_creation(Some(false))
                .include_cluster_authorized_operations(Some(false))
                .include_topic_authorized_operations(Some(false))
                .topics(Some(
                    [MetadataRequestTopic::default()
                        .name(Some(arg.topic.clone()))
                        .topic_id(Some(NULL_TOPIC_ID))]
                    .into(),
                )),
        )
        .await?;

    let topic_id = metadata
        .topics
        .as_deref()
        .unwrap_or_default()
        .iter()
        .find(|topic| topic.name.as_deref().is_some_and(|name| name == arg.topic))
        .and_then(|topic| topic.topic_id)
        .ok_or_else(|| Error::Message(format!("topic id not found for {}", arg.topic)))?;

    origin
        .call(
            FetchRequest::default()
                .cluster_id(None)
                .replica_id(None)
                .replica_state(None)
                .max_wait_ms(arg.max_wait_time_ms)
                .min_bytes(arg.min_bytes)
                .max_bytes(arg.max_bytes)
                .isolation_level(Some(0))
                .session_id(Some(-1))
                .session_epoch(Some(-1))
                .topics(Some(vec![
                    FetchTopic::default()
                        .topic(None)
                        .topic_id(Some(topic_id))
                        .partitions(Some(vec![
                            FetchPartition::default()
                                .partition(arg.partition)
                                .current_leader_epoch(Some(0))
                                .fetch_offset(arg.fetch_offset)
                                .last_fetched_epoch(Some(-1))
                                .log_start_offset(Some(-1))
                                .partition_max_bytes(arg.partition_max_bytes)
                                .replica_directory_id(None),
                        ])),
                ]))
                .forgotten_topics_data(Some([].into()))
                .rack_id(Some("".into())),
        )
        .await
        .inspect(|response| println!("{response:?}"))
        .and(Ok(()))
}
