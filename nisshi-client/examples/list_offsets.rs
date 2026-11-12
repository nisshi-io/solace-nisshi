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

use clap::{Parser, ValueEnum};
use dotenv::dotenv;
use nisshi_client::{Client, ConnectionManager, Error};
use nisshi_sans_io::{
    IsolationLevel, ListOffset, ListOffsetsRequest, MetadataRequest, NULL_TOPIC_ID,
    list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic},
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
    about = "List Offsets",
    long_about = None,
)]
struct Arg {
    #[arg(long, default_value = "tcp://localhost:9092")]
    broker: Url,

    topic: String,

    #[arg(long, default_value = "read-uncommitted")]
    isolation: Isolation,

    #[arg(long, default_value = "earliest")]
    offset: Offset,

    #[arg(long, default_value = "-1")]
    replica_id: i32,

    #[arg(long)]
    max_num_offsets: Option<i32>,

    #[arg(long, default_value = "-1")]
    current_leader_epoch: i32,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, ValueEnum)]
enum Isolation {
    ReadUncommitted,
    ReadCommitted,
}

impl From<Isolation> for Option<i8> {
    fn from(value: Isolation) -> Self {
        match value {
            Isolation::ReadCommitted => Some(IsolationLevel::ReadCommitted.into()),
            Isolation::ReadUncommitted => Some(IsolationLevel::ReadUncommitted.into()),
        }
    }
}

impl From<Isolation> for IsolationLevel {
    fn from(value: Isolation) -> Self {
        match value {
            Isolation::ReadCommitted => Self::ReadCommitted,
            Isolation::ReadUncommitted => Self::ReadUncommitted,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, ValueEnum)]
enum Offset {
    Earliest,
    Latest,
}

impl TryFrom<Offset> for i64 {
    type Error = Error;

    fn try_from(value: Offset) -> std::result::Result<Self, Self::Error> {
        match value {
            Offset::Earliest => i64::try_from(ListOffset::Earliest).map_err(Into::into),
            Offset::Latest => i64::try_from(ListOffset::Latest).map_err(Into::into),
        }
    }
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

    let partitions = metadata
        .topics
        .as_deref()
        .unwrap_or_default()
        .iter()
        .find_map(|topic| {
            if topic.name.as_deref().is_some_and(|name| name == arg.topic) {
                topic.partitions.as_deref().and_then(|partitions| {
                    partitions
                        .iter()
                        .map(|partition| partition.partition_index)
                        .max()
                })
            } else {
                None
            }
        })
        .ok_or_else(|| Error::Message(format!("topic not found in metadata: {}", arg.topic)))?;

    let timestamp = arg.offset.try_into()?;

    origin
        .call(
            ListOffsetsRequest::default()
                .isolation_level(arg.isolation.into())
                .replica_id(arg.replica_id)
                .topics(Some(
                    [ListOffsetsTopic::default().name(arg.topic).partitions(Some(
                        (0..partitions)
                            .map(|partition_index| {
                                ListOffsetsPartition::default()
                                    .partition_index(partition_index)
                                    .max_num_offsets(arg.max_num_offsets)
                                    .timestamp(timestamp)
                                    .current_leader_epoch(Some(arg.current_leader_epoch))
                            })
                            .collect::<Vec<_>>(),
                    ))]
                    .into(),
                )),
        )
        .await
        .inspect(|response| println!("{response:?}"))
        .and(Ok(()))
}
