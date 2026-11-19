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

//! A broker shares the topics that Metadata answered across its connections, so a failed read
//! of the storage answers a topic that one connection read on every other connection, with its
//! partitions and no leader.

use std::{net::SocketAddr, sync::Arc, time::Duration};

use anyhow::{Result, anyhow};
use nisshi_broker::{NODE_ID, broker::Broker, coordinator::group::administrator::Controller};
use nisshi_sans_io::{
    ApiKey as _, ErrorCode, MetadataRequest, MetadataResponse, NULL_TOPIC_ID,
    create_topics_request::CreatableTopic, metadata_request::MetadataRequestTopic,
};
use nisshi_storage::{ArcDynStorage, Storage as _, TopicId};
use nisshi_storage_dynostore::DynoStore;
use tokio::{
    net::TcpStream,
    time::{Instant, sleep, timeout},
};
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

use crate::{
    common::{
        alphanumeric_string, init_tracing,
        wire::{free_port, round_trip},
    },
    fetch_metadata_storage_error::FailMetaReadStore,
};

const TEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Sends a Metadata request for `topic` at v12 on `client`.
async fn metadata(client: &mut TcpStream, topic: &str) -> Result<MetadataResponse> {
    let frame = round_trip(
        client,
        MetadataRequest::KEY,
        12,
        1,
        MetadataRequest::default()
            .allow_auto_topic_creation(Some(false))
            .include_topic_authorized_operations(Some(false))
            .topics(Some(vec![
                MetadataRequestTopic::default()
                    .name(Some(topic.into()))
                    .topic_id(Some(NULL_TOPIC_ID)),
            ]))
            .into(),
    )
    .await?;

    Ok(MetadataResponse::try_from(frame.body)?)
}

/// Returns the topic code, and each partition's code and leader, of the only topic in
/// `response`.
fn codes(response: MetadataResponse) -> Result<(i16, Vec<(i16, i32)>)> {
    let topic = response
        .topics
        .unwrap_or_default()
        .pop()
        .ok_or_else(|| anyhow!("the metadata response has no topic"))?;

    Ok((
        topic.error_code,
        topic
            .partitions
            .unwrap_or_default()
            .iter()
            .map(|partition| (partition.error_code, partition.leader_id))
            .collect(),
    ))
}

#[tokio::test]
async fn a_topic_read_on_one_connection_is_answered_without_a_leader_on_another() -> Result<()> {
    let _guard = init_tracing()?;

    timeout(TEST_TIMEOUT, async {
        let object_store = Arc::new(FailMetaReadStore::new());
        let cluster_id = format!("metadata-known-topics-{}", Uuid::now_v7());
        let addr = SocketAddr::from(([127, 0, 0, 1], free_port().await?));
        let listener = Url::parse(&format!("tcp://{addr}"))?;

        let storage: ArcDynStorage = Arc::new(Box::new(
            DynoStore::new(&cluster_id, NODE_ID, object_store.clone())?
                .advertised_listener(listener.clone())
                .schemas(None)
                .lake(None),
        ));

        let topic = alphanumeric_string(15);
        _ = storage
            .create_topic(
                CreatableTopic::default()
                    .name(topic.clone())
                    .num_partitions(2)
                    .replication_factor(1)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            )
            .await?;

        let token = CancellationToken::new();
        let run = tokio::spawn(
            Broker::new(
                NODE_ID,
                &cluster_id,
                listener.clone(),
                listener,
                storage.clone(),
                Controller::with_storage(storage.clone())?,
                Uuid::now_v7(),
            )
            .with_cancellation(token.clone())
            .run(Instant::now()),
        );

        let mut first = loop {
            match TcpStream::connect(addr).await {
                Ok(client) => break client,
                Err(_) => sleep(Duration::from_millis(50)).await,
            }
        };

        assert_eq!(
            (
                i16::from(ErrorCode::None),
                vec![(i16::from(ErrorCode::None), NODE_ID); 2]
            ),
            codes(metadata(&mut first, &topic).await?)?
        );

        object_store.arm();

        // The storage answers a repeat read of `meta.json` from its cache for a few seconds, so
        // the test waits until a read through the broker's own storage fails.
        while storage
            .metadata(Some(&[TopicId::Name(topic.clone())]))
            .await
            .is_ok()
        {
            sleep(Duration::from_millis(100)).await;
        }

        let mut second = TcpStream::connect(addr).await?;

        assert_eq!(
            (
                i16::from(ErrorCode::None),
                vec![(i16::from(ErrorCode::LeaderNotAvailable), -1); 2]
            ),
            codes(metadata(&mut second, &topic).await?)?
        );

        token.cancel();
        _ = run.await?;

        Ok(())
    })
    .await?
}
