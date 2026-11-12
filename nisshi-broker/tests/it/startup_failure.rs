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

//! A broker that cannot start must report it, so the process exits non-zero
//! and a supervisor restarts it.

use std::time::Duration;

use anyhow::Result;
use nisshi_broker::{NODE_ID, broker::Broker, coordinator::group::administrator::Controller};
use nisshi_storage::ArcDynStorage;
use tokio::{
    net::TcpListener,
    time::{Instant, timeout},
};
use url::Url;
use uuid::Uuid;

#[tokio::test]
async fn main_fails_when_the_listener_cannot_bind() -> Result<()> {
    let occupied = TcpListener::bind("127.0.0.1:0").await?;
    let listener = Url::parse(&format!("tcp://{}", occupied.local_addr()?))?;

    let broker = Broker::<Controller<ArcDynStorage>, ArcDynStorage>::builder()
        .node_id(NODE_ID)
        .cluster_id(format!("startup-{}", Uuid::now_v7()))
        .incarnation_id(Uuid::now_v7())
        .advertised_listener(listener.clone())
        .storage(Url::parse("memory://")?)
        .listener(listener)
        .silent(true)
        .build()
        .await?;

    let result = timeout(Duration::from_secs(30), broker.main(Instant::now())).await?;

    assert!(result.is_err(), "expected an error, got {result:?}");
    Ok(())
}
