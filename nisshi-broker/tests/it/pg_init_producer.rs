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

#[cfg(feature = "postgres")]
use crate::common::{
    StorageType, alphanumeric_string, init_tracing, register_broker, storage_container,
};

#[cfg(feature = "postgres")]
use rand::{prelude::*, rng};

#[cfg(feature = "postgres")]
use nisshi_broker::Result;

#[cfg(feature = "postgres")]
use nisshi_storage::Storage;

#[cfg(feature = "postgres")]
use tracing::debug;

#[cfg(feature = "postgres")]
use url::Url;

#[cfg(feature = "postgres")]
use uuid::Uuid;

#[tokio::test]
#[cfg(feature = "postgres")]
async fn with_txn() -> Result<()> {
    let _guard = init_tracing()?;

    let mut rng = rng();

    let cluster_id = Uuid::now_v7();
    let broker_id = rng.random_range(0..i32::MAX);

    let sc = storage_container(
        StorageType::Postgres,
        cluster_id,
        broker_id,
        Url::parse("tcp://127.0.0.1/")?,
        None,
    )
    .await?;

    register_broker(cluster_id, broker_id, &sc).await?;

    let transaction_id: String = alphanumeric_string(10);
    debug!(?transaction_id);

    let transaction_timeout_ms = 10_000;

    let first = sc
        .init_producer(
            Some(transaction_id.as_str()),
            transaction_timeout_ms,
            Some(-1),
            Some(-1),
        )
        .await?;
    debug!(?first);

    assert_eq!(0, first.epoch);

    let second = sc
        .init_producer(
            Some(transaction_id.as_str()),
            transaction_timeout_ms,
            Some(-1),
            Some(-1),
        )
        .await?;
    debug!(?second);

    assert_eq!(first.id, second.id);
    assert_eq!(1, second.epoch);

    Ok(())
}
