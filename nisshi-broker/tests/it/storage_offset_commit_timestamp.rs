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

//! A committed offset may carry the time of the commit, a leader epoch and metadata. Every
//! backend must accept a commit that sets them, and `offset_fetch` must return the offset that
//! the commit stored.

use std::time::SystemTime;

use crate::common::{self, StorageType, alphanumeric_string, init_tracing};
use nisshi_broker::Result;
use nisshi_sans_io::{ErrorCode, create_topics_request::CreatableTopic};
use nisshi_storage::{OffsetCommitRequest, Storage, Topition};
use rand::{prelude::*, rng};
use url::Url;
use uuid::Uuid;

async fn commit_with_timestamp(storage: impl Storage) -> Result<()> {
    let topic = alphanumeric_string(15);
    let group_id = &alphanumeric_string(15)[..];

    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(topic.clone())
                .num_partitions(1)
                .replication_factor(0)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    let topition = Topition::new(topic, 0);

    let committed = storage
        .offset_commit(
            group_id,
            None,
            &[(
                topition.clone(),
                OffsetCommitRequest {
                    offset: 5,
                    leader_epoch: Some(0),
                    timestamp: Some(SystemTime::now()),
                    metadata: Some("metadata".into()),
                },
            )],
        )
        .await?;
    assert_eq!(ErrorCode::None, committed[0].1);

    let offsets = storage
        .offset_fetch(Some(group_id), std::slice::from_ref(&topition), None)
        .await?;
    assert_eq!(Some(&5), offsets.get(&topition));

    Ok(())
}

macro_rules! backend {
    ($name:ident, $feature:literal, $storage_type:expr) => {
        #[cfg(feature = $feature)]
        mod $name {
            use nisshi_storage::ArcDynStorage;

            use super::*;

            async fn storage_container() -> Result<ArcDynStorage> {
                common::storage_container(
                    $storage_type,
                    Uuid::now_v7(),
                    rng().random_range(0..i32::MAX),
                    Url::parse("tcp://127.0.0.1/")?,
                    None,
                )
                .await
            }

            #[tokio::test]
            async fn commit_with_timestamp() -> Result<()> {
                let _guard = init_tracing()?;
                super::commit_with_timestamp(storage_container().await?).await
            }
        }
    };
}

backend!(in_memory, "dynostore", StorageType::InMemory);
backend!(lite, "libsql", StorageType::Lite);
backend!(pg, "postgres", StorageType::Postgres);
backend!(slatedb, "slatedb", StorageType::SlateDb);
backend!(turso, "turso", StorageType::Turso);
