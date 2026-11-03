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

//! OffsetCommit answers `UNKNOWN_TOPIC_OR_PARTITION` for a partition index
//! that an existing topic does not have, on every engine, stores no offset
//! for it, and commits the other partitions of the request.

use std::{assert_matches, collections::BTreeMap};

use crate::common::{self, StorageType, alphanumeric_string, init_tracing, register_broker};
use nisshi_broker::Result;
use nisshi_sans_io::{ErrorCode, create_topics_request::CreatableTopic};
use nisshi_storage::{OffsetCommitRequest, Storage, Topition};
use rand::{prelude::*, rng};
use tracing::debug;
use url::Url;
use uuid::Uuid;

const PARTITIONS: i32 = 3;
const OFFSET: i64 = 42;

pub async fn offset_commit_rejects_partition_outside_topic<C, G>(
    cluster_id: C,
    broker_id: i32,
    sc: G,
) -> Result<()>
where
    C: Into<String>,
    G: Storage,
{
    register_broker(cluster_id, broker_id, &sc).await?;

    let topic_name: String = alphanumeric_string(15);
    let group_id: String = alphanumeric_string(15);
    debug!(?topic_name, ?group_id);

    _ = sc
        .create_topic(
            CreatableTopic::default()
                .name(topic_name.clone())
                .num_partitions(PARTITIONS)
                .replication_factor(0)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    let committed = Topition::new(topic_name.as_str(), PARTITIONS - 1);

    let expected_responses = vec![
        (committed.clone(), ErrorCode::None),
        (
            Topition::new(topic_name.as_str(), PARTITIONS),
            ErrorCode::UnknownTopicOrPartition,
        ),
        (
            Topition::new(topic_name.as_str(), -1),
            ErrorCode::UnknownTopicOrPartition,
        ),
        (
            Topition::new(topic_name.as_str(), i32::MAX),
            ErrorCode::UnknownTopicOrPartition,
        ),
    ];

    let offsets = expected_responses
        .iter()
        .map(|(topition, _)| {
            (
                topition.to_owned(),
                OffsetCommitRequest::default().offset(OFFSET),
            )
        })
        .collect::<Vec<_>>();

    assert_eq!(
        expected_responses,
        sc.offset_commit(&group_id, None, &offsets).await?
    );

    assert_eq!(
        BTreeMap::from([(committed, OFFSET)]),
        sc.committed_offset_topitions(&group_id)
            .await?
            .into_iter()
            .map(|(topition, committed)| (topition, committed.offset))
            .collect::<BTreeMap<_, _>>()
    );

    let fetched = sc
        .offset_fetch(
            Some(&group_id),
            &offsets
                .iter()
                .map(|(topition, _)| topition.to_owned())
                .collect::<Vec<_>>(),
            None,
        )
        .await?;

    for (topition, error_code) in &expected_responses {
        if *error_code == ErrorCode::None {
            assert_eq!(
                Some(OFFSET),
                fetched.get(topition).map(|committed| committed.offset),
                "{topition:?}"
            );
        } else {
            assert_matches!(
                fetched.get(topition).map(|committed| committed.offset),
                None | Some(-1),
                "{topition:?}"
            );
        }
    }

    Ok(())
}

async fn storage_container(
    storage_type: StorageType,
    cluster: impl Into<String> + Clone,
    node: i32,
) -> Result<nisshi_storage::ArcDynStorage> {
    common::storage_container(
        storage_type,
        cluster,
        node,
        Url::parse("tcp://127.0.0.1/")?,
        None,
    )
    .await
}

async fn run(storage_type: StorageType) -> Result<()> {
    let _guard = init_tracing()?;

    let cluster_id = Uuid::now_v7();
    let broker_id = rng().random_range(0..i32::MAX);

    offset_commit_rejects_partition_outside_topic(
        cluster_id,
        broker_id,
        storage_container(storage_type, cluster_id, broker_id).await?,
    )
    .await
}

#[cfg(feature = "postgres")]
mod pg {
    use super::*;

    #[tokio::test]
    async fn offset_commit_rejects_partition_outside_topic() -> Result<()> {
        run(StorageType::Postgres).await
    }
}

#[cfg(feature = "dynostore")]
mod in_memory {
    use super::*;

    #[tokio::test]
    async fn offset_commit_rejects_partition_outside_topic() -> Result<()> {
        run(StorageType::InMemory).await
    }
}

#[cfg(feature = "libsql")]
mod lite {
    use super::*;

    #[tokio::test]
    async fn offset_commit_rejects_partition_outside_topic() -> Result<()> {
        run(StorageType::Lite).await
    }
}

#[cfg(feature = "turso")]
mod turso {
    use super::*;

    #[ignore = "the turso backend fails its connection check in tests"]
    #[tokio::test]
    async fn offset_commit_rejects_partition_outside_topic() -> Result<()> {
        run(StorageType::Turso).await
    }
}
