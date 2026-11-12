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

//! An offset stage read and a ListOffsets lookup answer
//! `UNKNOWN_TOPIC_OR_PARTITION` for a topic or partition that does not
//! exist, and the broker keeps no watermark for it. Deleting a topic drops
//! its watermarks. The broker keeps each other watermark that it caches, so
//! a client that sends many indexes, or creates and deletes many topics,
//! would otherwise grow the broker's memory without bound.

use std::{assert_matches, sync::Arc, time::SystemTime};

use nisshi_sans_io::{
    ErrorCode, IsolationLevel, ListOffset, create_topics_request::CreatableTopic,
};
use nisshi_storage::{Error, Storage as _, TopicId, Topition};
use object_store::memory::InMemory;

use crate::dynostore::{DynoStore, tests::init_tracing};

const TOPIC: &str = "abc";
const PARTITIONS: i32 = 2;

/// Creates [`TOPIC`] with `partitions` partitions.
async fn create(storage: &DynoStore, partitions: i32) -> Result<(), Error> {
    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(TOPIC.into())
                .num_partitions(partitions)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    Ok(())
}

/// Storage with [`TOPIC`], created with [`PARTITIONS`] partitions.
async fn storage() -> Result<DynoStore, Error> {
    let storage = DynoStore::new("nisshi", 1, Arc::new(InMemory::new()))?;

    create(&storage, PARTITIONS).await?;

    Ok(storage)
}

/// Partitions that do not exist: indexes outside [`TOPIC`], and a partition
/// of a topic that was never created.
fn unknown() -> [Topition; 4] {
    [
        Topition::new(TOPIC, -1),
        Topition::new(TOPIC, PARTITIONS),
        Topition::new(TOPIC, i32::MAX),
        Topition::new("never-created", 0),
    ]
}

fn has_watermark(storage: &DynoStore, topition: &Topition) -> Result<bool, Error> {
    Ok(storage.watermarks.lock()?.contains_key(topition))
}

#[tokio::test]
async fn offset_stage_of_unknown_partition_keeps_no_watermark() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = storage().await?;

    for topition in unknown() {
        assert_matches!(
            storage.offset_stage(&topition).await,
            Err(Error::Api(ErrorCode::UnknownTopicOrPartition)),
            "{topition:?}"
        );
        assert!(!has_watermark(&storage, &topition)?, "{topition:?}");
    }

    let known = Topition::new(TOPIC, PARTITIONS - 1);
    assert_eq!(0, storage.offset_stage(&known).await?.high_watermark());

    Ok(())
}

#[tokio::test]
async fn list_offsets_of_unknown_partition_keeps_no_watermark() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = storage().await?;

    for request in [
        ListOffset::Earliest,
        ListOffset::Latest,
        ListOffset::Timestamp(SystemTime::UNIX_EPOCH),
    ] {
        let offsets = unknown()
            .into_iter()
            .map(|topition| (topition, request))
            .collect::<Vec<_>>();

        let responses = storage
            .list_offsets(IsolationLevel::ReadUncommitted, &offsets)
            .await?;

        assert_eq!(offsets.len(), responses.len());

        for (topition, response) in responses {
            assert_eq!(
                ErrorCode::UnknownTopicOrPartition,
                response.error_code,
                "{topition:?} {request:?}"
            );
            assert_eq!(None, response.offset, "{topition:?} {request:?}");
            assert!(!has_watermark(&storage, &topition)?, "{topition:?}");
        }
    }

    let known = Topition::new(TOPIC, 0);
    let responses = storage
        .list_offsets(
            IsolationLevel::ReadUncommitted,
            &[(known, ListOffset::Earliest)],
        )
        .await?;

    assert_eq!(ErrorCode::None, responses[0].1.error_code);
    assert_eq!(Some(0), responses[0].1.offset);

    Ok(())
}

#[tokio::test]
async fn delete_topic_drops_its_watermarks() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = storage().await?;

    for partition in 0..PARTITIONS {
        _ = storage
            .offset_stage(&Topition::new(TOPIC, partition))
            .await?;
    }

    assert_eq!(
        ErrorCode::None,
        storage.delete_topic(&TopicId::Name(TOPIC.into())).await?
    );

    for partition in 0..PARTITIONS {
        let topition = Topition::new(TOPIC, partition);
        assert!(!has_watermark(&storage, &topition)?, "{topition:?}");
    }

    // A topic created again with fewer partitions does not answer from a
    // watermark of the deleted topic.
    create(&storage, 1).await?;

    assert_matches!(
        storage
            .offset_stage(&Topition::new(TOPIC, PARTITIONS - 1))
            .await,
        Err(Error::Api(ErrorCode::UnknownTopicOrPartition))
    );

    Ok(())
}
