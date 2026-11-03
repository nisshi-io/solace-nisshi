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

//! An OffsetCommit answers `UNKNOWN_TOPIC_OR_PARTITION` for a partition
//! index that an existing topic does not have, and stores no offset for it,
//! while the other partitions of the request are committed.

use std::{assert_matches, collections::BTreeMap, sync::Arc};

use nisshi_sans_io::{ErrorCode, create_topics_request::CreatableTopic};
use nisshi_storage::{Error, OffsetCommitRequest, Storage as _, Topition};
use object_store::memory::InMemory;

use crate::dynostore::{DynoStore, tests::init_tracing};

const TOPIC: &str = "abc";
const PARTITIONS: i32 = 2;
const GROUP: &str = "group";
const OFFSET: i64 = 42;

#[tokio::test]
async fn offset_commit_rejects_partition_outside_topic() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = DynoStore::new("nisshi", 1, Arc::new(InMemory::new()))?;

    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(TOPIC.into())
                .num_partitions(PARTITIONS)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    let committed = Topition::new(TOPIC, PARTITIONS - 1);

    let expected_responses = vec![
        (committed.clone(), ErrorCode::None),
        (
            Topition::new(TOPIC, PARTITIONS),
            ErrorCode::UnknownTopicOrPartition,
        ),
        (Topition::new(TOPIC, -1), ErrorCode::UnknownTopicOrPartition),
        (
            Topition::new(TOPIC, i32::MAX),
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
        storage.offset_commit(GROUP, None, &offsets).await?
    );

    assert_eq!(
        BTreeMap::from([(committed, OFFSET)]),
        storage
            .committed_offset_topitions(GROUP)
            .await?
            .into_iter()
            .map(|(topition, committed)| (topition, committed.offset))
            .collect::<BTreeMap<_, _>>()
    );

    let fetched = storage
        .offset_fetch(
            Some(GROUP),
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
