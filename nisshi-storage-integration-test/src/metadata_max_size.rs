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

//! Commits offsets with metadata of a given length, and reads back the committed offsets.

use std::{iter, slice};

use crate::{broker, random};
use nisshi_broker::Result;
use nisshi_sans_io::create_topics_request::CreatableTopic;
use nisshi_storage::{Storage, Topition};

pub(crate) const OFFSET: i64 = 10;

/// The longest metadata that Kafka accepts, by default.
pub(crate) const MAX_METADATA_LENGTH: usize = 4096;

/// Returns metadata of `length` characters.
pub(crate) fn metadata_of_length(length: usize) -> String {
    iter::repeat_n('m', length).collect()
}

/// Registers a broker and creates a topic with two partitions, returning the topic's name.
pub(crate) async fn create_topic<G>(
    cluster_id: impl Into<String>,
    broker_id: i32,
    storage: &G,
) -> Result<String>
where
    G: Storage,
{
    broker::register_broker(cluster_id, broker_id, storage).await?;

    let topic = random::alphanumeric_string(15);

    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(topic.clone())
                .num_partitions(2)
                .replication_factor(0)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    Ok(topic)
}

/// Returns the offset that `storage` holds for `partition` of `topic` in `group_id`, or -1.
pub(crate) async fn committed_offset<G>(
    storage: &G,
    group_id: &str,
    topic: &str,
    partition: i32,
) -> Result<i64>
where
    G: Storage,
{
    let topition = Topition::new(topic.to_owned(), partition);

    Ok(storage
        .offset_fetch(Some(group_id), slice::from_ref(&topition), Some(false))
        .await?
        .get(&topition)
        .map_or(-1, |committed| committed.offset))
}
