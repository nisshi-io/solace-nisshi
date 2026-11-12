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

//! Reads the broker's state with typed requests, for a test that changes the state with a Kafka
//! tool and then checks what the broker stored. A test that read the state from a tool's printed
//! table would also depend on how the tool formats it.

use nisshi_client::{Client, ConnectionManager};
use nisshi_sans_io::{
    ErrorCode, OffsetFetchRequest, OffsetFetchResponse,
    offset_fetch_request::{OffsetFetchRequestGroup, OffsetFetchRequestTopics},
};
use tokio::runtime::{Builder, Runtime};
use url::Url;

use crate::settings;

/// A connection to a broker that asks it for its state.
#[derive(Debug)]
pub struct BrokerStateReader {
    runtime: Runtime,
    client: Client,
    bootstrap: String,
}

impl BrokerStateReader {
    /// Connects to the broker at `bootstrap`, a `host:port` as the Kafka tools take it. Panics if
    /// the harness can't connect, because then no test of that broker can check its state.
    #[track_caller]
    pub fn connect(bootstrap: &str) -> Self {
        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("the harness starts a runtime for its broker client");

        // The function panics in its body, not in a closure, so that `track_caller` reports the
        // test's line.
        let broker = match Url::parse(&format!("tcp://{bootstrap}")) {
            Ok(broker) => broker,
            Err(err) => panic!("`{bootstrap}` isn't a broker address: {err}"),
        };

        let pool = match runtime.block_on(
            ConnectionManager::builder(broker)
                .client_id(Some(env!("CARGO_PKG_NAME").into()))
                .build(),
        ) {
            Ok(pool) => pool,
            Err(err) => panic!("the harness can't connect to `{bootstrap}`: {err}"),
        };
        let client = Client::new(pool);

        Self {
            runtime,
            client,
            bootstrap: bootstrap.to_owned(),
        }
    }

    /// Connects to the broker that all tests share, at [`settings::shared_broker_bootstrap`].
    #[track_caller]
    pub fn shared() -> Self {
        Self::connect(&settings::shared_broker_bootstrap())
    }

    /// Returns the offset that `group` committed on `topic`'s `partition`, or `None` if it has
    /// committed none there. Panics if the broker answers `OffsetFetch` with an error.
    #[track_caller]
    pub fn committed_offset(&self, group: &str, topic: &str, partition: u32) -> Option<i64> {
        let partition_index = i32::try_from(partition).expect("a partition index fits in an i32");

        // The client sends the newest version that the broker supports, which takes the groups
        // in `groups` and requires `require_stable` and `member_epoch`. A member epoch of -1
        // fetches as a client outside the group, as the admin client does.
        let request = OffsetFetchRequest::default()
            .require_stable(Some(false))
            .groups(Some(vec![
                OffsetFetchRequestGroup::default()
                    .group_id(group.into())
                    .member_epoch(Some(-1))
                    .topics(Some(vec![
                        OffsetFetchRequestTopics::default()
                            .name(topic.into())
                            .partition_indexes(Some(vec![partition_index])),
                    ])),
            ]));

        let response = match self.runtime.block_on(self.client.call(request)) {
            Ok(response) => response,
            Err(err) => panic!("`{}` didn't answer OffsetFetch: {err}", self.bootstrap),
        };
        let OffsetFetchResponse { groups, .. } = &response;

        let Some(fetched_group) = groups
            .iter()
            .flatten()
            .find(|fetched_group| fetched_group.group_id == group)
        else {
            panic!(
                "`{}` didn't answer OffsetFetch for {group}: {response:?}",
                self.bootstrap
            )
        };

        assert_eq!(
            fetched_group.error_code,
            i16::from(ErrorCode::None),
            "`{}` answered OffsetFetch for {group} with an error: {response:?}",
            self.bootstrap
        );

        let Some(fetched_partition) = fetched_group
            .topics
            .iter()
            .flatten()
            .filter(|fetched_topic| fetched_topic.name == topic)
            .flat_map(|fetched_topic| fetched_topic.partitions.iter().flatten())
            .find(|fetched_partition| fetched_partition.partition_index == partition_index)
        else {
            panic!(
                "`{}` didn't answer OffsetFetch for {group} on {topic}-{partition}: {response:?}",
                self.bootstrap
            )
        };

        assert_eq!(
            fetched_partition.error_code,
            i16::from(ErrorCode::None),
            "`{}` answered OffsetFetch for {group} on {topic}-{partition} with an error: \
             {response:?}",
            self.bootstrap
        );

        // Kafka answers -1 for a partition on which the group has committed no offset.
        (fetched_partition.committed_offset >= 0).then_some(fetched_partition.committed_offset)
    }
}
