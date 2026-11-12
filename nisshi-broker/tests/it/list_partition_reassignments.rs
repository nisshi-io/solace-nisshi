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

//! The broker never moves a partition, so `ListPartitionReassignments` answers that no
//! reassignment is in progress, as Kafka does when nothing is moving: no error and an empty
//! topics list, whether the request lists every topic or names some, existing or not.

#[cfg(feature = "dynostore")]
mod in_memory {
    use std::assert_matches;

    use nisshi_broker::Result;
    use nisshi_sans_io::{
        ErrorCode, ListPartitionReassignmentsRequest, RequestInput,
        create_topics_request::CreatableTopic,
        list_partition_reassignments_request::ListPartitionReassignmentsTopics,
    };
    use nisshi_storage::{ListPartitionReassignmentsService, Storage};
    use rama::{Service, extensions::Extensions};
    use rand::{prelude::*, rng};
    use uuid::Uuid;

    use crate::common::{alphanumeric_string, init_tracing, memory_storage, register_broker};

    const NUM_PARTITIONS: i32 = 3;

    #[tokio::test]
    async fn no_reassignment_is_in_progress() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);
        let storage = memory_storage(cluster_id, broker_id).await?;
        register_broker(cluster_id, broker_id, &storage).await?;

        let topic = alphanumeric_string(15);
        _ = storage
            .create_topic(
                CreatableTopic::default()
                    .name(topic.clone())
                    .num_partitions(NUM_PARTITIONS)
                    .replication_factor(1),
                false,
            )
            .await?;

        let service = ListPartitionReassignmentsService { storage };

        let named = vec![
            ListPartitionReassignmentsTopics::default()
                .name(topic)
                .partition_indexes(Some((0..NUM_PARTITIONS).collect())),
            ListPartitionReassignmentsTopics::default()
                .name(alphanumeric_string(15))
                .partition_indexes(Some(vec![0])),
        ];

        for topics in [None, Some(named), Some(vec![])] {
            let response = service
                .serve(RequestInput {
                    request: ListPartitionReassignmentsRequest::default().topics(topics.clone()),
                    extensions: Extensions::default(),
                })
                .await?;

            assert_eq!(
                ErrorCode::None,
                ErrorCode::try_from(response.error_code)?,
                "{topics:?}"
            );
            assert_eq!(None, response.error_message, "{topics:?}");
            assert_matches!(response.topics.as_deref(), Some([]), "{topics:?}");
        }

        Ok(())
    }
}
