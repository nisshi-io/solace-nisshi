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

use crate::common::{alphanumeric_string, init_tracing};
use nisshi_broker::Error;
use nisshi_sans_io::{
    CreateTopicsRequest, DescribeTopicPartitionsRequest, ErrorCode, NULL_TOPIC_ID, RequestInput,
    create_topics_request::{CreatableReplicaAssignment, CreatableTopic, CreatableTopicConfig},
    create_topics_response::CreatableTopicResult,
    describe_topic_partitions_request::TopicRequest,
};
use nisshi_storage::{
    CreateTopicsService, CreateTopicsValidationLayer, DescribeTopicPartitionsService, Storage,
};
use rama::{Layer as _, Service as _, extensions::Extensions};

async fn create(storage: impl Storage + Clone) -> Result<(), Error> {
    let service = CreateTopicsValidationLayer::new().layer(CreateTopicsService { storage });

    let name = alphanumeric_string(15);
    let num_partitions = 5;
    let replication_factor = 3;
    let assignments = Some([].into());
    let configs = Some([].into());

    let response = service
        .serve(RequestInput {
            request: CreateTopicsRequest::default()
                .topics(Some(vec![
                    CreatableTopic::default()
                        .name(name.clone())
                        .num_partitions(num_partitions)
                        .replication_factor(replication_factor)
                        .assignments(assignments)
                        .configs(configs),
                ]))
                .validate_only(Some(false)),
            extensions: Extensions::default(),
        })
        .await?;

    let topics = response.topics.unwrap_or_default();

    assert_eq!(1, topics.len());
    assert_eq!(name, topics[0].name.as_str());
    assert_ne!(Some(NULL_TOPIC_ID), topics[0].topic_id);
    assert_eq!(Some(5), topics[0].num_partitions);
    assert_eq!(Some(3), topics[0].replication_factor);
    assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);

    Ok(())
}

async fn create_with_default(node_id: i32, storage: impl Storage + Clone) -> Result<(), Error> {
    let service = CreateTopicsValidationLayer::new().layer(CreateTopicsService {
        storage: storage.clone(),
    });

    let name = alphanumeric_string(15);
    let num_partitions = -1;
    let replication_factor = -1;
    let assignments = Some([].into());
    let configs = Some([].into());

    let extensions = Extensions::default();

    let response = service
        .serve(RequestInput {
            request: CreateTopicsRequest::default()
                .topics(Some(vec![
                    CreatableTopic::default()
                        .name(name.clone())
                        .num_partitions(num_partitions)
                        .replication_factor(replication_factor)
                        .assignments(assignments)
                        .configs(configs),
                ]))
                .validate_only(Some(false)),
            extensions: extensions.clone(),
        })
        .await?;

    let topics = response.topics.unwrap_or_default();

    assert_eq!(1, topics.len());
    assert_eq!(name, topics[0].name.as_str());
    assert_ne!(Some(NULL_TOPIC_ID), topics[0].topic_id);
    assert_eq!(Some(1), topics[0].num_partitions);
    assert_eq!(Some(1), topics[0].replication_factor);
    assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);

    let service = DescribeTopicPartitionsService {
        storage: storage.clone(),
    };

    let response = service
        .serve(RequestInput {
            request: DescribeTopicPartitionsRequest::default()
                .topics(Some([TopicRequest::default().name(name.clone())].into())),
            extensions: extensions.clone(),
        })
        .await?;

    let topics = response.topics.unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(Some(name), topics[0].name);
    assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);
    let partitions = topics[0].partitions.as_deref().unwrap_or_default();

    assert_eq!(1, partitions.len());

    for (index, partition) in partitions.iter().enumerate() {
        assert_eq!(index as i32, partition.partition_index);
        assert_eq!(node_id, partition.leader_id);
        assert_eq!(0, partition.leader_epoch);
        assert_eq!(ErrorCode::None, ErrorCode::try_from(partition.error_code)?);
    }

    let offline_replicas = partitions[0]
        .offline_replicas
        .as_deref()
        .unwrap_or_default();
    assert!(offline_replicas.is_empty());

    let last_known_elr = partitions[0].last_known_elr.as_deref().unwrap_or_default();
    assert!(last_known_elr.is_empty());

    let eligible_leader_replicas = partitions[0]
        .eligible_leader_replicas
        .as_deref()
        .unwrap_or_default();
    assert!(eligible_leader_replicas.is_empty());

    let isr_nodes = partitions[0].isr_nodes.as_deref().unwrap_or_default();
    assert_eq!(1, isr_nodes.len());
    assert!(isr_nodes.iter().all(|isr_node| *isr_node == node_id));

    Ok(())
}

async fn duplicate(storage: impl Storage + Clone) -> Result<(), Error> {
    let service = CreateTopicsValidationLayer::new().layer(CreateTopicsService {
        storage: storage.clone(),
    });

    let name = alphanumeric_string(15);
    let num_partitions = 5;
    let replication_factor = 3;
    let assignments = Some([].into());
    let configs = Some([].into());

    let extensions = Extensions::default();

    let response = service
        .serve(RequestInput {
            request: CreateTopicsRequest::default()
                .topics(Some(vec![
                    CreatableTopic::default()
                        .name(name.clone())
                        .num_partitions(num_partitions)
                        .replication_factor(replication_factor)
                        .assignments(assignments.clone())
                        .configs(configs.clone()),
                ]))
                .validate_only(Some(false)),
            extensions: extensions.clone(),
        })
        .await?;

    let topics = response.topics.unwrap_or_default();

    assert_eq!(1, topics.len());
    assert_eq!(name, topics[0].name.as_str());
    assert_ne!(Some(NULL_TOPIC_ID), topics[0].topic_id);
    assert_eq!(Some(5), topics[0].num_partitions);
    assert_eq!(Some(3), topics[0].replication_factor);
    assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);

    let response = service
        .serve(RequestInput {
            request: CreateTopicsRequest::default()
                .topics(Some(vec![
                    CreatableTopic::default()
                        .name(name.clone())
                        .num_partitions(num_partitions)
                        .replication_factor(replication_factor)
                        .assignments(assignments)
                        .configs(configs),
                ]))
                .validate_only(Some(false)),
            extensions: extensions.clone(),
        })
        .await?;

    let topics = response.topics.unwrap_or_default();

    assert_eq!(1, topics.len());
    assert_eq!(name, topics[0].name.as_str());
    assert_eq!(Some(NULL_TOPIC_ID), topics[0].topic_id);
    assert_eq!(Some(5), topics[0].num_partitions);
    assert_eq!(Some(3), topics[0].replication_factor);
    assert_eq!(
        ErrorCode::TopicAlreadyExists,
        ErrorCode::try_from(topics[0].error_code)?
    );
    Ok(())
}

/// Topics with an invalid name must be rejected with `InvalidTopicException`,
/// and must never actually reach storage. `DescribeTopicPartitions` never
/// auto-creates (only `Metadata` does), so if the rejected name shows up as
/// `UnknownTopicOrPartition` there, `CreateTopics` genuinely never called
/// `Storage::create_topic` for it.
async fn invalid_name_rejected(storage: impl Storage + Clone) -> Result<(), Error> {
    let create = CreateTopicsValidationLayer::new().layer(CreateTopicsService {
        storage: storage.clone(),
    });
    let describe = DescribeTopicPartitionsService {
        storage: storage.clone(),
    };

    let num_partitions = 3;
    let replication_factor = 1;
    let assignments = Some([].into());
    let configs = Some([].into());

    let too_long: String = "a".repeat(250);
    let invalid_names: [&str; 5] = ["", "a/b", ".", "..", too_long.as_str()];

    for name in invalid_names {
        let response = create
            .serve(RequestInput {
                request: CreateTopicsRequest::default()
                    .topics(Some(vec![
                        CreatableTopic::default()
                            .name(name.into())
                            .num_partitions(num_partitions)
                            .replication_factor(replication_factor)
                            .assignments(assignments.clone())
                            .configs(configs.clone()),
                    ]))
                    .validate_only(Some(false)),
                extensions: Extensions::default(),
            })
            .await?;

        let topics = response.topics.unwrap_or_default();
        assert_eq!(1, topics.len());
        assert_eq!(name, topics[0].name.as_str());
        assert_eq!(Some(NULL_TOPIC_ID), topics[0].topic_id);
        assert_eq!(
            ErrorCode::InvalidTopicException,
            ErrorCode::try_from(topics[0].error_code)?,
            "name = {name:?}"
        );

        let describe_response = describe
            .serve(RequestInput {
                request: DescribeTopicPartitionsRequest::default()
                    .topics(Some([TopicRequest::default().name(name.into())].into())),
                extensions: Extensions::default(),
            })
            .await?;

        let describe_topics = describe_response.topics.unwrap_or_default();
        assert_eq!(1, describe_topics.len());
        assert_eq!(
            ErrorCode::UnknownTopicOrPartition,
            ErrorCode::try_from(describe_topics[0].error_code)?,
            "name = {name:?} must never have reached storage"
        );
    }

    // boundary: 249 characters is the longest valid name, and must succeed.
    let boundary_name: String = "a".repeat(249);

    let response = create
        .serve(RequestInput {
            request: CreateTopicsRequest::default()
                .topics(Some(vec![
                    CreatableTopic::default()
                        .name(boundary_name.clone())
                        .num_partitions(num_partitions)
                        .replication_factor(replication_factor)
                        .assignments(assignments.clone())
                        .configs(configs.clone()),
                ]))
                .validate_only(Some(false)),
            extensions: Extensions::default(),
        })
        .await?;

    let topics = response.topics.unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(boundary_name, topics[0].name.as_str());
    assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);

    Ok(())
}

/// `num_partitions` of `0` or less than `-1` must be rejected with
/// `InvalidPartitions`; `-1` (the "use the default" sentinel) must still
/// succeed.
async fn invalid_partitions_rejected(storage: impl Storage + Clone) -> Result<(), Error> {
    let service = CreateTopicsValidationLayer::new().layer(CreateTopicsService {
        storage: storage.clone(),
    });

    let replication_factor = 1;
    let assignments = Some([].into());
    let configs = Some([].into());

    for num_partitions in [0, -2] {
        let name = alphanumeric_string(15);

        let response = service
            .serve(RequestInput {
                request: CreateTopicsRequest::default()
                    .topics(Some(vec![
                        CreatableTopic::default()
                            .name(name.clone())
                            .num_partitions(num_partitions)
                            .replication_factor(replication_factor)
                            .assignments(assignments.clone())
                            .configs(configs.clone()),
                    ]))
                    .validate_only(Some(false)),
                extensions: Extensions::default(),
            })
            .await?;

        let topics = response.topics.unwrap_or_default();
        assert_eq!(1, topics.len());
        assert_eq!(name, topics[0].name.as_str());
        assert_eq!(Some(NULL_TOPIC_ID), topics[0].topic_id);
        assert_eq!(
            ErrorCode::InvalidPartitions,
            ErrorCode::try_from(topics[0].error_code)?,
            "num_partitions = {num_partitions}"
        );
    }

    // -1 still means "use the broker default".
    let name = alphanumeric_string(15);

    let response = service
        .serve(RequestInput {
            request: CreateTopicsRequest::default()
                .topics(Some(vec![
                    CreatableTopic::default()
                        .name(name.clone())
                        .num_partitions(-1)
                        .replication_factor(-1)
                        .assignments(assignments.clone())
                        .configs(configs.clone()),
                ]))
                .validate_only(Some(false)),
            extensions: Extensions::default(),
        })
        .await?;

    let topics = response.topics.unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(name, topics[0].name.as_str());
    assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);
    assert_eq!(Some(1), topics[0].num_partitions);

    Ok(())
}

/// `replication_factor` of `0`, less than `-1`, or above 64 must be rejected
/// with `InvalidReplicationFactor` under both values of `validate_only`, and
/// the topic must never reach storage (SlateDB's `create_topic` ignores
/// `validate_only`, so the check has to run before storage either way).
/// `-1` (the "use the default" sentinel) and 64 must still succeed.
async fn invalid_replication_factor_rejected(storage: impl Storage + Clone) -> Result<(), Error> {
    let service = CreateTopicsValidationLayer::new().layer(CreateTopicsService {
        storage: storage.clone(),
    });
    let describe = DescribeTopicPartitionsService {
        storage: storage.clone(),
    };

    let num_partitions = 3;
    let assignments = Some([].into());
    let configs = Some([].into());

    for validate_only in [false, true] {
        for replication_factor in [0, -2, 65, i16::MAX] {
            let name = alphanumeric_string(15);

            let response = service
                .serve(RequestInput {
                    request: CreateTopicsRequest::default()
                        .topics(Some(vec![
                            CreatableTopic::default()
                                .name(name.clone())
                                .num_partitions(num_partitions)
                                .replication_factor(replication_factor)
                                .assignments(assignments.clone())
                                .configs(configs.clone()),
                        ]))
                        .validate_only(Some(validate_only)),
                    extensions: Extensions::default(),
                })
                .await?;

            let topics = response.topics.unwrap_or_default();
            assert_eq!(1, topics.len());
            assert_eq!(name, topics[0].name.as_str());
            assert_eq!(Some(NULL_TOPIC_ID), topics[0].topic_id);
            assert_eq!(
                ErrorCode::InvalidReplicationFactor,
                ErrorCode::try_from(topics[0].error_code)?,
                "replication_factor = {replication_factor}, validate_only = {validate_only}"
            );
            let expected_message = if replication_factor < 1 {
                String::from(
                    "Replication factor must be larger than 0, or -1 to use the default value.",
                )
            } else {
                format!("Replication factor {replication_factor} is above the maximum of 64.")
            };
            assert_eq!(Some(expected_message), topics[0].error_message);

            let describe_response = describe
                .serve(RequestInput {
                    request: DescribeTopicPartitionsRequest::default()
                        .topics(Some([TopicRequest::default().name(name.clone())].into())),
                    extensions: Extensions::default(),
                })
                .await?;

            let describe_topics = describe_response.topics.unwrap_or_default();
            assert_eq!(1, describe_topics.len());
            assert_eq!(
                ErrorCode::UnknownTopicOrPartition,
                ErrorCode::try_from(describe_topics[0].error_code)?,
                "replication_factor = {replication_factor}, validate_only = {validate_only} \
                 must never have reached storage"
            );
        }
    }

    // Both `num_partitions` and `replication_factor` invalid: the replication
    // factor is checked first, as Kafka's KRaft controller does.
    let name = alphanumeric_string(15);

    let response = service
        .serve(RequestInput {
            request: CreateTopicsRequest::default()
                .topics(Some(vec![
                    CreatableTopic::default()
                        .name(name.clone())
                        .num_partitions(0)
                        .replication_factor(0)
                        .assignments(assignments.clone())
                        .configs(configs.clone()),
                ]))
                .validate_only(Some(false)),
            extensions: Extensions::default(),
        })
        .await?;

    let topics = response.topics.unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(
        ErrorCode::InvalidReplicationFactor,
        ErrorCode::try_from(topics[0].error_code)?
    );

    // -1 still means "use the broker default".
    let name = alphanumeric_string(15);

    let response = service
        .serve(RequestInput {
            request: CreateTopicsRequest::default()
                .topics(Some(vec![
                    CreatableTopic::default()
                        .name(name.clone())
                        .num_partitions(-1)
                        .replication_factor(-1)
                        .assignments(assignments.clone())
                        .configs(configs.clone()),
                ]))
                .validate_only(Some(false)),
            extensions: Extensions::default(),
        })
        .await?;

    let topics = response.topics.unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(name, topics[0].name.as_str());
    assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);
    assert_eq!(Some(1), topics[0].replication_factor);

    // 64 is the largest replication factor accepted.
    let name = alphanumeric_string(15);

    let response = service
        .serve(RequestInput {
            request: CreateTopicsRequest::default()
                .topics(Some(vec![
                    CreatableTopic::default()
                        .name(name.clone())
                        .num_partitions(num_partitions)
                        .replication_factor(64)
                        .assignments(assignments.clone())
                        .configs(configs.clone()),
                ]))
                .validate_only(Some(false)),
            extensions: Extensions::default(),
        })
        .await?;

    let topics = response.topics.unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(name, topics[0].name.as_str());
    assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);
    assert_ne!(Some(NULL_TOPIC_ID), topics[0].topic_id);
    assert_eq!(Some(64), topics[0].replication_factor);

    Ok(())
}

/// One invalid topic in a batch must not affect the others: the invalid
/// entry is rejected, the valid one is still created.
async fn mixed_batch_partial_success(storage: impl Storage + Clone) -> Result<(), Error> {
    let service = CreateTopicsValidationLayer::new().layer(CreateTopicsService {
        storage: storage.clone(),
    });

    let valid_name = alphanumeric_string(15);
    let num_partitions = 3;
    let replication_factor = 1;
    let assignments = Some([].into());
    let configs = Some([].into());

    let response = service
        .serve(RequestInput {
            request: CreateTopicsRequest::default()
                .topics(Some(vec![
                    CreatableTopic::default()
                        .name("".into())
                        .num_partitions(num_partitions)
                        .replication_factor(replication_factor)
                        .assignments(assignments.clone())
                        .configs(configs.clone()),
                    CreatableTopic::default()
                        .name(valid_name.clone())
                        .num_partitions(num_partitions)
                        .replication_factor(replication_factor)
                        .assignments(assignments.clone())
                        .configs(configs.clone()),
                ]))
                .validate_only(Some(false)),
            extensions: Extensions::default(),
        })
        .await?;

    let topics = response.topics.unwrap_or_default();
    assert_eq!(2, topics.len());

    assert_eq!("", topics[0].name.as_str());
    assert_eq!(
        ErrorCode::InvalidTopicException,
        ErrorCode::try_from(topics[0].error_code)?
    );
    assert_eq!(Some(NULL_TOPIC_ID), topics[0].topic_id);

    assert_eq!(valid_name, topics[1].name.as_str());
    assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[1].error_code)?);
    assert_ne!(Some(NULL_TOPIC_ID), topics[1].topic_id);

    Ok(())
}

/// Kafka caps the sum of partitions requested across every topic in one
/// `CreateTopics` call at 10,000, and rejects the whole request, not just
/// the offending topic, before any per-topic validation (including the
/// name check) runs. This pins the boundary using invalid topic names so
/// neither case has to touch storage: exactly the cap lets the request
/// through the total-partitions check and on to per-topic validation,
/// which then rejects both topics for their bad names; one more partition
/// over the cap is rejected as a whole with `PolicyViolation`, including
/// the second topic, whose name is otherwise valid. Exactly the cap passing
/// is a deliberate difference from Kafka 3.9.1; see
/// `in_memory::total_partitions_cap_creates_at_the_boundary`.
async fn total_partitions_cap_rejected(storage: impl Storage + Clone) -> Result<(), Error> {
    let service = CreateTopicsValidationLayer::new().layer(CreateTopicsService {
        storage: storage.clone(),
    });

    let replication_factor = 1;
    let assignments = Some([].into());
    let configs = Some([].into());
    let invalid_name = "a/b";

    // Exactly the cap (9,999 + 1 = 10,000): the total-partitions check
    // lets the request through, so both topics are rejected for their
    // invalid name instead, proving the cap did not fire here.
    let response = service
        .serve(RequestInput {
            request: CreateTopicsRequest::default()
                .topics(Some(vec![
                    CreatableTopic::default()
                        .name(invalid_name.into())
                        .num_partitions(9_999)
                        .replication_factor(replication_factor)
                        .assignments(assignments.clone())
                        .configs(configs.clone()),
                    CreatableTopic::default()
                        .name(invalid_name.into())
                        .num_partitions(1)
                        .replication_factor(replication_factor)
                        .assignments(assignments.clone())
                        .configs(configs.clone()),
                ]))
                .validate_only(Some(false)),
            extensions: Extensions::default(),
        })
        .await?;

    let topics = response.topics.unwrap_or_default();
    assert_eq!(2, topics.len());
    for topic in &topics {
        assert_eq!(
            ErrorCode::InvalidTopicException,
            ErrorCode::try_from(topic.error_code)?,
            "at the cap (10,000 total): expected the request to reach \
             per-topic validation, not be rejected by the cap itself"
        );
    }

    // One more partition over the cap (10,000 + 1 = 10,001): the whole
    // request is rejected with `PolicyViolation`, including the second
    // topic, whose name is otherwise valid and would succeed on its own.
    let valid_name = alphanumeric_string(15);

    let response = service
        .serve(RequestInput {
            request: CreateTopicsRequest::default()
                .topics(Some(vec![
                    CreatableTopic::default()
                        .name(invalid_name.into())
                        .num_partitions(10_000)
                        .replication_factor(replication_factor)
                        .assignments(assignments.clone())
                        .configs(configs.clone()),
                    CreatableTopic::default()
                        .name(valid_name.clone())
                        .num_partitions(1)
                        .replication_factor(replication_factor)
                        .assignments(assignments)
                        .configs(configs),
                ]))
                .validate_only(Some(false)),
            extensions: Extensions::default(),
        })
        .await?;

    let topics = response.topics.unwrap_or_default();
    assert_eq!(2, topics.len());
    assert_eq!(valid_name, topics[1].name.as_str());
    for topic in &topics {
        assert_eq!(Some(NULL_TOPIC_ID), topic.topic_id);
        assert_eq!(
            ErrorCode::PolicyViolation,
            ErrorCode::try_from(topic.error_code)?,
            "name = {:?}: one partition over the cap must reject the whole request",
            topic.name
        );
        assert_eq!(
            Some(
                "Excessively large number of partitions per request: 10001 requested, limit 10000."
            ),
            topic.error_message.as_deref()
        );
    }

    // Each case below uses only invalid names, so nothing reaches storage
    // whether or not the cap fires: below the cap every topic gets
    // `InvalidTopicException`, over it every topic gets `PolicyViolation`.
    let topic = |num_partitions: i32, assignments: usize| {
        CreatableTopic::default()
            .name(invalid_name.into())
            .num_partitions(num_partitions)
            .replication_factor(replication_factor)
            .assignments(Some(
                (0..assignments)
                    .map(|partition_index| {
                        CreatableReplicaAssignment::default()
                            .partition_index(i32::try_from(partition_index).unwrap())
                            .broker_ids(Some(vec![111]))
                    })
                    .collect(),
            ))
            .configs(Some([].into()))
    };

    let cases = [
        // A `-1` topic counts as the broker default of 1 partition.
        (
            "9,999 + default",
            vec![topic(9_999, 0), topic(-1, 0)],
            false,
            ErrorCode::InvalidTopicException,
        ),
        (
            "10,000 + default",
            vec![topic(10_000, 0), topic(-1, 0)],
            false,
            ErrorCode::PolicyViolation,
        ),
        // A negative count can't pull the total back under the cap.
        (
            "large + i32::MIN",
            vec![topic(2_000_000_000, 0), topic(i32::MIN, 0)],
            false,
            ErrorCode::PolicyViolation,
        ),
        // The cap doesn't depend on `validate_only`.
        (
            "10,001 validate_only",
            vec![topic(10_000, 0), topic(1, 0)],
            true,
            ErrorCode::PolicyViolation,
        ),
        // Manual assignments count one partition each, whatever `num_partitions` says.
        (
            "default + 10,001 assignments",
            vec![topic(-1, 10_001)],
            false,
            ErrorCode::PolicyViolation,
        ),
    ];

    for (case, topics, validate_only, expected) in cases {
        let response = service
            .serve(RequestInput {
                request: CreateTopicsRequest::default()
                    .topics(Some(topics))
                    .validate_only(Some(validate_only)),
                extensions: Extensions::default(),
            })
            .await?;

        for topic in response.topics.unwrap_or_default() {
            assert_eq!(expected, ErrorCode::try_from(topic.error_code)?, "{case}");
            assert_eq!(Some(NULL_TOPIC_ID), topic.topic_id, "{case}");
        }
    }

    Ok(())
}

/// One invalid topic in a batch must not affect the others: the topic with
/// an invalid `replication_factor` is rejected, the valid one is still
/// created. The rejected topic's name must itself be valid, so that
/// `InvalidReplicationFactor` is what rejects it rather than the name check
/// (which runs first) firing on a bad name instead.
async fn mixed_batch_partial_success_replication_factor(
    storage: impl Storage + Clone,
) -> Result<(), Error> {
    let service = CreateTopicsValidationLayer::new().layer(CreateTopicsService {
        storage: storage.clone(),
    });

    let invalid_name = alphanumeric_string(15);
    let valid_name = alphanumeric_string(15);
    let num_partitions = 3;
    let assignments = Some([].into());
    let configs = Some([].into());

    let response = service
        .serve(RequestInput {
            request: CreateTopicsRequest::default()
                .topics(Some(vec![
                    CreatableTopic::default()
                        .name(invalid_name.clone())
                        .num_partitions(num_partitions)
                        .replication_factor(0)
                        .assignments(assignments.clone())
                        .configs(configs.clone()),
                    CreatableTopic::default()
                        .name(valid_name.clone())
                        .num_partitions(num_partitions)
                        .replication_factor(1)
                        .assignments(assignments.clone())
                        .configs(configs.clone()),
                ]))
                .validate_only(Some(false)),
            extensions: Extensions::default(),
        })
        .await?;

    let topics = response.topics.unwrap_or_default();
    assert_eq!(2, topics.len());

    assert_eq!(invalid_name, topics[0].name.as_str());
    assert_eq!(
        ErrorCode::InvalidReplicationFactor,
        ErrorCode::try_from(topics[0].error_code)?
    );
    assert_eq!(Some(NULL_TOPIC_ID), topics[0].topic_id);

    assert_eq!(valid_name, topics[1].name.as_str());
    assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[1].error_code)?);
    assert_ne!(Some(NULL_TOPIC_ID), topics[1].topic_id);

    Ok(())
}

/// A topic must not be created with a `retention.ms` or `retention.bytes` that Kafka rejects:
/// maintenance reads the value, and a `retention.ms` below -1 or a value that isn't a long makes it
/// delete every record or fail for every topic.
async fn invalid_retention_config_rejected(storage: impl Storage + Clone) -> Result<(), Error> {
    let service = CreateTopicsValidationLayer::new().layer(CreateTopicsService {
        storage: storage.clone(),
    });
    let describe = DescribeTopicPartitionsService {
        storage: storage.clone(),
    };

    for validate_only in [false, true] {
        for (key, value, reason) in [
            ("retention.ms", "-2", "Value must be at least -1"),
            ("retention.ms", "abc", "Not a number of type LONG"),
            (
                "retention.bytes",
                "99999999999999999999",
                "Not a number of type LONG",
            ),
        ] {
            let name = alphanumeric_string(15);

            let response = service
                .serve(RequestInput {
                    request: CreateTopicsRequest::default()
                        .topics(Some(vec![
                            CreatableTopic::default()
                                .name(name.clone())
                                .num_partitions(1)
                                .replication_factor(1)
                                .assignments(Some([].into()))
                                .configs(Some(vec![
                                    CreatableTopicConfig::default()
                                        .name(key.into())
                                        .value(Some(value.into())),
                                ])),
                        ]))
                        .validate_only(Some(validate_only)),
                    extensions: Extensions::default(),
                })
                .await?;

            let topics = response.topics.unwrap_or_default();
            assert_eq!(1, topics.len());
            assert_eq!(
                ErrorCode::InvalidConfig,
                ErrorCode::try_from(topics[0].error_code)?,
                "{key}={value}, validate_only = {validate_only}"
            );
            assert_eq!(
                Some(format!("Invalid value {value} for configuration {key}: {reason}").as_str()),
                topics[0].error_message.as_deref()
            );

            let describe_response = describe
                .serve(RequestInput {
                    request: DescribeTopicPartitionsRequest::default()
                        .topics(Some([TopicRequest::default().name(name.clone())].into())),
                    extensions: Extensions::default(),
                })
                .await?;

            let describe_topics = describe_response.topics.unwrap_or_default();
            assert_eq!(
                ErrorCode::UnknownTopicOrPartition,
                ErrorCode::try_from(describe_topics[0].error_code)?,
                "{key}={value}, validate_only = {validate_only} must never have reached storage"
            );
        }
    }

    Ok(())
}

/// Kafka answers `TOPIC_ALREADY_EXISTS` for an existing topic before it checks the topic's
/// configs. A client that treats that answer as success, and repeats a create whose configs the
/// broker rejects, must not fail.
async fn existing_topic_with_invalid_config_already_exists(
    storage: impl Storage + Clone,
) -> Result<(), Error> {
    let service = CreateTopicsValidationLayer::new().layer(CreateTopicsService { storage });
    let name = alphanumeric_string(15);

    for (configs, expected) in [
        (vec![], ErrorCode::None),
        (
            vec![
                CreatableTopicConfig::default()
                    .name("retention.ms".into())
                    .value(Some("-2".into())),
            ],
            ErrorCode::TopicAlreadyExists,
        ),
    ] {
        let response = service
            .serve(RequestInput {
                request: CreateTopicsRequest::default()
                    .topics(Some(vec![
                        CreatableTopic::default()
                            .name(name.clone())
                            .num_partitions(1)
                            .replication_factor(1)
                            .assignments(Some([].into()))
                            .configs(Some(configs)),
                    ]))
                    .validate_only(Some(false)),
                extensions: Extensions::default(),
            })
            .await?;

        let topics = response.topics.unwrap_or_default();
        assert_eq!(1, topics.len());
        assert_eq!(expected, ErrorCode::try_from(topics[0].error_code)?);
    }

    Ok(())
}

/// Serves one `CreateTopics` request for `topics`, each with one partition, one replica and the
/// given configs, and returns the topic results.
async fn create_topics(
    storage: impl Storage + Clone,
    topics: &[(&str, Vec<CreatableTopicConfig>)],
    validate_only: bool,
) -> Result<Vec<CreatableTopicResult>, Error> {
    let service = CreateTopicsValidationLayer::new().layer(CreateTopicsService { storage });

    let response = service
        .serve(RequestInput {
            request: CreateTopicsRequest::default()
                .topics(Some(
                    topics
                        .iter()
                        .map(|(name, configs)| {
                            CreatableTopic::default()
                                .name((*name).into())
                                .num_partitions(1)
                                .replication_factor(1)
                                .assignments(Some([].into()))
                                .configs(Some(configs.clone()))
                        })
                        .collect(),
                ))
                .validate_only(Some(validate_only)),
            extensions: Extensions::default(),
        })
        .await?;

    Ok(response.topics.unwrap_or_default())
}

/// Kafka refuses a topic whose name differs from an existing topic's only by '.' and '_',
/// because its per-topic metric names map '.' to '_'. It checks the name against existing
/// topics only, before it checks the configs, and an exact match is `TOPIC_ALREADY_EXISTS`.
async fn colliding_name_rejected(storage: impl Storage + Clone) -> Result<(), Error> {
    let prefix = alphanumeric_string(15);
    let period = format!("{prefix}.x");
    let underscore = format!("{prefix}_x");

    let topics = create_topics(storage.clone(), &[(&period, vec![])], false).await?;
    assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);

    for validate_only in [false, true] {
        let topics =
            create_topics(storage.clone(), &[(&underscore, vec![])], validate_only).await?;
        assert_eq!(1, topics.len());
        assert_eq!(
            ErrorCode::InvalidTopicException,
            ErrorCode::try_from(topics[0].error_code)?,
            "validate_only = {validate_only}"
        );
        assert_eq!(
            Some(format!("Topic '{underscore}' collides with existing topic: {period}").as_str()),
            topics[0].error_message.as_deref()
        );
        assert_eq!(Some(NULL_TOPIC_ID), topics[0].topic_id);
    }

    let topics = create_topics(storage.clone(), &[(&period, vec![])], false).await?;
    assert_eq!(
        ErrorCode::TopicAlreadyExists,
        ErrorCode::try_from(topics[0].error_code)?
    );

    let hyphen = format!("{prefix}-x");
    let topics = create_topics(storage.clone(), &[(&hyphen, vec![])], false).await?;
    assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);

    let invalid_retention = vec![
        CreatableTopicConfig::default()
            .name("retention.ms".into())
            .value(Some("-2".into())),
    ];
    let topics = create_topics(storage.clone(), &[(&underscore, invalid_retention)], false).await?;
    assert_eq!(
        ErrorCode::InvalidTopicException,
        ErrorCode::try_from(topics[0].error_code)?
    );

    let prefix = alphanumeric_string(15);
    let underscore = format!("{prefix}_x");
    let period = format!("{prefix}.x");

    let topics = create_topics(storage.clone(), &[(&underscore, vec![])], false).await?;
    assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);

    let topics = create_topics(storage.clone(), &[(&period, vec![])], false).await?;
    assert_eq!(
        ErrorCode::InvalidTopicException,
        ErrorCode::try_from(topics[0].error_code)?
    );
    assert_eq!(
        Some(format!("Topic '{period}' collides with existing topic: {underscore}").as_str()),
        topics[0].error_message.as_deref()
    );

    // Kafka checks a name against the topics that exist before the request, so two names in
    // one request do not collide with each other.
    let prefix = alphanumeric_string(15);
    let period = format!("{prefix}.x");
    let underscore = format!("{prefix}_x");

    let topics = create_topics(
        storage.clone(),
        &[(&period, vec![]), (&underscore, vec![])],
        false,
    )
    .await?;
    assert_eq!(2, topics.len());
    for topic in topics {
        assert_eq!(
            ErrorCode::None,
            ErrorCode::try_from(topic.error_code)?,
            "{}",
            topic.name
        );
    }

    // Kafka checks an invalid name for a collision too, and the collision message replaces the
    // invalid-name message.
    let topics = create_topics(storage.clone(), &[("_", vec![])], false).await?;
    assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);

    let topics = create_topics(storage, &[(".", vec![])], false).await?;
    assert_eq!(
        ErrorCode::InvalidTopicException,
        ErrorCode::try_from(topics[0].error_code)?
    );
    assert_eq!(
        Some("Topic '.' collides with existing topic: _"),
        topics[0].error_message.as_deref()
    );

    Ok(())
}

#[cfg(feature = "dynostore")]
mod in_memory {
    use nisshi_broker::Result;
    use nisshi_storage::ArcDynStorage;
    use rand::{RngExt as _, rng};
    use uuid::Uuid;

    use crate::common::memory_storage;

    use super::*;

    async fn storage_container(
        cluster: impl Into<String> + Clone,
        node: i32,
    ) -> Result<ArcDynStorage> {
        memory_storage(cluster, node).await
    }

    #[tokio::test]
    async fn create() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::create(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn create_with_default() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::create_with_default(broker_id, storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn duplicate() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::duplicate(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn invalid_name_rejected() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::invalid_name_rejected(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn invalid_partitions_rejected() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::invalid_partitions_rejected(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn mixed_batch_partial_success() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::mixed_batch_partial_success(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn total_partitions_cap_rejected() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::total_partitions_cap_rejected(storage).await?;

        Ok(())
    }

    /// Extra confidence beyond `total_partitions_cap_rejected`: exactly the
    /// cap, split across two topics with valid names, actually creates both
    /// topics rather than merely reaching per-topic validation.
    ///
    /// This pins a deliberate difference from Kafka 3.9.1, which rejects
    /// this request: it also caps metadata records per request at 10,000,
    /// and 10,000 partitions plus 2 `TopicRecord`s exceeds that. nisshi has
    /// no metadata records and counts partitions only. Runs on the
    /// in-memory backend only -- materializing 10,000 real partitions is
    /// cheap here, but not worth paying four times over across every
    /// backend.
    #[tokio::test]
    async fn total_partitions_cap_creates_at_the_boundary() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;
        let service = CreateTopicsValidationLayer::new().layer(CreateTopicsService {
            storage: storage.clone(),
        });

        let replication_factor = 1;
        let assignments = Some([].into());
        let configs = Some([].into());

        let name_a = alphanumeric_string(15);
        let name_b = alphanumeric_string(15);

        let response = service
            .serve(RequestInput {
                request: CreateTopicsRequest::default()
                    .topics(Some(vec![
                        CreatableTopic::default()
                            .name(name_a.clone())
                            .num_partitions(9_999)
                            .replication_factor(replication_factor)
                            .assignments(assignments.clone())
                            .configs(configs.clone()),
                        CreatableTopic::default()
                            .name(name_b.clone())
                            .num_partitions(1)
                            .replication_factor(replication_factor)
                            .assignments(assignments)
                            .configs(configs),
                    ]))
                    .validate_only(Some(false)),
                extensions: Extensions::default(),
            })
            .await?;

        let topics = response.topics.unwrap_or_default();
        assert_eq!(2, topics.len());
        for topic in &topics {
            assert_eq!(
                ErrorCode::None,
                ErrorCode::try_from(topic.error_code)?,
                "name = {:?}: exactly the cap (10,000 total) must succeed",
                topic.name
            );
            assert_ne!(Some(NULL_TOPIC_ID), topic.topic_id);
        }

        Ok(())
    }

    #[tokio::test]
    async fn invalid_replication_factor_rejected() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::invalid_replication_factor_rejected(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn mixed_batch_partial_success_replication_factor() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::mixed_batch_partial_success_replication_factor(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn invalid_retention_config_rejected() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::invalid_retention_config_rejected(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn existing_topic_with_invalid_config_already_exists() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::existing_topic_with_invalid_config_already_exists(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn colliding_name_rejected() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::colliding_name_rejected(storage).await?;

        Ok(())
    }
}

#[cfg(feature = "libsql")]
mod lite {
    use crate::common::lite_storage;
    use nisshi_broker::Result;
    use nisshi_storage::ArcDynStorage;
    use rand::{RngExt as _, rng};
    use uuid::Uuid;

    use super::*;

    async fn storage_container(
        cluster: impl Into<String> + Clone,
        node: i32,
    ) -> Result<ArcDynStorage> {
        lite_storage(cluster, node).await
    }

    #[tokio::test]
    async fn create() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::create(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn create_with_default() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::create_with_default(broker_id, storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn duplicate() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::duplicate(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn invalid_name_rejected() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::invalid_name_rejected(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn invalid_partitions_rejected() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::invalid_partitions_rejected(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn mixed_batch_partial_success() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::mixed_batch_partial_success(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn total_partitions_cap_rejected() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::total_partitions_cap_rejected(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn invalid_replication_factor_rejected() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::invalid_replication_factor_rejected(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn mixed_batch_partial_success_replication_factor() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::mixed_batch_partial_success_replication_factor(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn invalid_retention_config_rejected() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::invalid_retention_config_rejected(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn existing_topic_with_invalid_config_already_exists() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::existing_topic_with_invalid_config_already_exists(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn colliding_name_rejected() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::colliding_name_rejected(storage).await?;

        Ok(())
    }
}

#[cfg(feature = "turso")]
mod turso {
    use crate::common::turso_storage;
    use nisshi_broker::Result;
    use nisshi_storage::ArcDynStorage;
    use rand::{RngExt as _, rng};
    use uuid::Uuid;

    use super::*;

    async fn storage_container(
        cluster: impl Into<String> + Clone,
        node: i32,
    ) -> Result<ArcDynStorage> {
        turso_storage(cluster, node).await
    }

    #[tokio::test]
    async fn create() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::create(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn create_with_default() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::create_with_default(broker_id, storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn duplicate() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::duplicate(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn invalid_name_rejected() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::invalid_name_rejected(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn invalid_partitions_rejected() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::invalid_partitions_rejected(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn mixed_batch_partial_success() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::mixed_batch_partial_success(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn total_partitions_cap_rejected() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::total_partitions_cap_rejected(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn invalid_replication_factor_rejected() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::invalid_replication_factor_rejected(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn mixed_batch_partial_success_replication_factor() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::mixed_batch_partial_success_replication_factor(storage).await?;

        Ok(())
    }
}

#[cfg(feature = "slatedb")]
mod slatedb {
    use crate::common::slate_storage;
    use nisshi_broker::Result;
    use nisshi_storage::ArcDynStorage;
    use rand::{RngExt as _, rng};
    use uuid::Uuid;

    use super::*;

    async fn storage_container(
        cluster: impl Into<String> + Clone,
        node: i32,
    ) -> Result<ArcDynStorage> {
        slate_storage(cluster, node).await
    }

    #[tokio::test]
    async fn create() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::create(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn create_with_default() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::create_with_default(broker_id, storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn duplicate() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::duplicate(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn invalid_name_rejected() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::invalid_name_rejected(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn invalid_partitions_rejected() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::invalid_partitions_rejected(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn mixed_batch_partial_success() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::mixed_batch_partial_success(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn total_partitions_cap_rejected() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::total_partitions_cap_rejected(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn invalid_replication_factor_rejected() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::invalid_replication_factor_rejected(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn mixed_batch_partial_success_replication_factor() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::mixed_batch_partial_success_replication_factor(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn invalid_retention_config_rejected() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::invalid_retention_config_rejected(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn existing_topic_with_invalid_config_already_exists() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::existing_topic_with_invalid_config_already_exists(storage).await?;

        Ok(())
    }
}

#[cfg(feature = "postgres")]
mod pg {
    use crate::common::postgres_storage;
    use nisshi_broker::Result;
    use nisshi_storage::ArcDynStorage;
    use rand::{RngExt as _, rng};
    use uuid::Uuid;

    use super::*;

    async fn storage_container(
        cluster: impl Into<String> + Clone,
        node: i32,
    ) -> Result<ArcDynStorage> {
        postgres_storage(cluster, node).await
    }

    #[tokio::test]
    async fn create() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::create(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn create_with_default() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::create_with_default(broker_id, storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn duplicate() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::duplicate(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn invalid_name_rejected() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::invalid_name_rejected(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn invalid_partitions_rejected() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::invalid_partitions_rejected(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn mixed_batch_partial_success() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::mixed_batch_partial_success(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn total_partitions_cap_rejected() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::total_partitions_cap_rejected(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn invalid_replication_factor_rejected() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::invalid_replication_factor_rejected(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn mixed_batch_partial_success_replication_factor() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::mixed_batch_partial_success_replication_factor(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn invalid_retention_config_rejected() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::invalid_retention_config_rejected(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn existing_topic_with_invalid_config_already_exists() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::existing_topic_with_invalid_config_already_exists(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn colliding_name_rejected() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::colliding_name_rejected(storage).await?;

        Ok(())
    }
}
