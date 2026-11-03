# Compatibility and feature status

This page describes the `main` branch. To read it for a release, open `docs/compatibility.md` at
that release's tag.

Nisshi implements the Apache Kafka® wire protocol. It answers the Kafka APIs that clients and
admin tools use, and it does not implement the APIs that Kafka brokers and KRaft controllers use
between themselves. Every Nisshi broker reports itself as the only broker in the cluster, with
node ID 111, and the storage engine holds all the state.

The tables compare the PostgreSQL, SQLite, in-memory and S3 storage engines. The in-memory and S3
engines share one implementation, so their columns are the same. The tables do not cover Turso,
which is experimental, or SlateDB, which is deprecated.

The [storage engine table](storage.md) gives each engine's status, and
[What CI tests on each storage engine](../CONTRIBUTING.md#what-ci-tests-on-each-storage-engine)
gives the checks that CI runs on each engine.

- [Kafka APIs](#kafka-apis)
- [Topic configuration](#topic-configuration)
- [Kafka clients tested](#kafka-clients-tested)

In the tables:

- **Yes**: the engine supports it as Apache Kafka does.
- **Partial**: the engine supports it, with the differences in the Notes column.
- **No**: the engine does not support it.

## Kafka APIs

The Versions column gives the versions that the broker advertises in its ApiVersions response. A
dash means that the broker does not advertise the API. If a client sends a request for an API
that the broker does not advertise, the broker closes the connection and does not answer. A
client that reads the ApiVersions response first does not send such a request.

### Produce and fetch

| API | Key | Versions | PostgreSQL | SQLite | In-memory | S3 | Notes |
| --- | --: | --- | --- | --- | --- | --- | --- |
| Produce | 0 | 3-11 | Yes | Yes | Yes | Yes | Versions 0-2 use the message set format, which Nisshi does not decode. Some topic configs do not apply, see [Topic configuration](#topic-configuration). The broker rejects a `CreateTime` record more than an hour ahead of its clock with INVALID_TIMESTAMP, as Kafka 4.0 does by default. Kafka 3.9.1 accepts any future timestamp, and Nisshi has no setting to change the limit. |
| Fetch | 1 | 0-17 | Yes | Yes | Yes | Yes | Every fetch is a full fetch, because the broker does not keep incremental fetch sessions (KIP-227). |
| ListOffsets | 2 | 0-6 | Yes | Yes | Yes | Yes | Nisshi does not support the `max-timestamp` lookup, because a client requests it with version 7. |
| DeleteRecords | 21 | 0-2 | Yes | Yes | Yes | Yes | |
| OffsetForLeaderEpoch | 23 | – | No | No | No | No | |

### Topics and configuration

| API | Key | Versions | PostgreSQL | SQLite | In-memory | S3 | Notes |
| --- | --: | --- | --- | --- | --- | --- | --- |
| Metadata | 3 | 0-12 | Yes | Yes | Yes | Yes | The broker creates a missing topic, with `--num-partitions` partitions (default 1), if the client allows automatic topic creation and `--auto-create-topics-enable` is `true` (the default). If the missing topics of one request would need more than 10,000 partitions in total, the broker creates none of them, as Kafka does. Metadata does not list `__consumer_offsets`, because the broker keeps committed offsets in its storage engine, not in a topic. |
| CreateTopics | 19 | 0-7 | Yes | Yes | Yes | Yes | A topic with `num_partitions` of -1 gets `--num-partitions` partitions (default 1). |
| DeleteTopics | 20 | 0-6 | Yes | Yes | Yes | Yes | |
| DescribeConfigs | 32 | 0-4 | Partial | Partial | Partial | Partial | Lists every Kafka 3.9.1 topic config: each config set on the topic, and Kafka's default for each other config, as [Topic configuration](#topic-configuration) describes. The list does not include synonyms. Describing a broker, for example with `kafka-configs --describe --entity-type brokers`, returns an empty list. |
| AlterConfigs | 33 | – | No | No | No | No | Use IncrementalAlterConfigs. |
| CreatePartitions | 37 | – | No | No | No | No | You cannot add partitions to an existing topic. |
| IncrementalAlterConfigs | 44 | 0-1 | Partial | Partial | Partial | Partial | Changes topic configs, including APPEND and SUBTRACT on list configs. The broker accepts a change to a broker config, for example with `kafka-configs --alter --entity-type brokers`, and ignores it. |
| ListPartitionReassignments | 46 | 0 | Yes | Yes | Yes | Yes | Always answers an empty list of reassignments. |
| DescribeCluster | 60 | 0-1 | Yes | Yes | Yes | Yes | |
| DescribeTopicPartitions | 75 | 0 | Partial | Partial | Partial | Partial | Does not return a cursor for the next page. |

DescribeLogDirs (35), AlterReplicaLogDirs (34), ElectLeaders (43), AlterPartitionReassignments
(45) and UpdateFeatures (57) manage replicas, log directories and features on Kafka brokers.
Nisshi does not implement them, because the storage engine holds all the state.

### Consumer groups

[Consumer groups and offsets](consumer-groups.md) explains how groups and committed offsets work,
with an example and the limits of this build.

| API | Key | Versions | PostgreSQL | SQLite | In-memory | S3 | Notes |
| --- | --: | --- | --- | --- | --- | --- | --- |
| OffsetCommit | 8 | 0-9 | Yes | Yes | Yes | Yes | Answers OFFSET_METADATA_TOO_LARGE for a partition whose metadata is longer than 4096 characters, the default of Kafka's `offset.metadata.max.bytes`. Nisshi does not make the limit configurable. |
| OffsetFetch | 9 | 0-9 | Yes | Yes | Yes | Yes | |
| FindCoordinator | 10 | 0-6 | Yes | Yes | Yes | Yes | Answers with the broker that receives the request. |
| JoinGroup | 11 | 0-9 | Partial | Partial | Partial | Partial | A consumer group cannot change to the `cooperative-sticky` assignor with a rolling restart ([#1051]). |
| Heartbeat | 12 | 0-4 | Yes | Yes | Yes | Yes | |
| LeaveGroup | 13 | 0-5 | Yes | Yes | Yes | Yes | |
| SyncGroup | 14 | 0-5 | Yes | Yes | Yes | Yes | |
| DescribeGroups | 15 | 0-5 | Yes | Yes | Yes | Yes | |
| ListGroups | 16 | 0-5 | Yes | Yes | Yes | Yes | |
| DeleteGroups | 42 | 0-2 | Yes | Yes | Yes | Yes | |
| OffsetDelete | 47 | – | No | No | No | No | [#964] |
| ConsumerGroupHeartbeat | 68 | – | No | No | No | No | Nisshi does not support the KIP-848 consumer group protocol. Consumers use the classic protocol. |
| ConsumerGroupDescribe | 69 | 0 | Yes | Yes | Yes | Yes | Answers GROUP_ID_NOT_FOUND for each group, as Kafka does for a classic group, because every Nisshi group uses the classic protocol. Clients then describe the group with DescribeGroups. |

### Transactions

Nisshi supports the idempotent producer, and does not support transactions. The broker answers
TRANSACTIONAL_ID_AUTHORIZATION_FAILED to an InitProducerId request with a `transactional.id`, so
a transactional producer fails at `initTransactions()` with
TransactionalIdAuthorizationException. A Kafka Streams application with `processing.guarantee`
set to `exactly_once_v2` uses a transactional producer, so it fails the same way. The broker still
advertises the other transaction APIs, but a client cannot start a transaction to use them.

An idempotent producer can bump its own epoch after an error and restart its sequence at 0
(KIP-360), and the broker keeps the epoch for each partition, as Kafka does. A batch at an epoch
older than its partition's gets INVALID_PRODUCER_EPOCH, as in Kafka. Every engine refuses such a
batch also when the broker received it before the bump and the newer epoch's batch reached the
partition first. The broker refuses a bump to epoch 32766 or above with INVALID_PRODUCER_EPOCH,
and Kafka accepts it. The Java producer then moves to a new producer ID and continues.

The in-memory and S3 engines answer a retry of one of an idempotent producer's last 5 batches to
a partition as Kafka does: with no error and the offset of the stored batch. They answer any
other batch that does not start at the producer's next sequence with
OUT_OF_ORDER_SEQUENCE_NUMBER. The PostgreSQL and SQLite engines answer every batch whose sequence
is below the next one with DUPLICATE_SEQUENCE_NUMBER and base offset -1, and do not store it. A
client counts that answer as delivered.

| API | Key | Versions | PostgreSQL | SQLite | In-memory | S3 | Notes |
| --- | --: | --- | --- | --- | --- | --- | --- |
| InitProducerId | 22 | 0-5 | Partial | Partial | Partial | Partial | Gives a producer ID to an idempotent producer. Refuses a request with a `transactional.id`. |
| AddPartitionsToTxn | 24 | 0-3 | No | No | No | No | |
| AddOffsetsToTxn | 25 | 0-4 | No | No | No | No | |
| EndTxn | 26 | 0-4 | No | No | No | No | |
| TxnOffsetCommit | 28 | 0-4 | No | No | No | No | |
| DescribeProducers | 61 | – | No | No | No | No | |
| DescribeTransactions | 65 | – | No | No | No | No | |
| ListTransactions | 66 | – | No | No | No | No | |

### Security

| API | Key | Versions | PostgreSQL | SQLite | In-memory | S3 | Notes |
| --- | --: | --- | --- | --- | --- | --- | --- |
| SaslHandshake | 17 | 0-1 | Yes | Yes | No | No | Nisshi supports the SCRAM-SHA-256 and SCRAM-SHA-512 mechanisms. The broker starts with `--authentication` only on an engine that stores users. |
| ApiVersions | 18 | 0-4 | Yes | Yes | Yes | Yes | |
| DescribeAcls | 29 | 0-3 | No | No | No | No | Answers SECURITY_DISABLED, because Nisshi does not have an authorizer. |
| CreateAcls | 30 | 0-3 | No | No | No | No | Answers SECURITY_DISABLED. |
| DeleteAcls | 31 | 0-3 | No | No | No | No | Answers SECURITY_DISABLED. |
| SaslAuthenticate | 36 | 0-2 | Yes | Yes | No | No | |
| CreateDelegationToken, RenewDelegationToken, ExpireDelegationToken, DescribeDelegationToken | 38-41 | – | No | No | No | No | |
| DescribeClientQuotas | 48 | – | No | No | No | No | `kafka-configs` cannot describe a SCRAM user, because it also asks for the user's quotas ([#922]). |
| AlterClientQuotas | 49 | – | No | No | No | No | |
| DescribeUserScramCredentials | 50 | 0 | No | No | No | No | Answers an empty list of users. |
| AlterUserScramCredentials | 51 | 0 | Yes | Yes | No | No | The in-memory and S3 engines answer UNSUPPORTED_VERSION for each user. |

### Other APIs

| API | Key | Versions | PostgreSQL | SQLite | In-memory | S3 | Notes |
| --- | --: | --- | --- | --- | --- | --- | --- |
| GetTelemetrySubscriptions, PushTelemetry, ListClientMetricsResources | 71, 72, 74 | – | No | No | No | No | Nisshi does not support client telemetry (KIP-714). |
| ShareGroupHeartbeat, ShareGroupDescribe, ShareFetch, ShareAcknowledge and the share group state APIs | 76-79, 83-87 | – | No | No | No | No | Nisshi does not support share groups (KIP-932). |

Nisshi also does not implement the APIs between Kafka brokers and KRaft controllers, such as
LeaderAndIsr, Vote and BrokerRegistration.

## Topic configuration

The broker stores every topic config that a client sets, and returns it with DescribeConfigs.
For each Kafka topic config that a topic does not set, DescribeConfigs returns Kafka 3.9.1's
default, such as `retention.ms=604800000`, as `kafka-configs --describe --all` shows.
The broker applies only the configs that this table marks Yes. A default that DescribeConfigs
lists for a config marked No is Kafka's value, not what the broker does: an in-memory topic lists
`retention.ms=604800000`, and keeps every record. The broker applies
`cleanup.policy`, `retention.ms` and `retention.bytes` in a maintenance run, every 10 minutes by
default. The `maintenance_interval` option on the storage engine URL changes the interval.

| Config | PostgreSQL | SQLite | In-memory | S3 | Notes |
| --- | --- | --- | --- | --- | --- |
| `cleanup.policy` | Yes | Yes | No | No | Applies `delete` and `compact`. The broker accepts a record without a key on a compacted topic, and Kafka rejects it with INVALID_RECORD. |
| `retention.ms` | Yes | Yes | No | No | |
| `retention.bytes` | Yes | Yes | No | No | |
| `max.message.bytes` | Yes | Yes | Yes | Yes | |
| `message.timestamp.type` | Yes | Yes | Yes | Yes | |
| Every other Kafka topic config | No | No | No | No | |

## Kafka clients tested

CI runs these suites against the broker.

| Suite | What runs | Version | Tests that pass |
| --- | --- | --- | --- |
| [librdkafka](../compat/librdkafka) | The librdkafka integration test suite | v2.14.2 and v2.3.0 | [tests.allow](../compat/librdkafka/tests.allow) for v2.14.2, [tests-v2.3.0.allow](../compat/librdkafka/tests-v2.3.0.allow) for v2.3.0 |
| [franz-go](../compat/franz-go) | The franz-go `kgo` integration test suite | v1.22.2 | [tests.allow](../compat/franz-go/tests.allow) |
| [Kafka CLI smoke tests](../nisshi-smoke-test) | The Apache Kafka command line tools | Kafka 3.7.2, 3.9.2 and 4.3.1 | Every test that is not marked `#[ignore]` |

The `tests.allow` lists of the librdkafka and franz-go suites leave out the tests of these
consumer group behaviours, because the tests fail:

- Consumer groups that rebalance at the same time. Members get UNKNOWN_MEMBER_ID.
- A consumer that exceeds `max.poll.interval.ms`. The group rebalances one more time than in
  Kafka.
- An offset commit during a rebalance. The broker answers different error codes than Kafka.

The `FINDINGS.md` file in each client suite's folder gives the test numbers and the cause of
each failure. The findings come from runs on the in-memory engine.

[#922]: https://github.com/nisshi-io/solace-nisshi/issues/922
[#964]: https://github.com/nisshi-io/solace-nisshi/issues/964
[#1051]: https://github.com/nisshi-io/solace-nisshi/issues/1051
