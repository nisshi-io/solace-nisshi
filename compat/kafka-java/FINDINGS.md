# Findings: Kafka 3.9.1 integration tests against Nisshi

Run of every class under `core/src/test/scala/integration` (95 classes) against `memory://`, with
`harness.patch`. Of 1,272 tests, 676 were skipped as not applicable (ZooKeeper and KIP-848
variants), 188 passed and 408 failed. A second sweep, after the harness fixes below, replaced
the first (182 passed, 414 failed).

## What is worth running

| Group | Result |
| --- | --- |
| `PlaintextConsumer{Assign,Assignors,Callback,Commit,Poll,Subscription}Test` | 51 tests, all pass |
| `PlaintextConsumerTest`, `PlaintextConsumerFetchTest`, `PlaintextProducerSendTest` | 56 of 95 pass; the failures are in the tables below |

`tests.allow` lists 108 passing methods. The three `LogAppendTime` tests that compare a broker
timestamp with the test JVM's clock are left out, see "Harness artifacts". Recommended for CI first: the six classes in the first
row, then the three in the second row with a skip list. A class takes 10 to 70 s.

Classes that pass without using Nisshi, because they start their own cluster: `KRaftClusterTest`,
`ZkMigration*`, `DelayedFetchTest`, `MirrorMakerIntegrationTest`, `RaftClusterSnapshotTest`,
`MetadataVersionIntegrationTest`. Keep them out of any allow-list.

## Failures that need no Nisshi change

| Cause | Tests | Note |
| --- | --: | --- |
| The test needs embedded brokers (`brokers.head`, `killBroker`, `zkClient`, JMX) | about 240 | Not applicable to an external broker. |
| Transactions are not supported | 25 | `TransactionsTest` and its variants fail at `initTransactions()`. Documented in `docs/compatibility.md`. They are a ready acceptance suite if Nisshi adds transactions. |
| Delegation tokens, Kerberos, OAuth, tiered storage, ACL authorizer, TLS listener | about 110 | Not supported, or covered by `nisshi-smoke-test`. Several classes hit the 600 s cap. |
| `CREATE_PARTITIONS`, `ListOffsets` v7 (max timestamp) | 9 | Documented gaps. |
| `testListTopics` expects `__consumer_offsets` | 1 | Nisshi has no internal topics, by design. |
| `testCoordinatorFailover`, `*Rebootstrap*` | 4 | Need a broker restart. |

## Harness artifacts

Fixed in `harness.patch` or `run.sh`: `recreateBrokers` is a no-op, `createTopicWithAssignment` and
`createTopicExternal` use at least one partition, and `run.sh` starts the broker with
`--auto-create-topics-enable=false` for `ProducerFailureHandlingTest`. Still open:

- `ProducerCompressionTest` (5): the test builds its own embedded broker (`Seq(broker)`), so it
  cannot run against an external one. Not fixable in the harness.
- `testTooLargeRecordWithAckOne` (1): the Kafka test broker sets a small `message.max.bytes`, and
  Nisshi has no such broker setting.
- `testConsumeMessagesWithLogAppendTime` and `PlaintextProducerSendTest.test*LogAppendTime` (3): the
  test compares the broker's timestamp with the test JVM's clock. Syncing the colima VM clock to the
  host fixes a 2 s skew, but the VM clock still differs from the host's by tens of milliseconds, and
  the consumer test allows a 24 ms window. They passed in one run and failed in the next (the
  broker timestamp was 48 ms before the test's start time), so they are not on the allow-list. On a
  Linux runner the container shares the host's clock, so they are worth trying there.
- `LogAppendTimeTest.testProduceConsume`: the test sets `log.message.timestamp.type` on the Kafka
  broker config, which an external broker never sees, so the topic stays `CreateTime`. Nisshi has no
  such broker setting, so the class stays out of the allow-list.

## Tests that leak state between tests

Kafka's own tests start a fresh broker for each test, so they leave state behind without noticing.
Nisshi outlives the test, and two leaks made tests fail at random:

- **A deleted topic comes back.** A client from the previous test can send a metadata request for the
  topic after the teardown deleted it, and Nisshi auto-creates it with one partition. The next test
  then asks for two partitions, finds the topic, and waits 30 s for leaders that never come. On one
  long-lived broker, `testClusterResourceListener` failed in 3 of 4 runs before the fix and in none
  of 4 after it. The harness now also deletes all topics at the start of each test.
- **A consumer group keeps its members and offsets.** The harness closed consumers with a zero
  timeout, which may not send `LeaveGroup`, and used the one group id `group` in every test. A test
  that then commits with no generation gets `UNKNOWN_MEMBER_ID`, which the Java client reports as
  `CommitFailedException`. Nisshi matches Kafka 3.9.1 here (`validateOffsetCommit`). The harness now
  gives each test its own default group id and closes consumers with a 5 s timeout, so each leaves
  its group. A test that sets its own group id, such as `testRebalanceAndRejoin`, depends on the
  close timeout.

`PlaintextConsumerTest.testSimpleConsumption` failed once with `CommitFailedException` before these
changes, and I did not reproduce it before or after. It passed in all 8 runs with the changes, so it
is back on the allow-list, but the cause of the original failure is a theory.

## Candidate gaps, checked against Kafka 3.9.1

Each row says what the Java broker does, with the code read at the `3.9.1` tag, and whether the
difference matches, is stricter or looser than Kafka, or does not apply by design.

| Test | Kafka 3.9.1 | Nisshi | Verdict |
| --- | --- | --- | --- |
| `PlaintextConsumerFetchTest.testFetchHonoursFetchSizeIfLargeRecordNotFirst`, `...MaxPartitionFetchBytes...` | A read cuts a batch after the first at the fetch limit, and the client drops the partial batch ([`ReplicaManager.readFromLog`](https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/ReplicaManager.scala#L1733)). The consumer gets only the small record. | `send_batches_from` in `nisshi-storage-dynostore/src/dynostore.rs` sends the batch that reaches `max_bytes` whole, so the consumer gets 2 records. | Looser, and the code comment says it is deliberate. `docs/compatibility.md` does not mention it. A fetch can return one batch more than `fetch.max.bytes` and `max.partition.fetch.bytes`. Decide whether to keep it, and document it either way. |
| `ProducerFailureHandlingTest.testCannotSendToInternalTopic` | A produce to `__consumer_offsets` returns `INVALID_TOPIC_EXCEPTION` from `ReplicaManager.appendToLocalLog`, but only after the topic exists. The Kafka harness creates it first. | The topic does not exist and the broker does not create it, so the producer times out on metadata. | Not applicable by design. |
| `PlaintextProducerSendTest.testSendWithInvalidBeforeAndAfterTimestamp` (2 variants) | `LogValidator` rejects a `CreateTime` outside `message.timestamp.before.max.ms` and `after.max.ms`. | Rejects only a `CreateTime` more than 1 hour ahead, with no setting. | Looser, documented in `docs/compatibility.md`. Implementing the two topic configs would pass the test. |
| `PlaintextProducerSendTest.test*LogAppendTime`, `testConsumeMessagesWithLogAppendTime` | `LogAppendTime` works. | `LogAppendTime` works with Kafka's own `kafka-console-producer` and `kafka-console-consumer`. | Harness artifact. The callback asserts `metadata.timestamp >= startTime`, where `startTime` comes from the test JVM's clock in the colima VM. The broker stamps with the host clock, a few milliseconds earlier. The assertion fails in the producer's sender thread, so the future never completes and the test hangs until the 90 s limit. A VM clock that is 2 s ahead makes them hang. After `run.sh` syncs the clock, they pass or fail depending on a skew of tens of milliseconds, so they are off the allow-list under colima. |

### Found while checking

- **A replication factor above 1 is accepted.** `kafka-topics --create --replication-factor 2`
  succeeds on Nisshi. Kafka's `ReplicationControlManager` answers `INVALID_REPLICATION_FACTOR`
  when the factor is larger than the number of brokers. `nisshi-storage/src/assignment.rs` repeats
  node 111 once per replica on purpose. This is looser than Kafka, and `docs/compatibility.md` does
  not mention it. Decide whether to keep it and document it.

## Triage of the suspect failures

41 failures were not in an obvious "needs an embedded broker" class: the timeouts, the assertion
differences and the "other" bucket. I read each failure's message, and the test code where the
message was not enough. None is a new Nisshi bug. Each is a documented gap, a deliberate
difference, or a limit of running Kafka's tests against an external broker. I did not rerun each
test by hand.

| Count | Tests | Cause | Verdict |
| --: | --- | --- | --- |
| 10 | `UserQuotaTest`, `UserClientIdQuotaTest` (5 each) | `setUp` creates a topic with a GSSAPI admin client, which times out against the plaintext broker. The tests then need `AlterClientQuotas`. | Not applicable (Kerberos). Quotas themselves are not supported, see `docs/compatibility.md`, so these cannot pass without both. |
| 6 | `CustomQuotaCallbackTest`, `DescribeAuthorizedOperationsTest` (3), `MetricsTest` (2) | The client uses SASL and the broker does not enable it ("Timed out waiting for a node assignment", `IllegalSaslStateException`). | Not applicable here. Authentication is covered by `nisshi-smoke-test`. |
| 9 | `ConsumerBounceTest` (5), `PlaintextConsumerTest.testCoordinatorFailover`, `FetchFromFollowerIntegrationTest`, `FetchRequestTestDowngrade`, `MetadataRequestBetweenDifferentIbpTest` | The test restarts or kills a broker, or needs a second broker, a controller or an old inter-broker protocol. `EndToEndClusterIdTest` (not in this count) reads a broker-side metrics reporter. | Not applicable to an external single node. |
| 4 | `BrokerApiVersionsCommandTest`, `PlaintextConsumerTest.testListTopics`, `ProducerFailureHandlingTest.testCannotSendToInternalTopic`, `GroupCoordinatorIntegrationTest` | The test expects broker id 0, or the `__consumer_offsets` topic. | By design: node id 111 and no internal topics. |
| 10 | `AdminClientWithPoliciesIntegrationTest` (`ALTER_CONFIGS`), `PlaintextConsumerTest` and `PlaintextProducerSendTest` (`CREATE_PARTITIONS`, 2), `ProducerSendWhileDeletionTest` (`ALTER_PARTITION_REASSIGNMENTS`), `ConsumerBounceTest.testConsumerReceivesFatalExceptionWhenGroupPassesMaxSize` (`group.max.size`), `PlaintextProducerSendTest.testSendWithInvalidBeforeAndAfterTimestamp` (2), `ProducerIdExpirationTest` (transactions), `PlaintextConsumerFetchTest` fetch size (2) | An API or setting that Nisshi does not have, or the fetch difference described above. | Documented gaps in `docs/compatibility.md` and `docs/consumer-groups.md`, except the fetch difference, now documented there too. |
| 2 | `LogAppendTimeTest.testProduceConsume`, `ProducerFailureHandlingTest.testTooLargeRecordWithAckOne` | The test sets a broker-wide default (`log.message.timestamp.type`, `message.max.bytes`) that Nisshi cannot take. Nisshi has the per-topic `message.timestamp.type` and `max.message.bytes`, but not the broker-wide defaults. | Harness limit. A broker-wide default setting would make both tests runnable. |

The other 366 failures were assigned by keyword and not read one by one: embedded-broker use
(238, including `DynamicBrokerReconfigurationTest`), authentication and SSL, transactions, and
tiered storage (`RemoteTopicCrudTest`, 22). The embedded-broker count is probably an
undercount, because a failure goes in the first bucket that matches.

## Next

- The `Transactions*` classes and `ListOffsetsIntegrationTest` are ready acceptance suites for
  transactions and `ListOffsets` v7.
- A broker-wide `message.max.bytes` and `log.message.timestamp.type` would make two more tests
  runnable.
- Run the allow-list on Postgres and SQLite.
