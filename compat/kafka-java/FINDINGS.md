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

`tests.allow` lists 110 passing methods. `PlaintextConsumerTest.testSimpleConsumption` is left out: it
failed once in four runs with `CommitFailedException` (a rebalance during commit), which needs a look
before it counts as reliable. Recommended for CI first: the six classes in the first
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
- `testConsumeMessagesWithLogAppendTime`: fixed by syncing the colima VM clock to the host (it was
  2 s ahead). Both `PlaintextProducerSendTest.test*LogAppendTime` tests then pass.
- `LogAppendTimeTest.testProduceConsume`: the test sets `log.message.timestamp.type` on the Kafka
  broker config, which an external broker never sees, so the topic stays `CreateTime`. Nisshi has no
  such broker setting, so the class stays out of the allow-list.

## Candidate gaps, checked against Kafka 3.9.1

Each row says what the Java broker does, with the code read at the `3.9.1` tag, and whether the
difference matches, is stricter or looser than Kafka, or does not apply by design.

| Test | Kafka 3.9.1 | Nisshi | Verdict |
| --- | --- | --- | --- |
| `PlaintextConsumerFetchTest.testFetchHonoursFetchSizeIfLargeRecordNotFirst`, `...MaxPartitionFetchBytes...` | A read cuts a batch after the first at the fetch limit, and the client drops the partial batch ([`ReplicaManager.readFromLog`](https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/ReplicaManager.scala#L1733)). The consumer gets only the small record. | `send_batches_from` in `nisshi-storage-dynostore/src/dynostore.rs` sends the batch that reaches `max_bytes` whole, so the consumer gets 2 records. | Looser, and the code comment says it is deliberate. `docs/compatibility.md` does not mention it. A fetch can return one batch more than `fetch.max.bytes` and `max.partition.fetch.bytes`. Decide whether to keep it, and document it either way. |
| `ProducerFailureHandlingTest.testCannotSendToInternalTopic` | A produce to `__consumer_offsets` returns `INVALID_TOPIC_EXCEPTION` from `ReplicaManager.appendToLocalLog`, but only after the topic exists. The Kafka harness creates it first. | The topic does not exist and the broker does not create it, so the producer times out on metadata. | Not applicable by design. |
| `PlaintextProducerSendTest.testSendWithInvalidBeforeAndAfterTimestamp` (2 variants) | `LogValidator` rejects a `CreateTime` outside `message.timestamp.before.max.ms` and `after.max.ms`. | Rejects only a `CreateTime` more than 1 hour ahead, with no setting. | Looser, documented in `docs/compatibility.md`. Implementing the two topic configs would pass the test. |
| `PlaintextProducerSendTest.test*LogAppendTime`, `testConsumeMessagesWithLogAppendTime` | `LogAppendTime` works. | `LogAppendTime` works with Kafka's own `kafka-console-producer` and `kafka-console-consumer`. | Harness artifact. The callback asserts `metadata.timestamp >= startTime`, where `startTime` comes from the test JVM's clock in the colima VM. The broker stamps with the host clock, a few milliseconds earlier. The assertion fails in the producer's sender thread, so the future never completes and the test hangs until the 90 s limit. Confirmed on rerun: after syncing the VM clock to the host, both tests pass. Keep the VM clock synced (`colima ssh -- sudo date -u -s @$(date +%s)`) before a run. |

### Found while checking

- **A replication factor above 1 is accepted.** `kafka-topics --create --replication-factor 2`
  succeeds on Nisshi. Kafka's `ReplicationControlManager` answers `INVALID_REPLICATION_FACTOR`
  when the factor is larger than the number of brokers. `nisshi-storage/src/assignment.rs` repeats
  node 111 once per replica on purpose. This is looser than Kafka, and `docs/compatibility.md` does
  not mention it. Decide whether to keep it and document it.

## Not triaged

Quota classes (`*QuotaTest`, 5 failures each) and `DynamicBrokerReconfigurationTest` look like
embedded-broker limits, but I did not read their failures one by one.
