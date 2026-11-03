# librdkafka v2.14.2 sweep findings

Sweeps of the librdkafka integration test suite against
`nisshi broker --storage-engine=memory://`. Passing tests are in
`tests.allow`.

## Fixed

1. **No automatic topic creation on MetadataRequest** *(fixed 2026-06-11)* —
   implemented in `MetadataService`: unknown topics named in a
   MetadataRequest with `allow_auto_topic_creation` are created with
   `--num-partitions` partitions and replication factor 1. The broker
   default is 1, as in Kafka, but `just compat-librdkafka` passes
   `--num-partitions=4`, because tests such as 0012 produce to partition 1
   of a topic they leave to auto-creation. Invalid topic names are reported as
   `INVALID_TOPIC_EXCEPTION` and not created. This unlocked ~100 tests
   that previously could not run.

2. **IncrementalAlterConfigs APPEND/SUBTRACT panicked the broker**
   *(fixed for dynostore 2026-06-11, for every backend 2026-10-07)* — `todo!()` in dynostore `alter_topic` panicked
   while holding the topics lock, poisoning it: every subsequent request
   failed with `Poison` until restart (hit by 0011). Every backend now
   applies Kafka's list semantics, all or nothing per resource, and
   `IncrementalAlterConfigsService` rejects APPEND/SUBTRACT on a key that
   isn't a LIST with `INVALID_CONFIG`.

3. **0086_purge_remote raced GetTelemetrySubscriptions** *(fixed
   2026-10-04, re-enabled in `tests.allow`)* — nisshi used to always
   advertise the KIP-714 client telemetry APIs and answer
   `GetTelemetrySubscriptions` with an empty subscription. `0086_purge_remote`
   sets `max.in.flight=1`, produces 29 messages, adds a 50 s sockem delay to
   the socket as soon as the first ProduceResponse arrives, then asserts
   that a second ProduceRequest is written within 15 s. librdkafka also
   sends `GetTelemetrySubscriptions` to every learned broker that
   advertises it, and whether that request was queued before or after the
   first Produce was a thread race: when it landed after, its response sat
   behind the 50 s delay and, because `max.in.flight` counts every request
   type, the second Produce was never sent (reproduced locally against
   `memory://` at 5/25 under CPU load, ~3% in CI). nisshi removed
   its `GetTelemetrySubscriptions` route and its registration
   entirely (the service had no real telemetry sink behind it, and
   `PushTelemetry` was never routed either), so nisshi no longer
   advertises the telemetry APIs at all and this race can no longer
   happen. Kafka itself only advertises the telemetry APIs when a
   client-telemetry plugin is configured (`ApiVersionsResponse.filterApis`),
   which is why upstream never sees the race. librdkafka's `0150_telemetry_mock` runs only against a mock
   cluster and is unaffected either way.

## Open gaps, most impactful first

1. **Produce does not reject keyless records on compacted topics** —
   Kafka fails them per-record with `INVALID_RECORD`; nisshi accepts them
   (0011 `test_message_single_partition_record_fail`). Related to the
   deliberate retention of keyless records in batch compaction
   (commit 6db7a49).

2. **ListOffsets by timestamp returns wrong/no offsets** — offsets-for-
   times queries return incorrect offsets or time out (0031, 0034, 0054,
   0059, and 0015/0030 which seek by time).

3. **CreatePartitions returns an empty topics array** —
   `CreatePartitions_result_topics returned NULL` (0044, 0069, 0112).

4. **OffsetCommit `metadata` is not round-tripped** — committed metadata
   comes back empty from OffsetFetch (0099, 0140).

5. **AlterConfigs is not implemented** — 0092 reads a topic's configs
   with DescribeConfigs and writes them back with AlterConfigs. DescribeConfigs
   now lists every topic config with its default, as Kafka does, but the
   broker does not implement AlterConfigs.

6. **Transactions are refused** — nisshi answers InitProducerId with a
   `transactional.id` with `TRANSACTIONAL_ID_AUTHORIZATION_FAILED`, so
   the transactional tests (0103, 0129) fail at `init_transactions`.
   0061 and 0137 have transactional subtests that fail in a full run,
   and 0055 fails outside CI. CI's `-Q` quick mode skips the first two.
   Turning transactions back on needs these fixes first:
   - Every storage engine returns an empty `aborted_transactions`, so a
     `read_committed` consumer reads aborted records.
   - InitProducerId accepts a `transaction.timeout.ms` above
     `transaction.max.timeout.ms`; Kafka answers
     `INVALID_TRANSACTION_TIMEOUT` (0103 `do_test_misuse_txn`).
   - 0129's `abort_transaction` fails with `EndTxnRequest (KIP-98) not
     supported`, so the advertised EndTxn versions may not cover what
     librdkafka needs.
   - The `old_versions_bump_a_transactional_producer` test is ignored on
     each engine in `nisshi-broker/tests/it/init_producer_id.rs` (in
     memory, lite, slatedb and pg), with a reason that points here; CI
     never runs it, so remove those `#[ignore]`s when the refusal is
     lifted. The Turso engine's ignore has its own reason.

7. **Consumer group edge cases** — one extra rebalance versus Kafka in
   max-poll-exceeded scenarios (0091, 0093); commit-during-rebalance
   error codes differ (0118); message expected after `offsets_store`
   of an unassigned partition (0130).

8. **CreateTopics corner cases** — 0081 (admin) fails waiting for a
   metadata update after creating topics with mixed valid/invalid
   configs.

## Not nisshi issues (environmental skips/failures)

- 0052, 0077, 0109, 0115, 0119: need `KAFKA_PATH`/`ZK_ADDRESS` (drive
  Kafka's own CLI tools).
- 0064: needs an SSL-enabled librdkafka build.
- 0028/0075/0088: sockem tests, filtered by `-E`.
- 0101: needs RapidJSON. 0107: interactive. 0142: needs SASL listener.
