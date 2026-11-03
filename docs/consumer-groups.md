# Consumer groups and offsets

This page describes the `main` branch. To read it for a release, open `docs/consumer-groups.md` at
that release's tag.

A consumer group shares the partitions of its topics among its members and keeps the offset that
each partition has been read up to. Nisshi answers the group APIs of the Apache Kafka® protocol,
so Kafka clients and tools use consumer groups with Nisshi as they do with Kafka.

- [How consumer groups work](#how-consumer-groups-work)
- [Committed offsets](#committed-offsets)
- [Example](#example)
- [Managing groups](#managing-groups)
- [Timeouts and broker settings](#timeouts-and-broker-settings)
- [Limits](#limits)

The [Consumer groups](compatibility.md#consumer-groups) table on the compatibility page gives the
status and versions of each group API.

## How consumer groups work

Consumers use the classic group protocol, as they do with Kafka: each member joins the group with
JoinGroup, gets its partitions with SyncGroup, and sends Heartbeat to stay in the group. Nisshi
does not support the consumer group protocol of KIP-848 (`group.protocol=consumer`).

The group's leader, one of its members, assigns the partitions, as in Kafka. The assignor that the
consumers configure, for example with `partition.assignment.strategy`, decides which member reads
which partition.

The storage engine holds each group's state and committed offsets, not the memory of a broker.
FindCoordinator answers with the broker that the client asked, so every broker that shares the
storage engine can coordinate every group, and the members of one group can connect to different
brokers.

## Committed offsets

A consumer commits the offset of each partition with OffsetCommit, and reads it back with
OffsetFetch when it starts or when it gets a partition in a rebalance. A Kafka consumer commits by
itself when `enable.auto.commit` is `true` (the default), and the application commits when it is
`false`.

A committed offset is the offset of the next record to read, not of the last record read. A group
that has read records 0 to 4 of a partition commits offset 5.

When a partition has no committed offset, the consumer starts where `auto.offset.reset` says:
`earliest` at the first record, `latest` (the default) after the last record. Both work as in
Kafka.

A commit for a topic or partition that does not exist gets UNKNOWN_TOPIC_OR_PARTITION, as in Kafka.
Committed offsets stay after a broker restart on PostgreSQL, SQLite and S3. The in-memory engine
(`memory://`) loses them, with everything else, when the broker stops.

## Example

This example uses the Apache Kafka command line tools against the broker of the
[quickstart](quickstart.md), on `localhost:9092`. The Kafka CLI smoke tests run each of these
operations in CI. In the Apache Kafka download, the tools are in `bin/` and their names end
in `.sh`, for example `bin/kafka-topics.sh`. The output below comes from the Kafka 3.9 tools.

Create a topic with one partition, and send it five records:

```shell
kafka-topics --bootstrap-server localhost:9092 \
  --create --topic orders --partitions 1 --replication-factor 1

printf 'order-%s\n' 1 2 3 4 5 | kafka-console-producer \
  --bootstrap-server localhost:9092 --topic orders
```

Read three records as the group `demo`. The consumer commits its offset when it exits:

```shell
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic orders --group demo --from-beginning --max-messages 3
```

```text
order-1
order-2
order-3
Processed a total of 3 messages
```

Show where the group is. CURRENT-OFFSET is the committed offset, LOG-END-OFFSET is the offset of
the next record to be written, and LAG is the records left to read:

```shell
kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group demo
```

```text
Consumer group 'demo' has no active members.

GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID     HOST            CLIENT-ID
demo            orders          0          3               5               2               -               -               -
```

Move the group back to the start of the topic. The group must have no members:

```shell
kafka-consumer-groups --bootstrap-server localhost:9092 \
  --reset-offsets --to-earliest --group demo --topic orders --execute
```

```text
GROUP                          TOPIC                          PARTITION  NEW-OFFSET
demo                           orders                         0          0
```

The group now reads the topic again from the first record:

```shell
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic orders --group demo --max-messages 5
```

```text
order-1
order-2
order-3
order-4
order-5
Processed a total of 5 messages
```

Delete the group and its committed offsets:

```shell
kafka-consumer-groups --bootstrap-server localhost:9092 --delete --group demo
```

```text
Deletion of requested consumer groups ('demo') was successful.
```

## Managing groups

These `kafka-consumer-groups` operations work with Nisshi:

| Operation | Options |
| --- | --- |
| List groups | `--list`, with `--state` to show or filter by state |
| Describe a group | `--describe`, with `--members` or `--state` |
| Reset committed offsets | `--reset-offsets` with `--to-earliest`, `--to-latest`, `--to-offset`, `--to-datetime`, `--by-duration` or `--shift-by` |
| Delete a group | `--delete` |

`--delete-offsets` does not work, see [Limits](#limits).

As in Kafka, `--reset-offsets` and `--delete` refuse a group that has members. Stop the
consumers first.

## Timeouts and broker settings

A consumer's `session.timeout.ms` must be greater than 0 and at most 1,800,000 ms (30 minutes),
or JoinGroup answers INVALID_SESSION_TIMEOUT. Kafka 3.9.1 also rejects a session timeout below
6,000 ms, its default `group.min.session.timeout.ms`. Nisshi has no such minimum, and has no
setting for either bound.

Static membership (`group.instance.id`, KIP-345) works as in Kafka: a consumer that restarts with
the same instance ID takes its place back in a stable group without a rebalance.

Two broker options affect groups:

| Option | Environment variable | Default | Meaning |
| --- | --- | --- | --- |
| `--group-initial-rebalance-delay` | `GROUP_INITIAL_REBALANCE_DELAY` | `3s` | How long the first rebalance of an empty group waits for more members to join, as Kafka's `group.initial.rebalance.delay.ms`. |
| `--group-poll-interval` | `GROUP_POLL_INTERVAL` | `500ms` | How often a JoinGroup or SyncGroup that waits for a rebalance reads the group from the storage engine, to see changes made through other brokers. |

## Limits

- Nisshi does not support the KIP-848 consumer group protocol (`group.protocol=consumer`) or the
  share groups of KIP-932.
- Nisshi does not support OffsetDelete, so `kafka-consumer-groups --delete-offsets` fails
  ([#964]). Delete the whole group instead.
- Committed offsets do not expire. Nisshi has no `offsets.retention.minutes`: a group with no
  members keeps its offsets until you delete the group.
- A group uses the assignor protocol of its first member, and every member that joins later must
  list that protocol, or it gets INCONSISTENT_GROUP_PROTOCOL. Kafka picks the protocol that every
  member supports, so a group can move to `cooperative-sticky` with a rolling restart. With Nisshi
  it cannot ([#1051]): stop every consumer, then start them with the new assignor.
- Kafka applies a session timeout or a rebalance deadline when its timer fires. Nisshi applies it
  when the group gets its next request, so a member can leave the group, or a rebalance can end,
  later than it would in Kafka.
- The librdkafka and franz-go test suites in CI find three differences from Kafka, which the
  [Kafka clients tested](compatibility.md#kafka-clients-tested) section lists: members of groups
  that rebalance at the same time can get UNKNOWN_MEMBER_ID; a consumer that exceeds
  `max.poll.interval.ms` causes one more rebalance than in Kafka; and an offset commit during a
  rebalance gets different error codes.
- Nisshi has no `group.max.size`, so a group can have any number of members.
- Nisshi does not support transactions, so a consumer cannot commit offsets in a transaction
  (TxnOffsetCommit).
- `nisshi cat consume` reads one partition once, without joining a group or committing an offset.
  To consume as a group, use a Kafka client or `kafka-console-consumer`.

[#964]: https://github.com/nisshi-io/solace-nisshi/issues/964
[#1051]: https://github.com/nisshi-io/solace-nisshi/issues/1051
