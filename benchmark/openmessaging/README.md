# OpenMessaging Benchmark against nisshi + postgres

Runs the [OpenMessaging Benchmark](https://openmessaging.cloud/docs/benchmarks/) Kafka driver against a
single nisshi broker with the `postgres://` storage backend. Everything runs on the host: it needs a Rust
toolchain, a JDK and Maven for the benchmark, and a Homebrew `postgresql@18`. Docker is not required.

```shell
./run.sh build                       # clone openmessaging/benchmark into .omb/ and build it
./run.sh broker                      # postgres on :5432 (files in .pg/) + nisshi (release) on :9092
./run.sh run                         # workloads/smoke.yaml, 1 minute
./run.sh run workloads/16-partitions-3min.yaml                     # 16 partitions at 50k msg/s, 3 minutes
./run.sh run workloads/1-topic-16-partitions-1kb.yaml              # any upstream workload, 15 minutes
./run.sh stop
```

Result JSON lands in `results/`.

| Variable | Effect |
|---|---|
| `HEAP_OPTS` | benchmark JVM heap (upstream default `-Xms4G -Xmx4G`) |
| `DRIVER` | driver file for `run`, instead of `driver-nisshi.yaml` |
| `POSTGRES=docker` | postgres 18 in a container instead of Homebrew; pass it to every subcommand that talks to postgres |
| `PG_BIN` | postgres `bin` directory, default `/opt/homebrew/opt/postgresql@18/bin` |
| `POSTGRES_PORT` | postgres port, default 5432 |
| `WAL_DIR` | directory for the host postgres WAL, e.g. on a RAM disk; `broker` deletes it first |
| `STORAGE_QUERY` | query string for the broker's storage URL, e.g. `maintenance_interval=10s` to sweep retention every 10 seconds instead of every 10 minutes |
| `POSTGRES_ARGS` | extra arguments for the postgres server, e.g. `-c track_io_timing=on` |
| `OMB_COMMIT` | upstream benchmark commit to build |

## Things to know

- `./run.sh broker` deletes and recreates `.pg/`, so every start begins with an empty database. It also
  builds the release binary first, which takes 2 to 4 minutes after a source change.
- The 15 minute upstream workload takes about 17 minutes with its 1 minute warm-up and writes 45 to 55 GB
  into `.pg/`. The 3 minute workload writes about 9 GB. `./run.sh stop` stops the broker and postgres but
  leaves `.pg/` on disk.
- One run says little about the publish latency tail. Runs with identical settings on one machine have
  differed by an order of magnitude in aggregate p99.9, and the worst publish of a 15 minute run was
  about 2 s in most of them. Compare several runs, and record the host's memory pressure and disk.
- `fetch.max.wait.ms` changes the result a lot. With the default of 500 the single consumer on 16
  partitions held a standing backlog of several hundred thousand records. With 100 it kept up. Copy
  `driver-nisshi.yaml`, add `fetch.max.wait.ms=100` under `consumerConfig`, and pass the copy as
  `DRIVER=/path/to/copy.yaml ./run.sh run ...`. A very small value, such as 10, made the consumer fall
  far behind in one run, so do not take smaller as better.

- On a laptop SSD the publish p99.9 of the 15 minute workload sits near 2 s because postgres stalls
  on WAL flushes: every commit waits behind one slow `fsync`, and fsyncs of 200 to 300 ms came every
  few seconds. With the WAL on a RAM disk, two runs had a p99.9 of 17 and 20 ms and a worst publish of
  about 120 ms. A RAM disk makes fsync free, so this shows where the tail comes from and is not a
  durable setup. To try it on macOS:

  ```shell
  dev=$(hdiutil attach -nomount ram://$((12 * 2048 * 1024)))   # 12 GiB
  diskutil erasevolume APFS pgwal $dev                         # mounts at /Volumes/pgwal
  WAL_DIR=/Volumes/pgwal/wal ./run.sh broker
  ./run.sh run workloads/1-topic-16-partitions-1kb.yaml
  ./run.sh stop && hdiutil detach $dev
  ```
- The stalls go away with `-c synchronous_commit=off`, passed through `POSTGRES_ARGS`, with the WAL
  still on the SSD. Two runs had a p99.9 of 22 and 34 ms and a worst publish of 118 and 466 ms, and
  neither had a 10 second window with a publish above 500 ms. The same setting with `wal_buffers=128MB`
  gave 19 and 21 ms in two runs. `wal_sync_method=fdatasync`, `commit_delay=500` with
  `commit_siblings=5`, and `wal_buffers=128MB` alone left the tail at 1.7 to 1.9 s, and `commit_delay`
  made it worse. Each of those was one run. With `synchronous_commit=off` an acknowledged produce is not
  yet flushed, so a crash loses up to about 600 ms of acknowledged records. Use it to find the stalls,
  not as a durable setup. On macOS only `fsync_writethrough` flushes the drive cache, and it is too slow
  to run at 50k msg/s, so the default WAL is not flushed to the media either.

## Connections per client

The broker logs one `nisshi::connection` event when it accepts a connection and one when the connection
closes, and `broker` turns them on (`RUST_LOG=warn,nisshi::connection=info`) and asks for a snapshot of every
open connection each `CONNECTION_SNAPSHOT_INTERVAL` (default `10s`). A close event carries the connection's
`client_id`, its age, the requests it sent by API, and the topic-partitions it produced to (with the records
and bytes) and fetched from. If you set `RUST_LOG` yourself, keep `nisshi::connection=info` in it.

```shell
./run.sh run workloads/16-partitions-3min.yaml
./run.sh connections                                          # clients, their connections and requests
./run.sh connections --connections --csv results/connections.csv   # every connection, and a CSV of them
```

`connections` runs the `connections` binary of `nisshi-broker` on `broker.log`, so it covers everything since
`./run.sh broker`. The first run builds the binary in release mode, which took about a minute on a laptop. It groups the connections
by `client_id`, and by client kind, which is the id without its trailing number (`producer-3` is a
`producer`). A connection that closed before it sent a request has no `client_id` and is reported as
`(no request)`. A connection still open when the log ends is taken from its latest snapshot.

`run` also samples the established sockets on port 9092 with `lsof` every `SOCKET_SAMPLE_INTERVAL` seconds
(default 5, 0 turns it off) into `results/sockets-*.csv`, and prints the peak when the run ends. The
`broker_established` column is the sockets the broker holds, and `benchmark_established` is the sockets the
benchmark JVM holds to the broker. Each connection has one of each, so the columns agree except while a
connection opens or closes, and `broker_established` should reach the broker's `open at the peak` figure
from `connections`. `./run.sh sockets 1` samples every second without a run, for another client.

### Connections that the OMB Kafka driver opens

`./run.sh sweep` runs 1, 4 and 8 producers with 1, 4 and 8 consumers on a topic of 4 and of 16 partitions, at 5000
messages per second of 1 KiB, for 1 minute after the benchmark's 1 minute warm-up (`SWEEP_RATE` and `SWEEP_MINUTES`
change those). Each run has a new broker, so its log holds that run's connections only, and `results/sweep/` keeps
the report, the CSV and the socket samples of each. The table is one run of each combination, on the benchmark
commit that `build` checks out, with Kafka's Java client 3.6.1 and the broker on Postgres.

| producers | consumers | partitions | accepted | open at the peak | `lsof` peak, broker | `lsof` peak, benchmark |
|---|---|---|---|---|---|---|
| 1 | 1 | 4 | 8 | 7 | 7 | 7 |
| 4 | 1 | 4 | 14 | 13 | 13 | 13 |
| 1 | 4 | 4 | 17 | 16 | 16 | 16 |
| 8 | 8 | 4 | 43 | 38 | 38 | 38 |
| 1 | 1 | 16 | 8 | 7 | 7 | 7 |
| 4 | 1 | 16 | 14 | 13 | 13 | 13 |
| 1 | 4 | 16 | 17 | 16 | 16 | 16 |
| 8 | 8 | 16 | 43 | 42 | 42 | 42 |

Each client holds these connections, and none of them depends on the number of partitions:

| client | connections | what each carries |
|---|---|---|
| producer | 2 | the first: `ApiVersions`, `Metadata`, `InitProducerId`. The second: `ApiVersions`, `Metadata` and every `Produce` |
| consumer | 3 | the first: `FindCoordinator`, `Metadata`. The second: `JoinGroup`, `SyncGroup`, `Heartbeat`, `OffsetCommit`, `OffsetFetch`, `LeaveGroup`. The third: `Fetch`, `ListOffsets` |
| admin client (the benchmark's topic creator) | 2 | the first: `Metadata`. The second: `CreateTopics`. Both stay open for the whole run |

- The open connections at the peak are 2 + 2 × producers + 3 × consumers, so 7, 13, 16 and 42. The 8 producer and 8
  consumer run on 4 partitions peaked at 38, because a consumer opens its third connection, the one that fetches, only
  when it has a partition. Four of the eight consumers had none until the end of the run, when the others left the
  group and the partitions moved. Their fetch connections lived 0.5 to 2 seconds, and they are the 4 extra connections
  in that run's 43 accepted.
- A producer sends the `Produce` requests for every partition on one connection, because the broker is the leader of
  all of them. With 4 partitions, one connection carried 99001 `Produce` requests with 635768 records.
- The connection that `run.sh` opens to wait for the broker is the one `(no request)` connection of each run, and it is
  one of the accepted connections.
- The log's peak and the `lsof` peak at both ends agree in every run. Each run held its 5000 messages per second and the
  consumers kept up, with a publish p99 between 2.6 and 28.6 ms, the highest with 8 producers.
- The OMB consumer calls `commitAsync` after each poll that returns records, so the coordinator connection of a
  consumer carries about as many `OffsetCommit` requests as its fetch connection carries `Fetch` requests (29478 and 29496
  for the first consumer of the 8 producer and 8 consumer run on 4 partitions).

These counts are what Kafka's Java client does against nisshi, and the roles in the table are read from the requests
on each connection. The consumer ids that OMB sets end in a random suffix, so the `kind` column of a consumer differs
from run to run. The next section runs the same workloads against Apache Kafka.

### The same workloads on Apache Kafka

`BROKER=kafka` runs the `apache/kafka` image (3.9.2, `KAFKA_IMAGE`) in docker in place of nisshi and postgres, in KRaft
mode, with the driver's settings unchanged. `KAFKA_BROKERS=3` starts three brokers, each also a controller, on ports
9092, 9192 and 9292. They replicate over an internal listener, so the broker-side count holds client connections only,
and `driver-kafka3.yaml` is the driver with `replicationFactor: 3`. Each broker's log directory is a tmpfs of
`KAFKA_TMPFS_SIZE` (default 1g), which is cleared with each run, so a run needs no disk in the docker VM. At the sweep's
rate a run writes about 1 GB in 3 minutes, so give a broker that serves more than one run a larger tmpfs. Kafka has no
connection log, so the sampler counts the sockets with `netstat` inside each container and the report is skipped.

```shell
BROKER=kafka ./run.sh sweep                                                    # results/sweep-kafka/
BROKER=kafka KAFKA_BROKERS=3 DRIVER="$PWD/driver-kafka3.yaml" ./run.sh sweep   # results/sweep-kafka-3/
```

Peak established sockets on the broker, one run of each combination. The benchmark's own count agrees in every run. Each
Kafka cluster is new for each run:

| producers | consumers | partitions | nisshi | Kafka, 1 broker | Kafka, 3 brokers |
|---|---|---|---|---|---|
| 1 | 1 | 4 | 7 | 7 | 11 |
| 4 | 1 | 4 | 13 | 13 | 23 |
| 1 | 4 | 4 | 16 | 16 | 22 |
| 8 | 8 | 4 | 38 | 42 | 61 |
| 1 | 1 | 16 | 7 | 7 | 11 |
| 4 | 1 | 16 | 13 | 13 | 23 |
| 1 | 4 | 16 | 16 | 16 | 25 |
| 8 | 8 | 16 | 42 | 42 | 67 |

- One Kafka broker gives nisshi's counts, except for the 8 producer and 8 consumer run on 4 partitions: 42, against
  nisshi's 38. nisshi's 38 is repeatable (two sweeps).
- The 4 extra connections are a cold start of Kafka. A consumer asks for its group coordinator first, and on a new
  cluster Kafka answers `COORDINATOR_NOT_AVAILABLE` (error 15), because it creates `__consumer_offsets` on that first
  request. The request log has that error 8 times, once for each consumer. The client then retries on a connection to
  the broker's own node id, and keeps it for the run. A consumer without a partition held 3 connections on Kafka
  (bootstrap, node 1, coordinator) and 2 on nisshi (bootstrap, coordinator), which answers `FindCoordinator` the first
  time. The client log of one such consumer on each broker shows this.
- Run twice on one Kafka broker, the second run, with `__consumer_offsets` in place, peaked at 38 like nisshi. There the
  consumer without a partition opened its node connection only when it got a partition, as the group wound down. A
  3 second `group.initial.rebalance.delay.ms` instead of 0 changes nothing: 42 with 1 broker and 62 with 3.
- With three brokers a client opens a connection to each broker that it uses. The 8 producer and 8 consumer run on 4
  partitions, run twice on one cluster of 3 brokers, peaked at 60 the first time and 54 the second, which is the warm
  cluster. The warm peak is the sum of these connections, and the client logs count the same initiations:

  | client | connections each | clients | total |
  |---|---|---|---|
  | admin | 2 | 1 | 2 |
  | producer | 4: bootstrap, and one to each of the 3 brokers | 8 | 32 |
  | consumer with a partition | 3: bootstrap, coordinator, and one fetch connection to the broker that leads its partition | 4 | 12 |
  | consumer without a partition | 2: bootstrap, coordinator | 4 | 8 |

  The 6 extra connections of the cold run are the retry of the first `FindCoordinator`, which opens a connection to a
  broker's own node id. Eight consumers would add up to 8, but a consumer that has a partition may fetch from the
  broker that the retry used, and then the two share one connection. That split is read from the totals and not
  checked per connection. The 3-broker rows of the table above are cold runs.

To see which requests each connection carries on Kafka, `KAFKA_REQUEST_LOG=1` sets `kafka.request.logger` to DEBUG once
the brokers are up. The log is in `/opt/kafka/logs/kafka-request.log` of each container, one JSON line per request with
its connection id, client id and API, and `docker exec <container> cat` reads it before `run.sh stop` removes the
container. It slows the broker, so use it for the connection profile and not for rates. To see when a Java client opens
and closes each connection, run with `log4j2-connections.xml`, which writes the connection lines of `NetworkClient` and
`Selector` to a file:

```shell
HEAP_OPTS="-Xms4G -Xmx4G -Dlog4j.configurationFile=$PWD/log4j2-connections.xml -Dclient.log=$PWD/client.log" \
    ./run.sh run workloads/smoke.yaml
```

### Three nisshi brokers behind a balancer

`NISSHI_BROKERS=3` starts three nisshi brokers on the one postgres, on ports 9192, 9292 and 9392, each with a log of its
own (`broker-1.log` and so on). Each advertises `localhost:9092`, where `balance.py`, a small TCP balancer, hands each
connection to the next broker in turn, like a layer 4 balancer. Every broker keeps node 111, so Metadata still lists one
broker and the clients see a single one. The sampler adds up the sockets of the three brokers, and `sweep` reports each
broker's connections as `<run>.broker-N.md` and `.csv` in `results/sweep-nisshi-3/`.

```shell
NISSHI_BROKERS=3 ./run.sh sweep
```

The sweep held its rate in every run with no write errors, and the consumer groups formed with their members on
different brokers. The peak connections, one run of each combination, are the same as for one nisshi broker in all 8:
7, 13, 16 and 42, and 38 for 8 producers and 8 consumers on 4 partitions. The clients can't tell the three brokers from
one, so each opens the connections that it opens to one broker, and a producer holds 2, where it holds 4 on 3 Kafka
brokers (see the table above).

The balancer splits the connections evenly and the `Produce` traffic much less. A producer sends every partition on
one data connection, so all of a producer's `Produce` requests go to the one broker that got that connection:

| producers | consumers | partitions | connections at the peak, per broker | `Produce` requests per broker | producers with data on each |
|---|---|---|---|---|---|
| 1 | 1 | 4 | 2 / 3 / 2 | 189k / 0 / 0 | 1 / 0 / 0 |
| 4 | 1 | 4 | 4 / 5 / 4 | 332k / 165k / 166k | 2 / 1 / 1 |
| 1 | 4 | 4 | 5 / 6 / 5 | 194k / 0 / 0 | 1 / 0 / 0 |
| 8 | 8 | 4 | 12 / 13 / 13 | 284k / 277k / 183k | 3 / 3 / 2 |
| 8 | 8 | 16 | 14 / 14 / 14 | 334k / 334k / 227k | 3 / 3 / 2 |

The 16-partition runs for 1 and 4 producers split like the 4-partition ones. The balancer spreads producers, not the
partitions of one producer, so a single heavy producer loads one broker, where on Kafka it would use every broker that
leads one of its partitions. Spreading one producer's partitions over brokers takes distinct node ids in Metadata, which
nisshi does not have, so the brokers behind the balancer do not give it.

### Three node ids behind the balancer (prototype)

This is a prototype, on the branch `prototype-node-ids`, and not a feature. With `NISSHI_PROTOTYPE_NODE_IDS=111,222,333`
the Postgres `metadata` function lists each node id, all at the advertised address, which is the balancer's. It sorts the
ids and does not shuffle them as it did, so every broker assigns the same leader to a partition on every call. Nothing
else changes: `FindCoordinator` still answers node 111, so every consumer's coordinator is node 111, and `DescribeCluster`
lists one broker. The brokers can serve any partition, so a request that the balancer sends to a broker that is not the
node that the client meant is answered like any other.

```shell
NISSHI_BROKERS=3 NISSHI_PROTOTYPE_NODE_IDS=111,222,333 SWEEP_OUT="$PWD/results/sweep-nisshi-3-nodes" ./run.sh sweep
```

The Java client keeps a connection for each node id, and does not look at the address, so a client opens a connection to
each node that it uses, even though they all lead to the same address. Each run of the sweep held its rate with no write
errors. Peak connections, one run of each combination:

| producers | consumers | partitions | nisshi, 1 broker | balancer, plain | balancer, 3 node ids | Kafka, 3 brokers (new cluster) |
|---|---|---|---|---|---|---|
| 1 | 1 | 4 | 7 | 7 | 11 | 11 |
| 4 | 1 | 4 | 13 | 13 | 23 | 23 |
| 1 | 4 | 4 | 16 | 16 | 18 | 22 |
| 8 | 8 | 4 | 38 | 38 | 54 | 61 |
| 1 | 1 | 16 | 7 | 7 | 11 | 11 |
| 4 | 1 | 16 | 13 | 13 | 23 | 23 |
| 1 | 4 | 16 | 16 | 16 | 26 | 25 |
| 8 | 8 | 16 | 42 | 42 | 66 | 67 |

- A producer holds 4 connections, a bootstrap connection and one for each of the 3 node ids, where it held 2. A consumer
  with a partition holds 3 and a consumer without one holds 2, as before: it fetches from the node that leads its
  partition. These are the counts of a warm 3-broker Kafka cluster, which the 8 producer and 8 consumer run on 4
  partitions matches exactly (54). The rows where Kafka has more (22 and 61) are its new cluster's retry of the first
  `FindCoordinator`, which nisshi does not need.
- The cost is 42% more connections for the 8 producer and 8 consumer run on 4 partitions (54, from 38) and 57% more on
  16 (66, from 42).
- The connections spread evenly over the brokers: 18, 18 and 18 for the first of those runs, and 22, 22 and 22 for the
  second.

What a producer gains is that its `Produce` requests spread over the brokers, which the plain balancer did not do. The
table is the `Produce` requests of each broker, in thousands, and the producers that had a `Produce` connection on it:

| producers | consumers | partitions | `Produce` per broker | producers with data on each |
|---|---|---|---|---|
| 1 | 1 | 4 | 48 / 109 / 53 | 1 / 1 / 1 |
| 4 | 1 | 4 | 192 / 197 / 240 | 2 / 3 / 3 |
| 8 | 8 | 4 | 233 / 315 / 206 | 5 / 7 / 3 |
| 1 | 4 | 16 | 52 / 63 / 59 | 1 / 1 / 1 |
| 8 | 8 | 16 | 371 / 335 / 255 | 6 / 4 / 6 |

With the plain balancer one producer's traffic went to one broker (189 / 0 / 0). Now it goes to all three. In one run, 1
producer and 4 consumers on 4 partitions, one broker had no `Produce` requests (91 / 0 / 87). The producer had its three
data connections, so I think the balancer put two of them on one broker, but I did not check.

Each combination ran once. A feature would also have to decide the coordinator (which broker `FindCoordinator` names),
`DescribeCluster`, what happens to clients when the list of node ids changes, and the other storage engines' `metadata`,
which this prototype does not touch.

## Checking queries against large volumes

`seed.sql` fills a topic with generated records straight in postgres, far faster than producing
through the broker, and `explain` shows the plans of the fetch queries against it. Start postgres
with `./run.sh broker` first.

```shell
./run.sh seed partitions=16 rows=1000000 value_size=1024   # ~18 GB, see seed.sql for all variables
./run.sh explain seed 3 500000                             # topic, partition, fetch offset
```

Compare plans at several row counts, and from the start, middle and tail of a partition. The seeded
topic is an ordinary topic, so a consumer pointed at the broker can read it end to end.

- `driver-nisshi.yaml` is upstream's Kafka driver with `replicationFactor: 1` (nisshi is single node).
- The broker advertises `localhost:9092`, which is also the driver's bootstrap address.
- The build checks out upstream commit `5b1fa70951a323da26bd587174b58bb2c65b0b5c` (`OMB_COMMIT` overrides it) and stops with an error if its `Benchmark.main` no longer matches the patch.
- The build overrides upstream's Lombok version, which cannot compile on a current JDK, and makes the JVM exit after a run, which upstream's `Benchmark.main` does not.
