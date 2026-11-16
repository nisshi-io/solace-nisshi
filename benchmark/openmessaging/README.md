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
