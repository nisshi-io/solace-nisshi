# Storage options

Choose the engine with `--storage-engine` (or `STORAGE_ENGINE`) using the URL scheme below.

| Engine | URL scheme | Cargo feature | Status |
| --- | --- | --- | --- |
| PostgreSQL | `postgres://` | `postgres` | Suitable for production. Tested, with bugs and security issues fixed. Help comes from the community. |
| S3 and Google Cloud Storage | `s3://`, `gs://` | `dynostore` | Experimental. Available for continuity. Not recommended for production. |
| SQLite (libSQL) | `sqlite://` | `libsql` | Suitable for development. |
| In-memory | `memory://` | `dynostore` | Suitable for development and testing. Data is lost when Nisshi stops. |
| Turso | `turso://` | `turso` | Experimental. |
| SlateDB | `slatedb://` | `slatedb` | Deprecated. Not recommended for new deployments, because it allows a single writer. Only security fixes are accepted. The engine may be removed in a future release. Use PostgreSQL for production, or SQLite or in-memory for development. The broker logs a warning at startup. |

The default build enables the `dynostore`, `libsql`, and `postgres` features. To add an engine, add its feature to the `--features` list, for example
`--features turso`. Apache Parquet, Apache Iceberg, and Delta Lake output for schema backed
topics needs the `parquet`, `iceberg`, or `delta` feature.

The engines differ in what they support. For example, only PostgreSQL and SQLite apply
retention and compaction. [Compatibility and feature status](compatibility.md) lists what
each engine supports.

Switching storage engines does not move data: topics, messages, and consumer group offsets stay in the old engine, and the new one starts empty. The release binary starts on `memory://`, and Docker Compose runs on PostgreSQL unless you set `STORAGE_ENGINE`.

PostgreSQL can stream transaction logs to an archive with
[continuous archiving][continuous-archiving].

Nisshi OSS is provided as-is, with [community support][nisshi-community] only.

## Running Docker Compose with S3 (MinIO)

The following will configure a S3 storage engine
using the "nisshi" bucket (full context is in
[compose.yaml](../compose.yaml) and [example.env](../example.env)):

Copy `example.env` into `.env` so that you have a local working copy:

```shell
cp example.env .env
```

Edit `.env` so that `STORAGE_ENGINE` is defined as:

```shell
STORAGE_ENGINE="s3://nisshi/"
```

First time startup, you'll need to create a bucket, an access key
and a secret in minio.

Just bring minio up, without nisshi:

```shell
docker compose up -d minio
```

Create a minio `local` alias representing `http://localhost:9000` with the default credentials of `minioadmin`:

```shell
docker compose exec minio \
   /usr/bin/mc \
   alias \
   set \
   local \
   http://localhost:9000 \
   minioadmin \
   minioadmin
```

Create a `nisshi` bucket in minio using the `local` alias:

```shell
docker compose exec minio \
   /usr/bin/mc mb local/nisshi
```

Once this is done, you can start nisshi with:

```shell
docker compose up -d nisshi
```

Using the regular Apache Kafka CLI you can create topics, produce and consume
messages with Nisshi:

```shell
kafka-topics \
  --bootstrap-server localhost:9092 \
  --partitions=3 \
  --replication-factor=1 \
  --create --topic test
```

Describe the `test` topic:

```shell
kafka-topics \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic test
```

Note that node 111 is the leader and ISR for each topic partition.
This node represents the broker handling your request. Every Nisshi broker
reports the same node ID, 111; it does not identify a particular machine.

Producer:

```shell
echo "hello world" | kafka-console-producer \
    --bootstrap-server localhost:9092 \
    --topic test
```

Group consumer using `test-consumer-group`:

```shell
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --group test-consumer-group \
  --topic test \
  --from-beginning \
  --property print.timestamp=true \
  --property print.key=true \
  --property print.offset=true \
  --property print.partition=true \
  --property print.headers=true \
  --property print.value=true
```

Describe the consumer `test-consumer-group` group:

```shell
kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group test-consumer-group \
  --describe
```

## Running Docker Compose with PostgreSQL

To switch between the minio and PostgreSQL examples, firstly
shutdown Nisshi:

```shell
docker compose down nisshi
```

Switch to the PostgreSQL storage engine by updating `.env`:

```env
# minio storage engine
# STORAGE_ENGINE="s3://nisshi/"

# PostgreSQL storage engine -- use the compose service name `db`, not `localhost`
STORAGE_ENGINE="postgres://postgres:postgres@db"
```

Start PostgreSQL:

```shell
docker compose up -d db
```

The `db` service runs PostgreSQL 18, and that image does not start on a `db` volume that holds PostgreSQL 17 data. Run `docker compose down --volumes` once before the first start, or set `POSTGRES_IMAGE=postgres:17` in `.env` to stay on 17.

Nisshi creates its tables when it starts on an empty database, and records the schema version there. On a PostgreSQL server of your own, give Nisshi a role that owns its database:

```sql
create role nisshi login password '...';
create database nisshi owner nisshi;
```

A broker changes the schema only when the database doesn't record the broker's schema version: on the first start, and on the first start of a release that changes the schema. To run brokers with a role that can only read and write rows, start one broker once with the credentials of the role that owns the tables, then start every broker with the read/write role. Do the same before you upgrade to a release that changes the schema. A broker whose role can't change the schema refuses to start until that has happened.

Nisshi is tested on PostgreSQL 16 and later. On PostgreSQL 16 or later, an administrator who isn't a superuser, such as the admin user of a managed service, must first run `grant nisshi to <admin>`. Nisshi supports a database that 0.7.0-pre.3 or a later release set up.

A broker opens at most 16 connections to PostgreSQL. Set `pool_max_size` in the storage URL to change that, to a number from 2 to 262143. ListOffsets and Fetch requests can each use at most half of them, so a larger pool lets more consumers read at once. Together they can hold every connection, and then other requests wait for one, so a pool smaller than 16 makes slow reads more likely to hold up produce and group requests. When the `nisshi_storage_read_deadline_exceeded` metric rises with `wait` set to `engine_connection_pool` or `engine_read_limit`, reads are waiting for a connection or for the half of the pool that their kind of read may use, and a larger pool can help. With `wait` set to `connection_read_limit`, one client connection has more reads in flight than it may hold at once, and a larger pool does not help. The server's `max_connections` must allow `pool_max_size` connections for each broker, plus every other client's. The broker does not check this at startup, so a pool that is too large fails requests under load instead. The broker also reserves memory for every connection the pool may open, so a very large `pool_max_size` costs memory even when the connections are not open:

```shell
nisshi broker --storage-engine "postgres://nisshi:secret@db.example.com/nisshi?pool_max_size=32"
```

Every connection a broker opens to PostgreSQL has the `application_name` `nisshi-<host>-<cluster>`, for example `nisshi-broker-7d9f6-3f9a1c`, so `pg_stat_activity` shows which connections belong to a broker, for example to find the one that holds a lock. `application_name LIKE 'nisshi%'` finds all of them. The host is the name of the machine that the broker runs on, read from the `HOSTNAME` environment variable or from `/etc/hostname`. On Kubernetes it is the name of the pod, so a connection that holds a lock leads to its pod even where a pooler or proxy hides `client_addr`. Where the host is not known, the name has the node instead, `nisshi-<node>-<cluster>`, and every broker has node 111 today. Set `application_name` in the storage URL, or `-c application_name=...` in its `options`, to use your own name. A name that you set is used as it is. In the name that Nisshi sets, a character of the host or the cluster that is not an ASCII letter, digit, `.`, `_` or `-` becomes `_`, and the name is cut at 63 bytes, the most that PostgreSQL keeps. The cut takes the end of the cluster first. With the 7 bytes of `nisshi-` and the `-` after the host, the cluster is gone from a host of about 55 bytes, and the host itself is cut once it is longer than 56 bytes.

The broker also limits how many of those reads one client holds while another client waits for them, so that the reads of one client do not queue ahead of the others'. A read that the client no longer waits for, an abandoned read, keeps counting until the database ends its statement, which can take up to `statement_timeout` plus 5 seconds when a cancel request does not reach the server, for example behind pgbouncer. A client is the authenticated user together with the `client_id` of its requests, which is what Kafka keys its client quotas on, and the limit holds over all its connections, including new ones that it opens. A client's share of a limit is all of it except what one connection holds, 6 of 8 Fetch reads and 4 of 8 ListOffsets reads with the default pool, and never less than one connection holds. The share binds only while the limit is used up and another client waits for it. A client that is alone may hold the whole limit, and the broker reserves nothing for a newcomer: a client that abandons reads while it is alone can fill the limit, and a client that arrives later waits for a permit until some of those statements end, up to `statement_timeout` plus 5 seconds when a cancel request does not reach the server. A read of a client past its share goes ahead when a permit of the limit is free, and otherwise waits for one of the client's own reads to end, so it takes no place in the queue ahead of another client. The reads that the client already runs go on and return their permits as they end, and a permit that frees while another client waits goes to that client. Set `client_read_share` in the storage URL to a percentage from 1 to 100 of each limit to change the share. 100 lets a client hold the whole limit while others wait, which turns the share off in effect. A read then waits with `wait` set to `client_read_limit` only when its client holds every permit of the limit:

```shell
nisshi broker --storage-engine "postgres://nisshi:secret@db.example.com/nisshi?client_read_share=50"
```

A client whose reads already hold its share, while the limit is used up, sees a wait and nothing else. The connection stays open, and the response has no error and `throttle_time_ms` 0. If the request's deadline passes first, Fetch answers the partition with no records and ListOffsets answers `REQUEST_TIMED_OUT` for it. The `nisshi_storage_read_deadline_exceeded` metric then rises with `stage` set to `queued` and `wait` set to `client_read_limit`. When `wait` is `engine_read_limit`, a larger `pool_max_size` helps. When `wait` is `client_read_limit`, the client already held its share and the limit was full, whether or not another client waited. Give each instance its own `client.id`, or set a larger `client_read_share`, or raise `pool_max_size`: a larger pool raises the limit, and the share with it, because a client's share is all of a limit except what one connection holds. A larger pool does not stop a client that keeps abandoning reads, which can fill any limit while it is alone. Clients that send the same `client_id` and log in as the same user, or with authentication off, share one limit. That includes every instance of librdkafka's default `rdkafka`, and every replica of a Java consumer group without a `client.id`, which sends `consumer-<group.id>-<n>` with `n` counting from 1 in each JVM, so each replica sends `consumer-<group.id>-1`. A group that shares one share has all its replicas past it together once another client waits for the limit, so set a `client.id` for each instance, or set `client_read_share=100`. The `client_id` is not authenticated, and the limit is keyed on the user and the `client_id` together, so a client that sends a different `client_id`, with or without authentication, gets a limit of its own. The share protects a client that waits from one that abandons reads, and does not stop a user that changes its `client_id` on purpose. The broker has no limit on connections.

Kafka 3.9.1 has no such limit by default. Its `max.connections.per.ip`, `max.connections` and `max.connection.creation.rate` are unlimited. Its client quotas are unlimited too, and a client over one gets a response with `throttle_time_ms` set, which is empty for a Fetch.

The broker checks that a pooled connection is not read-only, with one query that fails when the session is read-only. It checks every new connection before its first use, and every fourth checkout of an idle connection, and it drops a connection that fails. After a failover, an old primary that stays up but turns read-only no longer keeps the broker's connections, so writes recover without a restart. A connection that turns read-only can serve up to three checkouts before the check finds it, and each write on it fails with a retriable error that the client retries. While the endpoint still reaches a read-only server, every new connection fails its check, so each checkout fails fast with a retriable error and the pool does not grow, until the endpoint reaches a writable server. The connect rate follows the request rate while the endpoint reaches a read-only server, and SOL-157751 will gate it. `read_only_check_every` sets how often the check runs on an idle connection, as a whole number from 0 to 1000000: 1 checks every checkout, 4 is the default, and 0 turns the check off, for the behavior before the check, in which the broker keeps a connection that is open. A check adds one round trip to a checkout, which halves the throughput of a pool that runs statements as short as the check, so a larger number costs less and finds a read-only connection later. The option goes in the storage URL next to `pool_max_size`:

```shell
nisshi broker --storage-engine "postgres://nisshi:secret@db.example.com/nisshi?pool_max_size=32&read_only_check_every=1"
```

When a check fails, the broker logs `a pooled connection failed its check and is dropped` at `warn`, with the cluster, the stage (`create` or `recycle`), the reason (`read_only`, `closed`, `timeout` or `error`) and the server's error, which names the read-only session, and counts it in `nisshi_sql_pool_replaced` with the same three attributes `cluster_id`, `stage` and `reason`. A check that takes longer than 5 seconds drops that connection and tries the next idle one, so when many idle connections are black-holed one checkout can take up to 5 seconds for each of them. A rise in connection opens and in read-only errors on the old primary comes from this check, not from a leak. To refuse a read-only server when connecting, list every candidate host and add `target_session_attrs=read-write`. With `load_balance_hosts=random`, the broker tries the hosts in random order and connects to the first that accepts writes; without it, it tries them in list order. With a single host it fails to connect instead, and the broker answers requests with a retriable error until that host accepts writes:

```shell
nisshi broker --storage-engine "postgres://nisshi:secret@db1.example.com,db2.example.com/nisshi?target_session_attrs=read-write&load_balance_hosts=random"
```

Bring Nisshi back up:

```shell
docker compose up -d nisshi
```

Using the regular Apache Kafka CLI you can create topics, produce and consume
messages with Nisshi:

```shell
kafka-topics \
  --bootstrap-server localhost:9092 \
  --partitions=3 \
  --replication-factor=1 \
  --create --topic test
```

Producer:

```shell
echo "hello world" | kafka-console-producer \
    --bootstrap-server localhost:9092 \
    --topic test
```

Consumer:

```shell
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --group test-consumer-group \
  --topic test \
  --from-beginning \
  --property print.timestamp=true \
  --property print.key=true \
  --property print.offset=true \
  --property print.partition=true \
  --property print.headers=true \
  --property print.value=true
```

Or using [librdkafka][librdkafka] to produce:

```shell
echo "Lorem ipsum dolor..." | \
  ./examples/rdkafka_example -P \
  -t test -p 1 \
  -b localhost:9092 \
  -z gzip
```

Consumer:

```shell
./examples/rdkafka_example \
  -C \
  -t test -p 1 \
  -b localhost:9092
```

[continuous-archiving]: https://www.postgresql.org/docs/current/continuous-archiving.html
[librdkafka]: https://github.com/confluentinc/librdkafka
[nisshi-community]: https://community.solace.com/c/nisshi-diskless-streaming/19
