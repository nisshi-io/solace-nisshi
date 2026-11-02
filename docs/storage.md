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

A broker opens at most 16 connections to PostgreSQL. Set `pool_max_size` in the storage URL to change that, to a number from 2 to 262143. ListOffsets and Fetch requests can each use at most half of them, so a larger pool lets more consumers read at once. Together they can hold every connection, and then other requests wait for one, so a pool smaller than 16 makes slow reads more likely to hold up produce and group requests. When the `nisshi_storage_read_deadline_exceeded` metric rises with `stage` set to `queued`, reads are waiting for a connection, and a larger pool can help. The server's `max_connections` must allow `pool_max_size` connections for each broker, plus every other client's. The broker does not check this at startup, so a pool that is too large fails requests under load instead. The broker also reserves memory for every connection the pool may open, so a very large `pool_max_size` costs memory even when the connections are not open:

```shell
nisshi broker --storage-engine "postgres://nisshi:secret@db.example.com/nisshi?pool_max_size=32"
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
