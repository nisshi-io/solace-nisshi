# Architecture

This page is for contributors: how a request moves through Nisshi, and where each part lives in
the code. For using Nisshi, see the [README](README.md) and [docs](docs/README.md).

## Overview

Nisshi is a Cargo workspace that builds one binary, `nisshi`. Its subcommands are `broker` (the
default), `cat`, `topic`, `user`, `generator`, `perf` and `proxy`.

The broker answers the Apache Kafka® protocol and keeps no data of its own. A storage engine
(PostgreSQL, SQLite, an object store or memory) holds topics, messages, consumer groups and users,
so brokers that share an engine can each answer any request. For a topic with a schema, the engine
validates each record it is given, and can write the topic to an Apache Iceberg or Delta Lake table
as well.

A request takes this path:

```text
Kafka client
    │  TCP (optionally TLS)
    ▼
TcpBytesLayer          bytes off the socket
    ▼
BytesFrameLayer        bytes → Kafka frame (header + typed request)
    ▼
FrameRouteService      routes on API key to a typed handler
    ▼
handler                e.g. ProduceService, FetchService, JoinGroupService
    ▼
Storage                StorageContainer → the engine picked by the storage URL
    │                    ├─ postgres://        PostgreSQL
    │                    ├─ sqlite://          libSQL
    │                    └─ s3:// gs:// memory://   object store
    ▼
schema registry ── validate the record ──► lake writer (Parquet, Iceberg, Delta)
```

The response goes back the same way: `FrameBytesLayer` encodes the frame and `BytesTcpService`
writes it to the socket.

## Crates

| Group | Crate | Role |
|---|---|---|
| Binary | `nisshi` | Entry point and subcommand dispatch |
| | `nisshi-cli` | Command line parsing (clap) |
| Protocol | `nisshi-sans-io` | Kafka wire protocol types and codec, generated from the Kafka message descriptors; no I/O |
| | `nisshi-model` | Reads the Kafka JSON message descriptors, for `nisshi-sans-io`'s build script |
| | `nisshi-auth` | SASL: mechanism handshake and SCRAM credential checks |
| Service | `nisshi-service` | Network layers that turn TCP bytes into typed requests and back |
| | `nisshi-client` | Async Kafka protocol client built on the same layers |
| Broker | `nisshi-broker` | The broker: request handlers and the consumer group coordinator |
| | `nisshi-proxy` | Kafka protocol proxy |
| Storage | `nisshi-storage` | The `Storage` and `StorageFactory` traits, `StorageContainer`, and the handlers that call storage |
| | `nisshi-storage-sql` | `postgres://` and `sqlite://` |
| | `nisshi-storage-dynostore` | `memory://`, `s3://` and `gs://`, on the `object_store` crate |
| | `nisshi-storage-slatedb` | `slatedb://` (deprecated) |
| | `nisshi-storage-null` | `null://`, which keeps topic metadata only |
| Schema and lake | `nisshi-schema` | Schema registry (Avro, JSON Schema, Protocol Buffers) and the Parquet, Iceberg and Delta writers |
| Tools | `nisshi-cat` | Produce and consume Avro, JSON and Protobuf messages |
| | `nisshi-topic` | Create, list and delete topics |
| | `nisshi-generator` | Generate fake data for schema-backed topics |
| | `nisshi-perf` | Performance producer and consumer |
| | `nisshi-otel` | OpenTelemetry setup shared by the commands |
| Tests | `nisshi-smoke-test` | Runs the Apache Kafka command line tools against a broker |
| | `fuzz` | cargo-fuzz targets for the codec and the storage handlers |

## Wire protocol

`nisshi-sans-io/build.rs` reads the Apache Kafka 3.9.1 message descriptors in
[`nisshi-sans-io/message`](nisshi-sans-io/message) and generates a Rust type for every request and
response, with its encoding at every version the descriptor lists. `ApiVersions` advertises those
ranges. The descriptors come from Apache Kafka unchanged, and the generated code is not edited by
hand: a protocol change starts in the descriptors or in the generator.

Because the crate does no I/O, the broker, the proxy, the client and the fuzz targets all share it.

## Service layers

`nisshi-service` composes the network path from [rama](https://crates.io/crates/rama) layers and
services, as in the diagram above. The broker, the proxy and the command line clients all use the
same layers, with different handlers at the end.

Each request carries a `rama` `Extensions` bag beside its value. It holds what a handler needs to
know about the connection, such as whether the client has authenticated. The bag lives as long as
the connection, not just one request. Handlers get their dependencies, such as the storage handle
or the group coordinator, as struct fields rather than through the bag.

## Storage

`Storage` (in `nisshi-storage`) is the trait every engine implements: produce, fetch, offsets,
topics, consumer groups, users and configuration. Each engine crate also implements
`StorageFactory`, which says which URL schemes it accepts and builds the engine. At startup,
`StorageContainer::builder()` collects a factory for each engine compiled in, and `build()` picks
the first whose scheme matches the `--storage-engine` URL. Cargo features choose which engines are
compiled in; [Storage options](docs/storage.md) lists them.

The handlers for most Kafka APIs live in `nisshi-storage/src/service/`, one file per API, and
call the `Storage` trait. Consumer group handlers live in `nisshi-broker/src/broker/group/` and call
the group coordinator in `nisshi-broker/src/coordinator/group/`, which keeps group state in
storage too.

## A stateless broker

A broker holds no data between requests that another broker would need, so brokers that share a
storage engine are interchangeable. There is no partition leadership to move and no replication
between brokers. Every broker reports the same node ID, 111, in metadata, and is the leader and the
only in-sync replica of every partition. The node ID names whichever broker the client is talking
to, not a particular machine.

This holds for an engine that brokers can share, such as PostgreSQL or S3. `memory://` keeps its
data in the broker's own memory, so no other broker can see it.

## Schema registry and lake output

The `--schema-registry` URL points at a directory or bucket holding a schema per topic
(`topic.avsc`, `topic.json` or `topic.proto`). The broker hands the registry, and the lake writer
if one is configured, to the storage engine when it builds it. On produce, the engine validates
each record against the topic's schema and rejects one that doesn't match. With a lake writer
(`broker parquet`, `broker iceberg` or `broker delta`, behind the `parquet`, `iceberg` and `delta`
features), it also writes each record to the topic's table. [Schema registry](docs/schema-registry.md)
describes the schema files.

## Tests

[CONTRIBUTING](CONTRIBUTING.md) explains how to build and test, and which checks CI runs on each
storage engine. In short: each of `nisshi-broker`, `nisshi-sans-io` and `nisshi-service` has one
integration test binary, `tests/it/`, with a file per feature or Kafka API; `nisshi-smoke-test`
runs the Kafka command line tools against a real broker; `compat/` runs the librdkafka and franz-go
test suites; and `fuzz/` fuzzes the codec and the storage handlers.
