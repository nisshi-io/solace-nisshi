<div align="center">

# Nisshi 🗃️
A stateless server that implements the Apache Kafka® API, with pluggable storage (PostgreSQL, SQLite, S3, memory)

<br>

[![License](https://img.shields.io/badge/License-Apache-165dfc.svg)](https://github.com/nisshi-io/solace-nisshi/blob/main/LICENSE)
&nbsp;
[![Built with Rust](https://img.shields.io/badge/built_with-Rust-165dfc.svg?logo=rust)](https://www.rust-lang.org/)
&nbsp;
<br>
[![Docs](https://img.shields.io/badge/📖%20docs-docs.nisshi.io-165dfc.svg)](https://docs.nisshi.io/)
&nbsp;
[![Blog](https://img.shields.io/badge/%F0%9F%93%98%20blog-blog.nisshi.io-165dfc.svg)](https://blog.nisshi.io/articles)
&nbsp;
[![Community](https://img.shields.io/badge/%F0%9F%92%AC%20community-Solace%20forum-165dfc.svg)](https://community.solace.com/c/nisshi-diskless-streaming/19)
&nbsp;
<br>
[![GitHub stars](https://img.shields.io/github/stars/nisshi-io/solace-nisshi?style=social)](https://github.com/nisshi-io/solace-nisshi)

<br>

</div>

The full documentation is at [docs.nisshi.io](https://docs.nisshi.io/). This README and the pages in [docs](docs) describe `main`, which can be ahead of the [latest release][nisshi-latest-release].

# What is Nisshi?

[Nisshi][github-com-nisshi-io] implements the Apache Kafka® API, so applications can use their Kafka clients
without a cluster of Kafka brokers to run. Nisshi brokers are stateless: they keep no data of their own,
and the storage engine holds topics, messages and consumer groups, so brokers that share an engine can
each answer any request.

Features:

- Implements the Apache Kafka® API
- Available with [PostgreSQL](https://www.postgresql.org), [libSQL](https://docs.turso.tech/libsql), [S3](https://en.wikipedia.org/wiki/Amazon_S3) or memory storage engines
- Topics [validated](docs/schema-registry.md) by [JSON Schema][json-schema-org], [Apache Avro](https://avro.apache.org)
  or [Protocol buffers][protocol-buffers] can be written as [Apache Iceberg](https://iceberg.apache.org) or [Delta Lake](https://delta.io) tables

## About

Nisshi is now part of Solace. Its creator, Peter Morgan, is a key member of the Solace team that works on it. [GOVERNANCE][nisshi-governance] says how the project is run.

The license stays Apache 2.0, and what is open stays open.

Nisshi OSS is provided as-is, with no warranty and no Solace Support commitment. Ask questions in the [Solace community][nisshi-community]. Report security issues through [SECURITY][nisshi-security].

## Get Started

> [!NOTE]
> Want the quickest way to start using Nisshi? Use a prebuilt binary. See [Get Started with Nisshi](docs/quickstart.md).

Other ways to run Nisshi, both described in [Install and run Nisshi](docs/install.md):

- Docker Compose — if you have Docker installed. See [Running with Docker Compose](docs/install.md#running-with-docker-compose).
- cargo — if you have Rust installed, or there is no prebuilt binary for your platform, such as Windows or an Intel Mac. See [Installing with crates.io](docs/install.md#installing-with-cratesio).

To see Nisshi write topics to a data lake, try the examples for [pyiceberg](https://github.com/nisshi-io/example-pyiceberg), [Apache Spark](https://github.com/nisshi-io/example-spark), or [Delta Lake](https://github.com/nisshi-io/example-delta-lake).

## Commands

Nisshi is a single statically linked binary containing the following, described in [Commands](docs/commands.md):

- [broker](docs/commands.md#broker) — an Apache Kafka®-compatible broker and schema registry
- [topic](docs/commands.md#topic) — create, list or delete topics (development and demos; plaintext only: no TLS or SASL)
- [user](docs/security.md#authentication) — create, list or delete users (development and demos; plaintext only: no TLS or SASL)
- [cat](docs/commands.md#cat) — consume or produce Avro, JSON or Protobuf messages to a topic (development and demos; plaintext only: no TLS or SASL)
- generator — generate fake messages for schema-backed topics (development and demos; plaintext only: no TLS or SASL)
- perf — measure produce and consume throughput against a broker (development and demos; plaintext only: no TLS or SASL)
- [proxy](docs/commands.md#proxy) — an Apache Kafka®-compatible proxy (development and demos, not production)

## Documentation

- [Get Started with Nisshi](docs/quickstart.md) — start a broker from a prebuilt binary, and send and read a message
- [Install and run Nisshi](docs/install.md) — which release to use, installing with cargo, and running with Docker Compose
- [Storage options](docs/storage.md) — the storage engines, their status, and running them with Docker Compose
- [Commands](docs/commands.md) — the `broker`, `topic`, `cat` and `proxy` commands
- [Topic configuration](docs/topic-configuration.md) — the topic settings Nisshi supports
- [Schema registry](docs/schema-registry.md) — validating topics with Avro, JSON Schema or Protocol Buffers
- [Securing a broker](docs/security.md) — TLS, outbound TLS, and SASL SCRAM authentication
- [Metrics](docs/metrics.md) — exporting metrics over OTLP
- [Compatibility and feature status](docs/compatibility.md) — the Kafka APIs, storage engine features and Kafka clients that are supported
- [Architecture](ARCHITECTURE.md) — how the crates fit together, for contributors
- [CHANGELOG](CHANGELOG.md) — what changed in each release, and the upgrade notes

## Compatibility

Nisshi implements the Apache Kafka® wire protocol, generated from the Apache Kafka 3.9.1
message descriptors in [`nisshi-sans-io/message`](nisshi-sans-io/message). Clients on
Kafka 4.x negotiate down to those protocol versions. Idempotent producers work. Transactions are
not supported, so Kafka Streams with `exactly_once_v2` fails.

For the details, see [Compatibility and feature status](docs/compatibility.md). It lists which
Kafka APIs and topic configurations each storage engine supports, and the client versions CI tests
along with their known gaps. Please [raise an issue][nisshi-issues] if a client or Kafka version
you rely on behaves differently.

## Feedback

Please [raise an issue][nisshi-issues] if you encounter a problem.

## Contributing

Contributions are welcome. See [CONTRIBUTING][nisshi-contributing] for how to get started,
and [GOVERNANCE][nisshi-governance] for how decisions get made. Everyone participating is
expected to follow our [Code of Conduct][nisshi-code-of-conduct]. To report a security
vulnerability, see [SECURITY][nisshi-security].

## License

Nisshi is licensed under [Apache 2.0][apache-license].

Apache, Apache Kafka, Kafka and the other Apache project names in this document are
trademarks of The Apache Software Foundation.

[apache-license]: https://www.apache.org/licenses/LICENSE-2.0
[github-com-nisshi-io]: https://github.com/nisshi-io/solace-nisshi
[json-schema-org]: https://json-schema.org/
[protocol-buffers]: https://protobuf.dev
[nisshi-issues]: https://github.com/nisshi-io/solace-nisshi/issues
[nisshi-community]: https://community.solace.com/c/nisshi-diskless-streaming/19
[nisshi-latest-release]: https://github.com/nisshi-io/solace-nisshi/releases/latest
[nisshi-contributing]: CONTRIBUTING.md
[nisshi-code-of-conduct]: CODE_OF_CONDUCT.md
[nisshi-security]: SECURITY.md
[nisshi-governance]: GOVERNANCE.md
