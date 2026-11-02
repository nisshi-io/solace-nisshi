# Nisshi documentation

These pages describe `main`, which can be ahead of the latest release. The full documentation
is at [docs.nisshi.io](https://docs.nisshi.io/).

- [Get Started with Nisshi](quickstart.md) — start a broker from a prebuilt binary, and send and read a message
- [Install and run Nisshi](install.md) — which release to use, installing with cargo, and running with Docker Compose
- [Storage options](storage.md) — the storage engines, their status, and running them with Docker Compose
- [Commands](commands.md) — the `broker`, `topic`, `cat` and `proxy` commands
- [Topic configuration](topic-configuration.md) — the topic settings Nisshi supports
- [Schema registry](schema-registry.md) — validating topics with Avro, JSON Schema or Protocol Buffers
- [Securing a broker](security.md) — TLS, outbound TLS, and SASL SCRAM authentication
- [Metrics](metrics.md) — exporting metrics over OTLP
- [Compatibility and feature status](compatibility.md) — the Kafka APIs, storage engine features and Kafka clients that are supported
- [Sarama](sarama.md) — using the Sarama console tools with Nisshi

For how the code is organised, see [ARCHITECTURE.md](../ARCHITECTURE.md). Upgrade notes for each
release are in [CHANGELOG.md](../CHANGELOG.md).
