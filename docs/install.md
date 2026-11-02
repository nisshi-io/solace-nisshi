# Install and run Nisshi

The quickest way to start is a prebuilt binary: see [Get Started with Nisshi](quickstart.md).
This page covers choosing a release, installing with cargo, and running with Docker Compose.

## Which release to use

[![Latest release](https://img.shields.io/github/v/release/nisshi-io/solace-nisshi?label=latest%20release)][nisshi-latest-release]

Use the [latest release][nisshi-latest-release], the one marked **Latest** on the Releases page. `cargo install nisshi` and the `latest` container image both skip pre-releases.

Releases marked **Pre-release**, with tags such as `v0.8.0-pre.1`, are builds of `main` published ahead of the next release. Their behaviour and storage formats can still change, and a later version may not read the data they wrote (upgrading from 0.7.0-pre.1 or pre.2 to 0.7.0 needs the manual steps in the 0.7.0 upgrade notes). Use them for testing, not for data you want to keep. To install one, name its exact version, for example `ghcr.io/nisshi-io/nisshi:0.8.0-pre.1`.

Upgrading? Read the upgrade notes in [CHANGELOG.md](../CHANGELOG.md) for every release after the one you run. The default cluster id and the Iceberg and Delta namespace changed in 0.7.0, and the topic configuration keys changed in 0.8.0.

This page describes `main`, which can be ahead of the latest release. To read the documentation of a release, open it at its tag, for example [v0.7.0](https://github.com/nisshi-io/solace-nisshi/blob/v0.7.0/README.md).

## Installing with crates.io

To build and install Nisshi from [crates.io](https://crates.io/crates/nisshi), run:

```shell
cargo install nisshi --locked
```

The build takes several minutes. It compiles some C libraries as well as the Rust code,
so it needs:

- Rust, installed with [rustup](https://rustup.rs). The project builds and tests with
  the Rust version in [rust-toolchain.toml](../rust-toolchain.toml).
- A C compiler. On macOS, install the Xcode Command Line Tools with
  `xcode-select --install`. On Debian or Ubuntu, run
  `sudo apt-get install build-essential`.

`--locked` builds with the dependency versions in the release's `Cargo.lock`, instead of
the newest compatible versions.

The default build includes the `memory://`, `s3://`, `gs://`, `sqlite://`, and `postgres://` storage engines.
To add another storage engine or a data lake format, see [Storage options](storage.md).

To check the install, run `nisshi --help`. Its last lines list the storage engines in
the build:

```shell
Storage engines: dynostore, libsql, postgres
Data lakes:
```

By default, the broker accepts connections from other computers on your network, and does not require clients to authenticate.
To start a broker that only listens to your computer, run:

```shell
nisshi --listener-url tcp://127.0.0.1:9092
```

The broker prints `ready` when it starts, and after that logs only warnings and errors. To see each request, start it with `RUST_LOG=nisshi_service::frame=debug`. That setting is verbose: it logs each request's API key as a number, and the whole request and response. The `RUST_LOG` setting in [example.env](../example.env) adds debug logs from the broker, storage and schema crates, but not one for each request.

To protect a broker that other computers connect to, see [Securing a broker](security.md) for TLS and authentication.

The broker keeps topics and messages in memory, and loses them when it stops. To keep them, see [Storage options](storage.md).

In a second terminal, create a topic, send a message, and read it:

```shell
nisshi topic create hello
echo '{"value": "hello world"}' | nisshi cat produce hello
nisshi cat consume hello
```

The `cat consume` command prints the message:

```json
[{"key":null,"value":"hello world"}]
```

To see the broker's topics and their partition counts, run `nisshi topic list`:

```text
TOPIC  PARTITIONS
hello  3
```

`nisshi topic list --output json` prints the broker's topic metadata as JSON, for scripts. Releases up to 0.8.1 print that JSON by default and don't accept `--output`.

Releases before 0.8.0 print nothing for `topic create` and `cat produce`, so on those releases only the last command prints anything.

## Running with Docker Compose

Run the broker with [Docker Compose](https://docs.docker.com/compose/) from a
clone of this repository. You don't need a `.env`: by default,
[compose.yaml](../compose.yaml) runs the latest release on PostgreSQL.

```shell
docker compose up -d nisshi
```

This starts PostgreSQL and the broker, which listens on `localhost:9092`.
Compose publishes their ports on 127.0.0.1 only, so other computers can't connect.

If PostgreSQL already runs on your computer, port 5432 is taken and the command
fails with "port is already allocated" or "address already in use". Publish the
`db` service on another port:

```shell
NISSHI_DB_PORTS=127.0.0.1:15432:5432 docker compose up -d nisshi
```

To create a topic, produce a message to it, and consume it:

```shell
docker compose exec nisshi /nisshi topic create hello
echo '{"value":"hello world"}' | docker compose exec -T nisshi /nisshi cat produce hello
docker compose exec nisshi /nisshi cat consume hello
```

The consume command prints the message:

```json
[{"key":null,"value":"hello world"}]
```

If a command fails, `docker compose ps` shows whether the broker is running,
and `docker compose logs nisshi` shows why it stopped.

Stop the broker and PostgreSQL with `docker compose down`. Add `--volumes` to
also delete the stored messages.

Next steps:

- Connect any Kafka client to `localhost:9092`.
- Use another storage engine: set `STORAGE_ENGINE` in a `.env` file, for example `STORAGE_ENGINE=memory://nisshi/`. [example.env](../example.env) lists the other settings, and it selects `s3://nisshi/`, which needs the bucket that [Running Docker Compose with S3 (MinIO)](storage.md#running-docker-compose-with-s3-minio) creates.
- Read the documentation at [docs.nisshi.io](https://docs.nisshi.io/).

[nisshi-latest-release]: https://github.com/nisshi-io/solace-nisshi/releases/latest
