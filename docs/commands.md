# Commands

Nisshi is a single statically linked binary containing the following:

- [broker](#broker) — an Apache Kafka®-compatible broker and schema registry
- [topic](#topic) — create, list or delete topics
- [user](security.md#authentication) — create, list or delete users
- [cat](#cat) — consume or produce Avro, JSON or Protobuf messages to a topic
- generator — generate fake messages for schema-backed topics (development and demos)
- perf — measure produce and consume throughput against a broker (development and demos)
- [proxy](#proxy) — an Apache Kafka®-compatible proxy (development and demos, not production)

## broker

The broker subcommand is default if no other command is supplied.

```shell
Usage: nisshi [OPTIONS]
       nisshi <COMMAND>

Commands:
  broker     Apache Kafka®-compatible broker with Avro, JSON, Protobuf schema validation [default if no command supplied]
  cat        Consume or produce Avro, JSON or Protobuf messages to a topic
  generator  Generate fake messages for schema-backed topics (development and demos)
  perf       Measure produce and consume throughput against a broker (development and demos)
  proxy      Apache Kafka®-compatible proxy (development and demos)
  topic      Create, list or delete topics managed by the broker
  user       Create, list or delete users managed by the broker
  help       Print this message or the help of the given subcommand(s)

Options:
      --kafka-cluster-id <KAFKA_CLUSTER_ID>
          All members of the same cluster should use the same id [env: CLUSTER_ID=RvQwrYegSUCkIPkaiAZQlQ] [default: nisshi_cluster]
      --kafka-listener-url <KAFKA_LISTENER_URL>
          The broker will listen on this address [env: LISTENER_URL=] [default: tcp://[::]:9092]
      --kafka-advertised-listener-url <KAFKA_ADVERTISED_LISTENER_URL>
          This location is advertised to clients in metadata [env: ADVERTISED_LISTENER_URL=tcp://127.0.0.1:9092] [default: tcp://127.0.0.1:9092]
      --storage-engine <STORAGE_ENGINE>
          Storage engine examples are: postgres://postgres:postgres@localhost, memory://nisshi/ or s3://nisshi/ [env: STORAGE_ENGINE=s3://nisshi/] [default: memory://nisshi/]
      --schema-registry <SCHEMA_REGISTRY>
          Schema registry examples are: file://./etc/schema or s3://nisshi/, containing: topic.json, topic.proto or topic.avsc [env: SCHEMA_REGISTRY=file://./etc/schema]
  -h, --help
          Print help
  -V, --version
          Print version
```

A broker can be started by simply running `nisshi`, all options have defaults. Nisshi pickup any existing environment,
loading any found in `.env`. An [example.env](../example.env) is provided as part of the distribution
and can be copied into `.env` for local modification. Sample schemas can be found in [etc/schema](../etc/schema), used in the examples.

If an Apache Avro, Protobuf or JSON schema has been assigned to a topic, the
broker will reject any messages that are invalid. Schema backed topics are written
as Apache Parquet when the broker runs with the `parquet` subcommand, for example
`nisshi broker --schema-registry file://./etc/schema parquet --location file://./lake`
(`--location` can also come from `DATA_LAKE`). A `file://` location must be an existing directory.
The `iceberg` and `delta` subcommands write Apache Iceberg and Delta Lake tables;
run `nisshi broker iceberg --help` or `nisshi broker delta --help` for their options.

## topic

Nisshi settings for a topic are listed in [topic configuration](topic-configuration.md).

The `nisshi topic` command has the following subcommands:

```shell
nisshi topic --help
Create, list or delete topics managed by the broker

Usage: nisshi topic <COMMAND>

Commands:
  create  Create a topic
  delete  Delete an existing topic
  list    List existing topics
  help    Print this message or the help of the given subcommand(s)

Options:
  -h, --help
          Print help
```

To create a topic use:

```shell
nisshi topic create taxi
```

The command prints the topic and its number of partitions:

```shell
Created topic taxi with 3 partitions.
```

`nisshi topic create` and `nisshi cat produce` exit with status 0 on success,
and with a non-zero status on failure. A script that only checks the exit
status can pass `--silent`, so that the command prints no confirmation line.
Errors still print.

## cat

The `nisshi cat` command, has the following subcommands:

```shell
nisshi cat --help
Easily consume or produce Avro, JSON or Protobuf messages to a topic

Usage: nisshi cat <COMMAND>

Commands:
  produce  Produce Avro/JSON/Protobuf messages to a topic
  consume  Consume Avro/JSON/Protobuf messages from a topic
  help     Print this message or the help of the given subcommand(s)

Options:
  -h, --help  Print help
```

The `produce` subcommand reads JSON formatted messages encoding them into
Apache Avro, Protobuf or JSON depending on the schema used by the topic.

For example, the `taxi` topic is backed by [taxi.proto](../etc/schema/taxi.proto).
Using [trips.json](../etc/data/trips.json) containing a JSON array of objects,
`nisshi cat produce` encodes each message into protobuf into the broker:

```
nisshi cat produce taxi etc/data/trips.json
```

The command prints the number of messages and the offsets that the broker
gave them:

```shell
Produced 4 messages to taxi partition 0, offsets 0 to 3.
```

Using [duckdb](https://duckdb.org) we can read the
[Apache Parquet](https://parquet.apache.org) files
created by the broker:

```shell
duckdb :memory: "SELECT * FROM 'data/taxi/*/*.parquet'"
```

Results in the following output:

```shell
|-----------+---------+---------------+-------------+---------------|
| vendor_id | trip_id | trip_distance | fare_amount | store_and_fwd |
|     int64 |   int64 |         float |      double |         int32 |
|-----------+---------+---------------+-------------+---------------|
|         1 | 1000371 |           1.8 |       15.32 |             0 |
|         2 | 1000372 |           2.5 |       22.15 |             0 |
|         2 | 1000373 |           0.9 |        9.01 |             0 |
|         1 | 1000374 |           8.4 |       42.13 |             1 |
|-----------+---------+---------------+-------------+---------------|
```

## Connecting to a broker with TLS or SASL

`topic`, `user`, `cat`, `generator` and `perf` connect in plaintext and don't log in, unless
you give them the options below. They share these options, and each reads the matching
environment variable when the option is absent. The `.env` file is read too.

| Option | Environment variable | |
| --- | --- | --- |
| `--tls` | | Connect over TLS, trusting the system's CA certificates (`SSL_CERT_FILE` or `SSL_CERT_DIR` replace them). Each `--tls-*` option below also turns TLS on |
| `--tls-ca-file <PEM>` | `TLS_CA_FILE` | The CA certificates that sign the broker's certificate, in place of the system's |
| `--tls-cert-file <PEM>`, `--tls-key-file <PEM>` | `TLS_CERT_FILE`, `TLS_KEY_FILE` | The client certificate chain and private key, for a broker that asks for one. Give both |
| `--tls-key-passphrase-file <file>` | `TLS_KEY_PASSPHRASE_FILE` | The passphrase of an encrypted PKCS#8 key, as for the broker's `--key-passphrase-file` |
| `--tls-server-name <name>` | `TLS_SERVER_NAME` | Check the broker's certificate against this name instead of the host of `--broker` |
| `--tls-no-hostname-check` | | Accept a certificate for another name. The certificate chain is still checked. This is Kafka's empty `ssl.endpoint.identification.algorithm`. Without `--tls-ca-file`, the system's CA certificates sign the chain, so a certificate that any public CA issued for any name passes, and the command prints a warning. Give `--tls-ca-file` with this option |
| `--sasl-username <name>` | `SASL_USERNAME` | Log in with SASL as this user |
| | `SASL_PASSWORD` | The password |
| `--sasl-password-file <file>` | `SASL_PASSWORD_FILE` | A file with the password (a trailing newline is not part of it), in place of `SASL_PASSWORD`. Setting both is an error |
| `--sasl-mechanism <mechanism>` | `SASL_MECHANISM` | `plain`, `scram-sha-256` or `scram-sha-512` (the default) |

These variable names are the same ones that other Kafka tools and CI setups use. A shell or
`.env` that already sets `SASL_USERNAME` makes every one of these commands log in, and one that
sets `TLS_CA_FILE` makes them connect over TLS, even when you pass no option. Unset the variable
for a command that must connect in plaintext.

There is no option for the password itself, because an option is in your shell history and
in the process list. `SASL_PASSWORD` and `SASL_USERNAME` together are Kafka's `SASL_SSL` with
`--tls`, and `SASL_PLAINTEXT` without it.

```shell
read -rs SASL_PASSWORD && export SASL_PASSWORD
nisshi topic list --broker tcp://broker.example.com:9092 \
  --tls-ca-file ca.pem --sasl-username alice --sasl-mechanism scram-sha-256
```

Nisshi's own broker enables SCRAM-SHA-256 and SCRAM-SHA-512 only. `plain` is for another
Kafka broker that enables it, and only with TLS, because PLAIN sends the password in clear text.
The mechanism name is not case sensitive, so Kafka's `SCRAM-SHA-512` works too. A SCRAM login
stops when the broker asks for fewer than 4096 iterations, which is the minimum that Kafka's
client enforces, or for more than 65,536, which is four times the most that a Kafka broker
asks for. A user that `nisshi user create --iterations` made with a count outside that range
cannot log in with these commands. Each connection
logs in once, as it opens, and a broker that limits the session lifetime has the command open a
new connection, which logs in again, a little before the limit. `proxy` has none of these
options.

If the TLS handshake or the login fails, the command says so, and which option to check.

## proxy

The `nisshi proxy` command runs a Kafka API proxy in front of an origin broker:

```shell
nisshi proxy --help
Apache Kafka®-compatible proxy

Usage: nisshi proxy [OPTIONS]

Options:
      --listener-url <LISTENER_URL>
          The proxy will listen on this address [env: LISTENER_URL=] [default: tcp://0.0.0.0:9092]
      --advertised-listener-url <ADVERTISED_LISTENER_URL>
          This location is advertised to clients in metadata [env: ADVERTISED_LISTENER_URL=] [default: tcp://127.0.0.1:9092]
      --origin-url <ORIGIN_URL>
          The proxy will forward traffic to this origin broker, which must not require authentication [env: ORIGIN_URL=] [default: tcp://127.0.0.1:9092]
      --otlp-endpoint-url <OTLP_ENDPOINT_URL>
          OTEL Exporter OTLP endpoint [env: OTEL_EXPORTER_OTLP_ENDPOINT=]
  -h, --help
          Print help
```

To proxy a broker that listens on `localhost:19092`:

```shell
nisshi proxy --origin-url tcp://localhost:19092
```

The proxy does not authenticate its clients, and it connects to the origin
over plain TCP without SASL. So the origin listener must not require
authentication, as with a Kafka `PLAINTEXT` listener. Every client that can
reach the proxy listener gets the proxy's access to the origin.

The proxy is for development and demos, not for production, and that includes
a proxy in front of a broker on PostgreSQL. A Postgres broker limits each
connection to its share of the Fetch and ListOffsets reads in flight. An
abandoned read is one that its client no longer waits for, but that the
database is still running, and it counts against its connection's share until
the statement ends. So one connection's abandoned reads are limited to its
share. The proxy takes an origin connection from its pool for each request,
so behind it the share bounds no single client: one client's abandoned reads
can spread over many origin connections and take up to the broker's whole
limit, and other clients' reads then wait. Nisshi does not isolate proxied
clients from each other here.

Behind the proxy, this shows as reads that miss their deadline: Fetch
responses with no records, and `REQUEST_TIMED_OUT` for ListOffsets
partitions. The `nisshi_storage_read_deadline_exceeded` metric rises with
`stage` set to `queued` and `limit` set to `engine`. A larger `pool_max_size`
does not fix this, because one client can still take the whole limit.

The proxy answers SaslHandshake and SaslAuthenticate itself with
`ILLEGAL_SASL_STATE`, as a Kafka `PLAINTEXT` listener does, and does not
forward them to the origin. So a client configured for SASL fails to connect.
