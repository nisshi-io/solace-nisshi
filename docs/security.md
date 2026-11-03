# Securing a broker

TLS on the listener, TLS on the connections the broker opens, and SASL SCRAM authentication.

## TLS

The listener speaks TLS when both `--cert` (the certificate chain) and `--key`
(the private key) are supplied as PEM files, and clap rejects one without the other:

```shell
nisshi broker --cert broker.pem --key broker-key.pem
```

With TLS configured the listener is TLS only: plaintext clients are refused
during the handshake, so clients must be configured with `security.protocol=SSL`
(and a truststore containing the certificate, if it is self-signed). The
`--listener-url` keeps its `tcp://` scheme. TLS here provides encryption only;
client authentication is still SASL (see `--authentication`). The private key
is a PKCS#8, SEC1 or RSA PEM key. `--cert` and `--key` may point at the same
file when the certificate chain and key are kept in one PEM bundle. Any
problem loading the certificate or key, or a key that does not match the
certificate, fails startup rather than falling back to plaintext.

A passphrase protected key (the counterpart of Kafka's `ssl.key.password`) is
read with `--key-passphrase-file`, a file holding the passphrase; a trailing
newline is ignored:

```shell
nisshi broker --cert broker.pem --key broker-key.pem --key-passphrase-file broker-key.passphrase
```

Only PKCS#8 encryption (`ENCRYPTED PRIVATE KEY`, PBES2 with PBKDF2-HMAC-SHA2 or
scrypt and AES-CBC or Triple DES, which is what `openssl pkcs8 -topk8` emits) is
supported. Legacy OpenSSL PEM encryption (`Proc-Type: 4,ENCRYPTED`) and keys
derived with a SHA-1 PRF (older OpenSSL releases' default) are rejected at
startup, naming the algorithm; re-encrypt the key first, keeping the passphrase:

```shell
openssl pkcs8 -topk8 -in broker-key.pem -out broker-key-pkcs8.pem -v2 aes-256-cbc -v2prf hmacWithSHA256
```

An empty passphrase file means no passphrase, so a mounted secret that is
absent for unencrypted keys works unchanged. A passphrase given for a key that
is not encrypted is ignored with a warning.

Note for existing deployments: before 0.7 these flags were accepted but had no
effect, so a broker started with `--cert` and `--key` was serving plaintext.
After upgrading, that same command line serves TLS only, and `--cert` without
`--key` (or the reverse) is rejected. The bundled `nisshi cat`, `topic`, `perf`
and `proxy` subcommands connect in plaintext and cannot yet talk to a TLS
listener. `--key-passphrase-file` is unknown to earlier releases, so rolling
the binary back means removing that flag from the command line as well.

## Outbound TLS

The broker opens connections to PostgreSQL, S3 and Google Cloud Storage,
Iceberg catalogs and data files, Delta tables, an OTLP collector, and the
remote `$ref`s of a JSON Schema. Each connection that uses TLS checks the
server's certificate chain and host name, and a failed check fails the
connection: the broker does not retry it in plaintext. A connection to an
`http://` endpoint, such as an `http://` OTLP or Iceberg catalog URL, or an
`AWS_ENDPOINT` with `AWS_ALLOW_HTTP=true`, uses no TLS.

- **Linux and the container image.** The certificates are checked against the
  system's CA certificates (`/etc/ssl/certs/ca-certificates.crt` in the image),
  and rustls-webpki also rejects a chain through any CA on its built-in
  distrust list. To trust a private CA, set `SSL_CERT_FILE` to a PEM bundle or
  `SSL_CERT_DIR` to a directory of PEM files. Either one replaces the system's
  CA certificates, so the bundle must also hold every public CA the broker
  needs. If the file or directory doesn't exist or holds no PEM certificate,
  the broker trusts no CA, and every TLS connection fails its certificate
  check.
- **macOS.** The trusted CAs come from the operating system. rustls-webpki
  checks the PostgreSQL, Iceberg REST catalog and Delta S3 connections against
  those CAs, with its built-in distrust list. macOS itself checks every other
  connection, including the S3 and Google Cloud Storage storage engines, and
  rustls-webpki's distrust list does not apply to them. Those connections
  rely on the distrust updates that Apple pushes, so keep the Mac online and
  up to date.

A PostgreSQL connection uses TLS only when the server agrees to it. The
default `sslmode` is `prefer`, which uses plaintext when the server, or
anything on the network path, declines TLS. Add `sslmode=require` to the
storage URL to refuse a plaintext connection:

```shell
nisshi broker --storage-engine "postgres://nisshi:secret@db.example.com/nisshi?sslmode=require"
```

`require` checks the server's certificate and host name, as libpq does with
`verify-full`. The broker refuses to start with `sslmode=verify-ca` or
`sslmode=verify-full`, so use `require` instead. If the server's certificate
comes from a CA that the system doesn't trust, such as a cloud provider's
database CA, set `SSL_CERT_FILE` to a bundle that holds that CA and the public
CAs.

The object store clients read `AWS_ALLOW_INVALID_CERTIFICATES`,
`GOOGLE_ALLOW_INVALID_CERTIFICATES`, `AZURE_ALLOW_INVALID_CERTIFICATES` and
`ALLOW_INVALID_CERTIFICATES` from the environment. When one of them is true,
that client accepts any certificate, so anyone on the network path can read
and change the data. These variables are for testing only and are not
supported. The broker logs a warning at startup for each one that is set to
true.

## Authentication

With `--authentication`, the broker requires each client to log in with SASL
SCRAM-SHA-256 or SCRAM-SHA-512. Before a client logs in, the broker answers
only ApiVersions, SaslHandshake and SaslAuthenticate, and it closes the
connection on any other request, as a Kafka SASL listener does. SCRAM does not
encrypt the connection, so use it with [TLS](#tls) when the network is not
trusted. Nisshi does not have an authorizer, so each user who logs in can use
every API, including the ones that create and delete topics and users.

The broker keeps its users in the storage engine. `--authentication` therefore
needs an engine that can store them, such as SQLite (`sqlite://`) or
PostgreSQL (`postgres://`). On an engine that can't, such as `memory://` or
`s3://`, the broker refuses to start and names the engines that can.

The `nisshi` commands, such as `user`, `topic` and `cat`, don't log in yet.
Against a broker with `--authentication` they fail with `error: Connected to
the broker, but it closed the connection`.

To create the first user, run the broker on the same storage without
`--authentication`, and then restart it:

1. Start the broker without `--authentication`. Without it, any client that
   can reach the broker can use it, so listen on `127.0.0.1` only:

   ```shell
   nisshi broker --storage-engine sqlite://nisshi.db --listener-url tcp://127.0.0.1:9092
   ```

2. In another shell, create the user. `--mechanism` is `scram256` or
   `scram512` (the default). The broker stores a credential for each
   mechanism, so create the user with the mechanism that its clients use, or
   once for each:

   ```shell
   nisshi user create alice alice-secret --mechanism scram256
   ```

   The password is an argument, so it is in your shell history, and other
   users of the machine can see it while the command runs.

3. Stop the broker, and start it again with `--authentication`:

   ```shell
   nisshi broker --storage-engine sqlite://nisshi.db --authentication
   ```

Until the `nisshi` commands support SASL, manage users and topics on a broker
with `--authentication` with the Apache Kafka® command line tools and a client
configuration file:

```properties
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-256
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="alice" password="alice-secret";
```

```shell
kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --command-config client.properties --list
kafka-configs.sh --bootstrap-server 127.0.0.1:9092 --command-config client.properties \
  --alter --entity-type users --entity-name bob --add-config 'SCRAM-SHA-256=[password=bob-secret]'
```

With TLS on the listener, set `security.protocol=SASL_SSL` instead.
