# Kafka Java integration tests

Runs Apache Kafka 3.9.1's own integration tests (`core/src/test/scala/integration`) against a
Nisshi broker. The tests use the real Java producer, consumer and admin client.

This directory is a spike. CI and `just` do not run it yet. [`FINDINGS.md`](FINDINGS.md) has the
results and the recommendation.

## Running

```shell
cargo build --features dynostore -p nisshi
./compat/kafka-java/run.sh                              # every class, about 3 hours
./compat/kafka-java/run.sh PlaintextConsumerCommitTest  # one class
```

`run.sh` needs Docker, and `target/debug/nisshi` built as above. It builds the tests in a `temurin:17` container, because Kafka 3.9.1's
Gradle does not run on a newer JDK. It starts a fresh `memory://` broker for each class. Under
colima the container reaches the broker at 192.168.5.2 (the default of `BROKER_HOST`); under
Docker Desktop, set `BROKER_HOST=host.docker.internal`.

Under colima, `run.sh` first sets the VM clock to the host's, because a 2 s skew hangs the
`LogAppendTime` tests. A skew of tens of milliseconds still fails three of them, which are off the
allow-list.

The results go to `target/compat/kafka-java.csv`, with a log and a JUnit XML per class in
`target/compat/kafka-java/`.

## The harness patch

`harness.patch` applies to the `3.9.1` tag. When `NISSHI_BOOTSTRAP` is set, the test harness:

- starts no embedded brokers or quorum, and returns that address from `bootstrapServers()`;
- creates topics through the admin client and waits for a leader, and deletes every topic after
  each test, because Nisshi outlives the test;
- skips the ZooKeeper and KIP-848 (`groupProtocol=consumer`) variants, through a JUnit
  `ExecutionCondition`;
- fails a test that runs longer than 90 s, because many Kafka tests wait on a future without a limit.

`tests.allow` lists the 108 test methods that pass reliably. Of the 1,272 tests in 95 classes,
188 pass, 408 fail and 676 are skipped as not applicable. `FINDINGS.md` explains the failures.
