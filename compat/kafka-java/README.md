# Kafka Java integration tests

Runs Apache Kafka 3.9.1's own integration tests (`core/src/test/scala/integration`) against a
Nisshi broker. The tests use the real Java producer, consumer and admin client.

[`FINDINGS.md`](FINDINGS.md) has the results of running every class. `just compat-kafka-java`
runs the classes in `tests.allow` and fails if a test in it fails or does not run. CI runs it on the
memory engine as an experimental job, which does not fail `ci-gate`.

## Running

```shell
just compat-kafka-java    # the tests in tests.allow, builds the broker first, about 10 minutes
```

The same recipe runs in CI as the `compat-kafka-java` job, on the memory engine. The job has
`continue-on-error`, so it does not fail `ci-gate` and shows green in the checks list. Read its
verdict in the job's step summary (a PASS or FAIL heading, the `allow-list:` lines and one row for
each class). To reproduce a failure, run `just compat-kafka-java` on a machine with Docker. The
`compat-kafka-java-results-memory` artifact of the run holds the results CSV, and a log, a broker
log (`broker-<Class>.log`) and a JUnit XML for each class, which has the failing assertion. The job
becomes a required check after 10 green runs in a row.

To run more than the allow-list, build the broker, then run the script directly:

```shell
cargo build --features dynostore -p nisshi
./compat/kafka-java/run.sh                              # every class, about 3 hours
./compat/kafka-java/run.sh PlaintextConsumerCommitTest  # one class
ALLOW_LIST_ONLY=1 ./compat/kafka-java/run.sh            # what the recipe runs; takes no class names
```

`run.sh` needs Docker, and `target/debug/nisshi` built as above. It builds the tests in a
`temurin:17` container, because Kafka 3.9.1's Gradle does not run on a newer JDK, and starts a
fresh broker for each class. Its environment variables:

- `STORAGE_ENGINE`: the broker's storage engine URL (default `memory://`).
- `ALLOW_LIST_ONLY`: `1` to run only the classes in `tests.allow`, and exit with 1 if a test in it
  fails or does not run.
- `BROKER_HOST`: the address of this machine as the container sees it. On Linux the container
  shares the host network and the default is `127.0.0.1`. Under colima the default is 192.168.5.2,
  and under Docker Desktop, set `BROKER_HOST=host.docker.internal`.
- `SECONDS_PER_CLASS` and `RESULTS_FILE`: the time allowed for one class, and the results CSV.

Under colima, `run.sh` first sets the VM clock to the host's, because a 2 s skew hangs the
`LogAppendTime` tests. A skew of tens of milliseconds still fails three of them, which are off the
allow-list.

The results go to `target/compat/kafka-java.csv`, with a log, a broker log and a JUnit XML per
class in `target/compat/kafka-java/`.

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
