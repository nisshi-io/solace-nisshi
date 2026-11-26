#!/usr/bin/env bash
# Runs Apache Kafka 3.9.1's own integration tests (core/src/test/scala/integration) against a
# Nisshi broker, one test class at a time, each on a fresh broker.
#
# Usage: run.sh [ClassName ...]   (default: every class under core/src/test/scala/integration)
#
# Needs: docker, and a built ./target/debug/nisshi (cargo build --features dynostore -p nisshi).
# Env:   BROKER_HOST   address of this machine as the docker container sees it (default 192.168.5.2,
#                      colima; on Docker Desktop use host.docker.internal)
#        SECONDS_PER_CLASS seconds allowed for one class (default 900)
#        RESULTS_FILE  CSV of class,tests,failures,skipped,seconds (default target/compat/kafka-java.csv)
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/../.." && pwd)
kafka_version=3.9.1
patched_kafka="$root/target/compat/kafka-java/kafka-$kafka_version"
broker_host=${BROKER_HOST:-192.168.5.2}
seconds_per_class=${SECONDS_PER_CLASS:-900}
results=${RESULTS_FILE:-$root/target/compat/kafka-java.csv}
gradle_home="$root/target/compat/kafka-java/gradle-home"
mkdir -p "$gradle_home" "$(dirname "$results")"

# The tests run from a separate worktree with the harness patch applied, so that kafka-ref/
# stays at the clean tag that `just kafka-ref` checks for.
if [[ ! -d "$patched_kafka" ]]; then
    just -f "$root/justfile" kafka-ref
    mkdir -p "$(dirname "$patched_kafka")"
    git -C "$root/kafka-ref" worktree add --detach "$patched_kafka" "refs/tags/$kafka_version" >/dev/null
    git -C "$patched_kafka" apply "$here/harness.patch"
fi

# The test JVM runs in the container and the broker runs on this machine. A colima VM clock that
# runs ahead of this machine's clock fails the tests that compare a broker timestamp with the
# test JVM's clock, and the failure hangs the test until its timeout.
if command -v colima >/dev/null && colima status >/dev/null 2>&1; then
    colima ssh -- sudo date -u -s "@$(date +%s)" >/dev/null
fi

if (($# > 0)); then
    classes=("$@")
else
    classes=()
    while IFS= read -r f; do classes+=("$(basename "$f" .scala)"); done < <(
        find "$patched_kafka/core/src/test/scala/integration" -name '*.scala' | sort)
fi

echo "class,tests,failures,skipped,seconds" >"$results"
for test_class in "${classes[@]}"; do
    # The Kafka test broker for this class turns off topic auto-creation.
    broker_args=()
    [[ "$test_class" == ProducerFailureHandlingTest ]] && broker_args+=(--auto-create-topics-enable=false)
    pkill -f 'nisshi broker' 2>/dev/null || true
    sleep 1
    "$root/target/debug/nisshi" broker --storage-engine=memory:// --listener-url=tcp://0.0.0.0:9092 \
        --advertised-listener-url="tcp://$broker_host:9092" ${broker_args[@]+"${broker_args[@]}"} >"$root/target/compat/kafka-java/broker.log" 2>&1 &
    sleep 2
    rm -rf "$patched_kafka/core/build/test-results/test"
    start=$SECONDS
    timeout "$seconds_per_class" docker run --rm -e NISSHI_BOOTSTRAP="$broker_host:9092" \
        -v "$patched_kafka":/kafka -v "$gradle_home":/root/.gradle -w /kafka eclipse-temurin:17-jdk \
        ./gradlew --no-daemon -q :core:test --tests "*.$test_class" \
        >"$root/target/compat/kafka-java/$test_class.log" 2>&1 || true
    xml=$(find "$patched_kafka/core/build/test-results/test" -name "TEST-*.$test_class.xml" 2>/dev/null | head -1)
    if [[ -n "$xml" ]]; then
        python3 -I - "$xml" "$test_class" "$((SECONDS - start))" >>"$results" <<'EOF'
import sys, xml.etree.ElementTree as ET
t = ET.parse(sys.argv[1]).getroot()
print(f"{sys.argv[2]},{t.get('tests')},{t.get('failures')},{t.get('skipped')},{sys.argv[3]}")
EOF
        cp "$xml" "$root/target/compat/kafka-java/$test_class.xml"
    else
        echo "$test_class,0,0,0,$((SECONDS - start))  # no result: build failure, timeout or no such class" >>"$results"
    fi
done
pkill -f 'nisshi broker' 2>/dev/null || true
cat "$results"
