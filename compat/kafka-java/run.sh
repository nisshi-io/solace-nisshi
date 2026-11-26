#!/usr/bin/env bash
# Runs Apache Kafka 3.9.1's own integration tests (core/src/test/scala/integration) against a
# Nisshi broker, one test class at a time, each on a fresh broker.
#
# Usage: run.sh [ClassName ...]   (default: every class under core/src/test/scala/integration)
#
# With ALLOW_LIST_ONLY=1 it runs only the classes in tests.allow, and exits with 1 if any test in
# tests.allow fails or does not run.
#
# Needs: docker, and a built ./target/debug/nisshi (cargo build --features dynostore -p nisshi).
# Env:   BROKER_HOST   address of this machine as the docker container sees it (default 127.0.0.1 on
#                      Linux, where the container shares the host network; 192.168.5.2 elsewhere,
#                      colima; on Docker Desktop use host.docker.internal)
#        STORAGE_ENGINE storage engine URL of the broker (default memory://)
#        ALLOW_LIST_ONLY 1 to run only the classes in tests.allow and check each test in it
#        SECONDS_PER_CLASS seconds allowed for one class (default 900)
#        RESULTS_FILE  CSV of class,tests,failures,skipped,seconds (default target/compat/kafka-java.csv)
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/../.." && pwd)
kafka_version=3.9.1
patched_kafka="$root/target/compat/kafka-java/kafka-$kafka_version"
storage_engine=${STORAGE_ENGINE:-memory://}
allow_list="$here/tests.allow"
docker_network=()
if [[ "$(uname)" == Linux ]]; then
    broker_host=${BROKER_HOST:-127.0.0.1}
    # The container shares the host network, and runs as this user so that the files it writes into
    # the mounted worktree can be removed by this script (root-owned files cannot).
    docker_network=(--network host --user "$(id -u):$(id -g)" -e HOME=/tmp)
else
    broker_host=${BROKER_HOST:-192.168.5.2}
fi
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
elif [[ "${ALLOW_LIST_ONLY:-}" == 1 ]]; then
    classes=()
    while IFS= read -r name; do classes+=("$name"); done < <(
        grep -v '^#' "$allow_list" | grep . | cut -d. -f1 | sort -u)
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
    "$root/target/debug/nisshi" broker --storage-engine="$storage_engine" --listener-url=tcp://0.0.0.0:9092 \
        --advertised-listener-url="tcp://$broker_host:9092" ${broker_args[@]+"${broker_args[@]}"} >"$root/target/compat/kafka-java/broker.log" 2>&1 &
    sleep 2
    rm -rf "$patched_kafka/core/build/test-results/test"
    start=$SECONDS
    container="kafka-java-$test_class"
    # `timeout` stops the docker client and not the container, so the container is removed by name.
    timeout "$seconds_per_class" docker run --rm --name "$container" ${docker_network[@]+"${docker_network[@]}"} \
        -e NISSHI_BOOTSTRAP="$broker_host:9092" \
        -v "$patched_kafka":/kafka -e GRADLE_USER_HOME=/gradle-home -v "$gradle_home":/gradle-home -w /kafka eclipse-temurin:17-jdk \
        ./gradlew --no-daemon -q :core:test --tests "*.$test_class" \
        >"$root/target/compat/kafka-java/$test_class.log" 2>&1 || true
    docker rm -f "$container" >/dev/null 2>&1 || true
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

if [[ "${ALLOW_LIST_ONLY:-}" == 1 ]]; then
    python3 -I - "$allow_list" "$root/target/compat/kafka-java" <<'EOF'
import re, sys, os
import xml.etree.ElementTree as ET

allowed = [l.strip() for l in open(sys.argv[1]) if l.strip() and not l.startswith('#')]
outcomes = {}
for class_name in sorted({a.split('.')[0] for a in allowed}):
    path = os.path.join(sys.argv[2], class_name + '.xml')
    if not os.path.exists(path):
        continue
    for case in ET.parse(path).getroot().iter('testcase'):
        if case.find('skipped') is not None:
            continue
        method = class_name + '.' + re.match(r'"?(\w+)', case.get('name')).group(1)
        passed = case.find('failure') is None and case.find('error') is None
        outcomes.setdefault(method, []).append(passed)

bad = [(a, 'did not run' if a not in outcomes else 'failed') for a in allowed
       if a not in outcomes or not all(outcomes[a])]
for name, why in bad:
    print(f'allow-list: {name} {why}')
print(f'allow-list: {len(allowed) - len(bad)} of {len(allowed)} tests pass')
sys.exit(1 if bad else 0)
EOF
fi
