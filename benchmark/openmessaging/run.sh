#!/usr/bin/env bash
# Run the OpenMessaging Benchmark (https://openmessaging.cloud/docs/benchmarks/) against a
# nisshi broker backed by postgres. Needs a JDK and Maven on the host.
#
#   run.sh build              clone the benchmark into .omb/ and build it into .omb/dist/
#   run.sh broker             start postgres (port 5432) and the nisshi broker. Postgres runs on
#                             the host, or in docker with POSTGRES=docker, see below.
#   run.sh run [workload]     run a workload: a file under benchmark/openmessaging/ (default:
#                             workloads/smoke.yaml), or else a file of the upstream benchmark's
#                             workloads/ directory, such as workloads/1-topic-16-partitions-1kb.yaml
#   run.sh seed [var=value]   fill postgres with generated records, see seed.sql for the variables
#   run.sh explain [topic [partition [offset]]]
#                             EXPLAIN ANALYZE the fetch queries against the seeded topic
#   run.sh stop               stop the broker and postgres
#
# By default postgres is a Homebrew postgres on 127.0.0.1, with all its files in .pg/ beside this
# script: data/, temp/ (the temporary file tablespace), postgresql.conf and postgres.log.
# `broker` deletes and recreates .pg/, and uses trust authentication, so the server accepts any
# local connection. Set PG_BIN to the postgres bin directory when it is not
# /opt/homebrew/opt/postgresql@18/bin, and POSTGRES_PORT when 5432 is taken.
#
# POSTGRES=docker runs postgres 18 in a container instead. Pass the same POSTGRES value to every
# subcommand that talks to postgres (broker, seed, explain, stop).
#
# WAL_DIR keeps the write-ahead log of the host postgres in that directory, for example on a RAM
# disk, to measure without WAL flush latency. `broker` deletes WAL_DIR first.
#
# STORAGE_QUERY is appended to the broker's storage URL as its query string, for a storage option such
# as maintenance_interval=10s (the default sweep interval is 10 minutes).
#
# POSTGRES_ARGS adds arguments to the postgres server, for example
# "-c shared_preload_libraries=auto_explain"
#
# DRIVER is the benchmark driver file `run` uses, driver-nisshi.yaml by default. Copy that file to
# change the producer or consumer settings, for example consumerConfig's fetch.max.wait.ms.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/../.." && pwd)
omb="$here/.omb"
dist="$omb/dist"
results="$here/results"

postgres=${POSTGRES:-local}
pg_bin=${PG_BIN:-/opt/homebrew/opt/postgresql@18/bin}
pg_dir="$here/.pg"
pg_port=${POSTGRES_PORT:-5432}

# The upstream commit that the patch and the build flags below were made for. Set OMB_COMMIT to
# try another.
omb_commit=${OMB_COMMIT:-5b1fa70951a323da26bd587174b58bb2c65b0b5c}

# Upstream pins a Lombok that cannot compile on a current JDK, so the build overrides it. Its
# Benchmark.main returns without System.exit, and KafkaTopicCreator's non-daemon
# ScheduledExecutorService is never shut down, so the JVM would idle forever after the run.
# Upstream also calls System.exit elsewhere, so the patch looks for the call after worker.close(),
# and the build stops when that call is not there afterwards, as it is when upstream has changed.
build() {
    if [[ ! -d "$omb" ]]; then
        git init -q "$omb"
        git -C "$omb" fetch -q --depth 1 https://github.com/openmessaging/benchmark "$omb_commit"
        git -C "$omb" checkout -q FETCH_HEAD
    fi
    local main_java="$omb/benchmark-framework/src/main/java/io/openmessaging/benchmark/Benchmark.java"
    perl -0pi -e 's/^(        worker\.close\(\);\n)(?!        System\.exit\(0\);)/$1        System.exit(0);\n/m' "$main_java"
    grep -A1 'worker\.close();' "$main_java" | grep -q 'System\.exit(0);' || {
        echo "run.sh: could not make Benchmark.main exit, upstream has changed since $omb_commit" >&2
        exit 1
    }
    (cd "$omb" && mvn -q install -DskipTests -Dlombok.version=1.18.48 -Dspotless.check.skip=true \
        -Dlicense.skip=true -Dcheckstyle.skip=true -Dspotbugs.skip=true)
    rm -rf "$dist"
    mkdir -p "$dist"
    tar -xzf "$omb"/package/target/openmessaging-benchmark-*-SNAPSHOT-bin.tar.gz -C "$dist" --strip=1
}

# A dedicated postgres container, not the compose `db` service. It binds 5432, so stop any
# other postgres or compose `db` holding that port first
docker_postgres() {
    local args=()
    # shellcheck disable=SC2206 # POSTGRES_ARGS is a list of words, split on purpose
    [[ -n ${POSTGRES_ARGS:-} ]] && args=(${POSTGRES_ARGS})
    docker rm -f nisshi-omb-postgres >/dev/null 2>&1 || true
    docker run -d --name nisshi-omb-postgres -p "127.0.0.1:$pg_port:5432" \
        -e POSTGRES_PASSWORD=postgres -e PGUSER=postgres \
        --health-cmd=pg_isready --health-interval=2s --health-retries=30 \
        postgres:18 postgres ${args[@]+"${args[@]}"} >/dev/null
    until [[ "$(docker inspect -f '{{.State.Health.Status}}' nisshi-omb-postgres)" == healthy ]]; do sleep 1; done
}

# Only 127.0.0.1 accepts connections and unix sockets are off, because a socket path must fit in
# 103 bytes and this directory's path does not. The settings are for a benchmark on one machine:
# a large WAL and a long checkpoint interval keep checkpoints off the write path.
local_postgres() {
    stop_local_postgres
    rm -rf "$pg_dir"
    mkdir -p "$pg_dir/data" "$pg_dir/temp"
    local waldir=()
    if [[ -n ${WAL_DIR:-} ]]; then
        rm -rf "$WAL_DIR"
        waldir=(--waldir="$WAL_DIR")
    fi
    "$pg_bin/initdb" -D "$pg_dir/data" -U postgres --auth=trust -E UTF8 ${waldir[@]+"${waldir[@]}"} >/dev/null
    cat >"$pg_dir/postgresql.conf" <<EOF
listen_addresses = '127.0.0.1'
port = $pg_port
unix_socket_directories = ''
max_connections = 100
shared_buffers = 1GB
max_wal_size = 8GB
checkpoint_timeout = 15min
shared_preload_libraries = 'auto_explain'
auto_explain.log_min_duration = 300
auto_explain.log_analyze = on
auto_explain.log_buffers = on
log_temp_files = 0
EOF
    "$pg_bin/pg_ctl" start -w -D "$pg_dir/data" -l "$pg_dir/postgres.log" \
        -o "-c config_file=$pg_dir/postgresql.conf ${POSTGRES_ARGS:-}" >/dev/null
    psql -c "create tablespace benchmark_temp location '$pg_dir/temp'" \
        -c "alter system set temp_tablespaces = 'benchmark_temp'" \
        -c "select pg_reload_conf()" >/dev/null
}

stop_local_postgres() {
    if [[ -f "$pg_dir/data/postmaster.pid" ]]; then
        "$pg_bin/pg_ctl" stop -D "$pg_dir/data" -m fast >/dev/null 2>&1 || true
    fi
}

broker() {
    cd "$root"
    local storage=postgres://postgres:postgres@localhost:$pg_port
    if [[ $postgres == local ]]; then
        local_postgres
        storage=postgres://postgres:postgres@127.0.0.1:$pg_port
    else
        docker_postgres
    fi
    storage=$storage${STORAGE_QUERY:+?$STORAGE_QUERY}
    cargo build --release --bin nisshi --no-default-features --features postgres
    pkill -f 'target/release/nisshi broker' 2>/dev/null || true
    RUST_LOG=${RUST_LOG:-warn} nohup target/release/nisshi broker \
        --storage-engine="$storage" \
        --advertised-listener-url=tcp://localhost:9092 \
        >"$here/broker.log" 2>&1 &
    echo $! >"$here/broker.pid"
    echo "broker pid $(cat "$here/broker.pid"), log $here/broker.log"
}

run() {
    local workload=${1:-workloads/smoke.yaml}
    [[ -f "$here/$workload" ]] && workload="$here/$workload"
    [[ -d "$dist" ]] || { echo "run.sh build first" >&2; exit 1; }
    mkdir -p "$results"
    # bin/benchmark resolves its classpath and the payload files relative to the working
    # directory, and writes result JSON there, so run in the distribution and move the results
    # out afterwards. Without HEAP_OPTS, bin/benchmark uses a 4 GB heap.
    local status=0
    (cd "$dist" && bin/benchmark --drivers "${DRIVER:-$here/driver-nisshi.yaml}" "$workload") || status=$?
    mv -f "$dist"/*.json "$results/" 2>/dev/null || true
    return "$status"
}

psql() {
    if [[ $postgres == local ]]; then
        "$pg_bin/psql" -h 127.0.0.1 -p "$pg_port" -U postgres -X -q "$@"
    else
        docker exec -i nisshi-omb-postgres psql -U postgres -X -q "$@"
    fi
}

# Generates records straight into postgres, which is far faster than producing through the broker
seed() {
    local args=() var
    for var in "$@"; do args+=(-v "$var"); done
    psql "${args[@]}" <"$here/seed.sql"
}

# Reads from the given offset up to the high watermark, as a fetch does, with a 1 MiB limit and the
# 10000 row limit and plan settings of the broker (ROWS_PER_FETCH and FETCH_PLAN_SETTINGS in pg.rs)
explain() {
    local topic=${1:-seed} partition=${2:-0} offset=${3:-0} file query high cluster binds
    cluster=$(psql -At -c "select c.name from cluster c join topic t on t.cluster = c.id where t.name = '$topic'")
    high=$(psql -At -c "select w.high from watermark w join topition tp on tp.id = w.topition join topic t on t.id = tp.topic where t.name = '$topic' and tp.partition = $partition")
    for file in record_fetch record_fetch_keyed; do
        echo "== $file, topic $topic partition $partition offsets $offset..$high"
        # prepared statements take their parameters from \bind, so drop the comments and final ;
        query=$(grep -v '^--' "$root/nisshi-storage-sql/src/pg/$file.sql" | sed 's/;[[:space:]]*$//')
        binds="$cluster $topic $partition $offset 1048576 $high"
        # the keyed query takes the key as $7, then the row limit as $8
        if [[ $file == record_fetch_keyed ]]; then binds="$binds key-1 10000"; else binds="$binds 10000"; fi
        # the settings of FETCH_PLAN_SETTINGS in nisshi-storage-sql/src/pg.rs
        printf 'set enable_bitmapscan = off;\nset enable_seqscan = off;\nset enable_sort = off;\nset enable_hashagg = off;\nset enable_nestloop = off;\nset enable_hashjoin = off;\nexplain (analyze, buffers) %s\n\\bind %s \\g\n' \
            "$query" "$binds" | psql
    done
}

stop() {
    [[ -f "$here/broker.pid" ]] && kill "$(cat "$here/broker.pid")" 2>/dev/null || true
    rm -f "$here/broker.pid"
    docker rm -f nisshi-omb-postgres >/dev/null 2>&1 || true
    stop_local_postgres
}

case "${1:-}" in
    build) build ;;
    broker) broker ;;
    run) shift; run "$@" ;;
    seed) shift; seed "$@" ;;
    explain) shift; explain "$@" ;;
    stop) stop ;;
    *) sed -n '2,25p' "$0"; exit 1 ;;
esac
