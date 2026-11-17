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
#   run.sh connections [args] report the connections each client opened, from the broker log:
#                             the clients, the requests they sent and the topic-partitions they
#                             produced to and fetched from. Args go to the connections binary
#                             (nisshi-broker/src/bin/connections), for example --connections or
#                             --csv results/connections.csv
#   run.sh sockets [seconds]  sample the sockets on the broker's port with lsof until interrupted
#   run.sh sweep              run 1, 4 and 8 producers and 1, 4 and 8 consumers on 4 and 16
#                             partitions, one broker each, and save the connection report and
#                             socket samples of each run in results/sweep/
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
# The broker logs one `nisshi::connection` event per connection opened and closed, and every
# CONNECTION_SNAPSHOT_INTERVAL (default 10s) the state of each open connection, which
# `connections` reads. RUST_LOG replaces the default filter, so keep nisshi::connection=info in it
# to keep those events. `run` also samples the established sockets on the broker's port with lsof
# every SOCKET_SAMPLE_INTERVAL seconds (default 5, 0 turns it off) into results/sockets-*.csv:
# the count the broker holds, and the count the benchmark JVM holds to it, as a check on the log.
#
# BROKER=kafka runs Apache Kafka (the apache/kafka image, KAFKA_IMAGE) in docker in place of nisshi
# and postgres, to compare the clients' connections. KAFKA_BROKERS is the number of brokers
# (default 1), on ports 9092, 9192 and so on, and KAFKA_REBALANCE_DELAY_MS the group.initial.rebalance.delay.ms (default 0).
# KAFKA_REQUEST_LOG=1 turns on Kafka's request log (kafka.request.logger=DEBUG), one line per request
# with its client id and connection, in /opt/kafka/logs/kafka-request.log of each broker
# container (docker exec cat). The level is set after startup: the image's own
# KAFKA_LOG4J_LOGGERS replaces the file appender, so the lines would go nowhere. Pass it to `broker`, `run`, `sweep` and
# `stop`. The sampler then counts the broker's sockets with netstat inside the container, there is
# no connection log, and `sweep` saves into results/sweep-kafka/ (results/sweep-kafka-N/ when
# KAFKA_BROKERS is set) and skips the report.
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
broker_port=9092

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

kafka_prefix=nisshi-omb-kafka
kafka_network=nisshi-omb-kafka-net
kafka_brokers=${KAFKA_BROKERS:-1}

# The host ports that the Kafka brokers advertise to clients: $broker_port, then 100 more for each
# further broker
kafka_ports() {
    local i
    for ((i = 0; i < kafka_brokers; i++)); do echo $((broker_port + 100 * i)); done
}

remove_kafka() {
    local names
    names=$(docker ps -a --format '{{.Names}}' | grep -E "^$kafka_prefix-[0-9]+\$" || true)
    # shellcheck disable=SC2086
    [[ -z $names ]] || docker rm -f $names >/dev/null 2>&1 || true
    docker network rm "$kafka_network" >/dev/null 2>&1 || true
}

# Starts KAFKA_BROKERS (default 1) Apache Kafka brokers in KRaft mode, each also a controller.
# Clients reach the broker on localhost:<port> (see kafka_ports) through the EXTERNAL listener, and
# the brokers reach each other on the INTERNAL one, so replication stays off the client port.
# The log directory is a tmpfs of KAFKA_TMPFS_SIZE (default 1g) per broker, so a run needs no disk
# in the docker VM and starts empty. A run writes about 1 GB per 3 minutes at the sweep's rate.
kafka_broker() {
    remove_kafka
    docker network create "$kafka_network" >/dev/null
    local i voters='' port name
    for ((i = 1; i <= kafka_brokers; i++)); do voters+="${voters:+,}$i@$kafka_prefix-$i:9093"; done
    for ((i = 1; i <= kafka_brokers; i++)); do
        port=$((broker_port + 100 * (i - 1)))
        name=$kafka_prefix-$i
        docker run -d --name "$name" --network "$kafka_network" -p "$port:9092" \
            --tmpfs "/tmp/kafka-logs:rw,size=${KAFKA_TMPFS_SIZE:-1g}" \
            -e KAFKA_NODE_ID="$i" -e KAFKA_PROCESS_ROLES=broker,controller \
            -e KAFKA_LISTENERS=EXTERNAL://:9092,INTERNAL://:19092,CONTROLLER://:9093 \
            -e KAFKA_ADVERTISED_LISTENERS=EXTERNAL://localhost:$port,INTERNAL://$name:19092 \
            -e KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL \
            -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
            -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT,INTERNAL:PLAINTEXT \
            -e KAFKA_CONTROLLER_QUORUM_VOTERS="$voters" \
            -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR="$kafka_brokers" \
            -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR="$kafka_brokers" \
            -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
            -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS="${KAFKA_REBALANCE_DELAY_MS:-0}" \
            "${KAFKA_IMAGE:-apache/kafka:3.9.2}" >/dev/null
    done
    # the port answers before the broker does, so wait for each startup message
    local _
    for ((i = 1; i <= kafka_brokers; i++)); do
        for _ in $(seq 240); do
            docker logs "$kafka_prefix-$i" 2>&1 | grep -q 'Kafka Server started' && continue 2
            sleep 0.5
        done
        echo "run.sh: kafka broker $i did not start" >&2
        return 1
    done
    if [[ -n ${KAFKA_REQUEST_LOG:-} ]]; then
        for ((i = 1; i <= kafka_brokers; i++)); do
            docker exec "$kafka_prefix-$i" /opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:19092 \
                --entity-type broker-loggers --entity-name "$i" --alter \
                --add-config kafka.request.logger=DEBUG >/dev/null
        done
    fi
}

# Prints how many client connections the Kafka brokers hold in all: the ESTABLISHED sockets whose
# local port is the clients' listener port in each container
kafka_established() {
    local i total=0 n
    for ((i = 1; i <= kafka_brokers; i++)); do
        n=$({ docker exec "$kafka_prefix-$i" netstat -tn 2>/dev/null || true; } | grep -c -E -e "^tcp.*:9092 .*ESTABLISHED" || true)
        total=$((total + n))
    done
    echo "$total"
}

broker() {
    if [[ ${BROKER:-nisshi} == kafka ]]; then
        kafka_broker
        return
    fi
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
    RUST_LOG=${RUST_LOG:-warn,nisshi::connection=info} \
        CONNECTION_SNAPSHOT_INTERVAL=${CONNECTION_SNAPSHOT_INTERVAL:-10s} \
        nohup target/release/nisshi broker \
        --storage-engine="$storage" \
        --advertised-listener-url=tcp://localhost:$broker_port \
        >"$here/broker.log" 2>&1 &
    echo $! >"$here/broker.pid"
    echo "broker pid $(cat "$here/broker.pid"), log $here/broker.log"
}

# Prints how many ESTABLISHED TCP sockets of process $1 have a name that matches the extended
# regular expression $2, or 0 when there is no such process. lsof's -F n output has one line
# per socket: n<local address>-><remote address>.
established() {
    local pid=$1 pattern=$2
    [[ -n $pid ]] || { echo 0; return; }
    { lsof -nP -a -p "$pid" -iTCP -sTCP:ESTABLISHED -Fn 2>/dev/null || true; } | grep -c -E -e "$pattern" || true
}

# Appends a line to the CSV $1 every $2 seconds, until killed: the sockets the broker holds on its
# port, and the sockets the benchmark JVM holds to that port. Each connection has one of each,
# so the two columns agree except while a connection opens or closes, or when a client outside
# the benchmark is connected.
sample_sockets() {
    local file=$1 interval=$2 broker_pid jvm_pid broker_count client_ports
    client_ports=$(kafka_ports | paste -sd'|' -)
    echo "time,broker_established,benchmark_established" >"$file"
    while true; do
        if [[ ${BROKER:-nisshi} == kafka ]]; then
            broker_count=$(kafka_established)
        else
            broker_pid=$(cat "$here/broker.pid" 2>/dev/null || true)
            broker_count=$(established "$broker_pid" ":$broker_port->")
        fi
        jvm_pid=$(pgrep -n -f io.openmessaging.benchmark.Benchmark || true)
        echo "$(date +%Y-%m-%dT%H:%M:%S),$broker_count,$(established "$jvm_pid" "->.*:($client_ports)\$")" >>"$file"
        sleep "$interval"
    done
}

# Prints the most sockets that a sample file in $1 holds at once, for each column
peak_sockets() {
    awk -F, 'NR > 1 { if ($2 > b) b = $2; if ($3 > j) j = $3 } END { printf "peak sockets: broker %d, benchmark %d\n", b, j }' "$1"
}

sampler_pid=
stop_sampler() {
    if [[ -n $sampler_pid ]]; then
        kill "$sampler_pid" 2>/dev/null || true
        wait "$sampler_pid" 2>/dev/null || true
        sampler_pid=
    fi
}

sockets() {
    local interval=${1:-${SOCKET_SAMPLE_INTERVAL:-5}}
    mkdir -p "$results"
    local file
    file="$results/sockets-$(date +%Y%m%d-%H%M%S).csv"
    echo "sampling every ${interval}s into $file, interrupt to stop" >&2
    sample_sockets "$file" "$interval"
}

connections() {
    [[ -f "$here/broker.log" ]] || { echo "run.sh: no broker.log, run.sh broker first" >&2; exit 1; }
    cargo run --quiet --release --manifest-path "$root/Cargo.toml" -p nisshi-broker \
        --no-default-features --features postgres --bin connections -- \
        --filename "$here/broker.log" "$@"
}

run() {
    local workload=${1:-workloads/smoke.yaml}
    [[ -f "$here/$workload" ]] && workload="$here/$workload"
    [[ -d "$dist" ]] || { echo "run.sh build first" >&2; exit 1; }
    mkdir -p "$results"
    # bin/benchmark resolves its classpath and the payload files relative to the working
    # directory, and writes result JSON there, so run in the distribution and move the results
    # out afterwards. Without HEAP_OPTS, bin/benchmark uses a 4 GB heap.
    local status=0 interval=${SOCKET_SAMPLE_INTERVAL:-5} sockets_file=
    if [[ $interval != 0 ]]; then
        sockets_file=${SOCKETS_CSV:-"$results/sockets-$(date +%Y%m%d-%H%M%S).csv"}
        sample_sockets "$sockets_file" "$interval" &
        sampler_pid=$!
        trap stop_sampler EXIT
    fi
    (cd "$dist" && bin/benchmark --drivers "${DRIVER:-$here/driver-nisshi.yaml}" "$workload") || status=$?
    stop_sampler
    mv -f "$dist"/*.json "$results/" 2>/dev/null || true
    if [[ -n $sockets_file ]]; then
        echo "socket samples: $sockets_file, $(peak_sockets "$sockets_file")"
    fi
    return "$status"
}

# Waits until the broker accepts connections. The probe is a connection of its own, which the
# broker logs as a connection that sent no request.
wait_for_broker() {
    local _
    for _ in $(seq 120); do
        (exec 3<>"/dev/tcp/127.0.0.1/$broker_port") 2>/dev/null && return
        sleep 0.5
    done
    echo "run.sh: the broker is not listening on port $broker_port" >&2
    return 1
}

# Runs a workload for each number of partitions and each count of producers and consumers, on a
# new broker each time so that its log holds the connections of that run only. The results are in
# results/sweep/: for each run the workload, the connection report (.md and .csv) and the socket
# samples. SWEEP_MINUTES is the test duration of each run (default 1) and SWEEP_RATE the producer
# rate of each run in messages per second (default 5000).
sweep() {
    local out="$results/sweep" partitions combo producers consumers name
    [[ ${BROKER:-nisshi} == kafka ]] && out="$results/sweep-kafka${KAFKA_BROKERS:+-$KAFKA_BROKERS}"
    mkdir -p "$out"
    for partitions in 4 16; do
        for combo in "1 1" "4 1" "1 4" "8 8"; do
            read -r producers consumers <<<"$combo"
            name="${producers}p-${consumers}c-${partitions}partitions"
            cat >"$out/$name.yaml" <<WORKLOAD
name: nisshi sweep, $producers producers / $consumers consumers on 1 topic of $partitions partitions
topics: 1
partitionsPerTopic: $partitions
messageSize: 1024
payloadFile: "payload/payload-1Kb.data"
subscriptionsPerTopic: 1
consumerPerSubscription: $consumers
producersPerTopic: $producers
producerRate: ${SWEEP_RATE:-5000}
consumerBacklogSizeGB: 0
testDurationMinutes: ${SWEEP_MINUTES:-1}
WORKLOAD
            echo "== $name"
            broker
            wait_for_broker
            SOCKETS_CSV="$out/$name.sockets.csv" run "$out/$name.yaml"
            # the benchmark's clients close their sockets as the JVM exits, and the broker
            # logs each close a moment later
            sleep 2
            if [[ ${BROKER:-nisshi} != kafka ]]; then
                connections --connections --csv "$out/$name.csv" >"$out/$name.md"
            fi
        done
    done
    stop
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
    remove_kafka
    stop_local_postgres
}

case "${1:-}" in
    build) build ;;
    broker) broker ;;
    run) shift; run "$@" ;;
    seed) shift; seed "$@" ;;
    explain) shift; explain "$@" ;;
    connections) shift; connections "$@" ;;
    sweep) sweep ;;
    sockets) shift; sockets "$@" ;;
    stop) stop ;;
    *) sed -n '2,32p' "$0"; exit 1 ;;
esac
