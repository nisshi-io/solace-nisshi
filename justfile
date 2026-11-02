set dotenv-load

default: fmt build test clippy

about:
    cargo about generate about.hbs > license.html

cargo-build +args:
    cargo build {{ args }}

clean-workspace:
    cargo clean --workspace

# seed a new worktree's target/ from the main checkout's, so dependencies don't rebuild
seed-target:
    #!/usr/bin/env bash
    set -euo pipefail
    if [[ "$(git rev-parse --path-format=absolute --git-dir)" == "$(git rev-parse --path-format=absolute --git-common-dir)" ]]; then
        echo "seed-target: this is the main checkout; nothing to seed" >&2
        exit 0
    fi
    if [[ -e target/debug ]]; then
        echo "seed-target: target/debug already exists; not seeding" >&2
        exit 0
    fi
    main=$(git worktree list --porcelain | awk '/^worktree /{print $2; exit}')
    # We seed only target/debug, because `cargo clean --workspace` cleans only the dev
    # profile. A release, profiling or cross-target directory would keep the main
    # checkout's workspace-crate artifacts, and cargo would reuse them.
    if [[ ! -d "$main/target/debug" ]]; then
        echo "seed-target: $main/target/debug doesn't exist; nothing to seed from" >&2
        exit 0
    fi
    # We build the seed in target/.seed and move it to target/debug last, so an
    # interrupted or failed seed leaves no target/debug for cargo to reuse. A rerun
    # starts again from here.
    rm -rf target/.seed
    mkdir -p target/.seed
    # cargo clean refuses a target directory without a CACHEDIR.TAG file.
    cp "$main/target/CACHEDIR.TAG" target/.seed/
    # We clone copy-on-write, so the clone shares the main checkout's disk blocks
    # until cargo rewrites a file.
    if [[ "$(uname)" == Darwin ]]; then
        cp -cRp "$main/target/debug" target/.seed/debug
    else
        # --reflink=always fails on a filesystem without copy-on-write, instead of
        # making a full copy of the main checkout's target/debug.
        cp -R --reflink=always --preserve=timestamps "$main/target/debug" target/.seed/debug
    fi
    # Cargo reuses a workspace crate's artifact when the artifact is newer than the
    # crate's sources. A cloned artifact can be newer than this checkout's sources
    # and still hold the main checkout's code, so we clean every workspace member
    # and cargo reuses only dependency artifacts.
    cargo clean --workspace --quiet --target-dir target/.seed
    mv target/.seed/debug target/debug
    rm -rf target/.seed
    echo "seed-target: seeded target/debug from $main/target/debug" >&2

# The Apache Kafka tag that `just kafka-ref` checks out. It must stay the Kafka
# version that the message descriptors in nisshi-sans-io/message/ come from.
kafka_ref_version := "3.9.1"

# clone Apache Kafka at the version the message descriptors come from into kafka-ref/, to read as the reference
kafka-ref:
    #!/usr/bin/env bash
    set -euo pipefail
    version={{ kafka_ref_version }}
    main=$(git worktree list --porcelain | awk '/^worktree /{print $2; exit}')
    # Every checkout reads one clone in the main checkout, and a linked worktree
    # gets a symlink to it, so a new worktree doesn't download Kafka again.
    clone="$main/kafka-ref"
    if [[ ! -e "$clone" ]]; then
        echo "kafka-ref: cloning Apache Kafka $version into $clone" >&2
        # We clone into kafka-ref.tmp and move it to kafka-ref last, so an
        # interrupted clone leaves no kafka-ref. A rerun starts again from here.
        rm -rf "$clone.tmp"
        # The clone is blobless: it fetches each file's contents only when a
        # command reads them. It keeps the full history, so `git log` and
        # `git blame` show Kafka's fixes, and origin/trunk shows what changed
        # since the tag.
        git clone --quiet --filter=blob:none --branch "$version" https://github.com/apache/kafka.git "$clone.tmp"
        mv "$clone.tmp" "$clone"
    elif [[ ! -e "$clone/.git" ]]; then
        # Without this check, git would find this repository above the
        # directory and switch this repository's checkout instead.
        echo "kafka-ref: $clone exists and isn't a git clone; remove it and rerun" >&2
        exit 1
    elif [[ "$(git -C "$clone" rev-parse HEAD)" != "$(git -C "$clone" rev-parse --verify --quiet "refs/tags/$version^{commit}")" ]]; then
        if [[ -n "$(git -C "$clone" status --porcelain)" ]]; then
            echo "kafka-ref: $clone has local changes; not switching it to $version" >&2
            exit 1
        fi
        echo "kafka-ref: switching $clone to $version" >&2
        git -C "$clone" fetch --quiet --tags origin
        git -C "$clone" checkout --quiet --detach "refs/tags/$version"
    fi
    if [[ "$PWD" != "$main" && ! -e kafka-ref && ! -L kafka-ref ]]; then
        ln -s "$clone" kafka-ref
    fi
    echo "kafka-ref: kafka-ref/ is Apache Kafka $version" >&2

license:
    cargo about generate about.hbs > license.html

build-all profile="dev" features="delta,dynostore,iceberg,libsql,parquet,postgres,slatedb": (cargo-build "--profile" profile "--timings" "--no-default-features" "--features" features "--all-targets")

build profile="dev" features="delta,dynostore,iceberg,libsql,parquet,postgres,slatedb" bin="nisshi": (cargo-build "--profile" profile "--timings" "--bin" bin "--no-default-features" "--features" features)

# Fails if a single-engine build compiles another engine's database client.
# ci.yml's build-storage job runs the same check; the client of each engine is
# listed in .github/scripts/check-storage-deps.sh.
check-storage-deps: (storage-deps "dynostore") (storage-deps "libsql") (storage-deps "postgres") (storage-deps "slatedb") (storage-deps "turso")

[private]
storage-deps feature:
    #!/usr/bin/env bash
    set -euo pipefail
    cargo tree -p nisshi --no-default-features --features {{ feature }} -e normal,build --prefix none -f '{p}' \
      | bash .github/scripts/check-storage-deps.sh {{ feature }}

build-storage: clean-workspace (build "dev" "libsql") (build "dev" "dynostore") (build "dev" "postgres") (build "dev" "slatedb") (build "dev" "turso")

build-examples: (cargo-build "--examples")

release: (cargo-build "--release" "--bin" "nisshi" "--no-default-features" "--features" "delta,dynostore,iceberg,libsql,parquet,postgres,slatedb")

release-sqlite: (cargo-build "--release" "--bin" "nisshi" "--no-default-features" "--features" "libsql")

test: test-workspace test-doc

test-workspace *args: (nextest "run" "--workspace" "--all-targets" "--all-features" "--no-fail-fast" "--exclude" "fuzz" "--exclude" "nisshi-smoke-test" args)

# the tests that need no docker compose service (no Postgres, MinIO or Iceberg catalog);
# NISSHI_WORKTREE_SERVICES=0 stops nextest's setup script starting this worktree's services
test-fast *args:
    NISSHI_WORKTREE_SERVICES=0 {{ just_executable() }} test-workspace --profile fast {{ args }}

nextest *args:
    cargo nextest {{ args }}

test-doc:
    cargo test --workspace --doc --all-features

# check the doc comments of every library and binary; pass --open to browse them
doc *args:
    RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps --document-private-items {{ args }}

cargo-fuzz +args:
    CARGO_PROFILE_RELEASE_LTO=false cargo +nightly fuzz {{ args }}

fuzz-request-decode: (cargo-fuzz "run" "fuzz_request_decode" "--" "-max_total_time=60")

fuzz-member-metadata: (cargo-fuzz "run" "fuzz_member_metadata" "--" "-max_total_time=60")

# write the seed corpus under fuzz/corpus/, on the toolchain fuzz-ci runs on
fuzz-generate-seed:
    cargo +nightly run --package fuzz --bin generate_seeds

# run every target listed under `tier` in fuzz/ci-targets.json for its budget
# in seconds (or only `target`), as .github/workflows/fuzz.yml does; see
# fuzz/README.md
fuzz-ci tier target="":
    #!/usr/bin/env bash
    set -euo pipefail
    config=fuzz/ci-targets.json
    tier={{ quote(tier) }}
    only={{ quote(target) }}
    jq -e --arg tier "$tier" 'has($tier)' "$config" > /dev/null || { echo "::error::no tier '$tier' in $config" >&2; exit 1; }
    bad=$(jq -r --arg tier "$tier" '.[$tier] | to_entries[] | select((.value | type) != "number" or .value < 1) | .key' "$config")
    [ -z "$bad" ] || { echo "::error::$config: budgets must be a number of seconds >= 1, not for: $bad" >&2; exit 1; }
    known=$(cargo +nightly fuzz list | grep -vx generate_seeds)
    unknown=$(jq -r --arg tier "$tier" '.[$tier] | keys[]' "$config" | grep -vxF -e "$known" || true)
    [ -z "$unknown" ] || { echo "::error::$config lists targets that aren't in fuzz/Cargo.toml: $unknown" >&2; exit 1; }
    unlisted=$(grep -vxF -e "$(jq -r '[.[] | keys[]] | unique[]' "$config")" <<< "$known" || true)
    [ -z "$unlisted" ] || echo "::warning::fuzz targets in no tier of $config, so CI never runs them: $unlisted" >&2
    if [ -n "$only" ]; then
        jq -e --arg tier "$tier" --arg t "$only" '.[$tier] | has($t)' "$config" > /dev/null || { echo "::error::'$only' isn't in tier '$tier' of $config" >&2; exit 1; }
        targets=$only
    else
        targets=$(jq -r --arg tier "$tier" '.[$tier] | keys[]' "$config")
    fi
    # cargo-fuzz defaults --target to the platform it was built for, which
    # is musl for its prebuilt Linux release, and ASan can't link against
    # musl. build, run and cmin must all get the same flags, or the next one
    # rebuilds from scratch. (--codegen-units above 1 would build faster,
    # but nightly rustc segfaults on typenum with it and the sanitizer.)
    opts=(--target "$(rustc +nightly -vV | sed -n 's/^host: //p')")
    # Fuzz builds turn off the workspace's cross-crate LTO instead of
    # inheriting `lto = "thin"` from Cargo.toml. With LTO, cargo compiles each
    # dependency to bitcode only, and every target then generates the machine
    # code for its whole dependency graph when it links, which takes minutes
    # per target. LTO also makes the targets run fewer inputs per second.
    # fuzz-cmin sets the same value, because cmin must get the same flags as
    # build and run. The cargo-fuzz recipe sets it to build as fast.
    export CARGO_PROFILE_RELEASE_LTO=false
    cargo +nightly run --quiet --package fuzz --bin generate_seeds > /dev/null
    # Build only the targets of this tier, up front, so a failure to compile
    # shows before any fuzzing starts. `fuzz build` without a target builds
    # every target in fuzz/Cargo.toml, and the storage targets, which link the
    # whole storage stack, take longer to build than the pr tier's budget.
    # `fuzz build` takes one target, so loop; the dependencies compile once,
    # and each target after the first compiles only its own code.
    for t in $targets; do
        cargo +nightly fuzz build "${opts[@]}" "$t"
    done
    failed=""
    for t in $targets; do
        secs=$(jq -r --arg tier "$tier" --arg t "$t" '.[$tier][$t]' "$config")
        echo "::group::$t (${secs}s)"
        # -timeout turns a hang on a single input into a reported failure
        cargo +nightly fuzz run "${opts[@]}" "$t" -- -max_total_time="$secs" -timeout=25 -rss_limit_mb=2048 || failed="$failed $t"
        echo "::endgroup::"
    done
    [ -z "$failed" ] || { echo "::error::fuzz targets failed:$failed (see the log above; crashing inputs are under fuzz/artifacts/)" >&2; exit 1; }

# minimise fuzz/corpus/<target>, built with the same flags as fuzz-ci
fuzz-cmin target:
    #!/usr/bin/env bash
    set -euo pipefail
    CARGO_PROFILE_RELEASE_LTO=false cargo +nightly fuzz cmin --target "$(rustc +nightly -vV | sed -n 's/^host: //p')" {{ quote(target) }}

# the target names under `tier` in fuzz/ci-targets.json, as a JSON array
fuzz-ci-matrix tier:
    @jq -c --arg tier {{ quote(tier) }} '.[$tier] | keys' fuzz/ci-targets.json

check:
    cargo check --workspace --all-features --all-targets

clippy:
    cargo clippy --workspace --all-features --all-targets -- -D warnings

fmt:
    cargo fmt --all --check

fmt-fix:
    cargo fmt --all

shellcheck:
    git ls-files -z '*.sh' '*.bash' | xargs -0 --no-run-if-empty shellcheck

# Not in `default`, because it needs these tools and a GitHub token for zizmor's online audits:
#   brew install actionlint zizmor pinact jq
#   gh auth login    (the recipe uses `gh auth token` when GITHUB_TOKEN and GH_TOKEN are unset)
# `pinact run` fixes what pinact reports.
#
# lint .github/workflows as workflow-lint.yml does, and check each pin's version comment with pinact
lint-workflows:
    #!/usr/bin/env bash
    set -euo pipefail
    missing=()
    for tool in actionlint zizmor pinact jq; do
        command -v "$tool" >/dev/null || missing+=("$tool")
    done
    if [[ ${#missing[@]} -gt 0 ]]; then
        echo "lint-workflows: missing ${missing[*]}; install with: brew install actionlint zizmor pinact jq" >&2
        exit 1
    fi
    # zizmor reads GH_TOKEN and pinact reads GITHUB_TOKEN, so the recipe sets both.
    token="${GITHUB_TOKEN:-${GH_TOKEN:-$(gh auth token 2>/dev/null || true)}}"
    if [[ -z "$token" ]]; then
        echo "lint-workflows: set GITHUB_TOKEN, or run gh auth login" >&2
        exit 1
    fi
    export GITHUB_TOKEN="$token" GH_TOKEN="$token"
    # Each check runs even when an earlier one fails, so one run reports every failure.
    status=0
    echo "== actionlint"
    actionlint || status=1
    # Keep the zizmor arguments in step with workflow-lint.yml.
    echo "== zizmor"
    zizmor --min-confidence medium --min-severity medium .github/workflows/ || status=1
    echo "== zizmor artipacked"
    artipacked_findings=$(zizmor --offline --no-exit-codes --strict-collection --format json .github/workflows/ \
        | jq -r '.[] | select(.ident == "artipacked" and (.ignored | not))
            | .locations[0] as $l
            | "\($l.symbolic.key.Local.verbatim_path):\($l.concrete.location.start_point.row + 1): artipacked: \($l.symbolic.annotation)"') || status=1
    if [[ -n "$artipacked_findings" ]]; then
        printf '%s\n' "$artipacked_findings" >&2
        status=1
    fi
    echo "== pinact"
    pinact run --check || status=1
    exit "$status"

# check that compose.yaml loads with no .env and no variables set, as on a fresh clone
compose-check:
    #!/usr/bin/env bash
    set -euo pipefail
    # dotenv-load puts .env in this recipe's environment, and compose reads that
    # environment before the defaults, so compose runs with an empty environment.
    status=0
    stderr=$(env -i PATH="$PATH" HOME="$HOME" docker compose --env-file /dev/null config --quiet 2>&1 >/dev/null) || status=$?
    if [[ -n "$stderr" ]]; then
        printf '%s\n' "$stderr" >&2
    fi
    # The check fails only on a load error or an unset variable, and prints
    # other warnings, because the runner's Compose version is not pinned and a
    # newer one can add a warning for an unchanged file.
    if [[ $status -ne 0 ]] || grep -q 'variable is not set' <<<"$stderr"; then
        echo "compose-check: compose.yaml must load with no .env and give each variable a default" >&2
        exit 1
    fi

miri:
    cargo +nightly miri test --no-fail-fast --all-features

docker_arch := if arch() == "aarch64" { "arm64" } else { "amd64" }

# build the static musl binary the Dockerfile packages into dist/linux/<arch>/nisshi
# (needs zig, cargo-zigbuild and cargo-about; works on macOS and Linux hosts)
docker-dist arch=docker_arch:
    #!/usr/bin/env bash
    set -euo pipefail
    case "{{ arch }}" in
        amd64) target=x86_64-unknown-linux-musl ;;
        arm64) target=aarch64-unknown-linux-musl ;;
        *) echo "unsupported arch: {{ arch }} (expected amd64 or arm64)" >&2; exit 1 ;;
    esac
    rustup target add "${target}"
    cargo zigbuild --release --bin nisshi --all-features --target "${target}"
    # honour a build.target-dir / CARGO_TARGET_DIR override rather than assuming ./target
    target_dir=$(cargo metadata --format-version 1 --no-deps | sed -n 's/.*"target_directory":"\([^"]*\)".*/\1/p')
    mkdir -p dist/linux/{{ arch }}
    cp "${target_dir}/${target}"/release/nisshi dist/linux/{{ arch }}/nisshi
    cargo about generate --all-features about.hbs > dist/linux/{{ arch }}/THIRD-PARTY-LICENSE.html

docker-build: docker-dist
    docker build --tag ghcr.io/nisshi-io/nisshi --progress plain --debug .

docker-build-cross: (docker-dist "amd64") (docker-dist "arm64")
    docker build --tag ghcr.io/nisshi-io/nisshi --progress plain --platform linux/amd64,linux/arm64 --debug .

minio-up: (docker-compose-up "minio")

minio-down: (docker-compose-down "minio")

minio-mc +args:
    docker compose exec minio /usr/bin/mc {{ args }}

minio-local-alias: (minio-mc "alias" "set" "local" "http://localhost:9000" "minioadmin" "minioadmin")

minio-nisshi-bucket: (minio-mc "mb" "local/nisshi")

minio-lake-bucket: (minio-mc "mb" "local/lake")

minio-ready-local: (minio-mc "ready" "local")

nisshi-up: (docker-compose-up "nisshi")

nisshi-down: (docker-compose-down "nisshi")

db-up: (docker-compose-up "db")

db-down: (docker-compose-down "db")

jaeger-up: (docker-compose-up "jaeger")

jaeger-down: (docker-compose-down "jaeger")

prometheus-up: (docker-compose-up "prometheus")

prometheus-down: (docker-compose-down "prometheus")

grafana-up: (docker-compose-up "grafana")

grafana-down: (docker-compose-down "grafana")

grafana-ui:
    open http://localhost:3000

lakehouse-catalog-up: (docker-compose-up "lakehouse-catalog")

lakehouse-catalog-down: (docker-compose-down "lakehouse-catalog")

# the lakehouse catalog's server, from ICEBERG_CATALOG, which .env names
lakehouse_url := trim_end_match(env("ICEBERG_CATALOG", "http://localhost:8181/catalog"), "/catalog")

lakehouse-accept-terms-of-use:
    curl {{ lakehouse_url }}/management/v1/bootstrap -H "Content-Type: application/json" --data '{"accept-terms-of-use": true}'

lakehouse-create-warehouse:
    curl {{ lakehouse_url }}/management/v1/warehouse -H "Content-Type: application/json" --data @etc/lakekeeper/create-default-warehouse.json

lakehouse-migrate:
    docker compose exec lakehouse-catalog /home/nonroot/iceberg-catalog migrate

docker-compose-up *args:
    docker compose --ansi never --progress plain up --no-color --quiet-pull --wait --detach {{ args }}

docker-compose-down *args:
    #!/usr/bin/env bash
    set -euo pipefail
    # Before services-up has written COMPOSE_PROJECT_NAME to .env, a worktree's
    # compose commands act on the default project, which the main checkout and
    # other worktrees use, so this recipe refuses to remove it.
    if [[ {{ worktree_services }} == true && "${COMPOSE_PROJECT_NAME:-}" != nisshi-wt-* ]]; then
        echo "docker-compose-down: run 'just services-up' first, so that this removes this worktree's services and not the default project's" >&2
        exit 1
    fi
    docker compose down --remove-orphans --volumes {{ args }}

ps:
    docker compose ps

docker-compose-logs *args:
    docker compose logs {{ args }}

psql:
    docker compose exec db psql $*

docker-run-postgres:
    docker run \
        --detach \
        --name postgres \
        --publish 5432:5432 \
        --env PGUSER=postgres \
        --env POSTGRES_PASSWORD=postgres \
        postgres:16.4@sha256:e62fbf9d3e2b49816a32c400ed2dba83e3b361e6833e624024309c35d334b412

docker-prune:
    docker system prune --force

docker-run:
    docker run --detach --name nisshi --publish 9092:9092 nisshi

docker-rm-f:
    docker rm --force nisshi

list-topics:
    kafka-topics --bootstrap-server ${ADVERTISED_LISTENER} --command-config etc/kafka-client/command.properties --list

list-topics-plain:
    kafka-topics --bootstrap-server ${ADVERTISED_LISTENER} --command-config etc/kafka-client/command-plain.properties --list

list-topics-scram-256:
    kafka-topics --bootstrap-server ${ADVERTISED_LISTENER} --command-config etc/kafka-client/command-scram-256.properties --list

list-topics-scram-512:
    kafka-topics --bootstrap-server ${ADVERTISED_LISTENER} --command-config etc/kafka-client/command-scram-512.properties --list

user-create user password profile mechanism="scram512":
    target/{{ replace(profile, "dev", "debug") }}/nisshi user create {{ user }} {{ password }} --mechanism {{ mechanism }}

add-alice-user profile="dev": (user-create "alice" "secret" profile "scram256") (user-create "alice" "secret" profile "scram512")

user-delete user profile mechanism="scram512":
    target/{{ replace(profile, "dev", "debug") }}/nisshi user delete {{ user }} --mechanism {{ mechanism }}

delete-alice-user profile="dev": (user-delete "alice" profile "scram256") (user-delete "alice" profile "scram512")

# add-alice-user:
#    kafka-configs --alter --add-config "SCRAM-SHA-256=[iterations=8192,password=secret],SCRAM-SHA-512=[iterations=8192,password=secret]" --entity-type users --entity-name alice --bootstrap-server localhost:9092

test-topic-describe:
    kafka-topics --bootstrap-server ${ADVERTISED_LISTENER} --describe --topic test

test-topic-create:
    kafka-topics --bootstrap-server ${ADVERTISED_LISTENER} --config cleanup.policy=compact --partitions=3 --replication-factor=1 --create --topic test

test-topic-create-1m-retention:
    kafka-topics --bootstrap-server ${ADVERTISED_LISTENER} --config cleanup.policy=delete --config retention.ms=60000 --partitions=3 --replication-factor=1 --create --topic test

test-topic-alter:
    kafka-configs --bootstrap-server ${ADVERTISED_LISTENER} --alter --entity-type topics --entity-name test --add-config retention.ms=3600000,retention.bytes=524288000

test-topic-delete:
    kafka-topics --bootstrap-server ${ADVERTISED_LISTENER} --delete --topic test

test-topic-get-offsets-earliest:
    kafka-get-offsets --bootstrap-server ${ADVERTISED_LISTENER} --topic test --time earliest

test-topic-get-offsets-latest:
    kafka-get-offsets --bootstrap-server ${ADVERTISED_LISTENER} --topic test --time latest

test-topic-produce:
    echo "h1:pqr,h2:jkl,h3:uio	qwerty	poiuy\nh1:def,h2:lmn,h3:xyz	asdfgh	lkj\nh1:stu,h2:fgh,h3:ijk	zxcvbn	mnbvc" | kafka-console-producer --bootstrap-server ${ADVERTISED_LISTENER} --topic test --property parse.headers=true --property parse.key=true

test-topic-consume:
    kafka-console-consumer --bootstrap-server ${ADVERTISED_LISTENER} --consumer-property fetch.max.wait.ms=15000 --group test-consumer-group --topic test --from-beginning --property print.timestamp=true --property print.key=true --property print.offset=true --property print.partition=true --property print.headers=true --property print.value=true

test-consumer-group-describe:
    kafka-consumer-groups --bootstrap-server ${ADVERTISED_LISTENER} --group test-consumer-group --describe

consumer-group-list:
    kafka-consumer-groups --bootstrap-server ${ADVERTISED_LISTENER} --list

test-reset-offsets-to-earliest:
    kafka-consumer-groups --bootstrap-server ${ADVERTISED_LISTENER} --group test-consumer-group --topic test:0 --reset-offsets --to-earliest --execute

topic-create topic *args:
    target/debug/nisshi topic create {{ topic }} {{ args }}

topic-delete topic:
    target/debug/nisshi topic delete {{ topic }}

cat-produce topic file:
    target/debug/nisshi cat produce {{ topic }} {{ file }}

cat-consume topic:
    target/debug/nisshi cat consume {{ topic }} --max-wait-time-ms=5000

generator topic *args:
    target/debug/nisshi generator {{ args }} {{ topic }} 2>&1 >generator.log

duckdb-k-unnest-v-parquet topic:
    duckdb -init etc/duckdb-init.sql :memory: "SELECT key,unnest(value) FROM '{{ replace(env("DATA_LAKE"), "file://./", "") }}/{{ topic }}/*/*.parquet'"

duckdb-parquet topic:
    duckdb -init etc/duckdb-init.sql :memory: "SELECT * FROM '{{ replace(env("DATA_LAKE"), "file://./", "") }}/{{ topic }}/*/*.parquet'"

# create person topic with schema etc/schema/person.json
person-topic-create: (topic-create "person")

# delete person topic
person-topic-delete: (topic-delete "person")

# produce etc/data/persons.json with schema etc/schema/person.json
person-topic-populate: (cat-produce "person" "etc/data/persons.json")

# produce valid data, that is accepted by the broker
person-topic-produce-valid:
    echo '{"key": "345-67-6543", "value": {"firstName": "John", "lastName": "Doe", "age": 21}}' | target/debug/nisshi cat produce person

# produce invalid data, that is rejected by the broker
person-topic-produce-invalid:
    echo '{"key": "ABC-12-4242", "value": {"firstName": "John", "lastName": "Doe", "age": -1}}' | target/debug/nisshi cat produce person

# person parquet
person-duckdb-parquet: (duckdb-k-unnest-v-parquet "person")

person-topic-consume:
    kafka-console-consumer \
        --bootstrap-server ${ADVERTISED_LISTENER} \
        --timeout-ms=15000 \
        --group person-consumer-group \
        --topic person \
        --from-beginning \
        --formatter-property print.timestamp=true \
        --formatter-property print.key=true \
        --formatter-property print.offset=true \
        --formatter-property print.partition=true \
        --formatter-property print.headers=true \
        --formatter-property print.value=true

# create search topic with etc/schema/search.proto
search-topic-create: (topic-create "search")

# delete search topic
search-topic-delete: (topic-delete "search")

# produce data to search topic with etc/schema/search.proto
search-topic-produce:
    echo '{"value": {"query": "abc/def", "page_number": 6, "results_per_page": 13, "corpus": "CORPUS_WEB"}}' | target/debug/nisshi cat produce search

# search parquet
search-duckdb-parquet: (duckdb-parquet "search")

nisshi-server:
    target/debug/nisshi broker --schema-registry file://./etc/schema 2>&1 | tee broker.log

kafka39:
    docker run --d -p 9092:9092 apache/kafka:3.9.0@sha256:fbc7d7c428e3755cf36518d4976596002477e4c052d1f80b5b9eafd06d0fff2f

proxy-kafka39:
    docker run --rm -p 19092:9092 apache/kafka:3.9.0@sha256:fbc7d7c428e3755cf36518d4976596002477e4c052d1f80b5b9eafd06d0fff2f

proxy-kafka41:
    docker run --rm -p 19092:9092 apache/kafka:4.1.0@sha256:bff074a5d0051dbc0bbbcd25b045bb1fe84833ec0d3c7c965d1797dd289ec88f

codespace-create:
    gh codespace create \
        --repo $(gh repo view --json nameWithOwner --jq .nameWithOwner) \
        --branch $(git branch --show-current) \
        --machine basicLinux32gb

codespace-delete:
    gh codespace ls \
        --repo $(gh repo view \
            --json nameWithOwner \
            --jq .nameWithOwner) \
        --json name \
        --jq '.[].name' | xargs --no-run-if-empty -n1 gh codespace delete --codespace

codespace-logs:
    gh codespace logs \
        --codespace $(gh codespace ls \
            --repo $(gh repo view \
                --json nameWithOwner \
                --jq .nameWithOwner) \
            --json name \
            --jq '.[].name')

codespace-ls:
    gh codespace list \
        --repo $(gh repo view \
            --json nameWithOwner \
            --jq .nameWithOwner)

codespace-ssh:
    gh codespace ssh \
        --codespace $(gh codespace ls \
            --repo $(gh repo view \
                --json nameWithOwner \
                --jq .nameWithOwner) \
            --json name \
            --jq '.[].name')

all: test miri

flamegraph *args:
    cargo flamegraph {{ args }}

benchmark-flamegraph: build docker-compose-down minio-up minio-ready-local minio-local-alias minio-nisshi-bucket prometheus-up grafana-up
    flamegraph -- target/debug/nisshi broker 2>&1  | tee broker.log

benchmark: build docker-compose-down minio-up minio-ready-local minio-local-alias minio-nisshi-bucket prometheus-up grafana-up
    target/debug/nisshi broker 2>&1  | tee broker.log

otel profile="dev" *args: build docker-compose-down db-up minio-up minio-ready-local minio-local-alias minio-nisshi-bucket prometheus-up grafana-up
    OTEL_METRIC_EXPORT_INTERVAL=5000 OTEL_EXPORTER_OTLP_ENDPOINT="http://localhost:9090/api/v1/otlp/" target/{{ replace(profile, "dev", "debug") }}/nisshi broker {{ args }}  | tee broker.log

otel-up: docker-compose-down db-up minio-up minio-ready-local minio-local-alias minio-nisshi-bucket prometheus-up grafana-up nisshi-up

nisshi-broker profile *args:
    target/{{ replace(profile, "dev", "debug") }}/nisshi broker {{ args }} 2>&1 >broker.log

flamegraph-nisshi-broker profile *args:
    #!/usr/bin/env zsh
    unset SCHEMA_REGISTRY
    export RUST_LOG=warn
    flamegraph --verbose -- ./target/{{ replace(profile, "dev", "debug") }}/nisshi broker {{ args }}

# tear down the default compose project and run a debug broker with configuration from .env
broker-default-project *args: build docker-compose-down prometheus-up grafana-up db-up minio-up minio-ready-local minio-local-alias minio-nisshi-bucket minio-lake-bucket lakehouse-catalog-up (nisshi-broker "debug" args)

# run a release broker with configuration from .env
broker-release *args: release docker-compose-down prometheus-up grafana-up db-up minio-up minio-ready-local minio-local-alias minio-nisshi-bucket minio-lake-bucket lakehouse-catalog-up (nisshi-broker "release" args)

# run a proxy with configuration from .env
proxy *args:
    target/debug/nisshi proxy {{ args }} 2>&1 | tee proxy.log

# teardown compose, rebuild: minio, db, nisshi and lake buckets
server-default-project: (cargo-build "--bin" "nisshi") docker-compose-down db-up minio-up minio-ready-local minio-local-alias minio-nisshi-bucket minio-lake-bucket lakehouse-catalog-up
    target/debug/nisshi broker 2>&1  | tee broker.log

gdb: (cargo-build "--profile" "debugging" "--bin" "nisshi") docker-compose-down db-up minio-up minio-ready-local minio-local-alias minio-nisshi-bucket minio-lake-bucket
    rust-gdb --args target/debugging/nisshi broker

lldb: (cargo-build "--profile" "debugging" "--bin" "nisshi") docker-compose-down db-up minio-up minio-ready-local minio-local-alias minio-nisshi-bucket minio-lake-bucket lakehouse-catalog-up
    rust-lldb target/debugging/nisshi broker

ci-default-project: docker-compose-down db-up minio-up minio-ready-local minio-local-alias minio-nisshi-bucket minio-lake-bucket lakehouse-catalog-up lakehouse-accept-terms-of-use lakehouse-create-warehouse

# true in a linked worktree outside CI, where the services run in the
# worktree's own compose project (etc/worktree-services.sh), so that `ci`,
# `broker` and `server` never remove the services that other worktrees use
worktree_services := if env("CI", "") != "" { "false" } else if env("NISSHI_WORKTREE_SERVICES", "") == "0" { "false" } else if `git rev-parse --path-format=absolute --git-dir` == `git rev-parse --path-format=absolute --git-common-dir` { "false" } else { "true" }

# start this worktree's services and write their ports to .env (does nothing in the main checkout)
services-up:
    etc/worktree-services.sh up

# remove this worktree's services and their volumes (does nothing in the main checkout)
services-down:
    etc/worktree-services.sh down

# remove the services of each removed worktree
services-prune:
    etc/worktree-services.sh prune

# (re)start the services that the tests need, with empty volumes: this worktree's own, or in the main checkout, the default project's
ci:
    #!/usr/bin/env bash
    set -euo pipefail
    if [[ {{ worktree_services }} == false ]]; then
        exec {{ quote(just_executable()) }} ci-default-project
    fi
    etc/worktree-services.sh down
    etc/worktree-services.sh up

# run a debug broker with configuration from .env
broker *args:
    #!/usr/bin/env bash
    set -euo pipefail
    if [[ {{ worktree_services }} == false ]]; then
        exec {{ quote(just_executable()) }} broker-default-project {{ args }}
    fi
    {{ quote(just_executable()) }} build
    etc/worktree-services.sh up
    # just read .env before the script wrote this worktree's ports to it.
    set -a
    # shellcheck disable=SC1091
    source .env
    set +a
    docker compose --ansi never up --wait --detach --quiet-pull prometheus grafana
    target/debug/nisshi broker {{ args }} 2>&1 >broker.log

# start this worktree's services (in the main checkout, rebuild the default project's) and run a debug broker
server:
    #!/usr/bin/env bash
    set -euo pipefail
    if [[ {{ worktree_services }} == false ]]; then
        exec {{ quote(just_executable()) }} server-default-project
    fi
    cargo build --bin nisshi
    etc/worktree-services.sh up
    # just read .env before the script wrote this worktree's ports to it.
    set -a
    # shellcheck disable=SC1091
    source .env
    set +a
    target/debug/nisshi broker 2>&1 | tee broker.log

# bring up postgres and minio (with the nisshi bucket), without the lakehouse catalog
compat-infra-up: docker-compose-down db-up minio-up minio-ready-local minio-local-alias minio-nisshi-bucket

# produce etc/data/observations.json with schema etc/schema/observation.avsc
observation-produce: (cat-produce "observation" "etc/data/observations.json")

# consume observation topic with schema etc/schema/observation.avsc
observation-consume: (cat-consume "observation")

# create observation topic with schema etc/schema/observation.avsc
observation-topic-create: (topic-create "observation")

# observation parquet
observation-duckdb-parquet: (duckdb-k-unnest-v-parquet "observation")

duckdb *sql:
    duckdb -init etc/duckdb-init.sql -markdown :memory: {{ sql }}

# produce etc/data/trips.json with schema etc/schema/taxi.proto
taxi-topic-populate: (cat-produce "taxi" "etc/data/trips.json")

# consume taxi topic with schema etc/schema/taxi.proto
taxi-topic-consume: (cat-consume "taxi")

# create taxi topic with generated fields with schema etc/schema/taxi.proto
taxi-topic-create: (topic-create "taxi" "--partitions=1" "--config=nisshi.lake.normalize=true" "--config=nisshi.lake.partition=meta.day" "--config=nisshi.lake.z_order=vendor_id" "--config=nisshi.lake.sink=true" "--config=nisshi.batch=true" "--config=nisshi.batch.max_records=200" "--config=nisshi.batch.timeout_ms=1000")

# create taxi topic with schema etc/schema/taxi.proto
taxi-topic-create-plain: (topic-create "taxi" "--partitions" "1" "--config" "nisshi.lake.sink=true")

# create taxi topic with a flattened schema etc/schema/taxi.proto
taxi-topic-create-normalize: (topic-create "taxi" "--partitions" "1" "--config" "nisshi.lake.sink=true" "--config" "nisshi.lake.normalize=true" "--config" "nisshi.lake.normalize.separator=_" "--config" "nisshi.lake.z_order=value_vendor_id")

taxi-topic-generator: (generator "taxi" "--broker=tcp://localhost:9092" "--per-second=10" "--producers=16" "--batch-size=1" "--duration-seconds=60")

# delete taxi topic
taxi-topic-delete: (topic-delete "taxi")

# taxi parquet
taxi-duckdb-parquet: (duckdb-parquet "taxi")

# taxi duckdb delta lake
taxi-duckdb-delta: (duckdb "\"select * from delta_scan('s3://lake/nisshi.taxi');\"")

# create employee topic with etc/schema/employee.proto
employee-topic-create: (topic-create "employee")

# produce etc/data/persons.json with etc/schema/person.json
employee-produce: (cat-produce "employee" "etc/data/employees.json")

# employee duckdb delta lake
employee-duckdb-delta: (duckdb "\"select * from delta_scan('s3://lake/nisshi.employee');\"")

# create customer topic with schema etc/schema/customer.proto
customer-topic-create *args: (topic-create "customer" "--partitions=1" "--config=nisshi.lake.normalize=true" "--config=nisshi.lake.partition=meta.day" "--config=nisshi.lake.sink=true" "--config=nisshi.batch=true" "--config=nisshi.batch.max_records=200" "--config=nisshi.batch.timeout_ms=1000" args)

customer-topic-generator *args: (generator "customer" args)

customer-duckdb-delta: (duckdb "\"select * from delta_scan('s3://lake/nisshi.customer');\"")

broker-memory profile="profiling": (build profile "dynostore") (nisshi-broker profile "--storage-engine=memory://" "--advertised-listener-url=tcp://127.0.0.1:9092/" "--silent")

# run the librdkafka integration test suite against the given storage engine; some of its tests
# rely on auto-created topics having more than one partition, so the broker uses 4
compat-librdkafka storage="memory://" features="dynostore": clean-nisshi-db (build "dev" features)
    #!/usr/bin/env bash
    set -euo pipefail
    ./target/debug/nisshi broker --storage-engine={{ storage }} \
        --advertised-listener-url=tcp://127.0.0.1:9092 --num-partitions=4 &
    broker=$!
    trap 'kill ${broker}' EXIT
    BROKER_PID=${broker} ./compat/librdkafka/run.sh

# run the franz-go integration test suite against the given storage engine
compat-franz-go storage="memory://" features="dynostore": clean-nisshi-db (build "dev" features)
    #!/usr/bin/env bash
    set -euo pipefail
    ./target/debug/nisshi broker --storage-engine={{ storage }} \
        --advertised-listener-url=tcp://127.0.0.1:9092 &
    broker=$!
    trap 'kill ${broker}' EXIT
    BROKER_PID=${broker} ./compat/franz-go/run.sh

# run the Kafka CLI smoke suite (nisshi-smoke-test) against postgres, sqlite, memory or s3 (needs Docker; starts its own broker and services); passes extra arguments to `cargo nextest run`
[positional-arguments]
smoke engine *nextest_args:
    nisshi-smoke-test/run.sh "$@"

compat-franz-go-test tests timeout="600s" count="1":
    #!/usr/bin/env bash
    cd target/compat/franz-go/pkg/kgo
    export KGO_TEST_RF=1
    export KGO_LOG_LEVEL=debug
    go test -count={{ count }} -timeout {{ timeout }} -v -run {{ tests }} .

broker-null profile="profiling": (build profile "default") (nisshi-broker profile "--storage-engine=null://")

clean-nisshi-db:
    rm -f nisshi.db* snapshot.db

clean-lake-dir:
    rm -rf lake/*

broker-sqlite-parquet profile="dev": clean-nisshi-db clean-lake-dir (build profile "libsql,parquet") (nisshi-broker profile "--storage-engine=sqlite://nisshi.db" "parquet" "--location=file://./lake")

broker-sqlite-delta profile="profiling": docker-compose-down minio-up minio-ready-local minio-local-alias minio-lake-bucket clean-nisshi-db (build profile "libsql,delta") (nisshi-broker profile "--storage-engine=sqlite://nisshi.db" "delta")

broker-sqlite profile="profiling": clean-nisshi-db (build profile "libsql") (nisshi-broker profile "--silent" "--storage-engine=sqlite://nisshi.db" "--advertised-listener-url=tcp://127.0.0.1:9092")

broker-sqlite-existing profile="profiling": (build profile "libsql") (nisshi-broker profile "--silent" "--storage-engine=sqlite://nisshi.db")

broker-sqlite-no-maintenance profile="profiling": clean-nisshi-db (build profile "libsql") (nisshi-broker profile "--silent" "--storage-engine='sqlite://nisshi.db'")

broker-sqlite-authentication profile="profiling": (build profile "libsql") (nisshi-broker profile "--authentication" "--storage-engine=sqlite://nisshi.db")

broker-sqlite-maintenance-1m profile="profiling": clean-nisshi-db (build profile "libsql") (nisshi-broker profile "--storage-engine=sqlite://nisshi.db?maintenance_interval=1m")

broker-sqlite-vacuum-into profile="profiling": clean-nisshi-db (build profile "libsql") (nisshi-broker profile "--storage-engine=sqlite://nisshi.db?vacuum_into=snapshot.db")

s3-up: docker-compose-down minio-up minio-ready-local minio-local-alias minio-nisshi-bucket

broker-s3 profile="profiling": (build profile "dynostore") s3-up (nisshi-broker profile "--storage-engine=s3://nisshi/" "--silent" "--advertised-listener-url=tcp://127.0.0.1:9092")

broker-postgres profile="profiling": (build profile "postgres") docker-compose-down db-up (nisshi-broker profile "--storage-engine=postgres://postgres:postgres@localhost" "--silent" "--advertised-listener-url=tcp://127.0.0.1:9092")

broker-postgres-existing profile="profiling": (build profile "postgres") (nisshi-broker profile "--silent" "--storage-engine=postgres://postgres:postgres@localhost")

broker-postgres-local profile="profiling": (build profile "postgres") (nisshi-broker profile "--silent" "--storage-engine=postgres://pmorgan@localhost/pmorgan")

broker-postgres-authentication profile="profiling": (build profile "postgres") (nisshi-broker profile "--authentication" "--storage-engine=postgres://postgres:postgres@localhost")

broker-postgres-maintenance-1m profile="profiling": (build profile "postgres") (nisshi-broker profile "--storage-engine=postgres://postgres:postgres@localhost?maintenance_interval=1m")

samply-null profile="profiling":
    cargo build --profile {{ profile }} --bin nisshi
    RUST_LOG=warn samply record ./target/{{ replace(profile, "dev", "debug") }}/nisshi --storage-engine=null://sink

flamegraph-null profile="profiling": (build profile "default") (flamegraph-nisshi-broker profile "--storage-engine=null://sink")

flamegraph-sqlite profile="profiling": (build profile "libsql") clean-nisshi-db (flamegraph-nisshi-broker profile "--storage-engine=sqlite://nisshi.db")

flamegraph-postgres profile="profiling": (build profile "postgres") docker-compose-down db-up (flamegraph-nisshi-broker profile "--storage-engine=postgres://postgres:postgres@localhost")

flamegraph-memory profile="profiling": (build profile "dynostore") (flamegraph-nisshi-broker profile "--storage-engine=memory://nisshi/")

flamegraph-s3 profile="profiling": (build profile "dynostore") docker-compose-down minio-up minio-ready-local minio-local-alias minio-nisshi-bucket (flamegraph-nisshi-broker profile "--storage-engine=s3://nisshi/")

samply-produce profile="profiling":
    cargo build --profile {{ profile }} --bin bench_produce_v11
    RUST_LOG=warn samply record ./target/{{ replace(profile, "dev", "debug") }}/bench_produce_v11

flamegraph-produce profile="profiling":
    cargo build --profile {{ profile }} --bin bench_produce_v11
    RUST_LOG=warn flamegraph -- ./target/{{ replace(profile, "dev", "debug") }}/bench_produce_v11

bench-hyperfine iterations="100000" profile="release": (build profile "libsql" "bench")
    hyperfine -N './target/{{ replace(profile, "dev", "debug") }}/bench --iterations {{ iterations }}'

bench-dhat mode="heap" profile="release": (build profile "libsql" "bench")
    valgrind --tool=dhat --mode={{ mode }} ./target/{{ replace(profile, "dev", "debug") }}/bench

bench-flamegraph profile="profiling": (build profile "libsql" "bench")
    RUST_LOG=warn flamegraph -- ./target/{{ replace(profile, "dev", "debug") }}/bench

bench-flamegraph-produce profile="profiling": (build profile "libsql" "bench")
    RUST_LOG=warn flamegraph -- ./target/{{ replace(profile, "dev", "debug") }}/bench --api-key=0

bench-flamegraph-fetch iterations="100000" profile="profiling": (build profile "libsql" "bench")
    RUST_LOG=warn flamegraph -- ./target/{{ replace(profile, "dev", "debug") }}/bench  --iterations {{ iterations }} --api-key=1

bench-perf profile="profiling": (build profile "libsql" "bench")
    RUST_LOG=warn perf record --call-graph dwarf ./target/{{ replace(profile, "dev", "debug") }}/bench 2>&1 >/dev/null

consumer-perf num_records="1000" topic="test":
    kafka-consumer-perf-test --topic {{ topic }} --num-records {{ num_records }} --bootstrap-server ${ADVERTISED_LISTENER}

soak-producer-perf seconds throughput="1000" record_size="1024":
    kafka-producer-perf-test --topic test --warmup-records {{ throughput }} --num-records $(({{ seconds }} * {{ throughput }})) --record-size {{ record_size }} --throughput {{ throughput }} --command-property bootstrap.servers=${ADVERTISED_LISTENER}

soak-producer-perf-500: (soak-producer-perf "600" "500" "1024")

soak-producer-perf-1000: (soak-producer-perf "3600" "1000" "1024")

producer-perf throughput="1000" record_size="1024" num_records="100000" topic="test":
    kafka-producer-perf-test --topic {{ topic }} --warmup-records {{ throughput }} --num-records $(({{ num_records }} + {{ throughput }})) --record-size {{ record_size }} --throughput {{ throughput }} --command-property bootstrap.servers=${ADVERTISED_LISTENER}

producer-perf-10: (producer-perf "10")

producer-perf-1000: (producer-perf "1000" "1024" "25000")

producer-perf-2000: (producer-perf "2000" "1024" "50000")

producer-perf-3000: (producer-perf "3000" "1024" "75000")

producer-perf-4000: (producer-perf "4000" "1024" "100000")

producer-perf-5000: (producer-perf "5000" "1024" "125000")

producer-perf-6000: (producer-perf "6000" "1024" "150000")

producer-perf-7000: (producer-perf "7000" "1024" "175000")

producer-perf-8000: (producer-perf "8000" "1024" "200000")

producer-perf-9000: (producer-perf "9000" "1024" "225000")

producer-perf-10000: (producer-perf "10000" "1024" "250000")

producer-perf-15000: (producer-perf "15000" "1024" "375000")

producer-perf-20000: (producer-perf "20000" "1024" "500000")

producer-perf-30000: (producer-perf "30000" "1024" "750000")

producer-perf-40000: (producer-perf "40000" "1024" "1000000")

producer-perf-45000: (producer-perf "45000" "1024" "1100000")

producer-perf-50000: (producer-perf "50000" "1024" "1250000")

producer-perf-60000: (producer-perf "60000" "1024" "1500000")

producer-perf-70000: (producer-perf "70000" "1024" "1750000")

producer-perf-80000: (producer-perf "80000" "1024" "2000000")

producer-perf-90000: (producer-perf "90000" "1024" "2250000")

producer-perf-100000: (producer-perf "100000" "1024" "2500000")

producer-perf-200000: (producer-perf "200000" "1024" "5000000")

producer-perf-300000: (producer-perf "300000" "1024" "7500000")

producer-perf-400000: (producer-perf "400000" "1024" "10000000")

producer-perf-500000: (producer-perf "500000" "1024" "12500000")

producer-perf-600000: (producer-perf "600000" "1024" "15000000")

producer-perf-1000000: (producer-perf "1000000" "1024" "25000000")

ps-nisshi-rss:
    ps -p $(pgrep nisshi) -o rss= | awk '{print $1/1024 " MB"}'

telemetry-topic-create: (topic-create "telemetry" "--config" "nisshi.virtual=true")

telemetry-produce-valid profile="dev":
    echo '{"key": "SK06 YPM", "value": {"latitude":52.930412156530465,"longitude":-4.894550244518114,"altitude":158.06766871179406}}' | target/{{ replace(profile, "dev", "debug") }}/nisshi cat produce telemetry

telemetry-consume:
    kafka-console-consumer \
        --bootstrap-server ${ADVERTISED_LISTENER} \
        --timeout-ms=15000 \
        --group telemetry-consumer-group \
        --topic telemetry \
        --from-beginning \
        --formatter-property print.timestamp=true \
        --formatter-property print.key=true \
        --formatter-property print.offset=true \
        --formatter-property print.partition=true \
        --formatter-property print.headers=true \
        --formatter-property print.value=true

telemetry-vrm-consume vrm="SK06 YPM":
    kafka-console-consumer \
        --bootstrap-server ${ADVERTISED_LISTENER} \
        --timeout-ms=15000 \
        --group telemetry-sk06-consumer-group \
        --topic 'telemetry/"{{ vrm }}"' \
        --from-beginning \
        --formatter-property print.timestamp=true \
        --formatter-property print.key=true \
        --formatter-property print.offset=true \
        --formatter-property print.partition=true \
        --formatter-property print.headers=true \
        --formatter-property print.value=true

postgres-local:
    LC_ALL="en_US.UTF-8" /opt/homebrew/opt/postgresql@18/bin/postgres -D /opt/homebrew/var/postgresql@18

group-consumer-example topics="test":
    cargo run --package nisshi-client --example group_consumer -- --topics {{ topics }}

la profile="dev" *args:
    target/{{ replace(profile, "dev", "debug") }}/la {{ args }}
