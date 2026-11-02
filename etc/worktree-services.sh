#!/usr/bin/env bash
#
# Runs the services that the tests need (db, minio and lakehouse-catalog) in a
# compose project of this worktree's own, so that worktrees testing at once
# never share or remove each other's services:
#
#   etc/worktree-services.sh up      start or update the project, and write .env
#   etc/worktree-services.sh down    remove the project and its volumes
#   etc/worktree-services.sh prune   remove the project of each removed worktree
#
# The script does nothing in the main checkout, when CI is set, or when
# NISSHI_WORKTREE_SERVICES is 0: there the services run in the default
# project, on the fixed ports in compose.yaml, and `just ci` starts them.
#
# The project is named nisshi-wt-<worktree directory>-<hash of its path>, and
# its services publish their ports on loopback ports that Docker picks. The
# hash keeps two worktrees with the same directory name, such as the same
# ticket in two clones, from sharing a project. `up` writes those
# ports to .env, as AWS_ENDPOINT, ICEBERG_CATALOG and NISSHI_TEST_DATABASE_URL,
# and writes COMPOSE_PROJECT_NAME and the NISSHI_*_PORTS variables there too, so
# that a later `docker compose` or `just` command in the worktree uses this
# project. When nextest runs `up` as a setup script, it also writes them to
# $NEXTEST_ENV, because a test process doesn't take a variable from .env that
# is already in its environment.
#
# NISSHI_WORKTREE_PROJECT names another project instead, which must start
# with nisshi-wt-, to run a second project beside the first.

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

prefix=nisshi-wt-

is_linked_worktree() {
    [[ "$(git rev-parse --path-format=absolute --git-dir)" != "$(git rev-parse --path-format=absolute --git-common-dir)" ]]
}

if [[ -n "${CI:-}" || "${NISSHI_WORKTREE_SERVICES:-}" == 0 ]] || ! is_linked_worktree; then
    exit 0
fi

# Returns the project name of the worktree at the given path. Compose accepts
# only lowercase letters, digits, dashes and underscores in a project name.
project_of() {
    local name hash
    name=$(basename "$1" | tr '[:upper:]' '[:lower:]' | tr -c 'a-z0-9_\n-' '-')
    hash=$(printf '%s' "$1" | git hash-object --stdin | cut -c1-8)
    echo "${prefix}${name}-${hash}"
}

project=${NISSHI_WORKTREE_PROJECT:-$(project_of "$(git rev-parse --show-toplevel)")}
if [[ "${project}" != "${prefix}"* ]]; then
    echo "worktree-services: NISSHI_WORKTREE_PROJECT must start with ${prefix}, not ${project}" >&2
    exit 2
fi
compose=(docker compose --project-name "${project}" --ansi never)

# Docker picks each host port, and keeps it while the container runs. A
# container that is created again can get another port, so `up` writes .env
# again each time.
export NISSHI_DB_PORTS=127.0.0.1::5432
export NISSHI_MINIO_PORTS=127.0.0.1::9000
export NISSHI_MINIO_CONSOLE_PORTS=127.0.0.1::9001
export NISSHI_LAKEHOUSE_PORTS=127.0.0.1::8181

# Removes the project of each worktree whose directory is gone. Compose labels
# each container with the directory it ran from, which is the worktree. The
# function removes a project only when its name is the name that this script
# gives that directory, so it never touches the default project, the smoke
# suite's projects, another tool's, or a project that NISSHI_WORKTREE_PROJECT
# named.
prune() {
    local name working_dir
    for name in $(docker compose ls --all --quiet --filter "name=${prefix}"); do
        working_dir=$(docker ps --all --filter "label=com.docker.compose.project=${name}" \
            --format '{{.Label "com.docker.compose.project.working_dir"}}' | head -n 1)
        if [[ -z "${working_dir}" || -e "${working_dir}" || "$(project_of "${working_dir}")" != "${name}" ]]; then
            continue
        fi
        echo "worktree-services: removing ${name}, whose worktree ${working_dir} is gone" >&2
        # This worktree's compose.yaml names the same services and volumes, so
        # down finds them by the project's name.
        docker compose --project-name "${name}" --ansi never down --volumes --remove-orphans >&2 ||
            echo "worktree-services: could not remove ${name}" >&2
    done
}

# Checks that the tools that `up` uses are here and that Docker answers, so
# that a missing one fails with its name instead of with a command's error.
check_prerequisites() {
    local tool
    for tool in docker jq curl; do
        if ! command -v "${tool}" >/dev/null; then
            echo "worktree-services: ${tool} isn't installed. Install it, or set NISSHI_WORKTREE_SERVICES=0 to use the default project's services." >&2
            exit 1
        fi
    done
    if ! docker compose version >/dev/null 2>&1; then
        echo "worktree-services: docker has no compose plugin (see CONTRIBUTING.md for colima)." >&2
        exit 1
    fi
    if ! docker info >/dev/null 2>&1; then
        echo "worktree-services: Docker isn't running. Start it, or set NISSHI_WORKTREE_SERVICES=0 to use the default project's services." >&2
        exit 1
    fi
}

# Sends a request to the catalog, and exits with the step, the URL and the
# catalog's answer when the request fails.
catalog_request() {
    local step=$1 url=$2 answer
    shift 2
    if ! answer=$(curl --silent --show-error --fail-with-body --max-time 30 "${url}" "$@" 2>&1); then
        echo "worktree-services: ${step} failed at ${url}: ${answer}" >&2
        exit 1
    fi
    printf '%s' "${answer}"
}

# Replaces each KEY=... line of the file with the new value, and adds each key
# that the file doesn't have. dotenv takes the first line of a key, so every
# line of the key gets the new value.
write_env() {
    local file=$1
    shift
    local tmp
    tmp=$(mktemp "${file}.XXXXXX")
    awk '
        BEGIN {
            for (i = 1; i < ARGC; i++) {
                split(ARGV[i], kv, "=")
                key = kv[1]
                value[key] = substr(ARGV[i], length(key) + 2)
                order[i] = key
                ARGV[i] = ""
            }
            count = ARGC - 1
        }
        {
            line = $0
            sub(/^export[ \t]+/, "", line)
            split(line, kv, "=")
            if (index(line, "=") > 0 && (kv[1] in value)) {
                print kv[1] "=" value[kv[1]]
                seen[kv[1]] = 1
                next
            }
            print
        }
        END {
            for (i = 1; i <= count; i++)
                if (!(order[i] in seen)) print order[i] "=" value[order[i]]
        }' "$@" <"${file}" >"${tmp}"
    mv "${tmp}" "${file}"
}

# Takes a lock on this worktree, so two runs of `up` at once, such as two
# nextest runs, don't start the project or write .env at the same time. The
# lock is a flock(2) on a file that this script holds open on descriptor 9,
# so the kernel releases it when the script exits, even when it is killed.
# macOS lacks flock(1), so perl takes the lock on the inherited descriptor.
lock_file=$(git rev-parse --path-format=absolute --git-dir)/worktree-services.lock

take_lock() {
    local attempt
    exec 9>"${lock_file}"
    for attempt in $(seq 300); do
        if perl -MFcntl=:flock -e 'open(my $lock, ">&=", 9) or die "$!"; flock($lock, LOCK_EX | LOCK_NB) or exit 1'; then
            return
        fi
        if [[ ${attempt} == 1 ]]; then
            echo "worktree-services: waiting for another run of up in this worktree, which holds ${lock_file}" >&2
        fi
        sleep 1
    done
    echo "worktree-services: another run of up in this worktree held ${lock_file} for 5 minutes" >&2
    exit 1
}

up() {
    check_prerequisites
    # A failed prune leaves another worktree's project running, which only
    # costs memory, so it doesn't stop this worktree's tests.
    prune || echo "worktree-services: prune failed, continuing" >&2
    take_lock

    "${compose[@]}" up --detach --wait --wait-timeout 300 --quiet-pull db minio lakehouse-catalog >&2

    "${compose[@]}" exec -T minio /usr/bin/mc ready local >/dev/null
    "${compose[@]}" exec -T minio /usr/bin/mc alias set local http://localhost:9000 minioadmin minioadmin >/dev/null
    "${compose[@]}" exec -T minio /usr/bin/mc mb --ignore-existing local/nisshi local/lake >/dev/null

    local db minio catalog
    db=$("${compose[@]}" port db 5432)
    minio=$("${compose[@]}" port minio 9000)
    catalog=$("${compose[@]}" port lakehouse-catalog 8181)

    # The catalog accepts its terms of use once, and answers each later
    # bootstrap with an error, so the script checks whether that happened first.
    local info
    info=$(catalog_request "reading the catalog's state" "http://${catalog}/management/v1/info")
    if ! jq -e '.bootstrapped' <<<"${info}" >/dev/null; then
        catalog_request "accepting the catalog's terms of use" "http://${catalog}/management/v1/bootstrap" \
            -H "Content-Type: application/json" --data '{"accept-terms-of-use": true}' >/dev/null
    fi
    # The catalog answers a second create of the warehouse with a 400 error
    # that it also gives for an invalid request, so the script lists the
    # warehouses first.
    local warehouse_file=etc/lakekeeper/create-default-warehouse.json warehouses
    warehouses=$(catalog_request "listing the warehouses" "http://${catalog}/management/v1/warehouse")
    if ! jq -e --arg name "$(jq -r '."warehouse-name"' "${warehouse_file}")" 'any(.warehouses[]; .name == $name)' <<<"${warehouses}" >/dev/null; then
        catalog_request "creating the warehouse" "http://${catalog}/management/v1/warehouse" \
            -H "Content-Type: application/json" --data @"${warehouse_file}" >/dev/null
    fi

    local settings=(
        "COMPOSE_PROJECT_NAME=${project}"
        "NISSHI_DB_PORTS=${NISSHI_DB_PORTS}"
        "NISSHI_MINIO_PORTS=${NISSHI_MINIO_PORTS}"
        "NISSHI_MINIO_CONSOLE_PORTS=${NISSHI_MINIO_CONSOLE_PORTS}"
        "NISSHI_LAKEHOUSE_PORTS=${NISSHI_LAKEHOUSE_PORTS}"
        "AWS_ENDPOINT=http://${minio}"
        "ICEBERG_CATALOG=http://${catalog}/catalog"
        "NISSHI_TEST_DATABASE_URL=postgres://postgres:postgres@${db}"
    )

    if [[ ! -f .env ]]; then
        cp example.env .env
    fi
    write_env .env "${settings[@]}"

    if [[ -n "${NEXTEST_ENV:-}" ]]; then
        printf '%s\n' "${settings[@]}" >>"${NEXTEST_ENV}"
    fi

    echo "worktree-services: ${project} is up: postgres ${db}, minio ${minio}, catalog ${catalog}" >&2
}

down() {
    "${compose[@]}" down --volumes --remove-orphans
}

case "${1:-}" in
    up) up ;;
    down) down ;;
    prune) prune ;;
    *) echo "usage: etc/worktree-services.sh <up|down|prune>" >&2; exit 2 ;;
esac
