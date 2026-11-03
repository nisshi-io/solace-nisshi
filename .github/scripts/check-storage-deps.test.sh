#!/usr/bin/env bash
#
# Self-test for check-storage-deps.sh. The regression that matters is a
# package list with a forbidden crate that passes, which would let a feature
# start compiling another engine's database client again. The package lists
# here are canned, so the test needs no cargo.
#
# Run manually:  .github/scripts/check-storage-deps.test.sh
# Runs in CI in workflow-lint.yml's actionlint job.
#
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
CHECK="${SCRIPT_DIR}/check-storage-deps.sh"

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

pass_count=0
fail_count=0

# expect <case> <want> <input> <text> <args>... - run the check with <input>
# on stdin, assert it exits 0 (want=pass) or non-zero (want=fail), and that
# its output contains <text> when it isn't empty.
expect() {
  local case="$1" want="$2" input="$3" text="$4" got=pass
  shift 4
  bash "$CHECK" "$@" < "$input" > "$WORK/out" 2>&1 || got=fail
  if [ "$got" = "$want" ] && { [ -z "$text" ] || grep -q -F -- "$text" "$WORK/out"; }; then
    pass_count=$((pass_count + 1)); printf 'ok   - %s: %s\n' "$case" "$want"
  else
    fail_count=$((fail_count + 1)); printf 'FAIL - %s: want %s %s, got %s:\n' "$case" "$want" "$text" "$got"
    sed 's/^/       /' "$WORK/out"
  fi
}

# A libsql build as cargo tree prints it: the root with its path, repeated
# packages marked (*), and libsql's own sys crates.
cat > "$WORK/libsql" <<'EOF'
nisshi v0.8.1 (/work/nisshi)
dotenv v0.15.0
nisshi-storage-sql v0.8.1 (/work/nisshi-storage-sql)
libsql v0.10.0
libsql-sys v0.10.0
libsql-ffi v0.10.0
deadpool v0.13.1
tokio v1.48.0 (*)
EOF
# A postgres build: tokio-postgres-rustls must not count as tokio-postgres.
cat > "$WORK/postgres-rustls-only" <<'EOF'
nisshi v0.8.1 (/work/nisshi)
tokio-postgres-rustls v0.14.0
EOF
{ cat "$WORK/libsql"; echo "tokio-postgres v0.7.18"; echo "tokio-postgres-rustls v0.14.0"; } > "$WORK/libsql-and-postgres"
{ cat "$WORK/libsql"; echo "turso v0.8.3 (*)"; } > "$WORK/libsql-and-turso"
{ cat "$WORK/libsql"; printf 'turso v0.8.3\r\n'; } > "$WORK/libsql-and-turso-crlf"
printf 'nisshi v0.8.1 (/work/nisshi)\nlibsql v0.10.0' > "$WORK/no-trailing-newline"
grep -v '^nisshi v' "$WORK/libsql" > "$WORK/no-root"
sed 's/ v/@/' "$WORK/libsql-and-postgres" > "$WORK/other-format"
: > "$WORK/empty"

expect "allowed client only" pass "$WORK/libsql" "compiles none of: tokio-postgres turso" libsql
expect "forbidden client" fail "$WORK/libsql-and-postgres" "compiles tokio-postgres v0.7.18" libsql
expect "forbidden client marked (*)" fail "$WORK/libsql-and-turso" "compiles turso v0.8.3" libsql
expect "forbidden client with CRLF" fail "$WORK/libsql-and-turso-crlf" "compiles turso v0.8.3" libsql
expect "every forbidden client is named" fail "$WORK/libsql-and-postgres" "compiles tokio-postgres v0.7.18" dynostore
expect "a longer crate name is not a match" pass "$WORK/postgres-rustls-only" "" libsql
expect "libsql-sys alone is not libsql" pass <(grep -v '^libsql v' "$WORK/libsql") "" dynostore
expect "last line without a newline" fail "$WORK/no-trailing-newline" "compiles libsql v0.10.0" postgres
expect "empty package list" fail "$WORK/empty" "is empty" libsql
expect "no root package" fail "$WORK/no-root" "has no 'nisshi v<version>' line" libsql
expect "another output format" fail "$WORK/other-format" "has no 'nisshi v<version>' line" libsql
expect "an unknown feature" fail "$WORK/libsql" "is not a storage engine feature" no-such-engine
expect "no feature" fail "$WORK/libsql" "usage:"
expect "a feature that is a pattern" fail "$WORK/libsql" "is not a storage engine feature" '.*'

printf '\n%d passed, %d failed\n' "$pass_count" "$fail_count"
[ "$fail_count" -eq 0 ]
