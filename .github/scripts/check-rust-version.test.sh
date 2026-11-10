#!/usr/bin/env bash
#
# Self-test for check-rust-version.sh. The regression that matters is a
# package list that passes although a package's rust-version differs from the
# toolchain channel, which would let the declared minimum drift from the
# toolchain that CI tests. The package lists here are canned, so the test
# needs no cargo.
#
# Run manually:  .github/scripts/check-rust-version.test.sh
# Runs in CI in workflow-lint.yml's actionlint job.
#
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
CHECK="${SCRIPT_DIR}/check-rust-version.sh"

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

pass_count=0
fail_count=0

# expect <case> <want> <input> <text> <toolchain file>... - run the check with
# <input> on stdin, assert it exits 0 (want=pass) or non-zero (want=fail), and
# that its output contains <text> when it isn't empty.
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

toolchain() {
  printf '[toolchain]\nchannel = "%s"\ncomponents = ["rustfmt", "clippy"]\n' "$1" > "$WORK/$2"
}
toolchain 1.98 channel-1.98
toolchain 1.99 channel-1.99
toolchain 1.98.1 channel-1.98.1
toolchain stable channel-stable
printf '[toolchain]\ncomponents = ["rustfmt"]\n' > "$WORK/no-channel"

printf 'nisshi 1.98\nnisshi-broker 1.98\nnisshi-sans-io 1.98\n' > "$WORK/all-1.98"
printf 'nisshi 1.98\nnisshi-broker 1.97\nnisshi-sans-io 1.98\n' > "$WORK/one-lower"
printf 'nisshi 1.98\nnisshi-broker \nnisshi-sans-io 1.98\n' > "$WORK/one-missing"
printf 'nisshi 1.98\nnisshi-broker 1.98\nnisshi-sans-io 1.98.0\n' > "$WORK/one-spelled-differently"
printf 'nisshi 1.98\r\nnisshi-broker 1.98\r\n' > "$WORK/crlf"
printf 'nisshi 1.98\nnisshi-broker 1.98' > "$WORK/no-trailing-newline"
printf 'nisshi-broker 1.98\n' > "$WORK/no-root"
: > "$WORK/empty"

expect "every package at the channel" pass "$WORK/all-1.98" "3 packages declare rust-version 1.98" "$WORK/channel-1.98"
expect "list with CRLF" pass "$WORK/crlf" "2 packages declare" "$WORK/channel-1.98"
expect "last line without a newline" pass "$WORK/no-trailing-newline" "2 packages declare" "$WORK/channel-1.98"
expect "channel moved ahead of the packages" fail "$WORK/all-1.98" "channel in $WORK/channel-1.99 is 1.99" "$WORK/channel-1.99"
expect "every package is named" fail "$WORK/all-1.98" "nisshi-sans-io has rust-version '1.98'" "$WORK/channel-1.99"
expect "one package lower than the channel" fail "$WORK/one-lower" "nisshi-broker has rust-version '1.97'" "$WORK/channel-1.98"
expect "one package without rust-version" fail "$WORK/one-missing" "nisshi-broker has rust-version ''" "$WORK/channel-1.98"
expect "a different spelling of the version" fail "$WORK/one-spelled-differently" "nisshi-sans-io has rust-version '1.98.0'" "$WORK/channel-1.98"
expect "channel with a patch version" fail "$WORK/all-1.98" "is 1.98.1" "$WORK/channel-1.98.1"
expect "channel that is not a version" fail "$WORK/all-1.98" "not a version number" "$WORK/channel-stable"
expect "no channel" fail "$WORK/all-1.98" "not a version number" "$WORK/no-channel"
expect "no toolchain file" fail "$WORK/all-1.98" "" "$WORK/missing-file"
expect "empty package list" fail "$WORK/empty" "is empty" "$WORK/channel-1.98"
expect "no root package" fail "$WORK/no-root" "has no 'nisshi' line" "$WORK/channel-1.98"
expect "no toolchain argument" fail "$WORK/all-1.98" "usage:"

printf '\n%d passed, %d failed\n' "$pass_count" "$fail_count"
[ "$fail_count" -eq 0 ]
