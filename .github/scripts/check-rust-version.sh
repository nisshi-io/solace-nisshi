#!/usr/bin/env bash
#
# Fails when a workspace package's `rust-version` differs from the channel in
# rust-toolchain.toml. rustup reads rust-toolchain.toml from the current
# directory, so the file does not reach a user's cargo install, and
# `rust-version` is what makes an older toolchain fail with a clear message.
# ci.yml's rust-version job runs this check, so a toolchain bump that leaves
# the minimum behind, or the reverse, fails CI instead of drifting.
#
# Reads the packages on stdin, one per line as `<name> <rust-version>`, as
# printed by:
#
#   cargo metadata --no-deps --format-version 1 \
#     | jq -r '.packages[] | "\(.name) \(.rust_version // "")"'
#
# cargo metadata prints the resolved value, so a member that lost
# `rust-version.workspace = true` shows up here as a package without one.
# The script also fails when the list is empty or has no `nisshi` package,
# or when the channel is not a version number, because a check that compares
# nothing would pass.
#
# Usage: check-rust-version.sh <rust-toolchain.toml>
# Local: just check-rust-version
# Test:  .github/scripts/check-rust-version.test.sh
#
set -euo pipefail

TOOLCHAIN_FILE="${1:?usage: check-rust-version.sh <rust-toolchain.toml>}"

CHANNEL=$(sed -n 's/^channel[[:space:]]*=[[:space:]]*"\([^"]*\)".*$/\1/p' "$TOOLCHAIN_FILE" | head -n 1)
if ! grep -q -E '^[0-9]+\.[0-9]+(\.[0-9]+)?$' <<<"$CHANNEL"; then
  echo "::error::The channel in ${TOOLCHAIN_FILE} is '${CHANNEL}', not a version number such as 1.98."
  exit 1
fi

# cargo metadata prints LF, but a list saved on Windows can end lines in CRLF.
PACKAGES=$(tr -d '\r')
if [ -z "$PACKAGES" ]; then
  echo "::error::The package list is empty."
  exit 1
fi
if ! grep -q -E '^nisshi ' <<<"$PACKAGES"; then
  echo "::error::The package list has no 'nisshi' line. Is it the output of cargo metadata --no-deps, as the header of check-rust-version.sh shows?"
  exit 1
fi

found=0
checked=0
while IFS= read -r line || [ -n "$line" ]; do
  [ -n "$line" ] || continue
  name="${line%% *}"
  version=""
  case "$line" in *" "*) version="${line#* }" ;; esac
  checked=$((checked + 1))
  if [ "$version" != "$CHANNEL" ]; then
    echo "::error::${name} has rust-version '${version}', but the channel in ${TOOLCHAIN_FILE} is ${CHANNEL}. Set [workspace.package] rust-version in Cargo.toml and the channel together, and give the package rust-version.workspace = true."
    found=$((found + 1))
  fi
done <<<"$PACKAGES"

if [ "$found" -gt 0 ]; then
  exit 1
fi
echo "${checked} packages declare rust-version ${CHANNEL}, the channel in ${TOOLCHAIN_FILE}."
