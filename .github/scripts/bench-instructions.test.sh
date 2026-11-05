#!/usr/bin/env bash
#
# Self-test for bench-instructions.sh. The regression that matters is a growth
# above the threshold that passes, because the job would then report nothing
# while the decoder slows down. The test puts a fake `valgrind` and a fake
# bench binary on the PATH, so it needs neither valgrind nor cargo.
#
# Run manually:  .github/scripts/bench-instructions.test.sh
# Runs in CI in workflow-lint.yml's actionlint job.
#
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SCRIPT="${SCRIPT_DIR}/bench-instructions.sh"

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

pass_count=0
fail_count=0

# check <case> <want> <text> <args>... - run the script with <args>, assert it
# exits 0 (want=pass) or non-zero (want=fail), and that its output contains
# <text> when it isn't empty.
check() {
  local case="$1" want="$2" text="$3" got=pass
  shift 3
  bash "$SCRIPT" "$@" > "$WORK/out" 2>&1 || got=fail
  if [ "$got" = "$want" ] && { [ -z "$text" ] || grep -q -F -- "$text" "$WORK/out"; }; then
    pass_count=$((pass_count + 1)); printf 'ok   - %s: %s\n' "$case" "$want"
  else
    fail_count=$((fail_count + 1)); printf 'FAIL - %s: want %s %s, got %s:\n' "$case" "$want" "$text" "$got"
    sed 's/^/       /' "$WORK/out"
  fi
}

mkdir "$WORK/bin"

# The fake valgrind runs the program that follows its options, and passes it
# the path where callgrind would write its profile.
cat > "$WORK/bin/valgrind" <<'EOF'
#!/usr/bin/env bash
for arg in "$@"; do
  case "$arg" in
    --callgrind-out-file=*) export FAKE_PROFILE="${arg#--callgrind-out-file=}" ;;
    --*) ;;
    *) break ;;
  esac
  shift
done
exec "$@"
EOF

# The fake bench binary costs 1000 instructions to start and `COST_<key>` (or
# COST_all) for each pass, the way callgrind counts the real one.
cat > "$WORK/bench" <<'EOF'
#!/usr/bin/env bash
iterations=1 key=all
while [ "$#" -gt 0 ]; do
  case "$1" in
    --iterations) iterations="$2"; shift ;;
    --api-key) key="$2"; shift ;;
  esac
  shift
done
cost_var="COST_${key}"
echo "summary: $((1000 + iterations * ${!cost_var}))" > "$FAKE_PROFILE"
EOF
# A binary whose second pass costs nothing extra, as when the profile misses it.
cat > "$WORK/bench-flat" <<'EOF'
#!/usr/bin/env bash
echo "summary: 1000" > "$FAKE_PROFILE"
EOF
chmod +x "$WORK/bin/valgrind" "$WORK/bench" "$WORK/bench-flat"

# measure: each row is the cost of one pass, without the 1000 of startup.
export PATH="$WORK/bin:$PATH" COST_all=5000 COST_18=300 COST_0=1200
check "measure writes one row per key" pass "" measure "$WORK/bench" "$WORK/measured.tsv" 18 0
if [ "$(cat "$WORK/measured.tsv")" = "$(printf 'all\t5000\n18\t300\n0\t1200')" ]; then
  pass_count=$((pass_count + 1)); echo "ok   - measure subtracts the startup cost"
else
  fail_count=$((fail_count + 1)); echo "FAIL - measure subtracts the startup cost:"; sed 's/^/       /' "$WORK/measured.tsv"
fi
check "measure fails when a second pass costs nothing" fail "did not cost more" measure "$WORK/bench-flat" "$WORK/flat.tsv"
check "measure needs an output file" fail "usage" measure "$WORK/bench"

printf 'all\t10000\n18\t1000\n0\t2000\n' > "$WORK/base.tsv"
printf 'all\t10000\n18\t1000\n0\t2000\n' > "$WORK/same.tsv"
printf 'all\t10200\n18\t1020\n0\t1900\n' > "$WORK/small.tsv"
printf 'all\t10600\n18\t1000\n0\t2000\n' > "$WORK/slower.tsv"
printf 'all\t9000\n18\t900\n0\t1800\n' > "$WORK/faster.tsv"
printf 'all\t10000\n18\t1000\n0\t2000\n99\t500\n' > "$WORK/added.tsv"
printf 'all\t10000\n0\t2000\n' > "$WORK/removed.tsv"
printf 'all\t1000000\n' > "$WORK/big-base.tsv"
printf 'all\t1060000\n' > "$WORK/big-slower.tsv"
printf 'all\t1020000\n' > "$WORK/big-small.tsv"

check "unchanged counts pass" pass "| all | 10000 | 10000 | +0.00% |" compare "$WORK/base.tsv" "$WORK/same.tsv"
check "growth below the threshold passes" pass "+2.00%" compare "$WORK/base.tsv" "$WORK/small.tsv"
check "growth above the threshold fails" fail "| all | 10000 | 10600 | +6.00% :warning: |" compare "$WORK/base.tsv" "$WORK/slower.tsv" 3 0
check "the failure names a warning" fail "::warning::bench instructions for API key all grew +6.00%" compare "$WORK/base.tsv" "$WORK/slower.tsv" 3 0
check "a lower threshold turns small growth into a failure" fail ":warning:" compare "$WORK/base.tsv" "$WORK/small.tsv" 1 0
check "a higher threshold lets the same growth pass" pass "+6.00%" compare "$WORK/base.tsv" "$WORK/slower.tsv" 10 0
# +6% of 10000 is 600 instructions: the default minimum of 1500 treats that as noise.
check "growth below the minimum passes whatever the percentage" pass "+6.00%" compare "$WORK/base.tsv" "$WORK/slower.tsv"
check "growth above both limits fails with the defaults" fail ":warning:" compare "$WORK/big-base.tsv" "$WORK/big-slower.tsv"
check "a large row under the threshold passes with the defaults" pass "+2.00%" compare "$WORK/big-base.tsv" "$WORK/big-small.tsv"
check "fewer instructions pass" pass "-10.00%" compare "$WORK/base.tsv" "$WORK/faster.tsv"
check "an added key has no percentage" pass "| 99 | - | 500 | n/a |" compare "$WORK/base.tsv" "$WORK/added.tsv"
check "a removed key has no percentage" pass "| 18 | 1000 | - | n/a |" compare "$WORK/base.tsv" "$WORK/removed.tsv"
check "unknown subcommand" fail "usage" nonsense

printf '\n%d passed, %d failed\n' "$pass_count" "$fail_count"
[ "$fail_count" -eq 0 ]
