#!/usr/bin/env bash
#
# Counts the instructions that the nisshi-sans-io `bench` binary executes, so
# that ci.yml can compare a pull request with its base. Wall-clock time on a
# shared runner varies by more than the regressions worth catching, and the
# instruction count of the same binary on the same input does not.
#
#   bench-instructions.sh measure BINARY OUTPUT.tsv [API_KEY...]
#   bench-instructions.sh compare BASE.tsv HEAD.tsv [THRESHOLD_PERCENT]
#
# `measure` runs BINARY under callgrind once for all fixtures (the `all` row)
# and once for each API_KEY, and writes one `<key>\t<instructions>` line per
# run. Each count is the cost of one pass over the fixtures: the run with
# `--iterations 2` minus the run with `--iterations 1`, which removes the
# process start and the lazily built message metadata.
#
# `compare` prints a Markdown table of the change per row. It exits 1 when
# any row grows by more than THRESHOLD_PERCENT (default 3), and writes a
# `::warning::` line for each such row.
#
# Tested by bench-instructions.test.sh.
#
set -euo pipefail

instructions() {
  local binary="$1" iterations="$2" out log
  shift 2
  out=$(mktemp)
  log=$(mktemp)
  if ! valgrind --tool=callgrind --callgrind-out-file="$out" \
    "$binary" --iterations "$iterations" "$@" > /dev/null 2> "$log"; then
    cat "$log" >&2
    rm -f "$out" "$log"
    return 1
  fi
  awk '/^summary:/ { print $2; found = 1; exit } END { exit !found }' "$out"
  rm -f "$out" "$log"
}

# steady_state <binary> <bench arguments>... - prints the instructions of one
# pass over the fixtures that the arguments select.
steady_state() {
  local binary="$1" once twice
  shift
  once=$(instructions "$binary" 1 "$@")
  twice=$(instructions "$binary" 2 "$@")
  if [ "$twice" -le "$once" ]; then
    echo "::error::two passes ($twice) did not cost more than one ($once): bench $*" >&2
    return 1
  fi
  echo $((twice - once))
}

measure() {
  local binary="$1" output="$2" key count
  shift 2
  # Assign before printing: a failure inside the argument of printf would not
  # stop the script.
  count=$(steady_state "$binary")
  printf 'all\t%s\n' "$count" > "$output"
  for key in "$@"; do
    count=$(steady_state "$binary" --api-key "$key")
    printf '%s\t%s\n' "$key" "$count" >> "$output"
  done
}

compare() {
  local base="$1" head="$2" threshold="${3:-3}" rows
  rows=$(mktemp)
  # A key present on one side only is a fixture added or removed, not a
  # change in cost, so it gets no percentage.
  join -t $'\t' -a1 -a2 -e - -o 0,1.2,2.2 \
    <(LC_ALL=C sort -t $'\t' -k1,1 "$base") <(LC_ALL=C sort -t $'\t' -k1,1 "$head") |
    awk -F '\t' -v threshold="$threshold" '
      {
        if ($2 == "-" || $3 == "-") { printf "0\t%s\t%s\t%s\tn/a\t\n", $1, $2, $3; next }
        pct = ($3 - $2) * 100 / $2
        flag = pct > threshold ? "regression" : ""
        printf "%f\t%s\t%s\t%s\t%+.2f%%\t%s\n", pct, $1, $2, $3, pct, flag
      }' | LC_ALL=C sort -t $'\t' -k1,1 -g -r > "$rows"

  echo "| API key | base | head | change |"
  echo "| --- | ---: | ---: | ---: |"
  awk -F '\t' '{ printf "| %s | %s | %s | %s%s |\n", $2, $3, $4, $5, ($6 == "" ? "" : " :warning:") }' "$rows"

  local status=0
  while IFS=$'\t' read -r _ key _ _ change flag; do
    [ "$flag" = regression ] || continue
    echo "::warning::bench instructions for API key ${key} grew ${change} (threshold ${threshold}%)" >&2
    status=1
  done < "$rows"
  rm -f "$rows"
  return "$status"
}

case "${1:-}" in
  measure) [ "$#" -ge 3 ] || { echo "usage: $0 measure BINARY OUTPUT.tsv [API_KEY...]" >&2; exit 2; }
    shift; measure "$@" ;;
  compare) [ "$#" -ge 3 ] || { echo "usage: $0 compare BASE.tsv HEAD.tsv [THRESHOLD_PERCENT]" >&2; exit 2; }
    shift; compare "$@" ;;
  *) echo "usage: $0 measure|compare ..." >&2; exit 2 ;;
esac
