#!/usr/bin/env bash
#
# Self-test for guardian-scan-context.sh. Runs the real script under each event
# shape and asserts all 14 output keys, so the pr/queue/trunk/branch
# classification is verified on every run rather than trusted. The dangerous
# regressions this pins: a queue entry, or a dispatch from a feature branch,
# falling through to trunk, which would upload scan results, run the Guardian DB
# sync + Jira report, and block on Guardian's verdict for a ref that isn't main;
# and a value the PR author chose reaching the shared actions' bash unfiltered.
#
# The script runs inside a throwaway git repo built under $WORK, so the cases
# don't depend on the checkout's HEAD or tags.
#
# Run manually:  .github/scripts/guardian-scan-context.test.sh
# Runs in CI as a step of the `setup` job in .github/workflows/guardian-scan.yaml.
#
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
RESOLVE="${SCRIPT_DIR}/guardian-scan-context.sh"

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

# Two commits: v1.0.0 on the first, then a nearer tag that is not a `v*` tag
# (the real repo carries `tansu-*` and bare `0.x` tags) on the second. Only
# `git describe --match 'v*'` yields v1.0.0-1-g<sha>; without --match it would
# pick the nearer tag.
REPO="$WORK/repo"
git init -q "$REPO"
g() { git -C "$REPO" -c user.name=test -c user.email=test@example.com -c commit.gpgsign=false -c tag.gpgsign=false "$@"; }
g commit -q --allow-empty -m first
g tag v1.0.0
g commit -q --allow-empty -m second
g tag tansu-9
g tag 0.5.0
SHA=$(g rev-parse HEAD)
# The script requires a trunk commit to be on main. HEAD is main's tip here,
# except in Case 10.
g update-ref refs/remotes/origin/main HEAD
DESCRIBED="v1.0.0-1-g${SHA:0:7}"

pass_count=0
fail_count=0
pass() { pass_count=$((pass_count + 1)); printf 'ok   - %s\n' "$1"; }
fail() { fail_count=$((fail_count + 1)); printf 'FAIL - %s\n' "$1"; [ -n "${2:-}" ] && printf '       %s\n' "$2"; return 0; }

# run - execute the script in $REPO with the env given as KEY=VALUE args; set
# globals STDOUT and OUT (path to the captured $GITHUB_OUTPUT file). The script
# must exit 0 on every shape tested here; a crash is a failure in its own right,
# not just empty outputs downstream.
run() {
  OUT="$WORK/output.$RANDOM"
  : > "$OUT"
  local rc=0
  STDOUT=$(cd "$REPO" && env -i PATH="$PATH" HOME="${HOME:-}" GITHUB_OUTPUT="$OUT" "$@" bash "$RESOLVE" 2>&1) || rc=$?
  if [ "$rc" -ne 0 ]; then fail "script exited $rc" "env: $* / stdout=[$STDOUT]"; fi
  return 0
}

# get <key> - value of <key> from the captured output file.
get() { sed -n "s/^$1=//p" "$OUT" | tail -n 1; }

# check <case> <mode> <ref> <fossa_branch> <fossa_revision> <full_version> <is_fork> <is_pr>
#       <is_trunk> <is_queue> <is_dependabot> <scan> <enforce> <fossa_mode>
# Asserts the whole tuple (sha is always the fixture repo's HEAD), so a change to
# any one output fails every case that sets it, not just the ones that looked.
check() {
  local case="$1" key want got
  shift
  local keys=(mode ref fossa_branch fossa_revision full_version is_fork is_pr is_trunk is_queue is_dependabot scan enforce fossa_mode)
  local wants=("$@")
  keys+=(sha)
  wants+=("$SHA")
  local i
  for i in "${!keys[@]}"; do
    key="${keys[$i]}"; want="${wants[$i]}"; got=$(get "$key")
    if [ "$got" = "$want" ]; then pass "$case: $key=$want"; else fail "$case: $key" "want [$want] got [$got]"; fi
  done
}

PR_SHA=0123456789abcdef0123456789abcdef01234567

# --- Case 1: pull_request -> pr mode, nothing enforced ------------------------
run EVENT_NAME=pull_request HEAD_SHA=$PR_SHA FULL_VERSION_INPUT=v1.2.3 \
    GITHUB_REF=refs/pull/7/merge IS_FORK=false
check "pr" pr refs/pull/7/merge PR $PR_SHA v1.2.3 false true false false false true false REPORT

# --- Case 2: merge_group -> queue mode, NOT trunk ------------------------------
run EVENT_NAME=merge_group FULL_VERSION_INPUT=v1.2.3-4-gabc \
    GITHUB_REF=refs/heads/gh-readonly-queue/main/pr-9 IS_FORK=false
check "queue" queue refs/heads/gh-readonly-queue/main/pr-9 merge-queue v1.2.3-4-gabc v1.2.3-4-gabc false false false true false false false REPORT
if grep -q 'Resolved: mode=queue' <<<"$STDOUT"; then
  pass "queue: resolved mode is logged to stdout"
else
  fail "queue: resolved mode is logged to stdout" "stdout=[$STDOUT]"
fi

# --- Case 3: push -> trunk mode, enforcing -------------------------------------
run EVENT_NAME=push FULL_VERSION_INPUT=v1.2.3 GITHUB_REF=refs/heads/main IS_FORK=false
check "push" trunk refs/heads/main main v1.2.3 v1.2.3 false false true false false true true BLOCK

# --- Case 4: workflow_dispatch is trunk only when it scans main or a tag ------
run EVENT_NAME=workflow_dispatch FULL_VERSION_INPUT=v1.2.3 GITHUB_REF=refs/heads/main IS_FORK=false
check "dispatch-main" trunk refs/heads/main main v1.2.3 v1.2.3 false false true false false true true BLOCK
run EVENT_NAME=workflow_dispatch FULL_VERSION_INPUT=v1.2.3 \
    GITHUB_REF=refs/heads/main GIT_REF_INPUT=refs/tags/v0.7.0-pre.2 IS_FORK=false
check "dispatch-tag" trunk refs/tags/v0.7.0-pre.2 main v1.2.3 v1.2.3 false false true false false true true BLOCK
# Dispatched from a feature branch, or from main with `git_ref` set to one.
run EVENT_NAME=workflow_dispatch FULL_VERSION_INPUT=v1.2.3-4-gabc \
    GITHUB_REF=refs/heads/feature/x IS_FORK=false
check "dispatch-branch" branch refs/heads/feature/x feature/x v1.2.3-4-gabc v1.2.3-4-gabc false false false false false true false REPORT
run EVENT_NAME=workflow_dispatch FULL_VERSION_INPUT=v1.2.3 \
    GITHUB_REF=refs/heads/main GIT_REF_INPUT=maintenance IS_FORK=false
check "dispatch-input-branch" branch maintenance maintenance v1.2.3 v1.2.3 false false false false false true false REPORT

# --- Case 5: a Dependabot PR skips the scans; other runs scan ------------------
# Its run holds Dependabot's secrets, not the Environment's. Only a PR run
# skips: a push by the same actor would be trunk and must scan.
run EVENT_NAME=pull_request HEAD_SHA=$PR_SHA FULL_VERSION_INPUT=v1 \
    GITHUB_REF=refs/pull/7/merge IS_FORK=false ACTOR='dependabot[bot]'
check "dependabot" pr refs/pull/7/merge PR $PR_SHA v1 false true false false true false false REPORT
run EVENT_NAME=push FULL_VERSION_INPUT=v1 GITHUB_REF=refs/heads/main IS_FORK=false ACTOR='dependabot[bot]'
check "dependabot-push" trunk refs/heads/main main v1 v1 false false true false false true true BLOCK
run EVENT_NAME=pull_request HEAD_SHA=$PR_SHA FULL_VERSION_INPUT=v1 \
    GITHUB_REF=refs/pull/7/merge IS_FORK=false ACTOR=someone
check "pr-human" pr refs/pull/7/merge PR $PR_SHA v1 false true false false false true false REPORT
run EVENT_NAME=pull_request HEAD_SHA=$PR_SHA FULL_VERSION_INPUT=v1 \
    GITHUB_REF=refs/pull/7/merge IS_FORK=true
check "pr-fork" pr refs/pull/7/merge PR $PR_SHA v1 true true false false false false false REPORT
run EVENT_NAME=merge_group FULL_VERSION_INPUT=v1 GITHUB_REF=refs/heads/gh-readonly-queue/main/pr-9 IS_FORK=false
check "queue-scan" queue refs/heads/gh-readonly-queue/main/pr-9 merge-queue v1 v1 false false false true false false false REPORT

# --- Case 5a: no repository setting turns the licensing block off on trunk --
# The licensing guard's mode follows enforce alone, so a leftover
# FOSSA_LICENSING_BLOCK=false still blocks.
run EVENT_NAME=push FULL_VERSION_INPUT=v1.2.3 \
    GITHUB_REF=refs/heads/main IS_FORK=false FOSSA_LICENSING_BLOCK=false
check "licensing-var-ignored" trunk refs/heads/main main v1.2.3 v1.2.3 false false true false false true true BLOCK
case "$STDOUT" in
  *"REPORT-only"*) fail "licensing-var-ignored: no warning" "stdout=[$STDOUT]" ;;
  *) pass "licensing-var-ignored: no warning" ;;
esac

# --- Case 6: ref precedence - explicit input wins, else github.ref -------------
run EVENT_NAME=push FULL_VERSION_INPUT=v1 GITHUB_REF=refs/heads/main \
    GIT_REF_INPUT=refs/tags/v9.9.9 IS_FORK=false
check "ref-input" trunk refs/tags/v9.9.9 main v1 v1 false false true false false true true BLOCK
run EVENT_NAME=push FULL_VERSION_INPUT=v1 GITHUB_REF=refs/heads/main \
    GIT_REF_INPUT="" IS_FORK=false
check "ref-fallback" trunk refs/heads/main main v1 v1 false false true false false true true BLOCK

# --- Case 6a: the edges of the trunk regex ---------------------------------------
# A qualified tag is trunk; a branch that merely contains `main` or a version
# is not, and neither is a bare `v1.2.3`, which checkout would resolve to a
# branch of that name before a tag. These pin the anchors and the required
# `refs/tags/` prefix.
for ref in refs/tags/v1.2.3 refs/tags/v0.7.0-pre.2 main refs/heads/main; do
  run EVENT_NAME=workflow_dispatch FULL_VERSION_INPUT=v1 GITHUB_REF=refs/heads/main GIT_REF_INPUT="$ref" IS_FORK=false
  check "trunk-ref $ref" trunk "$ref" main v1 v1 false false true false false true true BLOCK
done
for ref in refs/heads/feature/main refs/heads/release/v1.2.3 refs/heads/v1.2.3 refs/heads/main2 \
           refs/tags/v1.2 refs/tags/release-v1.2.3 xmain refs/heads/mainline v1.2.3 v0.7.0-pre.2; do
  run EVENT_NAME=workflow_dispatch FULL_VERSION_INPUT=v1 GITHUB_REF=refs/heads/main GIT_REF_INPUT="$ref" IS_FORK=false
  check "branch-ref $ref" branch "$ref" "${ref#refs/heads/}" v1 v1 false false false false false true false REPORT
done

# --- Case 7: FULL_VERSION_INPUT is used verbatim; empty falls back to describe -
run EVENT_NAME=push FULL_VERSION_INPUT=7.7.7-custom GITHUB_REF=refs/heads/main IS_FORK=false
check "version-input" trunk refs/heads/main main 7.7.7-custom 7.7.7-custom false false true false false true true BLOCK
# Runs `git describe` for real against the fixture repo, where the nearest tag
# is not a `v*` tag: only `--match 'v*'` gives v1.0.0-1-g<sha>.
run EVENT_NAME=push FULL_VERSION_INPUT="" GITHUB_REF=refs/heads/main IS_FORK=false
check "version-describe" trunk refs/heads/main main "$DESCRIBED" "$DESCRIBED" false false true false false true true BLOCK

# --- Case 8: the PR revision is the head SHA, never the branch name ---------
# shellcheck disable=SC2016 # the literal `$(id)` is the point
run EVENT_NAME=pull_request HEAD_REF='x";$(id)' HEAD_SHA=$PR_SHA \
    FULL_VERSION_INPUT=v1 GITHUB_REF=refs/pull/7/merge IS_FORK=false
check "hostile-ref" pr refs/pull/7/merge PR $PR_SHA v1 false true false false false true false REPORT

# --- Case 9: hostile or malformed inputs are rejected, not passed through ----
# run_fails - like run, but the script must exit non-zero.
run_fails() {
  OUT="$WORK/output.$RANDOM"
  : > "$OUT"
  if STDOUT=$(cd "$REPO" && env -i PATH="$PATH" HOME="${HOME:-}" GITHUB_OUTPUT="$OUT" "$@" bash "$RESOLVE" 2>&1); then
    fail "script accepted a hostile input" "env: $* / stdout=[$STDOUT]"
  else
    pass "hostile input rejected: $*"
  fi
}
# shellcheck disable=SC2016 # the literal `$(id)` is the point
run_fails EVENT_NAME=push FULL_VERSION_INPUT='v1";$(id)' \
    GITHUB_REF=refs/heads/main IS_FORK=false
run_fails EVENT_NAME=push FULL_VERSION_INPUT=v1 \
    GITHUB_REF=refs/heads/main GIT_REF_INPUT=$'main\nenforce=false' IS_FORK=false
# shellcheck disable=SC2016 # the literal `$(id)` is the point
run_fails EVENT_NAME=workflow_dispatch FULL_VERSION_INPUT=v1 \
    GITHUB_REF=refs/heads/main GIT_REF_INPUT='x";$(id)' IS_FORK=false
run_fails EVENT_NAME=pull_request HEAD_SHA=abc FULL_VERSION_INPUT=v1 \
    GITHUB_REF=refs/pull/7/merge IS_FORK=false

# --- Case 10: a trunk run requires a commit on main ----------------------------
# Only trunk runs in `guardian-trunk`, so only trunk checks. Main's tip passes
# (every trunk case above), a commit off main fails, and so does a checkout
# without `origin/main`. A branch run of the same commit still resolves.
g update-ref refs/remotes/origin/main HEAD~1
run_fails EVENT_NAME=workflow_dispatch FULL_VERSION_INPUT=v1 \
    GITHUB_REF=refs/heads/main GIT_REF_INPUT=refs/tags/v1.2.3 IS_FORK=false
run_fails EVENT_NAME=push FULL_VERSION_INPUT=v1 GITHUB_REF=refs/heads/main IS_FORK=false
run EVENT_NAME=workflow_dispatch FULL_VERSION_INPUT=v1 \
    GITHUB_REF=refs/heads/main GIT_REF_INPUT=refs/heads/feature/x IS_FORK=false
check "off-main-branch" branch refs/heads/feature/x feature/x v1 v1 false false false false false true false REPORT
g update-ref -d refs/remotes/origin/main
run_fails EVENT_NAME=push FULL_VERSION_INPUT=v1 GITHUB_REF=refs/heads/main IS_FORK=false
g update-ref refs/remotes/origin/main HEAD

printf '\n%d passed, %d failed\n' "$pass_count" "$fail_count"
[ "$fail_count" -eq 0 ]
