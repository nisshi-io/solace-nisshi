#!/usr/bin/env bash
#
# Classifies a guardian-scan.yaml run into its mode and derived settings and
# writes them to $GITHUB_OUTPUT. Kept out of the workflow so the
# classification is testable (guardian-scan-context.test.sh): this tuple
# decides whether a run uploads to Guardian, runs the DB sync + Jira report,
# and blocks on Guardian's verdict.
#
# Modes:
#   pr      pull_request event                 report/diff only, no upload
#   queue   merge_group event                  scan jobs skip entirely (see the workflow)
#   trunk   any other event on main or a v* tag  upload + enforce
#   branch  any other event on any other ref   report only, no upload, filed under its own branch
#
# Env (the workflow maps event context in, never interpolated into a script):
#   EVENT_NAME          github.event_name
#   HEAD_SHA            PR head commit (pull_request only)
#   GITHUB_REF          github.ref
#   GIT_REF_INPUT       workflow_dispatch / workflow_call `git_ref` input
#   FULL_VERSION_INPUT  workflow_call `product_full_version` input
#   ACTOR               github.actor
#   IS_FORK             "true" for a PR from a fork
#   GITHUB_OUTPUT       file the outputs are appended to
#
# Run with the working directory set to the checkout the scan jobs will use: the
# commit it is at becomes the `sha` output, which every later job checks out. A
# trunk run needs `origin/main` in that checkout. The
# script itself must come from the workflow's own commit, not from that checkout
# (see `setup` in the workflow), or a scanned branch could classify itself as trunk.
#
set -euo pipefail

# FOSSA, Prisma and the Guardian DB sync must all see the same version string,
# or the sync can't find the scan results it is meant to join. `--match 'v*'`
# keeps the `tansu-*` and bare `0.x` tags from being picked.
# Values that end up in `fossa.revision=...` must match this: the shared
# sca-scan action splices that line straight into a bash script.
SAFE_REF_RE='^[A-Za-z0-9._/+-]{1,200}$'

if [ -n "${FULL_VERSION_INPUT:-}" ]; then
  FULL_VERSION="$FULL_VERSION_INPUT"
else
  FULL_VERSION="$(git describe --tags --always --match 'v*')"
fi
if ! [[ "$FULL_VERSION" =~ $SAFE_REF_RE ]]; then
  echo "::error::product version '$FULL_VERSION' contains characters outside [A-Za-z0-9._/+-]" >&2
  exit 1
fi

# A PR's revision is its head commit, not its branch name: every push gets its
# own FOSSA revision instead of overwriting the last one, and a SHA is hex, so
# nothing the PR author chose reaches the shared actions' bash.
SHA_RE='^[0-9a-f]{40}$'
# main, or a release tag such as refs/tags/v0.7.0 / refs/tags/v0.7.0-pre.2. A
# tag must be spelled with `refs/tags/`: actions/checkout resolves an unqualified
# name to a branch before a tag, so a bare `v0.7.0` could scan a branch of that
# name in trunk. A bare `main` is safe, since checkout tries branches first.
TRUNK_REF_RE='^(refs/heads/main|main|refs/tags/v[0-9]+\.[0-9]+\.[0-9]+([.+-][A-Za-z0-9.+-]*)?)$'

REF="${GIT_REF_INPUT:-${GITHUB_REF:-}}"
if ! [[ "$REF" =~ $SAFE_REF_RE ]]; then
  echo "::error::git ref '$REF' contains characters outside [A-Za-z0-9._/+-]" >&2
  exit 1
fi

if [ "${EVENT_NAME:-}" = "pull_request" ]; then
  if ! [[ "${HEAD_SHA:-}" =~ $SHA_RE ]]; then
    echo "::error::PR head SHA '${HEAD_SHA:-}' is not a 40-character commit SHA" >&2
    exit 1
  fi
  MODE="pr"; IS_PR="true"; IS_TRUNK="false"; IS_QUEUE="false"
  FOSSA_BRANCH="PR"; FOSSA_REVISION="$HEAD_SHA"
elif [ "${EVENT_NAME:-}" = "merge_group" ]; then
  # Its own mode, and must not fall through to trunk: a queue entry is a
  # throwaway ref that may never land, so classifying it as trunk would upload
  # scan results, run the Guardian DB sync and its Jira report, and block on
  # Guardian's verdict, once per entry. FOSSA_BRANCH is still set, defensively:
  # if a queue scan is ever re-enabled, it must not masquerade as main.
  MODE="queue"; IS_PR="false"; IS_TRUNK="false"; IS_QUEUE="true"
  FOSSA_BRANCH="merge-queue"; FOSSA_REVISION="$FULL_VERSION"
elif [[ "$REF" =~ $TRUNK_REF_RE ]]; then
  # Push to main, or a dispatch / workflow_call that scans main or a release
  # tag (the release-readiness path).
  MODE="trunk"; IS_PR="false"; IS_TRUNK="true"; IS_QUEUE="false"
  FOSSA_BRANCH="main"; FOSSA_REVISION="$FULL_VERSION"
else
  # A dispatch or workflow_call against any other ref (a feature branch, a
  # SHA). Same reason as the queue: it must not be filed in FOSSA as main,
  # upload to Guardian, or block on its verdict.
  MODE="branch"; IS_PR="false"; IS_TRUNK="false"; IS_QUEUE="false"
  FOSSA_BRANCH="${REF#refs/heads/}"; FOSSA_REVISION="$FULL_VERSION"
fi

# The commit setup checked out. Later jobs check out this SHA rather than the
# ref, so a branch that moves mid-run can't swap in another commit's tree.
SHA="$(git rev-parse HEAD)"
if ! [[ "$SHA" =~ $SHA_RE ]]; then
  echo "::error::checked-out commit '$SHA' is not a 40-character SHA" >&2
  exit 1
fi

# A trunk run scans only a commit that is on main. Every trunk job runs in
# `guardian-trunk` and runs tools that execute code from the scanned tree, so
# the run stops here, before any of them starts. A release tag passes because
# release.yml's `on-main` job requires the tagged commit to be on main.
if [ "$MODE" = "trunk" ] && ! git merge-base --is-ancestor "$SHA" origin/main; then
  echo "::error::$SHA is not on main, so it does not run as trunk" >&2
  exit 1
fi

# A Dependabot-triggered PR run gets Dependabot's secrets, not the `guardian`
# Environment's, so its scans would fail on empty keys. It skips them like a
# fork PR; the trunk run after the merge scans the bump.
IS_DEPENDABOT="false"
if [ "$MODE" = "pr" ] && [ "${ACTOR:-}" = "dependabot[bot]" ]; then IS_DEPENDABOT="true"; fi
SCAN="true"
if [ "${IS_FORK:-false}" = "true" ] || [ "$IS_QUEUE" = "true" ] || [ "$IS_DEPENDABOT" = "true" ]; then SCAN="false"; fi

# Enforce (block) only on trunk. A release that must ship over a block does
# so through release.yml's override job, not by turning this off.
ENFORCE="$IS_TRUNK"
FOSSA_MODE="REPORT"
if [ "$ENFORCE" = "true" ]; then FOSSA_MODE="BLOCK"; fi

# The line an operator needs when asking "which mode did this run take?" -
# $GITHUB_OUTPUT is never displayed in the run log.
echo "Resolved: mode=$MODE event=${EVENT_NAME:-} sha=$SHA is_fork=${IS_FORK:-false} is_dependabot=$IS_DEPENDABOT scan=$SCAN enforce=$ENFORCE fossa_mode=$FOSSA_MODE fossa_branch=$FOSSA_BRANCH"

# A newline in any value would let it inject extra keys into $GITHUB_OUTPUT.
emit() {
  case "$2" in
    *$'\n'* | *$'\r'*) echo "::error::refusing to write a multi-line value for '$1'" >&2; exit 1 ;;
  esac
  printf '%s=%s\n' "$1" "$2" >> "$GITHUB_OUTPUT"
}

emit mode "$MODE"
emit ref "$REF"
emit sha "$SHA"
emit fossa_branch "$FOSSA_BRANCH"
emit fossa_revision "$FOSSA_REVISION"
emit full_version "$FULL_VERSION"
emit is_fork "${IS_FORK:-false}"
emit is_pr "$IS_PR"
emit is_trunk "$IS_TRUNK"
emit is_queue "$IS_QUEUE"
emit is_dependabot "$IS_DEPENDABOT"
emit scan "$SCAN"
emit enforce "$ENFORCE"
emit fossa_mode "$FOSSA_MODE"
