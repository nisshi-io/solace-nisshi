#!/usr/bin/env bash
#
# Self-test for check-agent-instructions.sh. The regression that matters is a
# repository that passes although an agent other than Claude Code would miss
# a rule, or would get AGENTS.md truncated. Each case builds a small
# repository in a temporary directory, so the test does not depend on the
# real AGENTS.md.
#
# Run manually:  .github/scripts/check-agent-instructions.test.sh
# Runs in CI in workflow-lint.yml's actionlint job.
#
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
CHECK="${SCRIPT_DIR}/check-agent-instructions.sh"

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

pass_count=0
fail_count=0

# expect <case> <want> <text> <repository> - run the check on <repository>,
# assert it exits 0 (want=pass) or non-zero (want=fail), and that its output
# contains <text>.
expect() {
  local case="$1" want="$2" text="$3" repository="$4" got=pass
  bash "$CHECK" "$repository" > "$WORK/out" 2>&1 || got=fail
  if [ "$got" = "$want" ] && grep -q -F -- "$text" "$WORK/out"; then
    pass_count=$((pass_count + 1)); printf 'ok   - %s: %s\n' "$case" "$want"
  else
    fail_count=$((fail_count + 1)); printf 'FAIL - %s: want %s %s, got %s:\n' "$case" "$want" "$text" "$got"
    sed 's/^/       /' "$WORK/out"
  fi
}

# repository <name> - create a repository that passes the check: two rules,
# a nested AGENTS.md, and a table with a row for each of them.
repository() {
  local dir="$WORK/$1"
  mkdir -p "$dir/.claude/rules" "$dir/fuzz"
  git init -q "$dir"
  touch "$dir/.claude/rules/comments.md" "$dir/.claude/rules/modules.md" "$dir/fuzz/AGENTS.md"
  cat > "$dir/AGENTS.md" <<'EOF'
# AGENTS.md

## Rules

| When | Read |
| --- | --- |
| Before you edit any `.rs` file | `.claude/rules/comments.md` |
| Before you edit any `.rs` file | `.claude/rules/modules.md` |
| Before you edit anything under `fuzz/` | `fuzz/AGENTS.md` |

## Testing Notes

| File | Purpose |
| --- | --- |
| `.claude/rules/unit-test-layout.md` | a table outside "## Rules" |
EOF
  echo "$dir"
}

valid=$(repository valid)
expect "every rule listed" pass "rule table names the 2 files in .claude/rules/" "$valid"

crlf=$(repository crlf)
awk '{ printf "%s\r\n", $0 }' "$crlf/AGENTS.md" > "$WORK/crlf.md" && mv "$WORK/crlf.md" "$crlf/AGENTS.md"
expect "AGENTS.md with CRLF" pass "names the 2 files" "$crlf"

new_rule=$(repository new-rule)
touch "$new_rule/.claude/rules/naming.md"
expect "rule without a row" fail ".claude/rules/naming.md has no row" "$new_rule"

removed_rule=$(repository removed-rule)
rm "$removed_rule/.claude/rules/modules.md"
expect "row for a removed rule" fail "names .claude/rules/modules.md, which does not exist" "$removed_rule"

duplicate=$(repository duplicate)
awk '{ print } /modules\.md/ { print }' "$duplicate/AGENTS.md" > "$WORK/duplicated" && mv "$WORK/duplicated" "$duplicate/AGENTS.md"
expect "rule listed twice" fail "names .claude/rules/modules.md more than once" "$duplicate"

missing_nested=$(repository missing-nested)
rm "$missing_nested/fuzz/AGENTS.md"
expect "row for a missing nested AGENTS.md" fail "names fuzz/AGENTS.md, which does not exist" "$missing_nested"

no_table=$(repository no-table)
printf '# AGENTS.md\n' > "$no_table/AGENTS.md"
expect "no rule table" fail "has no \"## Rules\" section" "$no_table"

at_limit=$(repository at-limit)
size=$(wc -c < "$at_limit/AGENTS.md")
head -c $((32768 - size)) /dev/zero | tr '\0' 'x' >> "$at_limit/AGENTS.md"
expect "AGENTS.md at 32 KiB" pass "AGENTS.md is 32768 bytes" "$at_limit"

too_large=$(repository too-large)
size=$(wc -c < "$too_large/AGENTS.md")
head -c $((32769 - size)) /dev/zero | tr '\0' 'x' >> "$too_large/AGENTS.md"
expect "AGENTS.md over 32 KiB" fail "AGENTS.md is 32769 bytes" "$too_large"

claude_md=$(repository claude-md)
mkdir -p "$claude_md/nisshi-sans-io"
printf '@AGENTS.md\n' > "$claude_md/nisshi-sans-io/CLAUDE.md"
expect "a CLAUDE.md in a crate" fail "nisshi-sans-io/CLAUDE.md exists" "$claude_md"

ignored_claude_md=$(repository ignored-claude-md)
mkdir -p "$ignored_claude_md/.claude/worktrees/old/nisshi"
printf '@AGENTS.md\n' > "$ignored_claude_md/.claude/worktrees/old/nisshi/CLAUDE.md"
printf '.claude/worktrees/\n' > "$ignored_claude_md/.gitignore"
expect "a CLAUDE.md in an ignored directory" pass "names the 2 files" "$ignored_claude_md"

expect "no AGENTS.md" fail "does not exist" "$WORK/missing"
expect "no repository argument" fail "usage:" ""

printf '\n%d passed, %d failed\n' "$pass_count" "$fail_count"
[ "$fail_count" -eq 0 ]
