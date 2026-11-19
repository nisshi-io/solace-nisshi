#!/usr/bin/env bash
#
# Fails when the root AGENTS.md no longer gives every agent the project's
# instructions:
#
# - The table in its "## Rules" section must name each file in
#   .claude/rules/ exactly once, and no other file there. Claude Code loads
#   .claude/rules/ by itself, but other agents, such as Codex, find a rule
#   only through that table.
# - Every other file that the table names, such as .github/AGENTS.md, must
#   exist.
# - AGENTS.md must fit in 32 KiB, the most that Codex loads by default from
#   all AGENTS.md files together; it truncates the file at that size and
#   skips the nested files. .codex/config.toml raises the limit only for a
#   project that the user trusts.
# - No CLAUDE.md may exist outside ignored files, because Claude Code reads AGENTS.md only where
#   no CLAUDE.md sits beside it.
#
# workflow-lint.yml's actionlint job runs this check on every pull request.
#
# Usage: check-agent-instructions.sh <repository root>
# Local: just check-agent-instructions
# Test:  .github/scripts/check-agent-instructions.test.sh
#
set -euo pipefail

ROOT="${1:?usage: check-agent-instructions.sh <repository root>}"
AGENTS="${ROOT}/AGENTS.md"
RULES_DIR=".claude/rules"
CODEX_DEFAULT_MAX_BYTES=32768

if [ ! -f "$AGENTS" ]; then
  echo "::error::${AGENTS} does not exist."
  exit 1
fi

errors=0
fail() {
  echo "::error::$1"
  errors=$((errors + 1))
}

size=$(wc -c < "$AGENTS" | tr -d ' ')
if [ "$size" -gt "$CODEX_DEFAULT_MAX_BYTES" ]; then
  fail "AGENTS.md is ${size} bytes, more than the ${CODEX_DEFAULT_MAX_BYTES} that Codex loads by default. Move a section into the AGENTS.md of the directory it covers, and add a row for that file to the table in \"## Rules\"."
fi

# Each table row in the "## Rules" section, up to the next "## " heading.
rows=$(tr -d '\r' < "$AGENTS" | awk '/^## /{in_rules = ($0 == "## Rules")} in_rules && /^\|/')
# Each backticked path that ends in .md names a file.
# shellcheck disable=SC2016 # the backticks are Markdown, not a command
named=$(grep -o -E '`[^`]+\.md`' <<<"$rows" | tr -d '`' || true)
if [ -z "$named" ]; then
  fail "AGENTS.md has no \"## Rules\" section with a table that names a file."
fi

duplicates=$(sort <<<"$named" | uniq -d)
for path in $duplicates; do
  fail "The table in AGENTS.md names ${path} more than once."
done

listed_rules=$(grep -E "^${RULES_DIR}/" <<<"$named" | sort -u || true)
present_rules=$(cd "$ROOT" && find "$RULES_DIR" -maxdepth 1 -type f -name '*.md' 2>/dev/null | sort)
for path in $(comm -13 <(echo "$listed_rules") <(echo "$present_rules")); do
  fail "${path} has no row in the table in AGENTS.md. Add one that says before which edit to read it."
done
for path in $(comm -23 <(echo "$listed_rules") <(echo "$present_rules")); do
  fail "The table in AGENTS.md names ${path}, which does not exist."
done

for path in $(grep -v -E "^${RULES_DIR}/" <<<"$named" | sort -u || true); do
  [ -f "${ROOT}/${path}" ] || fail "The table in AGENTS.md names ${path}, which does not exist."
done

# git lists tracked and untracked files but skips ignored ones, so a
# checkout inside an ignored directory, such as a worktree, does not count.
claude_files=$(git -C "$ROOT" ls-files --cached --others --exclude-standard -- 'CLAUDE.md' '**/CLAUDE.md')
for path in $claude_files; do
  fail "${path} exists, so Claude Code stops reading the AGENTS.md beside it. Delete it."
done

if [ "$errors" -gt 0 ]; then
  exit 1
fi
echo "AGENTS.md is ${size} bytes and its rule table names the $(wc -l <<<"$present_rules" | tr -d ' ') files in ${RULES_DIR}/."
