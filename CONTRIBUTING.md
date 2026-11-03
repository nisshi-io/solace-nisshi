# Contributing to Nisshi

Thanks for considering a contribution. This document covers how to file issues,
submit changes, and get them merged.

## Propose first

Nisshi's direction is set by the Solace Nisshi team (see
[GOVERNANCE.md](GOVERNANCE.md)), so please propose a change before you write
it:

1. Open an issue describing the problem and your idea.
2. Wait for approval from the Solace Nisshi team. An idea is approved when
   a maintainer listed in [MAINTAINERS.md](MAINTAINERS.md) comments on the
   issue to say so. For these purposes the maintainers are the members of
   the Solace Nisshi team.
3. Open a pull request that links the approved issue.

This respects your time. Pull requests without prior approval are generally
closed with a pointer to this process, however good the code is.

Small changes can go straight to a pull request without an issue:

- bug fixes with a small, local change
- typo fixes
- documentation improvements
- editorial changes

If you're unsure whether a change counts as small, open an issue and ask.

Approved pull requests are reviewed and either merged or given a clear
reason why not.

Sustained contribution can lead to becoming a maintainer. See
[MAINTAINERS.md](MAINTAINERS.md) for who maintains Nisshi and how that works.

## Questions

For questions and usage help, please ask in
the [Nisshi forum in the Solace Community](https://community.solace.com/c/nisshi-diskless-streaming/19). Answers are best-effort.

## Development setup

The project uses [`just`](https://github.com/casey/just) as its task runner:

```shell
cp example.env .env    # edit as needed
just ci                # start postgres, minio, and lakehouse via docker compose
just                   # fmt, build, test, clippy
```

Without Docker, `just test-fast` runs every test that needs no Postgres,
MinIO or Iceberg catalog, and `just test-doc` runs the doc tests. Run the full
suite with `just ci` and `just test` before you ask for review; CI runs it on
every pull request.

[ARCHITECTURE.md](ARCHITECTURE.md) explains how the crates fit together.
See `AGENTS.md` at the repo root for the full command reference.

In a linked git worktree, the tests start the worktree's own services
(`etc/worktree-services.sh`) on ports that Docker picks, and write those ports
to the worktree's `.env`, so several worktrees can run their tests at once.
This needs Docker, `jq` and `curl`. `just services-down` removes a
worktree's services, and `just services-prune` removes those of each
worktree that is gone. Set `NISSHI_WORKTREE_SERVICES=0` to use the default
project's services from a worktree instead.

### Local services on macOS (colima)

- Homebrew installs the `docker compose` plugin outside the Docker CLI's
  plugin directory. Add it to `~/.docker/config.json`:
  `"cliPluginsExtraDirs": ["/opt/homebrew/lib/docker/cli-plugins"]`.
- The smoke suite (`just smoke <engine>`) needs
  `NISSHI_SMOKE_DOCKER_HOST_IP=192.168.5.2`, the address at which colima's
  containers reach this machine.
- Each worktree's services use about 0.3 GB of the VM's memory when idle, and
  more while tests run. Size the VM for the worktrees that test at once, for
  example `colima start --memory 16` for about ten.

## Making a change

1. Fork the repo and create a branch off `main`.
2. Make your change. Match the existing code style; `just fmt` and
   `just clippy` will catch most of it. Write comments by the
   [comment rules](.claude/rules/comments.md), and choose names by the
   [naming rules](.claude/rules/naming.md).
3. Add or update tests. `just test` runs the full suite.
4. Open a pull request against `main`. Fill in the PR template — it's short
   on purpose. Merging requires an approval from a code owner and all
   required checks passing; see `.github/CODEOWNERS` for who that is.
   Link the approved issue unless the change is one of the small changes
   listed under [Propose first](#propose-first).

Keep pull requests focused on one change. A large, mixed-purpose PR is
harder to review and harder to revert if something goes wrong.

Before you ask for review, check that:

- every commit is signed off (see [below](#sign-off-your-commits-dco));
- `just fmt`, `just clippy`, and `just test` pass locally;
- tests cover the behavior you changed;
- the docs describe any user-facing behavior you changed.

## Pull request titles and merging

Pull requests are squash-merged through the merge queue. Each one becomes a
single commit on `main`: the PR title is the commit's subject, and the PR
description is its body. The changelog is generated from these commits when
a release is cut, so don't edit `CHANGELOG.md` in a pull request. Write the
description for someone reading `git log`: what a user or operator sees
differently, and why.

The title must follow [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/),
optionally after a Jira key prefix, and a check fails the PR until it does:

```text
type(scope): summary
```

- `type` is one of `feat`, `fix`, `perf`, `refactor`, `docs`, `test`,
  `build`, `ci`, `chore`, or `revert`.
- `scope` is optional. Use the crate or storage backend the change is about,
  for example `dynostore`, `sql`, or `proxy`.
- Add `!` after the type or scope for a breaking change, for example
  `feat(sql)!: summary`, and say what breaks in the description.
- GitHub's Revert button titles its pull request `Revert "<title>"`, which
  fails the check. Retitle it `revert: <title>`.
- A Jira key prefix is optional, for example `SOL-123: fix(sql): summary`.
  See the next section.

### Linking a Jira ticket

Solace maintainers track work in Jira, and Solace's autobots service links a
pull request to a ticket through a Jira key in its title. Prefer the key
after the type (`fix(sql): SOL-123 summary`), because the commit subject on
`main` then stays a Conventional Commits header, which a tool that parses the
spec strictly can read. The check also accepts the key in front
(`SOL-123: fix(sql): summary`). Autobots reads only the title, not the
description or the commits.

When the title has a key, autobots posts three statuses on the pull request:
`autobots/pr-checker/jira-found`, `jira-assignee-found`, and
`jira-status-check`. They report whether the ticket exists, has an assignee,
and is in an in-progress status. None of them is a required check, so they
never block a merge. A pull request without a key, from an outside
contributor or Dependabot for example, merges as usual.

When a pull request with a key merges, autobots moves the ticket on in its
Jira workflow. A SOL ticket, for example, closes as Done.

## Releases

Maintainers cut a release with the `/cut-release X.Y.Z` Claude Code skill
(`.claude/skills/cut-release/`). The skill opens a release PR that bumps the
version and adds a `CHANGELOG.md` section for every commit since the previous
stable tag. After that PR merges, the skill tags its commit on `main`. The
release workflow refuses a tag whose version differs from `Cargo.toml`, and a
stable tag that has no `CHANGELOG.md` section. A pre-release
(`/cut-release X.Y.Z-pre.N`) only bumps the version and has no changelog
section of its own. The next stable section covers its changes.

## What CI tests on each storage engine

CI runs its checks in three tiers:

- **Tier A, every PR**: on each push to a pull request.
- **Tier B, merge queue**: once more, after the pull request enters the merge queue. These jobs
  show as skipped on the pull request itself.
- **Tier C, twice a day**: on `main`, in the scheduled `tier-c` workflow.

A check marked "not required" runs, but the merge goes ahead even if it fails. In the merge queue,
the failure shows only in the job's report. In Tier C, a failure opens a "Nightly Tier C run
failed" issue, which closes when a later run passes. A dash means that CI does not run the check
on that engine.

| Check | PostgreSQL | SQLite | In-memory | S3 | Turso |
| --- | --- | --- | --- | --- | --- |
| Broker tests | Every PR, on PostgreSQL 16. Twice a day on PostgreSQL 17, 18 and the current 19 beta, not required. | Every PR | Every PR | – | Every PR, with most tests skipped ([#866]) |
| Build with only this engine | Merge queue | Merge queue | Merge queue | Merge queue | Merge queue |
| Kafka CLI smoke suite, with Kafka 3.7.2, 3.9.2 and 4.3.1 | Merge queue | Merge queue | Merge queue | Merge queue, topic create, list, describe and delete tests only | – |
| librdkafka v2.14.2 and franz-go v1.22.2 suites | Merge queue | Merge queue | Every PR | Twice a day, not required | – |
| librdkafka v2.3.0 suite | Merge queue, not required | Merge queue, not required | Merge queue, not required | Twice a day, not required | – |

The in-memory and S3 engines share one implementation, so the broker tests for the in-memory
engine also test the code that the S3 engine runs.

[#866]: https://github.com/nisshi-io/solace-nisshi/issues/866

## Sign off your commits (DCO)

This project's contributor-agreement position is the Developer Certificate
of Origin: no separate contributor license agreement (CLA) is required.

Every commit must include a `Signed-off-by` line certifying you wrote it or
otherwise have the right to submit it under this project's license (the
[Developer Certificate of Origin](https://developercertificate.org/)).

Add it automatically:

```shell
git commit -s -m "your commit message"
```

If you forgot on commits already made:

```shell
git rebase --signoff main
```

Pull requests are squash-merged, so the commit on `main` doesn't carry the
`Signed-off-by` lines. The commits in the pull request are the record, so
sign off each of them.

A `DCO` check reports on every pull request. It fails if a commit in the
pull request has no `Signed-off-by` line, or has one that doesn't match the
name and email of the commit's author or committer. It is a required status
check, so a pull request can't merge while it fails. To fix a failure, run
the `git rebase --signoff main` command above, then force-push. Commits already
on `main` predate this expectation and aren't checked.

## Code of conduct

Participation in this project is governed by our
[Code of Conduct](CODE_OF_CONDUCT.md).

## Reporting a security issue

Don't open a public issue for a security vulnerability — see
[SECURITY.md](SECURITY.md) instead.
