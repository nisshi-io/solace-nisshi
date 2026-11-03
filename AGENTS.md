# AGENTS.md

This file provides guidance to coding agents (Claude Code, Codex, Cursor and others) when working with code in this repository. `CLAUDE.md` imports it.

## Project Overview

Nisshi is a stateless server written in Rust that implements the Apache Kafka® API, with pluggable storage backends: PostgreSQL, libSQL (SQLite), S3/object store, and memory. Schema-backed topics (Avro, JSON Schema, Protocol Buffers) can be written as Apache Iceberg or Delta Lake tables.

- Rust edition 2024, toolchain pinned to 1.98 (`rust-toolchain.toml`)
- License: Apache-2.0
- `unsafe_code` is forbidden workspace-wide

## Build & Test Commands

The project uses `just` as a task runner (loads `.env` automatically).

```shell
just                 # default: fmt, build, test, clippy
just build           # build with all features (dev profile)
just build-all       # build every target (bins, examples, tests, benches) with all features - use this to verify a change builds cleanly workspace-wide
just test            # nextest + doc tests - use this to rerun the full test suite after a change
just test-workspace  # cargo nextest run --workspace --all-targets --all-features, excluding fuzz and nisshi-smoke-test
just test-doc        # cargo test --workspace --doc --all-features
just test-fast       # test-workspace without the tests that need a docker compose service (Postgres, MinIO, Iceberg catalog) - no `just ci` needed
just doc             # rustdoc, warnings denied, private items too; pass --open to browse
just clippy          # cargo clippy --workspace --all-features --all-targets -- -D warnings
just fmt             # cargo fmt --all --check
just shellcheck      # shellcheck every tracked *.sh / *.bash script
just lint-workflows  # actionlint, zizmor (as workflow-lint.yml runs them) and pinact over .github/workflows; not in `default`, see the recipe for install and token setup
just check           # cargo check --workspace --all-features --all-targets
just smoke <engine>  # Kafka CLI smoke suite (nisshi-smoke-test) against postgres, sqlite, memory or s3; starts and removes its own broker and services
just ci              # (re)starts the docker compose services (postgres, minio, lakehouse) that integration tests depend on, with empty volumes: in a worktree, the worktree's own project; in the main checkout, the shared default project (see Worktrees)
just services-up     # start this worktree's own services and write their ports to .env; services-down removes them, services-prune removes those of removed worktrees
just kafka-ref       # clone Apache Kafka at the version of nisshi-sans-io/message/ into kafka-ref/ (gitignored), to read as the reference; see Checking Against Apache Kafka
```

Run a single test with nextest:
```shell
cargo nextest run --workspace --all-features -E 'test(test_name_here)'
```

### Local Development Environment

```shell
cp example.env .env    # then edit .env as needed (AWS_ENDPOINT for local minio, etc.)
just ci                # starts minio, postgres, lakehouse via docker compose
just broker            # build + start broker with full infrastructure
just broker-postgres   # broker with postgres backend only
just broker-sqlite     # broker with sqlite backend only
just broker-memory     # broker with in-memory backend only
just broker-s3         # broker with S3/minio backend only
```

Note: when running nisshi directly (not via docker compose), set `AWS_ENDPOINT="http://localhost:9000"` in `.env`.

### Worktrees

- **Build output stays per worktree.** Each worktree builds into its own `target/`. Don't point worktrees at a shared one (`CARGO_TARGET_DIR`, `build.target-dir`). Cargo gives a workspace crate the same artifact names in every checkout and decides whether to rebuild by comparing file modification times, so a checkout whose sources are older than another checkout's last build reuses that build, and `cargo test` runs the other worktree's code.
- **Seed a new worktree with `just seed-target` before its first build.** The recipe clones the main checkout's `target/debug` copy-on-write, so the clone takes no extra disk until cargo rewrites a file. It then runs `cargo clean --workspace` on the clone, so only dependency artifacts carry over and the workspace crates rebuild from this worktree's sources. The recipe seeds no release, profiling or cross-target build, because that clean covers only the dev profile. It does nothing when `target/debug` already exists, and an interrupted seed leaves no `target/debug`, so a rerun starts again. Don't run it while the main checkout builds. Don't bring `target/` into a worktree any other way (`.worktreeinclude`, `worktree.symlinkDirectories`, a plain copy): those carry workspace-crate artifacts over, and cargo then reuses another checkout's code as the previous bullet describes.
- **Each worktree runs its own services.** In a linked worktree (not in the main checkout, not when `CI` is set, and not when `NISSHI_WORKTREE_SERVICES=0`), `etc/worktree-services.sh up` starts `db`, `minio` and `lakehouse-catalog` in the compose project `nisshi-wt-<worktree directory>-<hash of its path>`, on loopback ports that Docker picks. It needs Docker, `jq` and `curl`, and names the one that's missing. It then writes `.env`, starting from `example.env` if there is none: `COMPOSE_PROJECT_NAME`, the `NISSHI_*_PORTS` variables, `AWS_ENDPOINT`, `ICEBERG_CATALOG` and `NISSHI_TEST_DATABASE_URL`. A later `docker compose` or `just` command in the worktree then uses that project. nextest runs the script as a setup script before the `nisshi-broker`, `nisshi-storage-sql` and `nisshi-schema` tests, so the first test run starts the services. In a worktree, `just ci` removes and starts the worktree's own project, and `just broker` and `just server` start it before the broker; only grafana and prometheus keep fixed ports, so one worktree at a time runs `just broker`. `just services-up`, `just services-down` and `just services-prune` (removes the project of each worktree that is gone) run the script directly. Stacks stay up until `services-down` or a prune. In the main checkout, `just ci`, `just broker` and `just server` still tear down and restart the default project, which other sessions may be using, so don't run them there to restart services. See CONTRIBUTING.md for colima on macOS.
- **`.env` is copied into Claude Code worktrees.** `.worktreeinclude` lists `.env`, so `claude --worktree` and subagent worktrees get a copy, and the script above rewrites its service settings.

## Architecture

`ARCHITECTURE.md` is the overview for human contributors; this section keeps the details an agent needs. Update both when the architecture changes.

Cargo workspace (members, including the `fuzz` harness, are listed in the root `Cargo.toml`), producing a single binary (`nisshi`) with subcommands: `broker` (default), `cat`, `topic`, `user`, `generator`, `perf`, `proxy`.

### Key Crates

| Crate | Role |
|-------|------|
| `nisshi` | Binary entry point, subcommand dispatch |
| `nisshi-broker` | Kafka API broker: `Broker<G, S>` generic over Coordinator + Storage |
| `nisshi-sans-io` | **Code-generated** Kafka wire protocol (pure serde, no I/O) |
| `nisshi-service` | Network service layers built on `rama` (Layer/Service composition) |
| `nisshi-storage` | Storage abstraction: `Storage`/`StorageFactory` traits, `StorageContainer` builder |
| `nisshi-storage-dynostore` | Storage factories for `memory://`, `s3://`, `gs://` (ObjectStore-backed) |
| `nisshi-storage-null` | Storage factory for `null://`, a stub backend with no message persistence |
| `nisshi-storage-slatedb` | Storage factory for `slatedb://` (SlateDB, a KV store on object storage) |
| `nisshi-storage-sql` | Storage factories for `postgres://` and `sqlite://` (PostgreSQL, libSQL) |
| `nisshi-schema` | Schema registry + Iceberg/Delta/Parquet lake integration |
| `nisshi-client` | Async Kafka protocol client (rama service layers) |
| `nisshi-model` | Kafka JSON protocol definitions (used in build.rs) |
| `nisshi-auth` | SASL authentication: mechanism handshake, SCRAM credential verification |
| `nisshi-cat` | CLI: produce/consume Avro, JSON, Protobuf messages |
| `nisshi-cli` | Clap-based CLI argument parsing |
| `nisshi-generator` | CLI: generate fake data for schema-backed topics |
| `nisshi-otel` | OpenTelemetry common configuration |
| `nisshi-perf` | CLI: performance producer/consumer |
| `nisshi-proxy` | Apache Kafka®-compatible proxy |
| `nisshi-topic` | CLI: create, list, delete topics |

### Sans-I/O Code Generation (`nisshi-sans-io`)

`nisshi-sans-io/build.rs` reads ~185 official Kafka JSON message descriptors from `nisshi-sans-io/message/*.json` and generates typed Rust structs for every request/response pair. **Do not manually edit generated files.** The message JSON files are from upstream Apache Kafka.

The generator also emits `#[derive(arbitrary::Arbitrary)]` on every generated type, gated behind the `arbitrary` feature flag (`dep:arbitrary`, see `fn arbitrary_field_attribute` in `build.rs`) — this is what lets the `fuzz` crate construct arbitrary `Request`/`Response` values directly from fuzzer bytes instead of fuzzing only the wire decoder. A handful of fields need a hand-written `#[arbitrary(with = ...)]`/`#[arbitrary(default)]` attribute to stay sound (e.g. raw-byte fields route through `nisshi-sans-io/src/arbitrary_support.rs` rather than deriving); see the doc comment on `arbitrary_field_attribute` for the full list and why each one is needed.

### Service Layer Pattern (`nisshi-service`)

Uses `rama` crate for Layer/Service composition:
- `TcpBytesLayer` (TCP) -> `BytesFrameLayer` (bytes -> Kafka Frame) -> `FrameRouteService` (route to typed handlers) -> `FrameBytesLayer` -> `BytesTcpService`
- Same layering pattern used for broker, proxy, and CLI clients

#### rama 0.4.0: `Context<State>` replaced by `Extensions`

As of rama 0.4.0, `Service::serve` takes a single `req` parameter — the old `Service<State, Req>::serve(&self, ctx: Context<State>, req: Req)` two-parameter form and generic `State` are gone. Any ambient data (auth state, cluster id, maximum frame size, etc.) that used to travel via `Context<State>` now travels as a `rama::extensions::Extensions` bag carried *inside* the request wrapper type — see `nisshi-sans-io/src/input.rs` for `FrameInput`, `BodyInput`, `RequestInput<Q>`, and `BytesInput`, each a `{ value, extensions: Extensions }` pair.

Two things about `Extensions` are easy to get wrong when touching this code:

- **It is not fresh per request.** `Extensions` is `Arc`-backed; `.clone()` shares the same underlying store, and it is typically created once per connection (or per long-lived session, e.g. a consumer group) and reused/cloned across every request on it — not reconstructed per request the way the old per-call `Context` was. Code that inserts something once (e.g. `AuthenticationExtension` in `BytesFrameService`) must check `extensions.contains::<T>()` before inserting, since on the 2nd+ request on the same connection it will already be there. Don't assert it's absent — that's a real invariant violation waiting to break multi-round-trip flows like SCRAM.
- **`.fork()` vs `.clone()`**: `.fork()` creates an isolated child scope — reads fall through to the parent, but inserts land only on the child and don't leak back up. Use `.fork()` when issuing an internal/side request that should see the caller's extensions but not mutate them (e.g. `nisshi-proxy`'s internal `DescribeConfigsRequest` lookup). Use `.clone()` (sharing the same store) when the request is part of the same logical session and should accumulate/observe state alongside sibling requests (e.g. `ConsumerGroupService`'s per-session `Extensions` field).
- **`Extension` requires an explicit impl** (or `#[derive(Extension)]`) — there is no blanket impl for arbitrary `T`, so a generic type can't automatically be stored as an extension unless its concrete implementors opt in.

Per-handler dependencies (the `Coordinator` in `nisshi-broker/src/broker/group/*.rs`, the `Storage` handle `G` in `nisshi-storage/src/service/*.rs`) are still injected as plain struct fields (`struct FooService<C> { coordinator: C }`) on each handler rather than through the `Extensions` bag — that's deliberate, not a leftover of the migration.

### Storage Backends (`nisshi-storage`, `nisshi-storage-dynostore`, `nisshi-storage-null`, `nisshi-storage-slatedb`, `nisshi-storage-sql`)

Selected at compile time via feature flags, dispatched at runtime through a `StorageFactory` trait, not an enum: `StorageContainer::builder()` (`nisshi-storage/src/lib.rs`) collects one `Arc<dyn StorageFactory>` per enabled backend, and `.build()` picks the first whose `scheme()` regex matches the configured storage URL. Each backend crate implements the trait in its own `factory.rs`:
- `memory://`, `s3://`, `gs://` - ObjectStore-backed (`nisshi-storage-dynostore`, feature: `dynostore`)
- `null://` - stub backend, tracks topic metadata only, no message storage (`nisshi-storage-null`, always compiled in)
- `postgres://` - PostgreSQL (`nisshi-storage-sql`, feature: `postgres`)
- `sqlite://` - libSQL/SQLite (`nisshi-storage-sql`, feature: `libsql`)
- `slatedb://` - SlateDB KV store (`nisshi-storage-slatedb`, feature: `slatedb`). **Deprecated.** Only security fixes are accepted. When writing code, reviewing a PR or running a sweep, do not suggest or make bug fixes, refactors or feature changes for it, and do not flag non-security issues in it. Leave its tests and backend coverage as they are. Changes needed to keep it compiling and its tests passing after a workspace-wide change (a `Storage` or `StorageFactory` trait change, a shared dependency bump) are still expected, and so are routine Dependabot bumps of its dependencies.

`RequestChannelService`'s `Storage` trait impl (`nisshi-storage/src/service.rs`) sends a `Request` over a channel and extracts the matching `Response` variant; every method does this via the `serve_and_extract!(self, request_expr, ResponseVariant)` macro rather than repeating the match/unwrap boilerplate - add new methods the same way.

### Broker Specifics

- Node ID is always **111** (single-node, stateless design - this is intentional)
- Group coordination in `nisshi-broker/src/coordinator/group/`
- `EnvVarExp<T>` wrapper allows CLI args with `${VAR}` references expanded at parse time
- All `Error` types implement `Clone` (non-Clone errors wrapped in `Arc`)

## Checking Against Apache Kafka

Kafka's Java broker is the reference implementation, and no other spec exists. Compare against it when you plan, implement or review anything that a Kafka client can observe: the wire format, API version ranges, error codes, limits and defaults, what a client may do before it authenticates, and when the broker closes a connection.

- **Pin the version.** The message descriptors in `nisshi-sans-io/message/` are Kafka 3.9.1, and `ApiVersions` advertises their full ranges, so read Kafka at the `3.9.1` tag. `just kafka-ref` clones it into `kafka-ref/`: a blobless clone with full history, kept in the main checkout, and a linked worktree gets a symlink to it. Use `origin/trunk` in that clone only to ask whether something has changed since 3.9.1, and name the version you read.
- **Trace to where Kafka enforces it,** not to where it defines the constant. Find what the client sees on failure: an error code, a response, or a closed connection, and any delay.
- **Classify each difference** as one of: matches; stricter (check that no real client breaks); looser (needs a reason); or not applicable by design (node ID 111, a stateless broker, storage). Check Kafka's fix history in the same area (`git -C kafka-ref log -- <path>`) for bugs that Kafka already hit.
- **Evidence order:** running both beats reading code, and code beats docs. Run Kafka from the `apache/kafka:3.9.1` image, and the compat and smoke tests run real clients against Nisshi. A KIP shows intent, not what shipped.
- **Record it.** Cite permalinks pinned to the tag (`https://github.com/apache/kafka/blob/3.9.1/...`), never trunk, as `.claude/rules/comments.md` describes. `SaslHandshakeService::serve` in `nisshi-auth/src/handshake.rs` shows the link style. For a deliberate difference, comment at the code site with what Kafka does and why Nisshi differs.

## Feature Flags

Default: `dynostore`, `postgres`, `libsql`, `slatedb`. The `just build`/`release` feature lists below omit `turso`, which the broker and storage crates enable by default. Full build: `delta,dynostore,iceberg,libsql,parquet,postgres,slatedb`. The `nisshi` crate's own defaults, which `cargo install nisshi` builds, are `dynostore`, `libsql` and `postgres`; its tests check that they include the broker's default `memory://` engine.

Lake features: `parquet`, `iceberg`, `delta` - enable writing schema-backed topics to data lake tables.

## Testing Notes

- Tests use `cargo-nextest` (not `cargo test` for workspace tests)
- CI runs nextest with the `ci` profile in `.config/nextest.toml` (`NEXTEST_PROFILE=ci`), which writes a JUnit report to `target/nextest/ci/junit.xml`; local runs use the default profile. Both kill a test still running after 5 minutes, so a hung test doesn't hold its connections. The default profile runs at most 4 Iceberg tests (`nisshi-schema`'s `berg`) at once
- Tests that use PostgreSQL read its URL from `NISSHI_TEST_DATABASE_URL` (default `postgres://postgres:postgres@localhost`). A test that creates an Iceberg namespace drops it, with its tables, before it checks its results, so the catalog doesn't grow with each run
- Test logs go to `logs/<crate-name>/` (one file per test thread, dirs must exist)
- Integration tests require external services (postgres, minio, lakehouse): in a linked worktree, the first test run starts the worktree's own (see Worktrees); in the main checkout, `just ci` starts them. Rerun `just ci` if those services are in a bad state, then `just test` to rerun the suite
- Without Docker or the services (an agent sandbox, a laptop without Docker, or a quick loop that shouldn't start a stack), run `just test-fast` and `just test-doc`, which need no service. `just test-fast` runs `just test-workspace` with the `fast` profile in `.config/nextest.toml`, whose filter leaves out the `nisshi-broker` tests that need Postgres and the `nisshi-schema` tests that need the Iceberg catalog and MinIO, and it sets `NISSHI_WORKTREE_SERVICES=0`, so a worktree's run starts no services. Run `just test` before you ask for review; CI runs every test.
- The `fast` filter finds the tests that need a service by name, so place a new one where the filter sees it: a `nisshi-broker` test that needs Postgres goes in a `pg` module (by hand or with `backend!(pg, "postgres", ...)`), and a `nisshi-schema` test that needs the Iceberg catalog goes in the `berg` test target. A test that skips itself when its service is down (as the `nisshi-storage-sql` Postgres tests do) can stay in the fast tier.
- Tests load `.env` via `dotenv().ok()`
- Tests in `nisshi-broker` run against multiple backends: InMemory, Lite (libSQL), Postgres, SlateDb
- `nisshi-broker`, `nisshi-sans-io` and `nisshi-service` each build one integration-test binary, `it`. To add a test file, create `tests/it/<name>.rs` and declare it with `pub mod <name>;` in `tests/it/main.rs`; Cargo ignores undeclared files, and the `every_test_file_is_declared` test fails if one is missed. Gate backend-specific tests with `#[cfg(feature = "...")]` on a module, not `required-features`. Run one file's tests with a name filter, e.g. `cargo nextest run -p nisshi-broker --all-features -E 'test(/^fetch::/)'`
- Give each feature its own test file. Put the tests of one feature or one Kafka API in one file under `tests/it/`, named after it. Tests for a new feature go in a new file, not at the end of a file about another subject. A reader then finds them by name, and a name filter on the module selects them.
- Assert a pattern with `assert_matches!(value, pattern)` from std (`use std::assert_matches;`, stable since Rust 1.96, no crate needed), not `assert!(matches!(value, pattern))` or the `assert_matches` crate. On failure it prints the value, and it reads as one assertion. Nothing enforces this yet, and some older tests still use `assert!(matches!(..))`; leave those alone unless you're already editing them.
- `nisshi-smoke-test` runs the real Kafka CLI tools (in a `$KAFKA_IMAGE` container) against a broker, and stays out of `just test`; run it with `just smoke <engine>`. Its containers use the host network, so on macOS it needs Docker Desktop with host networking turned on, or, under colima, `NISSHI_SMOKE_DOCKER_HOST_IP=192.168.5.2`, which gives the Kafka CLI container its own network and sends its connections to `127.0.0.1` to this machine. Its tests talk to the shared broker through `KafkaCli::shared()`, or start their own with `Broker::isolated()`, and each uses its own topic and group names (`nisshi_smoke_test::unique::name`). A test that applies only to some engines is gated with `#[cfg(feature = "<engine>")]` or `#[cfg(any(...))]` on the engines' features, never with `cfg(not(...))`, because `just clippy` turns every feature on and would never compile it. The test files are grouped in folders under `tests/smoke/`: `main.rs` declares each folder's module, and `<folder>.rs` declares each file in it, which `every_test_file_is_declared` checks. S3 runs only `topic_admin::topic_lifecycle`, selected by the `smoke-s3` profile in `.config/nextest.toml`. A test that fails because of an open bug is marked `#[ignore = "<what is broken>"]`, or `#[cfg_attr(feature = "<engine>", ignore = "...")]` if it fails only on some engines. CI skips it, local runs still run it (`run.sh` passes `--run-ignored all` outside CI), and the change that fixes the bug removes the attribute
- Single-file test targets with specific feature requirements (e.g. `nisshi-schema`'s `berg`) use `required-features` in their `Cargo.toml`

## Fuzz Testing (`fuzz`)

The `fuzz` crate (`cargo-fuzz` + `libfuzzer-sys`) is a dev-only workspace member, excluded from `just test-workspace`. CI runs its targets through `fuzz.yml` (see CI Pipeline below), and you can run any of them locally. It requires the nightly toolchain, which `just` targets invoke explicitly (`cargo +nightly fuzz ...`); you don't need to switch your default toolchain.

```shell
just cargo-fuzz +args        # cargo +nightly fuzz {{args}}, with LTO off as in fuzz-ci
just fuzz-request-decode     # fuzz_request_decode, 60s
just fuzz-member-metadata    # fuzz_member_metadata, 60s
just fuzz-generate-seed      # (re)generate the corpus seed files, see below
just fuzz-ci <tier> [target]  # the targets of a tier in fuzz/ci-targets.json, each for its budget

cargo +nightly fuzz list                             # list all targets
just cargo-fuzz run <target> -- -max_total_time=60      # fuzz one target
just cargo-fuzz run <target> fuzz/artifacts/<target>/<crash-file>     # replay a saved crash
```

Three target shapes, all in `fuzz/fuzz_targets/`:
- **`fuzz_request_decode` / `fuzz_response_decode` / `fuzz_varint` / `fuzz_deflated_batch` / `fuzz_member_metadata`** — fuzz raw bytes straight through the wire decoder/encoder, no `Storage` involved.
- **`fuzz_<api>_storage`** (the majority, ~22 targets, one per Kafka API like `fuzz_fetch_storage`, `fuzz_create_topics_storage`) — build an arbitrary typed request (via `nisshi-sans-io`'s `arbitrary` feature) and run it end-to-end through the real `*Service` against an in-memory `StorageContainer` (storage starts empty every execution), to catch panics/invariant violations in service and storage logic rather than the decoder.
- **`generate_seeds`** (plain `fn main`, not a fuzz target) — writes hand-built binary seed files into `fuzz/corpus/<target>/`; rerun it after adding a new seed case.

`fuzz/src/lib.rs` provides the shared `check_request`/`check_response` helpers used by most storage targets: they encode a value at its API's latest version, decode it, re-encode the decoded value, and decode again — then assert the two decodes are equal. They deliberately do **not** assert the first decode equals the original fuzzed value: a sans-io codec is not required to be lossless against an arbitrary in-memory value (e.g. every generated sequence field is `Option<Vec<T>>` regardless of wire nullability, so a mandatory array left at `None` is wire-equivalent to `Some(vec![])` but not `PartialEq`). The round-trip property that must hold is that re-encoding an already-decoded (version-normalized) value decodes back to exactly the same thing. `normalize_request` returns that decoded value as a typed request, so a target can serve the service what a client could actually send instead of the raw fuzzed value (`fuzz_list_offsets_storage` does).

`fuzz/corpus/`, `fuzz/artifacts/` (crash reproducers) and `fuzz/.gitignore`'s `target`/`coverage` are all untracked — rebuild the corpus locally with `just fuzz-generate-seed` plus whatever `cargo +nightly fuzz run` accumulates. If you fix a bug found by fuzzing, replay every existing artifact for that target (`just cargo-fuzz run <target> fuzz/artifacts/<target>/<crash-file>`) to confirm the fix, not just the one input that was being worked on.

## CI Pipeline

GitHub Actions (`.github/workflows/ci.yml`) runs in two tiers, gated by `ci-gate`, the single required check that fans in every other job:

- **Tier A, every pull_request push:** `fmt`, `clippy` (which also runs `just doc`), `typos`, `shellcheck`, `image-ca-certificates` (builds the Dockerfile's `base` stage and fails if the image would trust a CA in `.github/scripts/distrusted-roots.txt`), `third-party-license`, `test` (postgres:16, the oldest version the project tests), the memory leg of `compat-librdkafka` and `compat-franz-go`.
- **Tier B, once per merge-queue entry (`merge_group`); a push to `main` runs only the cache-saving subset:** the `build-storage` matrix (one build per storage engine, `turso` included) and `build-storage-lake` (one build per lake format on `dynostore`), `test` on postgres:16 only, the postgres and sqlite compat legs (S3 compat runs in the nightly Tier C, which also runs `test` on Postgres 17, 18 and 19beta4), the `compat-librdkafka` legs at the oldest librdkafka version CI tests (v2.3.0) on every engine, experimental until they have a record in CI, `cargo-publish-dry-run`, `src`, `release` (the static Linux binaries), `macos` (`aarch64-apple-darwin`: a compile-only `cargo check`, and the full release build only on a `v*` tag; the scheduled Tier C workflow (twice a day) builds the macOS release on main), `smoke` (the `nisshi-smoke-test` suite: Kafka CLI tools from Kafka 3.9 and 4.3 against an image built in the job from `release`'s binaries on postgres, sqlite, memory and s3, on x86 and arm), `smoke-oldest-client` (the same suite with the Kafka 3.7 CLI tools, the oldest client CI tests, on postgres, sqlite, memory and s3 on x86 only); `smoke-report` puts every leg of both in one pass/fail grid in the step summary. The push to `main` repeats a tree that the queue already tested, so it runs only `fmt`, `clippy`, `test`, `cargo-publish-dry-run`, `release`, `macos` and `third-party-license` (the jobs that save a cache), plus `changes`, `typos`, `shellcheck`, `image-ca-certificates` and `ci-gate`. Every other job carries `github.ref != 'refs/heads/main'`, and `ci-gate` names the jobs that may skip there, so a new job must run on main until someone adds it to that list.
- **Publishing, from the scheduled Tier C run on main and on `v*` tags only:** `ci.yml` pushes no image. The scheduled Tier C workflow (`tier-c.yml`) pushes the multi-arch `edge` image of main's tip, also tagged `main` for external consumers until a follow-up drops that tag. Its `edge-gate` job requires a successful merge-queue run and a successful push run of `ci.yml` for the tip, and `edge-publish` builds the image from the `image-*` binaries of that push run, so nothing compiles. If a merge lands during the nightly run, or its push run is still in progress, the gate skips the publish and `edge` trails the tip until the next run. A `v*` tag doesn't trigger `ci.yml`: it runs `release.yml`, which calls `ci.yml` through `workflow_call` (so the full Tier B run happens on the tagged commit), an `on-main` check that the tagged commit is on main, a `release-tag` check (`.github/scripts/check-release-tag.sh`, tested in `workflow-lint.yml`) that the tag's version is the tagged commit's `Cargo.toml` version and that a stable tag has a `CHANGELOG.md` section, and `release-readiness` (`release-readiness-check.yaml`), which starts once `on-main` and `release-tag` pass, re-runs `guardian-scan.yaml` in trunk mode on the tag and blocks on the Guardian gate's verdict. Only when all four pass does it publish, one job after another: `publish-image` (semver and `latest`), `github-release` (holds `contents: write`, runs in the `release` environment, uploads the tarballs, and sets no body), then `publish-crates`. The `/cut-release` skill (`.claude/skills/cut-release/`) opens the release PR, tags its squash commit, and after the run sets the release body with `gh release edit`. If only `release-readiness` fails, an `override` job waits in the `release-override` environment, whose required reviewers are repo admins: approving it publishes over the failed check, a Guardian block and a scanner outage alike, and rejecting it ends the run with nothing published. The environment prevents self-review and admin bypass, and the `override` job refuses to run if either is off or if the run records no approval of `release-override` from someone other than whoever pushed the tag or started the re-run, so a second admin has to approve: whoever pushed the tag or started a re-run can't. The on-main check does not stop a tag whose commit edits the workflow, so a tag ruleset is the control for who may create `v*` tags. No other run pushes anything, and a failed leg can't leave a partial release. `release` needs only `typos` (and `changes`), and `smoke` needs only `release`, so `test`, `macos` and the build matrices run beside the `release` -> `smoke` chain instead of ahead of it. `test` doesn't wait for `fmt` or `clippy`.
- **Cache:** `test` saves the Rust cache under the `dev` shared key on main; the compat jobs restore it and never save. `build-storage` and `build-storage-lake` use no cache, because `cargo check` reuses little of a full build. rust-cache hashes every `CARGO*`, `CC*`, `CFLAGS*`, `CXX*`, `CMAKE*` and `RUST*` variable into the key, so set `RUST_BACKTRACE` and `RUST_LOG` on the step that needs them, not on the job.
- **Reports:** the `test` jobs (in `ci.yml` and `tier-c.yml`) run nextest and the doctests as separate steps, because doctests write no JUnit report; the doctests also run after a nextest failure. Both jobs then run the `.github/actions/test-reports` composite action, so a PR's `ci.yml` run exercises the steps the nightly run uses. Its `junit-summary.py` turns the JUnit report into the job's step summary: results per suite, the 10 slowest tests, and up to 30 failures, each with its message or the end of its stderr. The action uploads a `test-reports-<postgres image>` artifact with `junit.xml` and `cargo-timing.html`. `ci-gate` adds a table of job and step durations to its summary and uploads it as the `timings` artifact; it needs `actions: read`, so `release.yml`'s `ci` job grants it too. Every report step continues on error, so a report never changes a job's result.

Merging goes through a merge queue: "Merge when ready" queues the PR, the queue re-runs CI on it against the current tip of `main`, and squash-merges it if everything is green: the PR title becomes the commit's subject and the PR description its body. Tier B is skipped on same-repo PRs only while the `MERGE_QUEUE` repository variable is `on`; with it unset, they run everything. Fork PRs can't read the variable, so Tier B is always skipped on them and runs in the queue. The other required checks come from `codeql.yml`, `workflow-lint.yml`, `dependencies.yml` and `pr-title.yml` (the PR title must follow Conventional Commits; see `CONTRIBUTING.md`).

On a pull_request, a `changes` job in `ci.yml`, `codeql.yml`, `dependencies.yml` and `fuzz.yml` skips the Rust jobs when every changed path is CI config those workflows don't read, or prose. The skip list is `.github/scripts/ci-changes.sh`, tested by `ci-changes.test.sh`; the `changes` jobs run the base branch's copy, so a PR can't widen its own skip list.

`guardian-scan.yaml` reports supply-chain findings to Guardian, and its `Guardian scan gate` job is the one check a ruleset requires from it. On a PR it runs FOSSA's licensing and vulnerability checks in report-only mode and uploads nothing. Fork PRs, Dependabot PRs and merge-queue entries skip the scans and pass the gate with a logged reason. On push to main it also builds the musl image (with `cargo auditable`, like `release`), scans it with Prisma, syncs both results to Guardian and blocks on Guardian's vulnerability gate, and then `Update release manifest` writes the manifest row if the commit is still main's tip. `.github/scripts/guardian-scan-context.sh` decides the mode (pr, queue, trunk, branch) and `guardian-scan-gate.sh` decides the gate's verdict. Each has a self-test, and both run at the start of every scan. The scripts run from the workflow's own commit, never from the ref being scanned, and a dispatched tag must be spelled `refs/tags/<tag>` to count as a release (a bare `v1.2.3` is treated as a branch). Its secrets are split across two environments. `guardian` holds only `FOSSA_API_KEY`, for PR runs. `guardian-trunk` holds that plus `GUARDIAN_URL`, `GUARDIAN_API_TOKEN` and the three `PRISMACLOUD_*` credentials, and its deployment policy admits only the main branch and `v*` tags. Setting the `FOSSA_LICENSING_BLOCK` repository variable to `false` makes the licensing check report-only on trunk while the Guardian gate stays on. The repository also needs read access to the `maas-build-actions` package, the `solace-cloud-manifest-rw` custom property set to `true`, and the OIDC sub customization `[repo_property_solace-cloud-manifest-rw, repository_owner, ref]`, which the manifest role's trust policy matches for the main branch only.

`fuzz.yml` runs the cargo-fuzz targets in `fuzz/`: briefly on every PR (report-only, not a required check) and for longer nightly. Which targets run in each, and for how long, is set in `fuzz/ci-targets.json`; see `fuzz/README.md`. Run the PR set locally with `just fuzz-ci pr`.

## Key Files

| File | Purpose |
|------|---------|
| `justfile` | All build/test/run tasks |
| `example.env` | Template for local `.env` config |
| `compose.yaml` | Docker Compose: postgres, minio, grafana, jaeger, prometheus, lakehouse |
| `nisshi-storage-sql/src/pg/schema.sql` | PostgreSQL schema DDL that the broker applies at startup |
| `etc/schema/` | Sample schemas: `.avsc` (Avro), `.json` (JSON Schema), `.proto` (Protobuf) |
| `nisshi-sans-io/message/` | Kafka JSON protocol descriptors (upstream, ~185 files) |
| `nisshi-sans-io/build.rs` | Code generator: JSON descriptors -> Rust types |
| `fuzz/` | `cargo-fuzz` targets exercising the sans-io codec and storage services (see Fuzz Testing) |

## Lint Configuration

Workspace-level in `Cargo.toml`: `clippy::all = warn`, `unsafe_code = forbid`, `non_ascii_idents = forbid`, `rust_2018_idioms = deny`, `unreachable_pub = warn`, `broken_intra_doc_links = deny`, `private_intra_doc_links = deny`. CI runs `clippy -- -D warnings` (all warnings are errors) and `just doc`, which runs rustdoc with warnings denied.
