# AGENTS.md

This file provides guidance to coding agents (Claude Code, Codex, Cursor and others) when working with code in this repository. Claude Code reads it only while the repository has no `CLAUDE.md`, so don't add one.

## Rules

Before your first edit of a file that a row covers, read the file that the row names, with a tool call. Skip a row whose file your tool has already loaded: Claude Code loads `.claude/rules/` and the nested `AGENTS.md` files by itself.

| When | Read |
| --- | --- |
| Before you edit any `Cargo.toml` | `.claude/rules/cargo-dependencies.md` |
| Before you edit any `.rs`, `.proto`, `.sql`, `.sh`, `.toml`, `.yml` or `.yaml` file, a `justfile` or a `Dockerfile` | `.claude/rules/comments.md` |
| Before you create or edit any `.rs`, `.proto` or `.sql` file | `.claude/rules/file-header.md` |
| Before you edit any `.rs` file | `.claude/rules/modules.md` |
| Before you edit any `.rs` or `.sh` file or a `justfile` | `.claude/rules/naming.md` |
| Before you edit anything under `nisshi-smoke-test/` | `.claude/rules/smoke-tests.md` |
| Before you edit anything under `nisshi-broker/tests/it/` or `nisshi-storage-integration-test/` | `.claude/rules/storage-integration-tests.md` |
| Before you edit anything under `nisshi-storage-sql/` | `.claude/rules/sql-queries.md` |
| Before you edit any `.rs` file | `.claude/rules/testable-design.md` |
| Before you edit any `.rs` file | `.claude/rules/unit-test-layout.md` |
| Before you edit anything under `.github/` | `.github/AGENTS.md` |
| Before you edit anything under `fuzz/` | `fuzz/AGENTS.md` |

Before you hand off a change, reread each comment that you added or changed against the "Before You Hand Off" checklist in `.claude/rules/comments.md`.

## Project Overview

Nisshi is a stateless server written in Rust that implements the Apache Kafka® API, with pluggable storage backends: PostgreSQL, libSQL (SQLite), S3/object store, and memory. Schema-backed topics (Avro, JSON Schema, Protocol Buffers) can be written as Apache Iceberg or Delta Lake tables.

- Rust edition 2024, toolchain pinned to 1.98 (`rust-toolchain.toml`)
- Minimum Rust version: `rust-version` in the root `Cargo.toml`, inherited by every workspace crate, equal to the minor version of the pinned channel (1.98 for a channel of 1.98 or 1.98.1). `cargo install` on an older toolchain stops with cargo's message. The `rust-version` CI job checks that they match, so change both in one PR. A pin to a point release does not raise the minimum. A bump of the minor version does, for `cargo install` users and for every crate that depends on a published `nisshi-*` crate, so the PR description of such a bump must state the new minimum Rust version, because the changelog is generated from it
- License: Apache-2.0
- `unsafe_code` is forbidden workspace-wide

## Build & Test Commands

The project uses `just` as a task runner (loads `.env` automatically).

```shell
just                 # default: fmt, build, test, clippy
just build           # build with all features (dev profile)
just build-all       # build every target (bins, examples, tests, benches) with all features - use this to verify a change builds cleanly workspace-wide
just test            # nextest + doc tests - use this to rerun the full test suite after a change
just test-workspace  # cargo nextest run --workspace --all-targets --all-features, excluding nisshi-smoke-test
just test-doc        # cargo test --workspace --doc --all-features
just test-fast       # test-workspace without the tests that need a docker compose service (Postgres, MinIO, Iceberg catalog) - no `just ci` needed
just doc             # rustdoc, warnings denied, private items too; pass --open to browse
just clippy          # cargo clippy --workspace --all-features --all-targets -- -D warnings, then the same over fuzz/
just fmt             # cargo fmt --all --check, then the same over fuzz/
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
- **Each worktree runs its own services.** In a linked worktree (not in the main checkout, not when `CI` is set, and not when `NISSHI_WORKTREE_SERVICES=0`), `etc/worktree-services.sh up` starts `db`, `minio` and `lakehouse-catalog` in the compose project `nisshi-wt-<worktree directory>-<hash of its path>`, on loopback ports that Docker picks. It needs Docker, `jq` and `curl`, and names the one that's missing. It then writes `.env`, starting from `example.env` if there is none: `COMPOSE_PROJECT_NAME`, the `NISSHI_*_PORTS` variables, `AWS_ENDPOINT`, `ICEBERG_CATALOG` and `NISSHI_TEST_DATABASE_URL`. A later `docker compose` or `just` command in the worktree then uses that project. nextest runs the script as a setup script before the `nisshi-broker`, `nisshi-storage-sql` and `nisshi-schema` tests and the `nisshi-storage-dynostore` tests that need MinIO, so the first test run starts the services. In a worktree, `just ci` removes and starts the worktree's own project, and `just broker` and `just server` start it before the broker; only grafana and prometheus keep fixed ports, so one worktree at a time runs `just broker`. `just services-up`, `just services-down` and `just services-prune` (removes the project of each worktree that is gone) run the script directly. Stacks stay up until `services-down` or a prune. In the main checkout, `just ci`, `just broker` and `just server` still tear down and restart the default project, which other sessions may be using, so don't run them there to restart services. See CONTRIBUTING.md for colima on macOS.
- **`.env` is copied into Claude Code worktrees.** `.worktreeinclude` lists `.env`, so `claude --worktree` and subagent worktrees get a copy, and the script above rewrites its service settings.

## Architecture

`ARCHITECTURE.md` is the overview for human contributors; this section keeps the details an agent needs. Update both when the architecture changes.

Cargo workspace (members, including `nisshi-fuzz-harness`, are listed in the root `Cargo.toml`; `fuzz` is a workspace of its own), producing a single binary (`nisshi`) with subcommands: `broker` (default), `cat`, `topic`, `user`, `generator`, `perf`, `proxy`.

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
| `nisshi-storage-integration-test` | Tests of the broker on a storage engine that the caller passes in; `nisshi-broker` runs them on each engine in `tests/it/storage_integration.rs`. Not published |
| `nisshi-schema` | Schema registry + Iceberg/Delta/Parquet lake integration |
| `nisshi-client` | Async Kafka protocol client (rama service layers) |
| `nisshi-model` | Kafka JSON protocol definitions (used in build.rs) |
| `nisshi-auth` | SASL authentication: mechanism handshake, SCRAM credential verification |
| `nisshi-cat` | CLI: produce/consume Avro, JSON, Protobuf messages |
| `nisshi-cli` | Clap-based CLI argument parsing. `EnvVarExp<T>` wraps an argument so that `${VAR}` references expand at parse time, and a reference to an unset variable is a parse error |
| `nisshi-fuzz-harness` | The encode, decode and byte-compare checks that the fuzz targets and their tests share |
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

`RequestChannelService`'s `Storage` trait impl (`nisshi-storage/src/service.rs`) sends a `Request` over a channel and extracts the matching `Response` variant; every method does this via the `serve_and_extract!(self, request_expr, ResponseVariant)` macro rather than repeating the match/unwrap boilerplate - add new methods the same way. `fetch` and the two produce methods are the exceptions. `fetch` is one because a fetch read sends its records to the caller while it runs, instead of returning them at the end. The produce methods go through `produce_batch` instead, because a produce that waits for a slot drops its decoded batch first, so it reserves the slot itself and then sends through it. `Request::Fetch` carries the read's `FetchedSender` for that, and the response only says whether the read failed.

The caller (`FetchUntilDeadline::fetch_until_deadline` in `nisshi-storage/src/fetched.rs`) stops reading at the deadline. The channel sees this through the `FetchedSender`, and then:

- skips the read if it hasn't started;
- drops the read if it's running.

A read stuck in a blocking call can't be dropped, and the channel waits for it to end before it serves the next request, because mpsc mode serves one request at a time.

### Broker Specifics

- Node ID is always **111** (single-node, stateless design - this is intentional)
- Group coordination in `nisshi-broker/src/coordinator/group/`
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
- Without Docker or the services (an agent sandbox, a laptop without Docker, or a quick loop that shouldn't start a stack), run `just test-fast` and `just test-doc`, which need no service. `just test-fast` runs `just test-workspace` with the `fast` profile in `.config/nextest.toml`, whose filter leaves out the `nisshi-broker` tests that need Postgres, the `nisshi-schema` tests that need the Iceberg catalog and MinIO, and the `nisshi-storage-dynostore` tests that need MinIO, and it sets `NISSHI_WORKTREE_SERVICES=0`, so a worktree's run starts no services. Run `just test` before you ask for review; CI runs every test.
- The `fast` filter finds the tests that need a service by name, so place a new one where the filter sees it: a `nisshi-broker` test that needs Postgres goes in a `pg` module (by hand or with `backend!(pg, "postgres", ...)`), a `nisshi-schema` test that needs the Iceberg catalog goes in the `berg` test target, and a `nisshi-storage-dynostore` test that needs MinIO goes in a module of `dynostore::tests` whose name ends in `_s3`, such as `fetch_s3`. A test that skips itself when its service is down (as the `nisshi-storage-sql` Postgres tests do) can stay in the fast tier.
- Tests load `.env` via `dotenv().ok()`
- Tests in `nisshi-broker` run against multiple backends: InMemory, Lite (libSQL), Postgres, SlateDb
- `nisshi-broker`, `nisshi-sans-io` and `nisshi-service` each build one integration-test binary, `it`. To add a test file, create `tests/it/<name>.rs` and declare it with `pub mod <name>;` in `tests/it/main.rs`; Cargo ignores undeclared files, and the `every_test_file_is_declared` test fails if one is missed. Gate backend-specific tests with `#[cfg(feature = "...")]` on a module, not `required-features`. Run one file's tests with a name filter, e.g. `cargo nextest run -p nisshi-broker --all-features -E 'test(/^fetch::/)'`
- In `tests/it/`, give each feature its own test file. Put the tests of one feature or one Kafka API in one file there, named after it. Tests for a new feature go in a new file, not at the end of a file about another subject. A reader then finds them by name, and a name filter on the module selects them. (Unit tests in `src/` follow `.claude/rules/unit-test-layout.md`: a feature's unit tests go in `<module>/tests/<feature>.rs`.)
- Assert a pattern with `assert_matches!(value, pattern)` from std (`use std::assert_matches;`, stable since Rust 1.96, no crate needed), not `assert!(matches!(value, pattern))` or the `assert_matches` crate. On failure it prints the value, and it reads as one assertion. Nothing enforces this yet, and some older tests still use `assert!(matches!(..))`; leave those alone unless you're already editing them.
- `nisshi-smoke-test` runs the real Kafka CLI tools (in a `$KAFKA_IMAGE` container) against a broker, and stays out of `just test`; run it with `just smoke <engine>`. Its containers use the host network, so on macOS it needs Docker Desktop with host networking turned on, or, under colima, `NISSHI_SMOKE_DOCKER_HOST_IP=192.168.5.2`, which gives the Kafka CLI container its own network and sends its connections to `127.0.0.1` to this machine. Its tests talk to the shared broker through `KafkaCli::shared()`, or start their own with `Broker::isolated()`, and each uses its own topic and group names (`nisshi_smoke_test::unique::name`). A test that applies only to some engines is gated with `#[cfg(feature = "<engine>")]` or `#[cfg(any(...))]` on the engines' features, never with `cfg(not(...))`, because `just clippy` turns every feature on and would never compile it. The test files are grouped in folders under `tests/smoke/`: `main.rs` declares each folder's module, and `<folder>.rs` declares each file in it, which `every_test_file_is_declared` checks. S3 runs only `topic_admin::topic_lifecycle`, selected by the `smoke-s3` profile in `.config/nextest.toml`. A test that fails because of an open bug is marked `#[ignore = "<what is broken>"]`, or `#[cfg_attr(feature = "<engine>", ignore = "...")]` if it fails only on some engines. CI skips it, local runs still run it (`run.sh` passes `--run-ignored all` outside CI), and the change that fixes the bug removes the attribute
- Single-file test targets with specific feature requirements (e.g. `nisshi-schema`'s `berg`) use `required-features` in their `Cargo.toml`

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
| `fuzz/` | `cargo-fuzz` targets exercising the sans-io codec and storage services (see `fuzz/AGENTS.md`) |

## Lint Configuration

Workspace-level in `Cargo.toml`: `clippy::all = warn`, `unsafe_code = forbid`, `non_ascii_idents = forbid`, `rust_2018_idioms = deny`, `unreachable_pub = warn`, `broken_intra_doc_links = deny`, `private_intra_doc_links = deny`. CI runs `clippy -- -D warnings` (all warnings are errors) and `just doc`, which runs rustdoc with warnings denied.
