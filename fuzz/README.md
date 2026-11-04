# Fuzzing

cargo-fuzz targets for the Kafka wire protocol in `nisshi-sans-io`. Each target is a `[[bin]]` in `Cargo.toml` with its source in `fuzz_targets/`. `generate_seeds` isn't a target: it writes a starting corpus to `corpus/<target>/`.

## What CI runs

`ci-targets.json` decides which targets run in CI and for how long. It has two tiers, each a map of target name to `-max_total_time` in seconds:

```json
{
  "pr": { "fuzz_request_decode": 60 },
  "nightly": { "fuzz_request_decode": 1800 }
}
```

- `pr` runs on every pull request, all targets back to back in one job, so the job takes roughly the sum of the budgets plus the build. It's report-only: a crash fails the `fuzz` check but doesn't block the merge.
- `nightly` runs daily from `main` (and on demand from the Actions tab via "Run workflow"), one job per target.

To move a target between tiers, change its budget, or stop running it in CI, edit this file. Nothing in `.github/workflows/fuzz.yml` needs to change. A name in the file that isn't a target fails the run. A target that's in neither tier gets a warning, so a new target isn't silently skipped.

The PR job has a 50 minute timeout and each nightly job 75 minutes. The six `pr` budgets add up to 6 minutes of fuzzing, after the build. If the budgets outgrow the timeouts, raise `timeout-minutes` in `.github/workflows/fuzz.yml` too.

The fuzz recipes in the `justfile` build with `CARGO_PROFILE_RELEASE_LTO=false`, which turns off the workspace's cross-crate LTO (`lto = "thin"` in `Cargo.toml`) for fuzz builds only. With LTO, every target generates the machine code for its whole dependency graph when it links, which takes minutes per target, and the targets run fewer inputs per second. Without it, the dependencies compile once, and each further target compiles only its own code.

The `fuzz_<api>_storage` targets run in `nightly` only, 15 minutes each. `just fuzz-ci pr` builds only the targets of the `pr` tier, because a storage target links the whole storage stack and takes several minutes to build, which the `pr` job's timeout can't afford for all of them.

## Versions

A `fuzz_<api>_storage` target takes a `u16` beside the request. `nisshi_fuzz_harness::api_version` maps it onto one of the versions of the API that nisshi routes, and the target encodes the request and the response at that version, so old, non-flexible encodings are fuzzed too. Produce v0-2, ListOffsets v7-9 and AddPartitionsToTxn v4-5 are not routed, so they are not fuzzed. The checks live in the `nisshi-fuzz-harness` crate, whose `tests/versions.rs` runs them over every routed version of every storage target's API from fixed pseudo-random inputs, without libFuzzer. `just test-workspace` runs those tests, and so does `cargo test -p nisshi-fuzz-harness`.

The version selector is read before the request, so a crash artifact in `artifacts/<target>/`, or a corpus entry, saved before a storage target took it decodes to a different request, and replaying it passes. Replay such an artifact on a checkout from before the change. The `fuzz-corpus-*` cache that the nightly run restores starts these targets over for the same reason.

## Adding a target

1. Add `fuzz_targets/<name>.rs` and a `[[bin]]` entry in `Cargo.toml` (copy an existing one).
2. Add `<name>` to `pr`, `nightly` or both in `ci-targets.json`.
3. Optionally add seeds for it in `fuzz_targets/generate_seeds.rs`.

## Running locally

Needs a nightly toolchain, `cargo install cargo-fuzz` and `jq`. From the repo root:

```shell
just fuzz-ci pr                          # what a PR runs
just fuzz-ci nightly fuzz_request_decode # one target with its nightly budget
just cargo-fuzz run fuzz_varint          # a target with no time limit
```

## When CI finds something

Crashing, hanging (over 25s on one input) and out-of-memory (over 2 GB) inputs are written to `artifacts/<target>/` and uploaded as the `fuzz-artifacts-pr` or `fuzz-artifacts-<target>` artifact on the failed run. Download it and replay the input through the `cargo-fuzz` recipe, which builds without LTO, as CI does:

```shell
just cargo-fuzz run <target> path/to/crash-<hash>
```

## Corpus

Each nightly job minimises its target's corpus with `cargo fuzz cmin`, and the run saves all of them as one GitHub Actions cache entry (`fuzz-corpus-<run>`) from `main`. PR and nightly runs restore the latest entry before fuzzing, then add `generate_seeds`' seeds on top, so the short PR runs start from everything the nightly runs have found.
