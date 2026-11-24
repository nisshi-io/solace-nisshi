# nisshi-storage-integration-test

Tests of Nisshi's broker running on top of a storage engine that the caller supplies. If an
engine passes these tests, Nisshi's consumer group, offset, producer ID and transaction code works
on it. Produce, fetch and topic tests aren't in the suite yet.

A test here checks Nisshi code above the `Storage` trait, such as the group coordinator or a
request handler, and not the engine itself. The crate never builds an engine: each caller passes
in the engine that it tests. Nisshi runs the suite on its own engines in
`nisshi-broker/tests/it/storage_integration.rs`, and an engine outside Nisshi can run it the same
way.

The crate isn't published (`publish = false`), and there is no plan yet to publish it. Depend on it
through Git or a path, and take every `nisshi-*` crate from the same revision, so that your engine
implements the `Storage` trait that the suite uses.

## Run the suite on an engine

Call `storage_integration_tests!` once for each engine, in an integration test of your crate.
Give the arguments in the order below. Put `#![recursion_limit]` in the root file of the test
crate, such as `tests/it/main.rs`; in any other module it has no effect.

```rust
// The macro searches the `without` list once for each area, file and group of the suite, so
// a long list can need a higher limit than the compiler's default.
#![recursion_limit = "256"]

use nisshi_storage::ArcDynStorage;
use uuid::Uuid;

/// Builds a fresh engine. Each test calls it once.
async fn my_engine(cluster_id: Uuid, broker_id: i32) -> nisshi_storage::Result<ArcDynStorage> {
    // ...
}

nisshi_storage_integration_test::storage_integration_tests! {
    engine: my_engine,              // the name of the module that holds the tests
    storage: my_engine,             // async fn (Uuid, i32) -> Result<impl Storage + Clone, E>
    before_each: init_tracing,      // optional: runs first in each test
    attributes: [ignore = "..."],   // optional: added to every test
    without: [transactions],        // optional: areas and files to leave out
}
```

The tests use `#[tokio::test]`, so your crate needs `tokio` as a dev-dependency.
`tests/it/outside_caller.rs` is a complete example that builds its own in-memory engine.

Each test is named `<engine>::<area>::<file>::<case>`, after the path of the module that calls
the macro, such as `storage::my_engine::consumer_group::list_groups::simple` for a call in
`tests/it/storage.rs`.

## Leave out tests

- **An area or a file:** name it in `without`, such as `transactions` for an engine without
  transactions. A name that the suite doesn't have fails the build.
- **A single test:** skip it by its name in your test runner, such as with a nextest filter. The
  macro has no list of single tests.
- **The `not_on_<engine>` groups** belong to Nisshi's own engines. Each one leaves out the cases
  that one of Nisshi's engines doesn't run. Don't name them in another engine's call.

## Stability

The macro, the areas, the files and the test names may change in any commit. When you move to a
newer Nisshi, update your call and your skip list. In particular, a file that moves, splits or is
renamed changes which cases a `without` name leaves out.

## Areas

| Area | Cases about |
| --- | --- |
| `consumer_group` | Joining, syncing, describing, listing and deleting consumer groups |
| `offset` | Committing and fetching a group's offsets, without a transaction |
| `producer_id` | The producer IDs that the broker gives an idempotent producer |
| `transactions` | Anything that needs a transactional ID |

## Add a test

Read [`.claude/rules/storage-integration-tests.md`](../.claude/rules/storage-integration-tests.md)
first: it says which tests belong here, and how to add a case, a file or an area. The crate's own
tests (`cargo nextest run -p nisshi-storage-integration-test`) check that:

- the crate depends on no engine (`tests/it/dependencies.rs`);
- every `pub async fn` in a file of an area is a case in the macro (`tests/it/cases.rs`);
- an outside caller can run the suite on its own engine (`tests/it/outside_caller.rs`).

Clippy also stops the crate's code from calling the storage or broker builders, which build an
engine (`clippy.toml`).
