---
paths:
  - "nisshi-broker/tests/it/**"
  - "nisshi-storage-integration-test/**"
---

## What belongs in the suite

`nisshi-storage-integration-test` holds the tests of Nisshi code that runs on top of a storage
engine, such as the group coordinator (`Controller`) or a request handler (`XxxService`). Any
engine can run them: Nisshi runs them on its own engines in
`nisshi-broker/tests/it/storage_integration.rs`, and an engine outside Nisshi can run them on its
own (the crate isn't published; see its README). One question decides where a test goes: **if the
test failed on someone's engine, whose code would the bug be in?**

- **Nisshi's code running on the engine:** the test goes in the suite. Only Nisshi knows what that
  code expects of the engine, and every engine needs the check.
- **The engine itself:** the test stays in `nisshi-broker/tests/it`. A test that checks what it
  gets from the engine's own methods, such as `storage.list_groups()`, tests the engine, and the
  engine's authors test their own engine.

Judge a test by what it checks, not by its setup. A case may create a topic or commit an offset
through the engine, and then check the answer of Nisshi's code. Wire-level, authentication,
startup and shutdown tests, and tests of one engine or of a faulty engine, stay in
`nisshi-broker/tests/it`.

## Rules

- **Never build an engine in the suite.** A case takes the engine that its caller passes in, as
  `storage: G` with `G: Storage + Clone`. A case that builds its own engine passes on every
  caller while it tests only that engine. `clippy.toml` bans the storage and broker builders, and
  `tests/it/dependencies.rs` fails if the crate depends on an engine crate. Only
  `tests/it/outside_caller.rs` builds one, as a caller does.
- **Give a case one of the macro's argument lists.** A case takes `(cluster_id, broker_id,
  storage)`, `(broker_id, storage)` or `(storage)`, in that order, and returns a `Result`. For
  other arguments, write a small `pub async fn` with one of these lists that calls the shared
  function, as `new_cg::consumer_next_action_08c` does.
- **Register each case in the macro.** Add the case to its file's `__file!` list in `src/lib.rs`.
  A case that the macro doesn't list runs nowhere, and `tests/it/cases.rs` fails. A new file also
  needs a `pub mod` line in its area's module and a rule in `__unless_listed!`. A new area needs
  its `__area_<area>!` macro, a call in `__areas!` and a rule in `__unless_listed!`.
- **Put a case that needs a transactional ID in `transactions`.** An engine without transactions
  leaves out that one area. When a file has cases of both kinds, split it, and name the
  transactional half `txn_<file>`, as `txn_offset_fetch_commit_metadata` is.
- **Name each file once across all areas.** A `without` name leaves out every file of that name,
  so a name shared by two areas can't leave out only one of them.
- **Assert Kafka's behaviour.** When one of Nisshi's engines answers differently from Kafka, keep
  the Kafka assertion and tag the case `not_on_<engine>` for that engine. Write the reason at the
  engine's call in `storage_integration.rs`. Don't loosen the assertion to accept every engine's
  answer: the other engines would no longer pin the right one. `unknown_group_is_not_found` and
  `empty_group_id_is_not_found` in `delete_groups` show the form. When a case also checks
  something that every engine must pass, so that no engine can leave it out, it may accept more
  than one answer. Then add a strict case beside it that sends the same request, as
  `unknown_group_in_mixed_request_is_not_found` does for `mixed_request`.
- **Decide each new case's engines.** A new case runs on every engine that runs its file. SlateDB
  is deprecated and its coverage is frozen (`AGENTS.md`), so tag a new case `not_on_slatedb` when
  it lands in a file that SlateDB runs. Turso leaves out most files until #866 is done; check its
  `without` list.
- **Keep the groups for Nisshi's engines.** Each `not_on_<engine>` group names one of Nisshi's
  engines that leaves out the case, not what the case tests. An outside caller leaves out areas
  and files, and skips a single test by its name in its test runner. Write a case's attributes
  for every engine, such as `#[ignore = "..."]` with a reason, before its groups.
- **Tell callers when a file moves.** Moving, splitting or renaming a file changes which cases a
  caller's `without` names leave out, and the test names change. Say so in the PR.
- **Give each topic, group and transactional ID a random name.** Use
  `random::alphanumeric_string`. An engine may keep several tests' data in one database, as
  Nisshi's PostgreSQL engine does, apart only by cluster ID. A fixed name makes the case depend on
  that. Use a fixed name only when the case checks that name, such as an empty group ID or one
  with a `/`.

## When a check fails

- **`every_engine_building_file_is_listed`** (`nisshi-broker/tests/it/engine_building_files.rs`):
  a file in `nisshi-broker/tests/it` builds an engine and isn't on a list. Move its tests to the
  suite if they check Nisshi's code on any engine. If not, add the file to the list whose doc
  comment gives its reason. The check also fails for a listed file in which it no longer finds an
  engine builder, or that is on two lists. If such a file still builds an engine through a new
  helper, add the helper's name to `ENGINE_NAMES`.
- **`every_case_is_registered`** (`tests/it/cases.rs`): add the case to its file's `__file!` list.
  If the message names a missing `@search` rule, add the area's or file's rule to
  `__unless_listed!`, before the rule that steps over a name.
- **`dependencies_include_no_engine`** (`tests/it/dependencies.rs`): remove the engine crate, or
  the engine feature of `nisshi-broker`, from the crate's `[dependencies]`.
- **"recursion limit reached while expanding `__in_group!`"**: a long `without` list needs
  `#![recursion_limit = "256"]` in the caller's test crate.
