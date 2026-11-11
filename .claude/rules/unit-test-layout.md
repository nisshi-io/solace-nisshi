---
paths:
  - "**/*.rs"
---

## Unit-Test Layout

A reader opens `src/` to read the implementation, so every unit test sits in a module named
`tests` and nothing else in `src/` holds only tests.

- **Start with an inline `tests` module.** Put a module's unit tests at the end of its file, in
  `#[cfg(test)] mod tests { use super::*; ... }`.
- **Move the tests out once the inline module passes 300 lines.** Write `#[cfg(test)] mod tests;`
  in `<module>.rs` and move the body to `<module>/tests.rs`. Make the move a commit of its own,
  with no other change, so review sees a pure move.
- **Give a feature's tests a file under `<module>/tests/`.** When the tests of a new feature do not
  belong with the tests already in `tests.rs`, put them in `<module>/tests/<feature>.rs` and
  declare the file from `tests.rs`. Inside `src/`, this is what "one test file per feature" means.
- **Never declare a test-only module beside the implementation.** Do not add
  `#[cfg(test)] mod <feature>_tests;` or any other test-only module next to the implementation
  modules.
- **Keep a module's tests under that module, not in a tree that mirrors `src/`.** A test module
  outside the module it tests cannot see that module's private items, so a crate-wide `src/tests/`
  forces `pub(crate)` onto code for its tests. Do not point `tests` at another folder with
  `#[path]`.
- **Keep test helpers under `tests`.** A double or fixture that one module's tests use lives in
  that module's `tests/` folder.
- **Test the public API from the crate's `tests/` folder.** A test that needs only `pub` items is
  an integration test in `tests/it/`.
