---
paths:
  - "**/Cargo.toml"
---

## Cargo Dependencies

A dependency table in alphabetical order shows at a glance whether a crate is already there, and
two branches that each add a crate change different lines, so they merge without a conflict.

## Rules

- **Keep every dependency table in alphabetical order.** This covers `[workspace.dependencies]`,
  `[dependencies]`, `[dev-dependencies]`, `[build-dependencies]` and their
  `[target.'cfg(...)'.dependencies]` forms. Put a new crate in its place, not at the end.
- **Compare the key as written.** The order is the one an editor's sort-lines command gives:
  `futures-core` and `futures-util` come before `futures.workspace = true`, `tokio-util` before
  `tokio.workspace`, and `iceberg = { ... }` before `iceberg-catalog-rest`. Workspace crates
  (`nisshi-*`) take their place among the others, not a group of their own.
- **Keep one table as one block.** Don't split a table into groups with blank lines or heading
  comments: a group sorted on its own breaks the order of the table as a whole.
- **Move a comment with its entry.** A comment that explains a dependency, such as why its
  version is pinned, sits on the lines directly above it and moves when the entry moves.
