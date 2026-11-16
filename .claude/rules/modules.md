---
paths:
  - "**/*.rs"
---

## Modules

A reader should find a definition from the place that uses it: from a value to its type, and from
a path to its module. Rust has no rule that each type gets a file of its own. The rules below come
from the [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/) and the
[Rust Book](https://doc.rust-lang.org/book/ch07-04-bringing-paths-into-scope-with-the-use-keyword.html),
and they follow how the standard library lays out its code.

The examples below are illustrative, not taken from a real file, so they stay true when the code
changes.

## Rules

- **Make a function a method when it has a receiver.** A function that reads or changes one value
  belongs in the `impl` of that value's type ([C-METHOD]). A reader finds a method by typing the
  value and a dot, and rustdoc lists it on the type's page. Write `partition.high_watermark()`,
  not `high_watermark(&partition)`. A function that makes a value is an associated function of
  the value's type, such as `Connection::open(address)`.
- **Write a function, not a closure bound to a name.** A `let commit = |group| ...` that is
  called more than once has no doc comment and no written signature, and it reads the enclosing
  function's variables without saying which. Nothing outside that function can call it, so the
  next test that needs it defines it again. Write a private `fn` that takes what it uses as
  parameters. A closure passed straight to another function, as in `.map(|offset| offset + 1)`, is
  fine.
- **Give each file one cohesive unit.** Usually that is one type, with its `impl` blocks, its trait
  impls, its unit tests (in its `tests` module; see `unit-test-layout.md`), and the private helpers
  and small types that only it uses. Put two types in one file only when neither is used without
  the other.
- **Keep a type's inherent `impl` in the type's own file.** Rust accepts an `impl` block in any
  module of the crate, but a reader looks for a method in its type's file. An `impl` of a generic
  type for one type argument, such as `impl Response<Fetch>`, is part of what that argument means,
  so it lives with the argument's type.
- **Put a function without a receiver in a module named for its subject, and call it through
  that module.** The module name says what the functions are about, as `std::fs` and `std::env`
  do. A caller in another module imports the module and writes `fs::read_to_string(path)`, not
  `read_to_string(path)`, so the call shows where the function lives. Import a type by its own
  name. A private helper that only its own module calls needs no module of its own.
- **Give a subject its own file, however small.** A file of one or two functions is fine when it
  is the only natural home for them. Do not move unrelated modules into a parent's file, such as
  `lib.rs`, to save files: the parent then becomes a collection of things that have nothing in
  common. Write a module inline only when it belongs to the parent's own subject, as a `tests`
  module does.
- **Make a folder when its name says something true of every file in it.** A folder that groups
  files by role or by feature tells the reader where to look. A folder named for a word that its
  files only happen to share does not, and it adds a parent file that holds nothing but `mod`
  lines.
- **Let privacy show the boundary.** Code that needs a type's private fields belongs in the type's
  module. If another file needs a field opened with `pub(super)` to compile, move the code into
  the type's file, or give the type a method that does the work.
- **Name a module with children after its folder.** Write `topics.rs` beside `topics/`, never
  `topics/mod.rs`, so that each editor tab shows a different name. A folder never holds a file
  with the folder's own name, because clippy's `module_inception` rejects `topics/topics.rs`. The
  `common` module of a crate's integration tests is the exception, and stays `common/mod.rs`. In
  a crate that builds one integration-test binary, `it`, as `AGENTS.md` describes, it is
  `tests/it/common/mod.rs`. In a crate that builds a binary from each file in `tests/`, it is
  `tests/common/mod.rs`, because Cargo builds each `.rs` file directly in `tests/` as a binary of
  its own, and does not build a file in a subfolder alone.

[C-METHOD]: https://rust-lang.github.io/api-guidelines/predictability.html#c-method

## The Test

Pick a call. Find its definition from the call alone: the receiver's type, or the module in its
path. If you need a text search to find it, move the definition or change the call.
