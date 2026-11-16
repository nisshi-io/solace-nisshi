---
paths:
  - "**/*.rs"
  - "**/*.proto"
  - "**/*.sql"
---

## File Header

Every Nisshi source file says that Solace holds its copyright, and names the people outside
Solace whose code is in it. CONTRIBUTING.md's "File headers" section states the rule for human
contributors. This file adds what an agent needs.

- **Start each new `.rs`, `.proto` and `.sql` file with the header.** Copy the copyright line
  and the Apache-2.0 notice from a file of the same type, in that file's comment syntax:

      // Copyright (c) Solace Corporation

  and in SQL:

      -- Copyright (c) Solace Corporation

- **Don't copy a `Contributors outside Solace:` list into a new file.** The list names the
  people whose lines are in that file. When code moves from another file, bring along the names
  of the people who wrote the lines that move, and check them with `git blame`.
- **Add a name only for the author of the change, and only if they don't work at Solace.** The
  author is the person the commit is credited to, not the agent. If you can't tell whether the
  author works at Solace, ask. Never add a Solace employee. Add the name in alphabetical order.
- **Never remove a name,** also when you rewrite or delete that person's lines. Removing a name
  by mistake drops credit someone is owed, and keeping one costs nothing.
- **Write `(c)`, not `©` or `ⓒ`.** The broker sends `pg/schema.sql` to PostgreSQL with its
  comments, and PostgreSQL converts a whole query to the database's encoding, so a non-ASCII
  character fails on a database that is not UTF8.
- **Leave out the year.** A year goes stale, and the test compares the line exactly.
- **Put nothing above the header,** except an Emacs mode line (`-*- ... -*-`), because Emacs
  reads its mode line only from the first line.

`nisshi/tests/copyright_header.rs` fails for each tracked source file whose header is missing or
wrong. It checks only the copyright line. Whether a contributor list is right needs `git blame`,
so a reviewer checks it.
