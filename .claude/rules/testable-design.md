## Testable Design

A test checks the code that production runs, through the interface that production code calls.
A function, parameter or constructor that only tests use lets the test check that hook instead,
and leaves the path that production takes untested beside it. Design the code so that a test
needs no hook.

The examples below are illustrative, not taken from a real file, so they stay true when the code
changes.

## Rules

- **Don't add a second path for a test.** A function that copies a production path with a
  seam for the test, such as `send_with_clock(clock, ...)` beside `send(...)`, makes the test
  check the copy instead of the real path, and the copy can drift from it. A function that only
  tests call is fine when it stands on its own: a behaviour that a production caller could use
  as it is, such as a getter for state that the item already holds.
- **Make a dependency a parameter only when production varies it.** A storage handle, a
  connection or a configuration value that production callers pass in differently is a
  parameter, and a test passes its own. Don't turn a fixed dependency into a parameter just so
  that a test can replace it.
- **Read a global through its real output.** A metric goes to the process-wide meter provider.
  A test installs an in-memory exporter as that provider, before the code creates its first
  instrument, and reads what the code exported. nextest runs each test in a process of its own,
  so the provider belongs to that test.
- **Control time with Tokio's paused clock,** not with a clock parameter.
  `#[tokio::test(start_paused = true)]` and `tokio::time::advance` move the time that
  `Instant::now` and timers read.
- **Keep test helpers in test code.** A helper that builds a fixture or reads a result lives in
  the `tests` module or under `tests/`, never in the item's `impl`.
- **When a test needs a hook, change the design.** A hook usually points at a value that the
  code hides, such as an outcome that it only logs. Return the value, or record it where a test
  can read it, as the code would for any other caller.

## The Test

For each function, parameter and constructor, name a production caller that needs it, or say
why a production caller could use it as it is. If it exists only to give a test a different path
from the one production takes, remove it and test through the code that production calls.
