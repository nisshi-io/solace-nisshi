---
paths:
  - "**/*.rs"
  - "**/*.proto"
  - "**/*.sql"
  - "**/*.sh"
  - "**/*.toml"
  - "**/*.yml"
  - "**/*.yaml"
  - "**/justfile"
  - "**/Dockerfile"
---

## Comments

The default is **no comment**. Naming, structure, and a clear function boundary are cheaper than a comment and cannot go stale. Reach for a comment only when the code cannot carry the information by itself.

## When To Write One

Write a comment when a reader who understands the language still would not understand the *decision*. In practice that is a short list:

| Situation | What the comment must say |
|---|---|
| Non-obvious constraint | The external fact that forces this code — a kernel behaviour, a hardware quirk, a protocol requirement, an upstream bug |
| Code that looks wrong but isn't | Why the redundant-looking check or the unusual ordering is load-bearing |
| Deliberate narrowness | Why the simple approach was rejected, so nobody "fixes" it back |
| Bug workaround | The defect being worked around, with a link or a version guard, so a reader can tell when it is safe to delete |
| Safety-critical guard | The consequence of removing it |
| Duplicated state | What the value must stay equal to, and which writes must keep it equal |
| A guarantee this code relies on | The guarantee, stated as the contract that the other side keeps, not as its internals (see "Remote Comments") |

The test: **could a competent reader reconstruct this from the code alone?** If yes, delete the comment. If no, the comment is doing real work.

The examples below are illustrative, not quoted from a real file, so they stay true when the code changes. The first is a doc summary for an item that is not a getter: it states the item's role and the option that was turned down. The second states a duplicated-state rule, and the third explains a safety-critical guard.

```rust
/// Keeps each partition within its topic's `retention.ms`, by deleting each batch whose
/// newest record is older than that.
///
/// A failed run leaves the expired batches in place until the next run. So this task
/// logs each error and runs again on the next tick, instead of stopping, because a
/// stopped task would never delete another batch.
async fn enforce_retention(storage: Storage) { /* ... */ }

/// Returns the partition's high watermark: the offset after its last stored record.
///
/// The partition stores this value, so that a fetch does not scan the log for it. Each
/// write that appends a batch must update it atomically with the batch, and no other
/// write may change it.
fn high_watermark(partition: &Partition) -> i64 { /* ... */ }

fn read_records(frame: &mut Bytes, count: usize) -> Result<Vec<Record>> {
    // The function rejects a count larger than the bytes left in the frame, before it
    // allocates. The client sets the count, and each record takes at least one byte, so
    // a larger count is never valid. Without the check, one request can make the broker
    // allocate gigabytes.
    if count > frame.remaining() {
        return Err(Error::RecordCount(count));
    }
    let mut records = Vec::with_capacity(count);
    // ...
}
```

## When Not To

- **Never restate the code.** This is the single most common failure — if the comment is the next line translated into English, delete it. A comment reading `// increment the counter` above `count += 1` conveys strictly less than the code while any genuinely useful fact (thread safety, units, an invariant) goes unmentioned. `// loop through and output` above a `for` loop is the control-flow variant: it narrates the loop line and ignores whatever is actually surprising in the body.
- **Never comment to rescue unclear code.** Rename the variable, extract the function, split the conditional. A comment explaining a bad name is two problems.
- **Never leave commented-out code.** Delete it; version control remembers.
- **Never narrate history.** No "changed this because…", no "previously we…", no ticket numbers as scar tissue. Comments describe the end state; write for a future reader who has no context about what changed. The *external* defect a workaround exists for is end state and belongs (see Markers below) — a live constraint, not a record of a past edit.
- **Never section-label straight-line code.** `// Step 1`, `// Options` are a sign the block wants to be a function.
- **Never describe another file.** See "Remote Comments" below.
- **Never rewrite a comment that already passes these rules.** In a comment pass, change a comment only when it breaks a rule. A different wording that also passes is churn, and a reviewer has to check it again. When a comment does need a rewrite, keep every true fact from the old comment, and keep the most important one first.

## Function-Header Scope vs. Inline Detail

A function's leading comment (its doc comment or header comment) describes the function's **contract** — what it does, why it exists, and any non-obvious constraint that governs the whole function. It never narrates the body.

AI-written code is especially prone to the opposite failure: a paragraph at the top that walks the reader through the implementation step by step — "first parses the header, then validates it, then dispatches to the handler" — duplicating in prose what the code already shows in order. That paragraph goes stale the moment a step is reordered, added, or removed, and it teaches the reader nothing three lines down wouldn't.

If a specific line or block has a genuinely non-obvious detail — the actual subject of "When To Write One" above — put that comment **directly above the line or block it explains**, not folded into the function's header. The header stays about the function as a whole; a local comment stays local.

```rust
/// Parses a frame header and dispatches it to the matching handler.
///
/// Returns `Err` if the frame's declared length exceeds `MAX_FRAME_SIZE` — callers
/// rely on this to bound how much they buffer before this call returns.
fn dispatch(frame: &[u8]) -> Result<Response, DispatchError> {
    let header = parse_header(frame)?;

    // The wire format allows a zero-length payload only for a heartbeat; every
    // other message type must carry at least one field.
    if header.payload_len == 0 && header.kind != FrameKind::Heartbeat {
        return Err(DispatchError::EmptyPayload);
    }

    route(header, &frame[HEADER_LEN..])
}
```

The doc comment above `dispatch` states its contract — what triggers `Err` — and says nothing about parsing, then validating, then dispatching; the code already shows that. The one implementation detail worth a comment (why a zero-length payload is sometimes valid) sits directly above the `if` it explains, not folded into the function's opening paragraph.

## Remote Comments

A *remote comment* describes the contents or behaviour of some other file that this code is not coupled to. It is the worst kind of stale comment, because nobody editing that other file thinks to look here. Illustrative example (not from a real file):

```rust
// The following variants are handled by this dispatcher:
//  (1) Request::Create
//  (2) Request::Update
//  (3) Request::Delete
match request {
    Request::Create(r) => handle_create(r),
    Request::Update(r) => handle_update(r),
    Request::Delete(r) => handle_delete(r),
}
```

A hand-maintained index of variants declared elsewhere, sitting above the `match` that already enumerates them in its own arms. It is redundant the day it is written and silently wrong the day a fourth variant is added. Delete it — the `match` is the authoritative list.

The distinction is **coupling**, not proximity. The test: **would a change on the other side reach this code?** If yes, write the comment — the coupling is real and you are documenting *this* code's constraint. If no, it is remote: the other side changes, nothing here notices, and the prose rots unobserved.

**Allowed** — a real dependency this code has on the other side:

- An ordering requirement, a value that must be kept in sync, or an obligation a caller must honour.
- Local code that looks unusual, redundant, or plainly wrong and exists only to meet a remote API's requirements — a value that must be set before some function is called because the callee reads it lazily; a resource kept alive because the callee stores a reference to it rather than copying it. Without a note these read as mistakes and get "cleaned up."
- Why this code is shaped as it is to satisfy another component's published contract — e.g. `// the peer expects the length prefix in network byte order, so we call to_be_bytes() before writing it`. Such a comment can drift, but it drifts *with the caller*: break that contract and this code stops working, so whoever changes it is already editing here.

**Not allowed** — restating another file's or function's *internals*, inventorying its contents or steps, or naming a file just to point at it.

The line between them is the **contract**, never the other side's internals. "Read before the connection is established" and "must be called before `init()` returns" are contracts; "sets the `ready` flag under a lock" is an internal, and it rots unobserved because nothing here depends on it. The same holds in reverse — do not name a function's call sites in its own comments or doc comment; describe behaviour and contract instead. Naming a caller is acceptable only when the function is tightly coupled to exactly one site.

Prefer expressing a real coupling in something that *breaks* when it drifts — a `const` assertion, a `debug_assert!`, a lint, an intra-doc link, a test — and drop the comment. A comment is the fallback for coupling the language cannot express.

## Doc Comments

A doc comment (a language's own doc-comment convention — Rust's `///`, a Python docstring, a Markdown page's lead paragraph) is orientation for the next reader, not reference documentation:

- One-line summary for anything whose name is not self-evident. Skip it entirely when the signature or heading already says everything.
- **Make the summary explain the name.** A reader reads the name first, then the summary, so the summary uses the name's own words and says what each word means here. Define a word in the name that the reader can't know. If the summary needs other words to say what the item does, rename the item instead.
  - No: `/// Returns a pool that closes a connection after it has been unused for a while.` above `fn with_idle_timeout(&self, timeout: Duration)`. The summary never says "idle timeout", and it doesn't say how long "a while" is.
  - Yes: `/// Returns a pool with the given idle timeout: how long the pool keeps an unused connection open before closing it.`
- Longer form only when there is a real invariant, unit, or failure mode to state — see "Function-Header Scope vs. Inline Detail" above for where the line falls.
- **No boilerplate that restates the signature with no new information.** "Returns a `Result`" adds nothing the signature doesn't already show. Say which condition gives which error, or when the function panics, only when the signature can't show it, and then in a plain sentence after the summary: "Returns [`Error::Api`] with [`ErrorCode::UnknownTopicOrPartition`] for an unknown topic." Don't use a `# Errors` or `# Panics` header. A header and its blank lines take three lines before the one sentence that matters.

## Tests

A reader who did not write a test has to work out two things before they can review it: the
situation that its setup creates, and the outcome that it expects. When the code does not show
either one clearly, the reader has to rebuild the test in their head first. A test therefore gets
a comment on each line that the reader can't follow without help, and on no other line.

- **Make the setup and the expectation clear to a reader who did not write the test.** Write a
  comment above each line that the reader can't follow without help. Such a line is usually one
  of these:
  - a setup step whose purpose the code does not show, such as a port that nothing listens on or
    a table that the test drops to make a query fail
  - a value chosen for its effect, such as stored data that is valid JSON but not a valid record
  - an expected value that is not the obvious one, such as a saved value that the test compares
    with because building the value again gives a different result
  - an assertion on the test's own setup, which stops the test from failing, or passing, for a
    reason other than the code under test
- **Say what the line does for the test, and why the test needs it.** The test's name, and its
  doc comment if it has one, state the behaviour that the test checks. A comment in the body says
  only how its line serves that check, so the body and the doc comment never say the same thing.
- **Leave every other line uncommented.** A line that builds the input that the test's name
  describes, or whose purpose its code shows, needs no comment. A comment there gives the reader
  one more thing to read and tells them nothing new.
- **Comment a helper on its own terms.** A helper that tests call is a function like any other.
  Its comments explain what the helper does and why, and they do not say what a calling test does
  with the result. A comment about a caller becomes wrong when that caller changes, and the next
  test may use the helper differently.

## Rust

These rules add to the rules above for Rust code. They come from the [rustdoc book](https://doc.rust-lang.org/rustdoc/), [RFC 1574](https://rust-lang.github.io/rfcs/1574-more-api-documentation-conventions.html), the [Rust Style Guide](https://doc.rust-lang.org/style-guide/), and the [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/documentation.html).

- **Use `///` for an item's header, and `//!` for a crate or a module.** rustdoc and rust-analyzer show a `///` comment, also on a private item, and they do not show a `//` comment. Use `//` inside a body, and for a note about an attribute. Put a doc comment before the item's attributes.
- **Write the summary as the first sentence, and make it say what a caller can rely on.** rustdoc shows the first paragraph on module pages and in search results. The summary still has to pass the reconstruct-from-code test in "When To Write One". A function's summary starts with a verb in the third person. "Returns" or "Opens" fits a getter or a constructor. For a background task, a type, or a constant, state the item's role, because a summary of the body restates the code: "Keeps each partition within its retention limit" states a role, and "Deletes each expired batch" restates a loop.
- **Give a crate or a module a `//!` summary when its name does not state its purpose.** State the purpose. Do not list the items, because rustdoc lists them.
- **Link a named item with an intra-doc link,** such as ``[`Vec::push`]``. `just doc` fails CI when someone renames or removes the target of a link in a library or a binary. A public item must not link to a private item (`private_intra_doc_links`), so name a private item in plain backticks there. rustdoc resolves links only in doc comments.
- **Write an example as a doctest.** `just test-doc` compiles and runs it, as a separate crate, so it can call only public items. Add an example only when the signature does not show how to call the item. Use `no_run`, not `ignore`, so that the example still compiles, and use `?`, not `unwrap()` ([C-EXAMPLE](https://rust-lang.github.io/api-guidelines/documentation.html#c-example), [C-QUESTION-MARK](https://rust-lang.github.io/api-guidelines/documentation.html#c-question-mark)).
- **Do not comment a rule that a lint enforces.** CI runs clippy with `-D warnings`, so `clippy::await_holding_lock` already fails a build that holds a `std::sync` lock across an `.await`. A comment that says so adds nothing.
- **Write a clap doc comment for the operator, on one line.** clap shows a field's doc comment as its `--help` text.
- **Put an identifier in backticks.** rustdoc renders Markdown, so it shows the identifier as code, and an underscore in the name does not become emphasis.

## Form

- Use [ASD-STE100 Simplified Technical English](https://www.asd-ste100.org/): one idea per sentence, active voice, present tense, the same word for the same thing, no idioms.
- Complete sentences, capitalised, in the language's native comment syntax. A fragment like `// hack` never survives contact with a future reader.
- Explain **why**, not **what**. The code already says what.
- Put it where the confusion is — at the call site if that is where the surprise lands.
- Wrap to the file's prevailing width. Keep it shorter than the code it explains; a paragraph over a two-line function is a design smell.
- **Put each URL on a line of its own, at the end of the comment.** Write a reference-style link in the text, such as ``[`ConfigAdminManager.preprocess`]``, and define it on the comment's last lines, such as ``[`ConfigAdminManager.preprocess`]: https://github.com/apache/kafka/blob/3.9.1/...``. A URL does not break across lines, so a reference-style link keeps the prose within the width, and only the definition lines run past it. rustdoc renders the link in a `///` or `//!` comment. Write a `//` comment the same way, so that it reads the same as a doc comment.
- **One point per comment.** A comment above a condition or a block states the rule that the code enforces first, and the reason after it. When a block has several reasons, write one comment for each, above the line that it explains. One paragraph that covers all of them makes the reader work out which reason goes with which line.

## Say It Directly

Form is not enough on its own. The failure below survives every rule above, and it is the one a review catches again and again: prose that circles a fact instead of stating it.

**Name the actor, and say when the actor is us.** An agentless sentence hides who does the thing, and the reader then cannot tell whether the behaviour is ours to change. The rule applies when the actor is unclear. When the actor is the code right below the comment, a passive sentence is clear, and it does not need a "we".

- No: `// a preloaded library loads before the sanitizer runtime`
- Yes: `// we preload the syslog shim, so it loads before the sanitizer runtime`

**Make the mechanism the subject.** A sentence that opens with "That report", "This behaviour", or "It" sends the reader back to the previous sentence to find out what acts, and a relative clause that defines a name in passing ("quiet, which marks a failure the caller expects") hides the definition where nobody looks for one. Name the mechanism, say what it does, and attach the reason with "because." Write subject, verb, object. A sentence that opens with the object, or with a long clause before the verb, makes the reader hold the whole sentence in mind before they know what acts.

- No: `// That flag ignores quiet, which marks a failure the caller expects; nobody expects the command to be absent.`
- Yes: `// quiet does not ignore a missing command, because that is a critical failure.`

**State the fact. Do not invent a condition that the code has already decided.** When the code sets a value, say what the value does. A conditional word implies a choice that the reader does not have.

- No: `// The retry loop stops unless this flag allows another attempt.` The flag is set, and it does allow another attempt, so "unless" describes nothing.
- Yes: `// The retry loop stops after N attempts, and this flag raises that limit.`

**Name the thing.** A category noun makes the reader search for what is meant.

- No: "a read yields a fixed-size value, so keep the buffer that size"
- Yes: "a single `recv` call on this socket yields exactly one frame"

**Use the name that the code uses.** A term that only the prose uses spreads to other comments, and it leaves open what it covers. If prose needs a new term, define it once, where the thing is defined.

- No: `// the coordinator evicts each stale member`. The reader cannot tell whether a member that left the group is stale, or whether an eviction starts a rebalance.
- Yes: `// the coordinator removes each member whose session timeout has expired`

**Name the option that was turned down, and make the reason lead to the choice.** A comment that explains a choice states the alternative, because the alternative is half of the decision. Check that the reason supports the choice it sits next to. "A failed run only delays deletion" shows that one failure is harmless. It does not show why the task keeps running. This rule applies only to a choice that needs a comment by the test in "When To Write One". A choice that a reader would not question needs no comment, and so it needs no alternative.

- No: `// a failed run only delays deletion, so this task keeps running after each error`
- Yes: `// this task keeps running after every error instead of stopping, because a stopped task would never delete another batch`

**Do not list examples to make a point.** A list of examples names things that can change without notice. When one of them changes, the comment is wrong, and the list taught the reader nothing, because the code depends on none of the named items. State the point with a category noun.

- No: `// this assumes a POSIX shell with bash, zsh, or dash semantics.`
- Yes: `// this assumes POSIX shell semantics.`

**Negate the verb, never the noun.** "No X" and "without X" hide the negation inside a noun phrase, and the reader has to rebuild the sentence to find out what does not happen.

- No: `// The response contains no field that identifies the caller.`
- Yes: `// The response does not identify the caller.`

**Say what the code does, not what it does not do.** A negative sentence leaves the reader to work out the positive case, which is the one the code implements.

- No: `// This check does not run for a request that only touches cached fields.`
- Yes: `// This check runs only for a request that touches uncached fields.`

**Use plain words.** A metaphor makes the reader translate before they can check the claim. If the codebase has already adopted a metaphor as a domain term (a "queue," a "pool"), reuse it consistently; don't introduce a new one for effect.

- No: `// The retry landed cleanly, giving the request a two-step shape.`
- Yes: `// The retry succeeded. The request took two attempts.`

**Give agency only to a thing that acts.** A process, a check, or a function does something. A file, a flag, or a value is acted on, and a sentence that has it "want," "decide," or "know" hides the real actor. An abstract noun is not an actor either: a decision, a requirement, a result, or a failure does not need, expect, or know anything. A need belongs to the process or the person that has it, so write that actor as the subject.

- No: `// The flag knows the queue is unavailable.`
- Yes: `// The dispatcher reads the flag and skips the queue.`

**State a requirement, not an observation, and leave a breadcrumb.** A sentence about how the world is today rots the moment the world changes, and it rots silently, because nothing fails when it stops being true. A sentence about what this code requires stays true whatever the world does.

- Observation (rots silently): `// our process starts before the sidecar does.`
- Requirement (stays true): `// this code does not require the sidecar to be ready before startup.`

A requirement alone, though, strands the next reader: it says what was decided and gives them no way to reach why. Name the tracked issue if one exists — it holds the evidence that would otherwise rot in the comment (which conditions, why the cost was acceptable), and a reader who needs that history knows exactly where to find it. The comment states what is permanently true; the breadcrumb leads to what was true at the time.

The breadcrumb names an issue, never a pull request or a commit. A pull request or a commit records a change that is already done, and that is history (see "Never narrate history").

**Open with the plainest statement of what the code is.** The first sentence is a label, not a summary of everything the code does.

- No: `// The set of options that every process this workspace builds starts with.`
- Yes: `// The default startup options for this workspace's processes.`

The test: read the sentence aloud. Rewrite it as subject, verb, object when it takes a clause to reach its subject, when it gives agency to a thing that does not act, or when it uses "unless," "would," or "can" for something the code already settled.

## Markers

Unfinished work belongs in the project's issue tracker (GitHub issues), not in the source. Do not add a bare `// TODO` — it names no owner, no remedy, and no ticket, and is indistinguishable from work nobody is tracking.

If a marker is genuinely warranted, it must name the problem, the remedy, and a tracked-issue reference:

```rust
// TODO(#1234): drop the fallback once every client is upgraded; needs the
// downgrade path removed from the compatibility matrix first.
```
