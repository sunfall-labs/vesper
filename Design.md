# Design

What Vesper is, what it borrows, and what it deliberately does not build.
[`README.md`](README.md) describes what exists today and
[`CONTEXT.md`](CONTEXT.md) is the vocabulary; this file is the reasoning
behind the shape.

Vesper is an agent loop over `effect/unstable/ai`, which supplies
`LanguageModel`, `Tool`, `Toolkit`, `Prompt`, `Response`, and `Chat`, and
stops one round trip short of an agent. Two decisions carry the rest of the
design:

- **A run's unmet service requirements are a compile error.**
  `Agent.Instance<Name, Tools, Requires>` puts a subagent's service keys into
  its parent's requirement channel, so "you forgot the database" is caught by
  the compiler rather than by the model deciding to delegate.
- **A conversation is an event-sourced log, and that log is the only
  persistence mechanism.** A run is recorded to it, resumed from it, and
  steered through it. There is deliberately not a second one.

A third decision is negative and does as much work as either: wherever
`@earendil-works/pi-agent-core` already has an answer, take it rather than
write one. The sections below are the argument for each, and the last two are
what is missing.

## Leaning on pi-agent-core

Most of what an agent harness does has been done already in
`@earendil-works/pi-agent-core`, and the evidence that it is easy not to
notice is precise: the `Compacted` record here carries `summary` and
`firstKept`, and that is character-for-character
`Session.appendCompaction(summary, firstKeptEntryId, …)` in pi-agent-core.
Convergence that exact on a published signature is the signal that the work
being done is reimplementation of an importable package.

So the rule is to lean on Pi as hard as possible, and to differentiate only
where Pi structurally cannot follow.

What makes that workable is that Pi's _algorithms_ are separable from Pi's
_runtime_. `shouldCompact`, `estimateContextTokens`, and `loadSkills` take
messages, numbers, or an injectable environment; none of them touches
`AgentTool` or typebox. typebox is confined to `agentLoop` and the harness
tools. So the question for each piece of Pi is not "does it fit this runtime"
but "is it an algorithm, or is it welded to the loop".

## What is taken from Pi

**Providers**, through `packages/pi`. `@sunfall/vesper-pi` adapts
`@earendil-works/pi-ai` to Effect's `LanguageModel`, so every provider Pi
maintains is one Vesper can use, and the error classification Pi maintains
against real providers comes with it. `PiErrors.isContextOverflow` is the case
that proves the point: the hand-rolled regex it replaced recognised about
seven of the sixteen phrasings Pi already handled, and not Anthropic's own.

**Context-window heuristics**, through `packages/pi/src/compaction.ts`.
`estimateContextTokens` is the real win. Pi's raw `estimateTokens` is the
_identical_ four-characters-per-token guess `@sunfall/vesper-agent` already
had, so the estimate alone changes nothing; the anchoring is the improvement.
It walks back to the most recent assistant message carrying provider-reported
usage, takes that as exact, and estimates only what came after. On a long
conversation that is the difference between guessing about eighty thousand
tokens and guessing about two hundred.

**Version lockstep.** `pi-agent-core@X` needs `pi-ai@^X`. The repository pins
`pi-ai` at 0.80.2, so `pi-agent-core` is 0.80.2 too. A mismatch means two
copies of `pi-ai` in the graph and broken type identity at the seam — the
`Message` a `pi-agent-core` function accepts stops being the `Message` the
adapter produces. Do not bump either in isolation.

## What is deliberately not taken

**`generateSummary` and `generateSummaryWithUsage`.** Pi's _prompt_ is worth
having; Pi's _execution_ would cost Vesper's spans, retry policy, and typed
errors, because those calls run through Pi's own provider path.
`@sunfall/vesper-agent` runs the summarization call through Effect's
`LanguageModel` instead.

**`findCutPoint`, `prepareCompaction`, `compact`, and `Session`.** All written
against `SessionTreeEntry[]` — Pi's session tree, which Vesper does not have.
`@sunfall/vesper-log` answers the question that tree answers, differently.

**`agentLoop`, `Agent`, and the harness tools.** Welded to typebox. Taking
them would mean taking a second schema library alongside `effect/schema` and
losing the typed requirement channel that is the whole reason this exists.

**`SUMMARIZATION_SYSTEM_PROMPT`** — wanted, and unavailable.
`pi-agent-core@0.80.2` defines it in `harness/compaction/compaction.ts`, but it
is absent from `dist/index.d.ts` and the package's `exports` map lists only
`.`, `./node`, and `./package.json`, so no import reaches it.
`Compaction.defaultSystem` holds a transcribed copy, and nothing can notice Pi
changing it. Re-check on a bump; delete the copy the moment Pi exports the
real one.

## What is ours, because Pi has no answer

**Typed requirements.** `Agent.Instance<Name, Tools, Requires>` puts a
subagent's service keys into its parent's requirement channel, so "you forgot
the database" is a compile error rather than a runtime failure the first time
the model delegates.

**The log.** Postgres with producer fencing, per-record offsets, and
`LISTEN`/`NOTIFY`. Pi ships jsonl and in-memory session repositories and
nothing else, which is not enough for a multi-process deployment where one
process must wake on another process's append. Fencing is the other half: a
conversation has one writer, and a second concurrent run must fail its next
append rather than interleave two runs into one history.

Everything else in `@sunfall/vesper-agent` — the loop, subagents, skills,
interception, resumption — exists to serve those two, and should be
re-examined against Pi whenever Pi grows an answer.

### Why a `Promise`-returning `execute` cannot express the first one

The common design hands a tool an `execute` that returns a `Promise`. A
`Promise<A>` states what the tool produces and nothing about what it needs or
how it fails, so the services a tool reaches for are either captured in a
closure or pulled from a container at call time. Both are invisible to the
compiler, and the invisibility compounds through delegation: a parent whose
child's tool reads a database type-checks clean, and the missing database
surfaces the first time the model chooses to delegate — which is to say at
whatever point in some conversation the model happens to reach it, for one
user, in production.

`Effect<A, E, R>` carries what the `Promise` carries plus the failure type and
the requirement set, so composing a child into a parent composes their
requirements, and the unmet one is a type error at the call site that composed
them. `Definition.subagents` captures a tuple of `Agent.Named` rather than a
`ReadonlyArray<Agent.Any>` precisely so that composition survives; erased, the
whole property goes with it.

The cost is real and worth stating plainly. Every tool body is an `Effect`, so
a plain async function is not a tool here without being lifted, and the
requirement channel is a type parameter callers see in their own signatures.
That is the trade this library takes, and it is the only reason to prefer it
over a harness whose `execute` returns a `Promise`.

## What Durable Streams actually is

Researched rather than inferred. It is expensive to rediscover, which is why
it is written down here rather than in a commit message.

**Durable Streams is a public HTTP protocol** from ElectricSQL (announced
December 2025, `github.com/durable-streams/durable-streams`), generalising the
offset-based sync they ran in production for Postgres. There is a Rust
reference server with a conformance suite and a Go implementation. It is a
wire contract, not a library, and it **defines no storage interface** — any
pluggable-backend design is ours.

Offsets are opaque, lexicographically sortable, strictly increasing. The
reference server encodes them as two 16-digit zero-padded integers joined by
`_`, so byte comparison is correct ordering. Reads are exclusive-after, with
three modes required to be observationally identical: catch-up, long-poll, and
SSE. Appends may carry a Kafka-style idempotent-producer triple.

Vesper adopts a subset on purpose: the offset format, and one function.
Nothing here speaks the wire protocol — no stream creation or deletion, no
producer headers on the wire, no stream forks, no retention semantics, no
closure — and that is deferred until something outside the process reads the
log. DS's richer features (stream forks, sub-offsets, retention, CDN
collapsing, ETag caching) exist because it is a public CDN-fronted sync
protocol; the readers here are in-cluster.

### Two decisions taken from the research

**Offsets are per record, not per batch.** The obvious implementation assigns
one offset per append batch, which makes resuming mid-batch impossible — DS
has sub-offsets for exactly this, and a batch-granular offset throws them
away. This stream emits many small deltas, so inheriting that would lose up to
a turn on resume. The cost is a row per record; the mitigation is that the
agent loop **coalesces text deltas before appending**. The log exists for
durability and resumption, not byte-exact replay of the provider wire.

This has one concrete consequence in the Postgres backend. A batch-granular
store's uniqueness index is
`(path, producer_id, producer_epoch, producer_sequence)` — four columns, which
suffice because one batch is one row. Ours needs a fifth, `batch_index`,
because one batch is many rows sharing a producer sequence and a four-column
index would reject its own second row.

**The pluggable seam is a SQL dialect, not a store.** The producer-fencing
protocol is implemented once, and each backend supplies about eight constants,
so a Postgres adapter is a handful of lines rather than a second
implementation of fencing. The fencing model it implements: epoch plus
sequence, a uniqueness index as the idempotency mechanism, exact retry returns
the original offset, conflicting retry fails, sequence gap fails.

Two refinements on top of that. An append retry is matched on a **digest of
the encoded batch**, not on a proxy like record count: reusing a sequence for
different content would otherwise converge on the earlier offset and drop the
new records with nothing anywhere to say so. And `changes` must **emit an
opening wake-up once its subscription is live** and **fail rather than go
quiet**, because a reader has no other way to learn that subscribing took
effect, and a dead feed that looks healthy is indistinguishable from a
conversation where nothing is happening.

### What not to build behind that seam

Each of these is a shape a log interface drifts into, and each was rejected
for a specific reason:

- A `nextOffset` field that actually means _last written_, and works only
  because reads are exclusive-after. Ours is `cursor`.
- A `PersistenceAdapter` lifecycle (`connect`/`migrate`/`close`). That is
  `Layer` reinvented for a framework without one, and this has `Layer`.
- Optional interface methods with presence checks; a second service tag makes
  "this backend has no checkpoints" a layer choice the compiler sees.
- Authorisation queries inside the append transaction, which weld the log to
  whatever ledger authorises the write and make a standalone backend
  impossible.
- A `{"$chunkCount":N}` sentinel disambiguated from real data by whether the
  string starts with `{`. Large payloads get a typed column or an attachment
  reference.
- Process-local, best-effort `subscribe`, which survives only on a long-poll
  re-read every thirty seconds and never wakes a second process on the first
  one's append. The Postgres backend uses `LISTEN`/`NOTIFY`.

## Why there is one persistence mechanism

There were three, and the argument for collapsing them is worth keeping,
because it is the argument any future proposal to add a fourth has to beat.

Provider-seam checkpointing content-addressed each model call. Within a run it
was never a cache hit — each turn appends to history, so two calls in one run
cannot be byte-identical — and `forRun` gave unrelated runs disjoint
namespaces, so the **only** time a checkpoint was ever read was a replay of the
same run after a crash. That is exactly the scenario resumption exists for, and
resumption reaches it without the replay: it rebuilds the prompt from records
and starts the next turn.

Worse, checkpointing did not cover the case it was defended with.
`Durable.wrap` wrapped `generateText` and `streamText`; tool execution happens
inside `LanguageModel`'s resolution of the turn, past every checkpoint. A run
that died after tool A and before tool B replayed the model call for free and
**re-ran tool A**. The log has `ToolOutcome`, one record per settled call, and
`packages/agent/src/dispatch.ts` serves them — so the named case is covered by
the mechanism that was supposed to be insufficient for it.

Whole-conversation snapshots were the third, and were a second way to answer
the question the log answers. Neither mechanism saves an in-flight turn: a turn
interrupted halfway is re-paid under both, and the crash-mid-turn story is
about not repeating tool **side effects**, not about provider spend.

What survived is the retry, in `@sunfall/vesper-pi/retry`: jittered exponential
backoff honouring `retryAfter`, gated on `isRetryable`, absorbed inside the
attempt so a 429 never becomes recorded state. Two streaming rules ride with
it — a retry stops once a delta has reached the consumer, and a stream with no
content and no finish fails retryably rather than settling as a permanent empty
success.

Durable-workflow step idempotency — DBOS, for instance — is a separate seam,
not a competitor. Steps make **effects** exactly-once; the log makes
**conversation state** durable and replayable; the retry absorbs **provider
blips**. Producer fencing overlaps none of them — it decides which writer owns
a stream, not whether work re-runs.

## Not built, and why

**Branch summarization.** Branching is built: `agent.branchFrom(id, at, input)`
re-runs a conversation from an earlier record, and it cost one record case
rather than a parent pointer per record, because offsets are a total order and
Pi's `SessionTreeEntry.parentId` is compensating for a JSONL file that has
none. Concurrency is built too, as the deliberately different thing it is:
`agent.forkFrom(id, at, forkId, input)` seeds a new conversation from the same
prefix, so two forks are two streams and two producer claims and can run at
once, where two branches share one writer and cannot. The cost of the second
stream is what the marker avoided — the prefix is copied, and every
offset-valued pointer in a copied record has to be reseated onto the fork's own
offsets or reset when it names a stream the fork does not share. There is no
fork graph: the ancestor records nothing about having been forked, which keeps
a fork from disturbing a run that is live on it.

What is _not_ built is summarization. Switching away from a branch records
nothing about what it held, where Pi has `generateBranchSummary`; its
common-ancestor walk in `collectEntriesForBranchSummary` is the part worth
porting if that is ever wanted.

**A harness toolkit** (shell, read, edit) and **prompt templates**. Pi has
both, and `loadSkills` and `loadPromptTemplates` both take an injectable
`ExecutionEnv`, so both are separable. `@sunfall/vesper-workspace` is the seam
such tools would sit behind. Nothing sits behind it yet.

**Routing.** Making agents addressable — a registry, a router, identity
resolution — is the usual design once agents are served over a network. Here
they are values you import and call. Until something serves agents remotely,
routing adds indirection without buying anything. Do not build it
speculatively.

**A Cloudflare Durable Objects runtime.** It is a plausible home for an agent
loop, and it is also a second durability story with its own semantics to keep
aligned with the log's. Postgres already answers the question it would answer,
and the section above is the argument it has to beat.

**A fold checkpoint over the log.** Opening a session reads the whole
conversation, so resumption and the recovery index are O(records) per run. That
is the first thing that will hurt on a long conversation, and the answer is a
checkpoint validated on format version and offset, discardable, rebuilt by
replay. A cache built before the cost it avoids is measured is a second source
of truth with extra steps; the `growth` scenario in `benchmarks/` is where that
cost would first become visible.

## Where this is honestly unproven

This is pre-1.0. It was extracted from the system it was built for, and that
system did not come with it, so the open questions below are real rather than
rhetorical.

- **Nothing outside this repository consumes it.** The dependents are
  `examples/` and `benchmarks/`, which exercise the library rather than rely on
  it. Until something real is using it, the value of owning the loop is a
  hypothesis, and every remaining item here is easier to prioritise once that
  changes — the last three things that genuinely improved this library came
  from running it rather than planning it.
- **Only the examples talk to a real provider.** `examples/live-smoke` and
  `examples/compliance-relay` are the whole of the live-provider coverage;
  every test runs against Pi's faux one. That distinction has already cost
  something once: the context-overflow classifier was a regex written without
  a provider to check it against, and the first live run is what exposed it.
- **Encode `RunStarted.prompt` through `Prompt`'s own codec.** It is stored as
  decoded messages in a `Schema.Unknown` field, so a `FilePart` holding raw
  bytes does not survive a round trip.
- **Wire attachments to the log and to prompts.** `AttachmentRef` exists so
  that is a lookup rather than a redesign, and nothing writes one into a record
  or resolves one into a provider part.
- **Add a filesystem or object-store attachment backend.**
  `AttachmentStoreError` is declared for one and is unreachable from the memory
  backend.
- **Get document input into Pi.** Pi 0.80.2's content union has no document
  member, so this cannot be closed here — it is a change in Pi.
