# Vesper

An Effect-first agent harness built on `effect/unstable/ai`.

`effect/unstable/ai` supplies `LanguageModel`, `Tool`, `Toolkit`, `Prompt`,
`Response`, and `Chat`. What it does not supply is an agent loop:
`generateText` is one round trip. Vesper adds that loop and two things that are
hard to get any other way: **agent
definitions whose unmet service requirements are a compile error**, and an
**event-sourced conversation log** a run is recorded to, resumed from, and
steered through.

Provider integration comes directly from official Effect AI packages. Vesper
does not wrap provider SDKs or maintain a second provider registry.
[`Design.md`](Design.md) explains the boundary.

**Status: pre-1.0, preparing its first `alpha` publish.** It was extracted from
the system it was built for, and the [known gaps](#known-gaps) below are not a
formality — read them before picking this up.

## Packages

Vesper publishes four packages:

| Package                                               | What it does                                                     |
| ----------------------------------------------------- | ---------------------------------------------------------------- |
| [`@sunfall/vesper-agent`](packages/agent)             | The loop, subagents, skills, interception, recording, resumption |
| [`@sunfall/vesper-log`](packages/log)                 | Event-sourced conversations, offsets, tailing, memory + Postgres |
| [`@sunfall/vesper-workspace`](packages/workspace)     | Files plus a shell behind one swappable driver                   |
| [`@sunfall/vesper-attachments`](packages/attachments) | Content-addressed blobs, verified on read                        |

`workspace` and `attachments` are standalone: nothing in `agent` composes
them implicitly. Applications opt into either package directly.
`WorkspaceAgent.standard` exposes the standard workspace toolkit, and
`WorkspaceAgent.compose(applicationToolkit)` adds those tools to an application
toolkit without hiding the required layers. `WorkspaceTools` remains the
advanced lower-level interface for custom handler, root, and command-policy
wiring; see the [workspace guide](docs/workspace.md).

`@sunfall/vesper-agent/workflow` optionally binds a recorded agent to Effect's
native `Workflow`: Effect Workflow or Cluster owns durable execution and
wakeup, while Vesper's log owns durable conversation semantics. The binding
returns the native workflow plus its registration layer, preserving the
agent's complete `Requires` channel rather than hiding runtime wiring.
`AgentWorkflow.step` is the corresponding tool-level primitive: a named Effect
Workflow `Activity` whose completed result replays without rerunning its effect,
with a mandatory input-derived key separating repeated logical calls. Its
requirement channel prevents it being mistaken for durable work outside a
workflow.

## Install

```bash
npm install @sunfall/vesper-agent \
            @effect/ai-anthropic@4.0.0-rc.109 \
            @effect/platform-node@4.0.0-rc.109 effect@4.0.0-rc.109
```

Effect and its provider packages are pinned together at `4.0.0-rc.109` while
the APIs are release candidates. Every Vesper package peers on that exact
`effect` version so an application has one Effect service identity.

## A worked example

The code below is `examples/support-agent/src/main.ts`, a compiled example with
requirement-channel assertions, so it cannot drift from the API it documents.

### Wiring

```ts
import { AnthropicClient, AnthropicLanguageModel } from '@effect/ai-anthropic';
import * as NodeHttpClient from '@effect/platform-node/NodeHttpClient';
import { ContextWindow } from '@sunfall/vesper-agent/context-window';
import { Config, Layer } from 'effect';

const Anthropic = AnthropicLanguageModel.model('claude-sonnet-4-6').pipe(
  Layer.provide(
    AnthropicClient.layerConfig({
      apiKey: Config.redacted('ANTHROPIC_API_KEY'),
    }),
  ),
  Layer.provide(NodeHttpClient.layerUndici),
);

const ContextPolicy = Layer.succeed(
  ContextWindow.Service,
  ContextWindow.usageAnchored,
);

export const AiLive = Layer.merge(Anthropic, ContextPolicy);
```

The official provider owns prompt conversion, tools, streaming, usage,
credentials, telemetry, and errors. Its `Model` provides `LanguageModel`,
`ProviderName`, and `ModelName`. `ContextWindow.usageAnchored` is separate,
provider-independent policy: it anchors completed history on reported usage
and estimates only the messages after it.

### Defining the agent

```ts
import { Agent } from '@sunfall/vesper-agent/agent';
import { Skill } from '@sunfall/vesper-agent/skill';
import { Stop } from '@sunfall/vesper-agent/stop';
import { Context, Effect, Schema } from 'effect';
import { Tool, Toolkit } from 'effect/unstable/ai';

/** An ordinary application service. Nothing here is AI-specific. */
export class OrderRepo extends Context.Service<
  OrderRepo,
  {
    readonly status: (id: string) => Effect.Effect<string>;
    readonly refund: (id: string) => Effect.Effect<string>;
  }
>()('example/OrderRepo') {}

const lookupOrder = Tool.make('lookup_order', {
  description: 'Look up the fulfilment status of one order.',
  parameters: Schema.Struct({ orderId: Schema.String }),
  success: Schema.Struct({ status: Schema.String }),
  // Declaring the service is what puts `OrderRepo` into the agent's
  // requirement channel — the run will not compile without it.
  dependencies: [OrderRepo],
});

const issueRefund = Tool.make('issue_refund', {
  description: 'Refund one order. Irreversible; confirm the order first.',
  parameters: Schema.Struct({ orderId: Schema.String }),
  success: Schema.Struct({ confirmation: Schema.String }),
  dependencies: [OrderRepo],
});

const refundPolicy: Skill.Skill = {
  name: 'refund_policy',
  description: 'When a refund is allowed and how to issue one.',
  instructions: [
    'Refunds are allowed within 30 days of delivery.',
    'Damaged goods are refundable at any time.',
    'Always confirm the order status before promising a refund.',
  ].join('\n'),
};

/** A specialist the support agent can hand bounded work to. */
export const researcher = Agent.make({
  name: 'researcher',
  revision: '1',
  description: 'Digs through documentation to answer a specific question.',
  instructions: 'Answer the question directly. Cite nothing you did not read.',
  toolkit: Toolkit.make(),
});

export const supportAgent = Agent.make({
  name: 'support',
  revision: '1',
  instructions: [
    'You handle customer support for an online store.',
    'Check order status before making promises.',
    'Delegate open-ended research rather than guessing.',
  ].join('\n'),

  toolkit: Toolkit.make(lookupOrder, issueRefund),

  // Compiled into the toolkit; the child's requirements ride along.
  subagents: [researcher],

  // The catalog goes in the system prompt, the bodies load on demand — so
  // the cacheable prefix stays byte-identical across turns.
  skills: [refundPolicy],

  // Stop when the model stops calling tools, or at 12 steps, or as soon as
  // a refund has been issued.
  stopWhen: Stop.any(
    Stop.noToolCalls(),
    Stop.maxSteps(12),
    Stop.toolCalled('issue_refund'),
  ),

  compaction: {
    reserveTokens: 8_000,
    keepRecentTokens: 4_000,
    instructions: 'Summarise the customer’s issue and what has been tried.',
  },

  // Hard and shared by this root run, the researcher, and all descendants.
  runPolicy: {
    maxTurns: 24,
    maxModelCalls: 32,
    maxDelegatedTasks: 8,
    maxInputTokens: 250_000,
    maxOutputTokens: 32_000,
    wallClockMillis: 120_000,
  },
}).withHandlers({
  lookup_order: ({ orderId }) =>
    Effect.gen(function* () {
      const orders = yield* OrderRepo;
      return { status: yield* orders.status(orderId) };
    }),

  issue_refund: ({ orderId }) =>
    Effect.gen(function* () {
      const orders = yield* OrderRepo;
      return { confirmation: yield* orders.refund(orderId) };
    }),
});
```

### Running it

```ts
Effect.gen(function* () {
  const result = yield* supportAgent.run('where is order_1042?');
});
```

`stream` is the primitive and `run` is a fold of it, so a streaming consumer
and a blocking one take the same path through the loop. `streamIn` and `runIn`
are the same two against a `Chat` the caller already holds. A run stops when
its `stopWhen` condition holds; the default is "the model asked for no tools",
and `Stop` composes `maxSteps`, `maxOutputTokens`, `toolCalled`, `any`, `all`.
`Result.outcome` distinguishes `success` from `cancelled`; `steps` counts model
turns that actually started, so a queued cancellation can return zero while an
in-flight cancellation preserves its partial text, usage, and one started turn.
These are soft stops: a steer may request another turn. `runPolicy` is the hard
boundary and cannot be overridden. Its runtime is created once per root run and
passed into every descendant, so delegation cannot reset turn, model-call,
token, deadline, depth, breadth, or concurrent-child accounting. Requested
tool concurrency, including `unbounded`, is clamped to `maxToolConcurrency`.

Handlers attach as a method rather than a `Definition` field, mirroring
`toolkit.toLayer(handlers)` in `effect/unstable/ai`. Calling `withHandlers`
twice replaces the handlers rather than stacking a second set beneath them,
which is also how `recordingTo` and `intercepting` behave.

## What the types buy

`Agent.Instance<Name, Tools, Requires>` — and `Requires`, what still has to be
provided to run it, is the only parameter a caller normally writes. Two
failures the compiler catches that are otherwise silent until the model does
the wrong thing at runtime:

**A subagent's services reach its parent.** `Definition.subagents` captures a
tuple of `Agent.Named`, not `ReadonlyArray<Agent.Any>`, so a child whose tool
declares `dependencies: [OrderRepo]` puts `OrderRepo` in the parent's
requirement channel with nothing declared at the parent. Erased, a parent whose
child read a database compiled clean and died the first time the model
delegated.

**Tool names are checked against the toolkit.**
`Stop.toolCalled('issue_refund')` does not compile unless that tool exists. A
misspelled terminal tool never matches, so the run stops later than intended
and looks like a model behaving oddly rather than a typo.

Both are pinned by mutation-checked type tests in `type-tests/` and
`packages/agent/test/assertions.test.ts`: reverting the fix in the source fails
the build, not just the assertion.

None of it is expressible over an agent API whose `execute` returns a
`Promise`. That is the architectural reason this exists.

## Subagents and skills

A subagent is an agent definition compiled to a tool named `task_<child>` on
its parent, so delegation composes through the ordinary toolkit machinery.
Delegation depth defaults to 4 and is controlled by
`RunPolicy.Limits.maxDelegationDepth` with the other shared hard limits.

A skill is a `{ name, description, instructions }` value. The catalog — names
and one-line descriptions — is appended to the agent's instructions so the
system prefix stays byte-identical across turns and stays cacheable, and the
bodies load through a `load_skill` tool. The parameter schema is a literal
union of the skill names, so asking for one that does not exist fails
validation rather than returning an empty string the model may not notice.

Skills here are values passed to `Agent.make`; there is no discovery from disk.

## Compaction and the context window

Compaction replaces old history with a model-written summary. There are two
triggers. The reactive one fires when the provider rejects the request as too
long, retries the turn once against the compacted history, and is the one that
actually saves runs. The proactive one fires from a token estimate before a
turn that would not have fit — but only when the caller sets
`Compaction.Policy.contextWindow`, because the loop targets the `LanguageModel`
tag and that tag does not carry a window.

The estimate comes from `ContextWindow.Service`, a `Context.Reference` whose
default counts four characters per token. Applications can install
`ContextWindow.usageAnchored`, which takes the latest turn's reported usage as
exact and estimates only messages after that assistant response, so guesswork
is bounded by one turn's text rather than the whole conversation.

Compaction splits on whole messages rather than tokens, so a tool call is never
cut away from its result, and the agent's own system message always survives
into the resulting history.

## The conversation log

```ts
Effect.gen(function* () {
  const recording = supportAgent.recordingTo(conversationId);
  const result = yield* recording.run('where is order_1042?');
});

// elsewhere, in another fiber or another process
const records = Agent.streamFrom(conversationId, lastOffsetSeen);
```

`recordingTo` returns an agent that writes each run into
`@sunfall/vesper-log` as it happens: the run's input, model text, tool calls,
durable handler starts and outcomes, turn boundaries, compactions, signals
taken, completion, settlement. `streamFrom` replays those and then follows live, which is
`Tail.from` with a path convention rather than a second read-then-follow loop.
It yields records, not events — synthesising events would mean inventing text
deltas nobody sent.

Raw persistence is the explicit default. An application can filter only the
recorded representation without changing the values the live model and tools
see:

```ts
const recording = supportAgent.recordingTo(conversationId, {
  prompt: (prompt) => Redaction.redactPrompt(prompt),
  toolParameters: (params, call) => Redaction.redactTool(call.name, params),
  toolResult: (result, outcome) => Redaction.redactTool(outcome.name, result),
  signal: (signal) => Redaction.redactSignal(signal),
  cause: (rendered) => Redaction.redactCause(rendered),
});
```

Any Effect services those functions use are added to the returned agent's
`Requires`; raw recording adds only `LogStore.Service`. The compiled filter is
carried into child sessions. Since records are also the resumption source,
future resumed prompts and recovered tool outcomes use the filtered values;
filters should preserve enough shape for the tool and prompt codecs involved.

**Logging is optional, and the type says which you have.** `run` does not
require a `LogStore` and every non-recording call site is unchanged; the agent
`recordingTo` hands back requires `LogStore.Service`, so a caller who has not
provided one does not compile. It is deliberately not an ambient
`Context.Reference` with a no-op default: persistence behind a defaulted
reference gives a caller who forgot plausible behaviour and no signal.

Two properties worth knowing before reading records:

- **Text is coalesced**, one record per contiguous run of assistant text within
  a turn, flushed at a tool call, a tool result, a turn boundary, or
  completion. The log is for rebuilding a conversation, not for replaying the
  provider's wire format.
- **The append lands before the event reaches the consumer.** Records are
  written with `Stream.tap`, so nothing downstream can act on something the log
  does not already contain.

Claiming a conversation fences the previous producer, so two concurrent runs
against one id do not interleave: the older one fails its next append instead
of writing a history that never happened. A failed append is a **defect**, not
a typed failure — continuing past one would produce a run whose result exists
and whose history does not.

Two backends implement `LogStore`: an in-memory one, and Postgres. The Postgres
backend consumes Effect's official `PgClient`/`SqlClient`; transaction,
connection, interruption, and query lifecycles remain Effect SQL concerns. It
never issues DDL: the schema is the application's to own and migrate. The
authoritative DDL is published as `packages/log/migrations/001-initial.sql`;
the integration harness applies that same asset. Wake-ups cross processes
through `LISTEN`/`NOTIFY`, and its `changes` stream fails rather than going
quiet, because a dead feed that looks healthy is indistinguishable from a
conversation where nothing is happening. `VesperPgClient.layer` corrects the
RC.109 driver's listener readiness and failure propagation while still
providing the official `PgClient` and generic `SqlClient` services.

### Resuming a conversation

```ts
Effect.gen(function* () {
  const result = yield* supportAgent.resume(conversationId, 'and then?');
});
```

`recordingTo` puts a conversation down; `resume` picks it back up — rebuilding
the prompt from records, seeding it under the agent's _current_ instructions,
and continuing from the next turn. A conversation that does not exist yet
starts as one. The returned `usage` is cumulative across the whole conversation
rather than this run alone.

Every `Agent.make` definition requires a non-empty application revision. Vesper
persists that revision, the agent name, and its conversation-format version in
`RunStarted` and bounded resume aggregates. Resume, branch, fork, and child
session entry reject missing, unsupported, or mismatched metadata before a
model or tool call. Bump the revision whenever instructions, tools, schemas, or
other behavior make existing durable history unsafe; old unrevisioned history
must be migrated explicitly rather than adopted silently.

**This is what makes the log a durability mechanism and not an audit trail.** A
run that crashed mid-conversation resumes without re-asking the provider for
turns it completed and without re-running the tool calls those turns made.
`resume.test.ts` states both as numbers — the provider counts the prompts it
was handed, the tool handler counts its own invocations.

`AgentHistory.messagesFrom(records)` is the reconstruction on its own, for a
reader that has records and no agent. Two rules in it are worth knowing:

- **An unanswered tool call is dropped from the prompt, but not forgotten by
  dispatch.** Providers reject an assistant tool call with no matching result.
  A `ToolStarted` without `ToolOutcome` records that its handler may already
  have committed; recovery refuses to dispatch it unless
  `onIndeterminateToolCall` explicitly returns `Retry`, or reconciles it with
  an explicit answer.
- **Recovered results must still decode through the current tool result
  schema.** A changed schema fails with an actionable `AiError` before the
  stored value can masquerade as a typed success. Explicit indeterminate-call
  reconciliation answers are checked the same way.
- **The latest `Compacted` replaces the history before it.** The record carries
  the summary and `firstKept`, the offset of the first record that survived, so
  a rebuild is the summary as a user message, then the tail compaction kept,
  then everything after. A conversation compacts repeatedly and only the last
  one is read; each summary already subsumes the one before it. Without this a
  resumed conversation came back longer than the run it resumed and compacted
  again on its first turn.

### Settlement, and what an orphan looks like

Every run ends with a `RunSettled` record — `success`, `failure`, `cancelled`,
or `interrupted`, with the cause rendered. It is written from a finalizer, so
the ways a run ends without a `Completed` event are written down too: an
interrupted fiber, a consumer that took three events and walked away.

It is the one write here that is swallowed rather than turned into a defect. By
the time it runs there is nobody left to fail to, and a defect would replace
whatever actually went wrong with a complaint about the log. What a failed
settle leaves behind is a `RunStarted` with no `RunSettled`, which is exactly
the orphan shape a reader is told to look for. The absence is the signal.

### Child sessions

When the parent is recording, a delegation opens a child session with its own
conversation id, and one `ChildSession` record is written into **both** logs —
so whichever end a reader opens, it finds the same statement of who delegated
what to whom.

The child id is unambiguously derived from the parent's id and tool call id, so
a re-run of the same delegation resumes the child it already started instead of
orphaning it.

The session and shared root runtime are passed through an internal symbol
protocol while Vesper compiles delegation. They are not public Agent methods,
so consumers cannot invoke a packed child with an arbitrary session or budget.
The child's ordinary requirement channel remains exact because the session
already holds the store.

### Signals

```ts
Effect.gen(function* () {
  yield* AgentSignals.send(conversationId, {
    kind: 'steer',
    text: 'also check the invoice',
    source: 'operator',
  });
});
```

Out-of-band input to a running conversation. Signals live in a second stream,
`signals/<conversationId>`, which a run only ever reads — appending them to the
conversation itself would fence the run you are trying to steer. The run drains
them at each turn boundary, mirrors delivery into its own log as
`SignalReceived`, and emits a `Signalled` event so a consumer can render it.

A valid `cancel` in the next bounded page can interrupt an in-flight provider
stream before that boundary when no real tool or delegation handler has begun.
The boundary remains the sole durable acknowledgement and ordering point. Once
a real handler begins, cancellation waits for its durable outcome and the
normal boundary. The run settles as `cancelled` — cancellation ends a run, it
does not fail one, so partial work already done still
comes back. A `steer` becomes a user message on the next turn and **outranks
the stop condition** for that turn, including a step ceiling: a run that
consumed an instruction and stopped anyway has silently ignored it.
It never outranks a hard run budget.

Signal reads are bounded by `maxSignalsPerBoundary`; a backlog emits
`SignalBacklog` and remains after the durable cursor for a later boundary.
Oversized signals and steers over the run's cumulative byte budget emit
`SignalRejected` and are persisted as rejected `SignalReceived` records, so
advancing the cursor never silently discards them.
`AgentSignals.send` persists its separate incoming record raw. A queued signal
cannot be both recoverable with its original text and redacted at rest; apply
ingress protection before calling `send` when the sender intentionally wants
the delivered value transformed as well.

Delivery is resumable and at-least-once. `SignalReceived` records the offset it
consumed, so a signal queued before a run began is still delivered and one
already acted on is not delivered twice.

The drain is one bounded read from a cursor, deliberately. A production system
this design came out of built the same mechanism twice on a durable workflow
engine — first as a loop over the engine's blocking `recv`, then as a table
drained inside a step — and both failed the same way: delivery was built out of
an operation whose call count was itself recorded state, so a replay diverged.
A read from a cursor has no such count.

### Resuming a tool call after a crash

When a session opens, it indexes both tool outcomes and unresolved
`ToolStarted` records from orphaned runs. Completed outcomes are served instead
of re-running the tool. An unresolved start is never dispatched silently: the
dedicated `onIndeterminateToolCall` interceptor must explicitly return
`Interception.retry`, `Interception.reconcile(result)`, or
`Interception.reconcileFailure(result)`; without it the run fails safely and
remains orphaned. Ordinary `beforeToolCall` dispatch permission is not a retry
decision. Nothing in `LanguageModel` had to change: its `toolkit`
option already accepts an `Effect` producing a resolved toolkit, which is what
a `Toolkit` is.

It is gated on settlement, which is the safety property rather than a
diagnostic: a conversation whose last run finished has an empty index and
dispatches exactly as it always did, and a run can never serve itself its own
result. A match is tool name plus the provider-assigned call id, and nothing
else. Ids are random per call in practice, so this is sound in practice and
unfalsifiable from here; matching parameters would not help, because the log
records decoded parameters and dispatch is handed encoded ones.

## Interception

Spans observe. An interceptor intervenes.

```ts
const guarded = supportAgent.intercepting({
  beforeToolCall: (call) =>
    Effect.gen(function* () {
      const policy = yield* Policy;
      return (yield* policy.allows(call.name))
        ? Interception.dispatch
        : Interception.refuse(`${call.name} needs approval first`);
    }),
});
```

Four seams, named rather than general, each with a type that admits exactly
what it is for:

| seam                      | observe | change the input | answer instead | fail |
| ------------------------- | ------- | ---------------- | -------------- | ---- |
| `beforeTurn`              | yes     | yes              | no             | yes  |
| `beforeModelCall`         | yes     | no               | no             | yes  |
| `beforeToolCall`          | yes     | no               | yes            | yes  |
| `onIndeterminateToolCall` | yes     | no               | answer/retry   | yes  |

The alternative — a service holding one `(Effect) => Effect` applied wherever
the loop does something interesting — was rejected. It has no name and no
contract, so a reader of the loop has to assume every seam may do everything,
and the type says nothing that could be checked. `interception.ts` gives the
reasoning for each cell, including the two that look like omissions:
`beforeTurn` cannot end a run (that is `stopWhen`, and a second way to do it
would record `success` for a run nobody completed) and `beforeModelCall` cannot
rewrite the prompt, because only the turn's input is a value here and the rest
is `Chat`'s history.

An agent that never calls `intercepting` requires exactly what it required
before and takes the same branch through the loop; an agent that does requires
whatever its interceptor's seams require. Calling it again replaces the
interceptor rather than stacking a second one, because two opinions at one seam
need an order and every order is wrong for somebody.

### When the log and an interceptor disagree

Both have a view of a tool call, so the order is fixed: a completed recovery
outcome, an indeterminate-resolution callback, `beforeToolCall`, then the tool.
A call an unsettled earlier run already completed is served from the log and
the interceptor is **not**
consulted — that call already ran, and refusing it now would show the model a
refusal for work that actually happened. Stated as a limit: an interceptor
cannot revoke permission for a tool call a crashed run completed. Settling the
run empties the index; refusing does not.

The other direction is the same rule: a call the interceptor answered is
recorded as an ordinary `ToolOutcome`, so a later run recovers the substituted
answer rather than re-asking. What the log says happened is what happened.

An interceptor belongs to the agent it was attached to. A subagent is its own
loop and does not inherit its parent's — but the delegation itself is a tool
call, so `beforeToolCall` sees `task_<child>` like anything else.

## Trying it against a real model

```bash
ANTHROPIC_API_KEY=... nub run example:compliance-relay
```

`examples/compliance-relay` streams an answer from one agent through a second
that rewrites any sentence violating a policy. The speaker's stream is never
connected to the terminal — sentences go to the judge, and the judge's stream
is the output, so unreviewed text has no path out rather than being caught on
the way.

`examples/live-smoke` is the broader one: it drives tools, delegation, skills,
logging, branching, forking, the workspace toolkit, and both compaction
triggers against a real provider. Everything else in the repository runs
against scripted Effect `LanguageModel` implementations.

Neither example has been run against a live provider since the persistence
mechanisms were pruned to one.

## Development

```bash
npx @nubjs/nub@0.7.5 install   # bootstraps the pinned nub, then installs
nub run verify                 # build, then format + lint + typecheck, then tests
```

The toolchain is [nub](https://nubjs.com), pinned as the `@nubjs/nub`
devDependency; `npm install -g @nubjs/nub@0.7.5` afterwards puts it on `PATH`
for daily use. Individually: `nub run build`, `nub run test`,
`nub run typecheck`, `nub run lint`, `nub run format`. Tests live in
`packages/<name>/test/` and run from the repository root. The Postgres
integration suite is skipped unless `RUN_POSTGRES_INTEGRATION=1` and a
container runtime is available. `nub run benchmark` runs the timing and memory
harness in `benchmarks/`.

See [`docs/contributing.md`](docs/contributing.md) for the layering rules
between packages, which are the one thing worth reading before changing
anything.

## Known gaps

- **This has not run in production in its extracted form.** It came out of a
  working system, but the packaging, the pruning to one persistence mechanism,
  and this repository are all newer than that. The last three things that
  genuinely improved this library came from running it rather than planning it.
- **No branch summarization.** Switching away from a branch records no summary
  of what it contained.

  The concurrency half of this gap is closed. `agent.branchFrom(id, at, input)`
  re-runs a conversation from an earlier record in the same stream — a
  `ConversationRecord.BranchedFrom` marker names the point and
  `AgentBranch.activePath` folds the tree back out — and because a stream has
  one writer, two branches are sequential. `agent.forkFrom(id, at, forkId,
input)` is the other trade: it seeds a **new** conversation from the same
  prefix, so two forks are two streams and run side by side. A fork copies the
  prefix rather than costing one record, its offsets are its own — `log.ts`'s
  `reseat` is where `Compacted.firstKept` is rewritten onto them and
  `SignalReceived.at` is reset, because the fork's signal stream is a different
  one — and it leaves the ancestor untouched, so the relationship is not
  navigable from the ancestor's side.

- **No implicit harness toolkit or prompt templates.** Nothing installs shell,
  read, or edit tools on every agent by default. Applications explicitly opt in
  through `WorkspaceAgent.standard` or `WorkspaceAgent.compose` from
  `@sunfall/vesper-workspace/agent`.
- **The proactive compaction path is exercised only by tests.**
  `Compaction.Policy.contextWindow` is opt-in and nothing in this repository
  sets it, so in practice only the reactive overflow path runs.
- **Estimation accuracy against a real tokenizer is untested.** The fixture in
  `packages/agent/test/context-window.test.ts` stipulates its truth figure; what it
  demonstrates is that anchoring on reported usage beats a character count, not
  how close either lands to a tokenizer.
- **The `Compacted` schema is not stable yet.** The record used to carry counts
  alone and now carries `summary` and `firstKept`. Nothing is deployed against
  it, so this costs nothing today and will not stay free.
- **The compaction boundary is a message count resolved against the log.**
  Compaction runs against `Chat`'s in-memory history, which carries no record
  identity, so it reports how many messages it kept and the sink turns that
  into `firstKept` via `AgentHistory.boundaryFor`. That rests on the record
  rebuild and the live history being the same sequence of messages. Drift moves
  the boundary by a message or two rather than corrupting anything.
- **Signals only reach a run that is recording.** Steers apply only at turn
  boundaries. Cancels can preempt provider streaming, but not after a real tool
  or delegation handler has begun; backlog and rejected cancels never leapfrog
  the boundary drain.
- **Indeterminate recovery needs application knowledge.** `ToolStarted` closes
  the dangerous ambiguity but cannot determine whether an external system
  committed. Vesper deliberately has no default: applications must query or
  reconcile that system and explicitly Answer, or knowingly Retry.
- **History without a resume aggregate requires a full compatibility scan.**
  `RunSettled.resume` is the only bounded cumulative aggregate. Opening a
  compatible history without one writes no intermediate checkpoint, so later
  opens remain unbounded until a new run settles. Compacted prompts still page
  only their live suffix; orphaned and uncompacted prompt state remains
  proportional to the records it genuinely needs.
- **Performance is reasoned about more than it is measured.** `benchmarks/`
  covers turn cost, conversation growth, scaling, startup, and memory, with and
  without recording. One real problem was found and fixed before it existed:
  the streaming queue was unbounded, so a slow consumer buffered a whole
  response with no backpressure.

## License

MIT — see [`LICENSE`](LICENSE).
