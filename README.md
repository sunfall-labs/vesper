<p align="center">
  <img src="./vesper.png" alt="Vesper — an abstract violet bat emerging from the dark" width="680" />
</p>

<h1 align="center">Vesper</h1>

<p align="center">
  <strong>Effect-native agents with compile-time requirements and durable conversations.</strong>
</p>

<p align="center">
  <a href="#quick-start">Quick start</a> ·
  <a href="#packages">Packages</a> ·
  <a href="#complete-example">Complete example</a> ·
  <a href="./Design.md">Design</a> ·
  <a href="./docs/releasing.md">Releasing</a>
</p>

Vesper is an agent loop built directly on `effect/unstable/ai`. Effect supplies
the provider seam, tools, prompts, responses, and chat; Vesper adds the repeated
turn loop and makes two difficult properties ordinary:

| Compile-time confidence                                                               | Durable execution                                                                                    | Native composition                                                                              |
| ------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------- |
| Missing tool, subagent, state, and interceptor requirements fail during typechecking. | Conversations are recorded, resumed, branched, steered, compacted, and recovered from one event log. | Providers, Workflow, Cluster, Schema, Stream, Layer, tracing, and MCP remain Effect primitives. |

Vesper does not wrap provider SDKs or maintain a second provider registry.
Official Effect AI packages provide `LanguageModel` directly.

> [!IMPORTANT]
> **Vesper is pre-1.0. The current release is an early public alpha.** Read the
> [maturity and deliberate constraints](#maturity-and-deliberate-constraints)
> before adopting it. Migrating from the former durability methods? Start with
> [Migrating to Conversation](docs/migrating-to-conversation.md).

Vesper requires Node.js 22 or newer. The Effect packages in the examples are
release candidates and must use the same `4.0.0-rc.109` version.

## Quick start

```bash
npm install @sunfall/vesper-agent \
            @sunfall/vesper-log \
            @effect/ai-anthropic@4.0.0-rc.109 \
            @effect/platform-node@4.0.0-rc.109 effect@4.0.0-rc.109
```

The following is a complete, in-memory example. It makes two real Anthropic
requests and records the second run's durable continuation in memory; set
`ANTHROPIC_API_KEY` before running it. For a credential-free example, run
`nub run example:support-agent` from a checkout of this repository.

```ts
import { AnthropicClient, AnthropicLanguageModel } from '@effect/ai-anthropic';
import * as NodeHttpClient from '@effect/platform-node/NodeHttpClient';
import * as NodeRuntime from '@effect/platform-node/NodeRuntime';
import * as NodeServices from '@effect/platform-node/NodeServices';
import { Agent } from '@sunfall/vesper-agent/agent';
import { Conversation } from '@sunfall/vesper-agent/conversation';
import { LogStoreMemory } from '@sunfall/vesper-log/layer-memory';
import { Config, Effect, Layer } from 'effect';
import { Toolkit } from 'effect/unstable/ai';

const support = Agent.make({
  name: 'support',
  revision: '1',
  instructions: 'Resolve the customer’s request clearly and accurately.',
  toolkit: Toolkit.make(),
});

const program = Effect.gen(function* () {
  // One unrecorded run.
  const answer = yield* support.run('Where is my order?');

  // Or bind the same definition to durable conversation history.
  const conversation = Conversation.make(support, 'customer-42');
  const durableAnswer = yield* conversation.run('Where is my order?');

  return { answer, durableAnswer };
});

const model = AnthropicLanguageModel.model('claude-sonnet-4-6').pipe(
  Layer.provide(
    AnthropicClient.layerConfig({
      apiKey: Config.redacted('ANTHROPIC_API_KEY'),
    }),
  ),
);
const log = LogStoreMemory.layer.pipe(Layer.provide(NodeServices.layer));

NodeRuntime.runMain(
  program.pipe(
    Effect.provide(model),
    Effect.provide(log),
    Effect.provide(
      Layer.mergeAll(NodeServices.layer, NodeHttpClient.layerUndici),
    ),
  ),
);
```

Save it as `index.ts` and run it with:

```bash
ANTHROPIC_API_KEY=... node --experimental-strip-types index.ts
```

Provide an official Effect `LanguageModel` layer for both forms. Durable
conversations additionally require a Vesper `LogStore` and Effect `Crypto`
implementation; those requirements remain visible in the Effect channel and
are compile-time errors until provided.

Effect and its provider packages are pinned together at `4.0.0-rc.109` while
the interfaces are release candidates, ensuring one Effect service identity in
the application.

## Packages

Vesper publishes six focused packages:

| Package                                               | Purpose                                                                                |
| ----------------------------------------------------- | -------------------------------------------------------------------------------------- |
| [`@sunfall/vesper-agent`](packages/agent)             | Agent loop, typed eval suites, workflows, skills, recording, resumption, and subagents |
| [`@sunfall/vesper-log`](packages/log)                 | Event-sourced conversations, offsets, tailing, and the memory adapter                  |
| [`@sunfall/vesper-log-pg`](packages/log-pg)           | PostgreSQL log adapter and authoritative migration                                     |
| [`@sunfall/vesper-workspace`](packages/workspace)     | Filesystem and shell tools behind a swappable driver                                   |
| [`@sunfall/vesper-attachments`](packages/attachments) | Verified content-addressed blobs with memory and filesystem adapters                   |
| [`@sunfall/vesper-mcp`](packages/mcp)                 | Scoped, Effect-native MCP tools for Vesper agents                                      |

Workspace authority is always composed explicitly. Attachments remain a
separate reusable package and are only externalized when an
`AttachmentStore.Service` is provided.

### Documentation

- [Design and architectural boundaries](Design.md)
- [Agent guide](packages/agent/README.md)
- [Workspace composition](docs/workspace.md)
- [Conversation migration guide](docs/migrating-to-conversation.md)
- [Contributing and package layering](docs/contributing.md)
- [Release procedure and version policy](docs/releasing.md)
- [Benchmarks and operational probes](benchmarks/README.md)
- [Changelog](CHANGELOG.md) · [Security and support](SECURITY.md)

## Complete example

`examples/support-agent` is compiled with requirement-channel assertions and
runs entirely against a scripted model, in-memory application adapters, and the
in-memory conversation log. The definition excerpt below introduces the core
composition; the source also exercises State, handler-level authorization, and
a durable human approval inside the refund handler.

### Mocked world

```ts
import { ScriptedModel } from '@sunfall/vesper-agent/testing';

// Arrays of Effect `Response.StreamPartEncoded`, not Vesper response wrappers.
const fake = ScriptedModel.make(supportTurns);

const World = supportWorkflow.layer.pipe(
  Layer.provideMerge(WorkflowEngine.layerMemory),
  Layer.provide(fake.layer),
  Layer.provide(OrderRepoTest),
  Layer.provide(RefundAuthorizationTest),
  Layer.provideMerge(LogLive),
);
```

The fake is the provider: it implements Effect's `LanguageModel` seam directly
and models no vendor. Its strict script found the extra turn caused by an
accepted steer while this example was built. Application behavior is the same
definition used with a real provider; only Layers change.

### Agent definition

<details>
<summary><strong>View the complete typed agent and workflow</strong></summary>

This definition exercises tools, application requirements, subagents, skills,
run budgets, compaction, durable approval, and idempotent Workflow activities.

```ts
import { Agent } from '@sunfall/vesper-agent/agent';
import { Conversation } from '@sunfall/vesper-agent/conversation';
import { Skill } from '@sunfall/vesper-agent/skill';
import { Stop } from '@sunfall/vesper-agent/stop';
import { AgentWorkflow } from '@sunfall/vesper-agent/workflow';
import { Context, Effect, Schema } from 'effect';
import { Tool, Toolkit } from 'effect/unstable/ai';

/** An ordinary application service. Nothing here is AI-specific. */
export class OrderRepo extends Context.Service<
  OrderRepo,
  {
    readonly status: (id: string) => Effect.Effect<string>;
    readonly refund: (
      id: string,
      idempotencyKey: string,
    ) => Effect.Effect<string>;
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
  description: 'Refund one order after human approval.',
  parameters: Schema.Struct({ orderId: Schema.String }),
  success: Schema.Struct({
    status: Schema.Literals(['refunded', 'declined']),
    detail: Schema.String,
    actor: Schema.String,
  }),
  dependencies: [OrderRepo],
});

const refundApproval = AgentWorkflow.wait({
  name: 'refund-approval',
  key: ({ orderId }) => orderId,
  request: Schema.Struct({ orderId: Schema.String }),
  success: Schema.Struct({
    decision: Schema.Literals(['approve', 'deny']),
    actor: Schema.String,
  }),
  error: Schema.Never,
});

const refundOrder = AgentWorkflow.step({
  name: 'refund-order',
  key: (orderId: string) => orderId,
  success: Schema.String,
  error: Schema.Never,
  execute: (orderId: string) =>
    Effect.gen(function* () {
      const orders = yield* OrderRepo;
      const key = yield* AgentWorkflow.idempotencyKey('refund-order');
      return yield* orders.refund(orderId, key);
    }),
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

  toolkit: Toolkit.make(lookupOrder, AgentWorkflow.durable(issueRefund)),

  // Compiled into the toolkit; the child's requirements ride along.
  subagents: [researcher],

  // The catalog goes in the system prompt, the bodies load on demand — so
  // the cacheable prefix stays byte-identical across turns.
  skills: [refundPolicy],

  // Let the model observe the approved or declined result before stopping.
  stopWhen: Stop.any(Stop.noToolCalls(), Stop.maxSteps(12)),

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
      const approval = yield* refundApproval({ orderId });
      if (approval.decision === 'deny') {
        return {
          status: 'declined',
          detail: 'The supervisor declined the refund.',
          actor: approval.actor,
        } as const;
      }
      return {
        status: 'refunded',
        detail: yield* refundOrder(orderId),
        actor: approval.actor,
      } as const;
    }),
});

class SupportWorkflowFailure extends Schema.TaggedError<SupportWorkflowFailure>(
  'SupportWorkflowFailure',
)('SupportWorkflowFailure', { message: Schema.String }) {}

const SupportRequest = AgentWorkflow.request({ runId: Schema.String });
const supportWorkflow = AgentWorkflow.make(supportAgent, {
  tag: 'SupportStory',
  payload: SupportRequest,
  idempotencyKey: ({ runId }) => runId,
  error: SupportWorkflowFailure,
  mapError: (error) => new SupportWorkflowFailure({ message: String(error) }),
});
```

</details>

### Running it

```ts
Effect.gen(function* () {
  const request = {
    runId: 'case-1042/initial',
    conversationId: 'case-1042',
    input: 'Refund damaged order_1042 when allowed.',
  };
  yield* supportWorkflow.workflow.execute(request, { discard: true });

  const pending = yield* refundApproval.awaitPending(
    Conversation.make(supportAgent, request.conversationId),
    'order_1042',
  );
  yield* pending.complete({ decision: 'approve', actor: 'supervisor-7' });

  const result = yield* supportWorkflow.workflow.execute(request);
});
```

Run the complete credential-free story with:

```bash
nub run example:support-agent
```

It loads a skill, delegates policy research, invokes stateful tools whose typed
handlers enforce authorization, suspends on a typed approval, performs the
refund as an idempotent Workflow activity, accepts a steer, resumes, prints the full
durable trail, and evaluates the same researcher definition used as a
subagent. The mocked supervisor completes the approval automatically; use the
focused `example:approval-cli` story below to choose approve or deny
interactively.

Workflow input can also be any application-owned Effect Schema rather than a
string. `AgentWorkflow.makeWithInput` requires an exhaustive projection into
Effect's `Prompt.RawInput`, so typed participant events remain durable without
introducing a second prompt codec. See
[Schema-typed workflow input](packages/agent/README.md#schema-typed-workflow-input)
for the multiplayer composition and serialization pattern.

`stream` is the primitive and `run` is a fold of it, so a streaming consumer
and a blocking one take the same path through the loop. `streamIn` and `runIn`
are the same two against a `Chat` the caller already holds. A run stops when
its `stopWhen` condition holds; the default is "the model asked for no tools",
and `Stop` composes `maxSteps`, `maxOutputTokens`, `toolCalled`,
`toolCalledTimes`, `any`, `all`. `Result.outcome` is `success`, `cancelled`, or `suspended` — a tool call
durably waiting on approval, covered in [Tool approvals](#tool-approvals); `steps` counts model turns that actually started, so a queued
cancellation can return zero while an in-flight cancellation preserves its
partial text, usage, and one started turn. These are soft stops: a pending
steer, or a signal backlog a turn boundary could not fully drain, outranks a
positive stop decision for one more turn — so `Stop.maxSteps(N)` is not a hard
ceiling once a conversation takes signal traffic. `runPolicy` is the hard
boundary and cannot be overridden; `runPolicy.maxTurns` is the ceiling that
holds regardless. Its runtime is created once per root run and passed into
every descendant, so delegation cannot reset turn, model-call, token,
deadline, depth, breadth, or concurrent-child accounting. Requested tool
concurrency, including `unbounded`, is clamped to `maxToolConcurrency`.
`maxInputTokens` and `maxOutputTokens` are checked after each turn's usage is
known, not before a request is sent — there is no way to ask a provider
whether a turn will fit a budget before making it — so a run can overshoot
either ceiling by up to one turn's usage before the check after it fails the
run. The limits bound cumulative spend; they do not cap any single request.

Handlers attach as a method rather than a `Definition` field, mirroring
`toolkit.toLayer(handlers)` in `effect/unstable/ai`. Calling `withHandlers`
twice replaces the handlers rather than stacking a second set beneath them,
which is also how `intercepting` behaves. Two interceptors that should both
run are joined first with `Interception.compose`, which fixes their order
explicitly — per-seam rules are on its doc comment — and hands `intercepting`
one combined value, so attachment is still a single replace.

## Core concepts

### Compile-time guarantees

`Agent.Instance<Name, Tools, Requires>` — and `Requires`, what still has to be
provided to run it, is the only parameter a caller normally writes. Two
failures the compiler catches that are otherwise silent until the model does
the wrong thing at runtime:

**A subagent's services reach its parent.** `Definition.subagents` captures a
tuple of branded `Agent.Child`, not `ReadonlyArray<Agent.Any>`, so a child whose tool
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

### Subagents and skills

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

### Code mode

`codeMode: true` replaces direct tool advertisement with one isolated `exec`
tool: the model writes JavaScript that composes the toolkit's tools, the
script runs in a `CodeExecutor`, and each nested call dispatches through the
same gated toolkit an advertised call would — intercepted and metered, with
only the script's `text(...)` output returning to the model. Enabling it puts
`CodeExecutor.Service` on the agent's requirement channel, so a missing
executor is a compile error like any other missing service.

`codeMode: { except: ['release'] }` brokers everything _but_ the named tools,
which stay directly advertised — gated, intercepted, metered, and, when
marked `Tool.setNeedsApproval`, durably approvable exactly as if code mode
were off for them. That is the intended pairing: a broad toolkit behind
`exec` for composition, with the one or two consequential tools kept on the
provider seam where the approval machinery lives. A brokered tool that
requires approval is refused inside `exec` with a typed failure rather than
silently executed, and the excepted names are checked against the toolkit at
compile time — a misspelling is a type error, not a tool that never matches.

### Compaction and the context window

Compaction replaces old history with a model-written summary. There are two
triggers. The reactive one fires when the provider rejects the request as too
long, retries the turn once against the compacted history, and is the one that
actually saves runs. The proactive one fires from a token estimate before a
turn that would not have fit — but only when the caller sets
`Compaction.Policy.contextWindow`, because the loop targets the `LanguageModel`
tag and that tag does not carry a window. A policy configured without
`contextWindow` is not an error — the reactive trigger still protects the
run — but it means proactive compaction never fires, silently, for the whole
run. The agent logs an `Effect.logWarning` once per run when that happens, so
the gap shows up in logs instead of only in a postmortem.

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
  const conversation = Conversation.make(supportAgent, conversationId);
  const result = yield* conversation.run('where is order_1042?');
});

// elsewhere, in another fiber or another process
const records = Conversation.make(supportAgent, conversationId).follow(
  lastOffsetSeen,
);
```

`Conversation.make` binds an agent to a durable conversation and writes each
run into `@sunfall/vesper-log` as it happens: the run's input, model text, tool
calls, durable handler starts and outcomes, turn boundaries, compactions,
signals taken, completion, settlement. `follow` replays those and then follows
live, which is `Tail.from` with a path convention rather than a second
read-then-follow loop.
It yields records, not events — synthesising events would mean inventing text
deltas nobody sent. Use `conversation.records(after)` instead when the caller
needs a finite snapshot rather than a live tail.

Raw persistence is the explicit default. An application can filter only the
recorded representation without changing the values the live model and tools
see:

```ts
import { RecordingPolicy } from '@sunfall/vesper-agent/recording-policy';
import { Effect } from 'effect';

const redactText = (value: unknown) =>
  typeof value === 'string'
    ? Effect.succeed('[redacted]')
    : Effect.succeed(value);

const conversation = Conversation.make(supportAgent, conversationId, {
  prompt: redactText,
  toolParameters: (value) => redactText(value),
  toolResult: (value) => redactText(value),
  externalRequest: (value) => redactText(value),
  externalResult: (value) => redactText(value),
  signal: (signal) => Effect.succeed({ ...signal, text: '[redacted]' }),
  cause: () => Effect.succeed('[redacted cause]'),
} satisfies RecordingPolicy.Policy);
```

Recording policies are ordinary Effect functions; there is no built-in
`Redaction` helper. Use `RecordingPolicy.preserving(schema, transform)` for
schema-shaped values when the persisted value must retain its original type.

Any Effect services those functions use are added to the returned agent's
`Requires`; raw recording adds `LogStore.Service` and Effect's `Crypto` service
for portable identifiers. Node applications can provide `NodeServices.layer`.
The compiled filter is carried into child sessions. Since records are also the
resumption source, future resumed prompts and recovered tool outcomes use the
filtered values; filters should preserve enough shape for the tool and prompt
codecs involved.

**Logging is optional, and the type says which you have.** `run` does not
require a `LogStore` and every non-recording call site is unchanged; the agent
bound by `Conversation.make` requires `LogStore.Service`, so a caller who has not
provided the log store and `Crypto` does not compile. It is deliberately not an ambient
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

Two adapters implement `LogStore`: core's in-memory adapter and the opt-in
`@sunfall/vesper-log-pg` package. PostgreSQL consumes Effect's official
`PgClient`/`SqlClient`; transaction,
connection, interruption, and query lifecycles remain Effect SQL concerns. It
never issues DDL: the schema is the application's to own and migrate. The
authoritative DDL is published as `packages/log-pg/migrations/001-initial.sql`;
the integration harness applies that same asset. Wake-ups cross processes
through `LISTEN`/`NOTIFY`, and its `changes` stream fails rather than going
quiet, because a dead feed that looks healthy is indistinguishable from a
conversation where nothing is happening. `VesperPgClient.layer` corrects the
RC.109 driver's listener readiness and failure propagation while still
providing the official `PgClient` and generic `SqlClient` services.

### Resuming a conversation

```ts
Effect.gen(function* () {
  const conversation = Conversation.make(supportAgent, conversationId);
  const result = yield* conversation.run('and then?');
});
```

`conversation.run` always continues from the active durable history. It rebuilds
the prompt from records, seeds it under the agent's _current_ instructions, and
starts the next turn; when the conversation does not exist yet, the same method
starts it. `conversation.stream` is the streaming form of that exact operation.
The terminal `usage` is cumulative across the whole conversation rather than
the latest run alone.

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
  const conversation = Conversation.make(supportAgent, conversationId);
  yield* conversation.send({
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
`SignalBacklog` and remains after the durable cursor for a later boundary. A
boundary that could not fully drain its backlog **also outranks the stop
condition** for that turn, the same way a steer does — otherwise stopping now
would drop the undrained signals rather than let the next boundary see them.
Between the two, `Stop.maxSteps(N)` is a soft ceiling once a conversation
takes signal traffic; `runPolicy.maxTurns` is the one that still holds.
Ingress rejects individual payloads over 256 KiB, but Vesper does not impose a
conversation-wide signal count or storage quota. Authenticate and rate-limit
public senders, and enforce retention or storage quotas at the application and
log-store boundary.
Oversized signals and steers over the run's cumulative byte budget emit
`SignalRejected` and are persisted as rejected `SignalReceived` records, so
advancing the cursor never silently discards them.
`conversation.send` persists its separate incoming record raw. A queued signal
cannot be both recoverable with its original text and redacted at rest; apply
ingress protection before calling `send` when the sender intentionally wants
the delivered value transformed as well.

Delivery is resumable and at-least-once. `SignalReceived` records the offset it
consumed, so a signal queued before a run began is still delivered and one
already acted on is not delivered twice during recovery. `send` does not take
an idempotency key, so retrying after an ambiguous caller-side failure may
append the same logical signal at a new offset; deduplicate at the application
boundary when that matters.

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

### Tool approvals

A tool marked with `effect/unstable/ai`'s own `Tool.setNeedsApproval` is
suspended by `LanguageModel` before its handler is ever entered — that
primitive is upstream, not Vesper's. Vesper's half is making the suspension
durable: in a recorded conversation the run ends with `outcome: 'suspended'`
and surfaces `pendingApprovals` (tool name, call id, decoded input), the
suspension is recorded with the same `ToolSuspended`/`ToolWaitCompleted`
family every durable wait uses, and `resolveApproval` records the decision —
from this process or any other holding the same log.

```ts
const release = Tool.make('release', {
  description: 'release a build to an environment',
  parameters: Schema.Struct({ id: Schema.String }),
  success: Schema.Struct({ released: Schema.Boolean }),
}).setNeedsApproval(true);

const first = yield * conversation.run('release r1');
if (first.outcome === 'suspended') {
  for (const approval of first.pendingApprovals ?? []) {
    yield * conversation.resolveApproval(approval.toolCallId, 'approve');
  }
}
// No input: the decision, not a new user message, is what there is to act
// on, so the continuation runs from durable state alone.
const result = yield * conversation.run();
```

An approved call dispatches its handler for the first time on the next run; a
denied call settles a refusal-style tool result without the handler ever
running, and the model reacts to that the way it reacts to any returned tool
failure. An undecided approval can never dispatch: a later `run` re-surfaces
the same `suspended` result until a decision lands, a crash before the
decision leaves exactly the recovery-index orphan described above, and
resolving the same call twice is a typed `ApprovalResolutionError`. Unrecorded
`agent.run` fails outright for a `needsApproval` tool — there is nowhere
durable to record the decision such a run would wait on.

This is the whole approval surface. `AgentWorkflow.wait` remains the tool for
what it was built for — a handler that must durably wait for an arbitrary
external event, with `WorkflowEngine` replay around it — not the entry fee
for a yes-or-no on one tool call. The
[agent guide](packages/agent/README.md#tool-approval) covers the record-level
mechanism, and [Durable approval locally](#durable-approval-locally) below
demonstrates the `AgentWorkflow`-backed alternative.

## Interception

Spans observe. An interceptor intervenes.

```ts
const guarded = supportAgent.intercepting({
  beforeToolCall: (call) =>
    Effect.gen(function* () {
      const policy = yield* TenantToolPolicy;
      return (yield* policy.allows(call.name))
        ? Interception.dispatch
        : Interception.refuse(`${call.name} is disabled for this tenant`);
    }),
});
```

Typed handlers are the default authority for one operation's current
availability, authorization, and durable approval. The interceptor above is
for policy that deliberately spans the toolkit; it is not a second per-tool
handler API.

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

Tool advertisement and tool enforcement are deliberately separate. Static
tool, skill, and subagent definitions stay model-visible and cache-stable while
typed handlers read current Effect state on every call. `beforeToolCall` is the
cross-cutting override for policy spanning multiple tools. Either decision is
authoritative; hiding a definition is not a security mechanism. Use
`dynamicTools` only when schemas genuinely must be discovered at the beginning
of a run, such as an MCP catalog.

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

## Examples

### Durable approval locally

```bash
nub run example:approval-cli
```

`examples/approval-cli` needs no API key. A scripted agent asks its release
tool to change production; the handler yields one keyed durable approval
through `AgentWorkflow.wait`, the CLI lets you approve or deny it, and the
same handler resumes before the agent reacts to the result. For a
non-interactive run, use `nub run example:approval-cli --decision approve` or
`--decision deny`.

This is the `Effect Workflow`-backed form, for an external step with its own
request/result shape. For a plain approve/deny gate on one tool, with no
workflow engine at all, see [Tool
approval](packages/agent/README.md#tool-approval).

### Real providers

```bash
ANTHROPIC_API_KEY=... nub run example:compliance-relay \
  "Explain the refund options without making a promise."
```

`examples/compliance-relay` streams an answer from one agent through a second
that rewrites any sentence violating a policy. The speaker's stream is never
connected to the terminal — sentences go to the judge, and the judge's stream
is the output, so unreviewed text has no path out rather than being caught on
the way.

`examples/live-smoke` is the broader one: it drives tools, delegation, skills,
finite record snapshots, following, queued signals, resumption, branching,
independent forks, the workspace toolkit, and both compaction triggers against
a real provider. Everything else in the repository runs against scripted
Effect `LanguageModel` implementations. Run only the conversation phase with
`nub run example:live-smoke -- --phase log`.

The opt-in `durability` phase uses PostgreSQL instead of the memory backend. It
writes a run and queued signal, disposes that PostgreSQL runtime and pool, then
creates an independently scoped runtime and pool to resume the same
conversation. This demonstrates persistence across application resource
replacement; it does not spawn a second OS process. Apply
`packages/log-pg/migrations/001-initial.sql` to the database first, then run:

```bash
OPENROUTER_API_KEY=... \
VESPER_DATABASE_URL=postgres://... \
nub run example:live-smoke -- \
  --phase durability \
  --provider openrouter \
  --model openrouter/free
```

OpenRouter's free router still requires an account API key and is subject to
free-tier availability and rate limits. Select a specific free model with
`--model <model-id>:free` when deterministic model routing matters.

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

## Maturity and deliberate constraints

The alpha's confidence comes from the deterministic tests, type-level checks,
and the operational probes described below. It is not a production-support
promise; the remaining caveats are either operational evidence that only
adoption can provide or explicit design constraints:

- **Extracted-form production use is still unproven.** The original system ran
  the loop, but this package layout and the consolidation onto one conversation
  log are newer. Real-provider composition lives in `examples/live-smoke` and
  `examples/compliance-relay`; deterministic tests use the same Effect
  `LanguageModel` seam.
- **Branches are preserved, not automatically summarized.** Branching and
  concurrent forks are complete, including suspended workflow-wait restart
  rules. Automatically spending a model call to summarize an abandoned path is
  application policy, not part of changing the active path.
- **Workspace tools and prompt templates are explicit composition.** Use
  `WorkspaceAgent.standard` or `WorkspaceAgent.compose`; the agent package never
  silently grants filesystem or shell authority.
- **Context-window estimates remain provider-independent heuristics.**
  `ContextWindow.usageAnchored` incorporates real reported usage and the live
  smoke suite exercises proactive compaction. Provider overflow still triggers
  the reactive path. Applications needing tokenizer-specific estimates can
  provide another `ContextWindow.Service` implementation.
- **Persisted schemas may evolve before 1.0.** Conversation format identity,
  agent revision checks, and typed compatibility failures prevent silent
  misreads. Migration is explicit rather than guessed.
- **Signals address durable conversations.** Steers apply at turn boundaries;
  cancels may preempt provider streaming but never leapfrog backlog or interrupt
  a tool after its side effect has begun.
- **Indeterminate external side effects require application reconciliation.**
  A `ToolStarted` without an outcome cannot prove whether another system
  committed. The dedicated interceptor must explicitly Answer or Retry.
- **Legacy, manual, and orphaned histories may require a proportional scan.**
  Successful current runs write the bounded `RunSettled.resume` aggregate, and
  compacted prompts read only their live suffix. Benchmarks keep the remaining
  full-scan case measurable rather than adding a second source of truth.

The benchmark suite measures turn cost, conversation growth, concurrency,
history opening, backpressure, startup, and retained memory. CI additionally
executes the PostgreSQL store and workflow integration suites.

## License

MIT — see [`LICENSE`](LICENSE).
