# `@sunfall/vesper-agent`

For replacements for the former Agent durability methods, see
[Migrating to Conversation](../../docs/migrating-to-conversation.md).

The Effect-first agent loop for Vesper: subagents, skills, stop conditions,
tool-call interception, recording, resumption, compaction, and signals.

Every `Agent.make` definition requires a non-empty `revision`. Recorded runs
persist conversation format, agent name, and revision, and all resumed entry
points reject incompatible or unrevisioned history before model/tool work.
Child sessions validate against the child definition, not the parent revision.
Compatibility failures use the tagged `Conversation.CompatibilityError`
channel, included by `Conversation.Error<A>` alongside the agent and store
failures for that bound definition.
`Agent.Result.outcome` is `success` or `cancelled`, and `steps` counts model
turns that actually started rather than planned turn boundaries.

```bash
npm install @sunfall/vesper-agent effect@4.0.0-rc.109
```

Modules are exposed as explicit subpaths, including
`@sunfall/vesper-agent/agent`, `/conversation`, `/run-policy`,
`/recording-policy`, `/eval`, `/stop`, `/skill`, `/state`, `/interception`, and
`/testing`, plus `/workflow` and `/dynamic-toolkit`.

## Dynamic tools

Use `dynamicTools` for capabilities discovered when a run starts, such as MCP
servers or tenant-specific integrations:

```ts
import { DynamicToolkit } from '@sunfall/vesper-agent/dynamic-toolkit';

const runtimeTools = DynamicToolkit.make(discoverTools(), {
  resource: {
    id: 'tenant-tools',
    description: 'Tenant-specific tools',
  },
});

const agent = Agent.make({
  // ...
  toolkit: staticTools,
  dynamicTools: [runtimeTools],
});
```

Sources open concurrently and are scoped to the run. Definitions and handlers
form one stable snapshot across its model turns. Tool-name and resource-id
collisions fail before the first model request. Wrap a nonessential source with
`DynamicToolkit.optional(source, resource)` to continue without its tools and
make that unavailability explicit in the current system context.

## Durable file attachments

Inline `Uint8Array` and `URL` file parts remain the default transport. To
externalize byte payloads into content-addressed storage, provide an
`AttachmentStore` layer around the conversation run:

```ts
import { Effect } from 'effect';
import { AttachmentStoreMemory } from '@sunfall/vesper-attachments/layer-memory';
import { Conversation } from '@sunfall/vesper-agent/conversation';

const conversation = Conversation.make(agent, 'support-42');
const result = conversation
  .run(input)
  .pipe(Effect.provide(AttachmentStoreMemory.layer));
```

`RunStarted` records then carry verified attachment references instead of file
bytes. Opening or forking with the same store hydrates and verifies those
references before prompt reconstruction; without the layer, existing inline
behavior and service requirements are unchanged. Attachment writes are part
of the same append durability boundary as the conversation log: a store write
failure is surfaced as a typed `DurabilityError` with its tagged cause,
while resume-time missing or corrupt references remain typed compatibility
failures.

## Evals

`AgentEval.run` executes a real agent and captures its typed public evidence:
the final result, event stream, duration, tool calls, and tool results. It keeps
the agent's exact error and service requirement channels, so an eval cannot
silently replace production wiring with a test-only runtime.

```ts
import { Effect } from 'effect';
import { AgentEval } from '@sunfall/vesper-agent/eval';

const program = Effect.gen(function* () {
  const capture = yield* AgentEval.run(supportAgent, 'Where is order 1042?');

  return yield* AgentEval.evaluate(
    capture,
    [
      AgentEval.check('looked up the order', (sample) =>
        AgentEval.toolCalled(sample, 'lookup_order'),
      ),
      AgentEval.makeScorer('answer quality', (sample) =>
        judgeAnswer(sample.result.text),
      ),
    ],
    { passThreshold: 0.8 },
  );
});
```

Scorers are ordinary Effects and can use a deterministic predicate, another
model, or an application-owned service. Reports include every named score and
their weighted mean; `passed` requires every criterion to meet the threshold.
Scores outside `0..1` fail with `InvalidEvalScore`. The input is deliberately
not retained because prompts commonly contain secrets or customer data; keep a
dataset identifier beside the capture when a case needs one.

## Scripted model

`ScriptedModel` is a deterministic adapter for Effect's existing
`LanguageModel` seam. It owns call sequencing, request capture, exhaustion, and
optional repetition; response parts remain Effect's types rather than a second
Vesper vocabulary.

```ts
import { ScriptedModel } from '@sunfall/vesper-agent/testing';
import type { Response } from 'effect/unstable/ai';

const turn = [
  { type: 'text-start', id: 'answer' },
  { type: 'text-delta', id: 'answer', delta: 'Done.' },
  { type: 'text-end', id: 'answer' },
  {
    type: 'finish',
    reason: 'stop',
    usage: {
      inputTokens: { total: 1, uncached: 1, cacheRead: 0, cacheWrite: 0 },
      outputTokens: { total: 1 },
    },
  },
] satisfies ReadonlyArray<Response.StreamPartEncoded>;

const fake = ScriptedModel.make([turn]);
const result = agent.run('hello').pipe(Effect.provide(fake.layer));
```

Streaming turns and non-streaming `generate` responses have independent
cursors because agent turns use `streamText` while compaction uses
`generateText`. An unscripted call fails as an `AiError`; `{ repeatLast: true }`
is explicit when repetition is the behavior under test. `fake.requests`
exposes normalized prompts, tool names, and tool choice without retaining a
tracing span.

## Durable State

Define one typed state document and declare it as a tool dependency:

```ts
import { AgentState } from '@sunfall/vesper-agent/state';

const SupportState = AgentState.make({
  id: 'support-case',
  version: '1',
  schema: Schema.Struct({ phase: Schema.String }),
  initial: () => ({ phase: 'gathering' }),
});

const draft = Tool.make('draft', {
  parameters: Schema.Struct({}),
  success: Schema.Struct({ phase: Schema.String }),
  failure: AgentState.Error,
  dependencies: AgentState.dependencies(SupportState),
});

const support = Agent.make({
  // ...
  state: SupportState,
}).withHandlers({
  draft: () =>
    Effect.gen(function* () {
      const state = yield* SupportState;
      return yield* state.update(() => ({ phase: 'drafting' }));
    }),
});
```

`get`, `set`, `update`, and `modify` are typed from the schema. Declaring
`state` on the agent is the complete wiring: ordinary runs get isolated memory,
while recorded runs append a complete, fenced checkpoint before a mutation
returns. Resume restores the latest checkpoint, branches restore the selected
active-path checkpoint, forks copy it with the selected prefix, and child
conversations keep their own state. Concurrent mutations are serialized. A
checkpoint is independent of tool outcomes and external effects; use stable
idempotency keys when those effects need replay safety.

Callers do not select an ephemeral or recorded state layer for ordinary agent
use. The agent opens the handle from the run's lexical session; low-level
`AgentState.open` remains available for custom orchestration. If the codec
requires Effect services, those services remain in `Agent.Requires` and must
be provided by the caller; only the state handle itself is opened by the
agent.

State definition, compatibility, schema, and JSON-boundary failures use the
schema-tagged `AgentState.Error` union: `StateDefinitionError`,
`StateCompatibilityError`, `StateDecodeError`, `StateEncodeError`,
`StateJsonError`, and `DurabilityError`.
Direct state operations expose this error. Declare `failure: AgentState.Error`
when a tool handler lets mutation failures escape; state failures are never
converted to defects.

## Effect Workflow

`AgentWorkflow.make` binds an agent to `effect/unstable/workflow` without
introducing another workflow abstraction:

```ts
import { AgentWorkflow } from '@sunfall/vesper-agent/workflow';
import { Schema } from 'effect';

class RunFailure extends Schema.TaggedError<RunFailure>('my-app/RunFailure')(
  'RunFailure',
  { message: Schema.String },
) {}

const SupportRequest = AgentWorkflow.request({
  submissionId: Schema.String,
});

const supportWorkflow = AgentWorkflow.make(supportAgent, {
  tag: 'SupportAgent',
  payload: SupportRequest,
  idempotencyKey: ({ submissionId }) => submissionId,
  error: RunFailure,
  mapError: (error) => new RunFailure({ message: String(error) }),
});

// Effect-native: execute, poll, interrupt, and resume use whichever
// WorkflowEngine the application provides.
const result = supportWorkflow.workflow.execute({
  submissionId: 'request-1042',
  conversationId: 'customer-17',
  input: 'where is my order?',
});

// Register beside the application's WorkflowEngine, LogStore, model, and
// every service in Agent.Requires<typeof supportAgent>.
const SupportWorkflowLive = supportWorkflow.layer;
```

### Schema-typed workflow input

`AgentWorkflow.request(fields)` keeps the convenient `input: string` form.
Pass an Effect Schema as its second argument when the application's durable
input is richer, then bind it with `makeWithInput`. The projection is
deliberately one-way: Effect already owns the `Prompt.Prompt` codec, while
participant identity and authorization remain application meaning that a
provider prompt cannot reconstruct.

```ts
import { Match, Schema } from 'effect';
import type { Prompt } from 'effect/unstable/ai';

const RoomInput = Schema.TaggedUnion({
  ParticipantMessage: {
    participantId: Schema.String,
    text: Schema.String,
  },
  ModeratorNotice: {
    moderatorId: Schema.String,
    text: Schema.String,
  },
});

// Useful when the application also keeps its own typed room transcript.
const RoomInputJson = Schema.toCodecJson(RoomInput);

const toPrompt = Match.type<typeof RoomInput.Type>().pipe(
  Match.tagsExhaustive({
    ParticipantMessage: ({ participantId, text }): Prompt.RawInput => [
      { role: 'user', content: `[participant:${participantId}] ${text}` },
    ],
    ModeratorNotice: ({ moderatorId, text }): Prompt.RawInput => [
      { role: 'user', content: `[moderator:${moderatorId}] ${text}` },
    ],
  }),
);

const RoomRequest = AgentWorkflow.request(
  { submissionId: Schema.String },
  RoomInput,
);

const roomWorkflow = AgentWorkflow.makeWithInput(roomAgent, {
  tag: 'RoomAgent',
  payload: RoomRequest,
  idempotencyKey: ({ submissionId }) => submissionId,
  input: ({ input }) => toPrompt(input),
  error: RunFailure,
  mapError: (error) => new RunFailure({ message: String(error) }),
});
```

The Workflow payload codec validates and persists the complete application
event. The projection supplies only what the model should see. Prompt labels
are context, not authority: authenticate membership and moderator roles before
executing the workflow.

For concurrent participants, accept and persist submissions in an
application-owned Workflow or Cluster entity keyed by conversation id, then
order or batch them before starting one Vesper run. Producer fencing protects
the conversation from interleaved agent writers; it is intentionally not a
message queue. `conversation.follow()` can serve any number of observers, and
`steer` / `cancel` signals should remain run controls rather than ordinary room
messages.

Durable work inside tools is an ordinary named function:

```ts
const chargeCard = AgentWorkflow.step({
  name: 'charge-card',
  key: ({ orderId }: ChargeInput) => orderId,
  success: ChargeReceipt,
  error: ChargeError,
  execute: ({ customerId, amount }: ChargeInput) =>
    Effect.gen(function* () {
      const payments = yield* Payments;
      const idempotencyKey = yield* AgentWorkflow.idempotencyKey('charge-card');

      return yield* payments.charge({ customerId, amount, idempotencyKey });
    }),
});

const billingAgent = Agent.make({
  // ...
}).withHandlers({
  charge_card: (input) => chargeCard(input),
});
```

`step` is a small constructor for Effect Workflow `Activity`; it does not add
a second replay mechanism. A completed activity result is returned on replay
without running `execute` again. Its mandatory `key` distinguishes logical
calls within one workflow execution, so repeating the same input replays while
different orders execute independently; Vesper escapes the key before joining
it to the step name. Empty keys fail before execution. The Effect requirement
includes `WorkflowInstance`, which prevents a step from compiling as durable
outside an active workflow. `idempotencyKey(name)` derives a stable key for an
external system, but that system must enforce the key: no workflow engine can
make its side effect atomic with recording the activity result.

The payload and error schemas are application-owned because they cross a
durable boundary; arbitrary prompt values and failure causes are not assumed to
be serializable. Vesper's conversation log remains authoritative for model
turns, tool outcomes, compatibility, and prompt reconstruction. The supplied
Effect `WorkflowEngine` remains authoritative for accepted execution,
activities, suspension, interruption, and wakeup. Use
`WorkflowEngine.layerMemory` in tests or `ClusterWorkflowEngine.layer` for a
cluster-backed runtime; Vesper does not wrap either one. For a SQL-backed
single-process deployment, compose `ClusterWorkflowEngine.layer` with Effect's
`SingleRunner.layer` and the application's `SqlClient`/`Crypto` layers. The
runner persists workflow mailboxes, replies, and locks in SQL, but its runner
communication and health services are intentionally no-op: this composition
proves restart/reopen durability for one process, not distributed failover.

### Yielding from a tool handler

`AgentWorkflow.wait` is a typed durable wait for human review, webhooks, jobs,
or any other externally supplied result. Mark tools that use workflow
primitives with `AgentWorkflow.durable`; this keeps the workflow services in
the tool's requirement type instead of hiding them:

```ts
const ApprovalRequest = Schema.Struct({
  orderId: Schema.String,
  amount: Schema.Number,
});
const ApprovalDecision = Schema.TaggedUnion({
  Approved: { actor: Schema.String },
  Denied: { actor: Schema.String, reason: Schema.String },
});
class ChargeDenied extends Schema.TaggedError<ChargeDenied>('ChargeDenied')(
  'ChargeDenied',
  { reason: Schema.String },
) {}

const reviewCharge = AgentWorkflow.wait({
  name: 'review-charge',
  key: ({ orderId }) => orderId,
  request: ApprovalRequest,
  success: ApprovalDecision,
  error: Schema.Never,
});

const chargeCardTool = AgentWorkflow.durable(
  Tool.make('charge_card', {
    description: 'Charge an approved order',
    parameters: Schema.Struct({
      orderId: Schema.String,
      amount: Schema.Number,
    }),
    success: ChargeReceipt,
    failure: ChargeDenied,
  }),
);

const billingAgent = Agent.make({
  // ...
  toolkit: Toolkit.make(chargeCardTool),
}).withHandlers({
  charge_card: (input) =>
    Effect.gen(function* () {
      const decision = yield* reviewCharge(input);
      if (decision._tag === 'Denied') {
        return yield* new ChargeDenied({ reason: decision.reason });
      }
      return yield* chargeCard(input);
    }),
});
```

The wait appends `ToolSuspended` with its encoded request and completion token,
then suspends the owning Effect Workflow. The definition derives the same
stable application key when projecting that request, so an approval service
can wait for one independently keyed instance and complete it:

```ts
const pending =
  yield *
  reviewCharge.awaitPending(
    Conversation.make(billingAgent, conversationId),
    orderId,
  );

yield *
  pending.complete({
    _tag: 'Approved',
    actor: 'alice@example.com',
  });
```

Wait keys follow the same non-empty rule as durable step keys; an empty key is
rejected before a wait token is created. Request and replay-result encoding
failures stay in the typed `AiError` channel (as
`ToolResultEncodingError`) instead of becoming defects. External completion
also validates the success or failure value through its schema before asking
the workflow engine to persist it, so malformed values remain typed
`SchemaError`s.

`awaitPending` returns an Effect for one definition, conversation, and stable
key. It completes immediately when that request is already actionable or waits
on the full durable conversation until it becomes actionable. Internally it
uses log notifications as wakeups and re-projects durable records rather than
treating notification delivery as truth. Re-projecting also means an atomic
append containing a branch or restart is observed as a whole rather than
briefly exposing its superseded token.

The lookup follows the conversation's active branch, decodes requests with the
wait's request schema, and omits resumed, completed, restarted, superseded, and
settled waits. In particular, a fork that copied the source's audit prefix
exposes only its newly issued token. Schema decoding remains in the error and
requirement channels. If `RecordingPolicy.externalRequest` redacts the stored
request, its result must still satisfy this schema for typed discovery to work.

`conversation.waits()` and `conversation.followWaits()` expose the raw wait
lifecycle for auditing and projections. They are not an actionable approval
queue; consumers must fold their complete lifecycle before acting on a token.

`PendingWait.complete` supplies the typed success value; an application-level
denial is normally one case of that value. `PendingWait.fail` supplies the
wait's typed operational error. The definition's `complete(token, value)` and
`fail(token, error)` forms support serialized callbacks where the bound object
cannot cross an HTTP or process boundary. Re-running `awaitPending` returns an
unresolved instance again. Completion is first-write-wins, so duplicate or
concurrent submissions cannot overwrite the accepted result.

One active token per definition, conversation, and key is a checked invariant.
If durable history contains two different active tokens for that identity,
`awaitPending` fails with typed `WaitStateError` rather than choosing one by log
order.

Completion is durable, first-write-wins, and wakes the workflow. Vesper appends
`ToolResumed`, records the schema-encoded decision as `ToolWaitCompleted`,
re-enters the same logical handler, and finally records `ToolOutcome`.
`RecordingPolicy.externalResult` can redact the persisted decision without
changing the live value. Re-entry is workflow replay, not a serialized
JavaScript stack:
ordinary effects before the wait can run again, so external effects belong in
`AgentWorkflow.step` or must enforce `AgentWorkflow.idempotencyKey` themselves.
If replay crashes again, `ToolResumed` leaves the call suspended and safe to
re-enter; a bare `ToolStarted` without `ToolSuspended` remains indeterminate and
still requires `onIndeterminateToolCall`.

A branch or fork cannot silently copy a workflow-owned token. Restart it
explicitly through the binding; the original provider call and parameters are
re-entered under a new durable workflow execution and issue a new token:

```ts
const fork =
  yield *
  billingWorkflow.forkFrom(sourceIdentity, suspended.offset, forkPayload, {
    discard: true,
  });
```

`forkFrom` leaves the source workflow waiting independently. `branchFrom`
interrupts the superseded source workflow first. With `{ discard: true }`,
both return an `AgentWorkflow.Identity`, so the new path can itself be
cancelled, branched, or forked. Low-level `Conversation.branchFrom` and
`Conversation.forkFrom` require `{ pendingWait: 'restart' }`; omitting it
returns a typed `SuspendedConversationError` instead of guessing.

Every root run has hard production defaults for model/turn/token/delegation,
deadline, concurrency, and signal budgets. Set `Definition.runPolicy` to make
application limits stricter or larger; descendants share the root runtime and
cannot reset it by opening another agent loop. `StopCondition`s remain soft.

Recording persists raw values by default. Pass an effectful
`RecordingPolicy.Policy` as the third argument to
`Conversation.make(agent, id, policy)` to redact only
the persisted prompt, tool parameters/results, external wait requests and
results, delivered signals, and rendered failure causes. Its Effect services appear
exactly in `Agent.Requires`.
The separate incoming signal stream is explicitly raw so a resumed run can
deliver the same value; transform signals before `send` when required.

Steers remain turn-boundary input. For recorded runs, a valid cancel in the
next bounded signal page also interrupts an in-flight provider stream when no
real tool or delegation handler has started in that turn. The change feed is
only a wake-up: the boundary drain remains the sole cursor, budget, ordering,
and `SignalReceived` authority. Once dispatch commits at the turn's atomic gate,
cancellation waits for durable tool outcomes and the normal boundary.
Watcher read failures are logged and disable only responsive interruption for
that provider call; cancellation remains available at the boundary. Boundary
drain persistence failures remain fatal so delivery state cannot diverge.

Recorded tool execution writes `ToolStarted` immediately before entering a real
tool or delegation handler. If recovery finds a start without `ToolOutcome`, it
resolves the original recorded name, id, and params before the next model call.
Configure `onIndeterminateToolCall` to explicitly Retry or Answer after
application-specific reconciliation. Either result is durably recorded and the
prompt is rebuilt with it; without that seam, the run fails safely before the
provider is called and remains orphaned.

`maxToolConcurrency` covers the complete pull-based lifetime of each leaf-tool
handler stream, including recovery retries, and is shared by parent and child
loops. Delegation handlers use the separate child limits and never hold a leaf
permit while waiting for a child.

Settled runs write a resume aggregate inside `RunSettled`: compatibility
identity, cumulative physical usage, durable signal cursor, latest completed
result, and latest usable turn usage. A compacted resume pages backwards only
through its live active suffix and compaction boundary; lifetime records remain
solely in the append-only conversation log. Old records remain schema-decodable
so rejection is actionable, but missing compatibility metadata is not silently
self-upgraded. Uncompacted prompts, fork prefix copies, active branch targets
older than the available compaction boundary, and orphan records after the
latest anchor remain proportional to the records they genuinely require.
Existing history is compatibility-validated before producer acquisition, so an
incompatible resume, branch, or fork cannot fence a live compatible session.
The claim is compare-and-acquire bound to the validated epoch and head; a race
re-reads and revalidates in a small bounded retry loop before any epoch bump.

Recovered `ToolOutcome` values and indeterminate-call reconciliation answers
must decode through the current tool result schema. Decode failure is an
actionable `AiError`, never an unknown value presented as typed success.

See the [Vesper repository](https://github.com/sunfall-labs/vesper#readme) for
usage, package status, and the complete API walkthrough.
