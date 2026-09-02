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

`revision` is human-declared identity; nothing checks that it actually
changed when the definition did. `Agent.make` also computes `digest` —
`agent.digest`, read-only — a canonical SHA-256 over the parts of the
compiled definition that affect durable compatibility: the sorted tool names
with each tool's parameter, success, and failure JSON schema (the same
derivation the model boundary uses, `Tool.getJsonSchema`/
`Tool.getJsonSchemaFromSchema`); subagent names and their own digests,
recursively; the skill catalog names; `codeMode`; and
`resultOverflow.threshold`. It deliberately excludes `instructions` (an
application may build it per run), model choice and `runPolicy` (run-time
wiring, not definition shape), tool descriptions (documentation for the
model, not wire shape), and `resultOverflow.preview` (changes what the model
sees, not the pointer shape a resumed run decodes). Recorded runs persist
`digest` beside `revision`; resuming with the same revision but a different
digest — the definition changed and the revision was not bumped — fails with
a typed `Conversation.CompatibilityError` naming both digests. A record from
before this field existed has no digest at all, and that absence is accepted
as compatible rather than rejected: only a same-revision digest that actively
disagrees is a problem. See
[`docs/conversations.md`](../../docs/conversations.md)'s "Compatibility and
revisions" for the full resume-time contract.

```bash
npm install @sunfall/vesper-agent effect@4.0.0-rc.112
```

Node.js 22.13.0 or newer is required. The snippets below assume the application
has already provided Effect's `LanguageModel` service; see the repository
[Quick Start](https://github.com/sunfall-labs/vesper#quick-start) for complete
provider and runtime wiring.

Modules are exposed as explicit subpaths, including
`@sunfall/vesper-agent/agent`, `/conversation`, `/run-policy`,
`/recording-policy`, `/eval`, `/stop`, `/skill`, `/state`, `/interception`, and
`/testing`, plus `/turn-control`, `/workflow`, `/dynamic-toolkit`, and
`/model-plan`.

## Running an agent

`stream` is the primitive and `run` is a fold of it, so a streaming consumer
and a blocking one take the same path through the loop. `streamIn` and `runIn`
are the same two against a `Chat` the caller already holds. A run stops when
its `stopWhen` condition holds; the default is "the model asked for no tools",
and `Stop` composes `maxSteps`, `maxOutputTokens`, `toolCalled`,
`toolSucceeded`, `toolFailed`, `toolCalledTimes`, `any`, and `all`.
`Result.response` is Effect AI's canonical `Prompt.Prompt` for the final turn,
including reasoning and structured tool calls/results; Vesper does not define a
second response protocol. `Result.outcome` is `success`, `cancelled`, or
`suspended` — a tool call durably waiting on an external interaction, covered
in [Tool interactions](#tool-interactions). `steps` counts model turns that actually
started, so a queued cancellation can return zero while an in-flight
cancellation preserves its partial text, usage, and one started turn.

Structured completion and give-up are ordinary Effect AI tools. Define their
parameters and results with `Tool.make`, attach typed handlers as usual, and
stop only after the terminal handler succeeds:

```ts
import { Effect, Schema } from 'effect';
import { Tool, Toolkit } from 'effect/unstable/ai';
import { Agent } from '@sunfall/vesper-agent/agent';
import { Stop } from '@sunfall/vesper-agent/stop';

const submitAnswer = Tool.make('submit_answer', {
  description: 'Submit the final answer when it is ready.',
  parameters: Schema.Struct({ answer: Schema.String }),
  success: Schema.Void,
});

const agent = Agent.make({
  name: 'answerer',
  revision: '1',
  instructions: 'Use submit_answer for the final answer.',
  toolkit: Toolkit.make(submitAnswer),
  stopWhen: Stop.toolSucceeded('submit_answer'),
}).withHandlers({
  submit_answer: () => Effect.void,
});
```

A `give_up` tool is the same pattern with a typed `reason`. Compose it with
`Stop.any`; there is no Vesper-specific finish schema or parser to keep in sync
with Effect AI.

Preliminary tool results remain live stream progress. They are omitted from
`Stop.State.toolResults`, canonical response history, and durable settlement;
the authoritative final result follows through those boundaries exactly once.

A provider finish reason of `length`, `content-filter`, or `error` is an
incomplete finish, not a successful answer. The raw finish and partial text
remain visible on `stream`; `run` fails with `AiError.InvalidOutputError`. A
recorded conversation preserves the partial text for audit and writes failed
settlement without a `Completed` record. Effect AI begins automatic tool
resolution before emitting its deferred finish part, so this check cannot
retract a tool handler that already started.

Handlers attach as a method rather than a `Definition` field, mirroring
`toolkit.toLayer(handlers)` in `effect/unstable/ai`. Calling `withHandlers`
twice replaces the handlers rather than stacking a second set beneath them,
which is also how `intercepting` behaves. Two interceptors that should both
run are joined first with `Interception.compose`, which fixes their order
explicitly — per-seam rules are on its doc comment — and hands `intercepting`
one combined value, so attachment is still a single replace.

## Turn control and follow-ups

`stopWhen` owns termination. `nextTurn` is the smaller seam that may supply one
more Effect AI `Prompt.RawInput` after seeing the complete turn, or select an
Effect `LanguageModel.Service` for later turns. Returning
`TurnControl.keep` preserves the stop decision; returning
`TurnControl.continueWith(...)` continues. The state includes `wouldStop`, so a
policy can act only when the model would otherwise be done.

Application follow-ups use an Effect `Queue`; the inert agent definition does
not hide a mutable message array:

```ts
import { Effect, Queue } from 'effect';
import { Toolkit } from 'effect/unstable/ai';
import { Agent } from '@sunfall/vesper-agent/agent';
import { TurnControl } from '@sunfall/vesper-agent/turn-control';

const program = Effect.gen(function* () {
  const followUps = yield* Queue.bounded<string>(16);
  const agent = Agent.make({
    name: 'reviewer',
    revision: '1',
    instructions: 'Review the requested work.',
    toolkit: Toolkit.make(),
    nextTurn: TurnControl.followUps(followUps),
  });

  yield* Queue.offer(followUps, 'Now check the edge cases.');
  return yield* agent.run('Draft the implementation.');
});
```

By default queued inputs are taken one per turn; pass `{ mode: 'all' }` to
drain the current batch into one prompt. Queue capacity and backpressure remain
ordinary Effect concerns. Dynamic reasoning settings and model selection stay
at Effect's `LanguageModel` provider seam: a custom `nextTurn` policy may return
the service in `continueWith`, but Vesper adds no model registry or parallel
configuration type. Broader prompt transformation belongs in a wrapped Effect
`LanguageModel`, where every AI operation observes the same rule.

## Run policy and budgets

Every root run has hard production defaults for model/turn/token/delegation,
deadline, concurrency, and signal budgets. Set `Definition.runPolicy` to make
application limits stricter or larger; it is the hard boundary and cannot be
overridden mid-run. Its runtime is created once per root run and passed into
every descendant, so delegation cannot reset turn, model-call, token,
deadline, depth, breadth, or concurrent-child accounting by opening another
agent loop.

Stop conditions are soft stops: a pending steer, or a signal backlog a turn
boundary could not fully drain, outranks a positive stop decision for one
more turn — so `Stop.maxSteps(N)` is not a hard ceiling once a conversation
takes signal traffic. `runPolicy.maxTurns` is the ceiling that holds
regardless.

`maxInputTokens` and `maxOutputTokens` are checked after each turn's usage is
known, not before a request is sent — there is no way to ask a provider
whether a turn will fit a budget before making it — so a run can overshoot
either ceiling by up to one turn's usage before the check after it fails the
run. The limits bound cumulative spend; they do not cap any single request.

Requested tool concurrency, including `unbounded`, is clamped to
`maxToolConcurrency`, which covers the complete pull-based lifetime of each
leaf-tool handler stream, including recovery retries, and is shared by parent
and child loops. Delegation handlers use the separate child limits and never
hold a leaf permit while waiting for a child.

### Cost budgets

`runPolicy.maxCostMicrousd` bounds cumulative spend in micro-USD (1e-6 USD),
charged from provider usage after each model call — including compaction —
using `runPolicy.costModel`:

```ts
const agent = Agent.make({
  // ...
  runPolicy: {
    maxCostMicrousd: 500_000, // $0.50
    costModel: {
      inputMicrousdPerMillionTokens: 3_000_000, // $3.00 / million input tokens
      outputMicrousdPerMillionTokens: 15_000_000, // $15.00 / million output tokens
      cachedInputMicrousdPerMillionTokens: 300_000, // $0.30 / million cached tokens
    },
  },
});
```

`costModel` prices input tokens (cached input tokens separately, at
`cachedInputMicrousdPerMillionTokens` when set, or at the ordinary input rate
otherwise) and output tokens per million tokens. Setting `maxCostMicrousd`
without `costModel` fails at `Agent.make` with a typed
`RunPolicy.CostModelRequiredError` — a cost ceiling nobody can price is not a
smaller ceiling, it is one that never fires. Like `maxInputTokens`, cost is
checked after each turn's usage is known, so a run can overshoot by up to one
turn's cost before the check after it fails the run.

Accumulated cost is exposed the same way tokens are: `Stop.Usage.costMicrousd`
rides on `TurnFinished`, `Completed`, and `Agent.Result`, and
`AgentHistory.usageFrom` folds it into the conversation-wide usage projection.
It is present only once a `costModel` is configured — absent, not zero, on a
run or conversation that never priced its usage.

### Exhaustion mode

`runPolicy.onExhaustion` controls what happens once `maxTurns`, `maxModelCalls`,
`maxInputTokens`, `maxOutputTokens`, `maxCostMicrousd`, or `maxDelegatedTasks`
is exhausted. `'fail'`, the default, is today's behavior: the run fails with
`RunPolicy.RunPolicyExhausted`. `'final-answer'` instead lets the run make
exactly one more model call — with no tools available (`toolChoice: 'none'`)
and a short appended instruction that the budget is spent — and settles on
that call's output as an ordinary `outcome: 'success'` result:

```ts
const agent = Agent.make({
  // ...
  runPolicy: { maxModelCalls: 40, onExhaustion: 'final-answer' },
});
```

The settled `Result` (and the live `Completed` event) carries an `exhausted:
{ limit, used, maximum }` field naming the budget that forced the fallback, so
a caller can tell a normal finish from a budget-constrained one. `exhausted`
is not persisted to the durable conversation log — it lives only on the live
event stream and the `Result` from the run that produced it.

The wall-clock deadline, `maxToolConcurrency`'s clamp, and the signal limits
are never eligible for the fallback: a deadline still fails a run outright in
either mode, including the fallback call itself, since that call reuses the
same `remainingMillis` timeout an ordinary turn gets.

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

## Code mode

`codeMode: true` replaces direct tool advertisement with one isolated `exec`
tool. Its description contains a generated TypeScript SDK with the parameter
and result type of every brokered tool. The model writes the body of an async
function in erasable TypeScript; top-level `await` and `return` work, while
imports and syntax that requires transformation do not. The isolated executor
strips types before evaluation; the SDK and standard TypeScript globals are
available, while host-specific APIs are not. Each nested call dispatches
through the same gated toolkit an advertised call would — intercepted and
metered. The `exec` result separates streamed text from structured data as
`{ output, result? }`: `text(...)` appends to `output`, while a top-level
`return` supplies a JSON `result`. An outer execution failure is
`{ code: 'execution_failed', message }`.

Declared tool failures and broker validation failures reject the nested call
with a model-visible `ToolCallError`. Its `code` is `tool_failure`,
`dispatch_failed`, or `approval_required`; `tool` identifies the nested tool,
and `value` preserves a declared failure value when one exists. Scripts can
catch that class and branch without parsing an error string.

Enabling code mode puts
`CodeExecutor.Service` on the agent's requirement channel, so a missing
executor is a compile error like any other missing service. The bundled
executor requires Node.js 22.13.0 or newer for native type stripping, but the
execution substrate is not part of the model-visible contract.

`codeMode: { except: ['release'] }` brokers everything _but_ the named tools,
which stay directly advertised — gated, intercepted, metered, and, when
marked `Tool.setNeedsApproval`, durably approvable exactly as if code mode
were off for them. That is the intended pairing: a broad toolkit behind
`exec` for composition, with the one or two consequential tools kept on the
provider seam where the approval machinery lives. A brokered tool that
requires approval is rejected by `Agent.make` unless it is excepted. A
dynamically resolved approval tool fails closed with `approval_required`
rather than executing. Excepted names are checked against the toolkit at
compile time — a misspelling is a type error, not a tool that never matches.

## Compaction and the context window

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
into the resulting history. The default policy asks for a structured
continuation checkpoint with goal, constraints, progress, decisions, next
steps, and critical context. A summary with an incomplete finish is rejected
before it can replace history.

## Model fallback

Turn Effect's native `ExecutionPlan` into the ordinary `LanguageModel` layer an
agent already consumes:

```ts
import { AnthropicLanguageModel } from '@effect/ai-anthropic';
import { OpenAiLanguageModel } from '@effect/ai-openai';
import { Effect, ExecutionPlan } from 'effect';
import { ModelPlan } from '@sunfall/vesper-agent/model-plan';

const plan = ExecutionPlan.make(
  {
    provide: OpenAiLanguageModel.model('gpt-5.2'),
    attempts: 2,
  },
  {
    provide: AnthropicLanguageModel.model('claude-opus-4-6'),
  },
);

const result = agent
  .run('Ship the release.')
  .pipe(Effect.provide(ModelPlan.layer(plan)));
```

When a plan needs a `while` policy, `ModelPlan.when` gives the native Effect
step an exactly typed `AiError` without an annotation:

```ts
const plan = ExecutionPlan.make(
  {
    provide: OpenAiLanguageModel.model('gpt-5.2'),
    while: ModelPlan.when((error) => error.isRetryable),
  },
  { provide: AnthropicLanguageModel.model('claude-opus-4-6') },
);
```

Omit `while` when a step does not need an error-specific retry policy. The
predicate governs retries under that step; returning `false` ends those retries
without removing later fallback steps.

Provider client layers remain compile-time requirements of the resulting model
layer. Plan predicates receive `AiError.AiError`, so provider and model failures
can retry or fall back without widening tool-handler failures.

The plan wraps each language-model operation, not the whole agent run. A stream
may fall back before its first emitted part; after output becomes visible it
fails instead of splicing another model's response into the stream or repeating
tool work. A non-streaming operation that auto-resolves tools conservatively
does not fall back after an error because the model boundary cannot prove that a
handler has not already run. Typed tool and application failures bypass model
fallback unchanged.

## Dynamic tools

Use `dynamicTools` only for definitions genuinely discovered when a run starts,
such as MCP servers or tenant-specific integrations:

This is an API sketch: `discoverTools()` and `staticTools` stand for
application-owned values. The MCP package provides a complete dynamic source.

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
form one stable snapshot across its model turns. The Provider seam and dispatch
gate receive that same snapshot, so a name outside it cannot execute. Tool-name
and resource-id collisions fail before the first model request. Wrap a
nonessential source with `DynamicToolkit.optional(source, resource)` to continue
without its tools and make that unavailability explicit in the current system
context.

Do not use dynamic discovery for permissions, approvals, feature flags, or
temporary availability. Keep those tool definitions stable and check current
state in the typed handler. Reserve `beforeToolCall` for policy that genuinely
spans multiple tools, such as a tenant-wide denylist or dry-run mode (see
[Interception](#interception)). If an
external state change should proactively reach a recorded conversation, send a
durable `steer`; it is appended at the next turn boundary instead of rewriting
the cacheable prefix.

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

## Tool-result overflow

`resultOverflow` spills a tool result over a byte threshold into an
`AttachmentStore` instead of handing the whole thing to the model. The model
sees a small pointer — an attachment id, byte size, content type, and a head
preview — and a `read_attachment` tool the definition adds automatically to
read the rest back in ranges:

```ts
import { Effect } from 'effect';
import { AttachmentStoreMemory } from '@sunfall/vesper-attachments/layer-memory';

const agent = Agent.make({
  // ...
  resultOverflow: { threshold: 4_096, preview: 500 },
});

const result = agent
  .run('Summarize the build log.')
  .pipe(Effect.provide(AttachmentStoreMemory.layer));
```

Unset — the default — is exactly today's behavior: no extra tool, no extra
service requirement, every result reaches the model as the handler returned
it. Set, `AttachmentStore.Service` joins `Agent.Requires` the same way a
declared tool dependency does, so the application wires a store the same way
it wires `LanguageModel`.

Only the encoded result crosses the threshold check, so this composes with
recording exactly as durable state does: `ToolOutcome.result` stores the
pointer, not the payload, and a resumed conversation is rebuilt from that
pointer. Recovering a call an earlier crashed run had already settled
decodes a spilled result by its pointer shape rather than the tool's own
schema — the one deliberate exception to "recovered `ToolOutcome` values
must decode through the current tool result schema" (see
[Durable conversations](#durable-conversations)).

The attachment itself outlives nothing on its own: `AttachmentStore` has no
delete API, so retention and garbage collection of spilled content are the
application's responsibility, on whatever schedule fits its store.

An MCP server's result still crosses the transport as one buffered message
before `Mcp.make`'s own `maxResultBytes` or this seam ever sees it —
`resultOverflow` keeps an oversized result out of the model's context and the
log, not out of the MCP client's peak memory.

## Tool-result bounds

`resultOverflow` is opt-in and needs an `AttachmentStore` the application
wires. `resultBounds` is the unconditional backstop: a default 64 KiB
per-result byte bound, on for every agent whether or not `resultOverflow` is
configured, so one oversized tool result cannot poison a conversation that
never set up overflow spilling. It needs no extra service and adds no extra
tool.

```ts
import { Agent } from '@sunfall/vesper-agent/agent';

const agent = Agent.make({
  // ...
  resultBounds: { maxBytes: 8_192 }, // default is 65_536 (64 KiB)
});
```

Unset applies the 64 KiB default. Pass `resultBounds: false` to disable
bounding entirely and restore unbounded results — the alternative, a very
large `maxBytes`, works the same way but stays an explicit ceiling rather
than "off".

An excess result is replaced, everywhere it matters, by a small,
schema-encodable truncation envelope:

```ts
{ truncated: true, bytes: 200_000, maxBytes: 65_536, preview: '...' }
```

`bytes` is the encoded result's real UTF-8 size, `maxBytes` is the bound that
tripped, and `preview` is a fixed-length head of the original content —
there is nothing left to read back, unlike a `resultOverflow` pointer.
`ResultBounds.isTruncation` recognizes the shape independent of any tool's
schema, the same way `ResultOverflow.isPointer` does for a spilled pointer,
and recovering a call an earlier crashed run had already truncated decodes
by that shape rather than the tool's own schema, for the same reason
overflow's pointer does (see
[Durable conversations](#durable-conversations)).

When both are configured, `resultOverflow` always spills first: a spilled
result is already a small pointer, so `resultBounds` only ever truncates a
result overflow did not spill — for example when overflow's own threshold is
larger than the bound, or overflow is not configured at all. Preliminary
(intermediate progress) results are bounded the same way as final ones. A
provider-executed tool call never reaches a toolkit's `handle`, so it never
reaches either seam.

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

### Suites and regression compare

`AgentEval.suite` runs a named collection of cases against one agent and
scores each with `AgentEval.evaluate`. A case that fails to run at all — the
agent dies, a scorer throws, a score violates the normalized contract —
becomes a failed entry in the report rather than a failed suite Effect: the
point of a suite is one complete picture of every case in a single pass.
Cases run sequentially by default (`options.concurrency`), because a suite's
model layer is routinely one `ScriptedModel` with a single ordered request
cursor that concurrent cases would race.

The result, `AgentEval.SuiteReport`, is `Schema`-modelled the same way
`Agent.Result` is: applications persist it to a file, a DB, or CI artifact
storage, on whatever schedule fits their own store. Vesper does not persist
it and does not pick a store.

```ts
import { Effect, Schema } from 'effect';
import { AgentEval } from '@sunfall/vesper-agent/eval';
import { ScriptedModel } from '@sunfall/vesper-agent/testing';

const program = Effect.gen(function* () {
  const report = yield* AgentEval.suite(supportAgent, {
    name: 'order-lookup',
    cases: [
      { name: 'known order', input: 'Where is order 1042?' },
      { name: 'unknown order', input: 'Where is order 9999?' },
    ],
    scorers: [
      AgentEval.check('looked up the order', (sample) =>
        AgentEval.toolCalled(sample, 'lookup_order'),
      ),
    ],
  }).pipe(Effect.provide(scriptedForOrders.layer));

  const encoded = Schema.encodeSync(AgentEval.SuiteReport)(report);
  yield* writeReportJson(encoded); // application-owned; Vesper does not persist

  const baseline = yield* readBaselineReport(); // application-owned
  const comparison = AgentEval.compare(baseline, report);
  if (comparison.verdict === 'regressed') {
    return yield* Effect.fail(new Error('eval suite regressed'));
  }
});
```

`AgentEval.compare(baseline, current)` is a pure function over two reports —
no agent, no model layer, no I/O — so CI can commit a baseline `SuiteReport`
and diff every run against it. Each case is classified `new`, `removed`,
`regressed`, `improved`, or `unchanged` by score delta and pass/fail
transition; a case missing from `current` is `removed`, not `regressed`,
because dropped coverage and a case that got worse are different facts. The
overall `verdict` is `regressed` whenever any case is.

Out of scope, on purpose: Vesper does not sample live traffic into cases, does
not ship a built-in LLM-judge scorer (write one with `AgentEval.makeScorer`
against whatever model or rubric the application already trusts), does not
persist reports or baselines, and has no watch mode. A suite is data plus a
runner; scheduling, storage, and judging are the application's.

The same division covers nondeterminism. A live model needs repeated trials
before a score means anything, and a `SuiteReport` is plain data, so repeats
are a fold in application code rather than a framework feature:

```ts
const reports =
  yield *
  Effect.forEach(
    Array.from({ length: 5 }, (_, index) => index),
    () => AgentEval.suite(agent, definition),
  );
const meanOf = (caseName: string) => {
  const scores = reports.map(
    (report) => report.cases.find((one) => one.name === caseName)?.score ?? 0,
  );
  return scores.reduce((sum, score) => sum + score, 0) / scores.length;
};
```

Aggregate however the application judges best — mean, worst-of-N, variance
gates — and feed whichever aggregate matters back through
`AgentEval.compare` by folding the runs into one representative report.

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

## Tool interactions

Approval is the yes/no specialization. `Interaction.approval` is the explicit
Vesper spelling; existing tools using Effect AI's `setNeedsApproval` remain
equivalent and compatible:

```ts
const release = Tool.make('release', {
  parameters: Schema.Struct({ id: Schema.String }),
  success: Schema.Struct({ released: Schema.Boolean }),
}).pipe(Interaction.approval);
```

When the model calls it, `LanguageModel` suspends before the handler is ever
entered and emits a `tool-approval-request` part instead of dispatching. A
recorded run turns that into a durable `ToolSuspended` — the same record
family `AgentWorkflow.wait` uses below — and ends with `Result.outcome ===
'suspended'` and `pendingInteractions`. No
Effect Workflow, no `AgentWorkflow.durable`, and the handler has not run.

Model-emitted arguments cross their schema boundary before approval,
interception, or durable `ToolStarted`. Invalid arguments and unknown tool
names are framework input failures: they are returned to the model as typed
`AiError` tool results regardless of the tool handler's `failureMode`. That
setting controls handler failures; it cannot turn untrusted model output into
an agent-loop failure. Public model events therefore expose tool names as
`string` and parameters as `unknown`; only the toolkit handler receives the
schema-decoded parameter type. Durable replay validates again and settles
malformed records without invoking the handler.

```ts
const result = yield * conversation.run('release r1');
// result.outcome === 'suspended'
// result.pendingInteractions === [
//   { toolCallId, toolName: 'release', kind: 'approval', request: { id: 'r1' } },
// ]

yield * conversation.resolveApproval(toolCallId, 'approve');
// or: conversation.resolveApproval(toolCallId, 'deny', 'not this week')

const after = yield * conversation.run('release r1');
// after.outcome === 'success'
```

`resolveApproval` only records the decision; it does not itself dispatch.
The next `run` (or `stream`) picks it up: approved genuinely dispatches the
handler for the first time, denied settles a refusal `ToolOutcome` — an
encoded `AiError`, the same shape `failureMode: 'return'` already uses for a
framework-level failure — without ever entering the handler. Calling it
again for the same `toolCallId` fails with typed
`Conversation.ApprovalResolutionError` instead of silently applying, or
discarding, a second decision. Asking again before it is resolved (another
`run` call) re-surfaces the same `pendingInteractions` rather than calling the
model with an unanswered tool call.

For typed input rather than authorization, make the external answer the tool
result:

```ts
const question = Interaction.answer(
  Tool.make('question', {
    parameters: Schema.Struct({
      question: Schema.NonEmptyString,
      options: Schema.NullOr(Schema.Array(Schema.NonEmptyString)),
    }),
    success: Schema.Struct({ answer: Schema.NonEmptyString }),
    failureMode: 'return',
  }),
);

const questionAgent = Agent.make({
  // ...
  toolkit: Toolkit.make(question),
}).withHandlers({ question: () => Interaction.unreachable });

yield *
  conversation.resolveInteraction(question, toolCallId, {
    answer: 'the typed response',
  });
yield * conversation.run();
```

`Interaction.answer` brands the tool at the type level, so `resolve` cannot be
called with an ordinary or approval-only tool. It encodes through the tool's
success schema before appending anything. The next run records that encoded
answer as the successful `ToolOutcome` without entering the guard handler.

An unrecorded `agent.run(...)` fails outright with a typed `AiError` instead
of returning a suspension nothing can ever resolve — approval requires a
`Conversation`.

Neither native interaction needs a workflow engine. Reach for
`AgentWorkflow.wait` when a handler has already started and must durably wait
for a webhook, job, or other external event while doing replayable work around
the wait.

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

## Durable conversations

`Conversation.make(agent, id)` binds a definition to durable history: every
run is recorded as it happens, resumes from the log, and can be branched,
forked, steered, and suspended on a durable approval. The operational
semantics — what the log guarantees, and what a reader of it can rely on —
live in the
[durable-conversation guide](https://github.com/sunfall-labs/vesper/blob/main/docs/conversations.md).
The short version:

- Recording persists raw values by default. Pass an effectful
  `RecordingPolicy.Policy` as the third argument to
  `Conversation.make(agent, id, policy)` to redact only the persisted prompt,
  tool parameters/results, external wait requests and results, delivered
  signals, and rendered failure causes. Its Effect services appear exactly in
  `Agent.Requires`. The separate incoming signal stream is explicitly raw so
  a resumed run can deliver the same value; transform signals before `send`
  when required.
- Steers remain turn-boundary input, and a valid cancel can interrupt an
  in-flight provider stream before any real tool or delegation handler has
  started in that turn. The boundary drain remains the sole cursor, budget,
  ordering, and `SignalReceived` authority.
- Recovery never silently re-runs a tool call. A recorded `ToolStarted`
  without a `ToolOutcome` requires the `onIndeterminateToolCall` interceptor
  to explicitly Retry or Answer after application-specific reconciliation,
  and recovered results must decode through the current tool result schema —
  decode failure is an actionable `AiError`, never an unknown value presented
  as typed success.
- Settled runs write a bounded resume aggregate into `RunSettled`, so a
  compacted resume pages backwards only through its live active suffix, and
  existing history is compatibility-validated before producer acquisition.

See the [Vesper repository](https://github.com/sunfall-labs/vesper#readme) for
usage, package status, and the complete API walkthrough.
