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

Vesper packages require Node.js 22 or newer; the bundled agent code executor
requires Node.js 22.13.0 for native type stripping. Effect and its provider
packages are pinned together as one family (`4.0.0-rc.112`) while the
interfaces are release candidates, ensuring one Effect service identity in the
application; [releasing](docs/releasing.md) states the version policy.

## Quick start

```bash
npm install @sunfall/vesper-agent \
            @sunfall/vesper-log \
            @effect/ai-anthropic@4.0.0-rc.112 \
            @effect/platform-node@4.0.0-rc.112 effect@4.0.0-rc.112
```

The following is a complete, in-memory example: one typed tool, one agent,
one unrecorded run, and the same definition bound to durable conversation
history. It makes a handful of real Anthropic requests — the model will call
the tool — so set `ANTHROPIC_API_KEY` before running it. For a
credential-free example, run `nub run example:support-agent` from a checkout
of this repository.

```ts
import { AnthropicClient, AnthropicLanguageModel } from '@effect/ai-anthropic';
import * as NodeHttpClient from '@effect/platform-node/NodeHttpClient';
import * as NodeRuntime from '@effect/platform-node/NodeRuntime';
import * as NodeServices from '@effect/platform-node/NodeServices';
import { Agent } from '@sunfall/vesper-agent/agent';
import { Conversation } from '@sunfall/vesper-agent/conversation';
import { LogStoreMemory } from '@sunfall/vesper-log/layer-memory';
import { Config, Effect, Layer, Schema } from 'effect';
import { Tool, Toolkit } from 'effect/unstable/ai';

const orderStatus = Tool.make('order_status', {
  description: 'Look up the fulfilment status of one order.',
  parameters: Schema.Struct({ orderId: Schema.String }),
  success: Schema.Struct({ status: Schema.String }),
});

const support = Agent.make({
  name: 'support',
  revision: '1',
  instructions: 'Resolve the customer’s request clearly and accurately.',
  toolkit: Toolkit.make(orderStatus),
}).withHandlers({
  // Handlers are ordinary Effects with schema-typed parameters. A tool that
  // declares `dependencies: [SomeService]` puts that service into the
  // agent's requirement channel — running without providing it is a compile
  // error at the call site, not a runtime surprise mid-conversation.
  order_status: ({ orderId }) =>
    Effect.succeed({ status: `${orderId} shipped this morning` }),
});

const program = Effect.gen(function* () {
  // One unrecorded run.
  const answer = yield* support.run('Where is order_1042?');

  // Or bind the same definition to durable conversation history — recorded,
  // resumable, branchable, and suspendable on a human approval.
  const conversation = Conversation.make(support, 'customer-42');
  const durableAnswer = yield* conversation.run('Where is order_1042?');

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
- [Durable conversations](docs/conversations.md)
- [Durability guarantees](docs/guarantees.md)
- [The complete example, walked through](examples/support-agent/README.md)
- [Workspace composition](docs/workspace.md)
- [Conversation migration guide](docs/migrating-to-conversation.md)
- [Contributing and package layering](docs/contributing.md)
- [Release procedure and version policy](docs/releasing.md)
- [Benchmarks and operational probes](benchmarks/README.md)
- [Changelog](CHANGELOG.md) · [Security and support](SECURITY.md)

## Complete example

`examples/support-agent` is a compiled, credential-free story: one typed
agent definition that loads a skill, delegates policy research, invokes
stateful tools whose typed handlers enforce authorization, suspends on a
typed durable approval, performs the refund as an idempotent Workflow
activity, accepts a steer, resumes, and prints the full durable trail — all
against a scripted model and in-memory adapters. The definition and its
mocked world are walked through section by section in
[the example's README](examples/support-agent/README.md).

```bash
nub run example:support-agent
```

## Compile-time guarantees

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

Both are pinned by type-level regression tests in
`packages/agent/test/assertions.test.ts`, written so the source change that
would break the guarantee fails the build rather than a runtime assertion.
The separate `type-tests/` project pins the published provider-layer typings
the same way.

None of it is expressible over an agent API whose `execute` returns a
`Promise`. That is the architectural reason this exists.

## Going deeper

Each topic below is one section of a longer guide; the paragraph is the
summary and the link is the whole story.

**[Running an agent](packages/agent/README.md#running-an-agent).** `stream` is
the primitive and `run` is a fold of it, so a streaming consumer and a
blocking one take the same path through the loop. A run stops when its
`stopWhen` condition holds — `Stop` composes `maxSteps`, `toolCalled`, `any`,
`all`, and result-aware terminal tools through `toolSucceeded`. Follow-ups and
per-turn model selection use the small `nextTurn` seam while prompts, tools,
responses, schemas, and model services remain Effect AI values. `Result.outcome`
is `success`, `cancelled`, or `suspended`. Hard, shared run budgets are separate:
[run policy](packages/agent/README.md#run-policy-and-budgets) is the ceiling
delegation cannot reset.

**[Subagents and skills](packages/agent/README.md#subagents-and-skills).** A
subagent compiles to a `task_<child>` tool on its parent, so delegation
composes through the ordinary toolkit machinery — and the child's service
requirements ride into the parent's. A skill is a value: catalog in the
system prompt, body loaded on demand, prefix cacheable.

**[Code mode](packages/agent/README.md#code-mode).** `codeMode: true` replaces
direct tool advertisement with one isolated `exec` tool carrying a generated
TypeScript SDK; every nested call still dispatches through the same gated
toolkit. Consequential tools can stay directly advertised — and durably
approvable — via `codeMode: { except: [...] }`.

**[Compaction](packages/agent/README.md#compaction-and-the-context-window).**
A reactive trigger retries a too-long request against a model-written summary;
a proactive trigger fires from a token estimate when the caller supplies a
`contextWindow`. Splits are on whole messages, so a tool call is never cut
away from its result. The default summary is a structured continuation
checkpoint, and truncated summaries never replace history.

**[Interception](packages/agent/README.md#interception).** Four named seams —
`beforeTurn`, `beforeModelCall`, `beforeToolCall`, `onIndeterminateToolCall` —
each with a type that admits exactly what it is for. Spans observe; an
interceptor intervenes.

**[Durable conversations](docs/conversations.md).** The conversation log is a
durability mechanism, not an audit trail: recording and redaction, resumption,
settlement and what an orphan looks like, child sessions, signals and
steering, crash recovery for tool calls, and durable tool approvals — with
the invariants a reader of the log can rely on.

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
workflow engine at all, see
[Tool interactions](packages/agent/README.md#tool-interactions).

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

OpenRouter is composed through Effect's native `@effect/ai-openrouter`
provider, which uses OpenRouter's chat contract and preserves its reasoning
metadata across replayed turns. The provider contract test locks down that
replay shape independently of the live smoke test.

## Development

```bash
npx @nubjs/nub@0.7.5 install   # bootstraps the pinned nub, then installs
nub run verify                 # build, then format + lint + typecheck, then tests
```

[`docs/contributing.md`](docs/contributing.md) has the full toolchain, the
individual commands, the test layout, and the layering rules between
packages — the one thing worth reading before changing anything.

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
