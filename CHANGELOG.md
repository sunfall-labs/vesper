# Changelog

Notable changes to Vesper are recorded here. The project is pre-1.0, so every
entry should state compatibility impact and migration guidance when applicable.

## Unreleased

- Added [`docs/guarantees.md`](docs/guarantees.md), a numbered, cited contract
  for the durable conversation log and recovery: append atomicity and
  idempotent replay by fingerprint, producer epoch fencing, the fixed-attempt
  session claim, the authoritative/advisory split between `ToolOutcome` and
  tool advertisement, the indeterminate crash window between `ToolStarted`
  and `ToolOutcome`, at-least-once external effects, durable approvals and
  typed answers, append-only compaction, fork offset reseating, per-backend
  change notification, read limits, and what is explicitly not promised. Each
  item states its guarantee and matching non-guarantee against file and line,
  derived from `packages/log`, `packages/log-sqlite`, `packages/log-pg`, and
  `packages/agent` rather than from prose docs; a Discrepancies section notes
  one place `docs/conversations.md` and the code disagree (append failures
  are a typed `DurabilityError`, not the defect the guide describes).
  Compatibility: documentation only, no code changed.

- Provider finishes explicitly marked `length`, `content-filter`, or `error`
  no longer settle an agent run as a successful answer. The raw finish and
  partial text remain stream-visible; blocking runs fail with
  `AiError.InvalidOutputError`, and recorded runs preserve the partial text but
  write failed settlement without a `Completed` record. Compaction rejects the
  same incomplete finishes before replacing history, and its default prompt
  now produces a structured continuation checkpoint covering goal,
  constraints, progress, decisions, next steps, and critical context.
  Compatibility: callers that treated token-capped or filtered fragments as
  successful results must now handle the typed failure or raise the provider's
  output limit. Custom compaction instructions remain supported unchanged.

- Split the two largest agent source files along their internal seams, with
  no public-API or behavior change. `agent.ts` (3,207 lines) keeps the
  `Agent.*` surface, `make`'s compile step, and the method attachment; the
  turn loop moved to `internal/loop.ts`, part encoding to
  `internal/part-encoding.ts`, and provider-error normalization to
  `internal/provider-error.ts`. `log.ts` (1,861 lines) keeps the `AgentLog.*`
  surface — `Session`, `open`, `fork`, and the stream-addressing re-exports —
  while the claim machinery moved to `internal/session-open.ts`, bounded
  history reads to `internal/resume-read.ts`, compatibility validation to
  `internal/compatibility.ts`, and fork identity and prefix seeding to
  `internal/fork-seed.ts`. Every durable string, span name, and rationale
  comment moved verbatim; the `Agent` and `AgentLog` namespaces are
  unchanged. No behavior changed.

- Restructured the documentation for progressive disclosure. The root README
  is now the front door — positioning, quick start, packages, compile-time
  guarantees, and maturity — with each deep topic summarized under "Going
  deeper" and linked to its one canonical home. The agent guide
  (`packages/agent/README.md`) gained the run semantics, run-policy, subagent
  and skill, code-mode, compaction, and interception sections that previously
  lived in the root README, and its previously unheaded durability paragraphs
  now live under proper headings there or in the new
  [`docs/conversations.md`](docs/conversations.md), the operational guide to
  the durable conversation log (recording, resumption, settlement, signals,
  crash recovery, approvals). The complete support-agent walkthrough moved
  beside its source as `examples/support-agent/README.md`. No behavior
  changed.

- Added `@sunfall/vesper-agent/model-plan`, a typed adapter from Effect's
  native `ExecutionPlan` to the ordinary `LanguageModel` layer consumed by an
  agent. Plans preserve provider-layer requirements, retry and fall back only
  for `AiError`, refuse to splice a fallback into a partially emitted stream,
  and conservatively avoid replaying non-streaming operations after automatic
  tool resolution. The provider-contract example pins retry-then-fallback
  behavior across the official Anthropic and OpenAI adapters, and live smoke
  accepts an optional fallback provider. Additive: existing model layers and
  agent APIs are unchanged.

- `AgentEval.suite` now rejects an empty case list at construction with a
  `RangeError` instead of returning a vacuously passing report (zero
  failures, `meanScore: 1`). A suite that measures nothing should fail
  loudly, not green-light CI. Documented the user-space repeat-N pattern for
  live-model trials in the agent guide.

- Code mode now presents a generated TypeScript SDK with typed tool parameters
  and results, and the bundled isolated executor accepts erasable TypeScript
  rather than describing its source as JavaScript. Syntax requiring
  transformation, imports, and undeclared runtime globals remain unavailable.
  Compatibility: `@sunfall/vesper-agent` and the workspace now require Node.js
  22.13.0 or newer for native type stripping. `exec` now returns
  `{ output, result? }`: `text(...)` contributes output and top-level `return`
  contributes a structured JSON result. Nested failures are catchable as
  `ToolCallError`, with a stable `code`, tool name, message, and preserved
  declared failure value; outer failures return
  `{ code: 'execution_failed', message }`. Compatibility: custom executors
  must accept the documented erasable-TypeScript source contract, forward the
  optional `Completion.result`, and use the structured failure branch of
  `ToolResponse`.

- Added an eval suite runner and regression compare to `@sunfall/vesper-agent/eval`:
  `AgentEval.suite({ name, cases, scorers, options })` runs a named
  collection of cases against one agent, scoring each with the existing
  `AgentEval.evaluate`. A case that fails to run at all — the agent dies, a
  scorer throws, a score violates the normalized contract — is a failed
  entry in the report rather than a failed suite; cases run sequentially by
  default (`options.concurrency`) because a suite's model layer is
  routinely one `ScriptedModel` with a single ordered request cursor.
  `AgentEval.SuiteReport` is `Schema`-modelled, matching `Agent.Result`, so
  applications encode/decode it to whatever they persist (a file, a DB, CI
  artifact storage); Vesper does not persist it. `AgentEval.compare(baseline,
current)` is a pure function over two reports that classifies every case
  `new`, `removed`, `regressed`, `improved`, or `unchanged` and returns an
  overall verdict, for a CI pipeline to diff against a committed baseline.
  No LLM-judge is included; write one with the existing `AgentEval.makeScorer`.
  Purely additive: no existing export changed shape.
- `codeMode` accepts `{ except: [...] }`: broker every tool behind `exec`
  except the named ones, which stay directly advertised — gated,
  intercepted, metered, and durably approvable via `Tool.setNeedsApproval`
  exactly as if code mode were off for them. Names are compile-time-checked
  against the toolkit for literal arrays and rejected at construction
  otherwise. Approval-gated toolkit tools must be excepted; `Agent.make`
  rejects a configuration that would broker one, while dynamically resolved
  approval tools fail closed with `approval_required`. A toolkit tool named
  `exec` is rejected at construction whenever code mode is enabled, instead
  of being silently shadowed by the generated `exec` tool.

- Added durable tool approvals without `WorkflowEngine`. A tool marked with
  Effect's own `Tool.setNeedsApproval` now suspends durably in a recorded
  conversation: the run ends with `Result.outcome: 'suspended'` and
  `pendingApprovals`, `Conversation.resolveApproval(callId, decision)`
  records the decision, and the next run dispatches (approved) or settles a
  refusal-style result without entering the handler (denied). An undecided
  approval can never dispatch, and resolving the same call twice is a typed
  `ApprovalResolutionError`. Unrecorded `agent.run` fails outright for a
  `needsApproval` tool. Additive: `Result.outcome` is a strictly wider
  union, and no existing record, method, or export changed shape.
  `AgentWorkflow.wait` is unchanged and remains the path for arbitrary
  durable waits inside handlers. See
  [Tool approval](packages/agent/README.md#tool-approval).
- `Conversation.run` and `Conversation.stream` now accept no input, meaning
  "continue from durable state without appending a user message" — the shape
  a suspended run resumes with after `resolveApproval`. Additive.
- Added opt-in tool-result overflow: `Agent.Definition.resultOverflow`
  spills an oversized tool result into the `AttachmentStore` and hands the
  model a small pointer plus a ranged `read_attachment` tool. Unset, nothing
  changes; `@sunfall/vesper-agent` now depends on
  `@sunfall/vesper-attachments`.
- Added MCP tool-drift detection: `toolDrift: { fingerprints, onDrift }` on
  an MCP source pins SHA-256 fingerprints of each tool's rendered
  name/description/schema; a drifted tool is excluded (`'reject'`, the
  default) or kept with a logged `ToolDriftError` (`'warn'`). Obtain pins
  with `Mcp.fingerprints`. Vesper does not persist pins itself.
- Added `Interception.compose(first, second)`: joins two interceptors into
  one with documented per-seam ordering; `intercepting` still replaces
  rather than stacks.
- Added `Stop.toolCalledTimes(name, times)`: stop once a named tool has been
  called `times` times in total across the whole run, rather than once per
  turn. Additive; existing stop conditions are unchanged. The name is checked
  against the toolkit at compile time, the same as `Stop.toolCalled`.
- A compaction policy configured without `contextWindow` now logs a one-time
  warning per run that proactive compaction is inactive (reactive compaction
  still fires). No behavior changed beyond the log line.
- Documented that a pending steer, or a signal backlog a turn boundary could
  not fully drain, outranks a positive stop decision for one more turn — so
  `Stop.maxSteps(N)` is not a hard ceiling once a conversation takes signal
  traffic; `RunPolicy.maxTurns` is. Also documented that `maxInputTokens`/
  `maxOutputTokens` bound cumulative spend after each turn rather than
  capping any single request. No behavior changed.

## 0.1.0-alpha.1 - 2026-08-18

First public alpha of the six-package Vesper framework:

- Effect-native agent loops with typed tools, dynamic run-start tool sources,
  subagents, skills, interception, state, hard run budgets, and observability.
- Durable conversations with resumption, branching, steering, compaction,
  recorded tool outcomes, and keyed human approval waits that re-enter the
  original handler.
- Memory and PostgreSQL append-only log stores, content-addressed attachment
  stores, an explicitly authorized workspace toolkit, and namespaced MCP
  consumption.
- Bounded MCP model-facing metadata/results, log batches, signals, and
  workspace writes, with typed failures at each public boundary.
- Packed-artifact import and declaration checks, Node 22/24 CI, production
  dependency auditing, and provenance-ready npm publishing.

Compatibility: Node.js 22 or newer is required. `effect` and the official
Effect provider packages must resolve to `4.0.0-rc.112`. This is the first
published version, so there is no package migration; adopters of the earlier
repository API should follow [Migrating to Conversation](docs/migrating-to-conversation.md).
The former public `DynamicToolkit.append` helper is no longer exported;
dynamic sources are supplied on `Agent.make({ dynamicTools })` and composed by
the agent at run start.
