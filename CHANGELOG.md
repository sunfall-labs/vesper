# Changelog

Notable changes to Vesper are recorded here. The project is pre-1.0, so every
entry should state compatibility impact and migration guidance when applicable.

## Unreleased

Compatibility for this whole section: logs, SQLite databases, and PostgreSQL
schemas written by earlier versions are not readable. There is no migration
path; start from a fresh store. `ConversationRecord` gained cases and required
fields, and code that exhaustively matches it must handle them.

### Platform

- Moved to stable Effect 4.0.3. The `effect` peer range is `^4.0.3`; install
  `effect` and every `@effect/*` package at the same version. Imports moved
  with Effect from `effect/unstable/*` to `effect/*`. Publishing remains
  blocked on local patches backporting upstream #9008 until an Effect release
  includes it.
- The repository uses Bun as its package manager and script runner
  (`bun install`, `bun run verify`); `bun.lock` replaces `nub.lock`. Packages
  still target and are tested on Node.js.

### Durability

- Each conversation's log is a verifiable hash chain. Every batch stores a
  SHA-256 digest over the previous digest (genesis commits to path and
  identity), the batch's first offset, producer id, epoch and sequence, and its
  canonical bytes, computed under the lock that orders appends.
  `LogStore.meta` reports `chainHead`; `@sunfall/vesper-log/chain` exports
  `LogChain.verify` / `verifyBatches`, returning `Valid` with the head or the
  first broken offset and reason; `AgentLog.verify(conversationId)` wraps it.
  Pin a head outside the database to make truncation and whole-store rewrites
  detectable. Custom adapters implement `readBatches` and `chainHead` (use
  `LogChain.forAppend`). `@sunfall/vesper-log-pg` ships one migration,
  `001-initial.sql`.
- Streamed text and reasoning are committed during a turn as `PartialOutput`
  records once the oldest uncommitted delta is 200 ms old, so a crash
  mid-answer loses at most that window. Tune or disable it with
  `RecordingPolicy.Policy.partialOutput`. `AgentHistory.abortedOutputFrom`
  exposes output a run streamed and never settled; prompt rebuilds ignore it,
  and completed turns still record `Text` exactly once.
- `Conversation.send` accepts an optional `requestId`: a retry with the same
  key and payload is acknowledged against the original signal, and the same
  key with a different payload fails with
  `Conversation.SignalRequestConflictError`. The check holds across restarts
  and processes sharing a store.
- `Replay.safe(tool)` declares a tool idempotent to re-execute, and
  `agent.intercepting(Replay.policy)` resumes a crashed run by retrying
  replay-safe tools and answering every other indeterminate call with an
  `execution-interrupted` failure the model sees. Strict failure on an
  indeterminate start remains the default. `IndeterminateToolCallContext`
  carries the resolved `tool`.
- Code-mode nested tool calls are recorded as `NestedToolCall`,
  `NestedToolStarted`, and `NestedToolOutcome`, keyed by the parent `exec`
  call and the call's ordinal or an explicit `tools.name(input, { key })`.
  Retrying an indeterminate `exec` re-runs the script against the brokered
  toolkit; settled nested calls are served from the log and a
  started-but-unsettled one fails closed with `ToolCallError` code
  `indeterminate`.
- Fixed resume dropping a tool call a truncated turn left running: its
  `TurnFinished` preceded the answer resume recorded, so the prompt rebuild
  split the call from its answer and dropped both. A finished turn now stays
  open for its outstanding results.
- An approved handler is now a started call: the batch that resumes it writes
  `ToolStarted`, so a crash while it runs leaves the call indeterminate and
  resume asks `onIndeterminateToolCall` instead of running the handler again.
- A tool call is served from the log only when the previous, crashed run
  recorded the same name, id, and parameters. Providers reuse ids such as
  `call_0` across turns; a new call sharing an id was answered with the earlier
  call's result instead of running.
- `Conversation.resolveApproval` and `resolveInteraction` now record through
  the conversation's recording policy, so `externalResult` redacts a caller's
  answer like every other persisted value. They previously wrote it unfiltered.
- A conversation branched from `LogOffset.START` stays started over. The run
  after the branched one rebuilt its prompt with the abandoned records on the
  same page as the branch marker; resume now stops at the marker.
- `RunStarted` requires `formatVersion`, `agentRevision`, and the run's
  `budget`; `Completed` and settled resume state require `outcome`.
- The agent runs its own tool calls. The model is asked with
  `disableToolCallResolution` and `toolCallValidation: 'deferred'`, and once
  the response has finished each call is classified exactly once: a call to a
  tool the turn did not offer (decided by `Object.hasOwn`, so `toString` or
  `__proto__` is never mistaken for a tool) gets one failed result carrying a
  `ToolNotFoundError`; a call awaiting approval gets an approval request, asked
  only when its parameters decode; every other call runs through the dispatch
  gate. A call is durably recorded before its handler starts, and a response
  the provider cut off starts none of its calls. Tool spans are siblings of
  `Agent.model` under `Agent.turn`. Approval denials are recorded in Effect's
  native `{ type: 'execution-denied', reason }` shape, and stored results
  decode with `Tool.failureResultSchema`.

### Budgets

- Budget compliance is auditable from the log. `RunStarted.budget` records
  the budget id, owning root conversation, and effective limits, shared by
  every subagent run under that root; `RunSettled.stoppedBy` records the limit,
  use, and ceiling when a budget ends a run. `RunPolicy.audit` and
  `RunPolicy.auditStored` recompute turns, input and output tokens, and
  delegated tasks and report, per budget, whether each ceiling held. Token
  ceilings are checked after a turn, so a stopped run may overshoot by at most
  one in-flight turn per conversation; missing child logs and unsettled runs
  are `unprovable`, never compliant.

### Runtimes and tools

- `@sunfall/vesper-log-sqlite/layer-cloudflare` runs the SQLite log store on a
  Cloudflare Durable Object's `ctx.storage`, with transactions through
  `transactionSync` and Web Crypto for `Crypto`.
- `WorkspaceLocal.sandboxed` confines `run_shell` with an OS sandbox
  (`sandbox-exec` on macOS, `bwrap` on Linux) in `none`, `preferred`, or
  `required` mode: writes only under the workspace root and a private temp
  directory, reads limited to system directories and the root, network off by
  default. `required` without a working backend returns `SandboxRequired` and
  never runs the command. See the workspace README for the threat model.
- Tool-call parameters on `AgentEvents` parts are `deferred`: the
  model-authored value, validated by Toolkit before a handler runs, on a call
  that may name any tool. The part type is
  `Response.StreamPart<Tools, 'deferred'>` plus the agent's own result parts,
  including `AgentEvents.UndeclaredToolResult` for a call to a tool the turn
  did not offer. `ToolParameterValidationError` no longer carries
  `toolParams`.
- Reactive compaction no longer mistakes a throughput limit ("too many tokens
  per minute", rate limits, throttles, quotas) for a context overflow.
- Approval wait tokens are ULIDs drawn from `Crypto`; producer ids are UUIDv7.
  `@sunfall/vesper-log-pg` uses the official `@effect/sql-pg` listener and no
  longer depends on `pg` at runtime. Storage failures name their structured
  `SqlError` reason.

### Earlier in this cycle

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
Effect provider packages must resolve to `4.0.3` or a later `4.x`. This is the first
published version, so there is no package migration; adopters of the earlier
repository API should follow [Migrating to Conversation](docs/migrating-to-conversation.md).
The former public `DynamicToolkit.append` helper is no longer exported;
dynamic sources are supplied on `Agent.make({ dynamicTools })` and composed by
the agent at run start.
