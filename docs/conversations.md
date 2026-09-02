# Durable conversations

The [repository README](../README.md) is the front door; this guide is what
the conversation log durably guarantees: how runs are recorded, resumed,
branched, steered, recovered, and approved, and what a reader of the log
should expect to find when something goes wrong. Configuration that lives on
an agent definition — budgets, compaction, interception, approvals API — is
in the [agent guide](../packages/agent/README.md). The `supportAgent` used
below is the one built in the
[complete example](../examples/support-agent/README.md). For the same
guarantees stated as a numbered, cited contract against the store and agent
source rather than as narrative, see
[Durability guarantees](guarantees.md).

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

## Resuming a conversation

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

## Compatibility and revisions

Every `Agent.make` definition requires a non-empty application revision. Vesper
persists that revision, the agent name, and its conversation-format version in
`RunStarted` and bounded resume aggregates. Resume, branch, fork, and child
session entry reject missing, unsupported, or mismatched metadata before a
model or tool call. Bump the revision whenever instructions, tools, schemas, or
other behavior make existing durable history unsafe; old unrevisioned history
must be migrated explicitly rather than adopted silently.

Existing history is compatibility-validated before producer acquisition, so an
incompatible resume, branch, or fork cannot fence a live compatible session.
The claim is compare-and-acquire bound to the validated epoch and head; a race
re-reads and revalidates in a small bounded retry loop before any epoch bump.
Old records remain schema-decodable so rejection is actionable, but missing
compatibility metadata is not silently self-upgraded.
[Design.md](../Design.md) has the reasoning behind the explicit compatibility
identity.

## Settlement, and what an orphan looks like

Every run ends with a `RunSettled` record — `success`, `failure`, `cancelled`,
or `interrupted`, with the cause rendered. It is written from a finalizer, so
the ways a run ends without a `Completed` event are written down too: an
interrupted fiber, a consumer that took three events and walked away.

It is the one write here that is swallowed rather than turned into a defect. By
the time it runs there is nobody left to fail to, and a defect would replace
whatever actually went wrong with a complaint about the log. What a failed
settle leaves behind is a `RunStarted` with no `RunSettled`, which is exactly
the orphan shape a reader is told to look for. The absence is the signal.

### The resume aggregate

Settled runs write a resume aggregate inside `RunSettled`: compatibility
identity, cumulative physical usage, durable signal cursor, latest completed
result, and latest usable turn usage. A compacted resume pages backwards only
through its live active suffix and compaction boundary; lifetime records remain
solely in the append-only conversation log. Uncompacted prompts, fork prefix
copies, active branch targets older than the available compaction boundary,
and orphan records after the latest anchor remain proportional to the records
they genuinely require.

## Child sessions

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

## Signals

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
normal boundary. The change feed is only a wake-up: the boundary drain remains
the sole cursor, budget, ordering, and `SignalReceived` authority. Watcher read
failures are logged and disable only responsive interruption for that provider
call; cancellation remains available at the boundary. Boundary drain
persistence failures remain fatal so delivery state cannot diverge.
The run settles as `cancelled` — cancellation ends a run, it
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

## Resuming a tool call after a crash

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

How recovery interacts with an attached interceptor — including the rule that
an interceptor cannot revoke permission for a tool call a crashed run already
completed — is covered in the agent guide's
[When the log and an interceptor disagree](../packages/agent/README.md#when-the-log-and-an-interceptor-disagree).

## Tool interactions

An `Interaction.approval` tool (or an implicit approval marked directly with
Effect AI's `Tool.setNeedsApproval`) is
suspended by `LanguageModel` before its handler is ever entered — that
primitive is upstream, not Vesper's. Vesper's half is making the suspension
durable: in a recorded conversation the run ends with `outcome: 'suspended'`
and surfaces `pendingInteractions` (kind, tool name, call id, decoded request), the
suspension is recorded with the same `ToolSuspended`/`ToolWaitCompleted`
family every durable wait uses, and `resolveApproval` records the decision —
from this process or any other holding the same log. The API walkthrough,
with the approve/deny code, is the agent guide's
[Tool interactions](../packages/agent/README.md#tool-interactions) section.

An approved call dispatches its handler for the first time on the next run; a
denied call settles a refusal-style tool result without the handler ever
running, and the model reacts to that the way it reacts to any returned tool
failure. An undecided approval can never dispatch: a later `run` re-surfaces
the same `suspended` result until a decision lands, a crash before the
decision leaves exactly the recovery-index orphan described above, and
resolving the same call twice is a typed `ApprovalResolutionError`. Unrecorded
`agent.run` fails outright for a `needsApproval` tool — there is nowhere
durable to record the decision such a run would wait on.

`Interaction.answer` uses the same pre-handler suspension, but its schema-typed
external response becomes the successful tool result instead of authorizing a
handler. Both modes recover solely from the conversation log.

`AgentWorkflow.wait` remains the tool for what it was built for — a handler
that must durably wait for an arbitrary external event, with `WorkflowEngine`
replay around it. The
[Durable approval locally](../README.md#durable-approval-locally) example
demonstrates the `AgentWorkflow`-backed alternative.
