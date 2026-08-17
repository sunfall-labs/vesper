# `@sunfall/vesper-agent`

The Effect-first agent loop for Vesper: subagents, skills, stop conditions,
tool-call interception, recording, resumption, compaction, and signals.

Every `Agent.make` definition requires a non-empty `revision`. Recorded runs
persist conversation format, agent name, and revision, and all resumed entry
points reject incompatible or unrevisioned history before model/tool work.
Child sessions validate against the child definition, not the parent revision.
Compatibility failures use the tagged `AgentLog.CompatibilityError` channel.
`Agent.Result.outcome` is `success` or `cancelled`, and `steps` counts model
turns that actually started rather than planned turn boundaries.

```bash
npm install @sunfall/vesper-agent effect@4.0.0-rc.109
```

Modules are exposed as explicit subpaths, including
`@sunfall/vesper-agent/agent`, `/run-policy`, `/recording-policy`, `/stop`,
`/skill`, `/interception`, and `/workflow`.

## Effect Workflow

`AgentWorkflow.make` binds an agent to `effect/unstable/workflow` without
introducing another workflow abstraction:

```ts
import { AgentWorkflow } from '@sunfall/vesper-agent/workflow';
import { Schema } from 'effect';

class RunFailure extends Schema.TaggedError<RunFailure>()('RunFailure', {
  message: Schema.String,
}) {}

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
cluster-backed runtime; Vesper does not wrap either one.

Every root run has hard production defaults for model/turn/token/delegation,
deadline, concurrency, and signal budgets. Set `Definition.runPolicy` to make
application limits stricter or larger; descendants share the root runtime and
cannot reset it by opening another agent loop. `StopCondition`s remain soft.

Recording persists raw values by default. Pass an effectful
`RecordingPolicy.Policy` as the second argument to `recordingTo` to redact only
the persisted prompt, tool parameters/results, delivered signals, and rendered
failure causes. Its Effect services appear exactly in `Agent.Requires`.
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
