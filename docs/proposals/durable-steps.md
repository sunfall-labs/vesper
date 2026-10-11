# Proposal: one durable-step protocol inside the log

Status: proposed. Nothing here is implemented.

## Recommendation

Keep the conversation log as Vesper's one persistence mechanism, and inside it
replace the per-feature durable record families with **one step protocol**: a
step is opened, may wait, starts, and settles, and one fold decides what a crash
left behind. Tool calls, code-mode nested calls, approvals, and answer
interactions become steps; conversation structure (runs, turns, compaction,
branches, signals, state checkpoints) stays as it is.

Do not adopt pi-durable's task model. It solves the same problem with mutable
task rows, a scheduler, and documents beside the transcript — a second
persistence semantics, which `Design.md` ("Why there is one persistence
mechanism") already argues against, and one built on pi's single-writer
assumption that Vesper's producer fencing exists to avoid.

Phase 0 below — two dispatch bugs this investigation found — is done; the rest
is proposed.

## Why

### What the log holds today

`packages/log/src/record.ts` defines 23 record kinds. About half are step
lifecycles with the same shape — something is intended, an effect starts, an
outcome settles it, and a crash between start and outcome is ambiguous:

| Lifecycle                                               | Records                                                                  |
| ------------------------------------------------------- | ------------------------------------------------------------------------ |
| Top-level tool call                                     | `ToolCall`, `ToolStarted`, `ToolOutcome`                                 |
| Approval / answer interaction / workflow wait on a call | `ToolSuspended`, `ToolWaitCompleted`, `ToolResumed`, `ToolWaitRestarted` |
| Code-mode nested call                                   | `NestedToolCall`, `NestedToolStarted`, `NestedToolOutcome`               |

The rest describe the conversation itself — `RunStarted`, `Text`,
`TurnFinished`, `Completed`, `RunSettled`, `Compacted`, `BranchedFrom`,
`SignalReceived`, `Signal` (in its own stream), `ChildSession`,
`StateCheckpoint`, `CodeStateCheckpoint`, `PartialOutput` — and are not steps.

### The step half is implemented several times, inconsistently

- **Two copies of one fold.** `recovery.ts` folds the step records once at open
  (`fold`, ~185 lines) and again live (`make`'s tracker, ~200 lines), with
  near-identical handlers for the five tool-call record kinds.
- **Two runners for intent → start → outcome.** Top-level calls
  (`dispatch.ts`, ~530 lines across `handle` and `resolveIndeterminate`) and
  nested calls (`code-mode.ts`, ~95 lines) each implement it.
- **Three answers to "is this wait still pending?"** `recovery.ts`,
  `conversation.ts`, and `workflow.ts` each decide it, from different record
  sets.
- **Three crash policies for the same situation.** A started call with no
  outcome needs `onIndeterminateToolCall`'s consent at top level, fails hard
  with no consent hook inside code mode, and is retried automatically when it
  came through a suspension.
- **Eight keyed-idempotency schemes** (name+id, parent+key+digest, wait token,
  `already_resolved`, child-link compare, signal `requestId`, restart
  `priorToken`, fork-seed compare), on top of producer-sequence retries.
- **Six `ToolOutcome` writers**, with the recording sink suppressing recovered
  events so an outcome is not written twice.
- **Adding one record kind** touches the schema, 11 exhaustive matchers the
  compiler finds, and 4 that silently fall through to a default
  (`conversation.ts`, two in `workflow.ts`, and `history.ts`'s
  `NON_SUPERSEDING`); about 230 lines are no-op cases kept only for
  exhaustiveness.

### The inconsistency has already produced a bug

An approved native interaction runs its handler after `ToolResumed`, not after
`ToolStarted` (`dispatch.ts`), and the recovery fold treats `ToolResumed` as a
no-op. A process that dies while an approved handler is running therefore
resumes as "suspended, approved, not yet run" and **runs the handler again
automatically** — with no `onIndeterminateToolCall`, where an ordinary call in
the same position refuses to resume. Reproduced: seeding `ToolSuspended`,
`ToolWaitCompleted(success)`, `ToolResumed` with no outcome, then
`Conversation.run()` with no interceptor, executes the handler and writes a
second `ToolResumed`. Approval-gated tools are exactly the ones whose side
effects matter.

A related rule was stated but not enforced: recovery claimed to serve only what
an earlier run recorded, but dispatch read the live state the current run
updates, and matched calls by name and id alone. Providers reuse ids across
turns (`call_0` on every turn is common), so a second call reusing an id was
answered with the first call's result and its handler never ran. Reproduced
with two `call_0` calls carrying different parameters.

Both follow from the same cause: each path decides "did the effect start?" from
its own record, so one path can forget.

## What pi-durable shows, and what to take

pi-durable commits atomically across immutable transcript entries, task records
replaced on every checkpoint, and incrementally updated documents. Each durable
operation is a task: a tagged-union checkpoint per phase, an `initial`, a
required `abort`, an optional `migrate`; a phase that makes no durable progress
is a fault. A tool task commits its intent and replay policy before executing;
on recovery it reruns only if the stored and current policies are both `safe`,
and otherwise records `interrupted`. Nested calls are child tasks keyed by
position or explicit key. The transcript is derived from entries alone; task
state never reaches the model.

Worth taking:

1. **One state machine for every effect**, so "started without an outcome" has
   one meaning everywhere.
2. **Replay policy stored with the intent** and combined with the current
   policy at recovery (Vesper's `Replay.safe` is the current half).
3. **Keyed child steps** (`parent/key`), reattached on retry — already what
   code mode does, but bespoke.
4. **Small settlement records that the transcript projection reads**, with step
   bookkeeping kept out of the prompt.

Not worth taking: mutable rows, a scheduler that reconciles task trees,
documents as a second state store, abort cascades and `completing` holds, and
hot-reload handover. Vesper's ownership is Effect's: a step runs in a fiber in
the run's scope, and interruption is structured concurrency, not a durable abort
mark.

## Effect Workflow is not this layer

Vesper already composes Effect Workflow (`@sunfall/vesper-agent/workflow`), and
its activities are durable steps too — but for a different job. Activity
results live in the workflow engine's storage, not the conversation log; an
interrupted activity is retried (at-least-once), where an agent's started call
must not be re-run without consent; and an engine is optional infrastructure
that most conversations run without. Workflow remains the durability layer for
an application's own effects around an agent. Steps are the durability layer
for the agent's decisions inside the conversation, so the two share no
storage. The one place they meet is waits, which phase 4 unifies.

## Options

**A. Keep the bespoke records.** No migration. The cost is the duplication
above, and every new durable feature (the next one is likely MCP elicitation or
long-running tools) adds another family, fold, policy hook, and crash rule.

**B. One step protocol inside the log (recommended).** Five step records
replace eleven; one fold and one recovery decision serve every step kind. The
log stays the only mechanism, and fencing, branching, compaction, the hash
chain, and redaction are unchanged in kind.

**C. pi-style tasks.** The most general, and the wrong trade for Vesper: it
reintroduces checkpointed state beside the log, needs a scheduler and code
versioning for task checkpoints, and assumes one writer.

## Design of B

### Identity

A step is identified by `{ kind, key, parent? }` with one canonical encoding.
A top-level tool call is `{ kind: 'tool', key: <provider call id> }`; a nested
call is `{ kind: 'tool', key: <ordinal or explicit key>, parent: <exec step> }`.
The step identity is the idempotency key for everything keyed by a call today.

### Records

| Record          | Carries                                                                 | Replaces                                          |
| --------------- | ----------------------------------------------------------------------- | ------------------------------------------------- |
| `StepOpened`    | id, name, intent (provider-encoded params), replay policy, input digest | `ToolCall`, `NestedToolCall`                      |
| `StepSuspended` | id, wait, token, request, interaction                                   | `ToolSuspended`                                   |
| `StepResolved`  | id, token, decision, result                                             | `ToolWaitCompleted`, `ToolWaitRestarted`          |
| `StepStarted`   | id                                                                      | `ToolStarted`, `NestedToolStarted`, `ToolResumed` |
| `StepSettled`   | id, outcome, encoded result                                             | `ToolOutcome`, `NestedToolOutcome`                |

`StepStarted` is written immediately before every handler runs, however the
step got there — including after an approval. That single rule removes the
double-execution bug by construction.

### One fold

```
Opened ─▶ Started ─▶ Settled
Opened ─▶ Suspended ─▶ Resolved(approve) ─▶ Started ─▶ Settled
                       Resolved(deny|answer) ─▶ Settled
Started, no Settled  ⇒  Indeterminate, for every step kind
```

One pure fold over step records, used both when a session opens and live, with
the run boundary as an explicit input so "a run never serves itself" is a
property of the fold rather than a comment.

### One recovery decision

| State         | Action                                                                                                |
| ------------- | ----------------------------------------------------------------------------------------------------- |
| Settled       | serve the stored result                                                                               |
| Indeterminate | stored policy `safe` and current `safe` → retry; else `onIndeterminateToolCall`; else fail the resume |
| Suspended     | wait for resolution (unchanged)                                                                       |
| Resolved      | approve → start; deny or answer → settle                                                              |
| Opened only   | the effect never started → run it                                                                     |

Nested calls get `onIndeterminateToolCall` like any other step instead of a
hard failure.

### Unchanged

The model sees exactly what it sees today: the history projection reads
top-level `StepOpened` and `StepSettled` of kind `tool` and emits the same
`tool-call` and `tool-result` parts, with the same pairing rule. Redaction
becomes one hook per step phase (intent, request, result) keyed by step kind,
replacing the per-record hooks and the synthesized records nested calls use
today.

### Stays bespoke

Runs and turns (`RunStarted`, `Text`, `TurnFinished`, `Completed`,
`RunSettled.resume`), `Compacted` and `BranchedFrom` (they re-root offsets and
are read by backward walks), `Signal` (a stream outside run fencing),
`SignalReceived`, `ChildSession` (two streams, derived id), `StateCheckpoint`
and `CodeStateCheckpoint` (snapshots, not steps), and `PartialOutput`
(supersede-or-abort progress). Workflow waits keep Effect Workflow's
`DurableDeferred` token and replay-safe re-entry; they fit as a wait kind with
an implicit `safe` policy, but only after the native paths have moved.

## Invariants it must keep

- A call is durable before it starts; a start is durable before its handler.
- Batches that must be atomic stay atomic (resume plus outcome; branch plus
  restart).
- A call and its result reach the prompt together or not at all, including an
  outcome recorded after its turn finished.
- Recovery reads only the orphan suffix after the last `RunSettled`.
- One producer per stream; resolutions outside a run fence a live writer as
  they do now.
- Active-path scoping, fork reseating of offsets, and `firstKept` boundaries.
- Redaction runs before tracking and projection, so recovered results are the
  redacted ones.
- Hash-chain coverage, byte-identical retries, the budget audit's inputs.

## Plan

No backwards compatibility is required, so each phase replaces rather than
adapts.

0. **Done.** An approved handler writes `ToolStarted` in the batch that resumes
   it, so a crash while it runs is indeterminate like any other started call.
   Dispatch serves a call from the log only when the previous run recorded the
   same name, id, and parameters (`Tracker.prior`), so reused provider ids run.
   Both have scenarios that fail without the fix.
1. **Done.** One `StepFold` in `recovery.ts` applies every record, both when a
   session opens and as the run appends. The open-time snapshot is the only
   source of "what did the previous run do", so serving from the log cannot
   reach the current run's calls by construction.
2. **Steps for nested calls and native interactions.** Introduce the five step
   records; route code mode and native approvals/answers through one step
   runner shared with `dispatch.ts`. Nested calls gain the indeterminate policy.
3. **Top-level tool calls.** Move `dispatch.ts`, the recording sink, and the
   history projection onto steps; delete the `Tool*` and `NestedTool*` kinds
   and their policy hooks.
4. **Optional: workflow waits** as a step wait kind, retiring the third
   "pending wait" computation in `workflow.ts`.

Phases 0 and 1 stand on their own value. Proceed to 2 only if 1 shows the fold
generalizes cleanly; 3 is the large one and should follow a working 2.

## Costs and risks

- Phase 3 touches the most delicate code in the repository (dispatch ordering,
  the sink, the prompt projection). The scenario suite covers these paths
  end to end, which is what makes the migration reviewable.
- A generic record is less self-describing in a raw log dump than `ToolOutcome`.
  Step kinds and names keep it readable.
- Per-kind differences do not vanish: keys, policies, and redaction still vary
  by kind. The protocol makes them parameters of one machine rather than
  separate machines; it does not remove them.

## Open questions

- Should `CodeStateCheckpoint` become part of the `exec` step's settlement, so
  a crash cannot advance code state while `exec` is still indeterminate (today
  it can)?
- Should replay policy be recorded per step (as pi does) or derived from the
  current tool annotation at recovery (as today)? Recording it protects against
  a tool's policy changing between crash and resume.
- Does delegation become a step whose settlement is the child's `Completed`,
  retiring `ChildSession`'s parent-side copy?
