# Durability guarantees

A numbered contract for what Vesper's conversation log and recovery actually
promise, and what they deliberately do not. Each item cites the file and line
that implements or enforces it, as of this writing — read the cited code, not
this summary, when the two disagree. [`docs/conversations.md`](conversations.md)
is the narrative guide to the same mechanism; this page is its spec-shaped
complement, derived from `packages/log`, `packages/log-sqlite`,
`packages/log-pg`, and `packages/agent` rather than from either doc.

Ids are stable identifiers for cross-referencing, not a promise of ordering
between them.

## VSP-001 — Append atomicity

**Guarantee.** One `append` call writes its whole record batch or none of it.
The Postgres backend wraps the insert and the stream's counter advance in one
transaction; the SQLite backend does the same with `client.withTransaction`.
A rejected append (fenced, gap, encoding failure) leaves the log exactly as it
was before the call.
Sources: `packages/log/src/log-store.ts:304-317` (interface contract),
`packages/log-pg/src/layer.ts:420-543` (transaction with `WITH` CTEs),
`packages/log-sqlite/src/layer.ts:393-493` (`client.withTransaction`).

**Non-guarantee.** Atomicity covers one `append` call, not a run's several
appends across a turn. A crash between two separate `append` calls (for
instance between recording `ToolStarted` and `ToolOutcome`) leaves the first
durable and the second never sent — see VSP-006. Nothing here makes a whole
turn, or a whole run, transactional.

## VSP-002 — Idempotent replay by fingerprint

**Guarantee.** A retried append — same producer, same epoch, same sequence,
same encoded batch — succeeds without writing and returns the original last
offset. Sameness is decided by a digest (`fingerprint`) of the encoded batch,
not by a proxy such as record count, so a producer that crashed after writing
but before hearing the answer converges on replay instead of duplicating.
Sources: `packages/log/src/adapter.ts:138-149` (`decide`, the retry branch),
`packages/log/src/log-store.ts:304-317` (documents the retry contract).

**Non-guarantee.** A sequence reused with _different_ content is not treated
as a retry — it fails `conflict` rather than silently keeping the earlier
offset and dropping the new records. Idempotency is keyed to the exact
producer/epoch/sequence/content tuple, not to application-level intent; two
logically-equivalent-but-differently-encoded batches are not deduplicated.
Source: `packages/log/src/adapter.ts:138-149`.

## VSP-003 — Producer epoch fencing

**Guarantee.** A stream has exactly one writer at a time. `acquire` bumps the
stream's epoch and resets the producer sequence; every `append` must carry the
epoch it was handed. An append whose epoch does not match the stream's current
epoch fails with reason `fenced` — the producer has been superseded by a later
claim.
Sources: `packages/log/src/adapter.ts:111-124` (`decide`, the epoch check),
`packages/log/src/log-store.ts:64-73` (`fenced` reason documented),
`packages/log/src/log-store.ts:290-302` (`acquire` interface doc).

**Non-guarantee.** A fenced writer is not told to stop by any live signal —
Effect Workflow suspension, an interrupt, a callback — it only discovers
fencing the next time it tries to `append` and gets back a typed `fenced`
`LogStoreError`. Any work that writer did between being fenced and its next
append attempt (a tool handler already running, in particular) is not
retracted; VSP-006 covers what that leaves behind for recovery.

## VSP-004 — Session claim: compare-and-acquire, fixed attempt count

**Guarantee.** Opening a session validates compatibility and claims the
producer as one compare-and-acquire: `acquire` is called with the currently
observed `{ epoch, head }`, and a concurrent compatible writer that changed
the stream between the read and the claim causes a retry from the new
position — up to a fixed `ACQUIRE_ATTEMPTS = 4` attempts — rather than an
unbounded spin.
Source: `packages/agent/src/internal/session-open.ts:93,169-229`
(`ACQUIRE_ATTEMPTS` and the retry loop).

**Non-guarantee.** Exhausting the 4 attempts is not silently swallowed or
retried further — it fails with the last observed `conflict`, or dies if no
conflict was ever observed (a bug, not a contention case). A caller that
wants more resilience under heavy concurrent-open contention on one
conversation id must retry `open` itself; nothing inside `openWith` will.
Source: `packages/agent/src/internal/session-open.ts:225-229`.

## VSP-005 — Authoritative vs. advisory

**Guarantee.** A `ToolOutcome` record is authoritative: once a call is
`Settled`, recovery serves that outcome from the log without consulting the
interceptor, and "what the log says happened is what happened." Tool
_advertisement_ (the names/descriptions/schemas shown to the model) is
advisory only — it helps the model plan and grants no execution authority by
itself.
Sources: `packages/agent/src/dispatch.ts:174-200` (ordering comment, "an
interceptor cannot revoke permission for a tool call a crashed run already
completed"), `packages/agent/src/dispatch.ts:871-882` (`Settled` served
without interception), `packages/agent/README.md:1074-1080` ("Removing a
definition from Tool advertisement is not a security mechanism").

**Non-guarantee.** The reverse is not symmetric protection: a live
`beforeToolCall` `Answer` decision (`packages/agent/src/dispatch.ts:924-943`)
flows through the ordinary event stream and is recorded by the normal
recording path as an unremarkable `ToolOutcome` — only a `resultSource:
'substituted'` discriminant distinguishes it for a _live_ consumer
(`packages/agent/src/dispatch.ts:838-851`), and that discriminant is not
persisted. A recovery-time `Answer` decision is even more direct: it is
appended as `ToolOutcome` straight from `resolveIndeterminate`
(`packages/agent/src/dispatch.ts:379-401`). Either way, once written it is
just as authoritative as a real handler's result on the next replay — there
is no separate "this was substituted" trust tier a later reconciliation can
distinguish from a genuine handler outcome.

## VSP-006 — The crash window between `ToolStarted` and `ToolOutcome`

**Guarantee.** `ToolStarted` is appended and durable _before_ a handler is
entered; `ToolOutcome` is appended once it settles. A run that crashes
between the two leaves an **indeterminate** call: recovery's fold sees a
`ToolStarted` with no matching `ToolOutcome` and refuses to guess. It is
never re-dispatched automatically — only a dedicated
`onIndeterminateToolCall` interceptor may explicitly resolve it, returning
`Interception.retry` (re-enter the handler), `Interception.reconcile(result)`
(settle as success without re-entering it), or
`Interception.reconcileFailure(result)` (settle as failure without
re-entering it); absent that, the run fails safely (`indeterminateError`) and
the call stays orphaned until it is.
Sources: `packages/agent/src/dispatch.ts:945-978` (`ToolStarted` written
immediately before the real handler runs), `packages/agent/src/recovery.ts:127-133`
(`ToolStarted` sets `Indeterminate`), `packages/agent/src/dispatch.ts:230-264`
(`resolveIndeterminate`, the explicit-resolution gate), `packages/agent/src/dispatch.ts:1052-1063`
(`indeterminateError`), `packages/agent/src/interception.ts:282-308`
(`IndeterminateToolDecision`, `retry`, `reconcile`, `reconcileFailure`).

**Non-guarantee.** This narrows the crash window to "the handler may or may
not have committed" — it does not make the external side effect atomic with
the log, and it does not tell the application which of the two happened. That
determination is the application's, made against its own external system.
Source: `Design.md:297-304` ("`ToolStarted` narrows crash recovery to the
honest distributed-systems limit; it does not make an external side effect
atomic with the conversation log... There is no broad workflow engine or
implicit retry policy here").

## VSP-007 — External side effects are at least once; provider cost may repeat

**Guarantee.** Nothing in the log or recovery machinery de-duplicates an
external effect a tool handler performs, or a provider call a retried turn
makes. A `Retry` decision on an indeterminate call genuinely re-enters the
handler; `AgentWorkflow.step`'s durable replay depends on the _application_
enforcing `idempotencyKey`, not on Vesper.
Sources: `packages/agent/README.md:855-864` (`idempotencyKey(name)` "derives
a stable key for an external system, but that system must enforce the key: no
workflow engine can make its side effect atomic with recording the activity
result"), `Design.md:299-304`.

**Non-guarantee.** Compaction and ordinary resumption _do_ avoid repeating
provider spend for turns that already completed — `resume.test.ts` is cited
in `docs/conversations.md` as pinning this as a numeric assertion — but that
guarantee is scoped to turns the log recorded as finished, not to a turn or
tool call caught mid-flight by a crash, which is exactly VSP-006's window.

## VSP-008 — Approvals and typed answers survive restarts; double resolution is a typed error

**Guarantee.** A durable approval (`Interaction.approval`) or typed answer
(`Interaction.answer`) suspends before any handler runs, recorded as
`ToolSuspended`/`ToolWaitCompleted`. The decision is looked up from
`session.history` (not the recovery snapshot, which empties on settlement),
so it survives a process restart and any number of intervening `run` calls.
Resolving the same `toolCallId` twice fails with a typed
`ApprovalResolutionError` (reason `already_resolved`) or
`InteractionResolutionError`, never silently applying or discarding the
second decision.
Sources: `packages/agent/src/conversation.ts:329-403` (`resolveApproval`,
`nativeInteractionState` read from `session.history`, `already_resolved`
check), `packages/agent/src/conversation.ts:404-458` (`resolveInteraction`,
same double-resolve guard), `packages/agent/src/conversation-error.ts:60-91`
(`ApprovalResolutionError`, `InteractionResolutionError` definitions).

**Non-guarantee.** An _unrecorded_ `agent.run(...)` (no `Conversation`) fails
outright on a `needsApproval` tool instead of ever suspending durably — there
is nowhere to record a decision such a run would wait on.
Source: `packages/agent/README.md:717-719`.

## VSP-009 — Compaction never rewrites the log; incomplete summaries are rejected

**Guarantee.** `Compacted` is an appended record, not an edit: the log store's
`Interface` exposes no update or delete operation at all — only `create`,
`acquire`, `append`, `read`, `readBackwards`, `meta`, `changes` — so a
"replaced" history is a read-time reconstruction (`Compacted.firstKept` plus
the tail) over an append-only log, never a mutation of prior rows.
Compaction rejects an incomplete provider finish (`length`, `content-filter`,
`error`) or empty summary text before it can ever become a `Compacted`
record.
Sources: `packages/log/src/log-store.ts:275-372` (the complete `Interface`,
no update/delete method), `packages/agent/src/recording-sink.ts:58-97`
(`compaction`, appends a new `Compacted` record after flushing pending text),
`packages/agent/src/internal/compaction.ts:171-184` (incomplete-finish and
empty-text rejection before `Ref.set(chat.history, ...)`).

**Non-guarantee.** Rejection happens before the _live_ `Chat` history is
rewritten and before a `Compacted` event is emitted — but a summarization
call that fails outright (an `AiError`, a timeout) still spent the provider
call. Compaction retries the reactive trigger only once
(`packages/agent/src/internal/compaction.ts:208-215`); a second overflow
after compacting is not retried again.

## VSP-010 — Fork seeding and offset reseating

**Guarantee.** `forkFrom` copies an ancestor's active prefix into a freshly
claimed, empty stream, then rewrites every offset-valued pointer in the
copied records (`Compacted.firstKept`, `RunSettled.resume.signalCursor`) to
the destination's own offsets via an exhaustive `reseat` match; a
`SignalReceived.at` is reset to `LogOffset.START` rather than reseated,
because no offset in the fork's own signal stream corresponds to one in the
ancestor's.
Sources: `packages/agent/src/internal/fork-seed.ts:115-204` (`seedInto`, the
copy and index-aligned validation), `packages/agent/src/internal/fork-seed.ts:206-270`
(`reseat`, exhaustive `switch` with no `default` case).

**Non-guarantee.** A suspended workflow-owned token is never copied as if it
belonged to the fork — `forkFrom`/`branchFrom` require `{ pendingWait:
'restart' }` and fail with a typed `SuspendedConversationError` otherwise; a
restart re-enters the recorded provider call under a **new** durable
execution and issues a **new** token rather than sharing the source's.
Sources: `packages/agent/src/internal/session-open.ts:106-126`
(`validateSuspendedBoundary`), `packages/agent/README.md:1008-1018`.

## VSP-011 — Change notification: SQLite is process-local, Postgres is cross-process

**Guarantee.** The `changes` stream is a wake-up hint, deliberately payload-free
on both backends, and both must emit an opening tick as soon as the
subscription is live so "catch up, then follow" has no window. On Postgres
that tick, and every subsequent one, crosses process boundaries via
`LISTEN`/`NOTIFY` on a channel derived from an FNV-1a hash of the path; the
`NOTIFY` is issued inside the same transaction as the write, so a reader never
wakes before the rows it would read exist, and a rolled-back append wakes
nobody.
Sources: `packages/log/src/log-store.ts:338-371` (the `changes` contract, the
opening-tick and dead-feed-must-fail requirements), `packages/log-pg/src/layer.ts:98-127`
(`channelFor`, `NOTIFY_PAYLOAD`), `packages/log-pg/src/layer.ts:536-539`
(`pg_notify` inside the append transaction), `packages/log-pg/src/layer.ts:725-733`
(`changes`, backed by `client.listen`).

**Non-guarantee.** SQLite has no cross-process notification primitive at all.
Its `changes` uses Effect's process-local `Reactivity` service, invalidated
after an append commits in the _same process_; a second process writing to
the same SQLite file never wakes a first process's tail — that process will
only notice on its own next `read`.
Source: `packages/log-sqlite/src/layer.ts:13-21` (module doc: "SQLite has no
cross-process notification primitive, so `changes` uses Effect's process-local
Reactivity service"), `packages/log-sqlite/src/layer.ts:636-641` (`changes`
implementation).

## VSP-012 — Read limits

**Guarantee.** Every `read`/`readBackwards` call is bounded: `limit` defaults
to 256 and is capped at 10,000, validated identically for both backends
before either is entered, so one page can never allocate an unbounded result
regardless of how large the underlying stream is.
Sources: `packages/log/src/log-store.ts:206-208` (`DEFAULT_READ_LIMIT`,
`MAX_READ_LIMIT`), `packages/log/src/log-store.ts:225-273`
(`normalizeReadOptions`/`normalizeReadBackwardsOptions`, shared validation).

**Non-guarantee.** The cap bounds one page, not a whole read operation — a
caller that needs the full history of a long-lived, uncompacted conversation
still issues as many pages as the record count requires; nothing here turns
an unbounded conversation into a bounded number of round trips. `Design.md`
is explicit that this remains proportional to what a reader "genuinely
needs" rather than a second cache layer (`Design.md:285-289`).

## VSP-013 — What a reader may observe mid-batch

**Guarantee.** Offsets are assigned per record, not per append batch
(`packages/log/src/log-store.ts:17-25`), specifically so a reader's cursor
can resume between two records that were written in the same original
`append` call — the durability unit (VSP-001) and the reader's resumption
granularity are deliberately different sizes. Because the write itself is one
transaction, a reader can never observe _some but not all_ rows of one append
before the append is either fully visible or not visible at all; what is
mid-batch-observable is a reader's own progress landing between two already-
fully-committed records of what was one batch.

**Non-guarantee.** A page boundary can therefore land between a `ToolCall`
and its `ToolStarted`, or between `ToolStarted` and `ToolOutcome`, if those
happened to be produced by different appends (which they normally are — see
VSP-006). Reading a prefix of a conversation is not a promise that every tool
call visible in it has a matching outcome yet; that is exactly the shape
VSP-006 exists to handle, not a guarantee this layer makes for you.
Source: `packages/log/src/log-store.ts:168-190` (`Page.cursor`, exclusive-
after semantics).

## VSP-014 — What is not promised

- **No exactly-once external effects.** See VSP-007; Vesper's own text is
  direct about this: "There is no broad workflow engine or implicit retry
  policy here" (`Design.md:303-304`).
- **No cross-process wake on SQLite.** See VSP-011;
  `packages/log-sqlite/src/layer.ts:13-21`.
- **No storage migration promise.** The Postgres adapter issues no DDL at
  all — "Nothing here runs migrations. The authoritative DDL applications
  should put into their migration system ships at
  `migrations/001-initial.sql`" (`packages/log-pg/src/layer.ts:24-26`); the
  SQLite adapter's `migrate` is documented "safe to run at every startup"
  (`packages/log-sqlite/src/layer.ts:208-209`) but is additive
  (`CREATE TABLE IF NOT EXISTS`) with no schema-evolution story for a
  pre-existing table.
- **No unbounded resume aggregate.** A compatible history with no
  `RunSettled.resume` aggregate (never yet settled) is scanned in full on
  every open; opening does not write an intermediate checkpoint
  (`Design.md:292-294`).
- **No retention or garbage collection.** The log store interface has no
  delete operation (VSP-009); nothing here trims old records, expired signals,
  or spilled tool-result attachments. `AttachmentStore` "has no delete API,"
  and retention is explicitly the application's responsibility
  (`packages/agent/README.md:409-411`).
- **No idempotency key on `Conversation.send`.** Retrying a signal send after
  an ambiguous transport failure may append the same logical signal twice;
  deduplication is the application's job
  (`docs/conversations.md`, Signals section).

## VSP-015 — Convergence at named durable boundaries is chaos-tested, not asserted

**Guarantee.** `packages/agent/src/internal/failpoint.ts` names the durable
boundaries the recording and recovery machinery crosses as a closed
`Failpoint.Location` union, with at least one instrumented call site per
location across `dispatch.ts`, `internal/session-open.ts`,
`recording-sink.ts`, and `conversation.ts`
(`packages/agent/src/internal/failpoint.ts:58-72`); `test/failpoint.test.ts`
statically checks the union and the call sites cannot drift apart.
`Chaos.converge` (`packages/agent/src/testing.ts`) drives a scripted
conversation with a crash armed at one location, reopens it with the crash
disarmed, and checks the recovered result against a crash-free baseline, a
well-formed durable history, and no tool call replayed outside the
`ToolStarted`..`ToolOutcome` window it is legitimately reconciled through.
`test/chaos.test.ts` runs this scenario — two tool calls and one durable
approval — against both the in-memory and the SQLite log store, and
`claim:after-acquire`, `tool:before-started`, `approval:after-resolved`,
`turn:before-finished`, `turn:after-finished`, and `run:before-completed`
converge cleanly on both, the last of these with zero tolerance on how many
times the provider was asked
(`ChaosOptions.modelCallTolerance: 0`, `ChaosAttempt.modelCalls`) — not just
that the recovered `Agent.Result` happens to match, which a redundant call
that reproduces the same final text would not by itself catch.
`run:before-completed` used to be a documented gap here: recovering from a
crash between the final turn's `TurnFinished` and its `Completed` record
re-asked the provider for a turn whose content was already fully durable.
`agent.ts`'s `settledCompletion` now derives `Completed` from that
`TurnFinished` plus the same turn's durable `Text`/`ToolCall` records — the
same inference `messagesFrom` already relies on to drop an unanswered tool
call, applied to the opposite edge of a turn — gated on the agent's stop
condition and turn control interceptor both being left at their defaults
(`packages/agent/src/internal/history.ts`'s `completedFromTail` documents
why a custom `stopWhen`/`nextTurn` makes that inference unsafe: a
tool-call-free final turn is only reliable evidence the loop was done under
the default `noToolCalls` condition, and durable records alone cannot say
whether a custom one would have kept going).

**Non-guarantee.** The same suite found, and still pins as a regression test
rather than papering over, one location that does not converge cleanly:
cumulative usage undercounts by exactly one physical run's worth of tokens
after a crash strictly inside the `ToolStarted`..`ToolOutcome` window
(`tool:after-started`, `tool:before-outcome`, `tool:after-outcome`) or the
suspend-registration window (`approval:after-suspended`). Traced directly
(`test/resume.test.ts`'s "does not cache an interrupted run's own guessed
usage as a verified checkpoint"): the crashed run's own `RunSettled` carries
`{ input: 0, output: 0 }`, not because `session-open.ts`/`recording-sink.ts`
discard a number they had, but because neither has one yet at that point —
`recording-sink.ts`'s `pending.usage` only advances at a `TurnFinished`/
`Completed` lifecycle event, and Effect AI's own `LanguageModel.streamText`
defers emitting a turn's `finish` part (usage's only carrier) until every
tool call that turn requested has resolved, "to guarantee tool results are
emitted before finish in streaming mode." A crash in that window is a crash
before the number exists anywhere in the process, durable or not; no fold
over any durable record can recover it, so this is not a correctness
violation in the sense VSP-006 or VSP-001 promise — no tool re-runs, no
history corrupts, and what the same information-theoretic ceiling
`docs/conversations.md`'s Signals section and VSP-007 already accept for a
provider call caught genuinely mid-flight. What did change:
`session-open.ts`'s `trackedAppend` used to cache that necessarily-incomplete
guess into `RunSettled.resume` for every settlement outcome, so the shortfall
did not just cost this run once — it became a trusted checkpoint later opens
built on rather than re-derived past. It now only caches `resume` for a
`'success'`/`'cancelled'` settlement, where `pending.usage` is accurate at a
turn boundary either way, so a `'failure'`/`'interrupted'` settlement (what a
crash produces) always forces the next open to re-fold `usage` from durable
`TurnFinished`/`Completed` records instead of trusting a stale snapshot.
`test/chaos.test.ts` names the remaining location, with the record-level
trace it was diagnosed from, next to `USAGE_UNDERCOUNT_LOCATIONS`. This
scenario also does not yet cross a compaction threshold or send a signal, so
`compaction:before-append`, `compaction:after-append`, and
`signal:after-received` report `not-triggered` rather than either status — a
scenario gap, not a finding, tracked the same way.

- **`docs/conversations.md` says a failed append is a defect; the code makes
  it a typed failure.** `docs/conversations.md`'s "The conversation log"
  section states: "A failed append is a **defect**, not a typed failure —
  continuing past one would produce a run whose result exists and whose
  history does not." But `Session.append` (used by both the live recorder and
  `resolveApproval`/`resolveInteraction`) maps every `LogStoreError` — fenced,
  gap, storage, all of it — to a typed `DurabilityError`
  (`packages/agent/src/internal/session-open.ts:233-323`, specifically the
  `logDurabilityError` mapper at `packages/agent/src/internal/session-open.ts:655-662`),
  and `Session`'s own doc comment says the opposite of the guide: "Appends
  expose `DurabilityError` as an ordinary typed failure, so callers can
  distinguish durable infrastructure from model and application failures.
  Reads retain the historic defect boundary" (`packages/agent/src/log.ts:153-157`).
  `Conversation.Error<A>` includes both `LogStore.LogStoreError` and
  `AgentLog.DurabilityError` in its typed union
  (`packages/agent/src/conversation.ts:118-123`). Reads (`session.history`,
  `session.recorded`, the resume-history helpers) are what actually go
  through the `orDie` defect boundary
  (`packages/agent/src/internal/session-open.ts:640-653`), not appends. This
  looks like a doc that described an earlier design and was not updated when
  appends moved to the typed channel; the code, not the guide, is what a
  caller observes.
