# Vesper Vocabulary

Terms used consistently across `@sunfall/vesper-*`. Where a term already has a
meaning in `effect/unstable/ai`, that meaning wins.

**Turn** — one model call plus the resolution of any tool calls it requested.
`Chat.generateText` performs exactly one turn.

**Loop** — repeated turns until a `StopCondition` holds. The loop is what
`effect/unstable/ai` does not provide and `@sunfall/vesper-agent` adds.

**Soft stop** — a `StopCondition` asking the loop to finish. A steer may defer
it for another turn.

**Hard run budget** — non-overridable limits on turns, model calls (including
compaction), delegation count/depth/concurrency, tool concurrency, elapsed
time, tokens, and signal processing. One run-policy runtime is created for a
root run and passed by value through every descendant loop. Exhaustion fails
the run; a steer cannot override it.

**Agent** — a definition: name, explicit non-empty revision, instructions,
toolkit, subagents, skills, stop condition. Not a running thing. Reusable and
inert. The revision is application-owned durable compatibility identity.

**State** — one typed document attached to an agent definition. Each run opens
an isolated handle for it; a recorded run restores the document from its
conversation and persists every successful mutation before returning it.

**State checkpoint** — a complete encoded State value in the conversation log.
The latest checkpoint on the active path wins. A settled run carries the latest
checkpoint in its bounded resume aggregate so reopening does not require an
unbounded scan.

**Requires** — the third type parameter of `Agent`, and what still has to be
provided before a run can happen. Unmet requirements are a compile error, which
is the property this family exists for.

**Subagent** — an agent definition compiled to a `task_<name>` tool on its
parent, so delegation composes through the ordinary toolkit machinery and the
child's service requirements surface in the parent's `Requires`.

**Skill** — instructions loaded on demand through the `load_skill` tool rather
than concatenated into the system prompt, so the cacheable prefix stays
byte-identical across turns. Only the catalog line goes in the prompt.

**Record** — one thing that happened in a conversation, appended to the log:
`RunStarted`, `Text`, `ToolCall`, `ToolStarted`, `ToolOutcome`, `TurnFinished`,
`StateCheckpoint`, `Compacted`, `BranchedFrom`, `Completed`, `ChildSession`, `Signal`,
`SignalReceived`, `RunSettled`. The conversation log is the single persistence
mechanism in this family.

**Conversation format version** — Vesper's version for persisted conversation
semantics. It and the agent name/revision are written to `RunStarted` and the
compatibility-bearing `Compacted` and `RunSettled.resume` records. Unsupported,
missing, mismatched, or contradictory metadata rejects resumption before any
model or tool call; migration is explicit, never inferred.

**Offset** — a record's position in a stream, in Durable Streams' format:
opaque, lexicographically sortable, strictly increasing. One per record, not
one per append batch. Reads are exclusive-after, so a cursor names the last
record seen.

**Producer fencing** — epoch plus sequence on a stream, so a conversation has
exactly one writer. A second concurrent run fails its next append instead of
interleaving two runs into one history.

**Session** — a run's claim on one conversation: the producer it writes
through, the history it opened with, the recovery index built from that
history, the signal cursor, and any compiled recording policy.
`AgentLog.Session`. Held by value and passed, never looked up.

**Child session** — the conversation a delegation opens for the child, with an
id derived from the parent's id and the tool call id, referenced by one
`ChildSession` record written into both logs.

**Resumption** — continuing a conversation from its records: the prompt is
rebuilt from `Text`, `ToolCall`, `ToolOutcome` and the steers that redirected
it, and the loop starts the _next_ turn.

**Indeterminate tool call** — a `ToolStarted` with no later `ToolOutcome` in an
orphaned run. It is never redispatched by default. A dedicated interceptor must
explicitly Retry or Answer; ordinary `beforeToolCall` continuation is not a
retry decision.

**Settlement** — the `RunSettled` record that closes a run: `success`,
`failure`, `cancelled`, or `interrupted`. A `RunStarted` with no `RunSettled`
is an orphan, and it is what gates serving a crashed run's tool outcomes to a
later one.

**Signal** — out-of-band `steer` or `cancel` input addressed to a conversation.
Lives in a separate `signals/<conversationId>` stream, which a run only reads,
and is drained in bounded pages at turn boundaries. Oversized or cumulatively
over-budget signals are recorded as rejected; a page backlog emits an event and
remains after the cursor for a later boundary.
For recorded runs, a hint-only subscribe-before-read watcher may preempt an
in-flight provider stream for a valid, reachable cancel. It never advances the
cursor or records delivery. Steers remain boundary-only, and cancellation is
deferred once dispatch commits to any real tool or delegation handler.

**Recording policy** — an application-supplied, persistence-only filter for
run prompts, tool parameters/results, delivered signals, and rendered causes.
It is compiled against the application's Effect context and carried by the
session into child sessions. The default is explicit raw persistence. Filtering
does not change values used by the live model or tool, though resumed history
naturally contains the filtered persisted values.

**Compaction** — replacing old history with a model-written summary when the
token estimate crosses a threshold, or when the provider rejects the prompt as
too long. Itself a model call.

**Context-window heuristics** — how full the window is and whether that is too
full, as a seam (`ContextWindow.Service`) rather than a constant. The default
counts four characters per token; `ContextWindow.usageAnchored` anchors on
usage a provider reported for the last turn and estimates only what arrived
after it.

**Interceptor** — something given a say at the loop's three ordinary seams,
`beforeTurn`, `beforeModelCall`, and `beforeToolCall`, plus the recovery resolver
`onIndeterminateToolCall` as a fourth callback. Attached with
`agent.intercepting`, and what it may do is in each callback's type.

**Provider seam** — Effect's `LanguageModel` service. Official `@effect/ai-*`
packages provide it directly; Vesper neither wraps provider SDKs nor keeps a
second provider registry.
