# Vesper Vocabulary

Terms used consistently across `@sunfall/vesper-*`. Where a term already has a
meaning in `effect/unstable/ai`, that meaning wins.

**Turn** — one model call plus the resolution of any tool calls it requested.
`Chat.generateText` performs exactly one turn.

**Loop** — repeated turns until a `StopCondition` holds. The loop is what
`effect/unstable/ai` does not provide and `@sunfall/vesper-agent` adds.

**Agent** — a definition: name, instructions, toolkit, subagents, skills, stop
condition. Not a running thing. Reusable and inert.

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
`RunStarted`, `Text`, `ToolCall`, `ToolOutcome`, `TurnFinished`, `Compacted`,
`Completed`, `ChildSession`, `Signal`, `SignalReceived`, `RunSettled`. The
conversation log is the single persistence mechanism in this family.

**Offset** — a record's position in a stream, in Durable Streams' format:
opaque, lexicographically sortable, strictly increasing. One per record, not
one per append batch. Reads are exclusive-after, so a cursor names the last
record seen.

**Producer fencing** — epoch plus sequence on a stream, so a conversation has
exactly one writer. A second concurrent run fails its next append instead of
interleaving two runs into one history.

**Session** — a run's claim on one conversation: the producer it writes
through, the history it opened with, the recovery index built from that
history, and the signal cursor. `AgentLog.Session`. Held by value and passed,
never looked up.

**Child session** — the conversation a delegation opens for the child, with an
id derived from the parent's id and the tool call id, referenced by one
`ChildSession` record written into both logs.

**Resumption** — continuing a conversation from its records: the prompt is
rebuilt from `Text`, `ToolCall`, `ToolOutcome` and the steers that redirected
it, and the loop starts the _next_ turn.

**Settlement** — the `RunSettled` record that closes a run: `success`,
`failure`, `cancelled`, or `interrupted`. A `RunStarted` with no `RunSettled`
is an orphan, and it is what gates serving a crashed run's tool outcomes to a
later one.

**Signal** — out-of-band `steer` or `cancel` input addressed to a conversation.
Lives in a separate `signals/<conversationId>` stream, which a run only reads,
and is drained at turn boundaries.

**Compaction** — replacing old history with a model-written summary when the
token estimate crosses a threshold, or when the provider rejects the prompt as
too long. Itself a model call.

**Context-window heuristics** — how full the window is and whether that is too
full, as a seam (`ContextWindow.Service`) rather than a constant. The default
counts four characters per token; the Pi-backed implementation
`@sunfall/vesper-runtime` installs anchors on the usage a provider reported
for the last turn and estimates only what arrived after it.

**Interceptor** — something given a say at the loop's three named seams,
`beforeTurn`, `beforeModelCall`, and `beforeToolCall`. Attached with
`agent.intercepting`, and what it may do is in each seam's type.

**Provider seam** — the two hooks `LanguageModel.make` accepts (`generateText`,
`streamText`). Both the Pi adapter and the retry middleware attach here, which
is what keeps a retried blip from re-running a turn.
