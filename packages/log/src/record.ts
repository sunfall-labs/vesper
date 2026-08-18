import { Schema } from 'effect';

import { LogOffset } from './offset.js';
import { LogVocabulary } from './vocabulary.js';

/** The conversation record format understood by this Vesper release. */
export const FORMAT_VERSION = 1;

// What a conversation is made of, once it is events rather than a blob.
//
// Every case is a `Schema.TaggedStruct` and the union is a
// `Schema.TaggedUnion`, because these cross a persistence boundary in both
// directions: a record written by one process is decoded by another, months
// later, possibly by a version of the code that has since grown a case. A
// hand-written `_tag` union would give the same TypeScript and none of the
// decoding.
//
// Every case below is written by something that exists. Most come straight
// from what `@sunfall/vesper-agent` emits — see `agent/src/event.ts` — and the rest
// have a named producer too: `ChildSession` from a delegation, `Signal` from
// an outside sender, `SignalReceived` from the run that took delivery,
// `RunSettled` from the sink's exit finalizer, `BranchedFrom` from a run that
// claimed the conversation at an earlier point than its end. A log is only
// worth building against events that actually happen; speculative cases are
// schema debt with migration consequences and no reader.
//
// Not every case belongs to the same stream. `Signal` lives in a
// conversation's *signal* stream and nothing else does — see
// `agent/src/internal/signal-store.ts` for why that has to be a separate stream from the
// conversation it steers.
//
// **Text is coalesced before it is logged.** This log exists so a
// conversation can be rebuilt and resumed, not so a provider's wire format
// can be replayed byte for byte. A streaming turn emits hundreds of
// three-character `text-delta` parts; appending each one would make the row
// count a function of the provider's chunking, blow out storage, and give a
// resuming reader nothing a coalesced `Text` record does not. The live
// stream is where deltas belong. The producer coalesces contiguous deltas
// into one `Text` record per contiguous run of text in a turn, and flushes
// on a turn boundary or a tool call.

/** Cumulative token usage. Mirrors `Stop.Usage` without importing it. */
export const Usage = Schema.Struct({
  input: Schema.Natural,
  output: Schema.Natural,
});
export interface Usage extends Schema.Struct.Type<typeof Usage.fields> {}

/** Non-negative counters and indexes carried by durable records. */
const Count = Schema.Natural;

/** Epoch milliseconds may predate Unix epoch, but must be finite integers. */
const Timestamp = Schema.Int;

const CompletedValue = Schema.Struct({
  outcome: Schema.optionalKey(Schema.Literals(['success', 'cancelled'])),
  text: Schema.String,
  steps: Count,
  usage: Usage,
});

const ResumeState = Schema.Struct({
  /** Optional only so legacy records decode and can be rejected deliberately. */
  formatVersion: Schema.optionalKey(Count),
  agent: Schema.optionalKey(Schema.String),
  agentRevision: Schema.optionalKey(LogVocabulary.AgentRevision),
  usage: Usage,
  signalCursor: LogOffset.Offset,
  completed: Schema.optionalKey(CompletedValue),
  latestTurnUsage: Schema.optionalKey(Usage),
  state: Schema.optionalKey(
    Schema.Struct({
      id: Schema.String,
      version: Schema.String,
      value: Schema.Unknown,
    }),
  ),
});

/**
 * What an out-of-band instruction to a running conversation says.
 *
 * Declared once and spread into both signal record cases below, because the
 * two describe the same instruction at two points in its life — sent, and
 * delivered — and a second declaration is a second thing to keep in step.
 *
 * `kind` is a closed literal rather than free text: the loop acts on these,
 * and an unrecognised kind that reaches a running agent has no safe default.
 * A steer that is silently ignored looks exactly like a steer that arrived
 * too late.
 */
export const SignalBody = Schema.Struct({
  kind: Schema.Literals(['steer', 'cancel']),
  /** Steering text, or a cancellation reason. */
  text: Schema.String,
  /** Who sent it — a user id, a service name. Opaque to this package. */
  source: Schema.String,
});
export interface SignalBody extends Schema.Struct.Type<
  typeof SignalBody.fields
> {}

/**
 * One thing that happened in a conversation.
 *
 * `prompt`, `params`, and `result` are held as `Schema.Unknown` because their
 * real schemas belong to `effect/unstable/ai` and to whatever toolkit is in
 * play, and re-declaring them here would create a second definition that has
 * to stay in lockstep with one this package does not own.
 */
export const Record = Schema.TaggedUnion({
  /** A run began against this conversation. */
  RunStarted: {
    agent: Schema.String,
    /** Optional only so legacy records decode and can be rejected deliberately. */
    formatVersion: Schema.optionalKey(Count),
    agentRevision: Schema.optionalKey(LogVocabulary.AgentRevision),
    /** Encoded `Prompt.RawInput`, held opaque. */
    prompt: Schema.Unknown,
  },
  /**
   * Model text, already coalesced. Not one record per delta — see the note
   * at the top of this file.
   */
  Text: {
    step: Count,
    text: Schema.String,
  },
  ToolCall: {
    step: Count,
    /** Provider-assigned call id, unique within the conversation. */
    id: LogVocabulary.ToolCallId,
    name: Schema.String,
    /** Whether the provider executed this call outside Vesper's toolkit. */
    providerExecuted: Schema.optionalKey(Schema.Boolean),
    params: Schema.Unknown,
  },
  /**
   * A real tool or delegation handler is about to be invoked.
   *
   * Written after interception and recovery substitution, immediately before
   * entering the handler. Its absence therefore means the call was never
   * dispatched; its presence without a later `ToolOutcome` means execution is
   * indeterminate and must not be retried implicitly.
   */
  ToolStarted: {
    /** Provider-assigned call id, unique within the conversation. */
    id: LogVocabulary.ToolCallId,
    name: Schema.String,
  },
  /**
   * A tool handler deliberately yielded to an external actor.
   *
   * Unlike a bare `ToolStarted`, this is safe to enter again: the workflow
   * engine owns the handler's replay and returns recorded activity results
   * until it reaches the named durable wait. `token` addresses that wait and
   * `request` is the application-owned value presented to the actor.
   */
  ToolSuspended: {
    id: LogVocabulary.ToolCallId,
    name: Schema.String,
    /** Application-defined stable name of the wait point. */
    wait: Schema.String,
    /** Effect Workflow durable-deferred token, opaque to the log package. */
    token: Schema.String,
    /** Schema-encoded application request shown outside the agent run. */
    request: Schema.Unknown,
  },
  /**
   * Effect Workflow deliberately re-entered a previously suspended handler.
   *
   * This is an audit fact, not a state transition back to indeterminate: until
   * `ToolOutcome` is durable, another crash remains safe for workflow replay.
   */
  ToolResumed: {
    id: LogVocabulary.ToolCallId,
    name: Schema.String,
    /** The durable wait whose completion caused this replay. */
    token: Schema.String,
  },
  /**
   * The durable wait's externally supplied result was observed by its handler.
   *
   * `result` is the schema-encoded Effect `Exit`, so both typed success and
   * typed failure decisions remain auditable without the log package knowing
   * the application's schemas. This record does not settle the tool call;
   * only `ToolOutcome` does that.
   */
  ToolWaitCompleted: {
    id: LogVocabulary.ToolCallId,
    name: Schema.String,
    /** Application-defined stable name of the wait point. */
    wait: Schema.String,
    /** The exact durable wait that supplied this result. */
    token: Schema.String,
    outcome: Schema.Literals(['success', 'failure']),
    /** Schema-encoded Effect Exit supplied by the external actor. */
    result: Schema.Unknown,
  },
  /**
   * A branch or fork deliberately chose a new future for a suspended call.
   *
   * Recovery invokes the original provider call as a fresh handler execution;
   * the old token remains an audit fact but can never resume this new path.
   */
  ToolWaitRestarted: {
    id: LogVocabulary.ToolCallId,
    name: Schema.String,
    wait: Schema.String,
    /** Token owned by the abandoned or source workflow execution. */
    priorToken: Schema.String,
  },
  /**
   * How a tool call ended.
   *
   * The fine-grained case the roadmap flags as the one thing
   * provider-seam checkpointing covers and a coarse log would not: a crash
   * between two tool calls mid-turn is recoverable only if each call's
   * outcome was written down separately.
   */
  ToolOutcome: {
    step: Count,
    id: LogVocabulary.ToolCallId,
    name: Schema.String,
    /** Whether the provider, rather than Vesper, supplied this outcome. */
    providerExecuted: Schema.optionalKey(Schema.Boolean),
    outcome: Schema.Literals(['success', 'failure']),
    /**
     * The tool's result in the form the provider is shown — the toolkit's own
     * encoding of it, not the decoded value the handler returned.
     *
     * This is the field a resuming dispatch reads back, and the encoded form
     * is the only one that can be read back correctly: `Prompt` builds a
     * tool-result message from `encodedResult`, so serving a decoded value
     * would put something else in front of the model. It also removes the
     * failure mode the roadmap flags against storing decoded values — a
     * success type that decodes to a `Date` or a `bigint` has already been
     * encoded to JSON by the time it reaches here, instead of failing the
     * append.
     */
    result: Schema.Unknown,
  },
  TurnFinished: {
    step: Count,
    /** Cumulative across the run, not just this turn. */
    usage: Usage,
  },
  /**
   * History was summarized to fit the context window.
   *
   * The one record that *replaces* what came before it rather than adding to
   * it, which is why it carries the replacement and a boundary rather than a
   * pair of counts. It used to carry only counts, and a reader could
   * therefore learn that a conversation had been compacted and nothing about
   * what it had been compacted *into* — so resumption rebuilt from the full
   * record set, handed the model a longer conversation than the run it was
   * resuming, and compaction fired again immediately.
   *
   * `summary` is the model's summary text on its own, without the framing
   * sentence the compacted history wraps it in. The framing belongs to
   * the agent's compaction renderer, so the producer and a
   * resuming reader cannot drift into wrapping it two different ways.
   */
  Compacted: {
    /** Optional only so legacy records decode and can be rejected deliberately. */
    formatVersion: Schema.optionalKey(Count),
    agent: Schema.optionalKey(Schema.String),
    agentRevision: Schema.optionalKey(LogVocabulary.AgentRevision),
    step: Count,
    /** What the summarized history was replaced by. */
    summary: Schema.String,
    /**
     * The first record that survived, or {@link LogOffset.START} when none
     * did.
     *
     * A pointer rather than a copy: the records *are* the conversation, and a
     * record that embedded the surviving ones would be a second copy of them
     * that can disagree with the first. `START` is unambiguous as "nothing
     * before this survived" because it is the exclusive-lower-bound sentinel
     * — no record is ever written at it.
     *
     * It is resolved by the sink rather than by the loop, because the loop
     * has no offsets: compaction runs against `Chat`'s in-memory history and
     * knows only how many messages it kept. See `agent/src/log.ts`.
     */
    firstKept: LogOffset.Offset,
    /**
     * How many messages the summary replaced, and how many it kept. Kept for
     * observability — "this conversation compacts every other turn" is a
     * question about counts — and load-bearing for nothing: neither is enough
     * to rebuild anything, which is the mistake this record used to make.
     */
    summarizedMessages: Count,
    keptMessages: Count,
  },

  /**
   * The conversation continues from an earlier record, not from the one
   * immediately before this.
   *
   * The second case that changes what the records before it mean, and the
   * cheaper of the two. `Compacted` *replaces* history with a summary; this
   * one *re-roots* it. Everything between `at` and this marker stays in the
   * log, stays readable, and stops being on the path a model is shown — which
   * is what "edit an earlier message and re-run" needs and what an
   * append-only line could not otherwise express.
   *
   * **A sparse marker, not a parent pointer on every record.** A log with no
   * ordering primitive of its own — a JSONL file, say — has to carry a parent
   * id on each entry, because that is its only way to walk. Offsets are a
   * total order, so a record's parent is simply the record before it *unless
   * something says otherwise*, and this is the only thing that ever says
   * otherwise. One row per branch instead of one pointer per record, and the
   * whole tree is still recoverable: a record's parent is its predecessor, or
   * `at` when that predecessor is one of these.
   *
   * `at` is **inclusive** — the record it names is the last one on the new
   * path — and it is the same offset-valued pointer `Compacted.firstKept` and
   * `SignalReceived.at` already are, for the same reason the first of those
   * gives: the records *are* the conversation, and a marker that embedded them
   * would be a second copy that can disagree with the first.
   *
   * It is strictly before this record's own offset by construction, since a
   * producer can only name an offset it has already read. A reader does not
   * trust that — see `agent/src/branch.ts`, whose walk is monotonically
   * decreasing whatever `at` says, because a hand-edited row must not be able
   * to hang a tail.
   */
  BranchedFrom: {
    at: LogOffset.Offset,
  },

  Completed: {
    /** Optional only so records written before terminal outcomes still decode. */
    outcome: Schema.optionalKey(Schema.Literals(['success', 'cancelled'])),
    text: Schema.String,
    steps: Count,
    usage: Usage,
  },

  /** A complete, schema-versioned snapshot of application conversation state. */
  StateCheckpoint: {
    id: Schema.String,
    version: Schema.String,
    value: Schema.Unknown,
  },

  /**
   * A delegation, written into **both** conversations.
   *
   * In the parent's log it says "I handed this tool call to that agent, whose
   * work is over there"; in the child's log — as its first record — it says
   * "I am that agent's answer to that tool call". One record type with both
   * ids is what makes the reference canonical rather than a pair of
   * conventions that can disagree: whichever log a reader opens, it finds the
   * same statement.
   *
   * `childConversationId` is derived from the parent id and the tool call id
   * rather than minted randomly, so a re-run of the same delegation lands on
   * the same child conversation instead of orphaning the first one.
   */
  ChildSession: {
    /** The delegation tool call this child answers. Matches `ToolCall.id`. */
    toolCallId: LogVocabulary.ToolCallId,
    /** The child agent's name. */
    agent: Schema.String,
    parentConversationId: LogVocabulary.ConversationId,
    childConversationId: LogVocabulary.ConversationId,
    /** The child's delegation depth; 1 for a top-level agent's child. */
    depth: Count,
  },

  /**
   * An instruction sent to a conversation from outside the run.
   *
   * Lives in the conversation's **signal** stream, not in the conversation
   * itself. They have to be separate streams: the conversation stream is
   * producer-fenced so that two runs cannot interleave one history, and a
   * sender appending to it would fence the very run it is trying to steer.
   */
  Signal: SignalBody.fields,

  /**
   * A signal the run actually took delivery of, written into the conversation.
   *
   * `at` is the signal's offset in the signal stream, and it is what makes
   * delivery resumable: a new run resumes draining after the highest `at` its
   * conversation records, so a signal queued before the run began is still
   * delivered and one already acted on is not delivered twice. Delivery is
   * at-least-once — a run that drained and then died before appending this
   * sees the signal again — which is the right side to err on for steering.
   */
  SignalReceived: {
    ...SignalBody.fields,
    step: Count,
    at: LogOffset.Offset,
    /** Present only when policy rejected rather than delivered the signal. */
    disposition: Schema.optionalKey(Schema.Literal('rejected')),
    /** Stable hard-limit name explaining a rejection. */
    reason: Schema.optionalKey(
      Schema.Literals([
        'signal_bytes',
        'signals_per_boundary',
        'steered_bytes',
      ]),
    ),
  },

  /**
   * How a run ended.
   *
   * The durable half of settlement; `Exit` is the in-process half and does
   * not outlive the process that held it. Without this a reader cannot tell a
   * conversation that is still running from one whose process died — both are
   * "records stop arriving" — and **that distinction is what the resuming
   * tool dispatch is gated on**: a run that settled has nothing to recover,
   * so its tool outcomes must not be served to a later one.
   *
   * A `RunStarted` with no `RunSettled` after it is an orphan. That includes
   * the case where writing this record is itself what failed, which is why
   * nothing here retries or escalates: the absence is the signal.
   */
  RunSettled: {
    outcome: Schema.Literals([
      /** Reached `Completed` normally. */
      'success',
      /** The run failed. `detail` carries the cause, rendered. */
      'failure',
      /** A `cancel` signal ended it at a turn boundary. */
      'cancelled',
      /**
       * Interrupted, or the event stream was abandoned before `Completed` —
       * a consumer that stopped reading, a fiber that was killed.
       */
      'interrupted',
    ]),
    detail: Schema.String,
    steps: Count,
    usage: Usage,
    /** Bounded cumulative state used as the sole resume aggregate. */
    resume: Schema.optionalKey(ResumeState),
  },
});
export type Record = typeof Record.Type;

/** One case of {@link Record}, by tag. */
export type RecordOf<Tag extends Record['_tag']> = Extract<
  Record,
  { readonly _tag: Tag }
>;

/**
 * A record plus everything needed to place it, except its position.
 *
 * `timestamp` is supplied by the producer rather than stamped by the store.
 * That is what makes an append idempotent: a retry has to be byte-identical
 * to the attempt it repeats, and a store-side clock would make every retry a
 * different value.
 */
export const Entry = Schema.Struct({
  conversationId: LogVocabulary.ConversationId,
  /** Epoch milliseconds, from the producer's clock. */
  timestamp: Timestamp,
  record: Record,
});
export interface Entry extends Schema.Struct.Type<typeof Entry.fields> {}

/** An entry once the store has given it a position. */
export const Envelope = Schema.Struct({
  offset: LogOffset.Offset,
  conversationId: LogVocabulary.ConversationId,
  timestamp: Timestamp,
  record: Record,
});
export interface Envelope extends Schema.Struct.Type<typeof Envelope.fields> {}

export * as ConversationRecord from './record.js';
