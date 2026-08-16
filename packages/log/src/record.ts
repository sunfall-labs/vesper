import { Effect, Schema } from 'effect';

import { LogOffset } from './offset.js';

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
// `agent/src/signal.ts` for why that has to be a separate stream from the
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
  input: Schema.Number,
  output: Schema.Number,
});
export interface Usage extends Schema.Struct.Type<typeof Usage.fields> {}

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
    /** Encoded `Prompt.RawInput`, held opaque. */
    prompt: Schema.Unknown,
  },
  /**
   * Model text, already coalesced. Not one record per delta — see the note
   * at the top of this file.
   */
  Text: {
    step: Schema.Number,
    text: Schema.String,
  },
  ToolCall: {
    step: Schema.Number,
    /** Provider-assigned call id, unique within the conversation. */
    id: Schema.String,
    name: Schema.String,
    params: Schema.Unknown,
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
    step: Schema.Number,
    id: Schema.String,
    name: Schema.String,
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
    step: Schema.Number,
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
   * whoever renders it — `Compaction.summaryMessage` — so the producer and a
   * resuming reader cannot drift into wrapping it two different ways.
   */
  Compacted: {
    step: Schema.Number,
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
    summarizedMessages: Schema.Number,
    keptMessages: Schema.Number,
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
    text: Schema.String,
    steps: Schema.Number,
    usage: Usage,
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
    toolCallId: Schema.String,
    /** The child agent's name. */
    agent: Schema.String,
    parentConversationId: Schema.String,
    childConversationId: Schema.String,
    /** The child's delegation depth; 1 for a top-level agent's child. */
    depth: Schema.Number,
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
    step: Schema.Number,
    at: LogOffset.Offset,
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
    steps: Schema.Number,
    usage: Usage,
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
  conversationId: Schema.String,
  /** Epoch milliseconds, from the producer's clock. */
  timestamp: Schema.Number,
  record: Record,
});
export interface Entry extends Schema.Struct.Type<typeof Entry.fields> {}

/** An entry once the store has given it a position. */
export const Envelope = Schema.Struct({
  offset: LogOffset.Offset,
  conversationId: Schema.String,
  timestamp: Schema.Number,
  record: Record,
});
export interface Envelope extends Schema.Struct.Type<typeof Envelope.fields> {}

export const envelope = (offset: LogOffset.Offset, entry: Entry): Envelope => ({
  offset,
  conversationId: entry.conversationId,
  timestamp: entry.timestamp,
  record: entry.record,
});

/**
 * Codecs for the persistence boundary.
 *
 * Exported because a backend that stores JSON — the Postgres one, when it
 * lands — must round-trip through these rather than casting rows, and
 * because the contract suite uses them to prove a backend hands back decoded
 * records instead of whatever it happened to store.
 */
export const decodeEnvelope = Schema.decodeUnknownEffect(Envelope);
export const encodeEnvelope = Schema.encodeEffect(Envelope);
export const encodeEntry = Schema.encodeEffect(Entry);

/** A record that cannot be turned into its persisted form. */
export class EncodeError extends Schema.TaggedErrorClass<EncodeError>()(
  '@sunfall/vesper-log/RecordEncodeError',
  {
    detail: Schema.String,
  },
) {}

/**
 * A digest of a batch's persisted content.
 *
 * This is what makes an append retry safe to answer with the original
 * offset. Matching on producer, epoch, and sequence alone says "you asked
 * about this slot before"; it does not say the caller is asking about the
 * *same records*. A producer that reuses a sequence for different content is
 * not retrying, and converging it onto the earlier offset drops the new
 * records with nothing anywhere to indicate it happened. That is the one
 * failure mode this package exists to remove, so the check is exact rather
 * than a proxy like record count, which only catches the careless version.
 *
 * The digest is taken over the *encoded* form: what a backend persists, not
 * the in-memory objects, so two producers that build structurally identical
 * batches by different routes agree. Encoding is also where a payload that
 * cannot be stored is caught. `prompt`, `params`, and `result` are
 * `Schema.Unknown`, so encoding passes them through untouched and a `bigint`
 * or a cycle in one of them survives to `JSON.stringify` — where it fails
 * loudly, as it must, because a backend could not have written it either.
 */
export const fingerprint = (
  entries: ReadonlyArray<Entry>,
): Effect.Effect<string, EncodeError> =>
  Effect.gen(function* () {
    // Wrapped rather than passed by reference: `encodeEntry` takes an
    // optional `ParseOptions` second argument, which `Effect.forEach` would
    // fill with the element index.
    const encoded = yield* Effect.forEach(entries, (entry) =>
      encodeEntry(entry),
    ).pipe(
      Effect.mapError(
        (error) =>
          new EncodeError({
            detail: `records do not encode: ${String(error)}`,
          }),
      ),
    );

    return yield* Effect.try({
      try: () => digest(JSON.stringify(encoded)),
      catch: (cause) =>
        new EncodeError({
          detail: `encoded records are not serializable: ${String(cause)}`,
        }),
    });
  });

/**
 * FNV-1a, a non-cryptographic digest.
 *
 * A collision here would let a genuinely different batch pass as a retry.
 * The inputs are one producer's consecutive batches rather than an open
 * corpus, and the alternative is a hashing dependency on a path that already
 * has an exact fallback — the append is rejected on mismatch, never accepted
 * on a match alone, because producer, epoch, and sequence must line up too.
 */
const digest = (material: string): string => {
  let hash = 0x811c9dc5;
  for (let index = 0; index < material.length; index += 1) {
    hash ^= material.charCodeAt(index);
    hash = Math.imul(hash, 0x01000193) >>> 0;
  }
  return hash.toString(16).padStart(8, '0');
};

export * as ConversationRecord from './record.js';
