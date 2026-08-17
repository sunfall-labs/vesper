import { LogStore } from '@sunfall/vesper-log/log-store';
import { LogOffset } from '@sunfall/vesper-log/offset';
import { ConversationRecord, FORMAT_VERSION } from '@sunfall/vesper-log/record';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';
import {
  Cause,
  Clock,
  Effect,
  Exit,
  Option,
  Ref,
  Schema,
  Semaphore,
  Stream,
} from 'effect';
import { Prompt, type Response, type Tool } from 'effect/unstable/ai';

import { AgentBranch } from './branch.js';
import type { AgentEvents } from './event.js';
import { AgentHistory } from './history.js';
import { PromptTransport } from './prompt-transport.js';
import { AgentSignals } from './signal.js';
import type { Stop } from './stop.js';
import { RecordingPolicy } from './recording-policy.js';

// The sink that turns a run into records, and the handle it writes through.
//
// The loop already emits everything the log needs, so this is a sink and not
// new bookkeeping: `AgentEvents.Event` in, the same events out, records
// written on the way past. It is reached by `agent.recordingTo(id)` and by the
// internal delegation protocol opening a child session. An agent that does
// neither never opens a {@link Session} and never touches this file.
//
// ## Two ordering properties, both load-bearing
//
// Records are appended with `Stream.tap`, so the append completes *before*
// the event it describes reaches the consumer. That is the same rule
// `@sunfall/vesper-durable` states as "deltas stream live, but `finish` is withheld
// until the checkpoint is durable": a consumer must never act on something
// the durable record does not yet contain. Here the consumer is whatever runs
// tool calls in reaction to a turn, or a UI that will resume from an offset.
//
// And a batch is one append, so the text that preceded a tool call and the
// tool call itself land atomically or not at all. A reader cannot observe a
// `ToolCall` whose preceding assistant text was lost.
//
// ## Where text is cut
//
// Coalescing boundaries are **semantic**: one `Text` record per contiguous
// run of assistant text within a turn, flushed when the model does something
// other than talk — a tool call, a tool result, a signal, the end of a turn,
// the end of the run.
//
// The alternatives are a fixed character count or sentence detection, and
// both are worse for the same reason. The log exists to rebuild a
// conversation, and what it rebuilds a `Prompt` message from is exactly "what
// the model said before it did something else". A 512-character cut splits
// that in a place with no meaning, mid-word and mid-JSON, and makes the row
// count a function of a constant nobody can justify. Sentence detection is
// worse still: it is locale- and content-sensitive, it mangles code blocks,
// and it buys nothing a reader wants — nobody resumes a conversation at a
// sentence.
//
// The cost of the semantic boundary is that one very long uninterrupted
// answer is one large row. That is bounded by the model's max output tokens,
// which is the same bound the provider already imposes on a single turn.
//
// ## One producer per run, shared by everything the run writes
//
// A {@link Session} is that producer. The sink writes through it, the loop
// writes signal deliveries through it, delegation opens children through it.
// They have to be one producer and not several: the store's fencing gives a
// stream exactly one writer, so a second `acquire` for the same conversation
// would fence the first, and a run that recorded its signals through a
// separate claim would kill its own event sink on the next append.

/**
 * Where a conversation's stream lives.
 *
 * One function, exported, because the writer and {@link Agent.streamFrom}
 * have to agree and a convention that lives in two places is a convention
 * that eventually differs. The prefix leaves room for streams that are not
 * conversations — `signals/<id>`, and a per-agent identity stream if one ever
 * lands — without a collision that would only surface as one log interleaved
 * into another.
 */
export const pathFor = (conversationId: LogVocabulary.ConversationId): string =>
  `conversations/${conversationId}`;

/**
 * A conversation id for a child, derived rather than minted.
 *
 * Deterministic on purpose. A random id would give a recovered run a *second*
 * child conversation for the same delegation, leaving the first orphaned with
 * no reference to it from anywhere; deriving it means the retry lands on the
 * same stream, so the child's own log — and its own resumption — carries on
 * rather than starting again beside itself. Tool call ids are unique within a
 * conversation. Both inputs are arbitrary strings, so separators alone are
 * not sufficient: `a/b` + `c` and `a` + `b/c` used to name the same child.
 * The parent's UTF-16 length makes the boundary unambiguous without assuming
 * anything about provider ids, including their Unicode normalization.
 */
export const childIdFor = (
  parentConversationId: LogVocabulary.ConversationId,
  toolCallId: LogVocabulary.ToolCallId,
): LogVocabulary.ConversationId =>
  LogVocabulary.ConversationId.make(
    `child-v1:${parentConversationId.length}:${parentConversationId}${toolCallId}`,
  );

/** How a tool call ended, as a previous run recorded it. */
export interface Settled {
  readonly outcome: 'success' | 'failure';
  /** The encoded result, in the form the provider was shown it. */
  readonly result: unknown;
}

/** What an orphaned run durably established about a tool call. */
export type Recovery =
  | { readonly _tag: 'Indeterminate' }
  | ({ readonly _tag: 'Settled' } & Settled);

/** An orphaned handler start and the provider call that originally caused it. */
export interface IndeterminateToolCall {
  readonly step: number;
  readonly name: string;
  readonly toolCallId: LogVocabulary.ToolCallId;
  readonly params: unknown;
}

/** A signal this run has taken delivery of. */
export interface Delivered {
  readonly kind: 'steer' | 'cancel';
  readonly text: string;
  readonly source: string;
  /** The signal's offset in the signal stream. */
  readonly at: string;
}

export interface SignalDrain {
  readonly signals: ReadonlyArray<Delivered>;
  /** More signals remain after this bounded page. */
  readonly backlog: boolean;
}

/** Where a run picks a conversation up, when not at its end. */
export interface OpenOptions {
  /** The agent definition that will continue this conversation. */
  readonly compatibility: Compatibility;
  /**
   * Continue from this record instead of from the conversation's last one.
   *
   * The offset is **inclusive**: the record it names is the last one the new
   * run inherits, and everything after it becomes an abandoned branch. The
   * marker is written as part of claiming — see {@link open} — which is what
   * lets every reader below stay unaware that branching exists.
   */
  readonly branchFrom?: LogOffset.Offset;
}

/**
 * What {@link openWith} may write between claiming a stream and reading it.
 *
 * Both fields describe the same manoeuvre from two sides: put something into
 * the stream *before* the history read, so that everything derived from that
 * read — the prompt, the recovery index, the signal cursor — describes the
 * conversation the caller meant rather than the one the records literally
 * came from. `branchFrom` writes one marker into an existing conversation;
 * `seed` writes a copied prefix into a new one. See {@link open} for why the
 * ordering is the whole trick, and {@link fork} for the copy.
 *
 * Internal, and structurally a supertype of {@link OpenOptions}, so the public
 * `open` passes its own options straight through.
 */
interface ClaimOptions {
  readonly branchFrom?: LogOffset.Offset;
  readonly seed?: ReadonlyArray<ConversationRecord.Envelope>;
  readonly identity?: string;
  readonly compatibility: Compatibility;
}

/** Durable identity required before a definition may continue history. */
export interface Compatibility {
  readonly agent: string;
  readonly revision: LogVocabulary.AgentRevision;
}

/** A durable conversation cannot be opened by this compatibility identity. */
export class CompatibilityError extends Schema.TaggedError<CompatibilityError>()(
  '@sunfall/vesper-agent/CompatibilityError',
  {
    message: Schema.String,
    expectedAgent: Schema.String,
    expectedRevision: Schema.String,
    persistedFormat: Schema.optional(Schema.Number),
    persistedAgent: Schema.optional(Schema.String),
    persistedRevision: Schema.optional(LogVocabulary.AgentRevision),
  },
) {}

/** Ensure a claimed session is handed only to the definition that claimed it. */
export const assertCompatible = (
  session: Session,
  expected: Compatibility,
): Effect.Effect<void, CompatibilityError> =>
  session.compatibility.agent === expected.agent &&
  session.compatibility.revision === expected.revision
    ? Effect.void
    : Effect.fail(
        compatibilityError(
          expected,
          {
            formatVersion: FORMAT_VERSION,
            agent: session.compatibility.agent,
            agentRevision: session.compatibility.revision,
          },
          `session was claimed for agent "${session.compatibility.agent}" revision "${session.compatibility.revision}"`,
        ),
      );

export interface ChildOptions {
  /** The delegation tool call the child answers. */
  readonly toolCallId: LogVocabulary.ToolCallId;
  /** The child agent's name. */
  readonly agent: string;
  readonly revision: LogVocabulary.AgentRevision;
  /** The child's delegation depth; 1 for a top-level agent's child. */
  readonly depth: number;
}

const SessionTypeId: unique symbol = Symbol.for(
  '@sunfall/vesper-agent/AgentLog.Session',
);

/**
 * One run's claim on a conversation, and everything it can do with it.
 *
 * Passed by value rather than read from the context. It could have been a
 * `Context.Reference` defaulting to "not recording", and that shape is
 * rejected deliberately: `Checkpointer.RunId` is what a defaulted reference
 * for persistence looks like after it has gone wrong, and Effect's own
 * guidance is not to put persistence behind one. Handing the session down the
 * call chain means the loop, the sink, and delegation are all looking at the
 * same object because they were given it, not because a lookup happened to
 * find something.
 *
 * Nothing here fails. The store's errors become defects — see {@link orDie} —
 * for the reason `@sunfall/vesper-durable` gives for a checkpoint write: this is
 * infrastructure, not something the model did, and a run whose result exists
 * while its history does not is the exact divergence the log removes.
 */
export interface Session {
  /** @internal Prevents structural fabrication outside this module. */
  readonly [SessionTypeId]: typeof SessionTypeId;
  readonly conversationId: LogVocabulary.ConversationId;
  readonly compatibility: Compatibility;

  /** Usage copied from an ancestor when this conversation was forked. */
  readonly inheritedUsage: Stop.Usage;

  /** Cumulative physical-log usage through the point this session opened. */
  readonly usage: Stop.Usage;

  /** Provider usage for the latest uncompacted completed turn. */
  readonly latestTurnUsage: Stop.Usage | undefined;

  /** Latest completed result on the active path, including anchored history. */
  readonly completed: ReturnType<typeof AgentHistory.completedFrom>;

  /** How long teardown waits for this session's settlement append. */
  readonly settlementTimeoutMillis: number;

  /** @internal Persistence-only filtering inherited by child sessions. */
  readonly recordingPolicy: RecordingPolicy.Runtime;

  /**
   * The records required to rebuild the live prompt when this run claimed it.
   *
   * Read once at {@link open} and held rather than re-read:
   * a second read would race this run's own appends and hand back a history
   * that includes what this run has already written. `AgentHistory` turns it
   * into a `Prompt`, which is how a run continues a conversation instead of
   * restarting it.
   *
   * Compacted conversations retain only the active live tail from the latest
   * compaction boundary. Cumulative physical facts are exposed separately by
   * {@link usage} and the session's durable signal cursor.
   *
   * A run claimed with `branchFrom` follows that marker while paging and does
   * not retain the marker or the abandoned physical range in this array.
   */
  /** @internal Raw recovery snapshot used to rebuild the prompt. */
  readonly history: ReadonlyArray<ConversationRecord.Envelope>;

  /**
   * The active records required to rebuild the conversation **now**, including
   * what this run has already written.
   *
   * The deliberate opposite of {@link history}, and the reason both exist. A
   * reader that must not see this run's own records reads `history`; a writer
   * that needs to point at a record *it just wrote* reads this. Only
   * compaction does — the `Compacted` record names the record its summary
   * stops replacing, and that record is usually one this run appended.
   *
   * A backwards active-path read that stops at the latest compaction boundary.
   * Uncompacted history remains proportional to the prompt it must rebuild.
   */
  /** @internal Raw active-path persistence view. */
  readonly recorded: Effect.Effect<ReadonlyArray<ConversationRecord.Envelope>>;

  /** @internal Append plumbing for the run recorder. */
  readonly append: (
    records: ReadonlyArray<ConversationRecord.Record>,
    timeoutMillis?: number,
  ) => Effect.Effect<void>;

  /**
   * The state an **unsettled earlier run** recorded for this call: either a
   * completed outcome or a handler start with no outcome.
   *
   * Synchronous, because it is a lookup in an index built when the session
   * opened, and that timing is the safety property: everything in it predates
   * anything this run writes, so a run can never recover its own result.
   *
   * Empty unless the conversation's last `RunStarted` has no `RunSettled`
   * after it. A run that ended — successfully, in failure, or cancelled — has
   * nothing to recover, so its tool state is not offered to a later one.
   */
  /** @internal Recovery index for tool dispatch. */
  readonly recovery: (
    name: string,
    toolCallId: LogVocabulary.ToolCallId,
  ) => Option.Option<Recovery>;

  /** @internal Orphaned calls in their original durable ToolCall order. */
  readonly indeterminateToolCalls: ReadonlyArray<IndeterminateToolCall>;

  /** @internal Corrupt recovery state that cannot be safely reconciled. */
  readonly recoveryCorruption: string | undefined;

  /** @internal Whether a durable handler start still lacks an outcome. */
  readonly hasPendingToolCalls: Effect.Effect<boolean>;

  /** @internal Register work released after an outcome is durable. */
  readonly onToolSettled: (
    name: string,
    toolCallId: LogVocabulary.ToolCallId,
    effect: Effect.Effect<void>,
  ) => void;

  /**
   * Signals that have arrived since the last drain.
   *
   * One page per call. A backlog larger than a page is delivered across
   * successive turn boundaries rather than in one burst, which is fine —
   * nothing here promises to deliver a backlog atomically — and it keeps the
   * drain a single bounded read.
   */
  /** @internal Compatibility drain. Policies use {@link drainSignalsBounded}. */
  readonly drainSignals: Effect.Effect<ReadonlyArray<Delivered>>;
  /** @internal Bounded authoritative signal drain. */
  readonly drainSignalsBounded: (limit: number) => Effect.Effect<SignalDrain>;

  /**
   * Hint-only views of the next bounded signal page.
   *
   * Each change-feed tick re-reads from the authoritative delivery cursor but
   * never advances it. The opening tick closes the subscribe/read race. Only
   * {@link drainSignalsBounded} acknowledges records at a turn boundary.
   */
  /** @internal Hint-only signal watcher. */
  readonly signalPages: (
    limit: number,
  ) => Stream.Stream<SignalDrain, LogStore.LogStoreError>;

  /**
   * Record a delegation and open the child's own conversation.
   *
   * Writes the same `ChildSession` record into both logs: into this one as
   * "I delegated here", into the child's as its opening statement of whose
   * child it is. See the record's own documentation for why one record type
   * rather than two conventions.
   */
  /** @internal Open a recorded child session for delegation. */
  readonly child: (
    options: ChildOptions,
  ) => Effect.Effect<Session, CompatibilityError>;
}

/**
 * Claim a conversation and read back what a previous run left.
 *
 * `create` tolerates `conflict` — a second run on an existing conversation is
 * the normal case, not an error. Existing history is compatibility-validated
 * before `acquire`, because an incompatible reader has no right to fence its
 * writer. A compatible open then acquires normally, preserving the single
 * writer race: a second concurrent run fails on its next append rather than
 * interleaving two runs into one history.
 *
 * History is read backwards in two bounded views. Physical cumulative state
 * stops at the newest durable resume aggregate; prompt state follows branch
 * jumps and stops at the latest compaction's kept boundary. An uncompacted
 * prompt, a fork prefix, and compatible history without a resume aggregate
 * still read every record they require.
 *
 * ## Branching happens here, before anything is read
 *
 * `options.branchFrom` claims the conversation at an earlier record than its
 * last, which is what "edit an earlier message and re-run" is from the log's
 * side. The `BranchedFrom` marker is appended **between the claim and the
 * history read**, and that ordering is the whole trick: everything derived
 * below — the prompt a caller rebuilds from `history`, the recovery index, the
 * signal cursor — is derived from a log that already contains the marker, so
 * each of them describes the branch without a single one of them knowing that
 * branching exists.
 *
 * Doing it any later would be wrong in a way that is quiet. A marker appended
 * after `unsettledTools` ran would leave the crashed run the user branched
 * *away from* still holding the recovery index, and the new run would be
 * served tool results answering calls that are no longer in its prompt.
 */
export const open = Effect.fn('AgentLog.open')(function* (
  conversationId: LogVocabulary.ConversationId,
  options: OpenOptions,
) {
  const store = yield* LogStore.Service;
  return yield* openWith(store, conversationId, options);
});

const ACQUIRE_ATTEMPTS = 4;

/**
 * Start a **new** conversation from a prefix of an existing one.
 *
 * {@link open}'s `branchFrom` moves where one conversation continues from;
 * this makes a second conversation that begins where the first had reached at
 * `at`. The difference that matters is the stream: a branch stays in the
 * ancestor's stream and therefore inherits its single writer, so two branches
 * are sequential by construction. A fork is its own stream with its own
 * producer claim, so two forks of one conversation run **concurrently**,
 * which is the only reason this exists.
 *
 * The ancestor is **read, never claimed**. Reading takes no producer, so
 * forking cannot fence a run that is live on the conversation being forked
 * from, and the ancestor gets no record of having been forked — its own
 * history is exactly what it was.
 *
 * ## Records are copied, and their pointers are reseated
 *
 * The alternative was to seed the fork from a rebuilt prompt — one
 * `RunStarted` whose prompt is `messagesFrom(activePath(prefix))` — which is
 * cheaper and needs no pointer rewriting at all. It is rejected because of
 * what it does to the fork's *later* life, not to its first turn. `fold`
 * attributes every message from a `RunStarted` to that one record's offset,
 * so a seeded prefix collapses to a single position; `boundaryFor` would then
 * resolve any compaction boundary that fell inside it back to that one offset
 * and `keptFrom` would keep the entire ancestor history verbatim beside the
 * summary. The fork would be unable to compact its own inheritance, and would
 * re-trigger compaction immediately — precisely the bug
 * `ConversationRecord.Compacted` was given `firstKept` to fix. Copying keeps
 * one record per record, so a fork compacts, branches, and resumes exactly
 * like any other conversation.
 *
 * The price is the pointer trap, and it has two halves that are **not** the
 * same problem:
 *
 *   - `Compacted.firstKept` points into *this* stream. The copy gives every
 *     record a new offset, so the pointer is rewritten through a map from
 *     ancestor offset to fork offset — see {@link seedInto}, which flushes
 *     before each such record so the map is populated when it is read.
 *   - `SignalReceived.at` points into the conversation's **signal** stream,
 *     and the fork has a different one — empty, and starting from low offsets
 *     again. There is no offset in it that corresponds, so this cannot be
 *     rewritten; it is reset to {@link LogOffset.START}. That is the correct
 *     value rather than a fallback: `deliveredThrough` reads it as "how far
 *     this conversation has drained its own signal stream", and the fork has
 *     drained none of its own. Carrying the ancestor's number over would park
 *     the fork's cursor past every offset a genuine signal to the fork would
 *     be written at, and those signals would be **silently never delivered**.
 *     The record's body is kept, because a `steer` is a user message in the
 *     rebuilt conversation and dropping it would change what the model sees.
 *
 * `ChildSession` keeps the ancestor's ids deliberately. They name a child
 * conversation that really exists, over there; rewriting them to the fork
 * would name one that does not exist anywhere.
 *
 * Fork provenance lives in the destination stream's identity. That makes an
 * empty or partially seeded destination distinguishable from an unrelated
 * conversation after a crash, without introducing a second persistence
 * mechanism. A retry with the same source and boundary resumes its copy;
 * every other non-empty destination is rejected.
 */
export const fork = Effect.fn('AgentLog.fork')(function* (
  conversationId: LogVocabulary.ConversationId,
  at: LogOffset.Offset,
  forkConversationId: LogVocabulary.ConversationId,
  compatibility: Compatibility,
) {
  yield* validateCompatibilityInput(compatibility);
  const store = yield* LogStore.Service;

  const ancestor = yield* readAll(store, pathFor(conversationId));
  // Cut first, then fold: this is exactly the sequence `activePath` would
  // return if a `BranchedFrom { at }` had been appended to the ancestor, which
  // is what makes a fork inherit the same prefix `branchFrom` would have.
  const prefix = AgentBranch.activePath(
    ancestor.filter((envelope) => !LogOffset.isAfter(envelope.offset, at)),
  );
  yield* validateCompatibility(prefix, compatibility);
  const identity = forkIdentity({
    sourceConversationId: conversationId,
    at,
    records: prefix.length,
    inheritedUsage: AgentHistory.usageFrom(prefix),
  });

  return yield* openWith(store, forkConversationId, {
    seed: prefix,
    identity,
    compatibility,
  });
});

const openWith = (
  store: LogStore.Interface,
  conversationId: LogVocabulary.ConversationId,
  options: ClaimOptions,
): Effect.Effect<Session, CompatibilityError> =>
  Effect.gen(function* () {
    yield* validateCompatibilityInput(options.compatibility);
    const path = pathFor(conversationId);
    const identity = options?.identity ?? conversationId;

    yield* store.create(path, identity).pipe(
      Effect.asVoid,
      Effect.catchIf(
        (error) => error.reason === 'conflict',
        () => Effect.void,
      ),
      orDie,
    );

    if (options?.identity !== undefined) {
      const meta = yield* orDie(store.meta(path));
      if (Option.isNone(meta) || meta.value.identity !== options.identity) {
        return yield* Effect.die(
          new Error(
            `Conversation log ${path} is occupied by a different conversation or fork`,
          ),
        );
      }
    }

    // Validate and claim one exact stream position. If a compatible writer
    // changes it between the read and acquisition, retry from the new position;
    // if that change is incompatible, validation fails before any epoch bump.
    let claim: LogStore.ProducerClaim | undefined;
    let lastConflict: LogStore.LogStoreError | undefined;
    for (let attempt = 0; attempt < ACQUIRE_ATTEMPTS; attempt += 1) {
      const observed = Option.getOrThrow(yield* orDie(store.meta(path)));
      const existing = yield* options.branchFrom === undefined
        ? readAggregateSuffix(store, path)
        : readAll(store, path);
      const retainedBeforeClaim =
        options.branchFrom === undefined
          ? AgentBranch.activePath(existing)
          : AgentBranch.activePath(
              existing.filter(
                (envelope) =>
                  !LogOffset.isAfter(envelope.offset, options.branchFrom!),
              ),
            );
      yield* validateCompatibility(retainedBeforeClaim, options.compatibility);
      const acquired = yield* store
        .acquire(path, LogVocabulary.ProducerId.make(crypto.randomUUID()), {
          epoch: observed.epoch,
          head: observed.head,
        })
        .pipe(Effect.exit);
      if (Exit.isSuccess(acquired)) {
        claim = acquired.value;
        break;
      } else {
        const error = Exit.findErrorOption(acquired);
        if (Option.isNone(error) || error.value.reason !== 'conflict') {
          return yield* Effect.die(acquired.cause);
        }
        lastConflict = error.value;
      }
    }
    if (claim === undefined) {
      return yield* Effect.die(
        lastConflict ?? new Error('compare-and-acquire retry exhausted'),
      );
    }
    const sequence = yield* Ref.make(claim.nextSequence);
    const appendLock = yield* Semaphore.make(1);
    const childLock = yield* Semaphore.make(1);

    const append: Session['append'] = (records, timeoutMillis) =>
      records.length === 0
        ? Effect.void
        : appendLock.withPermits(1)(
            Effect.uninterruptibleMask((restore) =>
              Effect.gen(function* () {
                const timestamp = yield* Clock.currentTimeMillis;
                const next = yield* Ref.get(sequence);
                const persist = orDie(
                  store.append({
                    path,
                    producerId: claim.producerId,
                    epoch: claim.epoch,
                    sequence: next,
                    records: records.map((record) => ({
                      conversationId,
                      timestamp,
                      // Policy wrappers run outside this append; transport is
                      // therefore the last step before store preparation.
                      record:
                        record._tag === 'RunStarted'
                          ? {
                              ...record,
                              prompt: PromptTransport.encode(record.prompt),
                            }
                          : record,
                    })),
                  }),
                );

                // Keep the backend interruptible (and optionally bounded),
                // then resume masking before advancing the local sequence.
                yield* timeoutMillis === undefined
                  ? restore(persist)
                  : restore(persist).pipe(
                      Effect.timeout(Math.max(1, timeoutMillis - 1)),
                      Effect.orDie,
                    );

                // Advanced only on success, so a caller that retries a failed
                // append reuses the sequence — which the store answers
                // idempotently when the batch digest matches, and rejects when
                // it does not. The permit covers both operations: without it,
                // concurrent signal, event, and child writes can read the same
                // sequence and submit different batches under one producer key.
                yield* Ref.set(
                  sequence,
                  LogVocabulary.ProducerSequence.make(next + 1),
                );
              }),
            ),
          );

    if (options.branchFrom !== undefined) {
      const branchFrom = options.branchFrom;
      const prefix = AgentBranch.activePath(
        (yield* readAll(store, path)).filter(
          (envelope) => !LogOffset.isAfter(envelope.offset, branchFrom),
        ),
      );
      yield* validateCompatibility(prefix, options.compatibility);
      yield* append([{ _tag: 'BranchedFrom', at: branchFrom }]);
    }
    if (options?.seed !== undefined) {
      yield* seedInto(options.seed, append, readAll(store, path));
    }

    const opened = yield* loadOpenState(store, path);
    const history = opened.history;
    yield* validateCompatibility(
      options.branchFrom === undefined
        ? mergeByOffset(opened.aggregateSuffix, history)
        : history,
      options.compatibility,
    );
    // Scoped to the active path: a run this conversation branched away from
    // recorded tool outcomes for calls that are no longer in anyone's prompt,
    // and serving those back would answer questions the resumed run never
    // asked. The signal cursor immediately below is the opposite case, and
    // `branch.ts` says why.
    const recovered = unsettledTools(
      AgentBranch.activePath(opened.aggregateSuffix),
    );
    const recoverable = recovered.recoveries;
    const pendingToolCalls = yield* Ref.make(
      new Set(
        [...recoverable].flatMap(([key, recovery]) =>
          recovery._tag === 'Indeterminate' ? [key] : [],
        ),
      ),
    );
    const toolSettled = new Map<string, Array<Effect.Effect<void>>>();
    const signalCursor = yield* Ref.make(opened.signalCursor);

    const readSignalPage = (
      limit: number,
    ): Effect.Effect<
      { readonly page: LogStore.Page; readonly drain: SignalDrain },
      LogStore.LogStoreError
    > =>
      Effect.gen(function* () {
        const after = yield* Ref.get(signalCursor);
        const signalPath = AgentSignals.pathFor(conversationId);

        const page = yield* store.read(signalPath, { after, limit }).pipe(
          // No stream at all is the ordinary case: nobody has ever signalled
          // this conversation. It is not an empty page — the store
          // distinguishes those deliberately — so it is caught here rather than
          // by creating the stream from the reading side, which would leave an
          // empty signal stream behind every run that was never steered.
          Effect.catchIf(
            (error) => error.reason === 'not_found',
            () =>
              Effect.succeed({
                records: [],
                cursor: after,
                upToDate: true,
              } satisfies LogStore.Page),
          ),
        );

        const signals = page.records.flatMap(
          (envelope): ReadonlyArray<Delivered> =>
            envelope.record._tag === 'Signal'
              ? [
                  {
                    kind: envelope.record.kind,
                    text: envelope.record.text,
                    source: envelope.record.source,
                    at: envelope.offset,
                  },
                ]
              : [],
        );
        return { page, drain: { signals, backlog: !page.upToDate } };
      });

    const drainSignalsBounded = (limit: number) =>
      Effect.gen(function* () {
        // Boundary persistence remains fatal: advancing the live conversation
        // after a failed authoritative drain would diverge signal delivery.
        const { page, drain } = yield* orDie(readSignalPage(limit));
        yield* Ref.set(signalCursor, page.cursor);
        return drain;
      });

    const signalPages = (
      limit: number,
    ): Stream.Stream<SignalDrain, LogStore.LogStoreError> =>
      store
        .changes(AgentSignals.pathFor(conversationId))
        .pipe(
          Stream.mapEffect(() =>
            Effect.map(readSignalPage(limit), (result) => result.drain),
          ),
        );

    const child = (
      options: ChildOptions,
    ): Effect.Effect<Session, CompatibilityError> =>
      childLock
        .withPermits(1)(
          Effect.gen(function* () {
            const childConversationId = childIdFor(
              conversationId,
              options.toolCallId,
            );
            const reference: ConversationRecord.RecordOf<'ChildSession'> = {
              _tag: 'ChildSession',
              toolCallId: options.toolCallId,
              agent: options.agent,
              parentConversationId: conversationId,
              childConversationId,
              depth: options.depth,
            };

            yield* ensureChildReference(
              conversationId,
              yield* readAll(store, path),
              reference,
              append,
            );
            const session = yield* openWith(store, childConversationId, {
              compatibility: {
                agent: options.agent,
                revision: options.revision,
              },
            });
            yield* ensureChildReference(
              childConversationId,
              session.history,
              reference,
              session.append,
            );
            return session;
          }),
        )
        .pipe(Effect.withSpan('AgentLog.Session.child'));

    const meta = yield* orDie(store.meta(path));
    const inheritedUsage = Option.isSome(meta)
      ? (parseForkIdentity(meta.value.identity)?.inheritedUsage ?? {
          input: 0,
          output: 0,
        })
      : { input: 0, output: 0 };

    const completed = yield* Ref.make(AgentHistory.completedFrom(history));
    const latestTurn = yield* Ref.make<Stop.Usage | undefined>(
      AgentHistory.latestTurnUsageFrom(history),
    );
    const previousTurn = yield* Ref.make<Stop.Usage>({ input: 0, output: 0 });
    const compactedSinceTurn = yield* Ref.make(false);

    const trackedAppend: Session['append'] = (records, timeoutMillis) =>
      Effect.gen(function* () {
        let persisted = records;
        const settlement = records.find(
          (record): record is ConversationRecord.RecordOf<'RunSettled'> =>
            record._tag === 'RunSettled',
        );
        if (settlement !== undefined) {
          const cursor = yield* Ref.get(signalCursor);
          const state = resumeState(
            options.compatibility,
            addUsage(opened.usage, settlement.usage),
            cursor,
            yield* Ref.get(completed),
            yield* Ref.get(latestTurn),
          );
          persisted = records.map((record) =>
            record === settlement ? { ...settlement, resume: state } : record,
          );
        }

        yield* append(persisted, timeoutMillis);
        for (const record of records) {
          if (record._tag === 'ToolStarted') {
            recoverable.set(settledKey(record.name, record.id), {
              _tag: 'Indeterminate',
            });
          } else if (record._tag === 'ToolOutcome') {
            recoverable.set(settledKey(record.name, record.id), {
              _tag: 'Settled',
              outcome: record.outcome,
              result: record.result,
            });
          }
        }
        yield* Effect.forEach(records, (record) =>
          updateResumeState(
            record,
            completed,
            latestTurn,
            previousTurn,
            compactedSinceTurn,
          ),
        );
        yield* Effect.gen(function* () {
          yield* Ref.update(pendingToolCalls, (current) => {
            const next = new Set(current);
            for (const record of records) {
              if (record._tag === 'ToolStarted') {
                next.add(settledKey(record.name, record.id));
              } else if (record._tag === 'ToolOutcome') {
                next.delete(settledKey(record.name, record.id));
              }
            }
            return next;
          });
          for (const record of records) {
            if (record._tag !== 'ToolOutcome') continue;
            const key = settledKey(record.name, record.id);
            const callbacks = toolSettled.get(key) ?? [];
            toolSettled.delete(key);
            yield* Effect.forEach(callbacks, (callback) => callback, {
              discard: true,
            });
          }
        });
      });

    return {
      [SessionTypeId]: SessionTypeId,
      conversationId,
      compatibility: options.compatibility,
      inheritedUsage,
      usage: opened.usage,
      latestTurnUsage: AgentHistory.latestTurnUsageFrom(history),
      completed: AgentHistory.completedFrom(history),
      settlementTimeoutMillis: SETTLEMENT_TIMEOUT_MILLIS,
      recordingPolicy: RecordingPolicy.raw,
      history,
      recorded: readResumeHistory(store, path),
      append: trackedAppend,
      recovery: (name, toolCallId) =>
        Option.fromUndefinedOr(recoverable.get(settledKey(name, toolCallId))),
      indeterminateToolCalls: recovered.indeterminate,
      recoveryCorruption: recovered.corruption,
      hasPendingToolCalls: Effect.map(
        Ref.get(pendingToolCalls),
        (pending) => pending.size > 0,
      ),
      onToolSettled: (name, toolCallId, effect) => {
        const key = settledKey(name, toolCallId);
        toolSettled.set(key, [...(toolSettled.get(key) ?? []), effect]);
      },
      drainSignals: Effect.map(
        drainSignalsBounded(1_000),
        (page) => page.signals,
      ),
      drainSignalsBounded,
      signalPages,
      child,
    } satisfies Session;
  });

/** Attach a compiled persistence policy without changing the run's live values. */
export const withRecordingPolicy = (
  session: Session,
  recordingPolicy: RecordingPolicy.Runtime,
): Session => {
  const append: Session['append'] = (records) =>
    Effect.forEach(records, recordingPolicy.filter).pipe(
      Effect.flatMap(session.append),
    );
  return {
    ...session,
    recordingPolicy,
    append,
    child: (options) =>
      Effect.map(session.child(options), (child) =>
        withRecordingPolicy(child, recordingPolicy),
      ),
  };
};

const readAll = (
  store: LogStore.Interface,
  path: string,
): Effect.Effect<ReadonlyArray<ConversationRecord.Envelope>> =>
  Effect.gen(function* () {
    const all: ConversationRecord.Envelope[] = [];
    let cursor = LogOffset.START;
    let done = false;

    while (!done) {
      const page = yield* orDie(store.read(path, { after: cursor }));
      all.push(...page.records);
      cursor = page.cursor;
      done = page.upToDate;
    }

    return all;
  });

interface OpenState {
  readonly history: ReadonlyArray<ConversationRecord.Envelope>;
  readonly aggregateSuffix: ReadonlyArray<ConversationRecord.Envelope>;
  readonly usage: Stop.Usage;
  readonly signalCursor: LogOffset.Offset;
}

/** Read only the physical suffix needed to resume cumulative state. */
const loadOpenState = (
  store: LogStore.Interface,
  path: string,
): Effect.Effect<OpenState> =>
  Effect.gen(function* () {
    const aggregateSuffix = yield* readAggregateSuffix(store, path);
    return {
      history: yield* readResumeHistory(store, path),
      aggregateSuffix,
      usage: AgentHistory.usageFrom(aggregateSuffix),
      signalCursor: deliveredThrough(aggregateSuffix),
    };
  });

/** Read through the newest aggregate, or the full physical log when none exists. */
const readAggregateSuffix = (
  store: LogStore.Interface,
  path: string,
): Effect.Effect<ReadonlyArray<ConversationRecord.Envelope>> =>
  Effect.gen(function* () {
    const newest: ConversationRecord.Envelope[] = [];
    let before: LogOffset.Offset | undefined;
    let done = false;
    while (!done) {
      const page = yield* orDie(
        store.readBackwards(path, {
          ...(before === undefined ? {} : { before }),
          limit: RESUME_READ_LIMIT,
        }),
      );
      for (const envelope of page.records) {
        newest.push(envelope);
        if (
          envelope.record._tag === 'RunSettled' &&
          envelope.record.resume !== undefined
        ) {
          done = true;
          break;
        }
      }
      if (done || page.upToDate) break;
      before = page.cursor;
    }
    return newest.reverse();
  });

const mergeByOffset = (
  left: ReadonlyArray<ConversationRecord.Envelope>,
  right: ReadonlyArray<ConversationRecord.Envelope>,
): ReadonlyArray<ConversationRecord.Envelope> => {
  const retained = new Map(
    left.map((envelope) => [envelope.offset, envelope] as const),
  );
  for (const envelope of right) retained.set(envelope.offset, envelope);
  return [...retained.values()].sort((a, b) =>
    a.offset < b.offset ? -1 : a.offset > b.offset ? 1 : 0,
  );
};

interface PersistedCompatibility {
  readonly formatVersion?: number | undefined;
  readonly agent?: string | undefined;
  readonly agentRevision?: LogVocabulary.AgentRevision | undefined;
}

/** Validate every retained durable definition identity against one authority. */
const validateCompatibility = (
  history: ReadonlyArray<ConversationRecord.Envelope>,
  expected: Compatibility,
): Effect.Effect<void, CompatibilityError> => {
  const identities: PersistedCompatibility[] = [];
  for (const { record } of history) {
    if (record._tag === 'RunStarted') {
      identities.push({
        formatVersion: record.formatVersion,
        agent: record.agent,
        agentRevision: record.agentRevision,
      });
    } else if (record._tag === 'Compacted') {
      identities.push(record);
    } else if (record._tag === 'RunSettled' && record.resume !== undefined) {
      identities.push(record.resume);
    }
  }

  // A newly-created stream, or a child stream containing only links/signals,
  // has no definition identity to compare yet. Any actual conversation state
  // without one is legacy history and must not be adopted silently.
  if (identities.length === 0) {
    const hasConversationState = history.some(({ record }) =>
      record._tag === 'ChildSession' || record._tag === 'Signal' ? false : true,
    );
    return hasConversationState
      ? Effect.fail(
          compatibilityError(
            expected,
            {},
            'history predates explicit compatibility metadata',
          ),
        )
      : Effect.void;
  }

  for (const persisted of identities) {
    const problem =
      persisted.formatVersion === undefined ||
      persisted.agentRevision === undefined ||
      persisted.agent === undefined
        ? 'history predates explicit compatibility metadata'
        : persisted.formatVersion !== FORMAT_VERSION
          ? `conversation format ${persisted.formatVersion} is unsupported; this release supports format ${FORMAT_VERSION}`
          : persisted.agent !== expected.agent
            ? `history contains contradictory agent "${persisted.agent}", not "${expected.agent}"`
            : persisted.agentRevision !== expected.revision
              ? `history contains contradictory revision "${persisted.agentRevision}", not "${expected.revision}"`
              : undefined;
    if (problem !== undefined) {
      return Effect.fail(compatibilityError(expected, persisted, problem));
    }
  }
  return Effect.void;
};

const validateCompatibilityInput = (
  compatibility: Compatibility,
): Effect.Effect<void, CompatibilityError> => {
  const problem =
    compatibility.agent.trim() === ''
      ? 'agent name must be non-empty'
      : compatibility.revision.trim() === ''
        ? 'revision must be non-empty'
        : undefined;
  return problem === undefined
    ? Effect.void
    : Effect.fail(compatibilityError(compatibility, {}, problem));
};

/** An actionable error for incompatible durable history. */
const compatibilityError = (
  expected: Compatibility,
  persisted: PersistedCompatibility,
  problem: string,
): CompatibilityError =>
  new CompatibilityError({
    message:
      `Cannot resume durable conversation: ${problem}. ` +
      'Use the matching agent definition or explicitly migrate the history and its compatibility metadata.',
    expectedAgent: expected.agent,
    expectedRevision: expected.revision,
    ...(persisted.formatVersion === undefined
      ? {}
      : { persistedFormat: persisted.formatVersion }),
    ...(persisted.agent === undefined
      ? {}
      : { persistedAgent: persisted.agent }),
    ...(persisted.agentRevision === undefined
      ? {}
      : { persistedRevision: persisted.agentRevision }),
  });

/**
 * Walk the active path backwards, jumping over abandoned branches and stopping
 * once the latest compaction's kept boundary has been retained.
 */
const readResumeHistory = (
  store: LogStore.Interface,
  path: string,
): Effect.Effect<ReadonlyArray<ConversationRecord.Envelope>> =>
  Effect.gen(function* () {
    const newest: ConversationRecord.Envelope[] = [];
    let before: LogOffset.Offset | undefined;
    let boundary: LogOffset.Offset | undefined;
    let done = false;

    while (!done) {
      const page = yield* orDie(
        store.readBackwards(path, {
          ...(before === undefined ? {} : { before }),
          limit: RESUME_READ_LIMIT,
        }),
      );
      let jumped = false;
      for (const envelope of page.records) {
        if (envelope.record._tag === 'BranchedFrom') {
          if (
            envelope.record.at === LogOffset.START ||
            !LogOffset.isAfter(envelope.offset, envelope.record.at)
          ) {
            if (envelope.record.at === LogOffset.START) done = true;
            continue;
          }
          before = yield* offsetAfter(envelope.record.at);
          jumped = true;
          break;
        }

        newest.push(envelope);
        if (
          boundary !== undefined &&
          !LogOffset.isAfter(envelope.offset, boundary)
        ) {
          done = true;
          break;
        }
        if (boundary === undefined && envelope.record._tag === 'Compacted') {
          boundary = envelope.record.firstKept;
          if (boundary === LogOffset.START) {
            done = true;
            break;
          }
        }
      }
      if (done) break;
      if (jumped) continue;
      if (page.upToDate) break;
      before = page.cursor;
    }
    return newest.reverse();
  });

const RESUME_READ_LIMIT = 32;

const offsetAfter = (
  offset: LogOffset.Offset,
): Effect.Effect<LogOffset.Offset> =>
  LogOffset.toSeq(offset).pipe(
    Effect.map((sequence) => LogOffset.fromSeq(sequence + 1n)),
    Effect.orDie,
  );

const resumeState = (
  compatibility: Compatibility,
  usage: Stop.Usage,
  signalCursor: LogOffset.Offset,
  completed: ReturnType<typeof AgentHistory.completedFrom>,
  latestTurnUsage: Stop.Usage | undefined,
) => ({
  formatVersion: FORMAT_VERSION,
  agent: compatibility.agent,
  agentRevision: compatibility.revision,
  usage,
  signalCursor,
  ...(completed === undefined ? {} : { completed }),
  ...(latestTurnUsage === undefined ? {} : { latestTurnUsage }),
});

const addUsage = (left: Stop.Usage, right: Stop.Usage): Stop.Usage => ({
  input: left.input + right.input,
  output: left.output + right.output,
});

const updateResumeState = (
  record: ConversationRecord.Record,
  completed: Ref.Ref<ReturnType<typeof AgentHistory.completedFrom>>,
  latestTurn: Ref.Ref<Stop.Usage | undefined>,
  previousTurn: Ref.Ref<Stop.Usage>,
  compactedSinceTurn: Ref.Ref<boolean>,
): Effect.Effect<void> =>
  Effect.gen(function* () {
    switch (record._tag) {
      case 'RunStarted':
        yield* Ref.set(completed, undefined);
        yield* Ref.set(previousTurn, { input: 0, output: 0 });
        break;
      case 'Completed':
        yield* Ref.set(completed, {
          ...record,
          outcome: record.outcome ?? 'success',
        });
        break;
      case 'Compacted':
        yield* Ref.set(compactedSinceTurn, true);
        break;
      case 'TurnFinished': {
        const previous = yield* Ref.get(previousTurn);
        const compacted = yield* Ref.get(compactedSinceTurn);
        yield* Ref.set(
          latestTurn,
          compacted
            ? undefined
            : {
                input: record.usage.input - previous.input,
                output: record.usage.output - previous.output,
              },
        );
        yield* Ref.set(previousTurn, record.usage);
        yield* Ref.set(compactedSinceTurn, false);
        break;
      }
      default:
        break;
    }
  });

const FORK_IDENTITY_PREFIX = '@sunfall/vesper-agent/fork/v1:';

interface ForkIdentity {
  readonly sourceConversationId: LogVocabulary.ConversationId;
  readonly at: LogOffset.Offset;
  readonly records: number;
  readonly inheritedUsage: Stop.Usage;
}

const forkIdentity = (identity: ForkIdentity): string =>
  `${FORK_IDENTITY_PREFIX}${JSON.stringify([
    identity.sourceConversationId,
    identity.at,
    identity.records,
    identity.inheritedUsage.input,
    identity.inheritedUsage.output,
  ])}`;

const ForkIdentitySchema = Schema.Tuple([
  LogVocabulary.ConversationId,
  LogOffset.Offset,
  Schema.Number,
  Schema.Number,
  Schema.Number,
]);

const parseForkIdentity = (identity: string): ForkIdentity | undefined => {
  if (!identity.startsWith(FORK_IDENTITY_PREFIX)) return undefined;
  try {
    const value = Schema.decodeUnknownSync(ForkIdentitySchema)(
      JSON.parse(identity.slice(FORK_IDENTITY_PREFIX.length)),
    );
    return {
      sourceConversationId: value[0],
      at: value[1],
      records: value[2],
      inheritedUsage: { input: value[3], output: value[4] },
    };
  } catch {
    return undefined;
  }
};

const ensureChildReference = (
  conversationId: LogVocabulary.ConversationId,
  history: ReadonlyArray<ConversationRecord.Envelope>,
  reference: ConversationRecord.RecordOf<'ChildSession'>,
  append: Session['append'],
): Effect.Effect<void> => {
  const parentSide = conversationId === reference.parentConversationId;
  const links = history.flatMap(({ record }) =>
    record._tag === 'ChildSession' &&
    (parentSide
      ? record.parentConversationId === conversationId &&
        record.toolCallId === reference.toolCallId
      : record.childConversationId === conversationId)
      ? [record]
      : [],
  );
  if (
    links.some((link) => JSON.stringify(link) === JSON.stringify(reference))
  ) {
    return Effect.void;
  }
  if (links.length > 0) {
    return Effect.die(
      new Error(
        `Conversation ${conversationId} has a conflicting child-session link for tool call ${reference.toolCallId}`,
      ),
    );
  }
  return append([reference]);
};

/**
 * Copy an ancestor's prefix into a freshly claimed stream.
 *
 * Writes in as few batches as the pointers allow. A run of records with no
 * offset-valued pointer is one append; a `Compacted` forces a flush before it,
 * because it is rewritten through a map that only holds offsets already
 * written. Compaction is rare, so this is one append for almost every fork.
 *
 * The index alignment is what makes the map cheap and is why this runs inside
 * {@link openWith}'s claim: the stream was created empty by {@link fork} and
 * this producer is the only writer, so the nth envelope read back is the nth
 * record copied.
 */
const seedInto = (
  prefix: ReadonlyArray<ConversationRecord.Envelope>,
  append: Session['append'],
  recorded: Effect.Effect<ReadonlyArray<ConversationRecord.Envelope>>,
): Effect.Effect<void> =>
  Effect.gen(function* () {
    const reseated = new Map<LogOffset.Offset, LogOffset.Offset>();
    let written = yield* recorded;
    if (written.length > prefix.length) {
      return yield* Effect.die(
        new Error('Fork destination contains records beyond its seed prefix'),
      );
    }

    for (let index = 0; index < written.length; index += 1) {
      const source = prefix[index]!;
      const expected = reseat(source.record, reseated);
      if (JSON.stringify(written[index]!.record) !== JSON.stringify(expected)) {
        return yield* Effect.die(
          new Error(`Fork destination seed differs at record ${index}`),
        );
      }
      reseated.set(source.offset, written[index]!.offset);
    }

    let copied = written.length;
    let pending: Array<ConversationRecord.Record> = [];

    const flush = Effect.gen(function* () {
      if (pending.length === 0) return;
      yield* append(pending);
      pending = [];

      written = yield* recorded;
      while (copied < written.length) {
        reseated.set(prefix[copied]!.offset, written[copied]!.offset);
        copied += 1;
      }
    });

    for (let index = written.length; index < prefix.length; index += 1) {
      const { record } = prefix[index]!;
      // Flushed *before* any record whose pointer is rewritten through the
      // map, so that the offsets it may name are in it. `Compacted` is the
      // only such case today, and {@link reseat} is the tripwire: adding a
      // record that points into this stream stops it compiling, which is what
      // brings whoever adds it back to this line.
      if (record._tag === 'Compacted') yield* flush;

      pending.push(reseat(record, reseated));
    }

    yield* flush;
  });

/**
 * A copied record, with its offset-valued pointers made to mean the same thing
 * in the stream it is being copied into.
 *
 * Exhaustive on purpose, with no `default` that passes an unknown case
 * through. Every offset in this union is a pointer into some stream, and a
 * copy moves records between streams — so a new record case carrying one has
 * to decide here what it means over there. The `never` binding below is what
 * makes that decision unavoidable rather than a silent corruption of a field
 * nobody remembered. {@link fork} states the two live cases and why they are
 * handled differently.
 */
const reseat = (
  record: ConversationRecord.Record,
  reseated: ReadonlyMap<LogOffset.Offset, LogOffset.Offset>,
): ConversationRecord.Record => {
  switch (record._tag) {
    case 'Compacted':
      return {
        ...record,
        // A miss falls back to START, which is the same tolerance
        // `AgentHistory`'s `keptFrom` already shows a `firstKept` naming a
        // record that is not there: keep the summary, keep nothing verbatim
        // before it. START itself misses and maps to itself, correctly — no
        // record is ever written at the sentinel.
        firstKept: reseated.get(record.firstKept) ?? LogOffset.START,
      };

    case 'SignalReceived':
      // Not a rewrite — there is no offset in the fork's signal stream that
      // corresponds to one in the ancestor's. See {@link fork}.
      return { ...record, at: LogOffset.START };

    case 'RunSettled':
      return record.resume === undefined
        ? record
        : {
            ...record,
            resume: { ...record.resume, signalCursor: LogOffset.START },
          };

    case 'RunStarted':
    case 'Text':
    case 'ToolCall':
    case 'ToolStarted':
    case 'ToolOutcome':
    case 'TurnFinished':
    case 'BranchedFrom':
    case 'Completed':
    case 'ChildSession':
    case 'Signal':
      return record;

    default: {
      const unreachable: never = record;
      return unreachable;
    }
  }
};

/**
 * Map key for a tool outcome, keyed by tool name and call id together.
 *
 * The separator is U+001F UNIT SEPARATOR, the ASCII control character whose
 * defined meaning is exactly this: joining fields into one datum. It is used
 * rather than a printable character because it cannot occur in either
 * component, so no pair of distinct (name, id) can collide by containing the
 * separator itself.
 *
 * It is deliberately **not** U+0000. A NUL anywhere in a source file makes
 * every byte-oriented tool classify the file as binary: `file(1)` reports it
 * as `data`, and `grep` and `diff` silently produce no output for the whole
 * file — no error and no "binary file matches" line. A search for a symbol
 * that is plainly present comes back empty, which reads as absence.
 */
const settledKey = (
  name: string,
  toolCallId: LogVocabulary.ToolCallId,
): string => `${name}\u001f${toolCallId}`;

/**
 * Tool states belonging to runs that started and never settled.
 *
 * The gate on resuming dispatch, and the reason `RunSettled` exists.
 * `RunSettled` clears the map. A later `RunStarted` does not: recovery attempts
 * may themselves crash before reconciling the earlier start, and forgetting it
 * would turn the next attempt into an implicit redispatch. A conversation whose
 * last run finished yields an empty map, and dispatch behaves as it always did.
 *
 * Given the **active path**, not the log. A crashed run the conversation has
 * since branched away from is not a run this one is recovering; its outcomes
 * belong to tool calls the new prompt does not contain, and offering them
 * would hand the model results for questions it never asked. The caller
 * filters rather than this function, so that the two full-log folds nearby are
 * visibly a different decision and not an omission.
 */
const unsettledTools = (
  history: ReadonlyArray<ConversationRecord.Envelope>,
): {
  readonly recoveries: Map<string, Recovery>;
  readonly indeterminate: ReadonlyArray<IndeterminateToolCall>;
  readonly corruption: string | undefined;
} => {
  const recoveries = new Map<string, Recovery>();
  const calls = new Map<string, IndeterminateToolCall>();
  const starts = new Map<
    string,
    { readonly name: string; readonly id: string }
  >();
  const order: string[] = [];
  let running = false;

  for (const { record } of history) {
    switch (record._tag) {
      case 'RunStarted':
        running = true;
        break;
      case 'RunSettled':
        recoveries.clear();
        calls.clear();
        starts.clear();
        order.length = 0;
        running = false;
        break;
      case 'ToolCall':
        if (running) {
          const key = settledKey(record.name, record.id);
          if (!calls.has(key)) order.push(key);
          calls.set(key, {
            step: record.step,
            name: record.name,
            toolCallId: record.id,
            params: record.params,
          });
        }
        break;
      case 'ToolStarted':
        if (running) {
          const key = settledKey(record.name, record.id);
          recoveries.set(key, {
            _tag: 'Indeterminate',
          });
          starts.set(key, { name: record.name, id: record.id });
        }
        break;
      case 'ToolOutcome':
        if (running) {
          recoveries.set(settledKey(record.name, record.id), {
            _tag: 'Settled',
            outcome: record.outcome,
            result: record.result,
          });
        }
        break;
      default:
        break;
    }
  }

  // Dispatch commits before entering the handler, while ToolCall arrives via
  // the provider event stream. Either record can therefore win the append
  // race. Diagnose corruption only after the complete orphan suffix is folded.
  const unmatched = [...starts].find(
    ([key]) => recoveries.get(key)?._tag === 'Indeterminate' && !calls.has(key),
  )?.[1];

  return {
    recoveries,
    corruption:
      unmatched === undefined
        ? undefined
        : `Cannot recover indeterminate tool ${unmatched.name} (${unmatched.id}): ` +
          'durable ToolStarted has no matching ToolCall',
    indeterminate: order.flatMap((key) =>
      recoveries.get(key)?._tag === 'Indeterminate' && calls.has(key)
        ? [calls.get(key)!]
        : [],
    ),
  };
};

/**
 * The furthest signal offset this conversation has recorded taking.
 *
 * The **whole log**, and this is the row of `branch.ts`'s table that is worth
 * being careful about. A `SignalReceived` says a steer was delivered to a
 * running agent, which then acted on it — a fact about the world, not a claim
 * the conversation can withdraw by branching. If this were scoped to the
 * active path, branching away from the turn that took a steer would rewind the
 * cursor past it, and the next run would drain that steer from the signal
 * stream a second time and inject an instruction the agent has already
 * followed. Delivery is at-least-once by design; this keeps "at least" from
 * quietly becoming "every time anyone edits an earlier message".
 */
const deliveredThrough = (
  history: ReadonlyArray<ConversationRecord.Envelope>,
): LogOffset.Offset => {
  let at = LogOffset.START;
  for (const { record } of history) {
    if (record._tag === 'SignalReceived' && LogOffset.isAfter(record.at, at)) {
      at = record.at;
    }
    if (
      record._tag === 'RunSettled' &&
      record.resume !== undefined &&
      LogOffset.isAfter(record.resume.signalCursor, at)
    ) {
      at = record.resume.signalCursor;
    }
  }
  return at;
};

export interface Options {
  /** Agent name, written into `RunStarted`. */
  readonly agent: string;
  readonly revision: LogVocabulary.AgentRevision;
  /** The run's input, written into `RunStarted` as prompt messages. */
  readonly input: Prompt.RawInput;
}

/** Persist the effective input before any event from the run can escape. */
export const start = (
  session: Session,
  options: Options,
): Effect.Effect<void> =>
  session.append([
    {
      _tag: 'RunStarted',
      agent: options.agent,
      agentRevision: options.revision,
      formatVersion: FORMAT_VERSION,
      // `beforeTurn` has already run when this is called. Persisting here is
      // what makes reconstruction use the same input the provider saw.
      prompt: Prompt.make(options.input).content,
    },
  ]);

/**
 * Record a run's events into the session's conversation.
 *
 * The returned stream emits exactly what it was given, and — unlike the
 * version this replaces — does not change its requirement channel either: the
 * session already holds the store. A log write that fails becomes a
 * **defect**, not a failure, which is the same decision `@sunfall/vesper-durable`
 * makes for a checkpoint write and for the same reason: this is
 * infrastructure, not something the model did, and no retry policy written
 * for provider errors should absorb it. Continuing past a failed append would
 * produce a run whose result exists and whose history does not.
 *
 * The one exception is the settlement record, which is written from a
 * finalizer and cannot report anything to anyone. See {@link settle}.
 */
export const record = <Tools extends Record<string, Tool.Any>, E, R>(
  session: Session,
  events: Stream.Stream<AgentEvents.Event<Tools>, E, R>,
): Stream.Stream<AgentEvents.Event<Tools>, E, R> =>
  Stream.unwrap(
    Effect.sync(() => {
      const pending: Pending = {
        step: 0,
        text: '',
        steps: 0,
        usage: { input: 0, output: 0 },
        completed: false,
        cancelled: false,
      };

      return Stream.tap(events, (event) =>
        // Compaction is the one event whose record cannot be built from the
        // event alone: it names a position in the log, and only the log knows
        // positions. Everything else is a pure function of the event and what
        // the sink has seen.
        event._tag === 'Compacted'
          ? compaction(session, pending, event)
          : session.append(recordsFor(pending, event)),
      ).pipe(Stream.onExit((exit) => settle(session, pending, exit)));
    }),
  );

/**
 * Write down that history was replaced, and what it was replaced by.
 *
 * Two appends rather than one, in this order and for this reason: any text the
 * model had produced and not yet flushed is part of the history compaction
 * just summarized, so it has to be *in* the log before the boundary is
 * resolved against it. Resolving first would point `firstKept` at a log that
 * is one record short of the conversation the loop compacted.
 *
 * `keptMessages` arrives as a count because that is all the loop can supply —
 * compaction runs against `Chat`'s in-memory history, which carries no record
 * identity — and `AgentHistory.boundaryFor` is what turns it back into a
 * position. It has to be that function and not a private one here: the
 * boundary is only meaningful in terms of the messages `messagesFrom`
 * rebuilds, so the writer and the reader have to be reading the same
 * definition.
 */
const compaction = (
  session: Session,
  pending: Pending,
  event: Extract<AgentEvents.Lifecycle, { readonly _tag: 'Compacted' }>,
): Effect.Effect<void> =>
  Effect.gen(function* () {
    yield* session.append(flush(pending));

    const recorded = yield* session.recorded;

    yield* session.append([
      {
        _tag: 'Compacted',
        formatVersion: FORMAT_VERSION,
        agent: session.compatibility.agent,
        agentRevision: session.compatibility.revision,
        step: event.step,
        summary: event.summary,
        firstKept: AgentHistory.boundaryFor(recorded, event.keptMessages),
        summarizedMessages: event.summarizedMessages,
        keptMessages: event.keptMessages,
      },
    ]);
  });

/** Maximum time run teardown waits for the settlement append. */
export const SETTLEMENT_TIMEOUT_MILLIS = 5_000;

/**
 * Write down how the run ended, including the ways that end no stream.
 *
 * Teardown is uninterruptible, but the backend operation and the wait for its
 * serialization permit are interruptible and bounded. Timing out
 * intentionally leaves the orphan shape below.
 *
 * Failures here are swallowed after being logged, which is the opposite of
 * every other write in this file and is the only defensible option. There is
 * no one left to fail to: the stream has ended, its consumer has its value or
 * its error, and turning a settle-time store failure into a defect would
 * replace whatever actually went wrong with a complaint about the log. What
 * it leaves behind is a `RunStarted` with no `RunSettled` — which is exactly
 * the orphan shape a reader is told to look for, and which the resuming
 * dispatch treats conservatively by serving completed outcomes and refusing
 * to guess about indeterminate starts.
 */
const settle = (
  session: Session,
  pending: Pending,
  exit: Exit.Exit<unknown, unknown>,
): Effect.Effect<void> =>
  Effect.suspend(() => {
    const settlement: ConversationRecord.Record = {
      _tag: 'RunSettled',
      ...outcomeOf(pending, exit),
      steps: pending.steps,
      usage: pending.usage,
    };

    const write = Effect.flatMap(session.hasPendingToolCalls, (pending) =>
      pending
        ? Effect.logError(
            `Conversation ${session.conversationId} has indeterminate tool execution; leaving the run orphaned`,
          )
        : session.append([settlement], session.settlementTimeoutMillis),
    ).pipe(
      Effect.catchCause((cause) =>
        Effect.logError(
          `Conversation ${session.conversationId} could not record how its run settled`,
          cause,
        ),
      ),
    );

    return Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const completed = yield* restore(write).pipe(
          Effect.timeoutOption(session.settlementTimeoutMillis),
        );
        if (Option.isNone(completed)) {
          yield* Effect.logError(
            `Conversation ${session.conversationId} settlement append timed out after ${session.settlementTimeoutMillis}ms; leaving the run orphaned`,
          );
        }
      }),
    );
  });

const outcomeOf = (
  pending: Pending,
  exit: Exit.Exit<unknown, unknown>,
): { readonly outcome: SettledOutcome; readonly detail: string } => {
  if (Exit.isFailure(exit)) {
    return Cause.hasInterrupts(exit.cause)
      ? { outcome: 'interrupted', detail: 'the run was interrupted' }
      : { outcome: 'failure', detail: Cause.pretty(exit.cause) };
  }
  if (pending.cancelled) {
    return { outcome: 'cancelled', detail: 'a cancel signal ended the run' };
  }
  if (pending.completed) {
    return { outcome: 'success', detail: '' };
  }
  // The stream ended cleanly without a `Completed` event, which the loop
  // never does on its own: a consumer took a fixed number of events and
  // walked away. The run did not finish, and saying so is what stops the
  // record from claiming a success nobody got.
  return {
    outcome: 'interrupted',
    detail: 'the event stream was abandoned before the run completed',
  };
};

type SettledOutcome = ConversationRecord.RecordOf<'RunSettled'>['outcome'];

/** What the sink has seen so far: text awaiting a flush, and how it ended. */
interface Pending {
  step: number;
  text: string;
  steps: number;
  usage: Stop.Usage;
  completed: boolean;
  cancelled: boolean;
}

const flush = (pending: Pending): ReadonlyArray<ConversationRecord.Record> => {
  if (pending.text === '') return [];
  const record: ConversationRecord.Record = {
    _tag: 'Text',
    step: pending.step,
    text: pending.text,
  };
  pending.text = '';
  return [record];
};

const signalOffset = (offset: string): LogOffset.Offset => {
  try {
    return LogOffset.Offset.make(offset);
  } catch (cause) {
    throw new Error(`Signal event carried an invalid log offset: ${offset}`, {
      cause,
    });
  }
};

/**
 * What one event contributes to the log, coalescing text as it goes.
 *
 * Returns an empty array for the events that are not worth a row — turn
 * starts, text framing, reasoning, provider metadata — and `Stream.tap` skips
 * the append for those, so the common case (a text delta) costs a string
 * concatenation and nothing else.
 */
const recordsFor = <Tools extends Record<string, Tool.Any>>(
  pending: Pending,
  event: AgentEvents.Event<Tools>,
): ReadonlyArray<ConversationRecord.Record> => {
  switch (event._tag) {
    case 'TurnStarted':
      return [];
    case 'TurnFinished':
      pending.steps = event.step;
      pending.usage = event.usage;
      return [
        ...flush(pending),
        { _tag: 'TurnFinished', step: event.step, usage: event.usage },
      ];
    case 'Signalled':
      if (event.kind === 'cancel') pending.cancelled = true;
      return [
        // Flushed first, so a rebuilt prompt has the model's own words before
        // the instruction that redirected it rather than after.
        ...flush(pending),
        {
          _tag: 'SignalReceived',
          kind: event.kind,
          text: event.text,
          source: event.source,
          step: event.step,
          at: signalOffset(event.at),
        },
      ];
    case 'SignalRejected':
      return [
        ...flush(pending),
        {
          _tag: 'SignalReceived',
          kind: event.kind,
          text: event.text,
          source: event.source,
          step: event.step,
          at: signalOffset(event.at),
          disposition: 'rejected',
          reason: event.reason,
        },
      ];
    case 'SignalBacklog':
      return [];
    case 'Completed':
      pending.completed = true;
      pending.steps = event.steps;
      pending.usage = event.usage;
      return [
        ...flush(pending),
        {
          _tag: 'Completed',
          outcome: event.outcome,
          text: event.text,
          steps: event.steps,
          usage: event.usage,
        },
      ];
    case 'Compacted':
      // Written by {@link compaction}, which needs the log's own offsets and
      // therefore cannot be a pure function of the event. Listed here so the
      // match stays exhaustive rather than defaulted — a new lifecycle case
      // should fail this switch, which is the whole reason it has no
      // `default`.
      return [];
    case 'Part':
      return partRecords(pending, event.step, event.part);
  }
};

// Read through the encoded shape, exactly as `agent.ts`'s `observe` does. The
// stream carries decoded parts, whose *field names* are identical for
// everything matched below except one: a decoded tool result carries both the
// handler's value (`result`) and the toolkit's encoding of it
// (`encodedResult`), and it is the latter that is written down. That is the
// form `Prompt` puts in front of the model, so it is the only one a resuming
// dispatch can serve back without changing what the model sees — and it is
// already JSON, so a success type holding a `Date` no longer fails the append
// on its way to storage.
const partRecords = <Tools extends Record<string, Tool.Any>>(
  pending: Pending,
  step: number,
  part: Response.StreamPart<Tools>,
): ReadonlyArray<ConversationRecord.Record> => {
  const encoded = part as Response.StreamPartEncoded;
  switch (encoded.type) {
    case 'text-delta':
      pending.step = step;
      pending.text += encoded.delta;
      return [];
    case 'tool-call':
      return [
        ...flush(pending),
        {
          _tag: 'ToolCall',
          step,
          id: LogVocabulary.ToolCallId.make(encoded.id),
          name: encoded.name,
          params: encoded.params,
        },
      ];
    case 'tool-result':
      return [
        ...flush(pending),
        {
          _tag: 'ToolOutcome',
          step,
          id: LogVocabulary.ToolCallId.make(encoded.id),
          name: encoded.name,
          outcome: encoded.isFailure ? 'failure' : 'success',
          result: (part as Response.ToolResultPart<string, unknown, unknown>)
            .encodedResult,
        },
      ];
    default:
      return [];
  }
};

const orDie = <A, R>(
  effect: Effect.Effect<A, LogStore.LogStoreError, R>,
): Effect.Effect<A, never, R> =>
  Effect.catchTag(effect, '@sunfall/vesper-log/LogStoreError', (error) =>
    Effect.die(
      new Error(
        `Conversation log ${error.operation} failed (${error.reason}) for ${error.path}: ${error.detail}` +
          (error.reason === 'encoding'
            ? ' — a tool parameter or result did not survive JSON encoding.'
            : ''),
        { cause: error },
      ),
    ),
  );

export * as AgentLog from './log.js';
