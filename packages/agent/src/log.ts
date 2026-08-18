import { AttachmentStore } from '@sunfall/vesper-attachments/attachment-store';
import { LogStore } from '@sunfall/vesper-log/log-store';
import { LogOffset } from '@sunfall/vesper-log/offset';
import { ConversationRecord, FORMAT_VERSION } from '@sunfall/vesper-log/record';
import { Tail } from '@sunfall/vesper-log/tail';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';
import {
  Clock,
  Crypto,
  Effect,
  Exit,
  Option,
  Ref,
  Schema,
  Semaphore,
  Stream,
  SynchronizedRef,
} from 'effect';
import { Prompt } from 'effect/unstable/ai';

import { AgentBranch } from './branch.js';
import {
  CompatibilityError,
  SuspendedConversationError,
} from './conversation-error.js';
import { AgentHistory } from './history.js';
import * as AgentIds from './internal/ids.js';
import * as Observability from './internal/observability.js';
import { PromptTransport } from './prompt-transport.js';
import * as RecoveryState from './recovery.js';
import { ResumeProjection } from './resume-projection.js';

export {
  CompatibilityError,
  SuspendedConversationError,
} from './conversation-error.js';
import * as AgentSignals from './internal/signal-store.js';
import type { Stop } from './stop.js';
import { RecordingPolicyRuntime } from './recording-policy-runtime.js';
import * as RecordingSink from './recording-sink.js';

/**
 * Where a conversation's stream lives.
 *
 * One function, exported, because the writer and `Conversation.follow`
 * have to agree and a convention that lives in two places is a convention
 * that eventually differs. The prefix leaves room for streams that are not
 * conversations — `signals/<id>`, and a per-agent identity stream if one ever
 * lands — without a collision that would only surface as one log interleaved
 * into another.
 */
export const pathFor = (conversationId: LogVocabulary.ConversationId): string =>
  `conversations/${conversationId}`;

/** @internal Replay existing records and follow future appends. */
export const follow = (
  conversationId: LogVocabulary.ConversationId,
  after: LogOffset.Offset = LogOffset.START,
): Stream.Stream<
  ConversationRecord.Envelope,
  LogStore.LogStoreError,
  LogStore.Service
> => Tail.from(pathFor(conversationId), after);

/** @internal Read a finite snapshot of the records currently stored. */
export const snapshot = (
  conversationId: LogVocabulary.ConversationId,
  after: LogOffset.Offset = LogOffset.START,
): Stream.Stream<
  ConversationRecord.Envelope,
  LogStore.LogStoreError,
  LogStore.Service
> =>
  Stream.unwrap(
    Effect.map(LogStore.Service, (store) => {
      const path = pathFor(conversationId);
      const page = (
        cursor: LogOffset.Offset,
      ): Stream.Stream<ConversationRecord.Envelope, LogStore.LogStoreError> =>
        Stream.unwrap(
          Effect.map(store.read(path, { after: cursor }), (read) =>
            Stream.concat(
              Stream.fromIterable(read.records),
              read.upToDate ? Stream.empty : page(read.cursor),
            ),
          ),
        );
      return page(after);
    }),
  );

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

// Recovery types live in the private recovery module and are re-exported below
// so AgentLog's public type shape remains stable without exposing the Module.
/** How a tool call ended, as a previous run recorded it. */
export type Settled = RecoveryState.Settled;
/** What an orphaned run durably established about a tool call. */
export type Recovery = RecoveryState.Recovery;
/** An orphaned handler start and the provider call that originally caused it. */
export type IndeterminateToolCall = RecoveryState.IndeterminateToolCall;
/** A deliberately suspended call and its external wait identity. */
export type SuspendedToolCall = RecoveryState.SuspendedToolCall;

/** A signal this run has taken delivery of. */
export interface Delivered {
  readonly kind: 'steer' | 'cancel';
  readonly text: string;
  readonly source: string;
  /** The signal's offset in the signal stream. */
  readonly at: LogOffset.Offset;
}

export interface SignalDrain {
  readonly signals: ReadonlyArray<Delivered>;
  /** More signals remain after this bounded page. */
  readonly backlog: boolean;
}

/** A conversation checkpoint could not be made durable. */
export class DurabilityError extends Schema.TaggedError<DurabilityError>(
  '@sunfall/vesper-agent/DurabilityError',
)('DurabilityError', {
  source: Schema.Literals(['log', 'attachment', 'timeout']),
  operation: Schema.String,
  reason: Schema.String,
  detail: Schema.String,
  cause: Schema.Defect(),
}) {}

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
  /** Explicitly re-enter a suspended provider call with a fresh wait token. */
  readonly pendingWait?: 'restart';
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
  readonly pendingWait?: 'restart';
  readonly seed?: ReadonlyArray<ConversationRecord.Envelope>;
  readonly identity?: string;
  readonly compatibility: Compatibility;
}

/** Durable identity required before a definition may continue history. */
export interface Compatibility {
  readonly agent: string;
  readonly revision: LogVocabulary.AgentRevision;
}

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
 * Appends expose {@link DurabilityError} as an ordinary typed failure, so
 * callers can distinguish durable infrastructure from model and application
 * failures. Reads retain the historic defect boundary because they back
 * synchronous views. Attachment persistence is opt-in through
 * `AttachmentStore.Service`; without it, prompts retain the inline transport.
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

  /** @internal Bounded aggregate suffix used to restore recorded state. */
  readonly stateHistory: ReadonlyArray<ConversationRecord.Envelope>;

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
  ) => Effect.Effect<void, DurabilityError>;

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
  readonly pendingToolCalls: ReadonlyArray<IndeterminateToolCall>;

  /** @internal Indeterminate calls requiring an application decision. */
  readonly indeterminateToolCalls: ReadonlyArray<IndeterminateToolCall>;

  /** @internal Deliberately suspended calls, safe for workflow replay. */
  readonly suspendedToolCalls: ReadonlyArray<SuspendedToolCall>;

  /** @internal Corrupt recovery state that cannot be safely reconciled. */
  readonly recoveryCorruption: string | undefined;

  /** @internal Whether an external wait result is already durably audited. */
  readonly hasCompletedWait: (token: string) => boolean;

  /** @internal Why an open tool call prevents this run from settling. */
  readonly pendingToolState: Effect.Effect<RecoveryState.PendingToolState>;

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
  ) => Effect.Effect<
    Session,
    | CompatibilityError
    | SuspendedConversationError
    | LogStore.LogStoreError
    | DurabilityError,
    Crypto.Crypto
  >;
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
 * after the recovery fold ran would leave the crashed run the user branched
 * *away from* still holding the recovery index, and the new run would be
 * served tool results answering calls that are no longer in its prompt.
 */
export const open: (
  conversationId: LogVocabulary.ConversationId,
  options: OpenOptions,
) => Effect.Effect<
  Session,
  | CompatibilityError
  | SuspendedConversationError
  | LogStore.LogStoreError
  | DurabilityError,
  LogStore.Service | Crypto.Crypto
> = Effect.fn('AgentLog.open')(function* (conversationId, options) {
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
export const fork: (
  conversationId: LogVocabulary.ConversationId,
  at: LogOffset.Offset,
  forkConversationId: LogVocabulary.ConversationId,
  compatibility: Compatibility,
  pendingWait?: 'restart',
) => Effect.Effect<
  Session,
  | CompatibilityError
  | SuspendedConversationError
  | LogStore.LogStoreError
  | DurabilityError,
  LogStore.Service | Crypto.Crypto
> = Effect.fn('AgentLog.fork')(
  function* (
    conversationId,
    at,
    forkConversationId,
    compatibility,
    pendingWait,
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
    yield* validateSuspendedBoundary(conversationId, prefix, pendingWait);
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
      ...(pendingWait === undefined ? {} : { pendingWait }),
    });
  },
);

const restartWaits = (
  history: ReadonlyArray<ConversationRecord.Envelope>,
): ReadonlyArray<ConversationRecord.RecordOf<'ToolWaitRestarted'>> =>
  RecoveryState.fold(history).suspended.map((suspended) => ({
    _tag: 'ToolWaitRestarted',
    id: suspended.toolCallId,
    name: suspended.name,
    wait: suspended.wait,
    priorToken: suspended.token,
  }));

const validateSuspendedBoundary = (
  conversationId: LogVocabulary.ConversationId,
  history: ReadonlyArray<ConversationRecord.Envelope>,
  pendingWait: 'restart' | undefined,
): Effect.Effect<void, SuspendedConversationError> => {
  const suspended = RecoveryState.fold(history).suspended[0];
  return suspended === undefined || pendingWait === 'restart'
    ? Effect.void
    : Effect.fail(
        new SuspendedConversationError({
          message:
            `Conversation ${conversationId} cannot branch or fork while ` +
            `tool ${suspended.name} (${suspended.toolCallId}) is waiting at ` +
            `"${suspended.wait}"; choose a boundary before the tool started ` +
            'or after it records ToolOutcome',
          conversationId,
          toolCallId: suspended.toolCallId,
          wait: suspended.wait,
        }),
      );
};

const openWith = (
  store: LogStore.Interface,
  conversationId: LogVocabulary.ConversationId,
  options: ClaimOptions,
): Effect.Effect<
  Session,
  | CompatibilityError
  | SuspendedConversationError
  | LogStore.LogStoreError
  | DurabilityError,
  Crypto.Crypto
> =>
  Effect.gen(function* () {
    // Attachments are an explicit opt-in service. Without it, prompts retain
    // the existing inline transport and no attachment dependency is required.
    const attachmentStore = Option.getOrUndefined(
      yield* Effect.serviceOption(AttachmentStore.Service),
    );
    yield* validateCompatibilityInput(options.compatibility);
    const path = pathFor(conversationId);
    const identity = options?.identity ?? conversationId;

    yield* store.create(path, identity).pipe(
      Effect.asVoid,
      Effect.catchIf(
        (error) => error.reason === 'conflict',
        () => Effect.void,
      ),
    );

    if (options?.identity !== undefined) {
      const meta = yield* store.meta(path);
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
      const observed = Option.getOrThrow(yield* store.meta(path));
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
      if (options.branchFrom !== undefined) {
        yield* validateSuspendedBoundary(
          conversationId,
          retainedBeforeClaim,
          options.pendingWait,
        );
      }
      // Validate the active prompt before fencing the current producer. This
      // is deliberately the same bounded suffix used for compatibility: a
      // changed head is rejected by acquire and the post-claim full active
      // path check below catches any prompt records outside the suffix.
      yield* validatePromptHistory(
        retainedBeforeClaim,
        options.compatibility,
        attachmentStore,
      );
      const acquired = yield* store
        .acquire(path, yield* AgentIds.producerId, {
          epoch: observed.epoch,
          head: observed.head,
        })
        .pipe(Effect.exit);
      if (Exit.isSuccess(acquired)) {
        claim = acquired.value;
        break;
      } else {
        const error = Exit.findErrorOption(acquired);
        if (Option.isNone(error)) {
          return yield* Effect.die(acquired.cause);
        }
        if (error.value.reason !== 'conflict') {
          return yield* Effect.fail(error.value);
        }
        lastConflict = error.value;
      }
    }
    if (claim === undefined) {
      return yield* lastConflict === undefined
        ? Effect.die(new Error('compare-and-acquire retry exhausted'))
        : Effect.fail(lastConflict);
    }
    const sequence = yield* SynchronizedRef.make(claim.nextSequence);
    const childLock = yield* Semaphore.make(1);

    const append: Session['append'] = (records, timeoutMillis) =>
      records.length === 0
        ? Effect.void
        : SynchronizedRef.modifyEffect(sequence, (next) =>
            Effect.uninterruptibleMask((restore) =>
              Effect.gen(function* () {
                const timestamp = yield* Clock.currentTimeMillis;
                const persistedRecords = yield* Effect.forEach(
                  records,
                  (
                    record,
                  ): Effect.Effect<
                    ConversationRecord.Record,
                    DurabilityError
                  > => {
                    if (record._tag !== 'RunStarted') {
                      return Effect.succeed(record);
                    }
                    if (attachmentStore === undefined) {
                      return Effect.succeed({
                        ...record,
                        prompt: PromptTransport.encode(record.prompt),
                      });
                    }
                    return PromptTransport.encodeWithAttachments(
                      record.prompt,
                    ).pipe(
                      Effect.provideService(
                        AttachmentStore.Service,
                        attachmentStore,
                      ),
                      Effect.mapError(attachmentDurabilityError),
                      Effect.map((prompt) => ({ ...record, prompt })),
                    );
                  },
                );
                const persist = store
                  .append({
                    path,
                    producerId: claim.producerId,
                    epoch: claim.epoch,
                    sequence: next,
                    records: persistedRecords.map((record) => ({
                      conversationId,
                      timestamp,
                      // Policy wrappers run outside this append; transport is
                      // therefore the last step before store preparation.
                      record:
                        record._tag === 'RunStarted'
                          ? {
                              ...record,
                              prompt: record.prompt,
                            }
                          : record,
                    })),
                  })
                  .pipe(Effect.mapError(logDurabilityError));

                // Keep the backend interruptible (and optionally bounded),
                // then resume masking before advancing the local sequence.
                yield* timeoutMillis === undefined
                  ? restore(persist)
                  : restore(persist).pipe(
                      Effect.timeout(Math.max(1, timeoutMillis - 1)),
                      Effect.mapError((error) =>
                        error._tag === 'DurabilityError'
                          ? error
                          : new DurabilityError({
                              source: 'timeout',
                              operation: 'append',
                              reason: 'timeout',
                              detail: `Conversation append exceeded ${Math.max(1, timeoutMillis - 1)}ms`,
                              cause: error,
                            }),
                      ),
                    );

                // SynchronizedRef commits this next value only after the
                // restored append succeeds. A failed append therefore reuses
                // the sequence — which the store answers idempotently when
                // the batch digest matches, and rejects when it does not. Its
                // permit covers both operations, so concurrent signal, event,
                // and child writes cannot submit different batches under one
                // producer key.
                return [
                  undefined,
                  LogVocabulary.ProducerSequence.make(next + 1),
                ] as const;
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
      yield* append([
        { _tag: 'BranchedFrom', at: branchFrom },
        ...(options.pendingWait === 'restart' ? restartWaits(prefix) : []),
      ]);
    }
    if (options?.seed !== undefined) {
      yield* seedInto(options.seed, append, readAll(store, path));
      if (options.pendingWait === 'restart') {
        const existing = yield* readAll(store, path);
        const restarted = new Set(
          existing.flatMap(({ record }) =>
            record._tag === 'ToolWaitRestarted' ? [record.priorToken] : [],
          ),
        );
        yield* append(
          restartWaits(options.seed).filter(
            (record) => !restarted.has(record.priorToken),
          ),
        );
      }
    }

    const opened = yield* loadOpenState(store, path);
    const history = yield* hydrateHistory(opened.history, attachmentStore).pipe(
      Effect.mapError((error) =>
        compatibilityError(
          options.compatibility,
          {},
          `malformed persisted attachment: ${error.message}`,
        ),
      ),
    );
    // Prompt parsing is a typed open failure. Keeping it here means a caller
    // never receives a claimed session whose first continuation would defect
    // while rebuilding malformed durable messages.
    yield* validatePromptHistory(
      history,
      options.compatibility,
      attachmentStore,
    );
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
    const recovered = RecoveryState.fold(opened.aggregateSuffix);
    const toolRecovery = yield* RecoveryState.make(recovered);
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
        // Reading is not acknowledgement. The cursor advances only when the
        // corresponding SignalReceived record is durable in trackedAppend,
        // preserving at-least-once delivery if this stream is interrupted.
        const { drain } = yield* readSignalPage(limit);
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
    ): Effect.Effect<
      Session,
      | CompatibilityError
      | SuspendedConversationError
      | LogStore.LogStoreError
      | DurabilityError,
      Crypto.Crypto
    > =>
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
        .pipe(
          Effect.withSpan('AgentLog.Session.child', {
            attributes: {
              'vesper.conversation.id': conversationId,
              'vesper.child.agent': options.agent,
              'vesper.child.conversation.id': childIdFor(
                conversationId,
                options.toolCallId,
              ),
              'vesper.child.depth': options.depth,
            },
          }),
        );

    const meta = yield* store.meta(path);
    const inheritedUsage = Option.isSome(meta)
      ? (parseForkIdentity(meta.value.identity)?.inheritedUsage ?? {
          input: 0,
          output: 0,
        })
      : { input: 0, output: 0 };

    const initialResume = ResumeProjection.activeFrom(history);
    const resume = yield* Ref.make(initialResume);
    const projectionHistory = mergeByOffset(opened.aggregateSuffix, history);
    const state = yield* Ref.make(
      ResumeProjection.stateFrom(projectionHistory),
    );
    const projectionLock = yield* Semaphore.make(1);

    const trackedAppend: Session['append'] = (records, timeoutMillis) =>
      projectionLock.withPermits(1)(
        Effect.gen(function* () {
          let persisted = records;
          const currentResume = yield* Ref.get(resume);
          const currentState = yield* Ref.get(state);
          const currentSignalCursor = yield* Ref.get(signalCursor);
          const nextSignalCursor = records.reduce(
            (cursor, record) =>
              record._tag === 'SignalReceived' &&
              LogOffset.isAfter(record.at, cursor)
                ? record.at
                : cursor,
            currentSignalCursor,
          );
          const settlementIndex = records.findIndex(
            (record) => record._tag === 'RunSettled',
          );
          const settlement = records[settlementIndex];
          if (settlement?._tag === 'RunSettled') {
            const beforeSettlement = records.slice(0, settlementIndex);
            const settlementResume = beforeSettlement.reduce(
              ResumeProjection.update,
              currentResume,
            );
            const settlementState = beforeSettlement.reduce(
              ResumeProjection.updateState,
              currentState,
            );
            const resumeSnapshot = resumeState(
              options.compatibility,
              addUsage(opened.usage, settlement.usage),
              nextSignalCursor,
              settlementResume.completed,
              settlementResume.latestTurnUsage,
              settlementState,
            );
            persisted = records.map((record, index) =>
              index === settlementIndex
                ? { ...settlement, resume: resumeSnapshot }
                : record,
            );
          }

          yield* append(persisted, timeoutMillis);
          yield* Effect.forEach(
            records,
            (record) => {
              switch (record._tag) {
                case 'ToolSuspended':
                  return Observability.waitSuspended;
                case 'ToolWaitCompleted':
                  return Observability.waitCompleted;
                case 'ToolWaitRestarted':
                  return Observability.waitRestarted;
                default:
                  return Effect.void;
              }
            },
            { discard: true },
          );
          yield* Ref.set(signalCursor, nextSignalCursor);
          yield* Ref.set(
            state,
            records.reduce(ResumeProjection.updateState, currentState),
          );
          yield* Ref.set(
            resume,
            records.reduce(ResumeProjection.update, currentResume),
          );
          yield* toolRecovery.track(records);
        }),
      );

    return {
      [SessionTypeId]: SessionTypeId,
      conversationId,
      compatibility: options.compatibility,
      inheritedUsage,
      usage: opened.usage,
      latestTurnUsage: initialResume.latestTurnUsage,
      completed: initialResume.completed,
      settlementTimeoutMillis: SETTLEMENT_TIMEOUT_MILLIS,
      history,
      stateHistory: projectionHistory,
      recorded: orDie(readResumeHistory(store, path)),
      append: trackedAppend,
      recovery: toolRecovery.recovery,
      pendingToolCalls: toolRecovery.pendingToolCalls,
      indeterminateToolCalls: toolRecovery.indeterminateToolCalls,
      suspendedToolCalls: toolRecovery.suspendedToolCalls,
      recoveryCorruption: toolRecovery.recoveryCorruption,
      hasCompletedWait: toolRecovery.hasCompletedWait,
      pendingToolState: toolRecovery.pendingToolState,
      hasPendingToolCalls: toolRecovery.hasPendingToolCalls,
      onToolSettled: toolRecovery.onToolSettled,
      drainSignalsBounded: (limit) => orDie(drainSignalsBounded(limit)),
      signalPages,
      child,
    } satisfies Session;
  });

/** Attach a compiled persistence policy without changing the run's live values. */
export const withRecordingPolicy = (
  session: Session,
  recordingPolicy: RecordingPolicyRuntime.Runtime,
): Session => {
  const append: Session['append'] = (records) =>
    Effect.forEach(records, recordingPolicy.filter).pipe(
      Effect.flatMap(session.append),
    );
  return {
    ...session,
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
): Effect.Effect<
  ReadonlyArray<ConversationRecord.Envelope>,
  LogStore.LogStoreError
> =>
  Effect.gen(function* () {
    const all: ConversationRecord.Envelope[] = [];
    let cursor = LogOffset.START;
    let done = false;

    while (!done) {
      const page = yield* store.read(path, { after: cursor });
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
): Effect.Effect<OpenState, LogStore.LogStoreError> =>
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
): Effect.Effect<
  ReadonlyArray<ConversationRecord.Envelope>,
  LogStore.LogStoreError
> =>
  Effect.gen(function* () {
    const newest: ConversationRecord.Envelope[] = [];
    let before: LogOffset.Offset | undefined;
    let done = false;
    while (!done) {
      const page = yield* store.readBackwards(path, {
        ...(before === undefined ? {} : { before }),
        limit: RESUME_READ_LIMIT,
      });
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

/** Validate untrusted durable prompts before they reach the synchronous fold. */
const hydrateHistory = (
  history: ReadonlyArray<ConversationRecord.Envelope>,
  attachmentStore: AttachmentStore.Interface | undefined,
): Effect.Effect<
  ReadonlyArray<ConversationRecord.Envelope>,
  PromptTransport.DecodeError | AttachmentStore.GetError
> =>
  attachmentStore === undefined
    ? Effect.succeed(history)
    : Effect.forEach(history, (envelope) =>
        envelope.record._tag !== 'RunStarted'
          ? Effect.succeed(envelope)
          : PromptTransport.decodeWithAttachments(envelope.record.prompt).pipe(
              Effect.provideService(AttachmentStore.Service, attachmentStore),
              Effect.map((prompt) => ({
                ...envelope,
                record: {
                  ...envelope.record,
                  // Keep hydrated bytes in the in-memory history. The durable
                  // log remains content-addressed; this view is what the
                  // synchronous prompt rebuild hands to the model.
                  prompt,
                },
              })),
            ),
      );

const validatePromptHistory = (
  history: ReadonlyArray<ConversationRecord.Envelope>,
  expected: Compatibility,
  attachmentStore?: AttachmentStore.Interface,
): Effect.Effect<void, CompatibilityError> =>
  Effect.gen(function* () {
    for (const { record } of AgentBranch.activePath(history)) {
      if (record._tag === 'RunStarted') {
        const decode =
          attachmentStore === undefined
            ? PromptTransport.decodeMessages(record.prompt)
            : PromptTransport.decodeMessagesWithAttachments(record.prompt).pipe(
                Effect.provideService(AttachmentStore.Service, attachmentStore),
              );
        yield* decode;
      }
    }
    yield* Effect.try({
      try: () => AgentHistory.messagesFrom(history),
      catch: (cause) =>
        new PromptTransport.DecodeError({
          message: `Malformed persisted prompt messages: ${
            cause instanceof Error ? cause.message : String(cause)
          }`,
        }),
    });
  }).pipe(
    Effect.asVoid,
    Effect.mapError((error) =>
      compatibilityError(
        expected,
        {},
        `malformed persisted prompt: ${error.message}`,
      ),
    ),
  );

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
): Effect.Effect<
  ReadonlyArray<ConversationRecord.Envelope>,
  LogStore.LogStoreError
> =>
  Effect.gen(function* () {
    const newest: ConversationRecord.Envelope[] = [];
    let before: LogOffset.Offset | undefined;
    let boundary: LogOffset.Offset | undefined;
    let done = false;

    while (!done) {
      const page = yield* store.readBackwards(path, {
        ...(before === undefined ? {} : { before }),
        limit: RESUME_READ_LIMIT,
      });
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
  state: ConversationRecord.RecordOf<'StateCheckpoint'> | undefined,
) => ({
  formatVersion: FORMAT_VERSION,
  agent: compatibility.agent,
  agentRevision: compatibility.revision,
  usage,
  signalCursor,
  ...(completed === undefined ? {} : { completed }),
  ...(latestTurnUsage === undefined ? {} : { latestTurnUsage }),
  ...(state === undefined
    ? {}
    : { state: { id: state.id, version: state.version, value: state.value } }),
});

const addUsage = (left: Stop.Usage, right: Stop.Usage): Stop.Usage => ({
  input: left.input + right.input,
  output: left.output + right.output,
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
  Schema.Natural,
  Schema.Natural,
  Schema.Natural,
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
): Effect.Effect<void, DurabilityError> => {
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
  recorded: Effect.Effect<
    ReadonlyArray<ConversationRecord.Envelope>,
    LogStore.LogStoreError
  >,
): Effect.Effect<void, LogStore.LogStoreError | DurabilityError> =>
  Effect.gen(function* () {
    const reseated = new Map<LogOffset.Offset, LogOffset.Offset>();
    let written = yield* recorded;

    for (
      let index = 0;
      index < Math.min(written.length, prefix.length);
      index += 1
    ) {
      const source = prefix[index]!;
      const expected = reseat(source.record, reseated);
      if (JSON.stringify(written[index]!.record) !== JSON.stringify(expected)) {
        return yield* Effect.die(
          new Error(`Fork destination seed differs at record ${index}`),
        );
      }
      reseated.set(source.offset, written[index]!.offset);
    }

    // A matching fork identity may be opened again after its independent run
    // has appended beyond the copied prefix. Validate the entire seed above,
    // then leave those genuine destination records untouched.
    if (written.length >= prefix.length) return;

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
    case 'ToolSuspended':
    case 'ToolResumed':
    case 'ToolWaitCompleted':
    case 'ToolWaitRestarted':
    case 'ToolOutcome':
    case 'StateCheckpoint':
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
): Effect.Effect<void, DurabilityError> =>
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

export const record = RecordingSink.record;
export const SETTLEMENT_TIMEOUT_MILLIS =
  RecordingSink.SETTLEMENT_TIMEOUT_MILLIS;

const orDie = <A, R>(
  effect: Effect.Effect<A, LogStore.LogStoreError, R>,
): Effect.Effect<A, never, R> =>
  Effect.catchTag(effect, 'LogStoreError', (error) =>
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

const logDurabilityError = (error: LogStore.LogStoreError): DurabilityError =>
  new DurabilityError({
    source: 'log',
    operation: error.operation,
    reason: error.reason,
    detail: error.detail,
    cause: error,
  });

const attachmentDurabilityError = (
  error: AttachmentStore.AttachmentStoreError,
): DurabilityError =>
  new DurabilityError({
    source: 'attachment',
    operation: error.operation,
    reason: 'storage',
    detail:
      error.cause instanceof Error ? error.cause.message : String(error.cause),
    cause: error,
  });
