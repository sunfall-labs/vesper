import { AttachmentStore } from '@sunfall/vesper-attachments/attachment-store';
import type { LogStore } from '@sunfall/vesper-log/log-store';
import { LogOffset } from '@sunfall/vesper-log/offset';
import type { ConversationRecord } from '@sunfall/vesper-log/record';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';
import {
  Clock,
  Effect,
  Exit,
  Option,
  Ref,
  Semaphore,
  Stream,
  SynchronizedRef,
} from 'effect';
import type { Crypto } from 'effect';

import {
  DurabilityError,
  SuspendedConversationError,
  type CompatibilityError,
} from '../conversation-error.js';
import { AgentBranch } from '../branch.js';
import type {
  ChildOptions,
  Compatibility,
  Delivered,
  Session,
  SignalDrain,
} from '../log.js';
import * as AgentIds from './ids.js';
import * as Observability from './observability.js';
import { PromptTransport } from '../prompt-transport.js';
import * as RecoveryState from '../recovery.js';
import { ResumeProjection } from '../resume-projection.js';
import * as AgentSignals from './signal-store.js';
import * as RecordingSink from '../recording-sink.js';
import {
  compatibilityError,
  hydrateHistory,
  validateCompatibility,
  validateCompatibilityInput,
  validatePromptHistory,
} from './compatibility.js';
import { childIdFor, pathFor } from './conversation-stream.js';
import {
  ensureChildReference,
  parseForkIdentity,
  seedInto,
} from './fork-seed.js';
import {
  addUsage,
  loadOpenState,
  mergeByOffset,
  readAggregateSuffix,
  readAll,
  readResumeHistory,
  resumeState,
} from './resume-read.js';

// The session claim: create-or-continue one conversation stream, validate its
// compatibility, acquire the single producer, and hand back the `Session`
// value everything else reaches through. `log.ts` keeps the public `open` and
// `fork` entry points and their documentation; this module is the machinery
// they share.

export const SessionTypeId: unique symbol = Symbol.for(
  '@sunfall/vesper-agent/AgentLog.Session',
);

/**
 * What {@link openWith} may write between claiming a stream and reading it.
 *
 * Both fields describe the same manoeuvre from two sides: put something into
 * the stream *before* the history read, so that everything derived from that
 * read — the prompt, the recovery index, the signal cursor — describes the
 * conversation the caller meant rather than the one the records literally
 * came from. `branchFrom` writes one marker into an existing conversation;
 * `seed` writes a copied prefix into a new one. `AgentLog.open` documents why
 * the ordering is the whole trick, and `AgentLog.fork` the copy.
 *
 * Internal, and structurally a supertype of `AgentLog.OpenOptions`, so the
 * public `open` passes its own options straight through.
 */
export interface ClaimOptions {
  readonly branchFrom?: LogOffset.Offset;
  readonly pendingWait?: 'restart';
  readonly seed?: ReadonlyArray<ConversationRecord.Envelope>;
  readonly identity?: string;
  readonly compatibility: Compatibility;
}

const ACQUIRE_ATTEMPTS = 4;

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

export const validateSuspendedBoundary = (
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

export const openWith = (
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
      const branchFrom = options.branchFrom;
      const retainedBeforeClaim =
        branchFrom === undefined
          ? AgentBranch.activePath(existing)
          : AgentBranch.activePath(
              existing.filter(
                (envelope) => !LogOffset.isAfter(envelope.offset, branchFrom),
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
          return yield* error.value;
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
                              detail: `Conversation append exceeded ${String(Math.max(1, timeoutMillis - 1))}ms`,
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
      childOptions: ChildOptions,
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
              childOptions.toolCallId,
            );
            const reference: ConversationRecord.RecordOf<'ChildSession'> = {
              _tag: 'ChildSession',
              toolCallId: childOptions.toolCallId,
              agent: childOptions.agent,
              parentConversationId: conversationId,
              childConversationId,
              depth: childOptions.depth,
            };

            yield* ensureChildReference(
              conversationId,
              yield* readAll(store, path),
              reference,
              append,
            );
            const session = yield* openWith(store, childConversationId, {
              compatibility: {
                agent: childOptions.agent,
                revision: childOptions.revision,
                digest: childOptions.digest,
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
              'vesper.child.agent': childOptions.agent,
              'vesper.child.conversation.id': childIdFor(
                conversationId,
                childOptions.toolCallId,
              ),
              'vesper.child.depth': childOptions.depth,
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
    const codeState = yield* Ref.make(
      ResumeProjection.codeStateFrom(projectionHistory),
    );
    const projectionLock = yield* Semaphore.make(1);

    const trackedAppend: Session['append'] = (records, timeoutMillis) =>
      projectionLock.withPermits(1)(
        Effect.gen(function* () {
          let persisted = records;
          const currentResume = yield* Ref.get(resume);
          const currentState = yield* Ref.get(state);
          const currentCodeState = yield* Ref.get(codeState);
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
            const settlementCodeState = beforeSettlement.reduce(
              ResumeProjection.updateCodeState,
              currentCodeState,
            );
            const resumeSnapshot = resumeState(
              options.compatibility,
              addUsage(opened.usage, settlement.usage),
              nextSignalCursor,
              settlementResume.completed,
              settlementResume.latestTurnUsage,
              settlementState,
              settlementCodeState,
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
            (record) =>
              record._tag === 'ToolSuspended'
                ? Observability.waitSuspended
                : record._tag === 'ToolWaitCompleted'
                  ? Observability.waitCompleted
                  : record._tag === 'ToolWaitRestarted'
                    ? Observability.waitRestarted
                    : Effect.void,
            { discard: true },
          );
          yield* Ref.set(signalCursor, nextSignalCursor);
          yield* Ref.set(
            state,
            records.reduce(ResumeProjection.updateState, currentState),
          );
          yield* Ref.set(
            codeState,
            records.reduce(ResumeProjection.updateCodeState, currentCodeState),
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
      settlementTimeoutMillis: RecordingSink.SETTLEMENT_TIMEOUT_MILLIS,
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
      completedWait: toolRecovery.completedWait,
      pendingToolState: toolRecovery.pendingToolState,
      hasPendingToolCalls: toolRecovery.hasPendingToolCalls,
      onToolSettled: toolRecovery.onToolSettled,
      drainSignalsBounded: (limit) => orDie(drainSignalsBounded(limit)),
      signalPages,
      child,
    } satisfies Session;
  });

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
