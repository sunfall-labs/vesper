import { LogStore } from '@sunfall/vesper-log/log-store';
import type { ConversationRecord } from '@sunfall/vesper-log/record';
import type { LogVocabulary } from '@sunfall/vesper-log/vocabulary';
import { Clock, Effect, Schema, Semaphore, type Crypto } from 'effect';

import * as AgentIds from './ids.js';

/** Out-of-band input addressed to a durable conversation. */
export const Signal = Schema.Struct({
  kind: Schema.Literals(['steer', 'cancel']),
  /** Steering text, or a cancellation reason. */
  text: Schema.String,
  /** User or service that sent the signal. */
  source: Schema.String,
});
export type Signal = typeof Signal.Type;

/** Hard ingress ceiling; a run policy may impose a smaller per-run limit. */
export const MAX_SIGNAL_BYTES = 256 * 1024;

// A process-local lock prevents two ordinary senders from fencing one another
// between acquire and append. It cannot coordinate separate processes; the
// durable producer epoch remains the authority there.
const APPEND_MUTEX = Semaphore.makeUnsafe(1);

const utf8Length = (text: string): number => {
  let bytes = 0;
  for (const character of text) {
    const codePoint = character.codePointAt(0);
    if (codePoint === undefined) {
      continue;
    }
    bytes +=
      codePoint <= 0x7f
        ? 1
        : codePoint <= 0x7ff
          ? 2
          : codePoint <= 0xffff
            ? 3
            : 4;
  }
  return bytes;
};

// Out-of-band input to a running conversation: steering, and cancel.
//
// ## Why this is a second stream and not a second record in the first one
//
// The conversation stream is producer-fenced — one writer, and a second one
// `acquire`ing takes the stream away from whoever held it. That is exactly
// what you want for two competing runs and exactly what you must not do to a
// run you are trying to steer: an outsider appending a signal to the
// conversation would fence the agent, whose next append then fails and whose
// run dies. So signals live at `signals/<conversationId>`, which the run only
// ever *reads*, and delivery is mirrored back into the conversation as a
// `SignalReceived` record.
//
// ## Why draining is a read, not a receive
//
// A production system this design came out of built the same mechanism twice
// on top of a durable workflow engine, and both attempts failed the same way.
// The first looped the engine's blocking `recv` in a workflow body; the number
// of receives was a function of how many messages happened to be queued, which
// is external state, so a replayed workflow issued a different number of
// recorded operations than the original and the engine aborted the turn. The
// second moved to a table drained by a `DELETE … RETURNING` inside a step,
// which fixed the loop and moved the same problem to the step's position in
// the recorded operation log.
//
// The lesson generalises past any one engine: **do not build delivery out of
// an operation whose call count is itself recorded state.** A drain here is one
// `read` from a cursor. Its result is a pure function of what is durably in
// the signal stream and where this conversation says it had got to — no
// blocking receive, no count that depends on timing, and re-running it is
// indistinguishable from running it once. Nothing about it needs a workflow
// engine's cooperation, which is why it cannot desynchronise from one.
//
// Delivery is at-least-once. A run that drains and then dies before the
// `SignalReceived` record lands sees the signal again, because the resume
// cursor is derived from those records. For steering that is the right side
// to err on: a repeated instruction is visible and recoverable, a dropped one
// is neither.
//
// Sending has no idempotency key. If a caller loses the response after the
// append may have committed, retrying can duplicate the signal; callers that
// need exactly-once user-visible actions must deduplicate at their boundary.

/**
 * Where a conversation's signals live.
 *
 * Exported for the same reason `AgentLog.pathFor` is: the sender and the
 * drain have to agree, and a convention in two places is one that eventually
 * differs.
 */
export const pathFor = (conversationId: LogVocabulary.ConversationId): string =>
  `signals/${conversationId}`;

/**
 * Send a signal to a conversation, whether or not anything is running.
 *
 * Fails rather than dies, unlike every write on the recording path. A sender
 * is an ordinary caller — an HTTP handler, an operator tool — and the
 * failures it can hit are ones it can act on: `fenced` means another sender
 * claimed the stream in between and this one should retry, which is the whole
 * reason the error channel is not swallowed here.
 *
 * A signal sent while nothing is running is not lost. The next run resumes
 * draining from where its conversation says delivery got to, so a steer
 * queued in advance is delivered by the next run. A valid queued cancel may
 * stop its first provider call before it begins; its durable acknowledgement
 * still occurs through the ordinary first-turn boundary drain.
 */
/** @internal */
export const append = Effect.fn('AgentSignals.append')(function* (
  conversationId: LogVocabulary.ConversationId,
  signal: Signal,
) {
  const path = pathFor(conversationId);
  const validated = yield* validateSignal(path, signal);
  const store = yield* LogStore.Service;
  yield* APPEND_MUTEX.withPermits(1)(
    appendRecord(store, conversationId, {
      _tag: 'Signal',
      ...validated,
    }),
  );
});

const validateSignal = Effect.fn('AgentSignals.validate')(function* (
  path: string,
  input: unknown,
) {
  const signal = yield* Schema.decodeUnknownEffect(Signal)(input).pipe(
    Effect.mapError(() =>
      LogStore.makeError(
        path,
        'append',
        'encoding',
        'signal must contain a steer or cancel kind and string text and source',
      ),
    ),
  );
  const bytes = utf8Length(signal.text) + utf8Length(signal.source);
  if (bytes > MAX_SIGNAL_BYTES) {
    return yield* LogStore.makeError(
      path,
      'append',
      'encoding',
      `signal payload is ${String(bytes)} bytes; maximum is ${String(MAX_SIGNAL_BYTES)}`,
    );
  }
  return signal;
});

const appendRecord = (
  store: LogStore.Interface,
  conversationId: LogVocabulary.ConversationId,
  record: ConversationRecord.RecordOf<'Signal'>,
): Effect.Effect<void, LogStore.LogStoreError, Crypto.Crypto> => {
  const path = pathFor(conversationId);

  return Effect.gen(function* () {
    yield* store.create(path, conversationId).pipe(
      Effect.asVoid,
      Effect.catchIf(
        (error) => error.reason === 'conflict',
        () => Effect.void,
      ),
    );

    const claim = yield* store.acquire(path, yield* AgentIds.producerId);
    const timestamp = yield* Clock.currentTimeMillis;

    yield* store.append({
      path,
      producerId: claim.producerId,
      epoch: claim.epoch,
      sequence: claim.nextSequence,
      records: [
        {
          conversationId,
          timestamp,
          record,
        },
      ],
    });
  });
};
