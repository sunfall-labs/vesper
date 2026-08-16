import { LogStore } from '@sunfall/vesper-log/log-store';
import type { ConversationRecord } from '@sunfall/vesper-log/record';
import { Clock, Effect } from 'effect';

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

/**
 * Where a conversation's signals live.
 *
 * Exported for the same reason `AgentLog.pathFor` is: the sender and the
 * drain have to agree, and a convention in two places is one that eventually
 * differs.
 */
export const pathFor = (conversationId: string): string =>
  `signals/${conversationId}`;

/** What a sender says. Structural, so a caller needs no schema import. */
export interface Signal {
  readonly kind: 'steer' | 'cancel';
  /** Steering text, or a cancellation reason. */
  readonly text: string;
  /** Who sent it — a user id, a service name. Opaque here. */
  readonly source: string;
}

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
 * queued in advance is delivered at that run's first turn boundary.
 */
export const send = Effect.fn('AgentSignals.send')(function* (
  conversationId: string,
  signal: Signal,
) {
  const store = yield* LogStore.Service;
  const path = pathFor(conversationId);

  yield* store.create(path, conversationId).pipe(
    Effect.asVoid,
    Effect.catchIf(
      (error) => error.reason === 'conflict',
      () => Effect.void,
    ),
  );

  const claim = yield* store.acquire(path, crypto.randomUUID());
  const timestamp = yield* Clock.currentTimeMillis;
  const record: ConversationRecord.Record = { _tag: 'Signal', ...signal };

  yield* store.append({
    path,
    producerId: claim.producerId,
    epoch: claim.epoch,
    sequence: claim.nextSequence,
    records: [{ conversationId, timestamp, record }],
  });
});

export * as AgentSignals from './signal.js';
