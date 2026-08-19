import { Effect, Ref, Stream } from 'effect';

import { LogStore } from './log-store.js';
import type { LogOffset } from './offset.js';
import type { ConversationRecord } from './record.js';

// Resumable tailing, derived from `read` and `changes`.
//
// This is deliberately not a `LogStore` method. Every backend would have to
// implement the same catch-up-then-follow loop, every backend would get it
// subtly differently wrong, and the contract suite would have to test it
// once per backend instead of once. Two primitives in the interface, one
// derivation here, and a new backend inherits tailing by existing.
//
// The shape below is the answer to a race that cost a day. Catching up first
// and subscribing after leaves a window: a record appended between the read
// that reported `upToDate` and the subscription produces a wake-up nobody is
// listening for, and the tail then sits idle until some unrelated later
// append shakes it loose. Subscribing first does not fix it either, because
// there is no way from out here to know *when* a subscription took effect —
// forking a fiber that will subscribe is not the same as having subscribed,
// and the initial read easily wins that race.
//
// So the catch-up is not separate. `changes` is required to emit an opening
// wake-up once its subscription is live, and every drain — the first one
// included — is driven by a wake-up. Receiving one is the proof of
// subscription that could not otherwise be obtained, and the queue in front
// coalesces the rest: "read again" does not get truer by being said twice.

/**
 * Every record after `after`, then every record as it arrives.
 *
 * Never completes on its own; interrupt it, `Stream.take` it, or let its
 * scope close.
 */
export const from = (
  path: string,
  after: LogOffset.Offset,
): Stream.Stream<
  ConversationRecord.Envelope,
  LogStore.LogStoreError,
  LogStore.Service
> =>
  Stream.unwrap(
    Effect.gen(function* () {
      const store = yield* LogStore.Service;
      const cursor = yield* Ref.make(after);

      const wakeups = yield* store
        .changes(path)
        .pipe(Stream.toQueue({ capacity: 1, strategy: 'sliding' }));

      // Pages until the store says there is nothing more, carrying the
      // cursor in a `Ref` so a later wake-up resumes where this left off
      // rather than re-reading from `after`.
      const drain: Stream.Stream<
        ConversationRecord.Envelope,
        LogStore.LogStoreError
      > = Stream.unwrap(
        Effect.gen(function* () {
          const cursorAfter = yield* Ref.get(cursor);
          const page = yield* store.read(path, { after: cursorAfter }).pipe(
            // A tail may be installed before its producer creates the stream.
            // `changes` deliberately supports that ordering; keep the
            // subscription alive until a later append wakes this drain.
            Effect.catchIf(
              (error) => error.reason === 'not_found',
              () =>
                Effect.succeed({
                  records: [],
                  cursor: cursorAfter,
                  upToDate: true,
                } satisfies LogStore.Page),
            ),
          );
          yield* Ref.set(cursor, page.cursor);

          const emitted = Stream.fromIterable(page.records);
          return page.upToDate ? emitted : Stream.concat(emitted, drain);
        }),
      );

      return Stream.fromQueue(wakeups).pipe(Stream.flatMap(() => drain));
    }),
  );

export * as Tail from './tail.js';
