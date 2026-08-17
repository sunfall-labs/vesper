import {
  Effect,
  Layer,
  MutableHashMap,
  Option,
  PubSub,
  Semaphore,
  Stream,
} from 'effect';

import { LogStore } from './log-store.js';
import { LogOffset } from './offset.js';
import { ConversationRecord } from './record.js';

// In-process log store.
//
// The reference implementation of the contract, and the one the contract
// suite is developed against. It is not a fast path or a production
// fallback: everything lives in one process, and nothing
// survives a restart.
//
// Two things it does take seriously, because getting them wrong here would
// let a bug reach the Postgres backend by way of a contract test that never
// noticed. Appends validate completely before they mutate anything, so a
// rejected batch is genuinely all-or-nothing rather than partially applied
// and then rolled back by hand. And wake-ups go through a *sliding*
// `PubSub` of capacity one: publishing must never block an appender waiting
// on a reader, and a dropped wake-up is only safe if a newer one replaces
// it — which sliding guarantees and dropping does not.

interface StreamState {
  readonly identity: string;
  epoch: number;
  producerId: string | undefined;
  /**
   * Producer sequence of the last applied append in the current epoch, or
   * `-1` before the first. Reset by `acquire`; distinct from the record
   * sequence backing `lastOffset`, which never resets.
   */
  lastSequence: number;
  /** Digest of the batch `lastSequence` wrote, for exact retry detection. */
  lastFingerprint: string;
  lastOffset: LogOffset.Offset;
  readonly records: ConversationRecord.Envelope[];
}

/** First record whose offset is strictly greater than the exclusive cursor. */
const firstAfter = (
  records: ReadonlyArray<ConversationRecord.Envelope>,
  after: LogOffset.Offset,
): number => {
  let low = 0;
  let high = records.length;
  while (low < high) {
    const middle = low + Math.floor((high - low) / 2);
    const held = records[middle]!;
    if (LogOffset.isAfter(held.offset, after)) high = middle;
    else low = middle + 1;
  }
  return low;
};

type Operation = LogStore.LogStoreError['operation'];
type Reason = LogStore.LogStoreError['reason'];

const build = (
  failChangesFor: string | undefined,
): Layer.Layer<LogStore.Service> =>
  Layer.sync(LogStore.Service, () => {
    const streams = MutableHashMap.empty<string, StreamState>();
    const signals = MutableHashMap.empty<string, PubSub.PubSub<void>>();
    // One short critical section is enough: this backend is process-local and
    // every mutable invariant spans at least two fields on the same map entry.
    const mutex = Semaphore.makeUnsafe(1);
    const exclusive = mutex.withPermits(1);

    const failure = (
      path: string,
      operation: Operation,
      reason: Reason,
      detail: string,
    ): LogStore.LogStoreError =>
      new LogStore.LogStoreError({ path, operation, reason, detail });

    const lookup = (path: string, operation: Operation) =>
      Effect.gen(function* () {
        const state = MutableHashMap.get(streams, path);
        if (Option.isNone(state)) {
          return yield* Effect.fail(
            failure(path, operation, 'not_found', 'no stream at this path'),
          );
        }
        return state.value;
      });

    // `changes` has to work on a path that has not been created yet —
    // otherwise a reader that attaches a moment early gets a stream that
    // silently never wakes — so the signal is created on demand and cached.
    // The re-check after construction closes the window where two fibers
    // both miss the cache and the second one's `PubSub` replaces the first,
    // orphaning its subscribers.
    const signalFor = (path: string) =>
      Effect.gen(function* () {
        const existing = MutableHashMap.get(signals, path);
        if (Option.isSome(existing)) return existing.value;

        const created = yield* PubSub.sliding<void>(1);
        const raced = MutableHashMap.get(signals, path);
        if (Option.isSome(raced)) return raced.value;

        MutableHashMap.set(signals, path, created);
        return created;
      });

    const metaOf = (path: string, state: StreamState): LogStore.StreamMeta => ({
      path,
      identity: state.identity,
      epoch: state.epoch,
      producerId:
        state.producerId === undefined
          ? Option.none()
          : Option.some(state.producerId),
      head: state.lastOffset,
      records: state.records.length,
    });

    const createUnlocked = Effect.fn('AiLog.LogStore.create')(function* (
      path: string,
      identity: string,
    ) {
      if (MutableHashMap.has(streams, path)) {
        return yield* Effect.fail(
          failure(path, 'create', 'conflict', 'a stream already exists here'),
        );
      }

      yield* signalFor(path);
      const state: StreamState = {
        identity,
        epoch: 0,
        producerId: undefined,
        lastSequence: -1,
        lastFingerprint: '',
        lastOffset: LogOffset.START,
        records: [],
      };
      MutableHashMap.set(streams, path, state);
      return metaOf(path, state);
    });
    const create: LogStore.Interface['create'] = (path, identity) =>
      exclusive(createUnlocked(path, identity));

    const acquireUnlocked = Effect.fn('AiLog.LogStore.acquire')(function* (
      path: string,
      producerId: string,
      expected?: LogStore.AcquireExpected,
    ) {
      const state = yield* lookup(path, 'acquire');

      if (
        expected !== undefined &&
        (state.epoch !== expected.epoch || state.lastOffset !== expected.head)
      ) {
        return yield* Effect.fail(
          failure(
            path,
            'acquire',
            'conflict',
            `stream changed from epoch ${expected.epoch} at ${expected.head}`,
          ),
        );
      }

      state.epoch += 1;
      state.producerId = producerId;
      state.lastSequence = -1;
      state.lastFingerprint = '';

      return {
        path,
        producerId,
        epoch: state.epoch,
        nextSequence: 0,
      } satisfies LogStore.ProducerClaim;
    });
    const acquire: LogStore.Interface['acquire'] = (
      path,
      producerId,
      expected,
    ) => exclusive(acquireUnlocked(path, producerId, expected));

    const appendUnlocked = Effect.fn('AiLog.LogStore.append')(function* (
      input: LogStore.AppendInput,
    ) {
      const state = yield* lookup(input.path, 'append');
      const reject = (reason: Reason, detail: string) =>
        Effect.fail(failure(input.path, 'append', reason, detail));

      if (input.records.length === 0) {
        return yield* reject('empty', 'append carried no records');
      }
      if (!Number.isInteger(input.sequence) || input.sequence < 0) {
        return yield* reject(
          'conflict',
          `sequence ${input.sequence} is not a non-negative integer`,
        );
      }
      if (input.epoch !== state.epoch) {
        return yield* reject(
          'fenced',
          `epoch ${input.epoch} is not the current epoch ${state.epoch}`,
        );
      }
      if (input.producerId !== state.producerId) {
        return yield* reject(
          'conflict',
          `producer ${input.producerId} does not hold epoch ${state.epoch}`,
        );
      }

      // The last yield before the write. Fingerprinting is the only
      // expensive thing an append does, and it is worth it: it is what turns
      // "you asked about this slot before" into "you asked about these
      // records before".
      const prepared = yield* ConversationRecord.prepare(input.records).pipe(
        Effect.mapError((error) =>
          failure(input.path, 'append', 'encoding', error.detail),
        ),
      );
      const digest = prepared.fingerprint;

      if (input.sequence === state.lastSequence) {
        // A retry. Idempotent only if it repeats the same batch — a producer
        // that reuses a sequence for different records is not retrying, it
        // is overwriting, and answering with the old offset would drop the
        // new records with nothing to indicate it happened.
        if (digest !== state.lastFingerprint) {
          return yield* reject(
            'conflict',
            `sequence ${input.sequence} was reused with different content`,
          );
        }
        return state.lastOffset;
      }
      if (input.sequence !== state.lastSequence + 1) {
        return yield* reject(
          input.sequence > state.lastSequence + 1 ? 'gap' : 'conflict',
          `expected sequence ${state.lastSequence + 1}, got ${input.sequence}`,
        );
      }

      // Everything above rejects without writing; nothing below can fail.
      // That is what makes the batch atomic, and it is why the validation is
      // exhaustive up front rather than interleaved with the writes.
      let offset = state.lastOffset;
      for (const entry of prepared.entries) {
        offset = LogOffset.fromSeq(BigInt(state.records.length));
        state.records.push(ConversationRecord.envelope(offset, entry));
      }
      state.lastOffset = offset;
      state.lastSequence = input.sequence;
      state.lastFingerprint = digest;

      const signal = yield* signalFor(input.path);
      yield* PubSub.publish(signal, undefined);
      return offset;
    });
    const append: LogStore.Interface['append'] = (input) =>
      exclusive(appendUnlocked(input));

    const readUnlocked = Effect.fn('AiLog.LogStore.read')(function* (
      path: string,
      options?: LogStore.ReadOptions,
    ) {
      const normalized = yield* LogStore.normalizeReadOptions(options).pipe(
        Effect.mapError((error) =>
          failure(path, 'read', 'invalid', error.detail),
        ),
      );
      const state = yield* lookup(path, 'read');
      const { after, limit } = normalized;

      const start = firstAfter(state.records, after);
      if (start === state.records.length) {
        return {
          records: [],
          cursor: after,
          upToDate: true,
        } satisfies LogStore.Page;
      }

      const end = Math.min(start + limit, state.records.length);
      const records = state.records.slice(start, end);
      const last = records[records.length - 1];

      return {
        records,
        cursor: last === undefined ? after : last.offset,
        upToDate: end >= state.records.length,
      } satisfies LogStore.Page;
    });
    const read: LogStore.Interface['read'] = (path, options) =>
      exclusive(readUnlocked(path, options));

    const readBackwardsUnlocked = Effect.fn('AiLog.LogStore.readBackwards')(
      function* (path: string, options?: LogStore.ReadBackwardsOptions) {
        const normalized = yield* LogStore.normalizeReadBackwardsOptions(
          options,
        ).pipe(
          Effect.mapError((error) =>
            failure(path, 'readBackwards', 'invalid', error.detail),
          ),
        );
        const state = yield* lookup(path, 'readBackwards');
        const end = Option.isSome(normalized.before)
          ? firstAfter(state.records, normalized.before.value)
          : state.records.length;
        // `firstAfter` includes an exact match in the prefix; `before` does not.
        const exclusiveEnd =
          Option.isSome(normalized.before) &&
          end > 0 &&
          state.records[end - 1]?.offset === normalized.before.value
            ? end - 1
            : end;
        const start = Math.max(0, exclusiveEnd - normalized.limit);
        const records = state.records.slice(start, exclusiveEnd).reverse();
        return {
          records,
          cursor:
            records.at(-1)?.offset ??
            Option.getOrElse(normalized.before, () => state.lastOffset),
          upToDate: start === 0,
        } satisfies LogStore.BackwardsPage;
      },
    );
    const readBackwards: LogStore.Interface['readBackwards'] = (
      path,
      options,
    ) => exclusive(readBackwardsUnlocked(path, options));

    const metaUnlocked = (path: string) =>
      Effect.sync(() =>
        Option.map(MutableHashMap.get(streams, path), (state) =>
          metaOf(path, state),
        ),
      ).pipe(Effect.withSpan('AiLog.LogStore.meta'));
    const meta: LogStore.Interface['meta'] = (path) =>
      exclusive(metaUnlocked(path));

    // Subscribe, *then* emit the opening wake-up. The order is the contract:
    // a consumer that receives the first tick knows its subscription is
    // live, so nothing appended afterwards can be missed. Emitting first and
    // subscribing after would reintroduce exactly the race the tick exists
    // to close.
    const changes = (
      path: string,
    ): Stream.Stream<void, LogStore.LogStoreError> =>
      path === failChangesFor
        ? Stream.fail(
            failure(path, 'changes', 'storage', 'change feed unavailable'),
          )
        : Stream.unwrap(
            exclusive(
              Effect.gen(function* () {
                const subscription = yield* PubSub.subscribe(
                  yield* signalFor(path),
                );
                const opening: Stream.Stream<void> = Stream.make(undefined);
                return Stream.concat(
                  opening,
                  Stream.fromSubscription(subscription),
                );
              }),
            ),
          );

    return LogStore.Service.of({
      create,
      acquire,
      append,
      read,
      readBackwards,
      meta,
      changes,
    });
  });

export const layer: Layer.Layer<LogStore.Service> = build(undefined);

/**
 * The same backend with the change feed for one path broken.
 *
 * Fault injection belongs in a test double, and this one is nothing else.
 * It exists so the contract suite can check the thing a healthy backend can
 * never demonstrate: that a dead notification channel reaches the consumer
 * as a failure instead of as a tail that looks fine and delivers nothing.
 * A real backend supplies its own equivalent — for Postgres, a client whose
 * `LISTEN` connection is closed underneath it.
 */
export const layerFailingChanges = (
  path: string,
): Layer.Layer<LogStore.Service> => build(path);

export * as LogStoreMemory from './layer-memory.js';
