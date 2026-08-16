import { Effect, Layer, MutableHashMap, Option, PubSub, Stream } from 'effect';

import { LogStore } from './log-store.js';
import { LogOffset } from './offset.js';
import { ConversationRecord } from './record.js';

// In-process log store.
//
// The reference implementation of the contract, and the one the contract
// suite is developed against. It is not a fast path or a production
// fallback: reads scan, everything lives in one process, and nothing
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

type Operation = LogStore.LogStoreError['operation'];
type Reason = LogStore.LogStoreError['reason'];

const build = (
  failChangesFor: string | undefined,
): Layer.Layer<LogStore.Service> =>
  Layer.sync(LogStore.Service, () => {
    const streams = MutableHashMap.empty<string, StreamState>();
    const signals = MutableHashMap.empty<string, PubSub.PubSub<void>>();

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

    const create = Effect.fn('AiLog.LogStore.create')(function* (
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

    const acquire = Effect.fn('AiLog.LogStore.acquire')(function* (
      path: string,
      producerId: string,
    ) {
      const state = yield* lookup(path, 'acquire');

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

    const append = Effect.fn('AiLog.LogStore.append')(function* (
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
      const digest = yield* ConversationRecord.fingerprint(input.records).pipe(
        Effect.mapError((error) =>
          failure(input.path, 'append', 'encoding', error.detail),
        ),
      );

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
      for (const entry of input.records) {
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

    const read = Effect.fn('AiLog.LogStore.read')(function* (
      path: string,
      options?: LogStore.ReadOptions,
    ) {
      const state = yield* lookup(path, 'read');
      const after = options?.after ?? LogOffset.START;
      // A limit of zero would return an empty page that is not up to date,
      // which `Tail` would page forever.
      const limit = Math.max(1, options?.limit ?? LogStore.DEFAULT_READ_LIMIT);

      const start = state.records.findIndex((held) =>
        LogOffset.isAfter(held.offset, after),
      );
      if (start === -1) {
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

    const meta = (path: string) =>
      Effect.sync(() =>
        Option.map(MutableHashMap.get(streams, path), (state) =>
          metaOf(path, state),
        ),
      ).pipe(Effect.withSpan('AiLog.LogStore.meta'));

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
          );

    return LogStore.Service.of({
      create,
      acquire,
      append,
      read,
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
