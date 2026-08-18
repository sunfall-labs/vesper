import {
  Crypto,
  Effect,
  Layer,
  MutableHashMap,
  Option,
  PubSub,
  Schema,
  Semaphore,
  Stream,
} from 'effect';

import { LogStore } from './log-store.js';
import { LogOffset } from './offset.js';
import { ConversationRecord } from './record.js';
import { RecordBatch } from './record-batch.js';
import { LogVocabulary } from './vocabulary.js';
import * as AppendDecision from './append-decision.js';

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
  epoch: LogVocabulary.Epoch;
  producerId: LogVocabulary.ProducerId | undefined;
  /**
   * Producer sequence of the last applied append in the current epoch, or
   * `-1` before the first. Reset by `acquire`; distinct from the record
   * sequence backing `lastOffset`, which never resets.
   */
  lastSequence: LogVocabulary.ProducerSequence | -1;
  /** Digest of the batch `lastSequence` wrote, for exact retry detection. */
  lastFingerprint: string;
  lastOffset: LogOffset.Offset;
  readonly records: ConversationRecord.Envelope[];
}

interface SignalState {
  readonly pubsub: PubSub.PubSub<void>;
  subscribers: number;
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

const build = (
  failChangesFor: string | undefined,
): Layer.Layer<LogStore.Service, never, Crypto.Crypto> =>
  Layer.effect(
    LogStore.Service,
    Effect.map(Crypto.Crypto, (crypto) => {
      const streams = MutableHashMap.empty<string, StreamState>();
      const signals = MutableHashMap.empty<string, SignalState>();
      // One short critical section is enough: this backend is process-local and
      // every mutable invariant spans at least two fields on the same map entry.
      const mutex = Semaphore.makeUnsafe(1);
      const exclusive = mutex.withPermits(1);

      const failure = LogStore.makeError;

      const lookup = (path: string, operation: LogStore.Operation) =>
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
      // silently never wakes — so the signal is created on demand. Signals
      // are reference-counted and removed when their last subscription closes;
      // otherwise every typo or speculative path would stay reachable for the
      // lifetime of this layer.
      /** Acquire one registry reference. The caller holds `mutex`. */
      const acquireSignal = (path: string) =>
        Effect.gen(function* () {
          const existing = MutableHashMap.get(signals, path);
          if (Option.isSome(existing)) {
            existing.value.subscribers += 1;
            return existing.value;
          }

          const state: SignalState = {
            pubsub: yield* PubSub.sliding<void>(1),
            subscribers: 1,
          };
          MutableHashMap.set(signals, path, state);
          return state;
        });

      const releaseSignal = (path: string, signal: PubSub.PubSub<void>) =>
        exclusive(
          Effect.sync(() => {
            const current = MutableHashMap.get(signals, path);
            // A replacement can only happen after the old subscription has
            // released its reference, but checking identity keeps this
            // finalizer correct even if that invariant changes later.
            if (Option.isSome(current) && current.value.pubsub === signal) {
              current.value.subscribers -= 1;
              if (current.value.subscribers === 0) {
                MutableHashMap.remove(signals, path);
              }
            }
          }),
        );

      const metaOf = (
        path: string,
        state: StreamState,
      ): LogStore.StreamMeta => ({
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

      const createUnlocked = Effect.fn('LogStore.create')(function* (
        path: string,
        identity: string,
      ) {
        if (MutableHashMap.has(streams, path)) {
          return yield* Effect.fail(
            failure(path, 'create', 'conflict', 'a stream already exists here'),
          );
        }

        const state: StreamState = {
          identity,
          epoch: LogVocabulary.Epoch.make(0),
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

      const acquireUnlocked = Effect.fn('LogStore.acquire')(function* (
        path: string,
        producerId: LogVocabulary.ProducerId,
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

        const decodedProducerId = yield* LogVocabulary.ProducerId.pipe(
          Schema.decodeUnknownEffect,
        )(producerId).pipe(
          Effect.mapError(() =>
            failure(
              path,
              'acquire',
              'invalid',
              'producer id must be non-empty',
            ),
          ),
        );
        const epoch = LogVocabulary.Epoch.make(Number(state.epoch) + 1);
        state.epoch = epoch;
        state.producerId = decodedProducerId;
        state.lastSequence = -1;
        state.lastFingerprint = '';

        return {
          path,
          producerId: decodedProducerId,
          epoch,
          nextSequence: LogVocabulary.ProducerSequence.make(0),
        } satisfies LogStore.ProducerClaim;
      });
      const acquire: LogStore.Interface['acquire'] = (
        path,
        producerId,
        expected,
      ) => exclusive(acquireUnlocked(path, producerId, expected));

      const appendUnlocked = Effect.fn('LogStore.append')(function* (
        input: LogStore.AppendInput,
      ) {
        const validated = yield* AppendDecision.validateInput(input);
        const state = yield* lookup(input.path, 'append');
        const decision = yield* AppendDecision.decide(validated, {
          epoch: state.epoch,
          producerId: state.producerId,
          nextSequence: LogVocabulary.ProducerSequence.make(
            state.lastSequence + 1,
          ),
          lastFingerprint: state.lastFingerprint,
        }).pipe(Effect.provideService(Crypto.Crypto, crypto));
        if (decision.kind === 'retry') return state.lastOffset;

        // Everything above rejects without writing; nothing below can fail.
        // That is what makes the batch atomic, and it is why the validation is
        // exhaustive up front rather than interleaved with the writes.
        let offset = state.lastOffset;
        for (const entry of decision.prepared.entries) {
          offset = LogOffset.fromSeq(BigInt(state.records.length));
          state.records.push(RecordBatch.envelope(offset, entry));
        }
        state.lastOffset = offset;
        state.lastSequence = validated.sequence;
        state.lastFingerprint = decision.prepared.fingerprint;

        const signal = MutableHashMap.get(signals, input.path);
        if (Option.isSome(signal)) {
          yield* PubSub.publish(signal.value.pubsub, undefined);
        }
        return offset;
      });
      const append: LogStore.Interface['append'] = (input) =>
        exclusive(appendUnlocked(input));

      const readUnlocked = Effect.fn('LogStore.read')(function* (
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

      const readBackwardsUnlocked = Effect.fn('LogStore.readBackwards')(
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
        ).pipe(Effect.withSpan('LogStore.meta'));
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
                  const { subscription } = yield* Effect.acquireRelease(
                    Effect.uninterruptible(
                      Effect.gen(function* () {
                        const signal = yield* acquireSignal(path);
                        const subscription = yield* PubSub.subscribe(
                          signal.pubsub,
                        );
                        return { signal, subscription };
                      }),
                    ),
                    ({ signal }) => releaseSignal(path, signal.pubsub),
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
    }),
  );

export const layer: Layer.Layer<LogStore.Service, never, Crypto.Crypto> =
  build(undefined);

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
): Layer.Layer<LogStore.Service, never, Crypto.Crypto> => build(path);

export * as LogStoreMemory from './layer-memory.js';
