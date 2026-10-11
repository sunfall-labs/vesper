import * as NodeServices from '@effect/platform-node/NodeServices';
import { describe, expect, it } from '@effect/vitest';
import { Deferred, Effect, Fiber, Layer, Option, Stream } from 'effect';

import { LogStoreMemory } from '../src/layer-memory.js';
import { LogStore } from '../src/log-store.js';
import { LogOffset } from '../src/offset.js';
import type { ConversationRecord } from '../src/record.js';
import { Tail } from '../src/tail.js';
import { LogVocabulary } from '../src/vocabulary.js';
import { logStoreContract } from './log-store-contract.js';

const memoryLayer = LogStoreMemory.layer.pipe(
  Layer.provide(NodeServices.layer),
);

// The memory backend can fake a dead notification channel by wrapping the
// public store, so it runs the whole contract including the dead-feed case.
logStoreContract('memory', {
  layer: memoryLayer,
  layerWithFailingChanges: () =>
    Layer.effect(
      LogStore.Service,
      Effect.map(LogStore.Service, (store) =>
        LogStore.Service.of({
          ...store,
          changes: (path) =>
            Stream.fail(
              LogStore.makeError(
                path,
                'changes',
                'storage',
                'change feed unavailable',
              ),
            ),
        }),
      ),
    ).pipe(Layer.provide(memoryLayer)),
});

const entry = (value: string): ConversationRecord.Entry => ({
  conversationId: LogVocabulary.ConversationId.make('conversation-1'),
  timestamp: 1_700_000_000_000,
  record: { _tag: 'Text', step: 1, text: value },
});

const seed = (path: string, count: number) =>
  Effect.gen(function* () {
    const store = yield* LogStore.Service;
    yield* store.create(path, 'identity');
    const claim = yield* store.acquire(
      path,
      LogVocabulary.ProducerId.make('producer'),
    );
    yield* store.append({
      path,
      producerId: claim.producerId,
      epoch: claim.epoch,
      sequence: claim.nextSequence,
      records: Array.from({ length: count }, (_, index) =>
        entry(`r${String(index)}`),
      ),
    });
    return { store, claim };
  });

describe('memory log store scenarios', () => {
  it.effect('follows a stream that does not exist yet, losslessly', () =>
    Effect.gen(function* () {
      const store = yield* LogStore.Service;
      const subscribed = yield* Deferred.make<void>();
      const observing = LogStore.Service.of({
        ...store,
        changes: (path) =>
          store
            .changes(path)
            .pipe(Stream.tap(() => Deferred.succeed(subscribed, undefined))),
      });
      const reader = yield* Tail.from('later', LogOffset.START).pipe(
        Stream.take(1),
        Stream.runCollect,
        Effect.provideService(LogStore.Service, observing),
        Effect.forkChild,
      );
      yield* Deferred.await(subscribed);
      yield* seed('later', 1);
      expect((yield* Fiber.join(reader))[0]?.record).toMatchObject({
        text: 'r0',
      });
    }).pipe(Effect.provide(memoryLayer)),
  );

  it.effect('pages a backlog larger than one read', () =>
    Effect.gen(function* () {
      const total = LogStore.DEFAULT_READ_LIMIT + 17;
      yield* seed('long', total);
      const collected = yield* Tail.from('long', LogOffset.START).pipe(
        Stream.take(total),
        Stream.runCollect,
      );
      expect(collected).toHaveLength(total);
      expect(collected.at(-1)?.record).toMatchObject({
        text: `r${String(total - 1)}`,
      });
    }).pipe(Effect.provide(memoryLayer)),
  );

  it.effect('has one winner when creates and conflicting appends race', () =>
    Effect.gen(function* () {
      const store = yield* LogStore.Service;
      const creates = yield* Effect.all(
        Array.from({ length: 32 }, (_, index) =>
          store
            .create('raced', `identity-${String(index)}`)
            .pipe(Effect.result),
        ),
        { concurrency: 'unbounded' },
      );
      expect(
        creates.filter((outcome) => outcome._tag === 'Success'),
      ).toHaveLength(1);
      expect(Option.isSome(yield* store.meta('raced'))).toBe(true);

      const claim = yield* store.acquire(
        'raced',
        LogVocabulary.ProducerId.make('producer'),
      );
      const append = (value: string) =>
        store.append({
          path: 'raced',
          producerId: claim.producerId,
          epoch: claim.epoch,
          sequence: LogVocabulary.ProducerSequence.make(0),
          records: [entry(value)],
        });
      const appends = yield* Effect.all(
        [append('a').pipe(Effect.result), append('b').pipe(Effect.result)],
        { concurrency: 'unbounded' },
      );
      expect(
        appends.filter((outcome) => outcome._tag === 'Success'),
      ).toHaveLength(1);
      expect((yield* store.read('raced')).records).toHaveLength(1);
    }).pipe(Effect.provide(memoryLayer)),
  );
});
