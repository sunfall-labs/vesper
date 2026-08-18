import * as NodeServices from '@effect/platform-node/NodeServices';
import { describe, expect, it } from '@effect/vitest';
import { Deferred, Effect, Fiber, Layer, Option, Stream } from 'effect';

import { LogStoreMemory } from '../src/layer-memory.js';
import { LogStore } from '../src/log-store.js';
import { LogStoreContract } from '../src/log-store-contract.js';
import { LogVocabulary } from '../src/vocabulary.js';

const memoryLayer = LogStoreMemory.layer.pipe(
  Layer.provide(NodeServices.layer),
);

// The memory backend can fake a dead notification channel, so it runs the
// whole suite including the change-feed failure case. A backend that cannot
// skips that one and the suite says so in the test name.
LogStoreContract.logStoreContract('memory', {
  layer: memoryLayer,
  layerWithFailingChanges: (path) =>
    LogStoreMemory.layerFailingChanges(path).pipe(
      Layer.provide(NodeServices.layer),
    ),
});

describe('LogStore memory linearizability', () => {
  it.effect(
    'keeps a pre-creation change feed alive until its reader closes',
    () =>
      Effect.gen(function* () {
        const store = yield* LogStore.Service;
        const path = 'created-after-subscribe';
        const opening = yield* Deferred.make<void>();
        const reader = yield* store.changes(path).pipe(
          Stream.tap(() => Deferred.succeed(opening, undefined)),
          Stream.take(2),
          Stream.runCollect,
          Effect.forkChild,
        );

        yield* Deferred.await(opening);
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
          records: [
            {
              conversationId: LogVocabulary.ConversationId.make('conversation'),
              timestamp: 1_700_000_000_000,
              record: { _tag: 'Text', step: 1, text: 'hello' },
            },
          ],
        });

        expect((yield* Fiber.join(reader)).length).toBe(2);
      }).pipe(Effect.provide(memoryLayer)),
  );

  it.effect('has exactly one winner when creates race', () =>
    Effect.gen(function* () {
      const store = yield* LogStore.Service;
      const outcomes = yield* Effect.all(
        Array.from({ length: 32 }, (_, index) =>
          store.create('raced-create', `identity-${index}`).pipe(Effect.result),
        ),
        { concurrency: 'unbounded' },
      );
      const meta = yield* store.meta('raced-create');

      expect(
        outcomes.filter((outcome) => outcome._tag === 'Success'),
      ).toHaveLength(1);
      expect(Option.isSome(meta)).toBe(true);
    }).pipe(Effect.provide(memoryLayer)),
  );

  it.effect(
    'serializes conflicting appends into one write and one conflict',
    () =>
      Effect.gen(function* () {
        const store = yield* LogStore.Service;
        yield* store.create('raced-append', 'identity');
        const claim = yield* store.acquire(
          'raced-append',
          LogVocabulary.ProducerId.make('producer'),
        );
        const append = (value: string) =>
          store.append({
            path: 'raced-append',
            producerId: claim.producerId,
            epoch: claim.epoch,
            sequence: LogVocabulary.ProducerSequence.make(0),
            records: [
              {
                conversationId:
                  LogVocabulary.ConversationId.make('conversation-1'),
                timestamp: 1,
                record: { _tag: 'Text', step: 1, text: value },
              },
            ],
          });
        const outcomes = yield* Effect.all(
          [append('a').pipe(Effect.result), append('b').pipe(Effect.result)],
          { concurrency: 'unbounded' },
        );
        const page = yield* store.read('raced-append');

        expect(
          outcomes.filter((outcome) => outcome._tag === 'Success'),
        ).toHaveLength(1);
        expect(
          outcomes.filter((outcome) => outcome._tag === 'Failure'),
        ).toHaveLength(1);
        expect(page.records).toHaveLength(1);
      }).pipe(Effect.provide(memoryLayer)),
  );
});
