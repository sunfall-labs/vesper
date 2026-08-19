import * as NodeServices from '@effect/platform-node/NodeServices';
import { describe, expect, it } from '@effect/vitest';
import { Deferred, Effect, Fiber, Layer, Stream } from 'effect';

import { LogStoreMemory } from '../src/layer-memory.js';
import { LogStore } from '../src/log-store.js';
import { LogOffset } from '../src/offset.js';
import type { ConversationRecord } from '../src/record.js';
import { Tail } from '../src/tail.js';
import { LogVocabulary } from '../src/vocabulary.js';

const memoryLayer = LogStoreMemory.layer.pipe(
  Layer.provide(NodeServices.layer),
);

// `Tail` is derived rather than implemented per backend, so these are the
// only tests it gets — the wake-up half is exercised once per backend by the
// contract suite.

const entry = (value: string): ConversationRecord.Entry => ({
  conversationId: LogVocabulary.ConversationId.make('conversation-1'),
  timestamp: 1_700_000_000_000,
  record: { _tag: 'Text', step: 1, text: value },
});

const setup = (path: string, count: number) =>
  Effect.gen(function* () {
    const store = yield* LogStore.Service;
    yield* store.create(path, 'identity');
    const claim = yield* store.acquire(
      path,
      LogVocabulary.ProducerId.make('producer-1'),
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
    return store;
  });

describe('Tail', () => {
  it.effect('can subscribe before its stream exists', () =>
    Effect.gen(function* () {
      const store = yield* LogStore.Service;
      const subscribed = yield* Deferred.make<void>();
      const observingStore = LogStore.Service.of({
        ...store,
        changes: (path) =>
          store
            .changes(path)
            .pipe(Stream.tap(() => Deferred.succeed(subscribed, undefined))),
      });
      const reader = yield* Tail.from('not-created-yet', LogOffset.START).pipe(
        Stream.take(1),
        Stream.runCollect,
        Effect.provideService(LogStore.Service, observingStore),
        Effect.forkChild,
      );

      // The opening wake-up proves Tail is already subscribed and has tried
      // its first read before the producer exists.
      yield* Deferred.await(subscribed);
      yield* setup('not-created-yet', 1);

      expect((yield* Fiber.join(reader))[0]?.record).toMatchObject({
        text: 'r0',
      });
    }).pipe(Effect.provide(memoryLayer)),
  );

  it.effect('replays everything already written before following', () =>
    Effect.gen(function* () {
      yield* setup('short', 3);
      const collected = yield* Tail.from('short', LogOffset.START).pipe(
        Stream.take(3),
        Stream.runCollect,
      );

      expect(
        collected.map((envelope) =>
          envelope.record._tag === 'Text' ? envelope.record.text : '',
        ),
      ).toEqual(['r0', 'r1', 'r2']);
    }).pipe(Effect.provide(memoryLayer)),
  );

  // Catch-up pages: one `read` cannot return more than the backend's limit,
  // so the drain loop has to keep going while `upToDate` is false. Getting
  // this wrong stalls a resuming reader partway through its own history.
  it.effect('pages through a backlog larger than one read', () =>
    Effect.gen(function* () {
      const total = LogStore.DEFAULT_READ_LIMIT + 17;
      yield* setup('long', total);
      const collected = yield* Tail.from('long', LogOffset.START).pipe(
        Stream.take(total),
        Stream.runCollect,
      );

      expect(collected.length).toBe(total);
      expect(collected[collected.length - 1]?.record).toMatchObject({
        text: `r${String(total - 1)}`,
      });
    }).pipe(Effect.provide(memoryLayer)),
  );

  // Resumption from inside a batch, which is only possible because offsets
  // are per record. With per-batch offsets this test cannot be
  // written at all.
  it.effect('resumes from an offset in the middle of a batch', () =>
    Effect.gen(function* () {
      const store = yield* setup('mid', 4);
      const page = yield* store.read('mid');

      const secondRecord = page.records.at(1);
      if (secondRecord === undefined) {
        throw new Error('expected a second record to resume from');
      }
      const resumeOffset = secondRecord.offset;
      const collected = yield* Tail.from('mid', resumeOffset).pipe(
        Stream.take(2),
        Stream.runCollect,
      );

      expect(
        collected.map((envelope) =>
          envelope.record._tag === 'Text' ? envelope.record.text : '',
        ),
      ).toEqual(['r2', 'r3']);
    }).pipe(Effect.provide(memoryLayer)),
  );
});
