import { performance } from 'node:perf_hooks';

import * as NodeServices from '@effect/platform-node/NodeServices';
import { describe, expect, it } from '@effect/vitest';
import { Effect, Layer } from 'effect';

import { LogStoreMemory } from '../src/layer-memory.js';
import { LogStore } from '../src/log-store.js';
import { LogOffset } from '../src/offset.js';
import { LogVocabulary } from '../src/vocabulary.js';

const memoryLayer = LogStoreMemory.layer.pipe(
  Layer.provide(NodeServices.layer),
);

const describeScaling =
  process.env['RUN_LOG_SCALING'] === '1' ? describe : describe.skip;

describeScaling('LogStore memory pagination scaling', () => {
  for (const size of [1_000, 10_000]) {
    it.effect(
      `reads near the end of ${size.toLocaleString()} records`,
      () =>
        Effect.gen(function* () {
          const path = `scale-${size}`;
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
            sequence: LogVocabulary.ProducerSequence.make(0),
            records: Array.from({ length: size }, (_, index) => ({
              conversationId: LogVocabulary.ConversationId.make('conversation'),
              timestamp: 1_700_000_000_000 + index,
              record: {
                _tag: 'Text' as const,
                step: index,
                text: `text-${index}`,
              },
            })),
          });

          const after = LogOffset.fromSeq(BigInt(size - 2));
          const started = performance.now();
          let page: LogStore.Page | undefined;
          for (let iteration = 0; iteration < 10_000; iteration += 1) {
            page = yield* store.read(path, { after, limit: 1 });
          }
          const elapsedMs = performance.now() - started;

          if (page === undefined) {
            throw new Error('expected a page from the scaling read');
          }
          expect(page.records).toHaveLength(1);
          expect(page.records[0]?.offset).toBe(
            LogOffset.fromSeq(BigInt(size - 1)),
          );
          console.info(
            `memory ${size.toLocaleString()}-record tail reads: 10,000 in ${elapsedMs.toFixed(1)}ms`,
          );
        }).pipe(Effect.provide(memoryLayer)),
      { timeout: 30_000 },
    );
  }
});
