import { Effect, Option } from 'effect';
import { describe, expect, it } from 'vitest';

import { LogStoreMemory } from '../src/layer-memory.js';
import { LogStore } from '../src/log-store.js';
import { LogStoreContract } from '../src/log-store-contract.js';

// The memory backend can fake a dead notification channel, so it runs the
// whole suite including the change-feed failure case. A backend that cannot
// skips that one and the suite says so in the test name.
LogStoreContract.logStoreContract('memory', {
  layer: LogStoreMemory.layer,
  layerWithFailingChanges: LogStoreMemory.layerFailingChanges,
});

describe('LogStore memory linearizability', () => {
  const run = <A>(effect: Effect.Effect<A, unknown, LogStore.Service>) =>
    Effect.runPromise(effect.pipe(Effect.provide(LogStoreMemory.layer)));

  it('has exactly one winner when creates race', async () => {
    const result = await run(
      Effect.gen(function* () {
        const store = yield* LogStore.Service;
        const outcomes = yield* Effect.all(
          Array.from({ length: 32 }, (_, index) =>
            store
              .create('raced-create', `identity-${index}`)
              .pipe(Effect.result),
          ),
          { concurrency: 'unbounded' },
        );
        return { outcomes, meta: yield* store.meta('raced-create') };
      }),
    );

    expect(
      result.outcomes.filter((outcome) => outcome._tag === 'Success'),
    ).toHaveLength(1);
    expect(Option.isSome(result.meta)).toBe(true);
  });

  it('serializes conflicting appends into one write and one conflict', async () => {
    const result = await run(
      Effect.gen(function* () {
        const store = yield* LogStore.Service;
        yield* store.create('raced-append', 'identity');
        const claim = yield* store.acquire('raced-append', 'producer');
        const append = (value: string) =>
          store.append({
            path: 'raced-append',
            producerId: claim.producerId,
            epoch: claim.epoch,
            sequence: 0,
            records: [
              {
                conversationId: 'conversation-1',
                timestamp: 1,
                record: { _tag: 'Text', step: 1, text: value },
              },
            ],
          });
        const outcomes = yield* Effect.all(
          [append('a').pipe(Effect.result), append('b').pipe(Effect.result)],
          { concurrency: 'unbounded' },
        );
        return { outcomes, page: yield* store.read('raced-append') };
      }),
    );

    expect(
      result.outcomes.filter((outcome) => outcome._tag === 'Success'),
    ).toHaveLength(1);
    expect(
      result.outcomes.filter((outcome) => outcome._tag === 'Failure'),
    ).toHaveLength(1);
    expect(result.page.records).toHaveLength(1);
  });
});
