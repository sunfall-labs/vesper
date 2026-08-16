import { Effect, Stream } from 'effect';
import { describe, expect, it } from 'vitest';

import { LogStoreMemory } from '../src/layer-memory.js';
import { LogStore } from '../src/log-store.js';
import { LogOffset } from '../src/offset.js';
import type { ConversationRecord } from '../src/record.js';
import { Tail } from '../src/tail.js';

// `Tail` is derived rather than implemented per backend, so these are the
// only tests it gets — the wake-up half is exercised once per backend by the
// contract suite.

const entry = (value: string): ConversationRecord.Entry => ({
  conversationId: 'conversation-1',
  timestamp: 1_700_000_000_000,
  record: { _tag: 'Text', step: 1, text: value },
});

const run = <A>(effect: Effect.Effect<A, unknown, LogStore.Service>) =>
  Effect.runPromise(
    effect.pipe(Effect.provide(LogStoreMemory.layer)) as Effect.Effect<A>,
  );

const setup = (path: string, count: number) =>
  Effect.gen(function* () {
    const store = yield* LogStore.Service;
    yield* store.create(path, 'identity');
    const claim = yield* store.acquire(path, 'producer-1');
    yield* store.append({
      path,
      producerId: claim.producerId,
      epoch: claim.epoch,
      sequence: claim.nextSequence,
      records: Array.from({ length: count }, (_, index) => entry(`r${index}`)),
    });
    return store;
  });

describe('Tail', () => {
  it('replays everything already written before following', async () => {
    const collected = await run(
      Effect.gen(function* () {
        yield* setup('short', 3);
        return yield* Tail.from('short', LogOffset.START).pipe(
          Stream.take(3),
          Stream.runCollect,
        );
      }),
    );

    expect(
      collected.map((envelope) =>
        envelope.record._tag === 'Text' ? envelope.record.text : '',
      ),
    ).toEqual(['r0', 'r1', 'r2']);
  });

  // Catch-up pages: one `read` cannot return more than the backend's limit,
  // so the drain loop has to keep going while `upToDate` is false. Getting
  // this wrong stalls a resuming reader partway through its own history.
  it('pages through a backlog larger than one read', async () => {
    const total = LogStore.DEFAULT_READ_LIMIT + 17;

    const collected = await run(
      Effect.gen(function* () {
        yield* setup('long', total);
        return yield* Tail.from('long', LogOffset.START).pipe(
          Stream.take(total),
          Stream.runCollect,
        );
      }),
    );

    expect(collected.length).toBe(total);
    expect(collected[collected.length - 1]?.record).toMatchObject({
      text: `r${total - 1}`,
    });
  });

  // Resumption from inside a batch, which is only possible because offsets
  // are per record. With per-batch offsets this test cannot be
  // written at all.
  it('resumes from an offset in the middle of a batch', async () => {
    const collected = await run(
      Effect.gen(function* () {
        const store = yield* setup('mid', 4);
        const page = yield* store.read('mid');

        return yield* Tail.from('mid', page.records[1]!.offset).pipe(
          Stream.take(2),
          Stream.runCollect,
        );
      }),
    );

    expect(
      collected.map((envelope) =>
        envelope.record._tag === 'Text' ? envelope.record.text : '',
      ),
    ).toEqual(['r2', 'r3']);
  });
});
