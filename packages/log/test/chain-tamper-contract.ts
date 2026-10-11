import * as NodeServices from '@effect/platform-node/NodeServices';
import { Effect, Layer, Option } from 'effect';
import type { SqlClient, SqlError } from 'effect/sql';
import { expect, it } from '@effect/vitest';

import { LogChain } from '../src/chain.js';
import { LogStore } from '../src/log-store.js';
import { LogOffset } from '../src/offset.js';
import { LogVocabulary } from '../src/vocabulary.js';

// Chain tamper detection against a real SQL store. The store is written
// through its public API, then its stored rows are edited or deleted behind
// its back, and `LogChain.verify` must notice.

export interface TamperOptions<E> {
  readonly layer: Layer.Layer<LogStore.Service | SqlClient.SqlClient, E>;
  /** Rewrite the text of the record at `seq` directly in storage. */
  readonly editText: (
    path: string,
    seq: number,
  ) => Effect.Effect<void, SqlError.SqlError, SqlClient.SqlClient>;
  /** Delete the stored record at `seq` directly in storage. */
  readonly deleteRecord: (
    path: string,
    seq: number,
  ) => Effect.Effect<void, SqlError.SqlError, SqlClient.SqlClient>;
}

const entry = (value: string) => ({
  conversationId: LogVocabulary.ConversationId.make('conversation'),
  timestamp: 1,
  record: { _tag: 'Text' as const, step: 1, text: value },
});

export const chainTamperContract = <E>(
  name: string,
  options: TamperOptions<E>,
): void => {
  /** A stream of batches [a, b], [c], [d] (records 0..3) in a real store. */
  const seeded = (path: string) =>
    Effect.gen(function* () {
      const store = yield* LogStore.Service;
      yield* store.create(path, 'identity');
      const claim = yield* store.acquire(
        path,
        LogVocabulary.ProducerId.make('producer'),
      );
      for (const [index, batch] of [['a', 'b'], ['c'], ['d']].entries()) {
        yield* store.append({
          path,
          producerId: claim.producerId,
          epoch: claim.epoch,
          sequence: LogVocabulary.ProducerSequence.make(index),
          records: batch.map(entry),
        });
      }
      expect(yield* LogChain.verify(path)).toMatchObject({
        _tag: 'Valid',
        batches: 3,
        records: 4,
      });
      return store;
    });

  it.layer(Layer.merge(options.layer, NodeServices.layer))(
    `LogChain tamper detection: ${name}`,
    (scoped) => {
      scoped.effect(
        'detects an edited record, a deleted record, and truncation',
        () =>
          Effect.gen(function* () {
            yield* seeded('edited');
            yield* options.editText('edited', 1);
            expect(yield* LogChain.verify('edited')).toMatchObject({
              _tag: 'Broken',
              reason: 'content_modified',
              offset: LogOffset.fromSeq(0n),
            });

            yield* seeded('pruned');
            yield* options.deleteRecord('pruned', 2);
            expect(yield* LogChain.verify('pruned')).toMatchObject({
              _tag: 'Broken',
              reason: 'offset_gap',
            });

            // The stream row still reports the old head, so even an unpinned
            // verification notices a missing newest batch.
            const store = yield* seeded('truncated');
            const pinned = Option.getOrThrow(
              Option.getOrThrow(yield* store.meta('truncated')).chainHead,
            );
            yield* options.deleteRecord('truncated', 3);
            expect(yield* LogChain.verify('truncated')).toMatchObject({
              _tag: 'Broken',
              reason: 'head_mismatch',
            });
            expect(
              yield* LogChain.verify('truncated', { expectedHead: pinned }),
            ).toMatchObject({ _tag: 'Broken', reason: 'head_mismatch' });
          }),
      );
    },
  );
};
