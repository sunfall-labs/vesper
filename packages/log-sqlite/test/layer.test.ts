import { describe, expect, it } from '@effect/vitest';
import * as NodeServices from '@effect/platform-node/NodeServices';
import { Effect, Layer } from 'effect';
import * as ReactivityModule from 'effect/unstable/reactivity/Reactivity';
import { SqlClient } from 'effect/unstable/sql';

import { LogStoreSqlite } from '../src/layer.js';
import { SqliteNative } from '../src/layer-native.js';
import { LogStore } from '@sunfall/vesper-log/log-store';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';

const storeLayer = LogStoreSqlite.layer().pipe(
  Layer.provideMerge(
    Layer.mergeAll(
      SqliteNative.layer(':memory:'),
      NodeServices.layer,
      ReactivityModule.layer,
    ),
  ),
);

describe('LogStore SQLite backend', () => {
  it.effect('round-trips batches, retries, and process-local changes', () =>
    Effect.gen(function* () {
      yield* LogStoreSqlite.migrate();
      const store = yield* LogStore.Service;
      const producer = LogVocabulary.ProducerId.make('producer');
      yield* store.create('conversation', 'identity');
      const claim = yield* store.acquire('conversation', producer);
      const entry = {
        conversationId: LogVocabulary.ConversationId.make('conversation'),
        timestamp: 1,
        record: { _tag: 'Text' as const, step: 1, text: 'hello' },
      };
      const offset = yield* store.append({
        path: 'conversation',
        producerId: claim.producerId,
        epoch: claim.epoch,
        sequence: claim.nextSequence,
        records: [entry],
      });
      expect(
        yield* store.append({
          path: 'conversation',
          producerId: claim.producerId,
          epoch: claim.epoch,
          sequence: claim.nextSequence,
          records: [entry],
        }),
      ).toBe(offset);
      const page = yield* store.read('conversation');
      expect(page.records).toHaveLength(1);
      expect(page.records[0]?.record).toEqual(entry.record);
      expect(page.upToDate).toBe(true);
    }).pipe(Effect.provide(storeLayer)),
  );

  it.effect('supports positional value rows through the native client', () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql.unsafe('CREATE TABLE values_test (value TEXT)').unprepared;
      yield* sql.unsafe('INSERT INTO values_test VALUES (?)', ['ok'])
        .unprepared;
      const rows = yield* sql.unsafe('SELECT value FROM values_test').values;
      expect(rows).toEqual([['ok']]);
    }).pipe(Effect.provide(SqliteNative.layer(':memory:'))),
  );
});
