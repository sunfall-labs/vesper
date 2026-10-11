import { describe, expect, it } from '@effect/vitest';
import * as NodeServices from '@effect/platform-node/NodeServices';
import { Effect, Layer, Option } from 'effect';
import { Reactivity as ReactivityModule } from 'effect/reactivity';
import { SqlClient } from 'effect/sql';

import { LogStoreSqlite } from '../src/layer.js';
import { SqliteNative } from '../src/layer-native.js';
import { LogChain } from '@sunfall/vesper-log/chain';
import { LogStore } from '@sunfall/vesper-log/log-store';
import { LogOffset } from '@sunfall/vesper-log/offset';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';
import { logStoreContract } from '../../log/test/log-store-contract.js';

const storeLayer = LogStoreSqlite.layer().pipe(
  Layer.provideMerge(
    Layer.mergeAll(
      SqliteNative.layer(':memory:'),
      NodeServices.layer,
      ReactivityModule.layer,
    ),
  ),
);

// A fresh in-memory database, migrated, per build. The shared contract runs
// against it so SQLite is held to the same behaviour as memory and Postgres.
const migratedLayer = Layer.effect(
  LogStore.Service,
  Effect.gen(function* () {
    yield* LogStoreSqlite.migrate();
    return yield* LogStoreSqlite.make(yield* SqlClient.SqlClient);
  }),
).pipe(
  Layer.provide(
    Layer.mergeAll(
      SqliteNative.layer(':memory:'),
      NodeServices.layer,
      ReactivityModule.layer,
    ),
  ),
);

logStoreContract('sqlite', { layer: migratedLayer });

const textEntry = (value: string) => ({
  conversationId: LogVocabulary.ConversationId.make('conversation'),
  timestamp: 1,
  record: { _tag: 'Text' as const, step: 1, text: value },
});

/** Append one batch per element, claiming a producer first. */
const appendBatches = (
  path: string,
  batches: ReadonlyArray<ReadonlyArray<string>>,
  startSequence = 0,
) =>
  Effect.gen(function* () {
    const store = yield* LogStore.Service;
    const claim = yield* store.acquire(
      path,
      LogVocabulary.ProducerId.make('producer'),
    );
    for (const [index, batch] of batches.entries()) {
      yield* store.append({
        path,
        producerId: claim.producerId,
        epoch: claim.epoch,
        sequence: LogVocabulary.ProducerSequence.make(index + startSequence),
        records: batch.map(textEntry),
      });
    }
  });

const verify = (path: string) =>
  LogChain.verify(path).pipe(Effect.provide(NodeServices.layer));

describe('LogStore SQLite hash chain', () => {
  it.effect('detects row edits and deletions', () =>
    Effect.gen(function* () {
      yield* LogStoreSqlite.migrate();
      const store = yield* LogStore.Service;
      const sql = yield* SqlClient.SqlClient;
      yield* store.create('tampered', 'identity');
      yield* appendBatches('tampered', [['a', 'b'], ['c'], ['d']]);
      expect(yield* verify('tampered')).toMatchObject({
        _tag: 'Valid',
        batches: 3,
      });

      yield* sql.unsafe(
        `UPDATE vesper_log_records SET record = json_set(record, '$.text', 'x')
         WHERE path = 'tampered' AND record_offset = '${LogOffset.fromSeq(1n)}'`,
      ).unprepared;
      expect(yield* verify('tampered')).toMatchObject({
        _tag: 'Broken',
        reason: 'content_modified',
        offset: LogOffset.fromSeq(0n),
      });
    }).pipe(Effect.provide(storeLayer)),
  );

  it.effect('reports the same head from a store rebuilt on the same data', () =>
    Effect.gen(function* () {
      yield* LogStoreSqlite.migrate();
      const store = yield* LogStore.Service;
      yield* store.create('reopened', 'identity');
      yield* appendBatches('reopened', [['a', 'b'], ['c']]);
      const before = yield* store.meta('reopened');
      const reopened = yield* LogStoreSqlite.make(yield* SqlClient.SqlClient);
      const after = yield* reopened.meta('reopened');
      expect(Option.map(after, (m) => m.chainHead)).toEqual(
        Option.map(before, (m) => m.chainHead),
      );
      expect(Option.isSome(Option.getOrThrow(after).chainHead)).toBe(true);
    }).pipe(Effect.provide(storeLayer)),
  );

  it.effect('detects a deleted batch where the gap opens', () =>
    Effect.gen(function* () {
      yield* LogStoreSqlite.migrate();
      const store = yield* LogStore.Service;
      const sql = yield* SqlClient.SqlClient;
      yield* store.create('pruned', 'identity');
      yield* appendBatches('pruned', [['a'], ['b'], ['c']]);
      yield* sql.unsafe(
        `DELETE FROM vesper_log_records WHERE path = 'pruned' AND seq = '1'`,
      ).unprepared;
      expect(yield* verify('pruned')).toMatchObject({
        _tag: 'Broken',
        reason: 'offset_gap',
        offset: LogOffset.fromSeq(1n),
      });
    }).pipe(Effect.provide(storeLayer)),
  );

  it.effect('detects truncation, with or without a pinned head', () =>
    Effect.gen(function* () {
      yield* LogStoreSqlite.migrate();
      const store = yield* LogStore.Service;
      const sql = yield* SqlClient.SqlClient;
      yield* store.create('truncated', 'identity');
      yield* appendBatches('truncated', [['a'], ['b'], ['c']]);
      const pinned = Option.getOrThrow(
        Option.getOrThrow(yield* store.meta('truncated')).chainHead,
      );
      yield* sql.unsafe(
        `DELETE FROM vesper_log_records WHERE path = 'truncated' AND seq = '2'`,
      ).unprepared;
      // The stream row still reports the old head, so even an unpinned
      // verification notices.
      expect(yield* verify('truncated')).toMatchObject({
        _tag: 'Broken',
        reason: 'head_mismatch',
      });
      expect(
        yield* LogChain.verify('truncated', { expectedHead: pinned }).pipe(
          Effect.provide(NodeServices.layer),
        ),
      ).toMatchObject({ _tag: 'Broken', reason: 'head_mismatch' });
    }).pipe(Effect.provide(storeLayer)),
  );
});

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
