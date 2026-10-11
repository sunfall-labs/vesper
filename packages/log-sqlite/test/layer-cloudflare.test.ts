import { describe, expect, it } from '@effect/vitest';
import { LogStore } from '@sunfall/vesper-log/log-store';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';
import { Effect, Fiber, Layer, Stream } from 'effect';
import { Reactivity } from 'effect/reactivity';
import { SqlClient } from 'effect/sql';

import { logStoreContract } from '../../log/test/log-store-contract.js';
import { LogStoreSqlite } from '../src/layer.js';
import { SqliteCloudflare } from '../src/layer-cloudflare.js';
import type { DurableObjectStorage } from '../src/layer-cloudflare.js';
import { makeStandInStorage } from './durable-object-stand-in.js';

// Every test gets a fresh Durable Object, as each conversation does in prod.
const storeLayerFor = (storage: DurableObjectStorage) =>
  Layer.effectDiscard(LogStoreSqlite.migrate()).pipe(
    Layer.provideMerge(LogStoreSqlite.layer()),
    Layer.provideMerge(SqliteCloudflare.layer(storage)),
    Layer.provideMerge(
      Layer.mergeAll(SqliteCloudflare.cryptoLayer, Reactivity.layer),
    ),
  );

// The store hashes with the Worker crypto layer (the package's pure-JS SHA-256)
// while the contract's chain verification uses node:crypto, so a wrong digest
// anywhere fails the contract's hash-chain cases.
logStoreContract('sqlite on a Durable Object stand-in', {
  layer: Layer.unwrap(Effect.sync(() => storeLayerFor(makeStandInStorage()))),
});

describe('SqliteCloudflare', () => {
  it.effect(
    'commits through transactionSync and never issues control SQL',
    () => {
      const storage = makeStandInStorage();
      return Effect.gen(function* () {
        const store = yield* LogStore.Service;
        yield* store.create('conversation', 'identity');
        const claim = yield* store.acquire(
          'conversation',
          LogVocabulary.ProducerId.make('producer'),
        );
        yield* store.append({
          path: 'conversation',
          producerId: claim.producerId,
          epoch: claim.epoch,
          sequence: claim.nextSequence,
          records: [
            {
              conversationId: LogVocabulary.ConversationId.make('conversation'),
              timestamp: 1,
              record: { _tag: 'Text', step: 1, text: 'hello' },
            },
          ],
        });
        expect(storage.transactions()).toBeGreaterThan(0);
        expect(
          storage.statements.filter((sql) =>
            /^\s*(BEGIN|COMMIT|SAVEPOINT|ROLLBACK)/i.test(sql),
          ),
        ).toEqual([]);
      }).pipe(Effect.provide(storeLayerFor(storage)));
    },
  );

  it.effect('rolls back a failed transaction body and nests', () => {
    const storage = makeStandInStorage();
    return Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql.unsafe('CREATE TABLE t (v TEXT)').unprepared;
      const insert = (value: string) =>
        sql.unsafe('INSERT INTO t VALUES (?)', [value]).unprepared;
      const count = sql.unsafe('SELECT v FROM t').unprepared;

      yield* sql
        .withTransaction(Effect.andThen(insert('a'), Effect.fail('boom')))
        .pipe(Effect.ignore);
      expect(yield* count).toEqual([]);

      yield* sql.withTransaction(
        Effect.andThen(
          insert('kept'),
          sql
            .withTransaction(Effect.andThen(insert('inner'), Effect.fail('x')))
            .pipe(Effect.ignore),
        ),
      );
      expect(yield* count).toEqual([{ v: 'kept' }]);
    }).pipe(Effect.provide(SqliteCloudflare.layer(storage)));
  });

  it.effect('wakes a follower in the same Durable Object on append', () => {
    const storage = makeStandInStorage();
    return Effect.gen(function* () {
      const store = yield* LogStore.Service;
      yield* store.create('conversation', 'identity');
      const woken = yield* store
        .changes('conversation')
        .pipe(Stream.take(2), Stream.runCollect, Effect.forkChild);
      yield* Effect.yieldNow;
      const claim = yield* store.acquire(
        'conversation',
        LogVocabulary.ProducerId.make('producer'),
      );
      yield* store.append({
        path: 'conversation',
        producerId: claim.producerId,
        epoch: claim.epoch,
        sequence: claim.nextSequence,
        records: [
          {
            conversationId: LogVocabulary.ConversationId.make('conversation'),
            timestamp: 1,
            record: { _tag: 'Text', step: 1, text: 'hello' },
          },
        ],
      });
      expect(yield* Fiber.join(woken)).toHaveLength(2);
    }).pipe(Effect.provide(storeLayerFor(storage)));
  });
});
