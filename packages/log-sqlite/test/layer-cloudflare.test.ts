import { createHash, randomBytes } from 'node:crypto';

import { describe, expect, it } from '@effect/vitest';
import { LogStore } from '@sunfall/vesper-log/log-store';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';
import { Crypto, Effect, Fiber, Layer, Stream } from 'effect';
import { Reactivity } from 'effect/reactivity';
import { SqlClient } from 'effect/sql';

import { logStoreContract } from '../../log/test/log-store-contract.js';
import { LogStoreSqlite } from '../src/layer.js';
import { SqliteCloudflare } from '../src/layer-cloudflare.js';
import { sha256 } from '../src/sha256.js';
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

logStoreContract('sqlite on a Durable Object stand-in', {
  layer: Layer.unwrap(Effect.sync(() => storeLayerFor(makeStandInStorage()))),
});

describe('SqliteCloudflare', () => {
  it('rejects SQL-level transaction control like a Durable Object', () => {
    const storage = makeStandInStorage();
    for (const statement of ['BEGIN', 'COMMIT', 'SAVEPOINT x', 'ROLLBACK']) {
      expect(() => storage.sql.exec(statement)).toThrow(/transactionSync/);
    }
  });

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

const hex = (bytes: Uint8Array) => Buffer.from(bytes).toString('hex');
const text = (value: string) => new TextEncoder().encode(value);

describe('sha256', () => {
  const vectors: ReadonlyArray<readonly [string, Uint8Array, string]> = [
    [
      'empty',
      text(''),
      'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855',
    ],
    [
      'abc',
      text('abc'),
      'ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad',
    ],
    [
      'NIST 448-bit',
      text('abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq'),
      '248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1',
    ],
    [
      'NIST 896-bit',
      text(
        'abcdefghbcdefghicdefghijdefghijkefghijklfghijklmghijklmnhijklmnoijklmnopjklmnopqklmnopqrlmnopqrsmnopqrstnopqrstu',
      ),
      'cf5b16a778af8380036ce59e7b0492370b249b11e8f07a51afac45037afee9d1',
    ],
    [
      'one million a',
      new Uint8Array(1_000_000).fill(0x61),
      'cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0',
    ],
  ];

  for (const [name, input, expected] of vectors) {
    it(`matches the NIST vector: ${name}`, () => {
      expect(hex(sha256(input))).toBe(expected);
    });
  }

  it('agrees with node:crypto across padding boundaries', () => {
    for (let length = 0; length <= 200; length += 1) {
      const input = new Uint8Array(randomBytes(length));
      expect(hex(sha256(input))).toBe(
        createHash('sha256').update(input).digest('hex'),
      );
    }
  });

  it('is what the Worker Crypto layer uses, synchronously', () => {
    const digest = Effect.gen(function* () {
      const crypto = yield* Crypto.Crypto;
      return yield* crypto.digest('SHA-256', text('abc'));
    }).pipe(Effect.provide(SqliteCloudflare.cryptoLayer));
    expect(hex(Effect.runSync(digest))).toBe(
      'ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad',
    );
  });
});
