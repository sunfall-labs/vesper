import * as NodeServices from '@effect/platform-node/NodeServices';
import type { PgConnection } from '@effect/sql-pg';
import { describe, expect, it } from '@effect/vitest';
import { LogOffset } from '@sunfall/vesper-log/offset';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';
import { Effect, Queue, Schema, Stream } from 'effect';
import { SqlError } from 'effect/sql';

import { LogStorePg } from '../src/layer.js';

const at = <T>(values: ReadonlyArray<T>, index: number): T => {
  const value = values[index];
  if (value === undefined) {
    throw new Error(`missing value at index ${String(index)}`);
  }
  return value;
};

const inTransaction = <A, E, R>(effect: Effect.Effect<A, E, R>) => effect;
const unusedListen = () => Effect.die('not used');

describe('LogStore Postgres SQL', () => {
  it.effect('opens changes with a tick and fails when the listener dies', () =>
    Effect.gen(function* () {
      const notifications = yield* Queue.unbounded<
        PgConnection.Notification,
        SqlError.SqlError
      >();
      const store = yield* LogStorePg.make({
        unsafe: () => Effect.die('not used'),
        withTransaction: inTransaction,
        listen: () => Effect.succeed(notifications),
      });
      yield* Queue.offer(notifications, {
        processId: 1,
        channel: 'unused',
        payload: '',
      });
      yield* Queue.fail(
        notifications,
        new SqlError.SqlError({
          reason: new SqlError.ConnectionError({
            cause: new Error('Connection closed'),
            message: 'Connection closed',
            operation: 'listen',
          }),
        }),
      );

      const ticks: Array<void> = [];
      const error = yield* store.changes('stream').pipe(
        Stream.runForEach((tick) => Effect.sync(() => ticks.push(tick))),
        Effect.flip,
      );
      expect(ticks).toHaveLength(2);
      expect(error).toMatchObject({
        _tag: 'LogStoreError',
        operation: 'changes',
        reason: 'storage',
      });
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect(
    'uses one fixed-parameter recordset statement for the whole batch',
    () => {
      const statements: Array<{
        readonly text: string;
        readonly params: ReadonlyArray<unknown>;
      }> = [];
      const client = {
        unsafe: (text: string, params: ReadonlyArray<unknown> = []) => {
          statements.push({ text, params });
          if (text.includes('FOR UPDATE')) {
            return Effect.succeed([
              {
                identity: 'identity',
                epoch: 1,
                producer_id: 'producer',
                next_sequence: '0',
                next_producer_sequence: '0',
                last_fingerprint: '',
                chain_head: '',
                last_offset: '-1',
              },
            ]);
          }
          return Effect.succeed([]);
        },
        withTransaction: inTransaction,
        listen: unusedListen,
      } satisfies LogStorePg.Client;
      const records = Array.from({ length: 1_000 }, (_, index) => ({
        conversationId: LogVocabulary.ConversationId.make('conversation'),
        timestamp: 1_700_000_000_000 + index,
        record: {
          _tag: 'ToolCall' as const,
          step: index,
          id: LogVocabulary.ToolCallId.make(`call-${index}`),
          name: 'tool',
          params: { z: index, a: index },
        },
      }));

      return Effect.gen(function* () {
        const store = yield* LogStorePg.make(client);
        yield* store.append({
          path: 'large',
          producerId: LogVocabulary.ProducerId.make('producer'),
          epoch: LogVocabulary.Epoch.make(1),
          sequence: LogVocabulary.ProducerSequence.make(0),
          records,
        });

        expect(statements).toHaveLength(3);
        expect(at(statements, 0).text).toBe(
          'SET LOCAL statement_timeout = 30000',
        );
        const write = at(statements, 2);
        expect(write.text).toContain('jsonb_array_elements($6::jsonb)');
        expect(write.text).toContain('inserted AS');
        expect(write.text).toContain('advanced AS');
        expect(write.text).toContain('pg_notify($11, $12)');
        expect(write.params).toHaveLength(13);
        expect(write.params[11]).toBe('');
        const encoded = Schema.decodeUnknownSync(
          Schema.Array(
            Schema.Struct({
              record: Schema.Struct({
                params: Schema.Record(Schema.String, Schema.Number),
              }),
            }),
          ),
        )(JSON.parse(String(write.params[5])));
        expect(encoded).toHaveLength(1_000);
        expect(Object.keys(at(encoded, 0).record.params)).toEqual(['a', 'z']);
      }).pipe(Effect.provide(NodeServices.layer));
    },
  );

  const clientFor = (
    statements: Array<{
      readonly text: string;
      readonly params: ReadonlyArray<unknown>;
    }>,
  ): LogStorePg.Client => ({
    unsafe: (text: string, params: ReadonlyArray<unknown> = []) => {
      statements.push({ text, params });
      return Effect.succeed([{ epoch: '4' }]);
    },
    withTransaction: inTransaction,
    listen: unusedListen,
  });

  it.effect('binds expected epoch and head into the atomic epoch bump', () => {
    const statements: Array<{
      readonly text: string;
      readonly params: ReadonlyArray<unknown>;
    }> = [];

    return Effect.gen(function* () {
      const store = yield* LogStorePg.make(clientFor(statements));
      const claim = yield* store.acquire(
        'stream',
        LogVocabulary.ProducerId.make('producer'),
        {
          epoch: LogVocabulary.Epoch.make(3),
          head: LogOffset.fromSeq(7n),
        },
      );

      expect(claim.epoch).toBe(4);
      expect(statements).toHaveLength(1);
      expect(at(statements, 0).text).toContain('SET epoch = epoch + 1');
      expect(at(statements, 0).text).toContain('epoch = $3');
      expect(at(statements, 0).text).toContain('last_offset = $4');
      expect(at(statements, 0).params).toEqual([
        'stream',
        LogVocabulary.ProducerId.make('producer'),
        3,
        '0000000000000000_0000000000000007',
      ]);
    }).pipe(Effect.provide(NodeServices.layer));
  });

  it.effect('leaves expected predicates disabled for legacy acquire', () => {
    const statements: Array<{
      readonly text: string;
      readonly params: ReadonlyArray<unknown>;
    }> = [];

    return Effect.gen(function* () {
      const store = yield* LogStorePg.make(clientFor(statements));
      yield* store.acquire('stream', LogVocabulary.ProducerId.make('producer'));

      expect(statements).toHaveLength(1);
      expect(at(statements, 0).params).toEqual([
        'stream',
        'producer',
        null,
        null,
      ]);
    }).pipe(Effect.provide(NodeServices.layer));
  });

  const metaClientFor = (
    row: Readonly<Record<string, unknown>>,
  ): LogStorePg.Client => ({
    unsafe: () => Effect.succeed([row]),
    withTransaction: inTransaction,
    listen: unusedListen,
  });

  const validStreamRow = {
    identity: 'identity',
    epoch: 1,
    producer_id: 'producer',
    next_sequence: '0',
    next_producer_sequence: '0',
    last_fingerprint: '',
    last_offset: '-1',
    chain_head: '',
  };

  it.effect(
    'rejects null and object stream columns instead of coercing them',
    () =>
      Effect.gen(function* () {
        const nullIdentity = yield* LogStorePg.make(
          metaClientFor({ ...validStreamRow, identity: null }),
        );
        const nullError = yield* Effect.flip(nullIdentity.meta('stream'));
        expect(nullError).toMatchObject({
          _tag: 'LogStoreError',
          operation: 'meta',
          reason: 'storage',
        });

        const objectSequence = yield* LogStorePg.make(
          metaClientFor({ ...validStreamRow, next_sequence: {} }),
        );
        const objectError = yield* Effect.flip(objectSequence.meta('stream'));
        expect(objectError).toMatchObject({
          _tag: 'LogStoreError',
          operation: 'meta',
          reason: 'storage',
        });
      }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect(
    'rejects non-finite numeric stream columns as storage corruption',
    () =>
      Effect.gen(function* () {
        const store = yield* LogStorePg.make(
          metaClientFor({ ...validStreamRow, epoch: Number.POSITIVE_INFINITY }),
        );
        const error = yield* Effect.flip(store.meta('stream'));
        expect(error).toMatchObject({
          _tag: 'LogStoreError',
          operation: 'meta',
          reason: 'storage',
        });
      }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect(
    'rejects malformed persisted record columns as storage corruption',
    () =>
      Effect.gen(function* () {
        const store = yield* LogStorePg.make(
          metaClientFor({
            record_offset: '0000000000000000_0000000000000000',
            conversation_id: 'conversation',
            record_timestamp: Number.POSITIVE_INFINITY,
            record: { _tag: 'Text', step: 0, text: 'hello' },
          }),
        );
        const error = yield* Effect.flip(store.read('stream'));
        expect(error).toMatchObject({
          _tag: 'LogStoreError',
          operation: 'read',
          reason: 'storage',
        });
      }).pipe(Effect.provide(NodeServices.layer)),
  );
});
