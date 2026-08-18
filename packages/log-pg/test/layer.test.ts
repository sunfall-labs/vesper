import * as NodeServices from '@effect/platform-node/NodeServices';
import { describe, expect, it } from '@effect/vitest';
import { LogOffset } from '@sunfall/vesper-log/offset';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';
import { Effect, Fiber, Schema, Stream } from 'effect';

import { LogStorePg } from '../src/layer.js';
import {
  correctedListen,
  type ListenerClient,
} from '../src/internal/pg-listen.js';

class FakeListenerClient implements ListenerClient {
  endCalls = 0;

  constructor(readonly connectImpl: () => Promise<void>) {}

  on(
    _event: 'notification' | 'error' | 'end',
    _listener: ((...args: never[]) => void) | ((error: Error) => void),
  ): this {
    return this;
  }

  off(
    _event: 'notification' | 'error' | 'end',
    _listener: ((...args: never[]) => void) | ((error: Error) => void),
  ): this {
    return this;
  }

  connect(): Promise<void> {
    return this.connectImpl();
  }

  query(_text: string): Promise<void> {
    return Promise.resolve();
  }

  end(): Promise<void> {
    this.endCalls += 1;
    return Promise.resolve();
  }
}

describe('LogStore Postgres SQL', () => {
  const inTransaction = <A, E, R>(effect: Effect.Effect<A, E, R>) => effect;
  const unusedListen = () => Stream.die('not used');

  it.effect('closes a listener client when connect fails', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const client = new FakeListenerClient(() =>
          Promise.reject(new Error('connect failed')),
        );
        const outcome = yield* correctedListen(
          {},
          () => client,
        )('failed-connect').pipe(Stream.runDrain, Effect.result);

        expect(outcome._tag).toBe('Failure');
        expect(client.endCalls).toBe(1);
      }),
    ),
  );

  it.effect('closes a listener client when connect is interrupted', () =>
    Effect.scoped(
      Effect.gen(function* () {
        let resolveStarted!: () => void;
        const started = new Promise<void>((resolve) => {
          resolveStarted = resolve;
        });
        const client = new FakeListenerClient(
          () =>
            new Promise<void>(() => {
              resolveStarted();
            }),
        );
        const fiber = yield* correctedListen(
          {},
          () => client,
        )('interrupted-connect').pipe(Stream.runDrain, Effect.forkChild);

        yield* Effect.promise(() => started);
        yield* Fiber.interrupt(fiber);

        expect(client.endCalls).toBe(1);
      }),
    ),
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
        expect(statements[0]!.text).toBe('SET LOCAL statement_timeout = 30000');
        const write = statements[2]!;
        expect(write.text).toContain('jsonb_array_elements($6::jsonb)');
        expect(write.text).toContain('inserted AS');
        expect(write.text).toContain('advanced AS');
        expect(write.text).toContain('pg_notify($11, $12)');
        expect(write.params).toHaveLength(12);
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
        expect(Object.keys(encoded[0]!.record.params)).toEqual(['a', 'z']);
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
      expect(statements[0]!.text).toContain('SET epoch = epoch + 1');
      expect(statements[0]!.text).toContain('epoch = $3');
      expect(statements[0]!.text).toContain('last_offset = $4');
      expect(statements[0]!.params).toEqual([
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
      expect(statements[0]!.params).toEqual(['stream', 'producer', null, null]);
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
