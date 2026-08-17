import { PgClient } from '@effect/sql-pg';
import { describe, expect, it } from '@effect/vitest';
import { Effect } from 'effect';

import { LogStorePg } from '../src/layer-pg.js';
import { LogOffset } from '../src/offset.js';
import { LogVocabulary } from '../src/vocabulary.js';

describe('LogStore Postgres SQL', () => {
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
        withTransaction: (body: Effect.Effect<unknown>) => body,
        listen: () => Effect.die('not used'),
      } as unknown as PgClient.PgClient;
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
        yield* LogStorePg.make(client).append({
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
        const encoded = JSON.parse(String(write.params[5])) as Array<{
          record: { params: Record<string, number> };
        }>;
        expect(encoded).toHaveLength(1_000);
        expect(Object.keys(encoded[0]!.record.params)).toEqual(['a', 'z']);
      });
    },
  );

  const clientFor = (
    statements: Array<{
      readonly text: string;
      readonly params: ReadonlyArray<unknown>;
    }>,
  ) =>
    ({
      unsafe: (text: string, params: ReadonlyArray<unknown> = []) => {
        statements.push({ text, params });
        return Effect.succeed([{ epoch: '4' }]);
      },
      withTransaction: () => Effect.die('not used'),
      listen: () => Effect.die('not used'),
    }) as unknown as PgClient.PgClient;

  it.effect('binds expected epoch and head into the atomic epoch bump', () => {
    const statements: Array<{
      readonly text: string;
      readonly params: ReadonlyArray<unknown>;
    }> = [];

    return Effect.gen(function* () {
      const claim = yield* LogStorePg.make(clientFor(statements)).acquire(
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
    });
  });

  it.effect('leaves expected predicates disabled for legacy acquire', () => {
    const statements: Array<{
      readonly text: string;
      readonly params: ReadonlyArray<unknown>;
    }> = [];

    return Effect.gen(function* () {
      yield* LogStorePg.make(clientFor(statements)).acquire(
        'stream',
        LogVocabulary.ProducerId.make('producer'),
      );

      expect(statements).toHaveLength(1);
      expect(statements[0]!.params).toEqual(['stream', 'producer', null, null]);
    });
  });
});
