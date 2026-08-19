import * as NodeServices from '@effect/platform-node/NodeServices';
import { describe, expect, it } from '@effect/vitest';
import { Effect } from 'effect';

import { RecordBatch } from '../src/record-batch.js';

describe('record batch', () => {
  it.effect('canonicalizes branded record JSON before fingerprinting', () =>
    Effect.gen(function* () {
      const wire = {
        conversationId: 'conversation-1',
        timestamp: 1_700_000_000_000,
        record: {
          _tag: 'ToolCall',
          step: 2,
          id: 'call-1',
          name: 'search',
          params: { query: 'vesper' },
        },
      };
      const entry = yield* RecordBatch.decodeEntry(wire);
      const prepared = yield* RecordBatch.prepare([entry]);

      expect(prepared.encoded).toBe(
        JSON.stringify([
          {
            conversationId: 'conversation-1',
            record: {
              _tag: 'ToolCall',
              id: 'call-1',
              name: 'search',
              params: { query: 'vesper' },
              step: 2,
            },
            timestamp: 1_700_000_000_000,
          },
        ]),
      );
      expect(prepared.fingerprint).toBe(
        'e80efade27b874768388165413dafe56c6a5873d2066bf775bdbb23fc61f5f3d',
      );
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect('rejects non-finite and non-integral durable numeric fields', () =>
    Effect.gen(function* () {
      const base = {
        conversationId: 'conversation-1',
        timestamp: 1_700_000_000_000,
        record: {
          _tag: 'ToolCall' as const,
          step: 2,
          id: 'call-1',
          name: 'search',
          params: { query: 'vesper' },
        },
      };

      for (const invalid of [
        { ...base, timestamp: Number.NaN },
        { ...base, timestamp: 1.5 },
        { ...base, record: { ...base.record, step: -1 } },
        {
          ...base,
          record: {
            _tag: 'TurnFinished' as const,
            step: 2,
            usage: { input: Number.POSITIVE_INFINITY, output: 1 },
          },
        },
      ]) {
        const result = yield* RecordBatch.decodeEntry(invalid).pipe(
          Effect.result,
        );
        expect(result._tag).toBe('Failure');
      }
    }),
  );

  it.effect('uses exact optional metadata at the wire boundary', () =>
    Effect.gen(function* () {
      const result = yield* RecordBatch.decodeEntry({
        conversationId: 'conversation-1',
        timestamp: 1_700_000_000_000,
        record: {
          _tag: 'RunStarted',
          agent: 'assistant',
          prompt: [],
        },
      });

      if (result.record._tag !== 'RunStarted') {
        throw new Error(`expected RunStarted, got ${result.record._tag}`);
      }
      expect(result.record.formatVersion).toBeUndefined();
      expect(result.record.agentRevision).toBeUndefined();

      const explicitUndefined = yield* RecordBatch.decodeEntry({
        conversationId: 'conversation-1',
        timestamp: 1_700_000_000_000,
        record: {
          _tag: 'RunStarted',
          agent: 'assistant',
          formatVersion: undefined,
          prompt: [],
        },
      }).pipe(Effect.result);

      expect(explicitUndefined._tag).toBe('Failure');
    }),
  );

  it.effect('rejects oversized strings before fingerprinting', () =>
    Effect.gen(function* () {
      const entry = yield* RecordBatch.decodeEntry({
        conversationId: 'conversation-1',
        timestamp: 1_700_000_000_000,
        record: {
          _tag: 'Text',
          step: 1,
          text: 'x'.repeat(RecordBatch.MAX_STRING_CHARS + 1),
        },
      });
      const result = yield* RecordBatch.prepare([entry]).pipe(Effect.result);

      expect(result._tag).toBe('Failure');
      if (result._tag === 'Failure') {
        expect(result.failure.detail).toContain('maximum string length');
      }
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect(
    'rejects deeply nested opaque values without recursing forever',
    () =>
      Effect.gen(function* () {
        let value: unknown = { leaf: true };
        for (let depth = 0; depth <= RecordBatch.MAX_JSON_DEPTH; depth += 1) {
          value = { next: value };
        }

        const entry = yield* RecordBatch.decodeEntry({
          conversationId: 'conversation-1',
          timestamp: 1_700_000_000_000,
          record: {
            _tag: 'ToolCall',
            step: 1,
            id: 'call-1',
            name: 'nested',
            params: value,
          },
        });
        const result = yield* RecordBatch.prepare([entry]).pipe(Effect.result);

        expect(result._tag).toBe('Failure');
        if (result._tag === 'Failure') {
          expect(result.failure.detail).toContain('maximum JSON depth');
        }
      }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect('rejects batches above the append bound', () =>
    Effect.gen(function* () {
      const entry = yield* RecordBatch.decodeEntry({
        conversationId: 'conversation-1',
        timestamp: 1_700_000_000_000,
        record: { _tag: 'Text', step: 1, text: 'x' },
      });
      const result = yield* RecordBatch.prepare(
        Array.from({ length: RecordBatch.MAX_RECORDS + 1 }, () => entry),
      ).pipe(Effect.result);

      expect(result._tag).toBe('Failure');
      if (result._tag === 'Failure') {
        expect(result.failure.detail).toContain('maximum is');
      }
    }).pipe(Effect.provide(NodeServices.layer)),
  );
});
