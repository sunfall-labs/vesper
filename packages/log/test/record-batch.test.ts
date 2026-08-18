import * as NodeServices from '@effect/platform-node/NodeServices';
import { describe, expect, it } from '@effect/vitest';
import { Effect } from 'effect';

import { RecordBatch } from '../src/record-batch.js';

describe('record batch', () => {
  it('keeps branded record JSON and fingerprints representation-transparent', () =>
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

      expect(prepared.encoded).toBe(JSON.stringify([wire]));
      expect(prepared.fingerprint).toBe(
        '52444ef19045615f0c872c2a2ac3c8f9be729dfad83ff1fdafaf01fbd37a6705',
      );
    }).pipe(Effect.provide(NodeServices.layer)));

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
});
