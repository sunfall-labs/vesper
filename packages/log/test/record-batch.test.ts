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
    }));
});
