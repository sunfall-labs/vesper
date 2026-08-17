import { describe, expect, it } from '@effect/vitest';
import { Effect, Schema } from 'effect';

import { ConversationRecord } from '../src/record.js';
import { LogVocabulary } from '../src/vocabulary.js';

describe('log vocabulary', () => {
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
      const entry = yield* ConversationRecord.decodeEntry(wire);
      const prepared = yield* ConversationRecord.prepare([entry]);

      expect(prepared.encoded).toBe(JSON.stringify([wire]));
      expect(prepared.fingerprint).toBe(
        '52444ef19045615f0c872c2a2ac3c8f9be729dfad83ff1fdafaf01fbd37a6705',
      );
    }));

  it('rejects empty and malformed branded values', () => {
    expect(() =>
      Schema.decodeUnknownSync(LogVocabulary.ConversationId)(''),
    ).toThrow();
    expect(() =>
      Schema.decodeUnknownSync(LogVocabulary.AgentRevision)(' revision '),
    ).toThrow();
    expect(() => Schema.decodeUnknownSync(LogVocabulary.Epoch)(-1)).toThrow();
    expect(() =>
      Schema.decodeUnknownSync(LogVocabulary.ProducerSequence)(
        Number.MAX_SAFE_INTEGER + 1,
      ),
    ).toThrow();
  });
});

const conversationId = LogVocabulary.ConversationId.make('conversation');
const toolCallId = LogVocabulary.ToolCallId.make('call');
const producerId = LogVocabulary.ProducerId.make('producer');
const epoch = LogVocabulary.Epoch.make(1);
const producerSequence = LogVocabulary.ProducerSequence.make(1);

// @ts-expect-error distinct string brands are not interchangeable
const badToolCallId: LogVocabulary.ToolCallId = conversationId;
// @ts-expect-error producer ids are not conversation ids
const badProducerId: LogVocabulary.ProducerId = toolCallId;
// @ts-expect-error numeric protocol brands are not interchangeable
const badSequence: LogVocabulary.ProducerSequence = epoch;
// @ts-expect-error numeric protocol brands are not interchangeable
const badEpoch: LogVocabulary.Epoch = producerSequence;
// @ts-expect-error raw strings cannot enter low-level protocol fields
const rawProducerId: LogVocabulary.ProducerId = 'producer';

void producerId;
void badToolCallId;
void badProducerId;
void badSequence;
void badEpoch;
void rawProducerId;
