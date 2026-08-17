import { describe, expect, it } from '@effect/vitest';
import { Schema } from 'effect';

import { LogVocabulary } from '../src/vocabulary.js';

describe('log vocabulary', () => {
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
