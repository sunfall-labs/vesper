import { LogOffset } from '@sunfall/vesper-log/offset';
import {
  type ConversationRecord,
  FORMAT_VERSION,
} from '@sunfall/vesper-log/record';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';
import { describe, expect, it } from '@effect/vitest';
import { Prompt } from 'effect/ai';

import { AgentHistory } from '../src/history.js';
import { testBudget } from './budget-fixture.js';

const CONVERSATION = LogVocabulary.ConversationId.make('pairing');
const CALL = LogVocabulary.ToolCallId.make('call-1');

const envelopes = (
  records: ReadonlyArray<ConversationRecord.Record>,
): ReadonlyArray<ConversationRecord.Envelope> =>
  records.map((record, index) => ({
    offset: LogOffset.fromSeq(BigInt(index)),
    conversationId: CONVERSATION,
    timestamp: 0,
    record,
  }));

const started = (text: string): ConversationRecord.Record => ({
  _tag: 'RunStarted',
  budget: testBudget,
  agent: 'test',
  formatVersion: FORMAT_VERSION,
  agentRevision: LogVocabulary.AgentRevision.make('1'),
  prompt: Prompt.make(text).content,
});

describe('pairing a call with an answer recorded after its turn', () => {
  // A turn can finish before every call in it is answered: an approval is
  // decided, or an indeterminate call reconciled, after the turn's
  // `TurnFinished`. The model must still see the call followed by its
  // answer. Mutation-checked: closing the turn at `TurnFinished` drops both.
  it('keeps the call and its later answer adjacent', () => {
    const rebuilt = AgentHistory.messagesFrom(
      envelopes([
        started('look it up'),
        { _tag: 'ToolCall', step: 1, id: CALL, name: 'lookup', params: {} },
        { _tag: 'TurnFinished', step: 1, usage: { input: 1, output: 1 } },
        {
          _tag: 'ToolOutcome',
          step: 1,
          id: CALL,
          name: 'lookup',
          outcome: 'success',
          result: 'found',
        },
        started('and then?'),
      ]),
    );

    const roles = rebuilt.content.map((message) => message.role);
    expect(roles).toEqual(['user', 'assistant', 'tool', 'user']);
    expect(rebuilt.content[2]).toMatchObject({
      role: 'tool',
      content: [{ type: 'tool-result', id: CALL, result: 'found' }],
    });
  });

  it('drops a call whose answer never arrives', () => {
    const rebuilt = AgentHistory.messagesFrom(
      envelopes([
        started('look it up'),
        { _tag: 'ToolCall', step: 1, id: CALL, name: 'lookup', params: {} },
        { _tag: 'TurnFinished', step: 1, usage: { input: 1, output: 1 } },
        started('and then?'),
      ]),
    );

    expect(rebuilt.content.map((message) => message.role)).toEqual([
      'user',
      'user',
    ]);
  });
});
