import { describe, expect, it } from '@effect/vitest';
import { Effect } from 'effect';

import * as AppendDecision from '../src/append-decision.js';
import { LogStore } from '../src/log-store.js';
import { ConversationRecord } from '../src/record.js';
import { LogVocabulary } from '../src/vocabulary.js';

const producer = LogVocabulary.ProducerId.make('producer');
const epoch = LogVocabulary.Epoch.make(1);
const sequence = LogVocabulary.ProducerSequence.make;

const failure: AppendDecision.Failure = (path, operation, reason, detail) =>
  new LogStore.LogStoreError({ path, operation, reason, detail });

const text = (value: string): ConversationRecord.Entry => ({
  conversationId: LogVocabulary.ConversationId.make('conversation'),
  timestamp: 1,
  record: { _tag: 'Text', step: 1, text: value },
});

const input = (
  value: string,
  producerSequence = 0,
): AppendDecision.ValidatedInput => ({
  path: 'conversation',
  producerId: producer,
  epoch,
  sequence: sequence(producerSequence),
  records: [text(value)],
});

describe('append decision', () => {
  it.effect('advances a new append and recognizes its exact retry', () =>
    Effect.gen(function* () {
      const first = yield* AppendDecision.decide(
        input('first'),
        {
          epoch,
          producerId: producer,
          nextSequence: sequence(0),
          lastFingerprint: '',
        },
        failure,
      );
      expect(first).toMatchObject({ kind: 'append', nextSequence: 1 });

      const retry = yield* AppendDecision.decide(
        input('first'),
        {
          epoch,
          producerId: producer,
          nextSequence: sequence(1),
          lastFingerprint: first.prepared.fingerprint,
        },
        failure,
      );
      expect(retry).toMatchObject({ kind: 'retry', nextSequence: 1 });
    }),
  );

  it.effect('distinguishes changed retries, gaps, and fenced producers', () =>
    Effect.gen(function* () {
      const accepted = yield* AppendDecision.decide(
        input('first'),
        {
          epoch,
          producerId: producer,
          nextSequence: sequence(0),
          lastFingerprint: '',
        },
        failure,
      );
      const state: AppendDecision.FencingState = {
        epoch,
        producerId: producer,
        nextSequence: sequence(1),
        lastFingerprint: accepted.prepared.fingerprint,
      };

      const changed = yield* AppendDecision.decide(
        input('changed'),
        state,
        failure,
      ).pipe(Effect.flip);
      expect(changed.reason).toBe('conflict');

      const gap = yield* AppendDecision.decide(
        input('later', 3),
        state,
        failure,
      ).pipe(Effect.flip);
      expect(gap.reason).toBe('gap');

      const fenced = yield* AppendDecision.decide(
        input('stale'),
        { ...state, epoch: LogVocabulary.Epoch.make(2) },
        failure,
      ).pipe(Effect.flip);
      expect(fenced.reason).toBe('fenced');

      const producerConflict = yield* AppendDecision.decide(
        input('impostor', 1),
        {
          ...state,
          producerId: LogVocabulary.ProducerId.make('other-producer'),
        },
        failure,
      ).pipe(Effect.flip);
      expect(producerConflict.reason).toBe('conflict');
    }),
  );

  it.effect('validates the wire input before either storage adapter runs', () =>
    Effect.gen(function* () {
      const empty = yield* AppendDecision.validateInput(
        {
          path: 'conversation',
          producerId: producer,
          epoch,
          sequence: sequence(0),
          records: [],
        },
        failure,
      ).pipe(Effect.flip);
      expect(empty.reason).toBe('empty');

      const invalidSequence = yield* AppendDecision.validateInput(
        {
          path: 'conversation',
          producerId: producer,
          epoch,
          sequence: -1 as LogVocabulary.ProducerSequence,
          records: [text('invalid')],
        },
        failure,
      ).pipe(Effect.flip);
      expect(invalidSequence.reason).toBe('conflict');

      const invalidEpoch = yield* AppendDecision.validateInput(
        {
          path: 'conversation',
          producerId: producer,
          epoch: -1 as LogVocabulary.Epoch,
          sequence: sequence(0),
          records: [text('invalid')],
        },
        failure,
      ).pipe(Effect.flip);
      expect(invalidEpoch.reason).toBe('conflict');

      const invalidProducer = yield* AppendDecision.validateInput(
        {
          path: 'conversation',
          producerId: '' as LogVocabulary.ProducerId,
          epoch,
          sequence: sequence(0),
          records: [text('invalid')],
        },
        failure,
      ).pipe(Effect.flip);
      expect(invalidProducer.reason).toBe('conflict');
    }),
  );

  it.effect('classifies an unencodable current-producer payload', () =>
    Effect.gen(function* () {
      const unencodable: AppendDecision.ValidatedInput = {
        ...input('ignored'),
        records: [
          {
            conversationId: LogVocabulary.ConversationId.make('conversation'),
            timestamp: 1,
            record: {
              _tag: 'ToolCall',
              step: 1,
              id: LogVocabulary.ToolCallId.make('call'),
              name: 'tool',
              params: { value: 1n },
            },
          },
        ],
      };
      const error = yield* AppendDecision.decide(
        unencodable,
        {
          epoch,
          producerId: producer,
          nextSequence: sequence(0),
          lastFingerprint: '',
        },
        failure,
      ).pipe(Effect.flip);

      expect(error.reason).toBe('encoding');
    }),
  );
});
