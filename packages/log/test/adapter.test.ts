import * as NodeServices from '@effect/platform-node/NodeServices';
import { describe, expect, it } from '@effect/vitest';
import { Effect } from 'effect';

import { LogStoreAdapter } from '../src/adapter.js';
import type { ConversationRecord } from '../src/record.js';
import { LogVocabulary } from '../src/vocabulary.js';

const producer = LogVocabulary.ProducerId.make('producer');
const epoch = LogVocabulary.Epoch.make(1);
const sequence = LogVocabulary.ProducerSequence.make;

const text = (value: string): ConversationRecord.Entry => ({
  conversationId: LogVocabulary.ConversationId.make('conversation'),
  timestamp: 1,
  record: { _tag: 'Text', step: 1, text: value },
});

const input = (
  value: string,
  producerSequence = 0,
): LogStoreAdapter.ValidatedInput => ({
  path: 'conversation',
  producerId: producer,
  epoch,
  sequence: sequence(producerSequence),
  records: [text(value)],
});

describe('LogStoreAdapter', () => {
  it.effect('advances a new append and recognizes its exact retry', () =>
    Effect.gen(function* () {
      const first = yield* LogStoreAdapter.decide(input('first'), {
        epoch,
        producerId: producer,
        nextSequence: sequence(0),
        lastFingerprint: '',
      });
      expect(first).toMatchObject({ kind: 'append', nextSequence: 1 });

      const retry = yield* LogStoreAdapter.decide(input('first'), {
        epoch,
        producerId: producer,
        nextSequence: sequence(1),
        lastFingerprint: first.prepared.fingerprint,
      });
      expect(retry).toMatchObject({ kind: 'retry', nextSequence: 1 });
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect('distinguishes changed retries, gaps, and fenced producers', () =>
    Effect.gen(function* () {
      const accepted = yield* LogStoreAdapter.decide(input('first'), {
        epoch,
        producerId: producer,
        nextSequence: sequence(0),
        lastFingerprint: '',
      });
      const state: LogStoreAdapter.FencingState = {
        epoch,
        producerId: producer,
        nextSequence: sequence(1),
        lastFingerprint: accepted.prepared.fingerprint,
      };

      const changed = yield* LogStoreAdapter.decide(
        input('changed'),
        state,
      ).pipe(Effect.flip);
      expect(changed.reason).toBe('conflict');

      const gap = yield* LogStoreAdapter.decide(input('later', 3), state).pipe(
        Effect.flip,
      );
      expect(gap.reason).toBe('gap');

      const fenced = yield* LogStoreAdapter.decide(input('stale'), {
        ...state,
        epoch: LogVocabulary.Epoch.make(2),
      }).pipe(Effect.flip);
      expect(fenced.reason).toBe('fenced');

      const producerConflict = yield* LogStoreAdapter.decide(
        input('impostor', 1),
        {
          ...state,
          producerId: LogVocabulary.ProducerId.make('other-producer'),
        },
      ).pipe(Effect.flip);
      expect(producerConflict.reason).toBe('conflict');
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect('validates the wire input before either storage adapter runs', () =>
    Effect.gen(function* () {
      const empty = yield* LogStoreAdapter.validateInput({
        path: 'conversation',
        producerId: producer,
        epoch,
        sequence: sequence(0),
        records: [],
      }).pipe(Effect.flip);
      expect(empty.reason).toBe('empty');

      const invalidSequence = yield* LogStoreAdapter.validateInput({
        path: 'conversation',
        producerId: producer,
        epoch,
        sequence: -1 as LogVocabulary.ProducerSequence,
        records: [text('invalid')],
      }).pipe(Effect.flip);
      expect(invalidSequence.reason).toBe('conflict');

      const invalidEpoch = yield* LogStoreAdapter.validateInput({
        path: 'conversation',
        producerId: producer,
        epoch: -1 as LogVocabulary.Epoch,
        sequence: sequence(0),
        records: [text('invalid')],
      }).pipe(Effect.flip);
      expect(invalidEpoch.reason).toBe('conflict');

      const invalidProducer = yield* LogStoreAdapter.validateInput({
        path: 'conversation',
        producerId: '' as LogVocabulary.ProducerId,
        epoch,
        sequence: sequence(0),
        records: [text('invalid')],
      }).pipe(Effect.flip);
      expect(invalidProducer.reason).toBe('conflict');
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect('classifies an unencodable current-producer payload', () =>
    Effect.gen(function* () {
      const unencodable: LogStoreAdapter.ValidatedInput = {
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
      const error = yield* LogStoreAdapter.decide(unencodable, {
        epoch,
        producerId: producer,
        nextSequence: sequence(0),
        lastFingerprint: '',
      }).pipe(Effect.flip);

      expect(error.reason).toBe('encoding');
    }).pipe(Effect.provide(NodeServices.layer)),
  );
});
