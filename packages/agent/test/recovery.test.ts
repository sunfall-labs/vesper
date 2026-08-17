import { LogOffset } from '@sunfall/vesper-log/offset';
import { RecordBatch } from '@sunfall/vesper-log/record-batch';
import { ConversationRecord, FORMAT_VERSION } from '@sunfall/vesper-log/record';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';
import { describe, expect, it } from '@effect/vitest';
import { Deferred, Effect, Option } from 'effect';

import * as Recovery from '../src/recovery.js';

const conversationId = LogVocabulary.ConversationId.make('recovery-test');
const toolCallId = LogVocabulary.ToolCallId.make('call-1');
const secondToolCallId = LogVocabulary.ToolCallId.make('call-2');

const envelope = (
  sequence: number,
  record: ConversationRecord.Record,
): ConversationRecord.Envelope =>
  RecordBatch.envelope(LogOffset.fromSeq(BigInt(sequence)), {
    conversationId,
    timestamp: sequence,
    record,
  });

const orphan = (records: ReadonlyArray<ConversationRecord.Record>) =>
  records.map((record, index) => envelope(index, record));

const started = {
  _tag: 'RunStarted' as const,
  agent: 'test',
  formatVersion: FORMAT_VERSION,
  agentRevision: LogVocabulary.AgentRevision.make('1'),
  prompt: [],
};

describe('tool recovery state', () => {
  it('folds an orphan into one indeterminate call in provider order', () => {
    const snapshot = Recovery.fold(
      orphan([
        started,
        {
          _tag: 'ToolCall',
          step: 1,
          id: toolCallId,
          name: 'lookup',
          params: { id: '42' },
        },
        { _tag: 'ToolStarted', id: toolCallId, name: 'lookup' },
      ]),
    );

    expect(snapshot.indeterminate).toEqual([
      {
        step: 1,
        name: 'lookup',
        toolCallId,
        params: { id: '42' },
      },
    ]);
    expect(snapshot.corruption).toBeUndefined();
    expect(snapshot.recoveries.get('lookup\u001fcall-1')).toEqual({
      _tag: 'Indeterminate',
    });
  });

  it('clears a settled run and ignores an abandoned branch', () => {
    const settled = Recovery.fold(
      orphan([
        started,
        {
          _tag: 'ToolCall',
          step: 1,
          id: toolCallId,
          name: 'lookup',
          params: {},
        },
        { _tag: 'ToolStarted', id: toolCallId, name: 'lookup' },
        {
          _tag: 'RunSettled',
          outcome: 'interrupted',
          detail: 'test',
          steps: 1,
          usage: { input: 0, output: 0 },
        },
      ]),
    );
    expect(settled.recoveries.size).toBe(0);
    expect(settled.indeterminate).toEqual([]);

    const branched = Recovery.fold([
      envelope(0, started),
      envelope(1, {
        _tag: 'ToolCall',
        step: 1,
        id: toolCallId,
        name: 'lookup',
        params: {},
      }),
      envelope(2, { _tag: 'ToolStarted', id: toolCallId, name: 'lookup' }),
      envelope(3, { _tag: 'BranchedFrom', at: LogOffset.fromSeq(0n) }),
    ]);
    expect(branched.recoveries.size).toBe(0);
    expect(branched.indeterminate).toEqual([]);
  });

  it('reports a durable start without its provider call as corruption', () => {
    const snapshot = Recovery.fold(
      orphan([
        started,
        { _tag: 'ToolStarted', id: toolCallId, name: 'lookup' },
      ]),
    );

    expect(snapshot.corruption).toContain('no matching ToolCall');
    expect(snapshot.indeterminate).toEqual([]);
  });

  it.effect(
    'tracks live records and releases waiters after durable outcomes',
    () =>
      Effect.gen(function* () {
        const snapshot = Recovery.fold(
          orphan([
            started,
            {
              _tag: 'ToolCall',
              step: 1,
              id: toolCallId,
              name: 'lookup',
              params: { id: '42' },
            },
            { _tag: 'ToolStarted', id: toolCallId, name: 'lookup' },
          ]),
        );
        const tracker = yield* Recovery.make(snapshot);
        const released = yield* Deferred.make<void>();

        const initial = tracker.recovery('lookup', toolCallId);
        expect(Option.isSome(initial) ? initial.value : undefined).toEqual({
          _tag: 'Indeterminate',
        });
        expect(yield* tracker.hasPendingToolCalls).toBe(true);

        yield* tracker.track([
          { _tag: 'ToolStarted', id: secondToolCallId, name: 'lookup' },
        ]);
        const live = tracker.recovery('lookup', secondToolCallId);
        expect(Option.isSome(live) ? live.value : undefined).toEqual({
          _tag: 'Indeterminate',
        });

        tracker.onToolSettled(
          'lookup',
          toolCallId,
          Deferred.succeed(released, undefined),
        );
        yield* tracker.track([
          {
            _tag: 'ToolOutcome',
            step: 1,
            id: toolCallId,
            name: 'lookup',
            outcome: 'success',
            result: { status: 'ok' },
          },
          {
            _tag: 'ToolOutcome',
            step: 1,
            id: secondToolCallId,
            name: 'lookup',
            outcome: 'failure',
            result: { error: 'not found' },
          },
        ]);

        expect(yield* Deferred.await(released)).toBeUndefined();
        const settled = tracker.recovery('lookup', toolCallId);
        expect(Option.isSome(settled) ? settled.value : undefined).toEqual({
          _tag: 'Settled',
          outcome: 'success',
          result: { status: 'ok' },
        });
        expect(yield* tracker.hasPendingToolCalls).toBe(false);
      }),
  );
});
