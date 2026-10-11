import type { ConversationRecord } from '@sunfall/vesper-log/record';
import { LogOffset } from '@sunfall/vesper-log/offset';
import { describe, expect, it } from '@effect/vitest';
import { Effect, Option, Schema } from 'effect';
import { Tool, Toolkit } from 'effect/ai';

import { Agent } from '../src/agent.js';
import { Conversation } from '../src/conversation.js';
import { Replay } from '../src/replay.js';
import {
  callId,
  lookupCall,
  lookupOutcome,
  lookupStarted,
  openSession,
  provider,
  readRecords,
  run,
  says,
  seed,
  settled,
  started,
  testLogLayer,
} from './durability-fixtures.js';
import { toolCalled, toolStarted } from './tool-fixtures.js';

// Crash recovery. A run that died after a tool's handler was entered leaves
// that call indeterminate: the log cannot say whether the side effect
// happened. Resuming must never guess. It either re-runs a tool the author
// declared replay-safe, or answers the call as interrupted, or fails.

const ID = 'recovery';
const CALL = callId('call-1');

const parameters = Schema.Struct({ id: Schema.String });
const safeLookup = Replay.safe(
  Tool.make('lookup', {
    parameters,
    success: Schema.String,
    failureMode: 'return',
  }),
);
const send = Tool.make('send', {
  parameters,
  success: Schema.String,
  failureMode: 'return',
});
// A transforming parameter schema: the stored call holds the string, the
// handler must receive a `Date`.
const dated = Replay.safe(
  Tool.make('dated', {
    parameters: Schema.Struct({ at: Schema.DateFromString }),
    success: Schema.String,
    failureMode: 'return',
  }),
);

const replayAgent = (ran: { count: number }) =>
  Agent.make({
    name: 'test',
    revision: '1',
    instructions: 'be terse',
    toolkit: Toolkit.make(safeLookup, send, dated),
  }).withHandlers({
    dated: ({ at }) =>
      Effect.sync(() => {
        ran.count += 1;
        return at instanceof Date ? at.toISOString() : 'not a Date';
      }),
    lookup: ({ id }) =>
      Effect.sync(() => {
        ran.count += 1;
        return `lookup:${id}`;
      }),
    send: ({ id }) =>
      Effect.sync(() => {
        ran.count += 1;
        return `send:${id}`;
      }),
  });

/** Crash after `StepStarted`, resume, and report what happened. */
const recordsAfter = (
  name: 'lookup' | 'send' | 'dated',
  withPolicy: boolean,
  params?: unknown,
) =>
  Effect.gen(function* () {
    const ran = { count: 0 };
    const models = provider([says('done')]);
    return yield* run(
      Effect.gen(function* () {
        yield* seed(ID, [
          started('hi'),
          toolCalled(name, params ?? { id: '42' }, CALL),
          toolStarted(name, CALL),
        ]);
        const base = replayAgent(ran);
        const exit = yield* Conversation.make(
          withPolicy ? base.intercepting(Replay.policy) : base,
          ID,
        )
          .run('continue')
          .pipe(Effect.exit);
        return {
          exit,
          ran,
          asked: models.asked.map((prompt) => JSON.stringify(prompt)),
          records: (yield* readRecords(ID)).map(({ record }) => record),
        };
      }),
      models.layer,
    );
  });

describe('Replay after a crash inside a tool', () => {
  it.effect('reruns a replay-safe tool once', () =>
    Effect.gen(function* () {
      const result = yield* recordsAfter('lookup', true);

      expect(result.ran.count).toBe(1);
      expect(result.exit._tag).toBe('Success');
      expect(
        result.records.filter((record) => record._tag === 'StepSettled'),
      ).toMatchObject([{ id: CALL, outcome: 'success', result: 'lookup:42' }]);
      expect(result.asked[0]).toContain('lookup:42');
      expect(result.records.at(-1)).toMatchObject({
        _tag: 'RunSettled',
        outcome: 'success',
      });
    }),
  );

  it.effect(
    'reruns a transforming parameter schema from its stored value',
    () =>
      Effect.gen(function* () {
        const result = yield* recordsAfter('dated', true, {
          at: '2026-08-26T00:00:00.000Z',
        });

        expect(result.ran.count).toBe(1);
        expect(
          result.records.filter((record) => record._tag === 'StepSettled'),
        ).toMatchObject([
          { id: CALL, outcome: 'success', result: '2026-08-26T00:00:00.000Z' },
        ]);
      }),
  );

  it.effect(
    'answers a non-safe tool with execution-interrupted and continues',
    () =>
      Effect.gen(function* () {
        const result = yield* recordsAfter('send', true);

        expect(result.ran.count).toBe(0);
        expect(result.exit._tag).toBe('Success');
        expect(
          result.records.filter((record) => record._tag === 'StepSettled'),
        ).toMatchObject([
          {
            id: CALL,
            outcome: 'failure',
            result: { type: 'execution-interrupted' },
          },
        ]);
        expect(result.asked[0]).toContain('execution-interrupted');
      }),
  );

  it.effect('without a policy, resume refuses to guess', () =>
    Effect.gen(function* () {
      const result = yield* recordsAfter('lookup', false);

      expect(result.ran.count).toBe(0);
      expect(result.exit._tag).toBe('Failure');
      expect(result.asked).toEqual([]);
      expect(result.records.map((record) => record._tag)).not.toContain(
        'StepSettled',
      );
    }),
  );
});

// The state a reopened conversation reports for each call decides what resume
// does next, so each shape of crashed log gets its own expected state.
describe('the recovery state of a reopened conversation', () => {
  const call = lookupCall();
  const suspension = {
    _tag: 'StepSuspended',
    id: CALL,
    name: 'lookup',
    wait: 'review',
    token: 'workflow-token',
    request: { id: '42' },
  } satisfies ConversationRecord.Record;
  const stateOf = (records: ReadonlyArray<ConversationRecord.Record>) =>
    Effect.gen(function* () {
      yield* seed(ID, records);
      const session = yield* openSession(ID);
      const state = session.recovery('lookup', CALL);
      return Option.isSome(state) ? state.value : undefined;
    }).pipe(Effect.provide(testLogLayer), Effect.scoped);

  it.effect('reports an orphaned call as indeterminate', () =>
    Effect.gen(function* () {
      expect(yield* stateOf([started(), call, lookupStarted()])).toEqual({
        _tag: 'Indeterminate',
        replay: 'unsafe',
      });
    }),
  );

  const resolution = {
    _tag: 'StepResolved',
    id: CALL,
    name: 'lookup',
    wait: 'review',
    token: 'workflow-token',
    outcome: 'success',
    result: { _tag: 'Success', value: { approved: true } },
  } satisfies ConversationRecord.Record;

  it.effect(
    'reports a resumed wait as a replay-safe start that names its wait',
    () =>
      Effect.gen(function* () {
        expect(
          yield* stateOf([
            started(),
            call,
            lookupStarted(),
            suspension,
            resolution,
            {
              _tag: 'StepStarted',
              id: CALL,
              name: 'lookup',
              replay: 'safe',
              resumes: 'workflow-token',
            },
          ]),
        ).toEqual({
          _tag: 'Indeterminate',
          replay: 'safe',
          resumes: 'workflow-token',
        });
      }),
  );

  it.effect(
    'keeps a resolved wait suspended until its handler starts again',
    () =>
      Effect.gen(function* () {
        expect(
          yield* stateOf([
            started(),
            call,
            lookupStarted(),
            suspension,
            resolution,
          ]),
        ).toEqual({
          _tag: 'Suspended',
          wait: 'review',
          token: 'workflow-token',
          request: { id: '42' },
        });
      }),
  );

  it.effect('turns an explicitly restarted wait into a fresh dispatch', () =>
    Effect.gen(function* () {
      expect(
        yield* stateOf([
          started(),
          call,
          lookupStarted(),
          suspension,
          {
            _tag: 'StepRestarted',
            id: CALL,
            name: 'lookup',
            wait: 'review',
            priorToken: 'workflow-token',
          },
        ]),
      ).toEqual({ _tag: 'Restarting' });
    }),
  );

  it.effect('serves a durable outcome recorded after the last settlement', () =>
    Effect.gen(function* () {
      expect(
        yield* stateOf([
          started(),
          call,
          lookupStarted(),
          lookupOutcome('call-1', 'ok'),
        ]),
      ).toMatchObject({
        _tag: 'Settled',
        outcome: 'success',
        result: { status: 'ok' },
      });
    }),
  );

  it.effect('forgets calls of a settled run and of an abandoned branch', () =>
    Effect.gen(function* () {
      const crashed = [started(), call, lookupStarted()];
      expect(
        yield* stateOf([
          ...crashed,
          { ...settled(), outcome: 'interrupted', detail: 'test' },
        ]),
      ).toBeUndefined();
      expect(
        yield* stateOf([
          ...crashed,
          { _tag: 'BranchedFrom', at: LogOffset.fromSeq(0n) },
        ]),
      ).toBeUndefined();
    }),
  );

  it.effect.each([
    [
      'a durable start without its provider call',
      [started(), lookupStarted()],
      'StepStarted has no StepOpened',
    ],
    [
      'a suspension without its durable handler start',
      [started(), call, suspension],
      'StepSuspended has no earlier StepStarted',
    ],
  ] as const)('flags %s as corruption', ([, records, why]) =>
    Effect.gen(function* () {
      const corruption = yield* Effect.gen(function* () {
        yield* seed(ID, records);
        return (yield* openSession(ID)).recoveryCorruption;
      }).pipe(Effect.provide(testLogLayer), Effect.scoped);

      expect(corruption).toContain(why);
    }),
  );
});
