import { LogStore } from '@sunfall/vesper-log/log-store';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';
import { describe, expect, it } from '@effect/vitest';
import { Deferred, Effect, Fiber, Ref, Schema, Stream } from 'effect';
import { type Response, Tool, Toolkit } from 'effect/ai';

import { Agent } from '../src/agent.js';
import { Conversation } from '../src/conversation.js';
import * as AgentLog from '../src/log.js';
import {
  callsLookup,
  finish,
  lookup,
  lookupAgent,
  openSession,
  provider,
  readRecords,
  run,
  says,
  tags,
} from './durability-fixtures.js';
import { digestOf } from './tool-fixtures.js';

// Recording: what a run leaves in the durable log, and what a consumer can
// rely on about it. Every scenario drives `Conversation` against a scripted
// model and a real in-memory store.

const ID = 'recorded';
const turns = () => [callsLookup('he', 'l', 'lo'), says('do', 'ne')];

describe('recording a run', () => {
  it.effect(
    'writes one record per event, coalesced, as a verifiable chain',
    () =>
      Effect.gen(function* () {
        const { agent } = lookupAgent();
        const conversation = Conversation.make(agent, ID);
        const observed = yield* run(
          Effect.gen(function* () {
            const result = yield* conversation.run('where is order 42?');
            return {
              result,
              written: yield* readRecords(ID),
              replayed: Array.from(
                yield* conversation.records().pipe(Stream.runCollect),
              ),
              verdict: yield* AgentLog.verify(
                LogVocabulary.ConversationId.make(ID),
              ),
            };
          }),
          provider(turns()).layer,
        );

        expect(tags(observed.written)).toEqual([
          'RunStarted',
          'Text',
          'StepOpened',
          'StepStarted',
          'StepSettled',
          'TurnFinished',
          'Text',
          'TurnFinished',
          'Completed',
          'RunSettled',
        ]);
        // Three deltas arrived and one row was written: the row count is a
        // function of the conversation, not of the provider's chunking.
        const records = observed.written.map(({ record }) => record);
        expect(records.filter((record) => record._tag === 'Text')).toEqual([
          { _tag: 'Text', step: 1, text: 'hello' },
          { _tag: 'Text', step: 2, text: 'done' },
        ]);
        expect(records).toContainEqual({
          _tag: 'StepOpened',
          step: 1,
          id: 'call-1',
          name: 'lookup',
          inputDigest: digestOf('lookup', { id: '42' }),
          params: { id: '42' },
        });
        expect(records).toContainEqual({
          _tag: 'StepSettled',
          step: 1,
          id: 'call-1',
          name: 'lookup',
          outcome: 'success',
          result: { status: 'shipped:42' },
        });
        expect(records[0]).toMatchObject({
          _tag: 'RunStarted',
          budget: { root: ID },
          agent: 'test',
          formatVersion: 1,
          agentRevision: '1',
        });
        // The record the caller's result was built from is the one persisted.
        expect(records).toContainEqual(
          expect.objectContaining({
            _tag: 'Completed',
            text: observed.result.text,
            steps: observed.result.steps,
            usage: observed.result.usage,
          }),
        );
        expect(observed.verdict).toMatchObject({ _tag: 'Valid' });
        expect(tags(observed.replayed)).toEqual(tags(observed.written));
      }),
  );

  it.effect('serializes concurrent appends through one producer sequence', () =>
    Effect.gen(function* () {
      const written = yield* run(
        Effect.gen(function* () {
          const session = yield* openSession(ID);
          yield* Effect.all(
            Array.from({ length: 50 }, (_, index) =>
              session.append([{ _tag: 'Text', step: 1, text: String(index) }]),
            ),
            { concurrency: 'unbounded' },
          );
          return yield* readRecords(ID);
        }),
        provider([says('unused')]).layer,
      );

      expect(written).toHaveLength(50);
      expect(
        new Set(
          written.flatMap(({ record }) =>
            record._tag === 'Text' ? [record.text] : [],
          ),
        ).size,
      ).toBe(50);
    }),
  );

  it.effect(
    'streams preliminary tool progress without recording it as settled',
    () =>
      Effect.gen(function* () {
        const progressAgent = Agent.make({
          name: 'progress-test',
          revision: '1',
          instructions: 'be terse',
          toolkit: Toolkit.make(lookup),
        }).withHandlers({
          lookup: ({ id }, context) =>
            context
              .preliminary({ status: `checking:${id}` })
              .pipe(Effect.as({ status: `shipped:${id}` })),
        });
        const observed = yield* run(
          Effect.gen(function* () {
            const events = yield* Conversation.make(progressAgent, ID)
              .stream('hi')
              .pipe(Stream.runCollect);
            return {
              events: Array.from(events),
              written: yield* readRecords(ID),
            };
          }),
          provider(turns()).layer,
        );

        const live = observed.events.flatMap((event) =>
          event._tag === 'Part' && event.part.type === 'tool-result'
            ? [event.part.preliminary]
            : [],
        );
        expect(live).toEqual([true, false]);
        expect(
          observed.written.filter(
            ({ record }) => record._tag === 'StepSettled',
          ),
        ).toHaveLength(1);
      }),
  );

  it.effect('persists transformed tool parameters in encoded form', () =>
    Effect.gen(function* () {
      const dated = Tool.make('dated', {
        description: 'look an order up at a specific time',
        parameters: Schema.Struct({ at: Schema.DateFromString }),
        success: Schema.Struct({ status: Schema.String }),
      });
      const datedAgent = Agent.make({
        name: 'test',
        revision: '1',
        instructions: 'be terse',
        toolkit: Toolkit.make(dated),
      }).withHandlers({
        dated: ({ at }) => Effect.succeed({ status: `at:${at.toISOString()}` }),
      });
      const call: Response.StreamPartEncoded[] = [
        {
          type: 'tool-call',
          id: 'call-1',
          name: 'dated',
          params: { at: '2026-01-02T03:04:05.000Z' },
        },
        finish('tool-calls'),
      ];
      const written = yield* run(
        Effect.gen(function* () {
          yield* Conversation.make(datedAgent, ID).run('hi');
          return yield* readRecords(ID);
        }),
        provider([call, says('done')]).layer,
      );

      expect(written.map(({ record }) => record)).toContainEqual(
        expect.objectContaining({
          _tag: 'StepOpened',
          params: { at: '2026-01-02T03:04:05.000Z' },
        }),
      );
      // `StepOpened` is written before the handler runs, so it cannot show
      // whether the parameters reached the handler decoded exactly once. A
      // transforming schema decoded twice fails validation and the tool
      // answers with an error instead of executing.
      expect(written.map(({ record }) => record)).toContainEqual(
        expect.objectContaining({
          _tag: 'StepSettled',
          outcome: 'success',
          result: { status: 'at:2026-01-02T03:04:05.000Z' },
        }),
      );
    }),
  );

  it.effect(
    'restores the same canonical response from durable completion',
    () =>
      Effect.gen(function* () {
        const { agent } = lookupAgent();
        const conversation = Conversation.make(agent, ID);
        const models = provider(turns());
        const observed = yield* run(
          Effect.gen(function* () {
            const first = yield* conversation.run('where is order 42?');
            const restored = yield* conversation.run('this is not sent');
            return { first, restored, asked: models.asked.length };
          }),
          models.layer,
        );

        expect(observed.first.response?.content).toHaveLength(1);
        expect(observed.restored.response).toEqual(observed.first.response);
      }),
  );

  // A consumer must never act on something the durable record does not yet
  // contain: forking the append, or moving it after the emit, breaks this.
  it.effect(
    'has already written the record when the consumer sees the event',
    () =>
      Effect.gen(function* () {
        const { agent } = lookupAgent();
        const seen = yield* run(
          Effect.gen(function* () {
            const store = yield* LogStore.Service;
            const path = AgentLog.pathFor(
              LogVocabulary.ConversationId.make(ID),
            );
            const atToolCall: string[] = [];
            yield* Conversation.make(agent, ID)
              .stream('hi')
              .pipe(
                Stream.runForEach((event) =>
                  event._tag === 'Part' && event.part.type === 'tool-call'
                    ? Effect.gen(function* () {
                        const page = yield* store.read(path, { limit: 1000 });
                        atToolCall.push(...tags(page.records));
                      })
                    : Effect.void,
                ),
              );
            return atToolCall;
          }),
          provider(turns()).layer,
        );

        // The call is durable before its handler starts.
        expect(seen).toEqual(['RunStarted', 'Text', 'StepOpened']);
      }),
  );
});

describe('logging is optional', () => {
  it.effect(
    'runs an unrecorded agent without a LogStore and writes nothing',
    () =>
      Effect.gen(function* () {
        const { agent } = lookupAgent();
        // No store layer anywhere in this pipeline: `run` must not require one.
        const result = yield* agent
          .run('hi')
          .pipe(Effect.orDie, Effect.provide(provider(turns()).layer));
        expect(result.text).toBe('done');

        // A reachable LogStore is not consent to write to it.
        const meta = yield* run(
          Effect.gen(function* () {
            yield* agent.run('hi');
            const store = yield* LogStore.Service;
            return yield* store.meta(
              AgentLog.pathFor(LogVocabulary.ConversationId.make(ID)),
            );
          }),
          provider(turns()).layer,
        );
        expect(meta._tag).toBe('None');
      }),
  );

  it('puts LogStore in the type for a conversation only', () => {
    type EffR<T> =
      T extends Effect.Effect<unknown, unknown, infer R> ? R : never;
    const { agent } = lookupAgent();
    type Plain = Agent.Requires<typeof agent>;
    type Recording = EffR<
      ReturnType<ReturnType<typeof Conversation.make<typeof agent>>['run']>
    >;
    // Both fail to compile if binding a conversation stops changing the
    // requirement channel.
    const plainIsFree: LogStore.Service extends Plain ? false : true = true;
    const recordingNeedsIt: LogStore.Service extends Recording ? true : false =
      true;
    expect([plainIsFree, recordingNeedsIt]).toEqual([true, true]);
  });
});

describe('Conversation.records', () => {
  it.effect('resumes after an offset rather than from the beginning', () =>
    Effect.gen(function* () {
      const { agent } = lookupAgent();
      const conversation = Conversation.make(agent, ID);
      const observed = yield* run(
        Effect.gen(function* () {
          yield* conversation.run('hi');
          const written = yield* readRecords(ID);
          const after = yield* conversation
            .records(written[3]?.offset)
            .pipe(Stream.runCollect);
          return { written, after: Array.from(after) };
        }),
        provider(turns()).layer,
      );

      expect(tags(observed.after)).toEqual(tags(observed.written.slice(4)));
    }),
  );

  // The half that makes it a tail and not a query: the reader is held open
  // until it has drained every historical record, and only then is a new one
  // appended, so that record cannot reach it through catch-up.
  it.effect('follows records appended after the reader caught up', () =>
    Effect.gen(function* () {
      const { agent } = lookupAgent();
      const conversation = Conversation.make(agent, ID);
      const late = yield* run(
        Effect.gen(function* () {
          yield* conversation.run('hi');
          const historical = (yield* readRecords(ID)).length;
          const caughtUp = yield* Deferred.make<void>();
          const seen = yield* Ref.make(0);

          const reader = yield* conversation.follow().pipe(
            Stream.tap(() =>
              Ref.updateAndGet(seen, (n) => n + 1).pipe(
                Effect.flatMap((count) =>
                  count === historical
                    ? Deferred.succeed(caughtUp, undefined)
                    : Effect.void,
                ),
              ),
            ),
            Stream.take(historical + 1),
            Stream.runCollect,
            Effect.forkChild,
          );
          yield* Deferred.await(caughtUp);

          const session = yield* openSession(ID);
          yield* session.append([
            { _tag: 'Text', step: 3, text: 'appended later' },
          ]);
          return yield* Fiber.join(reader);
        }),
        provider(turns()).layer,
      );

      expect(Array.from(late).at(-1)?.record).toMatchObject({
        text: 'appended later',
      });
    }),
  );
});
