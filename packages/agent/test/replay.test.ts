import { LogStoreMemory } from '@sunfall/vesper-log/layer-memory';
import { LogStore } from '@sunfall/vesper-log/log-store';
import type { ConversationRecord } from '@sunfall/vesper-log/record';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';
import * as NodeServices from '@effect/platform-node/NodeServices';
import { Effect, Layer, Schema, Stream } from 'effect';
import { LanguageModel, type Response, Tool, Toolkit } from 'effect/ai';
import { describe, expect, it } from '@effect/vitest';

import { Agent } from '../src/agent.js';
import { Conversation } from '../src/conversation.js';
import * as AgentLog from '../src/log.js';
import { Replay } from '../src/replay.js';
import { testBudget } from './budget-fixture.js';

const testLogLayer = Layer.mergeAll(
  LogStoreMemory.layer.pipe(Layer.provide(NodeServices.layer)),
  NodeServices.layer,
);

const CALL_ID = LogVocabulary.ToolCallId.make('call-1');
const CONVERSATION = LogVocabulary.ConversationId.make('replay-conversation');
const PATH = AgentLog.pathFor(CONVERSATION);

const finish = {
  type: 'finish' as const,
  reason: 'stop' as const,
  usage: {
    inputTokens: { total: 10, uncached: 10, cacheRead: 0, cacheWrite: 0 },
    outputTokens: { total: 4 },
  },
};

/** A model that answers with text and records each prompt it was handed. */
const scripted = (prompts: string[]) =>
  Layer.effect(
    LanguageModel.LanguageModel,
    LanguageModel.make({
      generateText: () => Effect.succeed<Response.PartEncoded[]>([finish]),
      streamText: (options) => {
        prompts.push(JSON.stringify(options.prompt));
        return Stream.fromIterable<Response.StreamPartEncoded>([
          { type: 'text-start', id: 'b' },
          { type: 'text-delta', id: 'b', delta: 'done' },
          { type: 'text-end', id: 'b' },
          finish,
        ]);
      },
    }),
  );

const parameters = Schema.Struct({ id: Schema.String });
const lookup = Replay.safe(
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

const agentWith = (ran: { count: number }) =>
  Agent.make({
    name: 'test',
    revision: '1',
    instructions: 'be terse',
    toolkit: Toolkit.make(lookup, send, dated),
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

/** A run that died after `ToolStarted`, before any `ToolOutcome`. */
type Name = 'lookup' | 'send' | 'dated';

const crashedAfterStart = (name: Name, params: unknown = { id: '42' }) =>
  Effect.gen(function* () {
    const records: ReadonlyArray<ConversationRecord.Record> = [
      {
        _tag: 'RunStarted',
        budget: testBudget,
        agent: 'test',
        formatVersion: 1,
        agentRevision: LogVocabulary.AgentRevision.make('1'),
        prompt: [],
      },
      { _tag: 'ToolCall', step: 1, id: CALL_ID, name, params },
      { _tag: 'ToolStarted', id: CALL_ID, name },
    ];
    const store = yield* LogStore.Service;
    yield* store.create(PATH, CONVERSATION).pipe(Effect.orDie);
    const claim = yield* store
      .acquire(PATH, LogVocabulary.ProducerId.make('previous-run'))
      .pipe(Effect.orDie);
    yield* store
      .append({
        path: PATH,
        producerId: claim.producerId,
        epoch: claim.epoch,
        sequence: claim.nextSequence,
        records: records.map((record) => ({
          conversationId: CONVERSATION,
          timestamp: 1_700_000_000_000,
          record,
        })),
      })
      .pipe(Effect.orDie);
  });

const resume = (name: Name, policy: boolean, params?: unknown) =>
  Effect.gen(function* () {
    const ran = { count: 0 };
    const prompts: string[] = [];
    const settled = yield* Effect.gen(function* () {
      yield* crashedAfterStart(name, params);
      const base = agentWith(ran);
      const exit = yield* Conversation.make(
        policy ? base.intercepting(Replay.policy) : base,
        CONVERSATION,
      )
        .run('hi')
        .pipe(Effect.exit);
      const store = yield* LogStore.Service;
      const page = yield* store.read(PATH, { limit: 1000 }).pipe(Effect.orDie);
      return {
        exit,
        records: page.records.map((envelope) => envelope.record),
      };
    }).pipe(Effect.provide(Layer.merge(scripted(prompts), testLogLayer)));
    return { ...settled, ran, prompts };
  }).pipe(Effect.scoped);

describe('Replay', () => {
  it.effect('reruns a replay-safe tool once after a crash', () =>
    Effect.gen(function* () {
      const result = yield* resume('lookup', true);

      expect(result.ran.count).toBe(1);
      expect(result.exit._tag).toBe('Success');
      expect(
        result.records.filter((record) => record._tag === 'ToolOutcome'),
      ).toMatchObject([
        { id: CALL_ID, outcome: 'success', result: 'lookup:42' },
      ]);
      expect(result.prompts[0]).toContain('lookup:42');
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
        const result = yield* resume('dated', true, {
          at: '2026-08-26T00:00:00.000Z',
        });

        expect(result.ran.count).toBe(1);
        expect(
          result.records.filter((record) => record._tag === 'ToolOutcome'),
        ).toMatchObject([
          {
            id: CALL_ID,
            outcome: 'success',
            result: '2026-08-26T00:00:00.000Z',
          },
        ]);
      }),
  );

  it.effect(
    'answers a non-safe tool with execution-interrupted and continues',
    () =>
      Effect.gen(function* () {
        const result = yield* resume('send', true);

        expect(result.ran.count).toBe(0);
        expect(result.exit._tag).toBe('Success');
        expect(
          result.records.filter((record) => record._tag === 'ToolOutcome'),
        ).toMatchObject([
          {
            id: CALL_ID,
            outcome: 'failure',
            result: { type: 'execution-interrupted' },
          },
        ]);
        expect(result.prompts[0]).toContain('execution-interrupted');
        expect(result.records.at(-1)).toMatchObject({
          _tag: 'RunSettled',
          outcome: 'success',
        });
      }),
  );

  it.effect('without a policy, resume still fails', () =>
    Effect.gen(function* () {
      const result = yield* resume('lookup', false);

      expect(result.ran.count).toBe(0);
      expect(result.exit._tag).toBe('Failure');
      expect(result.prompts).toEqual([]);
      expect(result.records.map((record) => record._tag)).not.toContain(
        'ToolOutcome',
      );
    }),
  );

  it('reads the annotation', () => {
    expect(Replay.isSafe(lookup)).toBe(true);
    expect(Replay.isSafe(send)).toBe(false);
    expect(Replay.isSafe(undefined)).toBe(false);
  });
});
