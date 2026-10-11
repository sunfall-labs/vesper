import { createHash } from 'node:crypto';
import { LogStoreMemory } from '@sunfall/vesper-log/layer-memory';
import { LogStore } from '@sunfall/vesper-log/log-store';
import type { ConversationRecord } from '@sunfall/vesper-log/record';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';
import * as NodeServices from '@effect/platform-node/NodeServices';
import { describe, expect, it } from '@effect/vitest';
import { Effect, Layer, Schema, Stream } from 'effect';
import { type Response, Tool, Toolkit } from 'effect/ai';

import { Agent } from '../src/agent.js';
import { CodeExecutor } from '../src/code-executor.js';
import { CodeMode } from '../src/code-mode.js';
import { Conversation } from '../src/conversation.js';
import { Interception } from '../src/interception.js';
import * as AgentLog from '../src/log.js';
import { ScriptedModel } from '../src/testing.js';
import { testBudget } from './budget-fixture.js';

// Nested calls made by a code-mode script are recorded durably, keyed by the
// parent `exec` call and a deterministic key, and served from the log when the
// same `exec` call is entered again after a crash.

const logLayer = Layer.mergeAll(
  LogStoreMemory.layer.pipe(Layer.provide(NodeServices.layer)),
  NodeServices.layer,
);

const finish = (reason: 'stop' | 'tool-calls' = 'stop') => ({
  type: 'finish' as const,
  reason,
  usage: {
    inputTokens: { total: 1, uncached: 1, cacheRead: 0, cacheWrite: 0 },
    outputTokens: { total: 1 },
  },
});

const answeringTurn: ReadonlyArray<Response.StreamPartEncoded> = [
  { type: 'text-start', id: 'answer' },
  { type: 'text-delta', id: 'answer', delta: 'done' },
  { type: 'text-end', id: 'answer' },
  finish(),
];

const execTurn: ReadonlyArray<Response.StreamPartEncoded> = [
  {
    type: 'tool-call',
    id: 'exec-1',
    name: 'exec',
    params: { source: 'compose' },
  },
  finish('tool-calls'),
];

const lookup = Tool.make('lookup', {
  description: 'Look up an order.',
  parameters: Schema.Struct({ id: Schema.String }),
  success: Schema.Struct({ status: Schema.String }),
});
const charge = Tool.make('charge', {
  description: 'Charge an order.',
  parameters: Schema.Struct({ amount: Schema.Finite }),
  success: Schema.Struct({ receipt: Schema.String }),
});

interface Counts {
  lookup: number;
  charge: number;
  intercepted: number;
}

const agentWith = (counts: Counts) =>
  Agent.make({
    name: 'nested',
    revision: '1',
    instructions: 'Use code mode.',
    toolkit: Toolkit.make(lookup, charge),
    codeMode: true,
  }).withHandlers({
    lookup: ({ id }) =>
      Effect.sync(() => {
        counts.lookup += 1;
        return { status: `fresh:${id}` };
      }),
    charge: ({ amount }) =>
      Effect.sync(() => {
        counts.charge += 1;
        return { receipt: `fresh:${String(amount)}` };
      }),
  });

const newCounts = (): Counts => ({ lookup: 0, charge: 0, intercepted: 0 });

const call = (
  id: string,
  name: string,
  input: CodeExecutor.JsonValue,
  key?: string,
): CodeExecutor.Event => ({
  _tag: 'ToolCall',
  id,
  name,
  input,
  ...(key === undefined ? {} : { key }),
});

const script = (...events: ReadonlyArray<CodeExecutor.Event>) =>
  CodeExecutor.fake([
    ...events,
    { _tag: 'Output', value: 'ok' },
    { _tag: 'Completion', state: {} },
  ]);

const CONVERSATION = LogVocabulary.ConversationId.make('nested-conversation');
const PATH = AgentLog.pathFor(CONVERSATION);
const EXEC = LogVocabulary.ToolCallId.make('exec-1');

/** Write an earlier, crashed run's records straight into the conversation. */
const seed = Effect.fn('test.seed')(function* (
  records: ReadonlyArray<ConversationRecord.Record>,
) {
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

const crashedExec: ReadonlyArray<ConversationRecord.Record> = [
  {
    _tag: 'RunStarted',
    budget: testBudget,
    agent: 'nested',
    formatVersion: 1,
    agentRevision: LogVocabulary.AgentRevision.make('1'),
    prompt: [],
  },
  {
    _tag: 'ToolCall',
    step: 1,
    id: EXEC,
    name: 'exec',
    params: { source: 'compose' },
  },
  { _tag: 'ToolStarted', id: EXEC, name: 'exec' },
];

const nestedCall = (
  key: string,
  name: string,
  input: CodeExecutor.JsonValue,
): ConversationRecord.Record => ({
  _tag: 'NestedToolCall',
  parent: EXEC,
  key,
  name,
  inputDigest: createHash('sha256')
    .update(CodeMode.inputMaterial(name, input))
    .digest('hex'),
  params: input,
});
const nestedStarted = (
  key: string,
  name: string,
): ConversationRecord.Record => ({
  _tag: 'NestedToolStarted',
  parent: EXEC,
  key,
  name,
});
const nestedSettled = (
  key: string,
  name: string,
  result: unknown,
): ConversationRecord.Record => ({
  _tag: 'NestedToolOutcome',
  parent: EXEC,
  key,
  name,
  outcome: 'success',
  result,
});

const retry = {
  onIndeterminateToolCall: () => Effect.succeed(Interception.retry),
};

const records = <E, R>(conversation: {
  readonly records: () => Stream.Stream<ConversationRecord.Envelope, E, R>;
}) =>
  conversation.records().pipe(
    Stream.runCollect,
    Effect.map((all) => Array.from(all).map(({ record }) => record)),
  );

describe('code mode nested tool calls', () => {
  it.effect('records nested calls durably with parent and key', () =>
    Effect.gen(function* () {
      const counts = newCounts();
      const executor = script(
        call('n1', 'lookup', { id: '42' }),
        call('n2', 'charge', { amount: 5 }, 'charge-once'),
      );
      const model = ScriptedModel.make([execTurn, answeringTurn]);
      const conversation = Conversation.make(agentWith(counts), CONVERSATION);

      yield* conversation
        .run('go')
        .pipe(Effect.provide(Layer.merge(model.layer, executor.layer)));
      const written = yield* records(conversation);

      const nested = written.filter((record) =>
        record._tag.startsWith('NestedTool'),
      );
      const count = (tag: string) =>
        nested.filter((record) => record._tag === tag).length;
      expect([
        count('NestedToolCall'),
        count('NestedToolStarted'),
        count('NestedToolOutcome'),
      ]).toEqual([2, 2, 2]);
      expect(
        nested.every((record) => 'parent' in record && record.parent === EXEC),
      ).toBe(true);
      expect(
        nested.flatMap((record) =>
          record._tag === 'NestedToolOutcome'
            ? [[record.key, record.name, record.result]]
            : [],
        ),
      ).toEqual(
        expect.arrayContaining([
          ['#1', 'lookup', { status: 'fresh:42' }],
          ['k:charge-once', 'charge', { receipt: 'fresh:5' }],
        ]),
      );
      expect(counts).toEqual({ lookup: 1, charge: 1, intercepted: 0 });
    }).pipe(Effect.provide(logLayer), Effect.scoped),
  );

  it.effect('leaves the model-facing prompt exactly as without a session', () =>
    Effect.gen(function* () {
      const events = [
        call('n1', 'lookup', { id: '42' }),
        call('n2', 'charge', { amount: 5 }),
      ] as const;
      const plain = ScriptedModel.make([execTurn, answeringTurn]);
      const durable = ScriptedModel.make([execTurn, answeringTurn]);

      yield* agentWith(newCounts())
        .run('go')
        .pipe(
          Effect.provide(Layer.merge(plain.layer, script(...events).layer)),
        );
      yield* Conversation.make(agentWith(newCounts()), CONVERSATION)
        .run('go')
        .pipe(
          Effect.provide(Layer.merge(durable.layer, script(...events).layer)),
        );

      const plainPrompts = (yield* plain.requests).map((request) =>
        JSON.stringify(request.prompt),
      );
      const durablePrompts = (yield* durable.requests).map((request) =>
        JSON.stringify(request.prompt),
      );
      expect(durablePrompts).toEqual(plainPrompts);
      expect(durablePrompts[1]).not.toContain('NestedTool');
    }).pipe(Effect.provide(logLayer), Effect.scoped),
  );

  it.effect(
    'serves settled nested calls from the log and fails an unsettled one closed on Retry',
    () =>
      Effect.gen(function* () {
        const counts = newCounts();
        yield* seed([
          ...crashedExec,
          nestedCall('#1', 'lookup', { id: '42' }),
          nestedStarted('#1', 'lookup'),
          nestedSettled('#1', 'lookup', { status: 'from-log' }),
          nestedCall('#2', 'charge', { amount: 5 }),
          nestedStarted('#2', 'charge'),
        ]);
        const executor = script(
          call('n1', 'lookup', { id: '42' }),
          call('n2', 'charge', { amount: 5 }),
          call('n3', 'lookup', { id: '43' }),
        );
        const model = ScriptedModel.make([answeringTurn]);
        const conversation = Conversation.make(
          agentWith(counts).intercepting({
            ...retry,
            beforeToolCall: () => {
              counts.intercepted += 1;
              return Effect.succeed(Interception.dispatch);
            },
          }),
          CONVERSATION,
        );

        const result = yield* conversation
          .run('go')
          .pipe(Effect.provide(Layer.merge(model.layer, executor.layer)));
        const responses = yield* executor.responses;
        const written = yield* records(conversation);

        expect(result.outcome).toBe('success');
        // Only the call the earlier run never reached did real work, and it
        // was intercepted exactly once: served and refused calls bypass both.
        expect(counts).toEqual({ lookup: 1, charge: 0, intercepted: 1 });
        expect(responses.find(({ id }) => id === 'n1')).toEqual({
          id: 'n1',
          outcome: 'success',
          value: { status: 'from-log' },
        });
        expect(responses.find(({ id }) => id === 'n2')).toMatchObject({
          outcome: 'failure',
          error: { code: 'indeterminate' },
        });
        expect(responses.find(({ id }) => id === 'n3')).toEqual({
          id: 'n3',
          outcome: 'success',
          value: { status: 'fresh:43' },
        });
        expect(
          written.filter(
            (record) =>
              record._tag === 'NestedToolOutcome' && record.key === '#3',
          ),
        ).toHaveLength(1);
        expect(
          written.filter((record) => record._tag === 'NestedToolOutcome'),
        ).toHaveLength(2);
        expect(
          written.some(
            (record) => record._tag === 'ToolOutcome' && record.id === EXEC,
          ),
        ).toBe(true);
      }).pipe(Effect.provide(logLayer), Effect.scoped),
  );

  it.effect('finds an explicitly keyed call whatever its position', () =>
    Effect.gen(function* () {
      const counts = newCounts();
      yield* seed([
        ...crashedExec,
        nestedCall('k:charge-once', 'charge', { amount: 5 }),
        nestedStarted('k:charge-once', 'charge'),
        nestedSettled('k:charge-once', 'charge', { receipt: 'from-log' }),
      ]);
      const executor = script(
        call('n1', 'lookup', { id: '42' }),
        call('n2', 'charge', { amount: 5 }, 'charge-once'),
      );
      const model = ScriptedModel.make([answeringTurn]);

      yield* Conversation.make(
        agentWith(counts).intercepting(retry),
        CONVERSATION,
      )
        .run('go')
        .pipe(Effect.provide(Layer.merge(model.layer, executor.layer)));
      const responses = yield* executor.responses;

      expect(counts).toEqual({ lookup: 1, charge: 0, intercepted: 0 });
      expect(responses.find(({ id }) => id === 'n2')).toEqual({
        id: 'n2',
        outcome: 'success',
        value: { receipt: 'from-log' },
      });
    }).pipe(Effect.provide(logLayer), Effect.scoped),
  );

  it.effect(
    'refuses to serve a recorded call that a drifted script remakes',
    () =>
      Effect.gen(function* () {
        const counts = newCounts();
        yield* seed([
          ...crashedExec,
          nestedCall('#1', 'lookup', { id: '42' }),
          nestedStarted('#1', 'lookup'),
          nestedSettled('#1', 'lookup', { status: 'from-log' }),
        ]);
        const executor = script(call('n1', 'lookup', { id: 'other' }));
        const model = ScriptedModel.make([answeringTurn]);

        yield* Conversation.make(
          agentWith(counts).intercepting(retry),
          CONVERSATION,
        )
          .run('go')
          .pipe(Effect.provide(Layer.merge(model.layer, executor.layer)));
        const responses = yield* executor.responses;

        expect(counts.lookup).toBe(0);
        expect(responses[0]).toMatchObject({
          outcome: 'failure',
          error: { code: 'dispatch_failed' },
        });
      }).pipe(Effect.provide(logLayer), Effect.scoped),
  );

  it.effect('runs a recorded-but-never-started call on Retry', () =>
    Effect.gen(function* () {
      const counts = newCounts();
      yield* seed([...crashedExec, nestedCall('#1', 'lookup', { id: '42' })]);
      const executor = script(call('n1', 'lookup', { id: '42' }));
      const model = ScriptedModel.make([answeringTurn]);

      yield* Conversation.make(
        agentWith(counts).intercepting(retry),
        CONVERSATION,
      )
        .run('go')
        .pipe(Effect.provide(Layer.merge(model.layer, executor.layer)));

      expect(counts.lookup).toBe(1);
    }).pipe(Effect.provide(logLayer), Effect.scoped),
  );

  it.effect('rejects a key reused within one execution', () =>
    Effect.gen(function* () {
      const counts = newCounts();
      const executor = script(
        call('n1', 'lookup', { id: '1' }, 'same'),
        call('n2', 'lookup', { id: '2' }, 'same'),
      );
      const model = ScriptedModel.make([execTurn, answeringTurn]);

      yield* Conversation.make(agentWith(counts), CONVERSATION)
        .run('go')
        .pipe(Effect.provide(Layer.merge(model.layer, executor.layer)));
      const responses = yield* executor.responses;

      expect(counts.lookup).toBe(1);
      expect(
        responses.filter(({ outcome }) => outcome === 'failure'),
      ).toHaveLength(1);
    }).pipe(Effect.provide(logLayer), Effect.scoped),
  );

  it.effect('runs nested calls as before when the run is not recorded', () =>
    Effect.gen(function* () {
      const counts = newCounts();
      const executor = script(
        call('n1', 'lookup', { id: '42' }, 'explicit'),
        call('n2', 'charge', { amount: 5 }),
      );
      const model = ScriptedModel.make([execTurn, answeringTurn]);

      const result = yield* agentWith(counts)
        .run('go')
        .pipe(Effect.provide(Layer.merge(model.layer, executor.layer)));

      expect(result.text).toBe('done');
      expect(counts).toEqual({ lookup: 1, charge: 1, intercepted: 0 });
      expect(yield* executor.responses).toHaveLength(2);
    }),
  );
});
