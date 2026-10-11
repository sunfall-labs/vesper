import { createHash } from 'node:crypto';

import * as NodeServices from '@effect/platform-node/NodeServices';
import { LogStoreMemory } from '@sunfall/vesper-log/layer-memory';
import { LogStore } from '@sunfall/vesper-log/log-store';
import type { ConversationRecord } from '@sunfall/vesper-log/record';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';
import { type Crypto, DateTime, Effect, Layer, Stream } from 'effect';
import {
  IdGenerator,
  LanguageModel,
  type Prompt,
  type Response,
} from 'effect/ai';

import * as InputDigest from '../src/internal/input-digest.js';
import * as AgentLog from '../src/log.js';
import type { ScriptedModel } from '../src/testing.js';
import { testBudget } from './budget-fixture.js';

/** Shared scenario helpers for the tool, approval and interception suites. */

type Part = Response.StreamPartEncoded;

export const finish = (reason: 'stop' | 'tool-calls' = 'stop'): Part => ({
  type: 'finish',
  reason,
  usage: {
    inputTokens: { total: 10, uncached: 10, cacheRead: 0, cacheWrite: 0 },
    outputTokens: { total: 4 },
  },
});

/** A provider turn that answers with text and stops. */
export const textTurn = (text: string, id = 'answer'): ReadonlyArray<Part> => [
  { type: 'text-start', id },
  { type: 'text-delta', id, delta: text },
  { type: 'text-end', id },
  finish(),
];

export interface Call {
  readonly id: string;
  readonly name: string;
  readonly params: unknown;
}

/** A provider turn that asks for one or more tool calls. */
export const callTurn = (
  ...calls: ReadonlyArray<Call>
): ReadonlyArray<Part> => [
  ...calls.map(
    ({ id, name, params }): Part => ({ type: 'tool-call', id, name, params }),
  ),
  finish('tool-calls'),
];

/** A model whose turn is computed from the request, for scripts that must read earlier results. */
export const modelBy = (
  turn: (
    options: LanguageModel.ProviderOptions,
    index: number,
  ) => ReadonlyArray<Part>,
): Layer.Layer<LanguageModel.LanguageModel> =>
  Layer.effect(
    LanguageModel.LanguageModel,
    Effect.suspend(() => {
      let index = 0;
      return LanguageModel.make({
        generateText: () => Effect.succeed([]),
        streamText: (options) =>
          Stream.suspend(() => Stream.fromIterable(turn(options, index++))),
      });
    }),
  );

/** Fails the test the moment the provider is asked anything. */
export const unreachableModel = Layer.effect(
  LanguageModel.LanguageModel,
  LanguageModel.make({
    generateText: () => Effect.die('the model must not be called'),
    streamText: () => Stream.die('the model must not be called'),
  }),
);

const services = Layer.mergeAll(
  LogStoreMemory.layer.pipe(Layer.provide(NodeServices.layer)),
  NodeServices.layer,
  Layer.succeed(IdGenerator.IdGenerator, IdGenerator.defaultIdGenerator),
);

/** Run a scenario against a model, a real in-memory log store and node services. */
export const provide = <A, E>(
  effect: Effect.Effect<
    A,
    E,
    LogStore.Service | LanguageModel.LanguageModel | Crypto.Crypto
  >,
  model: Layer.Layer<LanguageModel.LanguageModel> | ScriptedModel.Handle,
): Effect.Effect<A, E> =>
  Effect.gen(function* () {
    const context = yield* Layer.build(
      Layer.merge('layer' in model ? model.layer : model, services),
    );
    return yield* effect.pipe(Effect.provideContext(context));
  }).pipe(Effect.scoped);

/** Every tool-result part the model was shown in a prompt. */
export const toolResultsIn = (prompt: Prompt.Prompt | undefined) =>
  (prompt?.content ?? []).flatMap((message) =>
    message.role === 'tool'
      ? message.content.filter((part) => part.type === 'tool-result')
      : [],
  );

/** The prompt of the nth provider request, as searchable text. */
export const promptText = (
  requests: ReadonlyArray<ScriptedModel.Request>,
  index: number,
): string => JSON.stringify(requests[index]?.prompt ?? null);

// ---------------------------------------------------------------- durable log

export const CALL_ID = LogVocabulary.ToolCallId.make('call-1');
export const CALL_2 = LogVocabulary.ToolCallId.make('call-2');
/** A date, in both the encoded and decoded forms a tool schema moves between. */
export const date = (iso: string): Date =>
  DateTime.toDateUtc(DateTime.makeUnsafe(iso));
export const RECORDED_ISO = '2020-05-06T07:08:09.000Z';
export const RECORDED = date(RECORDED_ISO);

/**
 * Write a previous run's records straight into a conversation.
 *
 * Deliberately not "run the agent and kill it": a crash must leave the log in
 * an exact state for a scenario to test recovery rather than whatever a killed
 * fiber happens to flush.
 */
export const seed = Effect.fn('test.seed')(function* (
  conversationId: string,
  records: ReadonlyArray<ConversationRecord.Record>,
) {
  const id = LogVocabulary.ConversationId.make(conversationId);
  const path = AgentLog.pathFor(id);
  const store = yield* LogStore.Service;
  yield* store.create(path, id).pipe(Effect.orDie);
  const claim = yield* store
    .acquire(path, LogVocabulary.ProducerId.make('previous-run'))
    .pipe(Effect.orDie);
  yield* store
    .append({
      path,
      producerId: claim.producerId,
      epoch: claim.epoch,
      sequence: claim.nextSequence,
      records: records.map((record) => ({
        conversationId: id,
        timestamp: 1_700_000_000_000,
        record,
      })),
    })
    .pipe(Effect.orDie);
});

/** The records currently stored for a conversation. */
export const readRecords = Effect.fn('test.readRecords')(function* (
  conversationId: string,
) {
  const store = yield* LogStore.Service;
  const path = AgentLog.pathFor(
    LogVocabulary.ConversationId.make(conversationId),
  );
  const page = yield* store.read(path, { limit: 1000 }).pipe(Effect.orDie);
  return page.records.map(({ record }) => record);
});

export const outcomesOf = (records: ReadonlyArray<ConversationRecord.Record>) =>
  records.flatMap((record) => (record._tag === 'StepSettled' ? [record] : []));

export const tagsOf = (records: ReadonlyArray<ConversationRecord.Record>) =>
  records.map((record) => record._tag);

export const runStarted = (agent: string): ConversationRecord.Record => ({
  _tag: 'RunStarted',
  budget: testBudget,
  agent,
  formatVersion: 1,
  agentRevision: LogVocabulary.AgentRevision.make('1'),
  prompt: [],
});

/** The `inputDigest` a session records for a step's name and input. */
export const digestOf = (name: string, input: unknown): string =>
  createHash('sha256').update(InputDigest.material(name, input)).digest('hex');

export const toolCalled = (
  name: string,
  params: unknown,
  id: string = CALL_ID,
): ConversationRecord.Record => ({
  _tag: 'StepOpened',
  step: 1,
  id: LogVocabulary.ToolCallId.make(id),
  name,
  inputDigest: digestOf(name, params),
  params,
});

export const toolStarted = (
  name: string,
  id: string = CALL_ID,
  replay: 'safe' | 'unsafe' = 'unsafe',
): ConversationRecord.Record => ({
  _tag: 'StepStarted',
  id: LogVocabulary.ToolCallId.make(id),
  name,
  replay,
});

export const toolOutcome = (
  name: string,
  result: unknown,
  outcome: 'success' | 'failure' = 'success',
): ConversationRecord.Record => ({
  _tag: 'StepSettled',
  step: 1,
  id: CALL_ID,
  name,
  outcome,
  result,
});

export const runSettled: ConversationRecord.Record = {
  _tag: 'RunSettled',
  outcome: 'success',
  detail: '',
  steps: 2,
  usage: { input: 0, output: 0 },
};
