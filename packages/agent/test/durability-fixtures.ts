import { LogStoreMemory } from '@sunfall/vesper-log/layer-memory';
import { LogStore } from '@sunfall/vesper-log/log-store';
import {
  type ConversationRecord,
  FORMAT_VERSION,
} from '@sunfall/vesper-log/record';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';
import * as NodeServices from '@effect/platform-node/NodeServices';
import { type Crypto, Effect, Layer, Ref, Schema, Stream } from 'effect';
import { LanguageModel, Prompt, type Response, Tool, Toolkit } from 'effect/ai';

import { Agent } from '../src/agent.js';
import * as AgentLog from '../src/log.js';
import { testBudget } from './budget-fixture.js';
import { toolCalled, toolStarted } from './tool-fixtures.js';

// Shared fixtures for the durability scenario suites (recording, resume,
// settlement, recovery, branch, fork, compatibility). Scenarios drive the
// public API against a scripted model and a real in-memory store; this file
// holds no assertions.

export const testLogLayer = Layer.mergeAll(
  LogStoreMemory.layer.pipe(Layer.provide(NodeServices.layer)),
  NodeServices.layer,
);

export const present = <A>(value: A | undefined): A => {
  if (value === undefined) {
    throw new Error('Expected test fixture value to be present');
  }
  return value;
};

export const finish = (
  reason: Response.FinishReason = 'stop',
): Response.FinishPartEncoded => ({
  type: 'finish',
  reason,
  usage: {
    inputTokens: { total: 10, uncached: 10, cacheRead: 0, cacheWrite: 0 },
    outputTokens: { total: 4 },
  },
});

/** A turn that streams the given deltas as one text part, then stops. */
export const says = (...deltas: ReadonlyArray<string>) => {
  const id = deltas.join('');
  return [
    { type: 'text-start' as const, id },
    ...deltas.map((delta) => ({ type: 'text-delta' as const, id, delta })),
    { type: 'text-end' as const, id },
    finish(),
  ] satisfies Response.StreamPartEncoded[];
};

/** A turn that streams text and then calls `lookup` for order 42. */
export const callsLookup = (...deltas: ReadonlyArray<string>) =>
  [
    ...says(...(deltas.length > 0 ? deltas : ['looking'])).slice(0, -1),
    {
      type: 'tool-call' as const,
      id: 'call-1',
      name: 'lookup',
      params: { id: '42' },
    },
    finish('tool-calls'),
  ] satisfies Response.StreamPartEncoded[];

/**
 * A scripted model that keeps every prompt it was handed. The last turn
 * repeats once the script runs out. `hold` lets a test make concurrent runs
 * rendezvous inside the provider.
 */
export const provider = (
  turns: ReadonlyArray<ReadonlyArray<Response.StreamPartEncoded>>,
  hold?: Effect.Effect<void>,
) => {
  const asked: Array<Prompt.Prompt> = [];
  const layer = Layer.effect(
    LanguageModel.LanguageModel,
    Effect.gen(function* () {
      const calls = yield* Ref.make(0);
      return yield* LanguageModel.make({
        generateText: () => Effect.succeed<Response.PartEncoded[]>([finish()]),
        streamText: (options) =>
          Stream.unwrap(
            Effect.gen(function* () {
              asked.push(options.prompt);
              if (hold !== undefined) {
                yield* hold;
              }
              const index = yield* Ref.getAndUpdate(calls, (n) => n + 1);
              return Stream.fromIterable(
                present(turns[Math.min(index, turns.length - 1)]),
              );
            }),
          ),
      });
    }),
  );
  return { asked, layer };
};

export const lookup = Tool.make('lookup', {
  description: 'look an order up',
  parameters: Schema.Struct({ id: Schema.String }),
  success: Schema.Struct({ status: Schema.String }),
});

/** The standard test agent; `ran.count` is how many times `lookup` executed. */
export const lookupAgent = (name = 'test') => {
  const ran = { count: 0 };
  const agent = Agent.make({
    name,
    revision: '1',
    instructions: 'be terse',
    toolkit: Toolkit.make(lookup),
  }).withHandlers({
    lookup: ({ id }) =>
      Effect.sync(() => {
        ran.count += 1;
        return { status: `shipped:${id}` };
      }),
  });
  return { agent, ran };
};

export const plainAgent = Agent.make({
  name: 'test',
  revision: '1',
  instructions: 'be terse',
  toolkit: Toolkit.make(),
});

/** Run against the given model and a fresh in-memory log; defects fail loudly. */
export const run = <A, E>(
  effect: Effect.Effect<
    A,
    E,
    LogStore.Service | LanguageModel.LanguageModel | Crypto.Crypto
  >,
  models: Layer.Layer<LanguageModel.LanguageModel>,
) =>
  Layer.build(Layer.merge(models, testLogLayer)).pipe(
    Effect.flatMap((context) => Effect.provideContext(effect, context)),
    Effect.orDie,
    Effect.scoped,
  );

export const compatibility = (revision = '1') => ({
  agent: 'test',
  revision: LogVocabulary.AgentRevision.make(revision),
});

export const openSession = (id: string, revision = '1') =>
  AgentLog.open(LogVocabulary.ConversationId.make(id), {
    compatibility: compatibility(revision),
  });

/** The durable records of a conversation, oldest first. */
export const readRecords = Effect.fn('test.readRecords')(function* (
  id: string,
) {
  const store = yield* LogStore.Service;
  const page = yield* store
    .read(AgentLog.pathFor(LogVocabulary.ConversationId.make(id)), {
      limit: 10_000,
    })
    .pipe(Effect.orDie);
  return page.records;
});

/**
 * Write a previous run's records straight into a conversation's log.
 *
 * A crash has to leave the log in an exact state (a `RunStarted` with no
 * `RunSettled`, a `StepStarted` with no `StepSettled`) that a cleanly interrupted
 * fiber never produces, because interruption settles the run.
 */
export const seed = Effect.fn('test.seed')(function* (
  id: string,
  records: ReadonlyArray<ConversationRecord.Record>,
) {
  const store = yield* LogStore.Service;
  const conversationId = LogVocabulary.ConversationId.make(id);
  const path = AgentLog.pathFor(conversationId);
  yield* store.create(path, conversationId).pipe(Effect.orDie);
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
        conversationId,
        timestamp: 1_700_000_000_000,
        record,
      })),
    })
    .pipe(Effect.orDie);
});

export const tags = (records: ReadonlyArray<ConversationRecord.Envelope>) =>
  records.map((envelope) => envelope.record._tag);

export const textIn = (prompt: Prompt.Prompt): string =>
  JSON.stringify(prompt.content);

export const rolesOf = (prompt: Prompt.Prompt) =>
  prompt.content.map((message) => message.role);

/** The first record of every hand-written history. */
export const started = (
  text = 'hi',
  agent = 'test',
  revision = '1',
  formatVersion: number = FORMAT_VERSION,
): ConversationRecord.RecordOf<'RunStarted'> => ({
  _tag: 'RunStarted',
  budget: testBudget,
  agent,
  formatVersion,
  agentRevision: LogVocabulary.AgentRevision.make(revision),
  prompt: Prompt.make(text).content,
});

export const said = (
  text: string,
  step = 1,
): ConversationRecord.RecordOf<'Text'> => ({
  _tag: 'Text',
  step,
  text,
});

export const turn = (
  usage = { input: 1, output: 1 },
  step = 1,
): ConversationRecord.RecordOf<'TurnFinished'> => ({
  _tag: 'TurnFinished',
  step,
  usage,
});

export const settled = (
  usage = { input: 1, output: 1 },
): ConversationRecord.RecordOf<'RunSettled'> => ({
  _tag: 'RunSettled',
  outcome: 'success',
  detail: '',
  steps: 1,
  usage,
});

export const callId = (id: string) => LogVocabulary.ToolCallId.make(id);

export const lookupCall = (id = 'call-1'): ConversationRecord.Record =>
  toolCalled('lookup', { id: '42' }, id);

export const lookupStarted = (id = 'call-1'): ConversationRecord.Record =>
  toolStarted('lookup', id);

export const lookupOutcome = (
  id = 'call-1',
  status = 'from-log',
): ConversationRecord.Record => ({
  _tag: 'StepSettled',
  step: 1,
  id: callId(id),
  name: 'lookup',
  outcome: 'success',
  result: { status },
});
