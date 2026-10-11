import { LogStoreMemory } from '@sunfall/vesper-log/layer-memory';
import { LogStore } from '@sunfall/vesper-log/log-store';
import type { ConversationRecord } from '@sunfall/vesper-log/record';
import * as NodeServices from '@effect/platform-node/NodeServices';
import { describe, expect, it } from '@effect/vitest';
import {
  Deferred,
  Effect,
  Fiber,
  Layer,
  Schema,
  Stream,
  Array as Arr,
} from 'effect';
import { TestClock } from 'effect/testing';
import {
  type LanguageModel as LanguageModelNamespace,
  LanguageModel,
  type Prompt,
  type Response,
  Toolkit,
} from 'effect/ai';

import { Agent } from '../src/agent.js';
import { Conversation } from '../src/conversation.js';
import { AgentHistory } from '../src/history.js';

const testLogLayer = Layer.mergeAll(
  LogStoreMemory.layer.pipe(Layer.provide(NodeServices.layer)),
  NodeServices.layer,
);

// Streamed output is committed before its turn settles, so a crash
// mid-answer loses at most one interval of it instead of the whole answer.
// What has to hold: the committed partial is readable as aborted output, the
// rerun turn is never shown it, and a turn that does finish records exactly
// what it recorded before progress existed.

const finish = () => ({
  type: 'finish' as const,
  reason: 'stop' as const,
  usage: {
    inputTokens: { total: 5, uncached: 5, cacheRead: 0, cacheWrite: 0 },
    outputTokens: { total: 2 },
  },
});

const delta = (text: string): Response.StreamPartEncoded => ({
  type: 'text-delta',
  id: 'a',
  delta: text,
});

const agent = Agent.make({
  name: 'test',
  revision: '1',
  instructions: 'be terse',
  toolkit: Toolkit.make(),
});

/** Drops the settlement append, which is what a killed process never writes. */
const diesBeforeSettling = Layer.effect(
  LogStore.Service,
  Effect.map(LogStore.Service, (store) =>
    LogStore.Service.of({
      ...store,
      append: (input) =>
        input.records.some(({ record }) => record._tag === 'RunSettled')
          ? Effect.fail(
              LogStore.makeError(
                input.path,
                'append',
                'storage',
                'process died',
              ),
            )
          : store.append(input),
    }),
  ),
).pipe(Layer.provide(testLogLayer));

/**
 * Streams reasoning and `Hel`, waits out the interval, streams `lo` and then
 * `wor`, and never finishes. `Hello` is committed at `lo`; `wor` arrived
 * inside a fresh window and is what a crash loses.
 */
const stallsMidAnswer = (reached: Deferred.Deferred<void>) => {
  const paused = Deferred.makeUnsafe<void>();
  return {
    paused,
    layer: Layer.effect(
      LanguageModel.LanguageModel,
      LanguageModel.make({
        generateText: () => Effect.succeed<Response.PartEncoded[]>([finish()]),
        streamText: () =>
          Stream.fromIterable<Response.StreamPartEncoded>([
            { type: 'reasoning-start', id: 'r' },
            { type: 'reasoning-delta', id: 'r', delta: 'thinking' },
            { type: 'text-start', id: 'a' },
            delta('Hel'),
          ]).pipe(
            Stream.concat(
              Stream.fromEffect(
                Deferred.succeed(paused, undefined).pipe(
                  Effect.andThen(Effect.sleep('250 millis')),
                  Effect.as(delta('lo')),
                ),
              ),
            ),
            Stream.concat(Stream.make(delta(' wor'))),
            Stream.concat(
              Stream.fromEffect(
                Deferred.succeed(reached, undefined).pipe(
                  Effect.andThen(Effect.never),
                ),
              ),
            ),
          ),
      }),
    ),
  };
};

/** Records every prompt it is asked to answer. */
const recording = (asked: Array<Prompt.Prompt>) =>
  Layer.effect(
    LanguageModel.LanguageModel,
    LanguageModel.make({
      generateText: () => Effect.succeed<Response.PartEncoded[]>([finish()]),
      streamText: (options: LanguageModelNamespace.ProviderOptions) =>
        Stream.suspend(() => {
          asked.push(options.prompt);
          return Stream.fromIterable<Response.StreamPartEncoded>([
            { type: 'text-start', id: 'b' },
            { type: 'text-delta', id: 'b', delta: 'fresh answer' },
            { type: 'text-end', id: 'b' },
            finish(),
          ]);
        }),
    }),
  );

/** Streams `Hel`, waits out the interval, then `lo`, and finishes. */
const slowAnswer = Layer.effect(
  LanguageModel.LanguageModel,
  LanguageModel.make({
    generateText: () => Effect.succeed<Response.PartEncoded[]>([finish()]),
    streamText: () =>
      Stream.fromIterable<Response.StreamPartEncoded>([
        { type: 'text-start', id: 'a' },
        delta('Hel'),
      ]).pipe(
        Stream.concat(
          Stream.fromEffect(
            Effect.sleep('250 millis').pipe(Effect.as(delta('lo'))),
          ),
        ),
        Stream.concat(
          Stream.make<Array<Response.StreamPartEncoded>>(
            { type: 'text-end', id: 'a' },
            finish(),
          ),
        ),
      ),
  }),
);

const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

/** Prompts and records rendered flat, for "does it mention" assertions. */
const json = (value: unknown): string => encodeJson(value);

const collect = (conversation: {
  readonly records: () => Stream.Stream<
    ConversationRecord.Envelope,
    LogStore.LogStoreError,
    LogStore.Service
  >;
}) =>
  Stream.runCollect(conversation.records()).pipe(Effect.map(Arr.fromIterable));

// A run's budget id is minted fresh per run, so two otherwise identical runs
// differ there and nowhere else.
const strip = (records: ReadonlyArray<ConversationRecord.Envelope>) =>
  records
    .map(({ record }) =>
      record._tag === 'RunStarted'
        ? { ...record, budget: { ...record.budget, id: '', root: '' } }
        : record,
    )
    .filter((record) => record._tag !== 'PartialOutput');

const tags = (records: ReadonlyArray<ConversationRecord.Envelope>) =>
  records.map(({ record }) => record._tag);

describe('partial output', () => {
  it.effect(
    'survives a crash mid-answer and stays out of the rerun prompt',
    () =>
      Effect.gen(function* () {
        const reached = yield* Deferred.make<void>();
        const stalled = stallsMidAnswer(reached);
        const conversation = Conversation.make(agent, 'crashing');

        const crashed = yield* conversation
          .run('hi')
          .pipe(
            Effect.orDie,
            Effect.provide(Layer.merge(stalled.layer, diesBeforeSettling)),
            Effect.forkChild,
          );
        yield* Deferred.await(stalled.paused);
        yield* TestClock.adjust('250 millis');
        yield* Deferred.await(reached);
        yield* Fiber.interrupt(crashed);

        const afterCrash = yield* Stream.runCollect(
          conversation.records(),
        ).pipe(Effect.map(Arr.fromIterable));
        expect(tags(afterCrash)).toEqual([
          'RunStarted',
          'PartialOutput',
          'PartialOutput',
        ]);
        expect(AgentHistory.abortedOutputFrom(afterCrash)).toMatchObject([
          { text: 'Hello', reasoning: 'thinking' },
        ]);
        // Never part of what a model is shown.
        expect(json(AgentHistory.messagesFrom(afterCrash))).not.toContain(
          'Hello',
        );

        const asked: Array<Prompt.Prompt> = [];
        const rerun = yield* conversation
          .run('again')
          .pipe(Effect.orDie, Effect.provide(recording(asked)));
        expect(rerun.text).toBe('fresh answer');
        expect(json(asked)).not.toContain('Hello');
        expect(json(asked)).not.toContain('thinking');

        const afterRerun = yield* Stream.runCollect(
          conversation.records(),
        ).pipe(Effect.map(Arr.fromIterable));
        // The crashed attempt stays on record as aborted, and is still not history.
        expect(AgentHistory.abortedOutputFrom(afterRerun)).toMatchObject([
          { text: 'Hello', reasoning: 'thinking' },
        ]);
        expect(json(AgentHistory.messagesFrom(afterRerun))).not.toContain(
          'Hello',
        );
      }).pipe(Effect.provide(testLogLayer)),
  );

  it.effect('records a finished turn exactly as it did without progress', () =>
    Effect.gen(function* () {
      const withProgress = Conversation.make(agent, 'with-progress');
      const without = Conversation.make(agent, 'without-progress', {
        partialOutput: false,
      });

      const forked = yield* Effect.all([
        withProgress.run('hi').pipe(Effect.orDie, Effect.forkChild),
        without.run('hi').pipe(Effect.orDie, Effect.forkChild),
      ]);
      // The sleeps are registered by fibers that may not have run yet, so keep
      // the clock moving until both runs have finished.
      yield* TestClock.adjust('50 millis').pipe(
        Effect.andThen(Effect.yieldNow),
        Effect.forever,
        Effect.forkChild,
      );
      yield* Effect.forEach(forked, Fiber.join);

      const a = yield* collect(withProgress);
      const b = yield* collect(without);

      expect(tags(a)).toContain('PartialOutput');
      expect(tags(b)).not.toContain('PartialOutput');
      expect(strip(a)).toEqual(strip(b));
      expect(AgentHistory.messagesFrom(a)).toEqual(
        AgentHistory.messagesFrom(b),
      );
      expect(AgentHistory.abortedOutputFrom(a)).toEqual([]);
    }).pipe(Effect.provide(Layer.merge(slowAnswer, testLogLayer))),
  );

  it.effect('writes nothing extra for a turn that streams quickly', () =>
    Effect.gen(function* () {
      const conversation = Conversation.make(agent, 'quick');
      yield* conversation.run('hi').pipe(Effect.orDie);
      const records = yield* Stream.runCollect(conversation.records()).pipe(
        Effect.map(Arr.fromIterable),
      );
      expect(tags(records)).not.toContain('PartialOutput');
    }).pipe(Effect.provide(Layer.merge(recording([]), testLogLayer))),
  );
});
