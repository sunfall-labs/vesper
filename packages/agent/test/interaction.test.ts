import * as NodeServices from '@effect/platform-node/NodeServices';
import { describe, expect, it } from '@effect/vitest';
import { LogStoreMemory } from '@sunfall/vesper-log/layer-memory';
import type { LogStore } from '@sunfall/vesper-log/log-store';
import { Effect, Layer, Ref, Schema, Stream, type Crypto } from 'effect';
import {
  IdGenerator,
  LanguageModel,
  type Response,
  Tool,
  Toolkit,
} from 'effect/unstable/ai';

import { Agent } from '../src/agent.js';
import { Conversation } from '../src/conversation.js';
import { Interaction } from '../src/interaction.js';

const finish = (reason: 'stop' | 'tool-calls' = 'stop') => ({
  type: 'finish' as const,
  reason,
  usage: {
    inputTokens: { total: 10, uncached: 10, cacheRead: 0, cacheWrite: 0 },
    outputTokens: { total: 4 },
  },
});

const Question = Interaction.answer(
  Tool.make('question', {
    description: 'ask the user for information',
    parameters: Schema.Struct({
      question: Schema.NonEmptyString,
      options: Schema.NullOr(Schema.Array(Schema.NonEmptyString)),
    }),
    success: Schema.Struct({ answer: Schema.NonEmptyString }),
  }),
);

const agent = Agent.make({
  name: 'interaction-test',
  revision: '1',
  instructions: 'ask through the question tool',
  toolkit: Toolkit.make(Question),
}).withHandlers({ question: () => Interaction.unreachable });

const model = Layer.effect(
  LanguageModel.LanguageModel,
  Effect.gen(function* () {
    const calls = yield* Ref.make(0);
    return yield* LanguageModel.make({
      generateText: () => Effect.succeed<Response.PartEncoded[]>([finish()]),
      streamText: () =>
        Stream.unwrap(
          Effect.gen(function* () {
            const index = yield* Ref.getAndUpdate(calls, (value) => value + 1);
            if (index === 0) {
              return Stream.fromIterable<Response.StreamPartEncoded>([
                {
                  type: 'tool-call',
                  id: 'question-call',
                  name: 'question',
                  params: {
                    question: 'Which color?',
                    options: ['blue', 'green'],
                  },
                },
                finish('tool-calls'),
              ]);
            }
            return Stream.fromIterable<Response.StreamPartEncoded>([
              { type: 'text-start', id: 'answer' },
              { type: 'text-delta', id: 'answer', delta: 'Blue it is.' },
              { type: 'text-end', id: 'answer' },
              finish(),
            ]);
          }),
        ),
    });
  }),
);

const provide = <A, E>(
  effect: Effect.Effect<
    A,
    E,
    LogStore.Service | LanguageModel.LanguageModel | Crypto.Crypto
  >,
): Effect.Effect<A, E> =>
  effect.pipe(
    Effect.provide(
      Layer.merge(
        model,
        Layer.mergeAll(
          LogStoreMemory.layer.pipe(Layer.provide(NodeServices.layer)),
          NodeServices.layer,
          Layer.succeed(
            IdGenerator.IdGenerator,
            IdGenerator.defaultIdGenerator,
          ),
        ),
      ),
    ),
    Effect.scoped,
  );

describe('native external interactions', () => {
  it.effect('durably turns a typed answer into the tool result', () =>
    provide(
      Effect.gen(function* () {
        const conversation = Conversation.make(agent, 'question-conversation');

        const suspended = yield* conversation.run('ask me');
        expect(suspended.outcome).toBe('suspended');
        if (suspended.outcome !== 'suspended') {
          throw new Error('expected question to suspend');
        }
        expect(suspended.pendingInteractions).toEqual([
          {
            toolCallId: 'question-call',
            toolName: 'question',
            kind: 'question',
            request: {
              question: 'Which color?',
              options: ['blue', 'green'],
            },
          },
        ]);

        yield* conversation.resolveInteraction(Question, 'question-call', {
          answer: 'blue',
        });
        const completed = yield* conversation.run();
        expect(completed).toMatchObject({
          outcome: 'success',
          text: 'Blue it is.',
        });

        const records = yield* conversation.records().pipe(Stream.runCollect);
        const suspension = Array.from(records).find(
          ({ record }) => record._tag === 'ToolSuspended',
        )?.record;
        expect(suspension).toMatchObject({
          interaction: { name: 'question', mode: 'answer' },
        });
        const outcome = Array.from(records).find(
          ({ record }) =>
            record._tag === 'ToolOutcome' && record.id === 'question-call',
        )?.record;
        expect(outcome).toMatchObject({
          outcome: 'success',
          result: { answer: 'blue' },
        });
      }),
    ),
  );

  it.effect('validates the answer before recording it', () =>
    provide(
      Effect.gen(function* () {
        const conversation = Conversation.make(agent, 'invalid-answer');
        yield* conversation.run('ask me');
        const exit = yield* Effect.exit(
          conversation.resolveInteraction(Question, 'question-call', {
            answer: '',
          }),
        );
        expect(exit._tag).toBe('Failure');
        const waits = yield* conversation.waits().pipe(Stream.runCollect);
        expect(
          Array.from(waits).filter(
            ({ record }) => record._tag === 'ToolWaitCompleted',
          ),
        ).toHaveLength(0);
      }),
    ),
  );
});
