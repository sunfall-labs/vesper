import { describe, expect, it } from '@effect/vitest';
import { Effect, Layer, Ref, Schema, Stream } from 'effect';
import {
  LanguageModel,
  type Response,
  Tool,
  Toolkit,
} from 'effect/unstable/ai';

import { Agent } from '../src/agent.js';

const readFile = Tool.make('read_file', {
  description: 'read a file',
  parameters: Schema.Struct({ path: Schema.String }),
  success: Schema.String,
  failureMode: 'return',
});

const toolkit = Toolkit.make(readFile);
const handlers = (calls: Ref.Ref<number>) =>
  toolkit.toLayer({
    read_file: ({ path }) =>
      Ref.update(calls, (count) => count + 1).pipe(Effect.as(path)),
  });

const finish = (
  reason: 'stop' | 'tool-calls' = 'stop',
): Response.FinishPartEncoded => ({
  type: 'finish',
  reason,
  usage: {
    inputTokens: { total: 1, uncached: 1, cacheRead: 0, cacheWrite: 0 },
    outputTokens: { total: 1 },
  },
});

const malformedModel = LanguageModel.make({
  generateText: () => Effect.succeed([]),
  streamText: () =>
    Stream.fromIterable<Response.StreamPartEncoded>([
      {
        type: 'tool-call',
        id: 'call-1',
        name: 'read_file',
        params: { path: { segments: ['src'] } },
      },
      finish('tool-calls'),
    ]),
});

describe('tool parameter recovery', () => {
  it.effect(
    'returns malformed model parameters to the model as a typed tool failure',
    () =>
      Effect.gen(function* () {
        const calls = yield* Ref.make(0);
        const resolved = yield* toolkit.pipe(Effect.provide(handlers(calls)));
        const parts = yield* LanguageModel.streamText({
          prompt: 'read the folder',
          toolkit: resolved,
        }).pipe(
          Stream.runCollect,
          Effect.provideServiceEffect(
            LanguageModel.LanguageModel,
            malformedModel,
          ),
        );

        const result = parts.find((part) => part.type === 'tool-result');
        expect(result).toMatchObject({
          type: 'tool-result',
          id: 'call-1',
          name: 'read_file',
          isFailure: true,
          result: {
            _tag: 'AiError',
            reason: {
              _tag: 'ToolParameterValidationError',
              toolName: 'read_file',
              toolParams: { path: { segments: ['src'] } },
            },
          },
        });
        expect(yield* Ref.get(calls)).toBe(0);
      }),
  );

  it.effect('lets the Vesper loop continue after malformed parameters', () =>
    Effect.gen(function* () {
      const handlerCalls = yield* Ref.make(0);
      const modelCalls = yield* Ref.make(0);
      const agent = Agent.make({
        name: 'parameter-recovery',
        revision: '1',
        instructions: 'use the file tool',
        toolkit,
      }).withHandlers({
        read_file: ({ path }) =>
          Ref.update(handlerCalls, (count) => count + 1).pipe(Effect.as(path)),
      });
      const model = Layer.effect(
        LanguageModel.LanguageModel,
        LanguageModel.make({
          generateText: () => Effect.succeed([]),
          streamText: () =>
            Stream.unwrap(
              Effect.gen(function* () {
                const turn = yield* Ref.getAndUpdate(
                  modelCalls,
                  (count) => count + 1,
                );
                return turn === 0
                  ? Stream.fromIterable<Response.StreamPartEncoded>([
                      {
                        type: 'tool-call',
                        id: 'call-1',
                        name: 'read_file',
                        params: { path: { segments: ['src'] } },
                      },
                      finish('tool-calls'),
                    ])
                  : Stream.fromIterable<Response.StreamPartEncoded>([
                      { type: 'text-start', id: 'answer' },
                      {
                        type: 'text-delta',
                        id: 'answer',
                        delta: 'Recovered.',
                      },
                      { type: 'text-end', id: 'answer' },
                      finish(),
                    ]);
              }),
            ),
        }),
      );

      const result = yield* agent
        .run('read the folder')
        .pipe(Effect.provide(model), Effect.orDie);

      expect(result.text).toBe('Recovered.');
      expect(yield* Ref.get(modelCalls)).toBe(2);
      expect(yield* Ref.get(handlerCalls)).toBe(0);
    }),
  );
});
