import { describe, expect, it } from '@effect/vitest';
import { Effect, Schema } from 'effect';
import {
  AiError,
  LanguageModel,
  type Response,
  Tool,
  Toolkit,
} from 'effect/unstable/ai';

import { Agent } from '../src/agent.js';
import { ScriptedModel } from '../src/testing.js';

const lookup = Tool.make('lookup_order', {
  description: 'Look up one order.',
  parameters: Schema.Struct({ orderId: Schema.String }),
  success: Schema.Struct({ status: Schema.String }),
});

const agent = Agent.make({
  name: 'scripted-model-test',
  revision: '1',
  instructions: 'Use the tool, then answer.',
  toolkit: Toolkit.make(lookup),
}).withHandlers({
  lookup_order: ({ orderId }) =>
    Effect.succeed({ status: `${orderId}:delivered` }),
});

const finish = (
  reason: Response.FinishPartEncoded['reason'] = 'stop',
): Response.FinishPartEncoded => ({
  type: 'finish',
  reason,
  usage: {
    inputTokens: { total: 1, uncached: 1, cacheRead: 0, cacheWrite: 0 },
    outputTokens: { total: 1 },
  },
});

const textTurn = (text: string): ReadonlyArray<Response.StreamPartEncoded> => [
  { type: 'text-start', id: 'text' },
  { type: 'text-delta', id: 'text', delta: text },
  { type: 'text-end', id: 'text' },
  finish(),
];

describe('ScriptedModel', () => {
  it.effect(
    'plays typed tool and text turns and captures normalized calls',
    () =>
      Effect.gen(function* () {
        const model = ScriptedModel.make([
          [
            {
              type: 'tool-call',
              id: 'lookup-1',
              name: 'lookup_order',
              params: { orderId: 'order-1' },
            },
            finish('tool-calls'),
          ],
          textTurn('Order order-1 was delivered.'),
        ]);

        const result = yield* agent
          .run('Where is order-1?')
          .pipe(Effect.provide(model.layer));
        const requests = yield* model.requests;

        expect(result.text).toBe('Order order-1 was delivered.');
        expect(requests.map((request) => request.operation)).toEqual([
          'streamText',
          'streamText',
        ]);
        expect(requests[0]!.tools).toEqual(['lookup_order']);
        expect(yield* model.remaining).toEqual({ generate: 0, stream: 0 });
      }),
  );

  it.effect('fails when the agent makes an unscripted call', () =>
    Effect.gen(function* () {
      const model = ScriptedModel.make([textTurn('first')]);
      yield* agent.run('first').pipe(Effect.provide(model.layer));

      const error = yield* agent
        .run('second')
        .pipe(Effect.provide(model.layer), Effect.flip);

      expect(error).toMatchObject({
        _tag: 'AiError',
        module: 'ScriptedModel',
        method: 'streamText',
      });
    }),
  );

  it.effect('injects a typed provider failure at an exact call', () =>
    Effect.gen(function* () {
      const injected = new AiError.AiError({
        module: 'fake-provider',
        method: 'streamText',
        reason: new AiError.RateLimitError({}),
      });
      const model = ScriptedModel.make([injected]);

      const error = yield* agent
        .run('fail now')
        .pipe(Effect.provide(model.layer), Effect.flip);

      expect(error).toBe(injected);
    }),
  );

  it.effect(
    'scripts generateText independently for compaction-style calls',
    () =>
      Effect.gen(function* () {
        const model = ScriptedModel.make([], {
          generate: [
            [{ type: 'text', text: 'Earlier history summary.' }, finish()],
          ],
        });

        const generated = yield* LanguageModel.generateText({
          prompt: 'Summarize this history.',
        }).pipe(Effect.provide(model.layer));
        const requests = yield* model.requests;

        expect(generated.text).toBe('Earlier history summary.');
        expect(requests.map((request) => request.operation)).toEqual([
          'generateText',
        ]);
        expect(yield* model.remaining).toEqual({ generate: 0, stream: 0 });
      }),
  );

  it.effect('repeats only when explicitly requested', () =>
    Effect.gen(function* () {
      const model = ScriptedModel.make([textTurn('same')], {
        repeatLast: true,
      });

      const first = yield* agent.run('first').pipe(Effect.provide(model.layer));
      const second = yield* agent
        .run('second')
        .pipe(Effect.provide(model.layer));

      expect([first.text, second.text]).toEqual(['same', 'same']);
    }),
  );
});
