import { Context, Effect, Layer, Ref, Schema, Stream } from 'effect';
import {
  LanguageModel,
  type Response,
  Tool,
  Toolkit,
} from 'effect/unstable/ai';
import { describe, expect, it } from '@effect/vitest';

import { Agent } from '../src/agent.js';
import { AgentEval } from '../src/eval.js';

const finish = (
  reason: 'stop' | 'tool-calls' = 'stop',
): Response.FinishPartEncoded => ({
  type: 'finish',
  reason,
  usage: {
    inputTokens: { total: 3, uncached: 3, cacheRead: 0, cacheWrite: 0 },
    outputTokens: { total: 2 },
  },
});

const lookup = Tool.make('lookup', {
  description: 'Look something up.',
  parameters: Schema.Struct({ id: Schema.String }),
  success: Schema.Struct({ value: Schema.String }),
});

const agent = Agent.make({
  name: 'eval-target',
  revision: '3',
  instructions: 'Use lookup, then answer.',
  toolkit: Toolkit.make(lookup),
}).withHandlers({
  lookup: ({ id }) => Effect.succeed({ value: `found:${id}` }),
});

const model = (calls: Ref.Ref<number>) =>
  Layer.effect(
    LanguageModel.LanguageModel,
    LanguageModel.make({
      generateText: () => Effect.succeed([finish()]),
      streamText: () =>
        Stream.unwrap(
          Effect.gen(function* () {
            const call = yield* Ref.getAndUpdate(calls, (value) => value + 1);
            return call === 0
              ? Stream.fromIterable<Response.StreamPartEncoded>([
                  {
                    type: 'tool-call',
                    id: 'call-1',
                    name: 'lookup',
                    params: { id: '42' },
                  },
                  finish('tool-calls'),
                ])
              : Stream.fromIterable<Response.StreamPartEncoded>([
                  { type: 'text-start', id: 'answer' },
                  { type: 'text-delta', id: 'answer', delta: 'found it' },
                  { type: 'text-end', id: 'answer' },
                  finish(),
                ]);
          }),
        ),
    }),
  );

// Capturing an eval is still a real agent run. The helper must preserve the
// production requirement channel rather than hiding services behind a test API.
class RequiredService extends Context.Service<
  RequiredService,
  { readonly value: string }
>()('eval-test/RequiredService') {}

const requiredTool = Tool.make('required', {
  parameters: Schema.Struct({}),
  success: Schema.Struct({ value: Schema.String }),
  dependencies: [RequiredService],
});
const requiresService = Agent.make({
  name: 'requires-service',
  revision: '1',
  instructions: 'x',
  toolkit: Toolkit.make(requiredTool),
}).withHandlers({
  required: () => Effect.map(RequiredService, ({ value }) => ({ value })),
});
type EffR<T> = T extends Effect.Effect<infer _A, infer _E, infer R> ? R : never;
type Has<M, U> = [M] extends [U] ? 'yes' : 'no';
const capturedRequired = AgentEval.run(requiresService, 'go');
const _requiredSurvives: Has<
  RequiredService,
  EffR<typeof capturedRequired>
> = 'yes';
void _requiredSurvives;

const toolNameAssertions = (
  capture: AgentEval.Capture<Agent.Tools<typeof agent>>,
): void => {
  AgentEval.toolCalled(capture, 'lookup');
  // @ts-expect-error Tool queries reject names outside the agent's toolkit.
  AgentEval.toolCalled(capture, 'missing');
};
void toolNameAssertions;

describe('AgentEval', () => {
  it.effect('captures a complete run and typed tool evidence', () =>
    Effect.gen(function* () {
      const calls = yield* Ref.make(0);
      const capture = yield* AgentEval.run(agent, 'find 42').pipe(
        Effect.provide(model(calls)),
      );

      expect(capture.agent).toBe('eval-target');
      expect(capture.revision).toBe('3');
      expect(capture.result).toEqual({
        outcome: 'success',
        text: 'found it',
        steps: 2,
        usage: { input: 6, output: 4 },
      });
      expect(capture.events.at(-1)?._tag).toBe('Completed');
      expect(capture.durationMillis).toBeGreaterThanOrEqual(0);
      expect(capture.toolCalls).toMatchObject([
        { step: 1, part: { name: 'lookup', params: { id: '42' } } },
      ]);
      expect(capture.toolResults).toMatchObject([
        {
          step: 1,
          part: {
            name: 'lookup',
            result: { value: 'found:42' },
            isFailure: false,
          },
        },
      ]);
      expect(AgentEval.toolCalled(capture, 'lookup')).toBe(true);
      expect(AgentEval.toolSucceeded(capture, 'lookup')).toBe(true);
    }),
  );

  it.effect('runs deterministic and effectful scorers with weights', () =>
    Effect.gen(function* () {
      const calls = yield* Ref.make(0);
      const capture = yield* AgentEval.run(agent, 'find 42').pipe(
        Effect.provide(model(calls)),
      );
      const report = yield* AgentEval.evaluate(
        capture,
        [
          AgentEval.check(
            'used lookup',
            (sample) => AgentEval.toolCalled(sample, 'lookup'),
            { weight: 3 },
          ),
          AgentEval.makeScorer('answer quality', (sample) =>
            Effect.succeed({
              value: sample.result.text === 'found it' ? 0.5 : 0,
              detail: 'example effectful judge',
            }),
          ),
        ],
        { passThreshold: 0.5 },
      );

      expect(report.passed).toBe(true);
      expect(report.score).toBe(0.875);
      expect(report.scores).toMatchObject([
        { name: 'used lookup', value: 1, weight: 3 },
        { name: 'answer quality', value: 0.5, weight: 1 },
      ]);
    }),
  );

  it.effect(
    'fails explicitly when a scorer violates the normalized contract',
    () =>
      Effect.gen(function* () {
        const calls = yield* Ref.make(0);
        const capture = yield* AgentEval.run(agent, 'find 42').pipe(
          Effect.provide(model(calls)),
        );
        const result = yield* AgentEval.evaluate(capture, [
          AgentEval.makeScorer('broken judge', () =>
            Effect.succeed({ value: Number.NaN }),
          ),
        ]).pipe(Effect.result);

        expect(result).toMatchObject({
          _tag: 'Failure',
          failure: { _tag: 'InvalidEvalScore', scorer: 'broken judge' },
        });
      }),
  );

  it('rejects invalid scorer and threshold configuration eagerly', () => {
    expect(() => AgentEval.check('', () => true)).toThrow('non-empty');
    expect(() =>
      AgentEval.check('bad weight', () => true, { weight: 0 }),
    ).toThrow('greater than 0');
    expect(() =>
      AgentEval.evaluate(
        {
          agent: 'x',
          revision: '1',
          result: {
            outcome: 'success',
            text: '',
            steps: 0,
            usage: { input: 0, output: 0 },
          },
          events: [],
          durationMillis: 0,
          toolCalls: [],
          toolResults: [],
        },
        [],
        { passThreshold: 2 },
      ),
    ).toThrow('between 0 and 1');
  });
});
