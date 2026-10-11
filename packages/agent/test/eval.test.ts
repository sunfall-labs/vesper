import { Context, Effect, Layer, Schema, Stream } from 'effect';
import { LanguageModel, Tool, Toolkit } from 'effect/ai';
import { describe, expect, it } from '@effect/vitest';

import { Agent } from '../src/agent.js';
import { AgentEval } from '../src/eval.js';
import { ScriptedModel } from '../src/testing.js';
import { finish, textTurn, toolTurn } from './scenario-fixtures.js';

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

const textAgent = Agent.make({
  name: 'suite-target',
  revision: '1',
  instructions: 'Answer directly.',
  toolkit: Toolkit.make(),
});

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

const lookupModel = () =>
  ScriptedModel.make([
    toolTurn('lookup', { id: '42' }, 'call-1'),
    textTurn('found it'),
  ]).layer;

const saidYes = AgentEval.check(
  'said yes',
  (capture) => capture.result.text === 'yes',
);

describe('AgentEval capture and scoring', () => {
  it.effect('captures a run, scores it with weights, rejects bad scorers', () =>
    Effect.gen(function* () {
      const capture = yield* AgentEval.run(agent, 'find 42').pipe(
        Effect.provide(lookupModel()),
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
      expect(AgentEval.toolSucceeded(capture, 'lookup')).toBe(true);

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

      const broken = yield* AgentEval.evaluate(capture, [
        AgentEval.makeScorer('broken judge', () =>
          Effect.succeed({ value: Number.NaN }),
        ),
      ]).pipe(Effect.result);
      expect(broken).toMatchObject({
        _tag: 'Failure',
        failure: { _tag: 'InvalidEvalScore', scorer: 'broken judge' },
      });

      expect(() => AgentEval.check('', () => true)).toThrow('non-empty');
      expect(() =>
        AgentEval.check('bad weight', () => true, { weight: 0 }),
      ).toThrow('greater than 0');
      expect(() =>
        AgentEval.evaluate(capture, [], { passThreshold: 2 }),
      ).toThrow('between 0 and 1');
    }),
  );
});

describe('AgentEval.suite and compare', () => {
  const runSuite = (turns: ReadonlyArray<string>, name = 'text-suite') =>
    AgentEval.suite(textAgent, {
      name,
      cases: turns.map((_, index) => ({
        name: `case-${String(index)}`,
        input: 'q',
      })),
      scorers: [saidYes],
    }).pipe(
      Effect.provide(ScriptedModel.make(turns.map((t) => textTurn(t))).layer),
    );

  it('rejects an empty case list instead of vacuously passing', () => {
    expect(() =>
      AgentEval.suite(textAgent, { name: 'empty-suite', cases: [] }),
    ).toThrow('Eval suite "empty-suite" has no cases');
  });

  it.effect('aggregates cases, honors overrides, and round-trips reports', () =>
    Effect.gen(function* () {
      const report = yield* runSuite(['yes', 'no']);

      expect(report.suite).toBe('text-suite');
      expect(report.passed).toBe(1);
      expect(report.failed).toBe(1);
      expect(report.meanScore).toBe(0.5);
      expect(report.cases).toMatchObject([
        { name: 'case-0', score: 1, passed: true },
        { name: 'case-1', score: 0, passed: false },
      ]);
      const decoded = Schema.decodeUnknownSync(AgentEval.SuiteReport)(
        Schema.encodeSync(AgentEval.SuiteReport)(report),
      );
      expect(decoded).toEqual(report);

      const nonEmpty = AgentEval.check(
        'non-empty',
        (capture) => capture.result.text.length > 0,
      );
      const overridden = yield* AgentEval.suite(textAgent, {
        name: 'override-suite',
        cases: [
          { name: 'twice', input: 'a', scorers: [saidYes] },
          { name: 'once', input: 'b' },
        ],
        scorers: [nonEmpty],
      }).pipe(
        Effect.provide(
          ScriptedModel.make([textTurn('yes'), textTurn('no')]).layer,
        ),
      );
      expect(overridden.cases[0]?.scores.map((s) => s.name)).toEqual([
        'non-empty',
        'said yes',
      ]);
      expect(overridden.cases[1]?.scores.map((s) => s.name)).toEqual([
        'non-empty',
      ]);
    }),
  );

  it.effect('reports a case that dies without failing the suite', () =>
    Effect.gen(function* () {
      let calls = 0;
      const diesOnce = Layer.effect(
        LanguageModel.LanguageModel,
        LanguageModel.make({
          generateText: () => Effect.succeed([finish()]),
          streamText: () =>
            calls++ === 0
              ? Stream.die(new Error('boom'))
              : Stream.fromIterable(textTurn('ok')),
        }),
      );
      const report = yield* AgentEval.suite(textAgent, {
        name: 'dies-suite',
        cases: [
          { name: 'dies', input: 'a' },
          { name: 'survives', input: 'b' },
        ],
        scorers: [AgentEval.check('always', () => true)],
      }).pipe(Effect.provide(diesOnce));

      expect(report.cases[0]).toMatchObject({
        name: 'dies',
        score: 0,
        passed: false,
      });
      expect(report.cases[0]?.failure).toContain('boom');
      expect(report.cases[1]).toMatchObject({ name: 'survives', passed: true });
    }),
  );

  it.effect(
    'compares a baseline run against a regressed and improved one',
    () =>
      Effect.gen(function* () {
        const baseline = yield* runSuite(
          ['yes', 'yes', 'no', 'yes'],
          'regress',
        );
        const current = yield* runSuite(
          ['yes', 'no', 'yes', 'yes', 'yes'],
          'regress',
        );
        const comparison = AgentEval.compare(baseline, current);
        const byName = new Map(
          comparison.cases.map((delta) => [delta.name, delta.status]),
        );

        expect(comparison.verdict).toBe('regressed');
        expect([byName.get('case-0'), byName.get('case-1')]).toEqual([
          'unchanged',
          'regressed',
        ]);
        expect(byName.get('case-2')).toBe('improved');
        expect(byName.get('case-4')).toBe('new');
        expect(comparison).toMatchObject({
          added: 1,
          regressed: 1,
          improved: 1,
          unchanged: 2,
        });

        expect(
          AgentEval.compare(current, baseline).cases.find(
            (delta) => delta.name === 'case-4',
          )?.status,
        ).toBe('removed');

        const stable = yield* runSuite(['yes']);
        expect(AgentEval.compare(stable, stable).verdict).toBe('pass');
      }),
  );
});
