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

const textAgent = Agent.make({
  name: 'suite-target',
  revision: '1',
  instructions: 'Answer directly.',
  toolkit: Toolkit.make(),
});

const textTurn = (text: string): ReadonlyArray<Response.StreamPartEncoded> => [
  { type: 'text-start', id: 'text' },
  { type: 'text-delta', id: 'text', delta: text },
  { type: 'text-end', id: 'text' },
  finish(),
];

const scripted = (
  turns: ReadonlyArray<ReadonlyArray<Response.StreamPartEncoded>>,
) => {
  let index = 0;
  return Layer.effect(
    LanguageModel.LanguageModel,
    LanguageModel.make({
      generateText: () => Effect.succeed([finish()]),
      streamText: () => {
        const turn = turns[index++];
        return turn === undefined
          ? Stream.die(new Error('script exhausted'))
          : Stream.fromIterable(turn);
      },
    }),
  );
};

describe('AgentEval.suite', () => {
  it.effect('scores every case and aggregates pass/fail', () =>
    Effect.gen(function* () {
      const saidYes = AgentEval.check(
        'said yes',
        (capture) => capture.result.text === 'yes',
      );
      const report = yield* AgentEval.suite(textAgent, {
        name: 'text-suite',
        cases: [
          { name: 'expects-yes', input: 'a' },
          { name: 'expects-yes-2', input: 'b' },
        ],
        scorers: [saidYes],
      }).pipe(Effect.provide(scripted([textTurn('yes'), textTurn('no')])));

      expect(report.suite).toBe('text-suite');
      expect(report.passed).toBe(1);
      expect(report.failed).toBe(1);
      expect(report.meanScore).toBe(0.5);
      expect(report.durationMillis).toBeGreaterThanOrEqual(0);
      expect(report.cases).toMatchObject([
        { name: 'expects-yes', score: 1, passed: true },
        { name: 'expects-yes-2', score: 0, passed: false },
      ]);
    }),
  );

  it.effect('combines suite scorers with a case-specific override', () =>
    Effect.gen(function* () {
      const nonEmpty = AgentEval.check(
        'non-empty',
        (capture) => capture.result.text.length > 0,
      );
      const saidYes = AgentEval.check(
        'is-yes',
        (capture) => capture.result.text === 'yes',
        { weight: 2 },
      );
      const report = yield* AgentEval.suite(textAgent, {
        name: 'override-suite',
        cases: [
          { name: 'checked-twice', input: 'a', scorers: [saidYes] },
          { name: 'checked-once', input: 'b' },
        ],
        scorers: [nonEmpty],
      }).pipe(Effect.provide(scripted([textTurn('yes'), textTurn('no')])));

      expect(report.cases[0]?.scores.map((score) => score.name)).toEqual([
        'non-empty',
        'is-yes',
      ]);
      expect(report.cases[1]?.scores.map((score) => score.name)).toEqual([
        'non-empty',
      ]);
      // 'no' still satisfies the suite-only scorer, so both cases pass.
      expect(report.passed).toBe(2);
    }),
  );

  it.effect(
    'reports a case that dies as a failure without failing the suite',
    () =>
      Effect.gen(function* () {
        let calls = 0;
        const diesOnce = Layer.effect(
          LanguageModel.LanguageModel,
          LanguageModel.make({
            generateText: () => Effect.succeed([finish()]),
            streamText: () => {
              const call = calls++;
              return call === 0
                ? Stream.die(new Error('boom'))
                : Stream.fromIterable(textTurn('ok'));
            },
          }),
        );
        const always = AgentEval.check('always', () => true);
        const report = yield* AgentEval.suite(textAgent, {
          name: 'dies-suite',
          cases: [
            { name: 'dies', input: 'a' },
            { name: 'survives', input: 'b' },
          ],
          scorers: [always],
        }).pipe(Effect.provide(diesOnce));

        expect(report.passed).toBe(1);
        expect(report.failed).toBe(1);
        expect(report.cases[0]).toMatchObject({
          name: 'dies',
          score: 0,
          passed: false,
        });
        expect(report.cases[0]?.failure).toContain('boom');
        expect(report.cases[1]).toMatchObject({
          name: 'survives',
          score: 1,
          passed: true,
        });
      }),
  );

  it.effect('round-trips a suite report through its schema', () =>
    Effect.gen(function* () {
      const saidYes = AgentEval.check(
        'said yes',
        (capture) => capture.result.text === 'yes',
      );
      const report = yield* AgentEval.suite(textAgent, {
        name: 'roundtrip-suite',
        cases: [{ name: 'expects-yes', input: 'a' }],
        scorers: [saidYes],
      }).pipe(Effect.provide(scripted([textTurn('yes')])));

      const encoded = Schema.encodeSync(AgentEval.SuiteReport)(report);
      const decoded = Schema.decodeUnknownSync(AgentEval.SuiteReport)(encoded);
      expect(decoded).toEqual(report);
    }),
  );
});

const caseReport = (
  name: string,
  score: number,
  passed: boolean,
): AgentEval.CaseReport => ({ name, score, passed, scores: [] });

const suiteReport = (
  name: string,
  cases: ReadonlyArray<AgentEval.CaseReport>,
): AgentEval.SuiteReport => ({
  suite: name,
  cases,
  passed: cases.filter((one) => one.passed).length,
  failed: cases.filter((one) => !one.passed).length,
  meanScore:
    cases.reduce((sum, one) => sum + one.score, 0) / (cases.length || 1),
  startedAt: 0,
  durationMillis: 0,
});

describe('AgentEval.compare', () => {
  it('classifies every case as new, removed, regressed, improved, or unchanged', () => {
    const baseline = suiteReport('regression-suite', [
      caseReport('a', 1, true),
      caseReport('b', 0.5, true),
      caseReport('c', 0.2, false),
      caseReport('d', 0.9, true),
    ]);
    const current = suiteReport('regression-suite', [
      caseReport('a', 1, true),
      caseReport('b', 0.2, false),
      caseReport('c', 0.9, true),
      caseReport('e', 0.5, true),
    ]);

    const comparison = AgentEval.compare(baseline, current);

    expect(comparison.verdict).toBe('regressed');
    expect(comparison.added).toBe(1);
    expect(comparison.removed).toBe(1);
    expect(comparison.regressed).toBe(1);
    expect(comparison.improved).toBe(1);
    expect(comparison.unchanged).toBe(1);
    const byName = new Map(
      comparison.cases.map((delta) => [delta.name, delta.status]),
    );
    expect(byName.get('a')).toBe('unchanged');
    expect(byName.get('b')).toBe('regressed');
    expect(byName.get('c')).toBe('improved');
    expect(byName.get('d')).toBe('removed');
    expect(byName.get('e')).toBe('new');
  });

  it('verdicts pass when nothing regressed', () => {
    const baseline = suiteReport('stable-suite', [caseReport('a', 1, true)]);
    const current = suiteReport('stable-suite', [caseReport('a', 1, true)]);

    expect(AgentEval.compare(baseline, current).verdict).toBe('pass');
  });
});
