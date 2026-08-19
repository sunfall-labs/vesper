import { Cause, Clock, Effect, Exit, Schema, Stream } from 'effect';
import type { Prompt, Tool } from 'effect/unstable/ai';

import type { Agent } from './agent.js';
import type { AgentEvents } from './event.js';

/** The tool-call evidence retained from one evaluated run. */
export interface ToolCall<Tools extends Record<string, Tool.Any>> {
  readonly step: number;
  readonly part: Extract<
    AgentEvents.StreamPart<Tools>,
    { readonly type: 'tool-call' }
  >;
}

/** The tool-result evidence retained from one evaluated run. */
export interface ToolResult<Tools extends Record<string, Tool.Any>> {
  readonly step: number;
  readonly part: Extract<
    AgentEvents.StreamPart<Tools>,
    { readonly type: 'tool-result' }
  >;
}

/**
 * A complete, inspectable execution sample.
 *
 * The input is deliberately absent. Prompts routinely contain secrets or
 * customer data, and an eval harness should not create a second accidental
 * persistence surface. Applications that want dataset identity can keep it
 * beside the capture in their own test case value.
 */
export interface Capture<Tools extends Record<string, Tool.Any>> {
  readonly agent: string;
  readonly revision: string;
  readonly result: Agent.Result;
  readonly events: ReadonlyArray<AgentEvents.ObservedEvent<Tools>>;
  readonly durationMillis: number;
  readonly toolCalls: ReadonlyArray<ToolCall<Tools>>;
  readonly toolResults: ReadonlyArray<ToolResult<Tools>>;
}

/** The value returned by one application-owned scorer. */
export interface ScoreValue {
  /** Normalized score from 0 (failed) through 1 (fully satisfied). */
  readonly value: number;
  readonly detail?: string | undefined;
}

/** A named, weighted result included in an evaluation report. */
export interface Score extends ScoreValue {
  readonly name: string;
  readonly weight: number;
}

/** An effectful criterion evaluated against one captured run. */
export interface Scorer<
  Tools extends Record<string, Tool.Any>,
  Error = never,
  Requires = never,
> {
  readonly name: string;
  readonly weight: number;
  readonly evaluate: (
    capture: Capture<Tools>,
  ) => Effect.Effect<ScoreValue, Error, Requires>;
}

export interface Report<Tools extends Record<string, Tool.Any>> {
  readonly capture: Capture<Tools>;
  readonly scores: ReadonlyArray<Score>;
  readonly score: number;
  readonly passed: boolean;
}

export interface ScorerOptions {
  readonly weight?: number | undefined;
}

export interface EvaluateOptions {
  /** Every individual score must meet this value. Defaults to 1. */
  readonly passThreshold?: number | undefined;
  readonly concurrency?: number | 'unbounded' | undefined;
}

/** A scorer returned a value outside the normalized 0..1 contract. */
export class InvalidScore extends Schema.TaggedError<InvalidScore>(
  '@sunfall/vesper-agent/InvalidEvalScore',
)('InvalidEvalScore', {
  scorer: Schema.String,
  value: Schema.String,
}) {}

const completedResult = <Tools extends Record<string, Tool.Any>>(
  events: ReadonlyArray<AgentEvents.ObservedEvent<Tools>>,
): Agent.Result => {
  for (let index = events.length - 1; index >= 0; index--) {
    const event = events[index];
    if (event?._tag === 'Completed') {
      return {
        outcome: event.outcome,
        text: event.text,
        steps: event.steps,
        usage: event.usage,
      };
    }
  }
  throw new Error('Agent stream ended without completing');
};

const evidence = <Tools extends Record<string, Tool.Any>>(
  events: ReadonlyArray<AgentEvents.ObservedEvent<Tools>>,
): {
  readonly toolCalls: ReadonlyArray<ToolCall<Tools>>;
  readonly toolResults: ReadonlyArray<ToolResult<Tools>>;
} => {
  const toolCalls: ToolCall<Tools>[] = [];
  const toolResults: ToolResult<Tools>[] = [];
  for (const event of events) {
    if (event._tag !== 'Part') {
      continue;
    }
    if (event.part.type === 'tool-call') {
      toolCalls.push({ step: event.step, part: event.part });
    } else if (event.part.type === 'tool-result') {
      toolResults.push({ step: event.step, part: event.part });
    }
  }
  return { toolCalls, toolResults };
};

interface EvalTarget<Tools extends Record<string, Tool.Any>, Error, Requires> {
  readonly name: string;
  readonly revision: string;
  readonly stream: (
    input: Prompt.RawInput,
  ) => Stream.Stream<AgentEvents.ObservedEvent<Tools>, Error, Requires>;
}

/**
 * Run an agent once and retain the public evidence an eval may assert on.
 *
 * The returned Effect preserves the agent's exact error and requirement
 * channels. An eval cannot accidentally make missing production services
 * disappear merely because it runs under a test runner.
 */
export const run = <Tools extends Record<string, Tool.Any>, Error, Requires>(
  agent: EvalTarget<Tools, Error, Requires>,
  input: Prompt.RawInput,
): Effect.Effect<Capture<Tools>, Error, Requires> =>
  Effect.gen(function* () {
    const started = yield* Clock.currentTimeNanos;
    const events = yield* Stream.runCollect(agent.stream(input));
    const finished = yield* Clock.currentTimeNanos;
    return {
      agent: agent.name,
      revision: agent.revision,
      result: completedResult(events),
      events,
      durationMillis: Number(finished - started) / 1_000_000,
      ...evidence(events),
    };
  });

const positiveFinite = (value: number): boolean =>
  Number.isFinite(value) && value > 0;

const normalized = (value: number): boolean =>
  Number.isFinite(value) && value >= 0 && value <= 1;

/** Construct an effectful scorer without coupling evals to a test framework. */
export const makeScorer = <
  Tools extends Record<string, Tool.Any>,
  Error = never,
  Requires = never,
>(
  name: string,
  evaluate: (
    capture: Capture<Tools>,
  ) => Effect.Effect<ScoreValue, Error, Requires>,
  options: ScorerOptions = {},
): Scorer<Tools, Error, Requires> => {
  const weight = options.weight ?? 1;
  if (name.trim().length === 0) {
    throw new RangeError('Eval scorer name must be non-empty');
  }
  if (!positiveFinite(weight)) {
    throw new RangeError(
      'Eval scorer weight must be finite and greater than 0',
    );
  }
  return { name, weight, evaluate };
};

/** Construct a deterministic pass/fail scorer. */
export const check = <Tools extends Record<string, Tool.Any>>(
  name: string,
  predicate: (capture: Capture<Tools>) => boolean,
  options: ScorerOptions = {},
): Scorer<Tools> =>
  makeScorer(
    name,
    (capture) => Effect.sync(() => ({ value: predicate(capture) ? 1 : 0 })),
    options,
  );

/** Evaluate application-owned scorers and calculate their weighted mean. */
export const evaluate = <
  Tools extends Record<string, Tool.Any>,
  Error = never,
  Requires = never,
>(
  capture: Capture<Tools>,
  scorers: ReadonlyArray<Scorer<Tools, Error, Requires>>,
  options: EvaluateOptions = {},
): Effect.Effect<Report<Tools>, Error | InvalidScore, Requires> => {
  const threshold = options.passThreshold ?? 1;
  if (!normalized(threshold)) {
    throw new RangeError('Eval passThreshold must be between 0 and 1');
  }
  return Effect.gen(function* () {
    const scores = yield* Effect.forEach(
      scorers,
      (scorer) =>
        Effect.gen(function* () {
          const score = yield* scorer.evaluate(capture);
          if (!normalized(score.value)) {
            return yield* new InvalidScore({
              scorer: scorer.name,
              value: String(score.value),
            });
          }
          return {
            name: scorer.name,
            weight: scorer.weight,
            ...score,
          } satisfies Score;
        }),
      { concurrency: options.concurrency ?? 1 },
    );
    const totalWeight = scores.reduce((sum, score) => sum + score.weight, 0);
    const weighted = scores.reduce(
      (sum, score) => sum + score.value * score.weight,
      0,
    );
    return {
      capture,
      scores,
      score: totalWeight === 0 ? 1 : weighted / totalWeight,
      passed: scores.every((score) => score.value >= threshold),
    };
  });
};

/** Query whether the model requested a particular typed tool. */
export const toolCalled = <Tools extends Record<string, Tool.Any>>(
  capture: Capture<Tools>,
  name: ToolCall<Tools>['part']['name'],
): boolean => capture.toolCalls.some((call) => call.part.name === name);

/** Query whether a particular typed tool produced a final successful result. */
export const toolSucceeded = <Tools extends Record<string, Tool.Any>>(
  capture: Capture<Tools>,
  name: ToolResult<Tools>['part']['name'],
): boolean =>
  capture.toolResults.some(
    (result) =>
      result.part.name === name &&
      !result.part.isFailure &&
      !result.part.preliminary,
  );

/** One named input a suite runs, with scorers specific to it. */
export interface Case<
  Tools extends Record<string, Tool.Any>,
  Error = never,
  Requires = never,
> {
  readonly name: string;
  readonly input: Prompt.RawInput;
  /** Combined with the suite's own scorers for this case only. */
  readonly scorers?: ReadonlyArray<Scorer<Tools, Error, Requires>> | undefined;
}

export interface SuiteOptions {
  /**
   * How many cases run at once. Defaults to 1 (sequential).
   *
   * A suite's model layer is routinely a single `ScriptedModel`, whose
   * request cursor is one shared, ordered sequence (see `testing.ts`).
   * Cases racing that cursor would turn a deterministic fixture into a
   * flaky one, so the safe default is sequential; raise this only when
   * every case has its own model layer or the live provider genuinely
   * tolerates concurrent calls.
   */
  readonly concurrency?: number | 'unbounded' | undefined;
  /** Forwarded to every case's `evaluate` call. Defaults to 1. */
  readonly passThreshold?: number | undefined;
}

/** A named collection of cases plus scorers shared by all of them. */
export interface SuiteDefinition<
  Tools extends Record<string, Tool.Any>,
  Error = never,
  Requires = never,
> {
  readonly name: string;
  readonly cases: ReadonlyArray<Case<Tools, Error, Requires>>;
  /** Applied to every case, alongside that case's own scorers. */
  readonly scorers?: ReadonlyArray<Scorer<Tools, Error, Requires>> | undefined;
  readonly options?: SuiteOptions | undefined;
}

/**
 * One score as it appears in a persisted report.
 *
 * Schema-modelled so a suite report round-trips through whatever an
 * application persists it as. `Score` above stays a plain interface because
 * scorers construct it inline; this is its stable wire shape.
 */
export const ScoreReport = Schema.Struct({
  name: Schema.String,
  weight: Schema.Finite,
  value: Schema.Finite,
  detail: Schema.optionalKey(Schema.String),
});
export interface ScoreReport extends Schema.Struct.Type<
  typeof ScoreReport.fields
> {}

/** One case's outcome in a persisted suite report. */
export const CaseReport = Schema.Struct({
  name: Schema.String,
  score: Schema.Finite,
  passed: Schema.Boolean,
  scores: Schema.Array(ScoreReport),
  /** Present only when the case failed to run or score at all. */
  failure: Schema.optionalKey(Schema.String),
});
export interface CaseReport extends Schema.Struct.Type<
  typeof CaseReport.fields
> {}

/**
 * The persisted result of running one suite.
 *
 * Schema-modelled for the same reason `Agent.Result` is: applications
 * checkpoint it, diff it in CI, or hand it to whatever store they already
 * have. Vesper does not persist it, and does not decide the store — see
 * `AgentEval.compare` for the one thing this library does with a pair of
 * them.
 */
export const SuiteReport = Schema.Struct({
  suite: Schema.String,
  cases: Schema.Array(CaseReport),
  passed: Schema.Natural,
  failed: Schema.Natural,
  meanScore: Schema.Finite,
  startedAt: Schema.Int,
  durationMillis: Schema.Finite,
});
export interface SuiteReport extends Schema.Struct.Type<
  typeof SuiteReport.fields
> {}

const asScoreReport = (score: Score): ScoreReport =>
  score.detail === undefined
    ? { name: score.name, weight: score.weight, value: score.value }
    : {
        name: score.name,
        weight: score.weight,
        value: score.value,
        detail: score.detail,
      };

/**
 * Run one case to a `CaseReport`, never failing the suite.
 *
 * `Effect.exit` catches both a typed run/scorer failure and a defect — the
 * agent throwing, a handler dying — because "the case broke" and "the case
 * returned InvalidScore" are the same fact from a suite's point of view: one
 * case produced no usable score.
 */
const runCase = <
  Tools extends Record<string, Tool.Any>,
  Error,
  Requires,
  ScorerError,
  ScorerRequires,
>(
  agent: EvalTarget<Tools, Error, Requires>,
  suiteScorers: ReadonlyArray<Scorer<Tools, ScorerError, ScorerRequires>>,
  testCase: Case<Tools, ScorerError, ScorerRequires>,
  passThreshold: number,
): Effect.Effect<CaseReport, never, Requires | ScorerRequires> =>
  Effect.gen(function* () {
    const scorers = [...suiteScorers, ...(testCase.scorers ?? [])];
    const exit = yield* Effect.exit(
      Effect.flatMap(run(agent, testCase.input), (capture) =>
        evaluate(capture, scorers, { passThreshold }),
      ),
    );
    if (Exit.isFailure(exit)) {
      return {
        name: testCase.name,
        score: 0,
        passed: false,
        scores: [],
        failure: Cause.pretty(exit.cause),
      };
    }
    return {
      name: testCase.name,
      score: exit.value.score,
      passed: exit.value.passed,
      scores: exit.value.scores.map(asScoreReport),
    };
  });

/**
 * Run every case in a suite against one agent and score it.
 *
 * A case that fails to run at all — the agent dies, a scorer throws, a score
 * violates the normalized contract — becomes a failed `CaseReport`, not a
 * failed suite. The point of a suite is one complete picture of every case
 * in a single pass; aborting on the first broken case would hide every
 * result after it.
 */
export const suite = <
  Tools extends Record<string, Tool.Any>,
  Error,
  Requires,
  ScorerError = never,
  ScorerRequires = never,
>(
  agent: EvalTarget<Tools, Error, Requires>,
  definition: SuiteDefinition<Tools, ScorerError, ScorerRequires>,
): Effect.Effect<SuiteReport, never, Requires | ScorerRequires> => {
  const threshold = definition.options?.passThreshold ?? 1;
  if (!normalized(threshold)) {
    throw new RangeError('Eval passThreshold must be between 0 and 1');
  }
  // An empty suite would report zero failures and a vacuous mean — a green
  // CI run measuring nothing, which is exactly the silent outcome a suite
  // exists to prevent. A case list that ends up empty (a filter that matched
  // nothing, a loader that found nothing) is a defect in the suite itself.
  if (definition.cases.length === 0) {
    throw new RangeError(`Eval suite "${definition.name}" has no cases`);
  }
  const scorers = definition.scorers ?? [];
  return Effect.gen(function* () {
    const startedAt = yield* Clock.currentTimeMillis;
    const started = yield* Clock.currentTimeNanos;
    const cases = yield* Effect.forEach(
      definition.cases,
      (testCase) => runCase(agent, scorers, testCase, threshold),
      { concurrency: definition.options?.concurrency ?? 1 },
    );
    const finished = yield* Clock.currentTimeNanos;
    const passed = cases.filter((one) => one.passed).length;
    const total = cases.reduce((sum, one) => sum + one.score, 0);
    return {
      suite: definition.name,
      cases,
      passed,
      failed: cases.length - passed,
      meanScore: total / cases.length,
      startedAt,
      durationMillis: Number(finished - started) / 1_000_000,
    };
  });
};

/** How one case's result changed between a baseline and a current report. */
export const CaseDelta = Schema.Struct({
  name: Schema.String,
  status: Schema.Literals([
    'new',
    'removed',
    'regressed',
    'improved',
    'unchanged',
  ]),
  /** Absent only when `status` is `new`. */
  baselineScore: Schema.optionalKey(Schema.Finite),
  /** Absent only when `status` is `removed`. */
  currentScore: Schema.optionalKey(Schema.Finite),
  /** Present only when the case ran in both reports. */
  scoreDelta: Schema.optionalKey(Schema.Finite),
});
export interface CaseDelta extends Schema.Struct.Type<
  typeof CaseDelta.fields
> {}

/** The regression comparison between two suite reports. */
export const Comparison = Schema.Struct({
  baseline: Schema.String,
  current: Schema.String,
  /** `regressed` whenever any case's status is `regressed`. */
  verdict: Schema.Literals(['pass', 'regressed']),
  cases: Schema.Array(CaseDelta),
  added: Schema.Natural,
  removed: Schema.Natural,
  regressed: Schema.Natural,
  improved: Schema.Natural,
  unchanged: Schema.Natural,
});
export interface Comparison extends Schema.Struct.Type<
  typeof Comparison.fields
> {}

/** Scores within this of each other count as unchanged. */
const SCORE_EPSILON = 1e-9;

const deltaStatus = (
  before: CaseReport,
  after: CaseReport,
): CaseDelta['status'] => {
  if (before.passed !== after.passed) {
    return before.passed ? 'regressed' : 'improved';
  }
  const delta = after.score - before.score;
  if (delta < -SCORE_EPSILON) {
    return 'regressed';
  }
  if (delta > SCORE_EPSILON) {
    return 'improved';
  }
  return 'unchanged';
};

/**
 * Diff two suite reports — one baseline, one current — into a per-case delta
 * and an overall verdict.
 *
 * Pure: it reads only the two reports, so a CI pipeline can commit a
 * baseline `SuiteReport`, run the suite again, and diff the two without
 * re-running anything or owning storage for either one. A case missing from
 * `current` is `removed` rather than `regressed` — dropped coverage is a
 * different fact than a case that got worse — and a case absent from
 * `baseline` is `new`.
 */
export const compare = (
  baseline: SuiteReport,
  current: SuiteReport,
): Comparison => {
  const before = new Map(baseline.cases.map((one) => [one.name, one]));
  const after = new Map(current.cases.map((one) => [one.name, one]));
  const names = new Set([...before.keys(), ...after.keys()]);

  const cases: CaseDelta[] = [];
  for (const name of names) {
    const was = before.get(name);
    const now = after.get(name);
    if (was !== undefined && now !== undefined) {
      cases.push({
        name,
        status: deltaStatus(was, now),
        baselineScore: was.score,
        currentScore: now.score,
        scoreDelta: now.score - was.score,
      });
    } else if (was !== undefined) {
      cases.push({ name, status: 'removed', baselineScore: was.score });
    } else if (now !== undefined) {
      cases.push({ name, status: 'new', currentScore: now.score });
    }
  }

  let added = 0;
  let removed = 0;
  let regressed = 0;
  let improved = 0;
  let unchanged = 0;
  for (const delta of cases) {
    switch (delta.status) {
      case 'new':
        added++;
        break;
      case 'removed':
        removed++;
        break;
      case 'regressed':
        regressed++;
        break;
      case 'improved':
        improved++;
        break;
      case 'unchanged':
        unchanged++;
        break;
    }
  }

  return {
    baseline: baseline.suite,
    current: current.suite,
    verdict: regressed > 0 ? 'regressed' : 'pass',
    cases,
    added,
    removed,
    regressed,
    improved,
    unchanged,
  };
};

export * as AgentEval from './eval.js';
