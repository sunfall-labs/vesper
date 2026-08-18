import { Clock, Effect, Schema, Stream } from 'effect';
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
    if (event._tag !== 'Part') continue;
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

export * as AgentEval from './eval.js';
