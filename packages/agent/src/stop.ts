import { Effect, Schema } from 'effect';
import type { Response, Tool } from 'effect/unstable/ai';

// When to stop looping. Kept as a first-class value rather than a boolean
// option because the interesting policies are compositions: "stop when the
// model stops calling tools, OR after 20 steps, OR when a terminal tool has
// been called".
//
// A stop condition is an Effect so a policy can consult a service — a budget
// tracker, a deadline, an approval queue — without the loop needing to know
// which.

export interface State<Tools extends Record<string, Tool.Any>> {
  /** 1 for the first model call. */
  readonly step: number;
  /** Tool calls the turn just requested. */
  readonly toolCalls: ReadonlyArray<Response.ToolCallPartEncoded>;
  /** Cumulative token usage across every step of this run. */
  readonly usage: Usage;
  readonly _tools?: Tools;
}

/**
 * Cumulative token usage for a run.
 *
 * A `Schema` rather than a plain interface because it crosses boundaries —
 * it rides on `Completed` events, and a persisted or transported run needs
 * to decode it. The same-name interface keeps call sites reading naturally.
 */
export const Usage = Schema.Struct({
  input: Schema.Natural,
  output: Schema.Natural,
});
export interface Usage extends Schema.Struct.Type<typeof Usage.fields> {}

export type StopCondition<Tools extends Record<string, Tool.Any>, R = never> = (
  state: State<Tools>,
) => Effect.Effect<boolean, never, R>;

/** Extract the services required by a stop condition. */
export type Services<Condition> =
  Condition extends StopCondition<infer _Tools, infer R> ? R : never;

type ToolsOf<Condition> =
  Condition extends StopCondition<infer Tools, infer _R> ? Tools : never;

type UnionToIntersection<U> = (
  U extends unknown ? (value: U) => void : never
) extends (value: infer Intersection) => void
  ? Intersection
  : never;

type CombinedTools<Conditions extends ReadonlyArray<unknown>> =
  UnionToIntersection<ToolsOf<Conditions[number]>> & Record<string, Tool.Any>;

/**
 * The default: stop as soon as a turn requests no tools.
 *
 * A turn with no tool calls is the model saying it is finished; continuing
 * would prompt it to talk to itself.
 */
export const noToolCalls =
  <Tools extends Record<string, Tool.Any>>(): StopCondition<Tools> =>
  (state) =>
    Effect.succeed(state.toolCalls.length === 0);

/**
 * Hard step ceiling.
 *
 * Always compose this with a real condition. A loop bounded only by step
 * count will happily burn the whole budget on a model stuck in a two-tool
 * cycle, and a loop with no ceiling at all will do it forever.
 */
export const maxSteps =
  <Tools extends Record<string, Tool.Any>>(
    limit: number,
  ): StopCondition<Tools> =>
  (state) =>
    Effect.succeed(state.step >= limit);

/** Stop once cumulative output tokens cross a budget. */
export const maxOutputTokens =
  <Tools extends Record<string, Tool.Any>>(
    limit: number,
  ): StopCondition<Tools> =>
  (state) =>
    Effect.succeed(state.usage.output >= limit);

/**
 * Stop as soon as a named tool has been called — the "terminal tool" pattern.
 *
 * The name is keyed to the toolkit rather than taken as a bare `string`, so a
 * condition naming a tool the agent does not have is a compile error. That is
 * worth the extra machinery: the failure it prevents is silent. A misspelled
 * terminal tool never matches, the condition never fires, and the run stops
 * only when some other clause in the composition happens to — which looks
 * like a model behaving oddly, not like a typo.
 */
export const toolCalled =
  <Tools extends Record<string, Tool.Any>>(
    name: keyof Tools & string,
  ): StopCondition<Tools> =>
  (state) =>
    Effect.succeed(state.toolCalls.some((call) => call.name === name));

/** Stop when any condition holds. */
export function any<
  Tools extends Record<string, Tool.Any> = Record<string, never>,
>(): StopCondition<Tools>;
export function any<
  const Conditions extends ReadonlyArray<
    // A function constraint avoids contextual widening before extraction.
    // oxlint-disable-next-line no-explicit-any
    (...args: any[]) => any
  >,
>(
  ...conditions: Conditions
): StopCondition<CombinedTools<Conditions>, Services<Conditions[number]>>;
export function any(
  // oxlint-disable-next-line no-explicit-any
  ...conditions: ReadonlyArray<StopCondition<any, any>>
  // oxlint-disable-next-line no-explicit-any
): StopCondition<any, any> {
  return (state) =>
    Effect.reduce(
      conditions,
      () => false,
      (stop, condition) => (stop ? Effect.succeed(true) : condition(state)),
    );
}

/** Stop only when every condition holds. */
export function all<
  Tools extends Record<string, Tool.Any> = Record<string, never>,
>(): StopCondition<Tools>;
export function all<
  const Conditions extends ReadonlyArray<
    // A function constraint avoids contextual widening before extraction.
    // oxlint-disable-next-line no-explicit-any
    (...args: any[]) => any
  >,
>(
  ...conditions: Conditions
): StopCondition<CombinedTools<Conditions>, Services<Conditions[number]>>;
export function all(
  // oxlint-disable-next-line no-explicit-any
  ...conditions: ReadonlyArray<StopCondition<any, any>>
  // oxlint-disable-next-line no-explicit-any
): StopCondition<any, any> {
  return (state) =>
    Effect.reduce(
      conditions,
      () => true,
      (stop, condition) => (stop ? condition(state) : Effect.succeed(false)),
    );
}

/**
 * What an agent uses when none is configured.
 *
 * The step ceiling is not a tuning parameter so much as a circuit breaker:
 * a model in a tool-call loop with no ceiling is an unbounded bill.
 */
export const defaultCondition = <
  Tools extends Record<string, Tool.Any>,
>(): StopCondition<Tools> =>
  any(noToolCalls<Tools>(), maxSteps<Tools>(32)) as StopCondition<Tools>;

export * as Stop from './stop.js';
