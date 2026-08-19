import { Clock, Effect, Ref, Semaphore, Stream } from 'effect';

import { RunPolicy } from './run-policy.js';
import type { Stop } from './stop.js';

export interface Exhaustion {
  readonly limit: RunPolicy.Limit;
  readonly used: number;
  readonly maximum: number;
}

interface Counters {
  readonly turns: number;
  readonly modelCalls: number;
  readonly delegatedTasks: number;
  readonly inputTokens: number;
  readonly outputTokens: number;
  readonly steeredBytes: number;
}

const emptyCounters: Counters = {
  turns: 0,
  modelCalls: 0,
  delegatedTasks: 0,
  inputTokens: 0,
  outputTokens: 0,
  steeredBytes: 0,
};

type SignalExhaustion = Exhaustion & {
  readonly limit: 'signal_bytes' | 'signals_per_boundary' | 'steered_bytes';
};

/** The result of applying one signal to a run's hard budgets. */
export type SignalDecision =
  | {
      readonly accepted: true;
      readonly bytes: number;
    }
  | {
      readonly accepted: false;
      readonly bytes: number;
      readonly exhaustion: SignalExhaustion;
    };

/** Non-mutating eligibility check used by responsive cancel detection. */
export const acceptsCancel = (
  limits: RunPolicy.Limits,
  text: string,
  index: number,
): boolean =>
  index < limits.maxSignalsPerBoundary &&
  new TextEncoder().encode(text).byteLength <= limits.maxSignalBytes;

/** One root run's shared, lexical budget state. */
export interface Runtime {
  readonly limits: RunPolicy.Limits;
  readonly turn: Effect.Effect<void, RunPolicy.RunPolicyExhausted>;
  readonly modelCall: Effect.Effect<void, RunPolicy.RunPolicyExhausted>;
  readonly addUsage: (
    usage: Stop.Usage,
  ) => Effect.Effect<void, RunPolicy.RunPolicyExhausted>;
  readonly delegation: <A, E, R>(
    effect: Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E | RunPolicy.RunPolicyExhausted, R>;
  readonly toolStream: <A, E, R>(
    stream: Stream.Stream<A, E, R>,
  ) => Stream.Stream<A, E, R>;
  readonly signal: (
    kind: 'steer' | 'cancel',
    text: string,
    index: number,
  ) => Effect.Effect<SignalDecision>;
  readonly remainingMillis: Effect.Effect<number, RunPolicy.RunPolicyExhausted>;
}

export const error = (exhaustion: Exhaustion): RunPolicy.RunPolicyExhausted =>
  new RunPolicy.RunPolicyExhausted(exhaustion);

export const create = Effect.fn('RunPolicy.create')(function* (
  configured: RunPolicy.Limits,
) {
  const limits = RunPolicy.make(configured);
  const started = yield* Clock.currentTimeMillis;
  const counters = yield* Ref.make(emptyCounters);
  const children = yield* Semaphore.make(limits.maxConcurrentChildren);
  const tools = yield* Semaphore.make(limits.maxToolConcurrency);

  const checkDeadline = Effect.gen(function* () {
    const now = yield* Clock.currentTimeMillis;
    const remaining = started + limits.wallClockMillis - now;
    if (remaining <= 0) {
      return yield* error({
        limit: 'deadline',
        used: now - started,
        maximum: limits.wallClockMillis,
      });
    }
    return remaining;
  });

  const increment = (
    field: 'turns' | 'modelCalls' | 'delegatedTasks',
    limit: RunPolicy.Limit,
    maximum: number,
  ): Effect.Effect<void, RunPolicy.RunPolicyExhausted> =>
    Effect.gen(function* () {
      yield* checkDeadline;
      const next = yield* Ref.modify(counters, (current) => {
        const used = current[field] + 1;
        return [used, { ...current, [field]: used }] as const;
      });
      if (next > maximum) {
        return yield* error({ limit, used: next, maximum });
      }
    });

  const addUsage: Runtime['addUsage'] = (usage) =>
    Effect.gen(function* () {
      yield* checkDeadline;
      const next = yield* Ref.updateAndGet(counters, (current) => ({
        ...current,
        inputTokens: current.inputTokens + usage.input,
        outputTokens: current.outputTokens + usage.output,
      }));
      if (next.inputTokens > limits.maxInputTokens) {
        return yield* error({
          limit: 'input_tokens',
          used: next.inputTokens,
          maximum: limits.maxInputTokens,
        });
      }
      if (next.outputTokens > limits.maxOutputTokens) {
        return yield* error({
          limit: 'output_tokens',
          used: next.outputTokens,
          maximum: limits.maxOutputTokens,
        });
      }
    });

  const delegation: Runtime['delegation'] = (effect) =>
    Effect.gen(function* () {
      yield* increment(
        'delegatedTasks',
        'delegated_tasks',
        limits.maxDelegatedTasks,
      );
      return yield* children.withPermit(effect);
    });

  const signal: Runtime['signal'] = (kind, text, index) =>
    Effect.gen(function* () {
      const bytes = new TextEncoder().encode(text).byteLength;
      if (index >= limits.maxSignalsPerBoundary) {
        return {
          accepted: false,
          bytes,
          exhaustion: {
            limit: 'signals_per_boundary',
            used: index + 1,
            maximum: limits.maxSignalsPerBoundary,
          },
        };
      }
      if (bytes > limits.maxSignalBytes) {
        return {
          accepted: false,
          bytes,
          exhaustion: {
            limit: 'signal_bytes',
            used: bytes,
            maximum: limits.maxSignalBytes,
          },
        };
      }
      if (kind === 'cancel') {
        return { accepted: true, bytes };
      }
      const total = yield* Ref.modify(counters, (current) => {
        const steeredBytes = current.steeredBytes + bytes;
        return [steeredBytes, { ...current, steeredBytes }] as const;
      });
      if (total > limits.maxSteeredBytes) {
        return {
          accepted: false,
          bytes,
          exhaustion: {
            limit: 'steered_bytes',
            used: total,
            maximum: limits.maxSteeredBytes,
          },
        };
      }
      return { accepted: true, bytes };
    });

  return {
    limits,
    turn: increment('turns', 'turns', limits.maxTurns),
    modelCall: increment('modelCalls', 'model_calls', limits.maxModelCalls),
    addUsage,
    delegation,
    toolStream: (stream) =>
      Stream.scoped(
        Stream.unwrap(
          Effect.acquireRelease(tools.take(1), (permits) =>
            tools.release(permits),
          ).pipe(Effect.as(stream)),
        ),
      ),
    signal,
    remainingMillis: checkDeadline,
  } satisfies Runtime;
});

export * as RunPolicyRuntime from './run-policy-runtime.js';
