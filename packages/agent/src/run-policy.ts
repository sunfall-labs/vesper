import { Clock, Effect, Ref, Semaphore, Stream } from 'effect';
import { AiError } from 'effect/unstable/ai';

import type { Stop } from './stop.js';

/** Non-overridable limits shared by a root run and every descendant loop. */
export interface Limits {
  readonly maxTurns: number;
  readonly maxModelCalls: number;
  readonly maxDelegatedTasks: number;
  readonly maxDelegationDepth: number;
  readonly maxConcurrentChildren: number;
  readonly maxToolConcurrency: number;
  readonly wallClockMillis: number;
  readonly maxInputTokens: number;
  readonly maxOutputTokens: number;
  readonly maxSignalBytes: number;
  readonly maxSignalsPerBoundary: number;
  readonly maxSteeredBytes: number;
}

/** Explicit production-safe default used unless the application supplies one. */
export const defaultLimits: Limits = {
  maxTurns: 128,
  maxModelCalls: 160,
  maxDelegatedTasks: 64,
  maxDelegationDepth: 4,
  maxConcurrentChildren: 8,
  maxToolConcurrency: 16,
  wallClockMillis: 15 * 60_000,
  maxInputTokens: 2_000_000,
  maxOutputTokens: 250_000,
  maxSignalBytes: 64 * 1024,
  maxSignalsPerBoundary: 32,
  maxSteeredBytes: 256 * 1024,
};

export type Limit =
  | 'turns'
  | 'model_calls'
  | 'delegated_tasks'
  | 'deadline'
  | 'input_tokens'
  | 'output_tokens'
  | 'signal_bytes'
  | 'signals_per_boundary'
  | 'steered_bytes';

export interface Exhaustion {
  readonly limit: Limit;
  readonly used: number;
  readonly maximum: number;
}

export interface Snapshot {
  readonly turns: number;
  readonly modelCalls: number;
  readonly delegatedTasks: number;
  readonly usage: Stop.Usage;
  readonly steeredBytes: number;
  readonly limits: Limits;
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

export interface SignalDecision {
  readonly accepted: boolean;
  readonly bytes: number;
  readonly exhaustion?:
    | (Exhaustion & {
        readonly limit:
          | 'signal_bytes'
          | 'signals_per_boundary'
          | 'steered_bytes';
      })
    | undefined;
}

/** Non-mutating eligibility check used by responsive cancel detection. */
export const acceptsCancel = (
  limits: Limits,
  text: string,
  index: number,
): boolean =>
  index < limits.maxSignalsPerBoundary &&
  new TextEncoder().encode(text).byteLength <= limits.maxSignalBytes;

/** One root run's shared, lexical budget state. */
export interface Runtime {
  readonly limits: Limits;
  readonly deadline: number;
  readonly turn: Effect.Effect<void, AiError.AiError>;
  readonly modelCall: Effect.Effect<void, AiError.AiError>;
  readonly addUsage: (
    usage: Stop.Usage,
  ) => Effect.Effect<void, AiError.AiError>;
  readonly delegation: <A, E, R>(
    effect: Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E | AiError.AiError, R>;
  readonly tool: <A, E, R>(
    effect: Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E, R>;
  readonly toolStream: <A, E, R>(
    stream: Stream.Stream<A, E, R>,
  ) => Stream.Stream<A, E, R>;
  readonly signal: (
    kind: 'steer' | 'cancel',
    text: string,
    index: number,
  ) => Effect.Effect<SignalDecision>;
  readonly remainingMillis: Effect.Effect<number, AiError.AiError>;
  readonly snapshot: Effect.Effect<Snapshot>;
}

const safeInteger = (
  name: keyof Limits,
  value: number,
  minimum: number,
): number => {
  if (!Number.isSafeInteger(value) || value < minimum) {
    throw new RangeError(
      `RunPolicy.${name} must be a safe integer >= ${minimum}`,
    );
  }
  return value;
};

export const make = (limits: Partial<Limits> = {}): Limits => {
  const merged = { ...defaultLimits, ...limits };
  for (const [name, value] of Object.entries(merged)) {
    const key = name as keyof Limits;
    safeInteger(
      key,
      value,
      key === 'maxConcurrentChildren' ||
        key === 'maxToolConcurrency' ||
        key === 'wallClockMillis' ||
        key === 'maxSignalsPerBoundary'
        ? 1
        : 0,
    );
  }
  return Object.freeze(merged);
};

export const error = (exhaustion: Exhaustion): AiError.AiError =>
  new AiError.AiError({
    module: 'AgentRunPolicy',
    method: exhaustion.limit,
    reason: new AiError.ContentPolicyError({
      description:
        `Hard run budget ${exhaustion.limit} exhausted ` +
        `(${exhaustion.used}/${exhaustion.maximum})`,
    }),
  });

export const create = Effect.fn('RunPolicy.create')(function* (
  configured: Limits,
) {
  const limits = make(configured);
  const started = yield* Clock.currentTimeMillis;
  const counters = yield* Ref.make(emptyCounters);
  const children = yield* Semaphore.make(limits.maxConcurrentChildren);
  const tools = yield* Semaphore.make(limits.maxToolConcurrency);

  const checkDeadline = Effect.gen(function* () {
    const now = yield* Clock.currentTimeMillis;
    const remaining = started + limits.wallClockMillis - now;
    if (remaining <= 0) {
      return yield* Effect.fail(
        error({
          limit: 'deadline',
          used: now - started,
          maximum: limits.wallClockMillis,
        }),
      );
    }
    return remaining;
  });

  const increment = (
    field: 'turns' | 'modelCalls' | 'delegatedTasks',
    limit: Limit,
    maximum: number,
  ): Effect.Effect<void, AiError.AiError> =>
    Effect.gen(function* () {
      yield* checkDeadline;
      const next = yield* Ref.modify(counters, (current) => {
        const used = current[field] + 1;
        return [used, { ...current, [field]: used }] as const;
      });
      if (next > maximum) {
        return yield* Effect.fail(error({ limit, used: next, maximum }));
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
        return yield* Effect.fail(
          error({
            limit: 'input_tokens',
            used: next.inputTokens,
            maximum: limits.maxInputTokens,
          }),
        );
      }
      if (next.outputTokens > limits.maxOutputTokens) {
        return yield* Effect.fail(
          error({
            limit: 'output_tokens',
            used: next.outputTokens,
            maximum: limits.maxOutputTokens,
          }),
        );
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
      if (kind === 'cancel') return { accepted: true, bytes };
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
    deadline: started + limits.wallClockMillis,
    turn: increment('turns', 'turns', limits.maxTurns),
    modelCall: increment('modelCalls', 'model_calls', limits.maxModelCalls),
    addUsage,
    delegation,
    tool: (effect) => tools.withPermit(effect),
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
    snapshot: Effect.map(Ref.get(counters), (current) => ({
      turns: current.turns,
      modelCalls: current.modelCalls,
      delegatedTasks: current.delegatedTasks,
      usage: { input: current.inputTokens, output: current.outputTokens },
      steeredBytes: current.steeredBytes,
      limits,
    })),
  } satisfies Runtime;
});

export const clampConcurrency = (
  requested: number | 'unbounded' | undefined,
  maximum: number,
): number =>
  requested === undefined || requested === 'unbounded'
    ? maximum
    : !Number.isFinite(requested)
      ? maximum
      : Math.max(1, Math.min(Math.floor(requested), maximum));

export * as RunPolicy from './run-policy.js';
