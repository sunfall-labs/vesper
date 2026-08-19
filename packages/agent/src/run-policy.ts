import { Schema } from 'effect';

export const Limit = Schema.Literals([
  'turns',
  'model_calls',
  'delegated_tasks',
  'deadline',
  'input_tokens',
  'output_tokens',
  'signal_bytes',
  'signals_per_boundary',
  'steered_bytes',
]);
export type Limit = typeof Limit.Type;

/** A non-overridable run budget was exhausted. */
export class RunPolicyExhausted extends Schema.TaggedError<RunPolicyExhausted>(
  '@sunfall/vesper-agent/RunPolicyExhausted',
)('RunPolicyExhausted', {
  limit: Limit,
  used: Schema.Natural,
  maximum: Schema.Natural,
}) {
  override get message(): string {
    return `Hard run budget ${this.limit} exhausted (${String(this.used)}/${String(this.maximum)})`;
  }
}

/** Non-overridable limits shared by a root run and every descendant loop. */
export interface Limits {
  readonly maxTurns: number;
  readonly maxModelCalls: number;
  readonly maxDelegatedTasks: number;
  readonly maxDelegationDepth: number;
  readonly maxConcurrentChildren: number;
  readonly maxToolConcurrency: number;
  readonly wallClockMillis: number;
  /**
   * Cumulative input tokens a run may spend, checked after each turn's usage
   * is known rather than before it is requested. There is no way to ask a
   * provider "would this turn fit the budget" before sending it, so a run can
   * overshoot this ceiling by up to one turn's input before the next check
   * fails it — the limit bounds spend, it does not cap any single request.
   */
  readonly maxInputTokens: number;
  /**
   * Cumulative output tokens a run may spend, checked after each turn's usage
   * is known rather than before it is requested. One large response can push
   * the total past this ceiling before the check after it fails the run, for
   * the same reason {@link maxInputTokens} can overshoot: usage is only known
   * once the provider reports it.
   */
  readonly maxOutputTokens: number;
  readonly maxSignalBytes: number;
  readonly maxSignalsPerBoundary: number;
  readonly maxSteeredBytes: number;
}

/** Explicit production-safe default used unless the application supplies one. */
export const defaultLimits: Limits = Object.freeze({
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
});

const safeInteger = (name: string, value: number, minimum: number): number => {
  if (!Number.isSafeInteger(value) || value < minimum) {
    throw new RangeError(
      `RunPolicy.${name} must be a safe integer >= ${String(minimum)}`,
    );
  }
  return value;
};

const minimumByLimit: Readonly<Record<string, number>> & {
  readonly [Name in keyof Limits]: number;
} = {
  maxTurns: 0,
  maxModelCalls: 0,
  maxDelegatedTasks: 0,
  maxDelegationDepth: 0,
  maxConcurrentChildren: 1,
  maxToolConcurrency: 1,
  wallClockMillis: 1,
  maxInputTokens: 0,
  maxOutputTokens: 0,
  maxSignalBytes: 0,
  maxSignalsPerBoundary: 1,
  maxSteeredBytes: 0,
};

export const make = (limits: Partial<Limits> = {}): Limits => {
  const merged = { ...defaultLimits, ...limits };
  for (const [name, value] of Object.entries(merged)) {
    const minimum = minimumByLimit[name];
    if (minimum === undefined) {
      throw new RangeError(`RunPolicy.${name} is not a recognized limit`);
    }
    safeInteger(name, value, minimum);
  }
  return Object.freeze(merged);
};

export * as RunPolicy from './run-policy.js';
