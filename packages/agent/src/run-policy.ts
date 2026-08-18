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
    return `Hard run budget ${this.limit} exhausted (${this.used}/${this.maximum})`;
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

export * as RunPolicy from './run-policy.js';
