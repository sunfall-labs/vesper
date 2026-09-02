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
  'maxCostMicrousd',
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

/**
 * `RunPolicy.make` (and therefore `Agent.make`, which funnels every
 * definition's `runPolicy` through it) was given `maxCostMicrousd` without a
 * `costModel` to charge usage against.
 *
 * A construction-time failure rather than a silently inert limit: a budget
 * nobody can compute is not a smaller budget, it is a limit that never fires,
 * and that is worth failing loudly for rather than discovering in production
 * once a run neither spends nor stops.
 */
export class CostModelRequiredError extends Schema.TaggedError<CostModelRequiredError>(
  '@sunfall/vesper-agent/RunPolicy/CostModelRequiredError',
)('CostModelRequiredError', {
  message: Schema.String,
}) {}

/**
 * Per-token pricing used to charge model usage against `Limits.maxCostMicrousd`.
 *
 * Rates are micro-USD (1e-6 USD) per million tokens, matching how providers
 * publish pricing without forcing floating-point dollars through a budget
 * ledger. `cachedInputMicrousdPerMillionTokens` is optional; when absent,
 * cached input tokens are charged at `inputMicrousdPerMillionTokens`, the same
 * rate as an uncached prompt token.
 */
export interface CostModel {
  readonly inputMicrousdPerMillionTokens: number;
  readonly outputMicrousdPerMillionTokens: number;
  readonly cachedInputMicrousdPerMillionTokens?: number;
}

/** How a run responds once a soft-fallback-eligible hard budget is exhausted. */
export type ExhaustionMode = 'fail' | 'final-answer';

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
  /**
   * Cumulative cost budget in micro-USD (1e-6 USD), charged from provider
   * usage after each model call using {@link costModel}. Unset — the default
   * — means no cost ceiling is enforced and usage is never priced. Setting
   * this without `costModel` fails at `RunPolicy.make`/`Agent.make` with
   * {@link CostModelRequiredError} rather than silently never firing.
   *
   * Checked the same way {@link maxInputTokens} is: after usage is known, so
   * a run can overshoot by up to one turn's cost before the check after it
   * fails the run.
   */
  readonly maxCostMicrousd?: number;
  /**
   * Pricing used to charge model usage against {@link maxCostMicrousd}.
   * Required when `maxCostMicrousd` is set; has no effect otherwise.
   */
  readonly costModel?: CostModel;
  /**
   * What happens when a soft-fallback-eligible hard budget — `maxTurns`,
   * `maxModelCalls`, `maxInputTokens`, `maxOutputTokens`, `maxCostMicrousd`,
   * or `maxDelegatedTasks` — is exhausted.
   *
   * `'fail'`, the default, preserves today's behaviour: the run fails with
   * {@link RunPolicyExhausted}. `'final-answer'` instead lets the run make
   * exactly one more model call with no tools available and a short appended
   * instruction that the budget is spent, and settles on that call's output
   * — see `internal/loop.ts` and the agent README's "Run policy and budgets"
   * section. `wallClockMillis`, `maxToolConcurrency`, and the signal limits
   * are never eligible: a deadline still fails a run outright even in
   * `'final-answer'` mode, including the final-answer call itself.
   */
  readonly onExhaustion?: ExhaustionMode;
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
  onExhaustion: 'fail',
});

const safeInteger = (name: string, value: number, minimum: number): number => {
  if (!Number.isSafeInteger(value) || value < minimum) {
    throw new RangeError(
      `RunPolicy.${name} must be a safe integer >= ${String(minimum)}`,
    );
  }
  return value;
};

const finiteNonNegative = (name: string, value: number): number => {
  if (!Number.isFinite(value) || value < 0) {
    throw new RangeError(`RunPolicy.${name} must be a finite number >= 0`);
  }
  return value;
};

/**
 * Fields validated as plain non-negative safe integers with a fixed minimum.
 * `maxCostMicrousd`, `costModel`, and `onExhaustion` are validated separately
 * below — the first is optional with no fixed minimum-only shape, the second
 * is not a number, and the third is not a number either.
 */
type NumericLimitName = Exclude<
  keyof Limits,
  'maxCostMicrousd' | 'costModel' | 'onExhaustion'
>;

const minimumByLimit: { readonly [Name in NumericLimitName]: number } = {
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

/** A user-defined guard rather than a cast: the only sound way back from a
 * runtime `string` key to the literal union `Object.keys` cannot express. */
const isNumericLimitName = (name: string): name is NumericLimitName =>
  Object.hasOwn(minimumByLimit, name);

const knownLimitNames = new Set<string>([
  ...Object.keys(minimumByLimit),
  'maxCostMicrousd',
  'costModel',
  'onExhaustion',
]);

const validateCostModel = (costModel: CostModel): void => {
  finiteNonNegative(
    'costModel.inputMicrousdPerMillionTokens',
    costModel.inputMicrousdPerMillionTokens,
  );
  finiteNonNegative(
    'costModel.outputMicrousdPerMillionTokens',
    costModel.outputMicrousdPerMillionTokens,
  );
  if (costModel.cachedInputMicrousdPerMillionTokens !== undefined) {
    finiteNonNegative(
      'costModel.cachedInputMicrousdPerMillionTokens',
      costModel.cachedInputMicrousdPerMillionTokens,
    );
  }
};

/**
 * Price one call's model usage against a {@link CostModel}, in whole
 * micro-USD.
 *
 * Pure and exported so the runtime's enforcement path and the loop's local
 * usage projection compute the identical figure from the identical inputs,
 * rather than one recomputing what the other already knows. `cachedInput` is
 * a subset of `input`, clamped so a caller's bookkeeping mistake cannot price
 * more cached tokens than were reported as input at all; the remainder is
 * charged as ordinary uncached input.
 */
export const costOf = (
  costModel: CostModel,
  usage: {
    readonly input: number;
    readonly output: number;
    readonly cachedInput?: number;
  },
): number => {
  const cachedInput = Math.min(usage.cachedInput ?? 0, usage.input);
  const uncachedInput = usage.input - cachedInput;
  const cachedRate =
    costModel.cachedInputMicrousdPerMillionTokens ??
    costModel.inputMicrousdPerMillionTokens;
  const microUsd =
    (uncachedInput * costModel.inputMicrousdPerMillionTokens) / 1_000_000 +
    (cachedInput * cachedRate) / 1_000_000 +
    (usage.output * costModel.outputMicrousdPerMillionTokens) / 1_000_000;
  return Math.round(microUsd);
};

export const make = (limits: Partial<Limits> = {}): Limits => {
  const merged = { ...defaultLimits, ...limits };
  for (const name of Object.keys(merged)) {
    if (!knownLimitNames.has(name)) {
      throw new RangeError(`RunPolicy.${name} is not a recognized limit`);
    }
    if (isNumericLimitName(name)) {
      safeInteger(name, merged[name], minimumByLimit[name]);
    }
  }
  if (merged.maxCostMicrousd !== undefined) {
    safeInteger('maxCostMicrousd', merged.maxCostMicrousd, 0);
  }
  if (merged.costModel !== undefined) {
    validateCostModel(merged.costModel);
  }
  if (merged.maxCostMicrousd !== undefined && merged.costModel === undefined) {
    throw new CostModelRequiredError({
      message:
        'RunPolicy.maxCostMicrousd requires RunPolicy.costModel to charge model usage against; set costModel or remove maxCostMicrousd.',
    });
  }
  if (
    merged.onExhaustion !== 'fail' &&
    merged.onExhaustion !== 'final-answer'
  ) {
    throw new RangeError(
      "RunPolicy.onExhaustion must be 'fail' or 'final-answer'",
    );
  }
  return Object.freeze(merged);
};

export * as RunPolicy from './run-policy.js';
