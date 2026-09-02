import type { ConversationRecord } from '@sunfall/vesper-log/record';
import { Cause, Effect, Exit, Layer, Ref, Schema, Stream } from 'effect';
import {
  AiError,
  LanguageModel,
  type Prompt,
  type Response,
} from 'effect/unstable/ai';

import type { Agent } from './agent.js';
import * as Failpoint from './internal/failpoint.js';

/** One normalized request observed at Effect's provider seam. */
export interface Request {
  readonly operation: 'generateText' | 'streamText';
  readonly index: number;
  readonly prompt: Prompt.Prompt;
  /** Exact tool definitions handed to the provider, in provider order. */
  readonly toolDefinitions: LanguageModel.ProviderOptions['tools'];
  /** Convenience projection for assertions that only care about names. */
  readonly tools: ReadonlyArray<string>;
  readonly toolChoice: LanguageModel.ProviderOptions['toolChoice'];
}

export interface Options {
  /** Responses for non-streaming calls such as compaction summaries. */
  readonly generate?: ReadonlyArray<GenerateStep> | undefined;
  /** Repeat the final entry after a script is exhausted. Defaults to false. */
  readonly repeatLast?: boolean | undefined;
}

/** Exact encoded output from one fake streaming provider call, or its failure. */
export type StreamStep =
  | ReadonlyArray<Response.StreamPartEncoded>
  | AiError.AiError;

/** Exact encoded output from one fake non-streaming provider call, or its failure. */
export type GenerateStep =
  | ReadonlyArray<Response.PartEncoded>
  | AiError.AiError;

/** A scripted model plus an inspectable record of every provider request. */
export interface Handle {
  readonly layer: Layer.Layer<LanguageModel.LanguageModel>;
  /** Build the same service directly, for policies that select a later model. */
  readonly service: Effect.Effect<LanguageModel.Service>;
  readonly requests: Effect.Effect<ReadonlyArray<Request>>;
  readonly remaining: Effect.Effect<{
    readonly generate: number;
    readonly stream: number;
  }>;
}

const unexpected = (
  operation: Request['operation'],
  index: number,
): AiError.AiError =>
  new AiError.AiError({
    module: 'ScriptedModel',
    method: operation,
    reason: new AiError.InvalidRequestError({
      description: `Unexpected ${operation} call at index ${index}; the script is exhausted`,
    }),
  });

const requestOf = (
  operation: Request['operation'],
  index: number,
  options: LanguageModel.ProviderOptions,
): Request => ({
  operation,
  index,
  prompt: options.prompt,
  toolDefinitions: Array.from(options.tools),
  tools: options.tools.map((tool): string => String(tool.name)),
  toolChoice: options.toolChoice,
});

const at = <A>(
  steps: ReadonlyArray<A>,
  index: number,
  repeatLast: boolean,
): A | undefined =>
  steps[index] ??
  (repeatLast && steps.length > 0 ? steps[steps.length - 1] : undefined);

/**
 * Build a deterministic implementation of Effect's `LanguageModel` seam.
 *
 * Stream and generate scripts have independent cursors because production
 * uses `streamText` for turns and `generateText` for compaction. Scripts fail
 * when exhausted unless `repeatLast` is explicitly enabled.
 */
export const make = (
  turns: ReadonlyArray<StreamStep>,
  options: Options = {},
): Handle => {
  const requests: Request[] = [];
  let generateIndex = 0;
  let streamIndex = 0;
  const generated = options.generate ?? [];
  const repeatLast = options.repeatLast ?? false;

  const service = LanguageModel.make({
    generateText: (providerOptions) =>
      Effect.suspend(() => {
        const index = generateIndex++;
        requests.push(requestOf('generateText', index, providerOptions));
        const step = at(generated, index, repeatLast);
        if (step === undefined) {
          return Effect.fail(unexpected('generateText', index));
        }
        return step instanceof AiError.AiError
          ? Effect.fail(step)
          : Effect.succeed(Array.from(step));
      }),
    streamText: (providerOptions) =>
      Stream.suspend(() => {
        const index = streamIndex++;
        requests.push(requestOf('streamText', index, providerOptions));
        const step = at(turns, index, repeatLast);
        if (step === undefined) {
          return Stream.fail(unexpected('streamText', index));
        }
        return step instanceof AiError.AiError
          ? Stream.fail(step)
          : Stream.fromIterable(step);
      }),
  });
  const layer = Layer.effect(LanguageModel.LanguageModel, service);

  return {
    layer,
    service,
    requests: Effect.sync(() => Array.from(requests)),
    remaining: Effect.sync(() => ({
      generate: Math.max(0, generated.length - generateIndex),
      stream: Math.max(0, turns.length - streamIndex),
    })),
  };
};

// ## Chaos: proving recovery converges
//
// `Chaos.converge` drives one scripted conversation once per
// `Failpoint.Location`, with a crash armed at that location, then reopens the
// same conversation with the crash disarmed and lets it run to completion. It
// asserts the recovered outcome equals a crash-free baseline and that the
// durable history it left behind is well-formed, and reports each location's
// status rather than throwing at the first bad one, so a test can see every
// location's fate in one run.
//
// The scenario itself — the agent, its scripted model, its tools, whatever
// interceptor resolves an indeterminate call — is entirely the caller's,
// because only the caller knows what "one more model call" or "the handler
// ran again" means for its own tools. `Chaos.converge` only needs three
// callbacks into that scenario per attempt: run it to a terminal result,
// count how many times each tool call id's handler actually executed, and
// read back the records it left in the log.

/** One isolated run of the caller's scenario, callable more than once against
 * the same durable conversation — {@link converge} calls `drive` once to
 * produce the crash, then again, after disarming, to recover. */
export interface ChaosAttempt {
  /** Continue the conversation from wherever its durable history currently
   * is, through to a terminal `Agent.Result` — including resolving any
   * approval the scenario suspends on. */
  readonly drive: Effect.Effect<Agent.Result>;
  /** How many times each tool call id's handler has executed so far,
   * cumulative across every `drive` call made against this attempt. */
  readonly executionCounts: Effect.Effect<ReadonlyMap<string, number>>;
  /** Every record currently in this attempt's conversation, read fresh. */
  readonly records: Effect.Effect<ReadonlyArray<ConversationRecord.Envelope>>;
}

export interface ChaosOptions {
  /**
   * Build one attempt: a fresh conversation id and fresh tool-call counters,
   * wired to the given `Failpoint` layer. Called once for the crash-free
   * baseline (`conversationId` ending `-baseline`, an inert `Failpoint`
   * layer) and once per location under test (`conversationId` naming the
   * location, {@link Failpoint.layerTest} backing that location's crash).
   * The returned `Effect` must already have discharged every other
   * requirement — `LogStore`, `Crypto`, the scripted model, tool
   * implementations — so `drive`/`executionCounts`/`records` need nothing
   * further.
   */
  readonly attempt: (
    conversationId: string,
    failpointLayer: Layer.Layer<never>,
  ) => Effect.Effect<ChaosAttempt>;
  /** Locations to check. Defaults to every {@link Failpoint.Location}. */
  readonly locations?: ReadonlyArray<Failpoint.Location> | undefined;
  /**
   * Tool call ids inside this scenario's `ToolStarted`..`ToolOutcome`
   * window — the only ids {@link converge} permits to show more than one
   * execution, and only because that window's own indeterminate-call
   * reconciliation genuinely re-enters a handler on an explicit `Retry`;
   * every other call id executing more than once is a replay bug.
   */
  readonly windowedCallIds?: ReadonlyArray<string> | undefined;
}

/** One location's outcome from {@link converge}. */
export type ChaosStatus =
  | { readonly _tag: 'converged' }
  | { readonly _tag: 'not-triggered' }
  | { readonly _tag: 'failed'; readonly reason: string };

export interface ChaosLocationResult {
  readonly location: Failpoint.Location;
  readonly status: ChaosStatus;
}

export interface ChaosReport {
  readonly results: ReadonlyArray<ChaosLocationResult>;
}

const isFailpointCrash = Schema.is(Failpoint.FailpointCrash);

const crashLocation = (
  exit: Exit.Exit<unknown>,
): Failpoint.Location | undefined => {
  if (Exit.isSuccess(exit)) {
    return undefined;
  }
  const squashed = Cause.squash(exit.cause);
  return isFailpointCrash(squashed) ? squashed.location : undefined;
};

const bump = (map: Map<string, number>, key: string): void => {
  map.set(key, (map.get(key) ?? 0) + 1);
};

/**
 * Plain recursive structural equality for comparing two `Agent.Result`
 * values. Deliberately not `JSON.stringify` (whose key ordering is not a
 * promise this comparison should depend on) and not `Equal.equals` (which
 * falls back to reference equality for plain object literals, not the
 * structural values a result actually is).
 */
const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === 'object' && value !== null && !Array.isArray(value);

const deepEqual = (a: unknown, b: unknown): boolean => {
  if (a === b) {
    return true;
  }
  if (Array.isArray(a) || Array.isArray(b)) {
    return (
      Array.isArray(a) &&
      Array.isArray(b) &&
      a.length === b.length &&
      a.every((item, index) => deepEqual(item, b[index]))
    );
  }
  if (isRecord(a) && isRecord(b)) {
    const keys = new Set([...Object.keys(a), ...Object.keys(b)]);
    return Array.from(keys).every((key) => deepEqual(a[key], b[key]));
  }
  return false;
};

/** Describes the top-level fields where two `Agent.Result` values differ, or
 * `undefined` when they match. Used only to build a chaos report's failure
 * reason. */
const describeMismatch = (
  baseline: Agent.Result,
  recovered: Agent.Result,
): string | undefined => {
  if (deepEqual(baseline, recovered)) {
    return undefined;
  }
  const left = new Map<string, unknown>(Object.entries(baseline));
  const right = new Map<string, unknown>(Object.entries(recovered));
  const keys = new Set([...left.keys(), ...right.keys()]);
  const diffs = Array.from(keys).flatMap((key) =>
    deepEqual(left.get(key), right.get(key))
      ? []
      : [
          `${key}: baseline=${String(left.get(key))} recovered=${String(right.get(key))}`,
        ],
  );
  return diffs.join('; ');
};

const wellFormed = (
  records: ReadonlyArray<ConversationRecord.Envelope>,
): string | undefined => {
  const started = new Map<string, number>();
  const settled = new Map<string, number>();
  const suspended = new Map<string, number>();
  const completed = new Map<string, number>();
  for (const { record } of records) {
    switch (record._tag) {
      case 'ToolCall':
        bump(started, record.id);
        break;
      case 'ToolOutcome':
        bump(settled, record.id);
        break;
      case 'ToolSuspended':
        bump(suspended, record.id);
        break;
      case 'ToolWaitCompleted':
        bump(completed, record.id);
        break;
      case 'BranchedFrom':
      case 'ChildSession':
      case 'CodeStateCheckpoint':
      case 'Compacted':
      case 'Completed':
      case 'RunSettled':
      case 'RunStarted':
      case 'Signal':
      case 'SignalReceived':
      case 'StateCheckpoint':
      case 'Text':
      case 'ToolResumed':
      case 'ToolStarted':
      case 'ToolWaitRestarted':
      case 'TurnFinished':
        break;
      default:
        break;
    }
  }
  for (const id of started.keys()) {
    const outcomes = settled.get(id) ?? 0;
    if (outcomes !== 1) {
      return `tool call ${id} has ${String(outcomes)} ToolOutcome records, expected exactly 1`;
    }
  }
  for (const [token, count] of suspended) {
    const resolutions = completed.get(token) ?? 0;
    if (count > 0 && resolutions !== 1) {
      return `suspended wait ${token} has ${String(resolutions)} ToolWaitCompleted records, expected exactly 1`;
    }
  }
  return undefined;
};

/** Run {@link ChaosOptions.attempt}'s scenario once per location, crash it
 * there, reopen and recover, and report whether recovery converged. */
export const converge = (options: ChaosOptions): Effect.Effect<ChaosReport> =>
  Effect.gen(function* () {
    const locations = options.locations ?? Failpoint.locations;
    const windowed = new Set(options.windowedCallIds ?? []);

    const baseline = yield* options.attempt(
      'chaos-baseline',
      Failpoint.layerNoop,
    );
    const baselineResult = yield* baseline.drive;

    const results: Array<ChaosLocationResult> = [];
    for (const location of locations) {
      const handlerRef = yield* Ref.make<Failpoint.Handler>(
        Failpoint.crashAt(location),
      );
      const attempt = yield* options.attempt(
        `chaos-${location}`,
        Failpoint.layerTest(handlerRef),
      );
      const crashExit = yield* Effect.exit(attempt.drive);
      const triggeredAt = crashLocation(crashExit);
      if (triggeredAt === undefined) {
        results.push({ location, status: { _tag: 'not-triggered' } });
        continue;
      }

      yield* Ref.set(handlerRef, Failpoint.passthrough);
      const recoveredExit = yield* Effect.exit(attempt.drive);
      if (Exit.isFailure(recoveredExit)) {
        results.push({
          location,
          status: {
            _tag: 'failed',
            reason: `recovery did not settle: ${Cause.pretty(recoveredExit.cause)}`,
          },
        });
        continue;
      }

      const mismatch = describeMismatch(baselineResult, recoveredExit.value);
      if (mismatch !== undefined) {
        results.push({
          location,
          status: {
            _tag: 'failed',
            reason: `recovered result diverged from the crash-free baseline: ${mismatch}`,
          },
        });
        continue;
      }

      const records = yield* attempt.records;
      const malformed = wellFormed(records);
      if (malformed !== undefined) {
        results.push({
          location,
          status: { _tag: 'failed', reason: `malformed history: ${malformed}` },
        });
        continue;
      }

      const counts = yield* attempt.executionCounts;
      const replayed = Array.from(counts).filter(
        ([id, count]) => count > 1 && !windowed.has(id),
      );
      if (replayed.length > 0) {
        results.push({
          location,
          status: {
            _tag: 'failed',
            reason: `tool call(s) executed more than once outside the ToolStarted..ToolOutcome window: ${replayed
              .map(([id, count]) => `${id}×${String(count)}`)
              .join(', ')}`,
          },
        });
        continue;
      }

      results.push({ location, status: { _tag: 'converged' } });
    }

    return { results };
  });

export * as ScriptedModel from './testing.js';
export * as Chaos from './testing.js';
