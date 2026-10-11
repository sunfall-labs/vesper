import { Context, Effect, Option, type Schema } from 'effect';
import type { Tool } from 'effect/ai';

import { Interception } from './interception.js';

// Recovery's default is strict: a `StepStarted` with no `StepSettled` is
// indeterminate, the handler may already have committed externally, and with no
// `onIndeterminateToolCall` configured the resume fails (`dispatch.ts`). This
// module is the explicit, one-line opt-out of that strictness for toolkits
// whose tools can say what re-execution costs.
//
// The declaration lives on the tool, because only the tool's author knows
// whether running it twice is harmless; the policy lives on the agent, because
// only the agent's owner decides to resume without a human reconciling each
// call. Neither alone changes recovery: an unannotated tool is never retried,
// and an annotated tool is never retried unless a policy that reads the
// annotation is attached.

/** Tool annotation: re-executing this tool after an unrecorded start is safe. */
export class Safe extends Context.Service<Safe, boolean>()(
  '@sunfall/vesper-agent/Replay/Safe',
) {}

/**
 * Declare a tool idempotent to re-execute.
 *
 * Mark a tool only when running its handler a second time, with the same
 * parameters, after a first attempt of unknown extent, leaves the world as one
 * run would. Reads, upserts keyed by an idempotency key, and pure computation
 * qualify; "send an email" does not.
 */
export const safe = <
  Name extends string,
  Config extends {
    readonly parameters: Schema.Constraint;
    readonly success: Schema.Constraint;
    readonly failure: Schema.Constraint;
    readonly failureMode: Tool.FailureMode;
  },
  R,
>(
  tool: Tool.Tool<Name, Config, R>,
): Tool.Tool<Name, Config, R> => tool.annotate(Safe, true);

/** True when the tool was declared replay-safe with {@link safe}. */
export const isSafe = (tool: Tool.Any | undefined): boolean =>
  tool !== undefined &&
  Option.getOrElse(Context.getOption(tool.annotations, Safe), () => false);

/**
 * The failure a model sees for a call that may have run, in Effect's native
 * `Tool.ExecutionFailure` shape and the wording style `LanguageModel` uses for
 * the calls it interrupts itself.
 */
export const interruptedFailure = (
  reason = 'Tool call execution was interrupted before it recorded a result; it may or may not have taken effect',
): { readonly type: 'execution-interrupted'; readonly reason: string } => ({
  type: 'execution-interrupted',
  reason,
});

/**
 * Answer an indeterminate call with an `execution-interrupted` failure.
 *
 * Recorded as an ordinary failed `StepSettled` and decoded through the tool's
 * failure result schema, which always includes `ExecutionFailure`. The handler
 * does not run and the turn continues, so the model can report or work around
 * the gap.
 */
export const interrupted = (
  reason?: string,
): Interception.IndeterminateToolDecision =>
  Interception.reconcileFailure(interruptedFailure(reason));

/**
 * Retry replay-safe tools; answer every other indeterminate call with
 * {@link interrupted}.
 *
 * Usable directly as `onIndeterminateToolCall`, or as the fallback inside a
 * hand-written one that handles some tools itself.
 */
export const onIndeterminateToolCall = (
  context: Interception.IndeterminateToolCallContext,
): Effect.Effect<Interception.IndeterminateToolDecision> =>
  Effect.succeed(isSafe(context.tool) ? Interception.retry : interrupted());

/**
 * The one-line opt-in: `agent.intercepting(Replay.policy)`.
 *
 * Survives a crash without a human in the loop: replay-safe tools run again,
 * everything else becomes an interrupted result the model can see. Compose
 * with other seams via `Interception.compose`; this interceptor decides every
 * indeterminate call, so it must come first there.
 */
export const policy = {
  onIndeterminateToolCall,
} satisfies Interception.Interceptor;

export * as Replay from './replay.js';
