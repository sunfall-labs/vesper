import type { Effect } from 'effect';
import type { AiError, Prompt } from 'effect/unstable/ai';

import type { Stop } from './stop.js';

// Three named places in the loop where something other than the loop gets a
// say, and exactly what it may say at each.
//
// ## Why this is not `(Effect) => Effect`
//
// The common design is a single execution interceptor that wraps a step with
// the ability to intervene. The obvious Effect translation is a service
// holding a function from effect to effect, applied wherever the loop does
// something interesting. That translation is rejected here.
//
// A `(Effect) => Effect` has no name and no contract. Given one, a reader of
// the loop cannot tell whether it may replace the model's answer, re-run a
// tool, swallow a cancellation, or return a different result type — it may do
// all of those, so every seam has to be read as if it does. The type says
// nothing, so the documentation has to say everything, and documentation is
// not checked.
//
// So the seams are named, and each one's type is the smallest thing that
// admits what it is for:
//
// | seam               | observe | change the input | answer instead | fail |
// | ------------------ | ------- | ---------------- | -------------- | ---- |
// | `beforeTurn`       | yes     | yes              | no             | yes  |
// | `beforeModelCall`  | yes     | no               | no             | yes  |
// | `beforeToolCall`   | yes     | no               | yes            | yes  |
//
// Each row is a decision, not an omission:
//
// **`beforeTurn` may rewrite the input and may not end the run.** Rewriting is
// the whole point — a budget reminder, retrieved context, a policy preamble —
// and the input is the one part of the turn that is a value the loop is
// holding rather than history inside `Chat`. Ending the run cleanly is
// `Stop.StopCondition`'s job and is already a public option; a second way to
// do it would report `success` in `RunSettled` for a run nobody completed, and
// saying otherwise would need a `ConversationRecord` case that `@sunfall/vesper-log`
// does not have. An interceptor that must stop a run *fails*, which is
// recorded as a failure, because that is what it is.
//
// **`beforeModelCall` may only observe or refuse.** It cannot rewrite the
// prompt: by the time the provider is called, the prompt is `Chat`'s history
// plus the turn's input, and only the second half is visible here — a seam
// that appeared to edit "the prompt" while reaching one message of it would be
// a trap. It cannot answer instead either, because answering means
// synthesising `Response.StreamPart`s the provider never sent, inventing ids
// and usage, and bypassing the history `Chat` keeps. Its return type is
// `Effect<void>`, which is precisely "look, or refuse".
//
// **`beforeToolCall` may answer instead and may not rewrite the parameters.**
// Answering is what an approval gate, a denylist, or a dry-run mode needs, and
// it costs nothing to model: a substituted result travels the same path a real
// one does. Rewriting parameters is excluded because it would make three
// things disagree — what the model asked for, what the log records it asked
// for, and what actually ran — and because the parameters arriving here are
// the provider's *encoded* form, so a rewrite would have to be re-validated
// against the tool's parameter schema, which this seam has no general way to
// do.
//
// ## What an interceptor is not told
//
// There is no `afterX`. Everything after a seam is already observable: the
// event stream carries every part and every turn boundary, and a recording run
// writes all of it down. Interception exists for the part observation cannot
// do, and adding a mirror of the event stream here would be a second, worse
// way to watch a run.
//
// ## Scope
//
// An interceptor belongs to the agent it was attached to. A subagent is its
// own agent and its own loop, so a parent's interceptor does not run inside a
// child's turns — attach one to the child if that is what you want. What the
// parent *does* see is the delegation itself: a subagent compiles to a tool,
// so `beforeToolCall` fires for `task_<child>` like any other call.

/**
 * Which run a seam is firing in.
 *
 * @category models
 * @since 0.1.0
 */
export interface Run {
  /** The agent whose loop this is. */
  readonly agent: string;
  /**
   * The conversation being recorded into, or `undefined` when the run is not
   * recording.
   *
   * Present rather than assumed, because interception is independent of
   * logging: an agent may be intercepted without `recordingTo`, and an
   * interceptor keyed on a conversation has to be able to tell.
   */
  readonly conversationId: string | undefined;
}

/**
 * A turn is about to start.
 *
 * @category models
 * @since 0.1.0
 */
export interface TurnContext extends Run {
  /** 1 for the first turn. */
  readonly step: number;
  /** Totals accumulated by the turns that already finished. */
  readonly usage: Stop.Usage;
  /**
   * What this turn will add to the conversation before the model sees it.
   *
   * Normalised to a `Prompt` so an interceptor does not have to re-implement
   * `RawInput`'s three shapes to find out what is being said. Empty on every
   * turn after the first unless a steer arrived.
   */
  readonly input: Prompt.Prompt;
}

/**
 * Why the provider is being called.
 *
 * A turn calls the provider once, and twice when the first call overflowed the
 * context window and compaction retried it. The distinction is visible because
 * an interceptor that counts calls, budgets tokens, or rate-limits has to
 * count both, while one that logs "the model was asked" usually wants to know
 * the second is a retry.
 *
 * @category models
 * @since 0.1.0
 */
export type Attempt = 'initial' | 'after-compaction';

/**
 * The provider is about to be called.
 *
 * @category models
 * @since 0.1.0
 */
export interface ModelCallContext extends Run {
  readonly step: number;
  readonly attempt: Attempt;
}

/**
 * A tool is about to be dispatched.
 *
 * @category models
 * @since 0.1.0
 */
export interface ToolCallContext extends Run {
  readonly name: string;
  /**
   * The provider-assigned id for this call, when there is one.
   *
   * Optional because `Toolkit.WithHandler['handle']` makes it optional; every
   * provider in practice supplies it.
   */
  readonly toolCallId: string | undefined;
  /**
   * The parameters as dispatch received them — the provider's encoded form,
   * before the tool's parameter schema has decoded them.
   */
  readonly params: unknown;
}

/**
 * What an interceptor decided about a turn.
 *
 * @category models
 * @since 0.1.0
 */
export type TurnDecision =
  | { readonly _tag: 'Proceed' }
  | { readonly _tag: 'ProceedWith'; readonly input: Prompt.RawInput };

/**
 * Start the turn with the input the loop already had.
 *
 * @category constructors
 * @since 0.1.0
 */
export const proceed: TurnDecision = { _tag: 'Proceed' };

/**
 * Start the turn with different input.
 *
 * Replaces rather than appends. An interceptor that means "and also say this"
 * has the original in {@link TurnContext.input} and can build the union it
 * wants; a seam that always appended could not express "say this instead".
 *
 * @category constructors
 * @since 0.1.0
 */
export const proceedWith = (input: Prompt.RawInput): TurnDecision => ({
  _tag: 'ProceedWith',
  input,
});

/**
 * What an interceptor decided about a tool call.
 *
 * @category models
 * @since 0.1.0
 */
export type ToolDecision =
  | { readonly _tag: 'Dispatch' }
  | {
      readonly _tag: 'Answer';
      readonly result: unknown;
      readonly isFailure: boolean;
    };

/**
 * Run the tool.
 *
 * @category constructors
 * @since 0.1.0
 */
export const dispatch: ToolDecision = { _tag: 'Dispatch' };

/**
 * Do not run the tool; this is its result.
 *
 * `result` is the **encoded** form — what a tool-result message puts in front
 * of the model, and what a recording run writes into the log. It is not
 * validated against the tool's success schema, because the cases this exists
 * for (a refusal, a canned dry-run answer) usually cannot satisfy it. The live
 * event stream's decoded half is produced by running it back through the
 * tool's own codec and falling back to this value when it does not fit, which
 * is exactly what the log-recovery path does with a stored result.
 *
 * @category constructors
 * @since 0.1.0
 */
export const answer = (result: unknown): ToolDecision => ({
  _tag: 'Answer',
  result,
  isFailure: false,
});

/**
 * Do not run the tool; this is its result, and it is a failure.
 *
 * The shape an approval gate wants. A model shown a failed tool result knows
 * the call did not happen and can say so or try something else, where a
 * success-shaped refusal reads as though the work was done.
 *
 * @category constructors
 * @since 0.1.0
 */
export const refuse = (result: unknown): ToolDecision => ({
  _tag: 'Answer',
  result,
  isFailure: true,
});

/**
 * Something with an opinion about what the loop is about to do.
 *
 * Every seam is optional, so an interceptor that cares about one thing
 * declares one thing and the other two seams cost nothing — the loop checks
 * for the property and skips the effect entirely when it is absent.
 *
 * `R` is what the seams need from the context. Attaching an interceptor with
 * `agent.intercepting(...)` adds exactly `R` to the agent's requirement
 * channel and nothing else, so a policy check that reads a database is
 * expressible without every un-intercepted agent inheriting the database.
 *
 * Failures are `AiError.AiError` because that is the loop's error channel; an
 * interceptor with a richer error type maps it before it gets here, the same
 * way a tool handler does.
 *
 * @category models
 * @since 0.1.0
 */
export interface Interceptor<R = never> {
  /**
   * Before a turn begins, and before its `TurnStarted` event.
   *
   * Fires once per turn, including the turn a steer produced. Failing here
   * ends the run with that error and no `TurnStarted` for the turn that never
   * ran.
   */
  readonly beforeTurn?: (
    context: TurnContext,
  ) => Effect.Effect<TurnDecision, AiError.AiError, R>;

  /**
   * Before the provider is called, and once more if compaction retries it.
   *
   * Observe or refuse. See the note at the top of this file for why this seam
   * cannot rewrite the prompt or answer in the model's place.
   */
  readonly beforeModelCall?: (
    context: ModelCallContext,
  ) => Effect.Effect<void, AiError.AiError, R>;

  /**
   * Before a tool runs.
   *
   * Does **not** fire for a call the conversation log is answering from a
   * crashed earlier run — see `dispatch.ts` for why recovery outranks
   * interception and what that means for an approval gate.
   */
  readonly beforeToolCall?: (
    context: ToolCallContext,
  ) => Effect.Effect<ToolDecision, AiError.AiError, R>;
}

export * as Interception from './interception.js';
