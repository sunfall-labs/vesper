import { Effect, Ref } from 'effect';
import { AiError, Chat, LanguageModel, Prompt } from 'effect/unstable/ai';

import { Compaction } from '../compaction.js';
import { ContextWindow } from '../context-window.js';
import * as Observability from './observability.js';

// Compaction: replace old history with a summary when the conversation
// outgrows the context window.
//
// Two things about this are easy to get wrong.
//
// First, compaction is itself a model call, and it rewrites `Chat`'s history
// in place. That rewrite has to be **reported**, because it is the one thing a
// run does that a later reconstruction cannot infer from the records: every
// other record adds to a conversation, and this one replaces part of it. So
// `compact` returns what it did rather than whether it did anything, the loop
// turns that into an `AgentEvents.Lifecycle` event, and the log sink turns
// that into a `Compacted` record. Before that chain existed a resumed
// conversation was rebuilt from the full record set, came back longer than the
// run it resumed, and compacted again immediately.
//
// Second, the trigger. Counting tokens exactly requires the provider's
// tokenizer, and the estimate is always a guess. So there are two triggers:
// a proactive one from an estimate, and a reactive one when the provider
// rejects the request outright. The reactive path is the one that actually
// saves a run, because the estimate is wrong more often than anyone likes.
//
// The estimate, the trigger, and the summarizer's system prompt are not
// written here any more. They are read from `ContextWindow.Service`, whose
// default is the four-characters-per-token guess this file used to hold and
// whose usage-anchored implementation lives in `context-window.ts`.
// What stays here is where to cut, that the system message survives, and that
// the rewrite is reported.
//
// ## Why context-window is a separate module
//
// `context-window.ts` is not the other half of this module; it is the
// *contract* this module consumes. Everything in it — `Heuristics`, `pure`,
// `Service`, `TurnUsage` — exists to be implemented by somebody else, and
// `ContextWindow.usageAnchored` provides a richer value of that shape. This
// file is the sole consumer, and none of it is
// implementable by a provider: where to cut a history, that the agent's system
// message survives the cut, and that the rewrite is announced are decisions
// this package has to keep.
//
// Folding them together would put a published extension point in the same file
// as the policy that reads it, so an implementor would import the mechanism to
// get at the interface. The published compaction module exposes policy while
// this implementation remains private to the loop.

/**
 * Rough token estimate for a prompt, with no provider involved.
 *
 * Re-exported from {@link ContextWindow.estimateTokens} so the name callers
 * already use keeps working, and because the cut heuristic below genuinely
 * wants the dependency-free version: it measures single messages, where there
 * is no reported usage to anchor to and nothing for a richer estimator to
 * improve on.
 */
export const estimateTokens = ContextWindow.estimateTokens;

/**
 * True when the conversation is close enough to the ceiling to compact.
 *
 * An `Effect` rather than a plain predicate because the answer is the
 * installed heuristics' to give, not this module's. The requirement channel
 * does not grow: `ContextWindow.Service` is a `Context.Reference`, so this is
 * `Effect<boolean>` and callers are unaffected.
 *
 * `usage` is what the provider reported for the most recent completed turn.
 * Passing it is the difference between an estimate anchored on the provider's
 * own count and a character count of the whole conversation.
 */
export const shouldCompact = (
  prompt: Prompt.Prompt,
  contextWindow: number,
  policy: Compaction.Policy = Compaction.defaultPolicy,
  usage?: ContextWindow.TurnUsage | undefined,
): Effect.Effect<boolean> =>
  Effect.map(ContextWindow.Service, (heuristics) =>
    heuristics.shouldCompact(
      heuristics.estimate(prompt, usage).tokens,
      contextWindow,
      policy,
    ),
  );

/**
 * What one compaction did.
 *
 * Returned rather than a `boolean`, because "it compacted" is not enough for
 * anyone downstream: the loop has to announce the summary and the log has to
 * store it. `undefined` is the no-op — there was nothing old enough to be
 * worth a model call.
 */
export interface Summarized {
  /**
   * The model's summary, unframed.
   *
   * The framing sentence lives in {@link summaryMessage} and not here, so the
   * message a compacted `Chat` holds and the message a resumed conversation
   * rebuilds are produced by one function rather than two that agree today.
   */
  readonly summary: string;
  /** Messages the summary replaced. */
  readonly summarizedMessages: number;
  /** Messages kept verbatim after it, newest-last. */
  readonly keptMessages: number;
  /** Provider usage spent producing the summary. */
  readonly usage: { readonly input: number; readonly output: number };
}

/**
 * How a summary is presented to the model.
 *
 * A user message, because that is the role every provider already knows what
 * to do with, and inventing one would mean teaching every adapter about it.
 */
export const summaryMessage = (summary: string): Prompt.Message =>
  Prompt.makeMessage('user', {
    content: [
      Prompt.makePart('text', {
        text: `Summary of earlier conversation:\n\n${summary}`,
      }),
    ],
  });

/**
 * Replace everything but the recent tail with a model-written summary.
 *
 * The agent's own system message is always preserved in the *resulting*
 * history: it is the agent's identity, not conversation, and dropping it
 * changes behaviour rather than shortening context. It is deliberately not
 * sent on the summarization call itself, which runs under the summarizer's
 * system prompt instead — an agent's instructions tell a model how to do the
 * agent's job, and a summarizer asked to do that job will do it rather than
 * summarize.
 */
export const compact = Effect.fn('Agent.compact')(function* (
  chat: Chat.Service,
  policy: Compaction.Policy = Compaction.defaultPolicy,
) {
  const history = yield* Ref.get(chat.history);
  const split = splitAt(history, policy.keepRecentTokens);
  yield* Effect.annotateCurrentSpan({
    'vesper.compaction.applied': split.older.length > 0,
    'vesper.compaction.summarizedMessages': split.older.length,
    'vesper.compaction.keptMessages': split.recent.length,
  });

  if (split.older.length === 0) {
    // Nothing old enough to summarize. Compacting anyway would spend a model
    // call to replace recent history with a lossy paraphrase of itself.
    return undefined;
  }

  const summary = yield* LanguageModel.generateText({
    prompt: Prompt.make([
      // Every message below this one is a conversation, and a model handed a
      // conversation continues it. This is the sentence that says the job is
      // to describe it instead.
      { role: 'system', content: policy.system ?? Compaction.defaultSystem },
      ...split.older,
      { role: 'user', content: [{ type: 'text', text: policy.instructions }] },
    ]),
  });
  yield* Observability.usage({
    input: summary.usage.inputTokens.total ?? 0,
    output: summary.usage.outputTokens.total ?? 0,
  });
  yield* Observability.compaction;

  yield* Ref.set(
    chat.history,
    Prompt.make([
      ...split.system,
      summaryMessage(summary.text),
      ...split.recent,
    ]),
  );

  return {
    summary: summary.text,
    summarizedMessages: split.older.length,
    keptMessages: split.recent.length,
    usage: {
      input: summary.usage.inputTokens.total ?? 0,
      output: summary.usage.outputTokens.total ?? 0,
    },
  };
});

/**
 * Compact once and retry when a turn fails on context overflow.
 *
 * This is the reactive trigger. The estimate above will sometimes be wrong;
 * this is what makes that survivable. It retries once — a second overflow
 * after compacting means the recent tail alone does not fit, and looping
 * would just spend model calls discovering that repeatedly.
 */
export const withCompaction =
  (chat: Chat.Service, policy: Compaction.Policy = Compaction.defaultPolicy) =>
  <A, R>(
    turn: Effect.Effect<A, AiError.AiError, R>,
  ): Effect.Effect<A, AiError.AiError, R | LanguageModel.LanguageModel> =>
    turn.pipe(
      Effect.catchIf(Compaction.isContextOverflow, () =>
        compact(chat, policy).pipe(Effect.andThen(turn)),
      ),
    );

interface Split {
  readonly system: ReadonlyArray<Prompt.Message>;
  readonly older: ReadonlyArray<Prompt.Message>;
  readonly recent: ReadonlyArray<Prompt.Message>;
}

// Walk backwards accumulating until the recent budget is spent; everything
// before that is summarized. Splitting on whole messages rather than tokens
// avoids cutting a tool call away from its result, which would leave the
// model looking at a call it never got an answer to.
const splitAt = (prompt: Prompt.Prompt, keepRecentTokens: number): Split => {
  const system = prompt.content.filter((message) => message.role === 'system');
  const rest = prompt.content.filter((message) => message.role !== 'system');

  const recent: Prompt.Message[] = [];
  let budget = keepRecentTokens;

  for (let index = rest.length - 1; index >= 0; index -= 1) {
    const message = rest[index]!;
    const cost = estimateTokens(Prompt.make([message]));
    if (budget - cost < 0 && recent.length > 0) break;
    budget -= cost;
    recent.unshift(message);
  }

  let firstRecent = rest.length - recent.length;

  // Effect Chat stores one turn's calls in an assistant message and their
  // outcomes in the following tool message. A token boundary between those
  // messages creates an invalid prompt, so move the matching assistant message
  // into the kept tail as one atomic unit.
  if (firstRecent > 0 && rest[firstRecent]?.role === 'tool') {
    const calls = rest[firstRecent - 1];
    const results = rest[firstRecent];
    if (
      calls?.role === 'assistant' &&
      results?.role === 'tool' &&
      calls.content.some(
        (call) =>
          call.type === 'tool-call' &&
          results.content.some(
            (result) => result.type === 'tool-result' && result.id === call.id,
          ),
      )
    ) {
      firstRecent -= 1;
    }
  }

  return {
    system,
    older: rest.slice(0, firstRecent),
    recent: rest.slice(firstRecent),
  };
};

export * as CompactionRuntime from './compaction.js';
