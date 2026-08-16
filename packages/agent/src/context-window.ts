import { Context } from 'effect';
import type { Prompt } from 'effect/unstable/ai';

// How full the context window is, and whether that is too full.
//
// Two questions, and neither has an answer this package can compute well: how
// many tokens a conversation currently occupies, and when that is close enough
// to the ceiling to act. Both are heuristics somebody else maintains against
// real providers.
//
// So they are a seam rather than a constant. `@sunfall/vesper-agent` must not import
// `@sunfall/vesper-pi` — that rule is what keeps the loop provider-agnostic — so the
// loop states the *question* here and `@sunfall/vesper-pi/compaction` answers it with
// Pi's maintained implementations, which `@sunfall/vesper-runtime` wires as the
// default. Nothing in this file knows Pi exists, and nothing in Pi's adapter
// imports this file: it produces a value of this shape structurally, and
// `runtime` is where the two are checked against each other. That is the same
// arrangement `Compaction.CONTEXT_OVERFLOW` already uses, for the same reason.
//
// ## Why a `Context.Reference` and not a service
//
// Because the safe default is real. {@link pure} is a complete, correct
// implementation — it is precisely what this package did before the seam
// existed — so an agent that never wires a provider still compacts, and a test
// still runs without Pi in scope. That is the same argument `Subagent.Depth`
// rests on, and it is *not* the argument the family rejects elsewhere: a
// defaulted `LogStore` would hide persistence behind plausible behaviour,
// whereas a defaulted estimate hides nothing — the run works, the guess is
// just cruder, and the reactive overflow path catches what it misses.
//
// The requirement channel is the practical payoff. A `Reference`'s identifier
// is `never`, so reading one adds nothing to `R`: the loop's signatures are
// unchanged by this file's existence, and no call site has to learn about it.

/**
 * What a provider reported about one completed turn.
 *
 * The whole point of the seam. A character-count estimate is a guess about
 * text the provider has already counted exactly, and it is wrong in both
 * directions — it ignores the system prompt's cached prefix, tool schemas,
 * images, and every provider's own framing. A turn's reported usage is the
 * ground truth for everything up to that turn, so an estimator handed one only
 * has to guess about what came *after* it.
 *
 * Deliberately not `Stop.Usage`, which is cumulative across a run: summing
 * turns would count the same prompt once per turn and diverge without bound.
 * This is one turn's figures.
 */
export interface TurnUsage {
  /** Every input token the provider billed, cached and uncached. */
  readonly inputTokens: number;
  readonly outputTokens: number;
}

/**
 * How full the window is, and how much of that is known rather than guessed.
 *
 * The split is reported rather than folded into one number because it is the
 * only way to tell a confident estimate from a shot in the dark: `tokens ===
 * trailingTokens` means nothing was anchored and the whole figure is a
 * character count.
 */
export interface Estimate {
  /** Total context tokens: {@link usageTokens} plus {@link trailingTokens}. */
  readonly tokens: number;
  /** Tokens a provider reported. Zero when no usage was available. */
  readonly usageTokens: number;
  /** Tokens estimated for whatever followed the reported usage. */
  readonly trailingTokens: number;
}

/**
 * The part of a compaction policy the trigger actually reads.
 *
 * Structural and minimal on purpose: `Compaction.Policy` satisfies it, and so
 * does Pi's `CompactionSettings`, so neither side has to import the other's
 * type to talk about headroom.
 */
export interface Settings {
  readonly reserveTokens: number;
}

/**
 * The maintained answers to the two questions above.
 *
 * One service rather than two because they are one algorithm: a trigger is
 * meaningless apart from the estimate it reads, and the two disagreeing about
 * what a token is — Pi counts an image at 4,800 characters, a character count
 * at zero — is a compaction that fires at the wrong size in a way nothing
 * reports. Splitting them would let one be replaced and the other silently
 * left behind.
 *
 * The summarizer's system prompt is deliberately *not* here. It looked like
 * the third member of this set, and Pi does maintain one — but
 * `pi-agent-core@0.80.2` does not re-export `SUMMARIZATION_SYSTEM_PROMPT` from
 * its entry point and its `exports` map admits no deep import, so no adapter
 * can supply Pi's. Putting an unfillable slot on this interface would make
 * every implementation invent one. It lives on `Compaction.Policy` instead,
 * where a caller can override it.
 */
export interface Heuristics {
  /**
   * How many tokens this conversation occupies.
   *
   * `usage`, when given, is what the provider reported for the most recent
   * completed turn — the one whose assistant message is last in `prompt`. An
   * implementation should treat it as exact for everything up to that message
   * and estimate only what follows.
   */
  readonly estimate: (
    prompt: Prompt.Prompt,
    usage?: TurnUsage | undefined,
  ) => Estimate;

  /** Whether `contextTokens` is close enough to the ceiling to compact. */
  readonly shouldCompact: (
    contextTokens: number,
    contextWindow: number,
    settings: Settings,
  ) => boolean;
}

/**
 * Four characters per token, the usual English approximation.
 *
 * Kept as the fallback rather than deleted: it needs no provider, no network,
 * and no dependency, so this package stays runnable and testable on its own.
 * It is also the honest floor — every richer implementation in this family
 * degrades to exactly this when no usage is available.
 */
export const estimateTokens = (prompt: Prompt.Prompt): number => {
  let characters = 0;
  for (const message of prompt.content) {
    if (typeof message.content === 'string') {
      characters += message.content.length;
      continue;
    }
    for (const part of message.content) {
      if ('text' in part && typeof part.text === 'string') {
        characters += part.text.length;
      }
    }
  }
  return Math.ceil(characters / 4);
};

/**
 * The dependency-free implementation, and the default.
 *
 * `estimate` ignores `usage` entirely — that is the difference a provider-
 * backed implementation makes, and blurring it here would hide how much the
 * seam is worth. What it does instead is count characters, which is what this
 * package has always done.
 */
export const pure: Heuristics = {
  estimate: (prompt) => {
    const tokens = estimateTokens(prompt);
    return { tokens, usageTokens: 0, trailingTokens: tokens };
  },
  shouldCompact: (contextTokens, contextWindow, settings) =>
    contextTokens > Math.max(0, contextWindow - settings.reserveTokens),
};

/**
 * @category services
 * @since 0.1.0
 */
export const Service = Context.Reference<Heuristics>(
  '@sunfall/vesper-agent/ContextWindow',
  { defaultValue: () => pure },
);

/**
 * The one message a caller can hand an estimator as its anchor.
 *
 * Exported because the loop and its tests both need to turn a provider's
 * finish part into this shape, and doing it in two places is how the two
 * would come to mean different things.
 */
export const usageFromTurn = (usage: {
  readonly inputTokens: { readonly total?: number | undefined };
  readonly outputTokens: { readonly total?: number | undefined };
}): TurnUsage => ({
  inputTokens: usage.inputTokens.total ?? 0,
  outputTokens: usage.outputTokens.total ?? 0,
});

export * as ContextWindow from './context-window.js';
