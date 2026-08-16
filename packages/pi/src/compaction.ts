import {
  estimateContextTokens,
  estimateTokens as estimatePiMessageTokens,
  shouldCompact as piShouldCompact,
} from '@earendil-works/pi-agent-core';
import type {
  Message as PiMessage,
  Usage as PiUsage,
} from '@earendil-works/pi-ai';
import type { Prompt } from 'effect/unstable/ai';

import { PiPrompt } from './prompt.js';

// Pi's compaction heuristics, in the shape `@sunfall/vesper-agent` asks for.
//
// The loop states three questions in `@sunfall/vesper-agent/context-window` — how
// full is the window, is that too full, and under what system prompt is a
// summary produced — and deliberately does not answer them well. This answers
// them with the implementations Pi maintains against real providers.
//
// ## What is taken, and what is not
//
// Taken: `estimateContextTokens` and `shouldCompact`. The first is the one
// that matters. Pi's raw `estimateTokens` is the *same* four-characters-per-
// token heuristic `@sunfall/vesper-agent` already had, so the estimate alone is not
// the improvement — the anchoring is. `estimateContextTokens` walks back to
// the most recent assistant message carrying provider-reported usage, takes
// that figure as exact, and estimates only what came after it. On a long
// conversation that is the difference between guessing about eighty thousand
// tokens and guessing about two hundred.
//
// Wanted and unavailable: `SUMMARIZATION_SYSTEM_PROMPT`. Pi defines and
// maintains it, but `pi-agent-core@0.80.2` does not re-export it from its
// entry point and its `exports` map lists only `.`, `./node` and
// `./package.json`, so there is no import that reaches it. The text is
// transcribed into `Compaction.defaultSystem` instead, with the drift risk
// that implies; re-check it on a version bump.
//
// Not taken: `generateSummary` and `generateSummaryWithUsage`, which run
// through Pi's own provider path. `@sunfall/vesper-agent` executes the summarization
// call through Effect's `LanguageModel`, which carries this family's spans,
// retry policy, and typed errors. Pi's *prompt* is worth having; Pi's
// *execution* would cost all three. Also not taken: `findCutPoint`,
// `prepareCompaction`, `compact` and `Session`, which are written against
// `SessionTreeEntry[]` — Pi's session tree, which this family does not have
// and does not want, because `@sunfall/vesper-log` already answers the question it
// answers. And not `agentLoop`, `Agent`, or the harness tools, which are
// welded to typebox.
//
// ## No import in either direction
//
// This module does not import `@sunfall/vesper-agent`, and `@sunfall/vesper-agent` must not
// import this package. {@link heuristics} is structurally a
// `ContextWindow.Heuristics`, and `@sunfall/vesper-runtime` — the one package that
// depends on both — is where that is checked, at the point it assigns this
// value to that service. A drift in either shape therefore fails a build
// rather than silently disabling the seam. `Compaction.CONTEXT_OVERFLOW` and
// `PiErrors.CONTEXT_OVERFLOW` already have exactly this arrangement.

/** Mirrors `ContextWindow.TurnUsage`; see the note above on why it is copied. */
interface TurnUsage {
  readonly inputTokens: number;
  readonly outputTokens: number;
}

/** Mirrors `ContextWindow.Estimate`. */
interface Estimate {
  readonly tokens: number;
  readonly usageTokens: number;
  readonly trailingTokens: number;
}

/** Mirrors `ContextWindow.Settings`. */
interface Settings {
  readonly reserveTokens: number;
}

const NO_COST = {
  input: 0,
  output: 0,
  cacheRead: 0,
  cacheWrite: 0,
  total: 0,
} as const;

/**
 * A turn's reported usage as Pi's `Usage`.
 *
 * Only `totalTokens` is populated with anything meaningful, because that is
 * the field Pi's `calculateContextTokens` prefers and the only one this seam
 * carries. Effect's `inputTokens.total` already includes cache reads and
 * writes, so splitting it back out would double-count.
 */
const toPiUsage = (usage: TurnUsage): PiUsage => ({
  input: usage.inputTokens,
  output: usage.outputTokens,
  cacheRead: 0,
  cacheWrite: 0,
  totalTokens: usage.inputTokens + usage.outputTokens,
  cost: NO_COST,
});

/**
 * Attach a turn's usage to the message Pi will anchor on.
 *
 * `PiPrompt.toPiContext` zeroes usage on every assistant message — a converted
 * `Prompt` carries none, and a zeroed record is what keeps conversion
 * deterministic for replay. Pi skips zeroed usage when looking for an anchor,
 * so without this the estimate degrades to a pure character count, which is
 * exactly what the seam exists to stop.
 *
 * The last assistant message is the right carrier because that is what the
 * figure describes: a provider reports input tokens for the prompt it was
 * given and output tokens for the message it produced, so everything up to and
 * including that message is covered and everything after it is not — which is
 * precisely the split `estimateContextTokens` makes.
 */
const anchoredOn = (
  messages: ReadonlyArray<PiMessage>,
  usage: TurnUsage,
): PiMessage[] => {
  const anchored = [...messages];
  for (let index = anchored.length - 1; index >= 0; index -= 1) {
    const message = anchored[index]!;
    if (message.role !== 'assistant') continue;
    anchored[index] = { ...message, usage: toPiUsage(usage) };
    return anchored;
  }
  // No assistant message: nothing has been billed yet, so there is nothing to
  // anchor to and the estimate is a character count. Returning the list
  // unchanged rather than inventing a carrier keeps that visible in the
  // `usageTokens: 0` an unanchored estimate reports.
  return anchored;
};

/**
 * The system prompt's tokens, which Pi's estimator does not see.
 *
 * Pi models a conversation as `{ systemPrompt, messages }` and
 * `estimateContextTokens` takes only the messages, so a converted `Prompt`
 * would lose its system message from the count — and an agent's instructions
 * plus a skill catalog is not a rounding error.
 *
 * Added only when the estimate is unanchored. Once a provider has reported
 * usage, the system prompt is already inside the figure it reported, and
 * adding it again would inflate every estimate on every long conversation —
 * the direction that compacts too early and pays for summaries nobody needed.
 */
const estimateSystemPrompt = (systemPrompt: string | undefined): number =>
  systemPrompt === undefined || systemPrompt === ''
    ? 0
    : estimatePiMessageTokens({
        role: 'user',
        content: [{ type: 'text', text: systemPrompt }],
        timestamp: 0,
      });

const estimate = (
  prompt: Prompt.Prompt,
  usage?: TurnUsage | undefined,
): Estimate => {
  const context = PiPrompt.toPiContext(prompt, undefined);
  const messages =
    usage === undefined
      ? context.messages
      : anchoredOn(context.messages, usage);

  const estimated = estimateContextTokens(messages);

  if (estimated.lastUsageIndex !== null) {
    return {
      tokens: estimated.tokens,
      usageTokens: estimated.usageTokens,
      trailingTokens: estimated.trailingTokens,
    };
  }

  const system = estimateSystemPrompt(context.systemPrompt);
  return {
    tokens: estimated.tokens + system,
    usageTokens: 0,
    trailingTokens: estimated.trailingTokens + system,
  };
};

/**
 * Pi's maintained answers, ready to be wired as `ContextWindow.Service`.
 *
 * `enabled` is supplied here rather than travelling through the seam: in this
 * family a caller opts out of compaction with `Agent.Definition.compaction:
 * false`, which removes the whole mechanism, so a policy that reaches this
 * function is by construction one that is switched on.
 */
export const heuristics = {
  estimate,
  shouldCompact: (
    contextTokens: number,
    contextWindow: number,
    settings: Settings,
  ): boolean =>
    piShouldCompact(contextTokens, contextWindow, {
      enabled: true,
      reserveTokens: settings.reserveTokens,
      // Read only by Pi's cut-point search, which this family does not use.
      keepRecentTokens: 0,
    }),
};

export * as PiCompaction from './compaction.js';
