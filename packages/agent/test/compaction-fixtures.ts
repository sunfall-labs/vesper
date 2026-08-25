import { Effect, Layer, Ref, Stream } from 'effect';
import {
  AiError,
  type LanguageModel as LanguageModelNamespace,
  LanguageModel,
  type Prompt,
  type Response,
} from 'effect/unstable/ai';

import { Compaction } from '../src/compaction.js';
import { ContextWindow } from '../src/context-window.js';

// Fakes shared by the two compaction test files.
//
// Not a convenience. Compaction is tested from both ends — what one compaction
// does to a `Chat` in memory, and what a compacted conversation rebuilds to
// from its log — and both ends need the same thing from a provider: a
// summarizer behind `generateText`, a turn behind `streamText`, and a record of
// what each was asked. Three separate transcriptions of that had already
// drifted into three different shapes for the same fake, which is how the
// summarization call ended up asserted against a different usage record in each
// file for no reason anyone could state.
//
// This file deliberately holds no assertions and no `describe`. It is a
// project file rather than a `.test.ts` so vitest does not try to collect it.

/** A single-text-part message, the shape every fixture below is built from. */
export const message = (role: 'user' | 'assistant', text: string) => ({
  role,
  content: [{ type: 'text' as const, text }],
});

/**
 * A provider's end-of-turn usage report.
 *
 * Spelled out rather than defaulted because the anchoring tests read these
 * exact numbers back out of the estimator: `inputTokens` is what a turn claims
 * it was billed, and the loop is supposed to hand precisely that to the next
 * estimate.
 */
export const finish = (
  input: number,
  output: number,
  reason: Response.FinishReason = 'stop',
): Response.FinishPartEncoded => ({
  type: 'finish' as const,
  reason,
  usage: {
    inputTokens: { total: input, uncached: input, cacheRead: 0, cacheWrite: 0 },
    outputTokens: { total: output },
  },
});

/** The structural overflow marker, as a provider adapter would raise it. */
export const overflow = new AiError.AiError({
  module: 'test',
  method: 'streamText',
  reason: new AiError.InvalidRequestError({
    constraint: Compaction.CONTEXT_OVERFLOW,
  }),
});

/** One scripted turn: some text, then a finish part. */
export const turnOf = (
  id: string,
  text: string,
): Response.StreamPartEncoded[] => [
  { type: 'text-start' as const, id },
  { type: 'text-delta' as const, id, delta: text },
  { type: 'text-end' as const, id },
  finish(10, 4),
];

export interface ProviderOptions {
  /** Text returned by the summarization call. Defaults to `SUMMARY`. */
  readonly summaryText?: string;
  /** Finish reason reported by the summarization call. Defaults to `stop`. */
  readonly summaryFinishReason?: Response.FinishReason;
  /**
   * Turns to serve in order, the last repeating once exhausted.
   *
   * Omitted, the provider answers every turn with the same text and reports
   * {@link ProviderOptions.inputTokens}.
   */
  readonly turns?: ReadonlyArray<ReadonlyArray<Response.StreamPartEncoded>>;
  /** Input tokens the default turn reports. Ignored when `turns` is given. */
  readonly inputTokens?: number;
  /**
   * Refuse any prompt estimated above this many tokens.
   *
   * What makes a resumption test non-vacuous. A provider scripted to fail on
   * call number two would succeed whether or not the rebuild honoured the
   * compaction record; one that refuses on *size* rejects an over-long rebuild
   * exactly as a real provider does, so the regression shows up as a summary
   * count rather than as a scripted outcome.
   */
  readonly limit?: number;
}

/**
 * A model that summarizes, answers, and remembers what it was asked.
 *
 * `generateText` is the summarization call and `streamText` is a turn, behind
 * the same tag, exactly as in production — which is the part worth sharing,
 * because a fake that split them across two layers would let a test pass while
 * the loop sent the summarization call somewhere the real one does not.
 */
export const fakeProvider = (options: ProviderOptions = {}) => {
  const asked: Array<Prompt.Prompt> = [];
  const summaries: Array<Prompt.Prompt> = [];

  const layer = Layer.effect(
    LanguageModel.LanguageModel,
    Effect.gen(function* () {
      const calls = yield* Ref.make(0);
      return yield* LanguageModel.make({
        generateText: (opts: LanguageModelNamespace.ProviderOptions) =>
          Effect.sync(() => {
            summaries.push(opts.prompt);
            return [
              {
                type: 'text' as const,
                text: options.summaryText ?? 'SUMMARY',
              },
              finish(10, 4, options.summaryFinishReason),
            ] satisfies Response.PartEncoded[];
          }),
        streamText: (opts: LanguageModelNamespace.ProviderOptions) =>
          Stream.unwrap(
            Effect.gen(function* () {
              asked.push(opts.prompt);

              if (
                options.limit !== undefined &&
                ContextWindow.estimateTokens(opts.prompt) > options.limit
              ) {
                return Stream.fail(overflow);
              }

              const index = yield* Ref.getAndUpdate(calls, (n) => n + 1);

              if (options.turns !== undefined) {
                const turn = options.turns.at(
                  Math.min(index, options.turns.length - 1),
                );
                if (turn === undefined) {
                  throw new Error('scripted provider needs at least one turn');
                }
                return Stream.fromIterable(turn);
              }

              return Stream.fromIterable([
                { type: 'text-start' as const, id: 'a' },
                { type: 'text-delta' as const, id: 'a', delta: 'answer' },
                { type: 'text-end' as const, id: 'a' },
                finish(options.inputTokens ?? 10, 20),
              ] satisfies Response.StreamPartEncoded[]);
            }),
          ),
      });
    }),
  );

  return { asked, summaries, layer };
};
