import { Effect, Layer, Logger, Ref, Stream } from 'effect';
import {
  AiError,
  Chat,
  LanguageModel,
  Prompt,
  Toolkit,
} from 'effect/unstable/ai';
import { describe, expect, it } from '@effect/vitest';

import { Agent } from '../src/agent.js';
import {
  fakeProvider,
  finish,
  message,
  overflow,
  turnOf,
} from './compaction-fixtures.js';
import { defaultSystem, isContextOverflow } from '../src/compaction.js';
import { ContextWindow } from '../src/context-window.js';
import {
  compact,
  estimateTokens,
  shouldCompact,
  summaryMessage,
  withCompaction,
} from '../src/internal/compaction.js';

const required = <A>(value: A | undefined): A => {
  if (value === undefined) {
    throw new Error('expected a value');
  }
  return value;
};

// When compaction fires, and what one compaction does.
//
// This file covers the mechanism end to end in memory: the estimate and the
// seam it is read through (`context-window.ts`), the policy and the rewrite
// (`compaction.ts`), and the two triggers the loop wires (`agent.ts`) — the
// proactive one that fires from an estimate and the reactive one that fires
// when a provider refuses. They are one subject: every one of them is a
// statement about the same question, "is this conversation too big, and what do
// we do about it".
//
// What is *not* here is what a compacted conversation rebuilds to from its log.
// That is a different question with a different dependency surface — a log
// store, records, offsets — and it lives in `compaction-resume.test.ts`.

const POLICY = {
  reserveTokens: 100,
  keepRecentTokens: 10,
  instructions: 'sum',
};

/** A conversation already long enough that a character count crosses 1,000. */
const seeded = Prompt.make([
  { role: 'system' as const, content: 'BE TERSE' },
  message('user', 'a'.repeat(4_000)),
  message('assistant', 'b'.repeat(4_000)),
  message('user', 'recent'),
]);

/** Run against a fresh fake provider and hand back what it was asked. */
const run = <A, E>(
  build: (
    chat: Chat.Service,
  ) => Effect.Effect<A, E, LanguageModel.LanguageModel>,
  history: Prompt.Prompt,
): Effect.Effect<
  {
    value: A;
    history: Prompt.Prompt;
    models: ReturnType<typeof fakeProvider>;
  },
  E
> => {
  const models = fakeProvider();

  return Effect.gen(function* () {
    const chat = yield* Chat.fromPrompt(history);
    const value = yield* build(chat);
    return { value, history: yield* Ref.get(chat.history), models };
  }).pipe(Effect.provide(models.layer));
};

const textsOf = (prompt: Prompt.Prompt) =>
  prompt.content.map((m) =>
    typeof m.content === 'string'
      ? m.content
      : m.content.map((p) => ('text' in p ? p.text : '')).join(''),
  );

const seededHistory = () =>
  Chat.fromPrompt(
    Prompt.make([
      { role: 'system', content: 'S' },
      message('user', 'a'.repeat(400)),
      message('assistant', 'b'.repeat(400)),
    ]),
  );

const textOf = (prompt: Prompt.Prompt) =>
  JSON.stringify(
    prompt.content.map((m) =>
      typeof m.content === 'string'
        ? m.content
        : m.content.map((p) => ('text' in p ? p.text : '')).join(''),
    ),
  );

/** Collects every `Warn`-level message logged while `layer` is provided. */
const captureWarnings = () => {
  const messages: unknown[] = [];
  const logger = Logger.make<unknown, void>(
    ({ logLevel, message: logMessage }) => {
      if (logLevel === 'Warn') {
        messages.push(logMessage);
      }
    },
  );
  return { messages, layer: Logger.layer([logger]) };
};

describe('estimateTokens', () => {
  it('counts text across message shapes', () => {
    const prompt = Prompt.make([
      { role: 'system', content: 'x'.repeat(40) },
      message('user', 'y'.repeat(40)),
    ]);

    expect(estimateTokens(prompt)).toBe(20);
  });

  // The name `compaction.ts` exports is the one `context-window.ts` defines,
  // not a second copy that agrees today. The cut heuristic and the fallback
  // estimator have to price a message identically or a split is made against a
  // budget nothing else measures in.
  it('is the estimator context-window defines, not a copy', () => {
    expect(estimateTokens).toBe(ContextWindow.estimateTokens);
  });
});

describe('pure', () => {
  const prompt = Prompt.make([message('user', 'z'.repeat(400))]);

  // The whole figure is a guess, and it says so. An implementation that
  // reported `usageTokens: 100` here would be claiming a provider agreed with
  // it, which is the one thing a caller reads that split to find out.
  it('reports the entire estimate as unanchored', () => {
    expect(ContextWindow.pure.estimate(prompt)).toEqual({
      tokens: 100,
      usageTokens: 0,
      trailingTokens: 100,
    });
  });

  // Deliberate: the fallback exists to be dependency-free, and pretending to
  // use an anchor it cannot interpret would hide how much the seam is worth.
  it('ignores a usage anchor rather than half-using it', () => {
    expect(
      ContextWindow.pure.estimate(prompt, {
        inputTokens: 90_000,
        outputTokens: 500,
      }).tokens,
    ).toBe(100);
  });

  it('fires strictly above the window minus the reserve', () => {
    const settings = { reserveTokens: 20 };

    expect(ContextWindow.pure.shouldCompact(80, 100, settings)).toBe(false);
    expect(ContextWindow.pure.shouldCompact(81, 100, settings)).toBe(true);
  });

  // A reserve wider than the window means every conversation is over budget,
  // not that the threshold is negative and nothing ever is.
  it('clamps a reserve wider than the window', () => {
    expect(
      ContextWindow.pure.shouldCompact(1, 10, { reserveTokens: 100 }),
    ).toBe(true);
  });
});

describe('Service', () => {
  // A `Reference`, so nothing has to be provided and nothing appears in `R`.
  // If this became an ordinary service the whole package would stop being
  // runnable without wiring, which is the property that keeps every other
  // test in this directory free of runtime policy.
  it.effect('defaults to the pure heuristics with nothing provided', () =>
    Effect.gen(function* () {
      const installed = yield* ContextWindow.Service;
      expect(installed).toBe(ContextWindow.pure);
    }),
  );

  it.effect('takes an override', () =>
    Effect.gen(function* () {
      const fixed: ContextWindow.Heuristics = {
        estimate: () => ({ tokens: 7, usageTokens: 7, trailingTokens: 0 }),
        shouldCompact: () => true,
      };

      const installed = yield* ContextWindow.Service.pipe(
        Effect.provideService(ContextWindow.Service, fixed),
      );

      expect(installed).toBe(fixed);
    }),
  );
});

describe('usageFromTurn', () => {
  it('reads the provider totals', () => {
    expect(
      ContextWindow.usageFromTurn({
        inputTokens: { total: 1_200 },
        outputTokens: { total: 340 },
      }),
    ).toEqual({ inputTokens: 1_200, outputTokens: 340 });
  });

  // A provider that reports no total leaves the anchor at zero rather than
  // `NaN` — which would propagate through every arithmetic comparison in the
  // trigger and make it silently always-false.
  it('treats a missing total as zero', () => {
    expect(
      ContextWindow.usageFromTurn({
        inputTokens: {},
        outputTokens: { total: 5 },
      }),
    ).toEqual({ inputTokens: 0, outputTokens: 5 });
  });
});

describe('shouldCompact', () => {
  const policy = {
    reserveTokens: 20,
    keepRecentTokens: 10,
    instructions: '',
  };

  it.effect('fires once the estimate crosses the window minus reserve', () =>
    Effect.gen(function* () {
      const prompt = Prompt.make([message('user', 'z'.repeat(400))]);

      expect(yield* shouldCompact(prompt, 200, policy)).toBe(false);
      expect(yield* shouldCompact(prompt, 110, policy)).toBe(true);
    }),
  );

  // The point of the seam. Without this the trigger is whatever this package
  // guessed, and installing a provider-backed estimator changes nothing.
  it.effect(
    'asks the installed heuristics rather than counting characters itself',
    () =>
      Effect.gen(function* () {
        const seen: Array<ContextWindow.TurnUsage | undefined> = [];

        const fixed: ContextWindow.Heuristics = {
          estimate: (_prompt, usage) => {
            seen.push(usage);
            return {
              tokens: 99_000,
              usageTokens: 98_000,
              trailingTokens: 1_000,
            };
          },
          shouldCompact: (tokens, window, settings) =>
            tokens > window - settings.reserveTokens,
        };

        // Four characters of text. Nothing a character count does with this
        // prompt could cross a 100,000-token window.
        const prompt = Prompt.make([message('user', 'tiny')]);

        const fired = yield* shouldCompact(
          prompt,
          100_000,
          { ...policy, reserveTokens: 20_000 },
          { inputTokens: 98_000, outputTokens: 0 },
        ).pipe(Effect.provideService(ContextWindow.Service, fixed));

        expect(fired).toBe(true);
        // And the anchor reached the estimator rather than being dropped on the
        // way, which is the half of this that fails silently.
        expect(seen).toEqual([{ inputTokens: 98_000, outputTokens: 0 }]);
      }),
  );
});

describe('compact', () => {
  const policy = {
    reserveTokens: 0,
    keepRecentTokens: 10,
    instructions: 'sum',
  };

  const terse = Prompt.make([
    { role: 'system' as const, content: 'BE TERSE' },
    message('user', 'a'.repeat(200)),
    message('assistant', 'b'.repeat(200)),
    message('user', 'recent'),
  ]);

  const headless = Prompt.make([
    message('user', 'a'.repeat(200)),
    message('assistant', 'b'.repeat(200)),
    message('user', 'recent'),
  ]);

  // A summarization request is a conversation with an instruction stapled to
  // the end, and a model handed one without being told otherwise answers the
  // conversation. This is the sentence that tells it otherwise.
  it.effect(
    'runs the summarization call under the summarizer system prompt',
    () =>
      Effect.gen(function* () {
        const { models } = yield* run((chat) => compact(chat, policy), terse);

        const sent = required(models.summaries[0]);
        expect(sent.content[0]).toMatchObject({
          role: 'system',
          content: defaultSystem,
        });
        // And the agent's own instructions are not what the summarizer runs
        // under: an agent told to be terse will write a terse summary, but an
        // agent told to file support tickets will file one.
        expect(JSON.stringify(sent.content)).not.toContain('BE TERSE');
      }),
  );

  it.effect('lets a caller replace the summarizer system prompt', () =>
    Effect.gen(function* () {
      const { models } = yield* run(
        (chat) => compact(chat, { ...policy, system: 'MINE' }),
        headless,
      );

      expect(required(models.summaries[0]).content[0]).toMatchObject({
        role: 'system',
        content: 'MINE',
      });
    }),
  );

  it.effect('replaces old turns with a summary and keeps the recent tail', () =>
    Effect.gen(function* () {
      const { history } = yield* run((chat) => compact(chat, policy), terse);

      const texts = textsOf(history);

      // System survives — it is the agent's identity, not conversation.
      expect(texts[0]).toBe('BE TERSE');
      expect(texts[1]).toContain('SUMMARY');
      // The recent tail is preserved verbatim, not paraphrased.
      expect(texts[texts.length - 1]).toBe('recent');
      expect(texts.some((t) => t.includes('a'.repeat(200)))).toBe(false);
    }),
  );

  // Spending a model call to paraphrase recent history as itself is pure
  // waste, and it loses fidelity for nothing.
  it.effect('does nothing when there is no older history to summarize', () =>
    Effect.gen(function* () {
      const { value, models } = yield* run(
        (chat) => compact(chat, policy),
        Prompt.make([
          { role: 'system', content: 'BE TERSE' },
          message('user', 'hi'),
        ]),
      );

      expect(value).toBeUndefined();
      expect(models.summaries).toHaveLength(0);
    }),
  );

  // What it did, not that it did something. The loop announces this and the
  // log stores it; a boolean could carry neither, which is how `Compacted`
  // ended up a record with counts and no summary in it.
  it.effect('reports the summary and the shape of the split', () =>
    Effect.gen(function* () {
      const { value } = yield* run((chat) => compact(chat, policy), terse);

      expect(value).toEqual({
        summary: 'SUMMARY',
        summarizedMessages: 2,
        keptMessages: 1,
        usage: { input: 10, output: 4 },
      });
    }),
  );

  it.effect(
    'keeps assistant tool calls and their tool results on the same side',
    () =>
      Effect.gen(function* () {
        const call = Prompt.makeMessage('assistant', {
          content: [
            Prompt.makePart('tool-call', {
              id: 'call-1',
              name: 'lookup',
              params: {},
              providerExecuted: false,
            }),
          ],
        });
        const result = Prompt.makeMessage('tool', {
          content: [
            Prompt.makePart('tool-result', {
              id: 'call-1',
              name: 'lookup',
              result: 'ok',
              isFailure: false,
              providerExecuted: false,
            }),
          ],
        });
        const history = Prompt.make([
          message('user', 'old'.repeat(100)),
          call,
          result,
        ]);

        const { history: compacted } = yield* run(
          (chat) => compact(chat, { ...policy, keepRecentTokens: 1 }),
          history,
        );

        expect(compacted.content.map((entry) => entry.role)).toEqual([
          'user',
          'assistant',
          'tool',
        ]);
      }),
  );

  // One renderer, used by the compacted `Chat` and by the rebuild in
  // `history.ts`. Two would agree until one of them was edited.
  it.effect('frames the summary the way summaryMessage does', () =>
    Effect.gen(function* () {
      const { history } = yield* run((chat) => compact(chat, policy), headless);

      expect(history.content[0]).toEqual(summaryMessage('SUMMARY'));
    }),
  );
});

describe('isContextOverflow', () => {
  // Built from the literal rather than from `CONTEXT_OVERFLOW`, so this pins
  // the wire value a provider adapter actually sets. Reading the constant back
  // would pass even if both sides drifted to something new together.
  it('recognizes the structural marker without importing the provider', () => {
    expect(
      isContextOverflow(
        new AiError.AiError({
          module: 'test',
          method: 'generateText',
          reason: new AiError.InvalidRequestError({
            constraint: 'context-window',
          }),
        }),
      ),
    ).toBe(true);
  });

  it.each([
    'prompt is too long: 213462 tokens > 200000 maximum',
    'maximum context length is 128000 tokens',
    'model_context_window_exceeded',
  ])('recognizes official provider descriptions: %s', (description) => {
    const error = new AiError.AiError({
      module: 'test',
      method: 'generateText',
      reason: new AiError.InvalidRequestError({ description }),
    });

    expect(isContextOverflow(error)).toBe(true);
  });

  it('does not treat other request errors as overflow', () => {
    const other = new AiError.AiError({
      module: 'test',
      method: 'generateText',
      reason: new AiError.InvalidRequestError({ constraint: 'aborted' }),
    });

    expect(isContextOverflow(other)).toBe(false);
  });
});

describe('withCompaction', () => {
  const policy = {
    reserveTokens: 0,
    keepRecentTokens: 10,
    instructions: 'sum',
  };

  // The reactive trigger is what actually saves a run: the estimate is wrong
  // often enough that relying on it alone would strand conversations.
  it.effect('compacts and retries once when a turn overflows', () =>
    Effect.gen(function* () {
      const models = fakeProvider();

      const result = yield* Effect.gen(function* () {
        const attempts = yield* Ref.make(0);

        const chat = yield* Chat.fromPrompt(
          Prompt.make([
            { role: 'system', content: 'S' },
            message('user', 'a'.repeat(200)),
            message('assistant', 'b'.repeat(200)),
            message('user', 'recent'),
          ]),
        );

        const turn = Effect.gen(function* () {
          const attempt = yield* Ref.updateAndGet(attempts, (n) => n + 1);
          if (attempt === 1) {
            return yield* Effect.fail(overflow);
          }
          return 'ok';
        });

        const value = yield* turn.pipe(
          withCompaction(chat, policy),
          Effect.provide(models.layer),
        );

        return { value, attempts: yield* Ref.get(attempts) };
      });

      expect(result.value).toBe('ok');
      expect(result.attempts).toBe(2);
      expect(models.summaries).toHaveLength(1);
    }),
  );

  // Looping would spend a model call per attempt to rediscover that the
  // recent tail alone does not fit.
  it.effect('does not retry a second time', () =>
    Effect.gen(function* () {
      const models = fakeProvider();

      const result = yield* Effect.gen(function* () {
        const attempts = yield* Ref.make(0);

        const chat = yield* Chat.fromPrompt(
          Prompt.make([
            { role: 'system', content: 'S' },
            message('user', 'a'.repeat(200)),
            message('user', 'recent'),
          ]),
        );

        const turn = Ref.update(attempts, (n) => n + 1).pipe(
          Effect.andThen(Effect.fail(overflow)),
        );

        const outcome = yield* turn.pipe(
          withCompaction(chat, policy),
          Effect.provide(models.layer),
          Effect.result,
        );

        return { outcome, attempts: yield* Ref.get(attempts) };
      }) as Effect.Effect<{ outcome: { _tag: string }; attempts: number }>;

      expect(result.outcome._tag).toBe('Failure');
      expect(result.attempts).toBe(2);
    }),
  );
});

// The reactive trigger as the *loop* wires it, which is not the same code as
// `withCompaction` above and behaves differently in one way that only a real
// provider exposed.
//
// `Chat.streamText` builds its request as `history + input` and writes the
// result back to `history` from a finalizer that runs on failure too, so a
// rejected turn leaves its own input in the history. Compaction keeps that
// input — it is the newest message, and the newest message is never summarized
// — and the retry used to supply it a *second* time. Against Anthropic, with
// the input being the 136k tokens that caused the overflow in the first place,
// the retry was rejected for exactly the same reason and the run died having
// paid for a summary.
describe('the loop’s reactive compaction', () => {
  const reactive = Agent.make({
    name: 'reactive',
    revision: '1',
    instructions: 'S',
    toolkit: Toolkit.make(),
    // No `contextWindow`, so nothing fires from an estimate and the only
    // trigger left is a provider refusal.
    compaction: { reserveTokens: 0, keepRecentTokens: 10, instructions: 'sum' },
  });

  const overflowing = () => {
    // Big enough that history plus input is refused, small enough that the
    // summary plus the kept tail plus the input is not.
    const models = fakeProvider({ limit: 150 });

    return Effect.gen(function* () {
      const chat = yield* seededHistory();
      const value = yield* reactive.runIn(chat, 'MARKER');
      return {
        value,
        history: yield* Ref.get(chat.history),
        models,
      };
      // `orDie` rather than an assertion on the whole effect: it is the
      // error channel `runPromise` needs cleared, and saying so leaves the
      // success type inferred instead of restated.
    }).pipe(Effect.orDie, Effect.provide(models.layer));
  };

  it.effect(
    'does not send the overflowed turn’s input twice on the retry',
    () =>
      Effect.gen(function* () {
        const { models } = yield* overflowing();

        expect(models.asked).toHaveLength(2);
        const retried = textOf(required(models.asked[1]));

        expect(retried.split('MARKER')).toHaveLength(2);
      }),
  );

  // The input is what the summarizer must be shown *behind*, not what it
  // summarizes: it is the newest message, so it is the recent tail `splitAt`
  // protects and the history behind it is what gets replaced. That is the only
  // split that shrinks a request whose bulk is the input itself.
  it.effect('summarizes the history behind the input and keeps the input', () =>
    Effect.gen(function* () {
      const { models } = yield* overflowing();

      const summarized = textOf(required(models.summaries[0]));
      expect(summarized).toContain('a'.repeat(400));
      expect(summarized).not.toContain('MARKER');

      const retried = textOf(required(models.asked[1]));
      expect(retried).toContain('Summary of earlier conversation');
      expect(retried).not.toContain('a'.repeat(400));
    }),
  );

  // The retry has to be a smaller request than the one that was refused, or
  // compaction has spent a model call to change nothing.
  it.effect('retries with a smaller prompt than the one that was refused', () =>
    Effect.gen(function* () {
      const { models } = yield* overflowing();

      expect(textOf(required(models.asked[1])).length).toBeLessThan(
        textOf(required(models.asked[0])).length,
      );
    }),
  );

  it.effect('completes the run once the retry fits', () =>
    Effect.gen(function* () {
      const { value } = yield* overflowing();

      expect(value.text).toBe('answer');
    }),
  );

  it.effect('does not retry when compaction cannot change the prompt', () =>
    Effect.gen(function* () {
      const asked: Prompt.Prompt[] = [];
      const layer = Layer.effect(
        LanguageModel.LanguageModel,
        LanguageModel.make({
          generateText: () => Effect.succeed([]),
          streamText: (options) => {
            asked.push(options.prompt);
            return Stream.fail(overflow);
          },
        }),
      );
      const short = Agent.make({
        name: 'short',
        revision: '1',
        instructions: 'S',
        toolkit: Toolkit.make(),
        compaction: {
          reserveTokens: 0,
          keepRecentTokens: 10_000,
          instructions: 'sum',
        },
      });

      const exit = yield* short
        .run('only recent')
        .pipe(Effect.exit, Effect.provide(layer));

      expect(exit._tag).toBe('Failure');
      expect(asked).toHaveLength(1);
    }),
  );

  it.effect(
    'does not retry after partial output became externally visible',
    () =>
      Effect.gen(function* () {
        const asked: Prompt.Prompt[] = [];
        const layer = Layer.effect(
          LanguageModel.LanguageModel,
          LanguageModel.make({
            generateText: () => Effect.succeed([]),
            streamText: (options) => {
              asked.push(options.prompt);
              return Stream.concat(
                Stream.fromIterable([
                  { type: 'text-start' as const, id: 'partial' },
                  {
                    type: 'text-delta' as const,
                    id: 'partial',
                    delta: 'visible',
                  },
                ]),
                Stream.fail(overflow),
              );
            },
          }),
        );
        const events: string[] = [];

        yield* Effect.gen(function* () {
          const chat = yield* seededHistory();
          return yield* reactive.streamIn(chat, 'next').pipe(
            Stream.tap((event) =>
              Effect.sync(() => {
                if (event._tag === 'Part') {
                  events.push(event.part.type);
                }
              }),
            ),
            Stream.runDrain,
          );
        }).pipe(Effect.exit, Effect.provide(layer));

        expect(events).toContain('text-delta');
        expect(asked).toHaveLength(1);
      }),
  );

  it.effect(
    'normalizes structured in-band overflow errors for reactive compaction',
    () =>
      Effect.gen(function* () {
        let calls = 0;
        const layer = Layer.effect(
          LanguageModel.LanguageModel,
          LanguageModel.make({
            generateText: () =>
              Effect.succeed([
                { type: 'text' as const, text: 'SUMMARY' },
                finish(10, 4),
              ]),
            streamText: () => {
              calls += 1;
              return calls === 1
                ? Stream.make({
                    type: 'error' as const,
                    error: {
                      code: 'model_context_window_exceeded',
                      message: 'maximum context length exceeded',
                      metadata: { requestId: 'req-1' },
                    },
                  })
                : Stream.fromIterable(turnOf('answer', 'answer'));
            },
          }),
        );

        const result = yield* Effect.gen(function* () {
          const chat = yield* seededHistory();
          return yield* reactive.runIn(chat, 'next');
        }).pipe(Effect.orDie, Effect.provide(layer));

        expect(result.text).toBe('answer');
        expect(calls).toBe(2);
      }),
  );
});

// The proactive trigger: compaction that fires from an estimate, before the
// provider has refused anything.
//
// This is the path that had no wiring at all. The proactive compaction check
// existed, was exported, was tested — and nothing in the loop called it, so
// every compaction this family had ever performed was reactive: a request
// rejected, a summary written, the turn re-run. That is one wasted round trip
// per compaction, and on a provider that charges for the rejected prompt it is
// not free either.
//
// It stayed unwired for a real reason rather than an oversight: the loop
// targets the `LanguageModel` tag and that tag carries no context window, so
// there was no threshold to compare an estimate against. `Policy.contextWindow`
// is the caller supplying one. With it absent the loop behaves exactly as it
// did, which is what every other test in this directory relies on.
describe('compaction from an estimate', () => {
  it.effect(
    'compacts before the turn when the estimate crosses the threshold',
    () =>
      Effect.gen(function* () {
        const models = fakeProvider({ inputTokens: 50 });
        const agent = Agent.make({
          name: 'test',
          revision: '1',
          instructions: 'be terse',
          toolkit: Toolkit.make(),
          compaction: { ...POLICY, contextWindow: 1_000 },
        });

        const asked = yield* Effect.gen(function* () {
          const chat = yield* Chat.fromPrompt(seeded);
          yield* agent.runIn(chat, 'next');
          return models.asked;
        }).pipe(Effect.provide(models.layer), Effect.orDie);

        // One summary, and it happened before the provider was ever asked to
        // serve a turn — so no turn was rejected and none was re-run.
        expect(models.summaries).toHaveLength(1);
        expect(asked).toHaveLength(1);

        const sent = JSON.stringify(required(asked[0]).content);
        expect(sent).toContain('SUMMARY');
        expect(sent).not.toContain('a'.repeat(4_000));
      }),
  );

  // The gate, and the reason every other test in this directory is unaffected.
  it.effect('does nothing when the caller named no context window', () =>
    Effect.gen(function* () {
      const models = fakeProvider({ inputTokens: 50 });
      const agent = Agent.make({
        name: 'test',
        revision: '1',
        instructions: 'be terse',
        toolkit: Toolkit.make(),
        compaction: POLICY,
      });

      yield* Effect.gen(function* () {
        const chat = yield* Chat.fromPrompt(seeded);
        yield* agent.runIn(chat, 'next');
      }).pipe(Effect.provide(models.layer), Effect.orDie);

      expect(models.summaries).toHaveLength(0);
      expect(JSON.stringify(required(models.asked[0]).content)).toContain(
        'a'.repeat(4_000),
      );
    }),
  );

  it.effect(
    'announces the rewrite, so a resumed conversation can honour it',
    () =>
      Effect.gen(function* () {
        const models = fakeProvider({ inputTokens: 50 });
        const agent = Agent.make({
          name: 'test',
          revision: '1',
          instructions: 'be terse',
          toolkit: Toolkit.make(),
          compaction: { ...POLICY, contextWindow: 1_000 },
        });

        const events = yield* Effect.gen(function* () {
          const chat = yield* Chat.fromPrompt(seeded);
          return yield* Stream.runCollect(agent.streamIn(chat, 'next'));
        }).pipe(Effect.provide(models.layer), Effect.orDie);

        const compacted = events.filter((event) => event._tag === 'Compacted');
        expect(compacted).toHaveLength(1);
        expect(compacted[0]).toMatchObject({ step: 1, summary: 'SUMMARY' });

        // After the turn opened, matching where the reactive path puts it. A
        // reader folding this stream sees one shape for both triggers.
        const tags = events.map((event) => event._tag);
        expect(tags.indexOf('Compacted')).toBe(tags.indexOf('TurnStarted') + 1);
      }),
  );

  it.effect('includes summarization usage in the completed result', () =>
    Effect.gen(function* () {
      const models = fakeProvider({ inputTokens: 50 });
      const agent = Agent.make({
        name: 'test',
        revision: '1',
        instructions: 'be terse',
        toolkit: Toolkit.make(),
        compaction: { ...POLICY, contextWindow: 1_000 },
      });

      const result = yield* Effect.gen(function* () {
        const chat = yield* Chat.fromPrompt(seeded);
        return yield* agent.runIn(chat, 'next');
      }).pipe(Effect.provide(models.layer), Effect.orDie);

      expect(result.usage).toEqual({ input: 60, output: 24 });
    }),
  );

  // The wiring that makes the seam worth anything. The estimator is handed
  // what the provider reported for the previous turn; without it the whole
  // conversation remains a character count and reported usage is wasted.
  it.effect('hands the estimator the previous turn own reported usage', () =>
    Effect.gen(function* () {
      const models = fakeProvider({ inputTokens: 7_777 });
      const seen: Array<ContextWindow.TurnUsage | undefined> = [];

      const recording: ContextWindow.Heuristics = {
        estimate: (_prompt, usage) => {
          seen.push(usage);
          return { tokens: 0, usageTokens: 0, trailingTokens: 0 };
        },
        shouldCompact: () => false,
      };

      const agent = Agent.make({
        name: 'test',
        revision: '1',
        instructions: 'be terse',
        toolkit: Toolkit.make(),
        compaction: { ...POLICY, contextWindow: 1_000 },
        // Two turns, so there is a previous turn to have reported anything.
        stopWhen: ({ step }) => Effect.succeed(step >= 2),
      });

      yield* Effect.gen(function* () {
        const chat = yield* Chat.fromPrompt(seeded);
        yield* agent.runIn(chat, 'next');
      }).pipe(
        Effect.provide(models.layer),
        Effect.provideService(ContextWindow.Service, recording),
        Effect.orDie,
      );

      // Nothing had been billed before the first turn, and the first turn's
      // figures anchor the second.
      expect(seen).toEqual([
        undefined,
        { inputTokens: 7_777, outputTokens: 20 },
      ]);
    }),
  );
});

// The footgun this whole trigger has: a policy without `contextWindow`
// compiles, runs, and never proactively compacts, and nothing about a run
// that never overflows looks different from one that silently skipped its
// only early warning. `entryFor` logs it once per run instead of leaving it
// for someone to notice only after a run dies on a context-window error.
describe('the proactive-compaction misconfiguration warning', () => {
  it.effect('warns exactly once per run when contextWindow is unset', () =>
    Effect.gen(function* () {
      const models = fakeProvider();
      const { messages, layer } = captureWarnings();
      const agent = Agent.make({
        name: 'test',
        revision: '1',
        instructions: 'be terse',
        toolkit: Toolkit.make(),
        compaction: POLICY,
        // Two turns, so a per-turn check would double-count the warning.
        stopWhen: ({ step }) => Effect.succeed(step >= 2),
      });

      yield* Effect.gen(function* () {
        const chat = yield* Chat.fromPrompt(seeded);
        yield* agent.runIn(chat, 'next');
      }).pipe(Effect.provide(Layer.merge(models.layer, layer)), Effect.orDie);

      expect(messages).toHaveLength(1);
      expect(String(messages[0])).toContain('contextWindow');
    }),
  );

  it.effect('does not warn when contextWindow is configured', () =>
    Effect.gen(function* () {
      const models = fakeProvider();
      const { messages, layer } = captureWarnings();
      const agent = Agent.make({
        name: 'test',
        revision: '1',
        instructions: 'be terse',
        toolkit: Toolkit.make(),
        compaction: { ...POLICY, contextWindow: 1_000 },
      });

      yield* Effect.gen(function* () {
        const chat = yield* Chat.fromPrompt(seeded);
        yield* agent.runIn(chat, 'next');
      }).pipe(Effect.provide(Layer.merge(models.layer, layer)), Effect.orDie);

      expect(messages).toHaveLength(0);
    }),
  );

  // `compaction: false` opts out of compaction entirely, so there is nothing
  // misconfigured to warn about.
  it.effect('does not warn when compaction is disabled', () =>
    Effect.gen(function* () {
      const models = fakeProvider();
      const { messages, layer } = captureWarnings();
      const agent = Agent.make({
        name: 'test',
        revision: '1',
        instructions: 'be terse',
        toolkit: Toolkit.make(),
        compaction: false,
      });

      yield* Effect.gen(function* () {
        const chat = yield* Chat.fromPrompt(seeded);
        yield* agent.runIn(chat, 'next');
      }).pipe(Effect.provide(Layer.merge(models.layer, layer)), Effect.orDie);

      expect(messages).toHaveLength(0);
    }),
  );
});
