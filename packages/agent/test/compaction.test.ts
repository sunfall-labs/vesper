import { LogStoreMemory } from '@sunfall/vesper-log/layer-memory';
import type { LogStore } from '@sunfall/vesper-log/log-store';
import { LogOffset } from '@sunfall/vesper-log/offset';
import {
  type ConversationRecord,
  FORMAT_VERSION,
} from '@sunfall/vesper-log/record';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';
import * as NodeServices from '@effect/platform-node/NodeServices';
import { Effect, Layer, Logger, Ref, Stream, type Crypto } from 'effect';
import { AiError, Chat, LanguageModel, Prompt, Toolkit } from 'effect/ai';
import { describe, expect, it } from '@effect/vitest';

import { Agent } from '../src/agent.js';
import { Compaction } from '../src/compaction.js';
import { ContextWindow } from '../src/context-window.js';
import { Conversation } from '../src/conversation.js';
import { AgentHistory } from '../src/history.js';
import { testBudget } from './budget-fixture.js';
import {
  fakeProvider,
  finish,
  message,
  overflow,
  turnOf,
} from './compaction-fixtures.js';

// Compaction scenarios: when it fires, what it does, and what a compacted
// conversation resumes as. Every case drives `Agent`/`Conversation` against a
// scripted `LanguageModel` and observes only the prompts the provider was sent,
// the events emitted and the records logged.

const POLICY = {
  reserveTokens: 0,
  keepRecentTokens: 10,
  instructions: 'sum',
};

const required = <A>(value: A | undefined): A => {
  if (value === undefined) {
    throw new Error('expected a value');
  }
  return value;
};

const textOf = (prompt: Prompt.Prompt) => JSON.stringify(prompt.content);

const makeAgent = (compaction: Compaction.Policy | false, twoTurns = false) =>
  Agent.make({
    name: 'test',
    revision: '1',
    instructions: 'BE TERSE',
    toolkit: Toolkit.make(),
    compaction,
    ...(twoTurns
      ? {
          stopWhen: ({ step }: { readonly step: number }) =>
            Effect.succeed(step >= 2),
        }
      : {}),
  });

const seededChat = (prefix = 400) =>
  Chat.fromPrompt(
    Prompt.make([
      { role: 'system', content: 'S' },
      message('user', 'a'.repeat(prefix)),
      message('assistant', 'b'.repeat(prefix)),
    ]),
  );

/** A history long enough that a character count crosses 1,000 tokens. */
const bulky = () =>
  Chat.fromPrompt(
    Prompt.make([
      { role: 'system' as const, content: 'BE TERSE' },
      message('user', 'a'.repeat(4_000)),
      message('assistant', 'b'.repeat(4_000)),
      message('user', 'recent'),
    ]),
  );

/** A provider whose first `failures` turns fail with `error`, then answers. */
const refusing = (error: AiError.AiError, failures = 1) => {
  const asked: Prompt.Prompt[] = [];
  const summaries: Prompt.Prompt[] = [];
  const layer = Layer.effect(
    LanguageModel.LanguageModel,
    LanguageModel.make({
      generateText: (options) =>
        Effect.sync(() => {
          summaries.push(options.prompt);
          return [{ type: 'text' as const, text: 'SUMMARY' }, finish(10, 4)];
        }),
      streamText: (options) => {
        asked.push(options.prompt);
        return asked.length <= failures
          ? Stream.fail(error)
          : Stream.fromIterable(turnOf('answer', 'answer'));
      },
    }),
  );
  return { asked, summaries, layer };
};

const invalid = (init: { description?: string; constraint?: string }) =>
  new AiError.AiError({
    module: 'test',
    method: 'streamText',
    reason: new AiError.InvalidRequestError(init),
  });

const warnings = () => {
  const messages: unknown[] = [];
  const logger = Logger.make<unknown, void>(({ logLevel, message: m }) => {
    if (logLevel === 'Warn') {
      messages.push(m);
    }
  });
  return { messages, layer: Logger.layer([logger]) };
};

const CONVERSATION = LogVocabulary.ConversationId.make('compacted-1');

const envelopes = (
  records: ReadonlyArray<ConversationRecord.Record>,
): ReadonlyArray<ConversationRecord.Envelope> =>
  records.map((record, index) => ({
    offset: LogOffset.fromSeq(BigInt(index)),
    conversationId: CONVERSATION,
    timestamp: 0,
    record,
  }));
const compacted = (
  summary: string,
  firstKept: LogOffset.Offset,
): ConversationRecord.Record => ({
  _tag: 'Compacted',
  formatVersion: FORMAT_VERSION,
  agent: 'test',
  agentRevision: LogVocabulary.AgentRevision.make('1'),
  step: 1,
  summary,
  firstKept,
  summarizedMessages: 2,
  keptMessages: 1,
});
const started = (prompt: string): ConversationRecord.Record => ({
  _tag: 'RunStarted',
  budget: testBudget,
  agent: 'test',
  formatVersion: 1,
  agentRevision: LogVocabulary.AgentRevision.make('1'),
  prompt: Prompt.make(prompt).content,
});
const said = (text: string): ConversationRecord.Record => ({
  _tag: 'Text',
  step: 1,
  text,
});
const finished: ConversationRecord.Record = {
  _tag: 'TurnFinished',
  step: 1,
  usage: { input: 1, output: 1 },
};

describe('reactive compaction: the provider refuses a turn', () => {
  const reactive = makeAgent(POLICY);

  // Big enough that history plus input is refused, small enough that the summary
  // plus the kept tail plus the input is not.
  const overflowing = () => {
    const models = fakeProvider({ limit: 150 });
    return Effect.gen(function* () {
      const chat = yield* seededChat();
      const value = yield* reactive.runIn(chat, 'MARKER');
      return { value, history: yield* Ref.get(chat.history), models };
    }).pipe(Effect.orDie, Effect.provide(models.layer));
  };

  it.effect('summarizes the history behind the input and retries smaller', () =>
    Effect.gen(function* () {
      const { value, models } = yield* overflowing();

      expect(value.text).toBe('answer');
      expect(models.asked).toHaveLength(2);
      expect(models.summaries).toHaveLength(1);

      // The input is the newest message: it is kept, never summarized, and
      // sent exactly once on the retry even though the rejected turn left it
      // in the chat history.
      const summarized = textOf(required(models.summaries[0]));
      expect(summarized).toContain('a'.repeat(400));
      expect(summarized).not.toContain('MARKER');

      const refused = textOf(required(models.asked[0]));
      const retried = textOf(required(models.asked[1]));
      expect(retried.split('MARKER')).toHaveLength(2);
      expect(retried).toContain('Summary of earlier conversation');
      expect(retried).not.toContain('a'.repeat(400));
      expect(retried.length).toBeLessThan(refused.length);
    }),
  );

  it.effect(
    'runs the summarizer under its own system prompt, or the caller’s',
    () =>
      Effect.gen(function* () {
        const { models } = yield* overflowing();
        expect(required(models.summaries[0]).content[0]).toMatchObject({
          role: 'system',
          content: Compaction.defaultSystem,
        });
        // The agent's identity is not what the summarizer runs under.
        expect(textOf(required(models.summaries[0]))).not.toContain('BE TERSE');

        const custom = fakeProvider({ limit: 150 });
        yield* Effect.gen(function* () {
          const chat = yield* seededChat();
          yield* makeAgent({ ...POLICY, system: 'MINE' }).runIn(chat, 'next');
        }).pipe(Effect.orDie, Effect.provide(custom.layer));
        expect(required(custom.summaries[0]).content[0]).toMatchObject({
          role: 'system',
          content: 'MINE',
        });
      }),
  );

  it.effect('keeps a tool call and its result on the same side', () =>
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
      const models = fakeProvider({ limit: 150 });
      yield* Effect.gen(function* () {
        const chat = yield* Chat.fromPrompt(
          Prompt.make([
            message('user', 'old'.repeat(100)),
            call,
            result,
            message('assistant', 'done'),
          ]),
        );
        yield* makeAgent({ ...POLICY, keepRecentTokens: 1 }).runIn(chat, 'go');
      }).pipe(Effect.orDie, Effect.provide(models.layer));

      for (const prompt of [...models.summaries, models.asked.at(-1)]) {
        const roles = required(prompt).content.map((m) => m.role);
        expect(roles.filter((r) => r === 'tool')).toHaveLength(
          roles.filter((r, i) => r === 'assistant' && roles[i + 1] === 'tool')
            .length,
        );
      }
    }),
  );

  it.effect.each([
    'prompt is too long: 213462 tokens > 200000 maximum',
    'maximum context length is 128000 tokens',
    'model_context_window_exceeded',
  ])('treats the provider description %j as overflow', (description) =>
    Effect.gen(function* () {
      const models = refusing(invalid({ description }));
      const result = yield* Effect.gen(function* () {
        const chat = yield* seededChat();
        return yield* reactive.runIn(chat, 'next');
      }).pipe(Effect.orDie, Effect.provide(models.layer));

      expect(result.text).toBe('answer');
      expect(models.summaries).toHaveLength(1);
      expect(models.asked).toHaveLength(2);
    }),
  );

  it.effect(
    'treats the structural marker and an in-band error as overflow',
    () =>
      Effect.gen(function* () {
        const structural = refusing(overflow);
        let calls = 0;
        const inBand = Layer.effect(
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

        const first = yield* Effect.gen(function* () {
          return yield* reactive.runIn(yield* seededChat(), 'next');
        }).pipe(Effect.orDie, Effect.provide(structural.layer));
        const second = yield* Effect.gen(function* () {
          return yield* reactive.runIn(yield* seededChat(), 'next');
        }).pipe(Effect.orDie, Effect.provide(inBand));

        expect(first.text).toBe('answer');
        expect(second.text).toBe('answer');
        expect(structural.summaries).toHaveLength(1);
        expect(calls).toBe(2);
      }),
  );

  // Compacting in answer to a throttle would rewrite history because the
  // provider was busy.
  it.effect.each([
    'Request too large for model: too many tokens per minute (TPM). Limit 6000, Requested 9000',
    'Rate limit reached: too many tokens, please slow down',
    'ThrottlingException: Too many tokens, please wait before trying again (quota exceeded)',
  ])('does not compact for the throughput limit %j', (description) =>
    Effect.gen(function* () {
      const models = refusing(invalid({ description }), 99);
      const exit = yield* Effect.gen(function* () {
        return yield* reactive.runIn(yield* seededChat(), 'next');
      }).pipe(Effect.exit, Effect.provide(models.layer));

      expect(exit._tag).toBe('Failure');
      expect(models.summaries).toHaveLength(0);
      expect(models.asked).toHaveLength(1);
    }),
  );

  it.effect('retries at most once', () =>
    Effect.gen(function* () {
      const models = refusing(overflow, 99);
      const exit = yield* Effect.gen(function* () {
        return yield* reactive.runIn(yield* seededChat(), 'next');
      }).pipe(Effect.exit, Effect.provide(models.layer));

      expect(exit._tag).toBe('Failure');
      expect(models.asked).toHaveLength(2);
      expect(models.summaries).toHaveLength(1);
    }),
  );

  it.effect('does not retry when there is nothing older to summarize', () =>
    Effect.gen(function* () {
      const models = refusing(overflow, 99);
      const exit = yield* makeAgent({ ...POLICY, keepRecentTokens: 10_000 })
        .run('only recent')
        .pipe(Effect.exit, Effect.provide(models.layer));

      expect(exit._tag).toBe('Failure');
      expect(models.asked).toHaveLength(1);
      expect(models.summaries).toHaveLength(0);
    }),
  );

  it.effect('does not retry after partial output became visible', () =>
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
        const chat = yield* seededChat();
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

  // A summary that is empty or cut off must never replace history.
  it.effect.each([
    { summaryText: '' },
    { summaryFinishReason: 'length' as const },
  ])('fails the run rather than keep a bad summary %j', (options) =>
    Effect.gen(function* () {
      const models = fakeProvider({ limit: 150, ...options });
      const exit = yield* Effect.gen(function* () {
        return yield* reactive.runIn(yield* seededChat(), 'next');
      }).pipe(Effect.exit, Effect.provide(models.layer));

      expect(exit._tag).toBe('Failure');
      expect(models.summaries).toHaveLength(1);
      expect(models.asked).toHaveLength(1);
    }),
  );
});

describe('proactive compaction: an estimate crosses the threshold', () => {
  const windowed = makeAgent({ ...POLICY, contextWindow: 1_000 });

  it.effect('compacts before the turn, announces it, and bills it', () =>
    Effect.gen(function* () {
      const models = fakeProvider({ inputTokens: 50 });
      const events = yield* Effect.gen(function* () {
        return yield* Stream.runCollect(
          windowed.streamIn(yield* bulky(), 'next'),
        );
      }).pipe(Effect.provide(models.layer), Effect.orDie);

      // One summary, before the provider served any turn: nothing was rejected
      // and nothing re-run.
      expect(models.summaries).toHaveLength(1);
      expect(models.asked).toHaveLength(1);
      const sent = textOf(required(models.asked[0]));
      expect(sent).toContain('SUMMARY');
      expect(sent).not.toContain('a'.repeat(4_000));

      const announced = events.filter((event) => event._tag === 'Compacted');
      expect(announced).toHaveLength(1);
      expect(announced[0]).toMatchObject({ step: 1, summary: 'SUMMARY' });
      // Same place as the reactive path puts it: right after the turn opens.
      const tags = events.map((event) => event._tag);
      expect(tags.indexOf('Compacted')).toBe(tags.indexOf('TurnStarted') + 1);

      const completed = events.find((event) => event._tag === 'Completed');
      expect(completed).toMatchObject({ usage: { input: 60, output: 24 } });
    }),
  );

  it.effect('does nothing without a context window, and warns once', () =>
    Effect.gen(function* () {
      const models = fakeProvider({ inputTokens: 50 });
      const { messages, layer } = warnings();
      yield* Effect.gen(function* () {
        yield* makeAgent(POLICY, true).runIn(yield* bulky(), 'next');
      }).pipe(Effect.provide(Layer.merge(models.layer, layer)), Effect.orDie);

      expect(models.summaries).toHaveLength(0);
      expect(textOf(required(models.asked[0]))).toContain('a'.repeat(4_000));
      // Once per run, not once per turn.
      expect(messages).toHaveLength(1);
      expect(String(messages[0])).toContain('contextWindow');
    }),
  );

  it.effect.each([
    {
      when: 'a context window is set',
      compaction: { ...POLICY, contextWindow: 1_000 },
    },
    { when: 'compaction is disabled', compaction: false as const },
  ])('does not warn when $when', ({ compaction }) =>
    Effect.gen(function* () {
      const models = fakeProvider();
      const { messages, layer } = warnings();
      yield* Effect.gen(function* () {
        yield* makeAgent(compaction).runIn(yield* seededChat(), 'next');
      }).pipe(Effect.provide(Layer.merge(models.layer, layer)), Effect.orDie);

      expect(messages.map(String)).toEqual([]);
    }),
  );

  // The estimator is handed what the provider reported for the previous turn.
  it.effect('anchors each estimate on the previous turn’s reported usage', () =>
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

      yield* Effect.gen(function* () {
        yield* makeAgent({ ...POLICY, contextWindow: 1_000 }, true).runIn(
          yield* bulky(),
          'next',
        );
      }).pipe(
        Effect.provide(models.layer),
        Effect.provideService(ContextWindow.Service, recording),
        Effect.orDie,
      );

      expect(seen).toEqual([
        undefined,
        { inputTokens: 7_777, outputTokens: 20 },
      ]);
    }),
  );

  // Four characters of text could never cross a 100,000-token window by a
  // character count; the reported usage does.
  it.effect('trusts reported usage over a character count', () =>
    Effect.gen(function* () {
      const run = (heuristics: ContextWindow.Heuristics) => {
        const models = fakeProvider({ inputTokens: 98_000 });
        return Effect.gen(function* () {
          const chat = yield* Chat.fromPrompt(
            Prompt.make([
              message('user', 'o'.repeat(400)),
              message('assistant', 'p'.repeat(400)),
              message('user', 'tiny'),
            ]),
          );
          yield* makeAgent(
            { ...POLICY, reserveTokens: 20_000, contextWindow: 100_000 },
            true,
          ).runIn(chat, 'next');
          return models.summaries.length;
        }).pipe(
          Effect.provide(models.layer),
          Effect.provideService(ContextWindow.Service, heuristics),
          Effect.orDie,
        );
      };

      expect(yield* run(ContextWindow.pure)).toBe(0);
      expect(yield* run(ContextWindow.usageAnchored)).toBeGreaterThan(0);
    }),
  );
});

// Conversations resume from their log; a compaction must survive that.
//
// The provider rejects any prompt over LIMIT tokens, so whether the rebuild
// honoured the compaction is observable as the count of summarization calls and
// as the content of the last prompt.
describe('a conversation that compacted, resumed', () => {
  const LIMIT = 500;
  const BULK = 'L'.repeat(4000);

  const testLogLayer = Layer.mergeAll(
    LogStoreMemory.layer.pipe(Layer.provide(NodeServices.layer)),
    NodeServices.layer,
  );

  const conversationOf = (policy: Compaction.Policy, id: string) =>
    Conversation.make(
      makeAgent(policy),
      LogVocabulary.ConversationId.make(id),
      POLICY,
    );

  const run = <A, E>(
    effect: Effect.Effect<
      A,
      E,
      LogStore.Service | LanguageModel.LanguageModel | Crypto.Crypto
    >,
    models: Layer.Layer<LanguageModel.LanguageModel>,
  ): Effect.Effect<A> =>
    effect.pipe(
      Effect.orDie,
      Effect.provide(Layer.merge(models, testLogLayer)),
      Effect.scoped,
    );

  const conversation = conversationOf(POLICY, 'compacted-1');

  const scripted = () =>
    fakeProvider({
      limit: LIMIT,
      turns: [
        turnOf('a', BULK),
        turnOf('b', 'second answer'),
        turnOf('c', 'third answer'),
      ],
    });

  it.effect('compacts once, records it, and resumes from the summary', () =>
    Effect.gen(function* () {
      const models = scripted();
      const observed = yield* run(
        Effect.gen(function* () {
          yield* conversation.run('question one');
          yield* conversation.run('question two');
          const afterCompacting = models.summaries.length;
          yield* conversation.run('question three');
          return {
            afterCompacting,
            records: Array.from(
              yield* conversation.records().pipe(Stream.runCollect),
            ),
          };
        }),
        models.layer,
      );

      // Once for the conversation, not once per resumption: one stream call for
      // run one, two for run two (overflow and retry), one for run three.
      expect(observed.afterCompacting).toBe(1);
      expect(models.summaries).toHaveLength(1);
      expect(models.asked).toHaveLength(4);

      const compactions = observed.records.flatMap((envelope) =>
        envelope.record._tag === 'Compacted' ? [envelope.record] : [],
      );
      expect(compactions).toHaveLength(1);
      const record = required(compactions[0]);
      expect(record.summary).toBe('SUMMARY');
      expect(record.summarizedMessages).toBe(2);
      expect(record.keptMessages).toBe(1);

      // The pointer names the record that opened the kept tail: the second
      // run's `RunStarted`. Not a count.
      const secondRun = required(
        observed.records.filter((e) => e.record._tag === 'RunStarted')[1],
      );
      expect(record.firstKept).toBe(secondRun.offset);
      expect(record.firstKept).not.toBe(LogOffset.START);

      const completions = observed.records.filter(
        (e) => e.record._tag === 'Completed',
      );
      expect(completions[1]?.record).toMatchObject({
        usage: { input: 20, output: 8 },
      });

      const last = textOf(required(models.asked.at(-1)));
      expect(last).toContain('Summary of earlier conversation');
      expect(last).toContain('SUMMARY');
      expect(last).toContain('question two');
      expect(last).toContain('second answer');
      expect(last).toContain('question three');
      expect(last).not.toContain('question one');
      expect(last).not.toContain(BULK);
    }),
  );

  it.effect('compacts proactively against the current resumed input', () =>
    Effect.gen(function* () {
      const policy = { ...POLICY, contextWindow: 100, keepRecentTokens: 0 };
      const proactive = conversationOf(policy, 'compacted-proactive');
      const models = fakeProvider({
        turns: [turnOf('first', BULK), turnOf('second', 'second answer')],
      });

      const observed = yield* run(
        Effect.gen(function* () {
          yield* proactive.run('question one');
          const second = yield* proactive
            .run('question two')
            .pipe(Effect.result);
          const afterSecond = models.summaries.length;
          const third = yield* proactive
            .run('question three')
            .pipe(Effect.result);
          return { second, third, afterSecond };
        }),
        models.layer,
      );

      expect(observed.second).toMatchObject({
        _tag: 'Success',
        success: { text: 'second answer' },
      });
      expect(observed.third).toMatchObject({ _tag: 'Success' });
      expect(observed.afterSecond).toBe(1);
      expect(models.summaries).toHaveLength(1);
      expect(textOf(required(models.asked.at(-1)))).toContain('question two');
    }),
  );

  // Each later summary subsumes the earlier one, so a rebuild reads the latest
  // compaction only, and compacts again if even the summary is too big.
  it.effect('reads the latest of several compactions', () =>
    Effect.gen(function* () {
      const policy = { ...POLICY, contextWindow: 100, keepRecentTokens: 0 };
      const verbose = conversationOf(policy, 'compacted-verbose');
      const models = fakeProvider({
        summaryText: (call) => `SUMMARY-${String(call)}-${'S'.repeat(800)}`,
        turns: [turnOf('first', BULK), turnOf('second', 'answer')],
      });

      yield* run(
        Effect.gen(function* () {
          yield* verbose.run('question one');
          yield* verbose.run('question two');
          yield* verbose.run('question three');
        }),
        models.layer,
      );

      expect(models.summaries).toHaveLength(2);
      const last = textOf(required(models.asked.at(-1)));
      expect(last).toContain('SUMMARY-1-');
      expect(last).not.toContain('SUMMARY-0-');
    }),
  );

  // Hand-built records, because a trimmed log or a pointer from a differently
  // numbered version cannot be produced by running a conversation.
  it('rebuilds from the summary when the kept pointer is START or unknown', () => {
    for (const pointer of [LogOffset.START, LogOffset.fromSeq(900n)]) {
      const rebuilt = AgentHistory.messagesFrom(
        envelopes([
          started('old'),
          said('older answer'),
          finished,
          compacted('ALL OF IT', pointer),
          said('after'),
          finished,
        ]),
      );
      const body = JSON.stringify(rebuilt.content);
      expect(rebuilt.content.map((m) => m.role)).toEqual(['user', 'assistant']);
      expect(body).toContain('ALL OF IT');
      expect(body).toContain('after');
      expect(body).not.toContain('older answer');
    }
  });
});
