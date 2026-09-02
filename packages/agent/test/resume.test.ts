import { LogStoreMemory } from '@sunfall/vesper-log/layer-memory';
import { LogStore } from '@sunfall/vesper-log/log-store';
import { LogOffset } from '@sunfall/vesper-log/offset';
import type { ConversationRecord } from '@sunfall/vesper-log/record';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';
import * as NodeServices from '@effect/platform-node/NodeServices';
import { beforeEach, describe, expect, it } from '@effect/vitest';
import { type Crypto, Effect, Layer, Ref, Schema, Stream } from 'effect';
import {
  type LanguageModel as LanguageModelNamespace,
  LanguageModel,
  Prompt,
  type Response,
  Tool,
  Toolkit,
} from 'effect/unstable/ai';

import { Agent } from '../src/agent.js';
import { Conversation } from '../src/conversation.js';
import { ContextWindow } from '../src/context-window.js';
import { AgentEvents } from '../src/event.js';
import { AgentHistory } from '../src/history.js';
import { AgentHistory as AgentHistoryRuntime } from '../src/internal/history.js';
import * as AgentLog from '../src/log.js';

const testLogLayer = Layer.mergeAll(
  LogStoreMemory.layer.pipe(Layer.provide(NodeServices.layer)),
  NodeServices.layer,
);

const required = <A>(value: A | undefined): A => {
  if (value === undefined) {
    throw new Error('expected a value');
  }
  return value;
};

// Resumption from the log — the writer half, and the parity evidence for
// deleting `@sunfall/vesper-durable`'s checkpointer.
//
// The claim being tested is the one the roadmap rests the whole prune on: a
// conversation resumed from records does not re-pay the provider for turns it
// already completed, and does not re-run the tool calls those turns made.
// Checkpointing could offer the first of those, and only by replaying the loop
// from turn one to get there; it never offered the second, because tools run
// past the checkpoint boundary.
//
// Two things make these assertions non-vacuous rather than a restatement of
// the implementation:
//
//   - the provider counts the prompts it is handed, so "was not re-asked" is a
//     number, not an absence;
//   - the tool handler counts its own invocations, so "was not re-run" is too.
//
// Mutation-checked: making `AgentHistory.messagesFrom` return `Prompt.empty`
// fails `continues from what the crashed run left`, `carries the conversation
// into the next turn`, and every reconstruction case below.

const finish = (reason: 'stop' | 'tool-calls' = 'stop') => ({
  type: 'finish' as const,
  reason,
  usage: {
    inputTokens: { total: 10, uncached: 10, cacheRead: 0, cacheWrite: 0 },
    outputTokens: { total: 4 },
  },
});

const text = (
  id: string,
  deltas: ReadonlyArray<string>,
): Response.StreamPartEncoded[] => [
  { type: 'text-start' as const, id },
  ...deltas.map((delta) => ({ type: 'text-delta' as const, id, delta })),
  { type: 'text-end' as const, id },
];

/** Turn one: a sentence, then a tool call. */
const callingTurn: Response.StreamPartEncoded[] = [
  ...text('a', ['he', 'l', 'lo']),
  {
    type: 'tool-call' as const,
    id: 'call-1',
    name: 'lookup',
    params: { id: '42' },
  },
  finish('tool-calls'),
];

/** Turn two: an answer and nothing else, which is what stops the loop. */
const answeringTurn: Response.StreamPartEncoded[] = [
  ...text('b', ['do', 'ne']),
  finish(),
];

/** Turn three, so a third model call is distinguishable from a replayed one. */
const laterTurn: Response.StreamPartEncoded[] = [
  ...text('c', ['again']),
  finish(),
];

/** A provider-executed call has no Vesper ToolOutcome by design. */
const providerExecutedTurn: Response.StreamPartEncoded[] = [
  ...text('remote', ['searched']),
  {
    type: 'tool-call' as const,
    id: 'provider-call-1',
    name: 'lookup',
    params: { id: '42' },
    providerExecuted: true,
  },
  finish('tool-calls'),
];

/**
 * A scripted provider that keeps every prompt it was handed.
 *
 * The prompts are the evidence. "Did not re-ask for turn one" is
 * `asked.length`, and "resumed with the conversation" is what is in the last
 * one — neither of which can be read off the agent's own result.
 */
const provider = (
  turns: ReadonlyArray<ReadonlyArray<Response.StreamPartEncoded>>,
) => {
  const asked: Array<Prompt.Prompt> = [];

  const layer = Layer.effect(
    LanguageModel.LanguageModel,
    Effect.gen(function* () {
      const calls = yield* Ref.make(0);
      return yield* LanguageModel.make({
        generateText: () => Effect.succeed<Response.PartEncoded[]>([finish()]),
        streamText: (options: LanguageModelNamespace.ProviderOptions) =>
          Stream.unwrap(
            Effect.gen(function* () {
              asked.push(options.prompt);
              const index = yield* Ref.getAndUpdate(calls, (n) => n + 1);
              return Stream.fromIterable(
                required(turns[Math.min(index, turns.length - 1)]),
              );
            }),
          ),
      });
    }),
  );

  return { asked, layer };
};

const lookup = Tool.make('lookup', {
  description: 'look an order up',
  parameters: Schema.Struct({ id: Schema.String }),
  success: Schema.Struct({ status: Schema.String }),
});

/** Counts handler invocations, so "the tool did not run again" is a number. */
const dispatched = { count: 0 };

const agent = Agent.make({
  name: 'test',
  revision: '1',
  instructions: 'be terse',
  toolkit: Toolkit.make(lookup),
}).withHandlers({
  lookup: ({ id }) =>
    Effect.sync(() => {
      dispatched.count += 1;
      return { status: `shipped:${id}` };
    }),
});

const CONVERSATION = 'conversation-1';
const conversation = Conversation.make(agent, CONVERSATION);
const PATH = AgentLog.pathFor(LogVocabulary.ConversationId.make(CONVERSATION));

beforeEach(() => {
  dispatched.count = 0;
});

const run = <A, E>(
  effect: Effect.Effect<
    A,
    E,
    LogStore.Service | LanguageModel.LanguageModel | Crypto.Crypto
  >,
  models: Layer.Layer<LanguageModel.LanguageModel>,
) =>
  effect.pipe(Effect.orDie, Effect.provide(Layer.merge(models, testLogLayer)));

const rolesOf = (prompt: Prompt.Prompt) =>
  prompt.content.map((message) => message.role);

/** Every text fragment in a prompt, whatever role or part carries it. */
const textIn = (prompt: Prompt.Prompt): string =>
  JSON.stringify(prompt.content);

const readAll = Effect.fn('test.readAll')(function* (path: string = PATH) {
  const store = yield* LogStore.Service;
  const page = yield* store.read(path, { limit: 1000 });
  return page.records;
});

const envelopes = (
  records: ReadonlyArray<ConversationRecord.Record>,
): ReadonlyArray<ConversationRecord.Envelope> =>
  records.map((record, index) => ({
    offset: LogOffset.fromSeq(BigInt(index)),
    conversationId: LogVocabulary.ConversationId.make(CONVERSATION),
    timestamp: 0,
    record,
  }));

describe('resuming a crashed run', () => {
  // The parity test. A run is abandoned after its tool call settles — the
  // consumer walks away mid-conversation, which is what a crash looks like
  // from the log's side — and the conversation is then resumed.
  it.effect(
    'continues from what the crashed run left, without re-asking or re-running it',
    () =>
      Effect.gen(function* () {
        const models = provider([callingTurn, answeringTurn, laterTurn]);

        const observed = yield* run(
          Effect.gen(function* () {
            // Run one, abandoned the moment the tool result arrives.
            yield* conversation.stream('where is order 42?').pipe(
              Stream.takeUntil(
                (event) =>
                  AgentEvents.isPart(event) &&
                  event.part.type === 'tool-result',
              ),
              Stream.runDrain,
              Effect.orDie,
            );

            const afterCrash = yield* readAll();
            const askedDuringCrash = models.asked.length;
            const dispatchedDuringCrash = dispatched.count;

            // Run two: the same conversation, picked back up.
            const result = yield* conversation
              .run('and then?')
              .pipe(Effect.orDie);

            return {
              afterCrash: afterCrash.map((envelope) => envelope.record._tag),
              askedDuringCrash,
              dispatchedDuringCrash,
              asked: models.asked,
              dispatchedTotal: dispatched.count,
              result,
            };
          }),
          models.layer,
        );

        // The crashed run got one turn's worth of records and no `Completed`.
        expect(observed.afterCrash).toEqual([
          'RunStarted',
          'ToolStarted',
          'Text',
          'ToolCall',
          'ToolOutcome',
          'RunSettled',
        ]);
        expect(observed.askedDuringCrash).toBe(1);
        expect(observed.dispatchedDuringCrash).toBe(1);

        // One further provider call, for the turn that had not happened yet. The
        // completed turn was not re-asked — under replay-from-checkpoint recovery
        // this number is two, because recovery re-runs the loop from turn one.
        expect(observed.asked).toHaveLength(2);

        // And the tool that already ran did not run again. Checkpointing never
        // covered this at all: tool execution happens past the provider seam.
        expect(observed.dispatchedTotal).toBe(1);

        // The resumed call was given the conversation, not a fresh start.
        const resumed = required(observed.asked[1]);
        expect(rolesOf(resumed)).toEqual([
          'system',
          'user',
          'assistant',
          'tool',
          'user',
        ]);
        expect(textIn(resumed)).toContain('where is order 42?');
        expect(textIn(resumed)).toContain('hello');
        expect(textIn(resumed)).toContain('call-1');
        expect(textIn(resumed)).toContain('shipped:42');
        expect(textIn(resumed)).toContain('and then?');

        expect(observed.result.text).toBe('done');
      }),
  );

  // The resumed run is an ordinary recorded run: it appends to the same
  // conversation rather than starting a second history beside it.
  it.effect('records the resumed run into the same conversation', () =>
    Effect.gen(function* () {
      const models = provider([callingTurn, answeringTurn, laterTurn]);

      const tags = yield* run(
        Effect.gen(function* () {
          yield* conversation.stream('hi').pipe(
            Stream.takeUntil(
              (event) =>
                AgentEvents.isPart(event) && event.part.type === 'tool-result',
            ),
            Stream.runDrain,
            Effect.orDie,
          );

          yield* conversation.run('and then?').pipe(Effect.orDie);
          return (yield* readAll()).map((envelope) => envelope.record._tag);
        }),
        models.layer,
      );

      expect(tags).toEqual([
        'RunStarted',
        'ToolStarted',
        'Text',
        'ToolCall',
        'ToolOutcome',
        'RunSettled',
        'RunStarted',
        'Text',
        'TurnFinished',
        'Completed',
        'RunSettled',
      ]);
    }),
  );

  // `run:before-completed`: a run whose final turn already durably finished
  // — `Text` and `TurnFinished` both settled — but whose own `Completed`
  // (and the successful `RunSettled` that would carry it forward) never got
  // appended before the run was abandoned. Recovery has no tool call to
  // reconcile and no unanswered call to drop from the rebuilt prompt: the
  // model's answer is already fully durable, so resuming must settle from
  // that history instead of asking the provider a second time for a turn it
  // already received in full.
  it.effect(
    'settles from a durable TurnFinished instead of re-asking the model when only Completed is missing',
    () =>
      Effect.gen(function* () {
        const models = provider([answeringTurn, laterTurn]);

        const observed = yield* run(
          Effect.gen(function* () {
            yield* conversation.stream('hi').pipe(
              Stream.takeUntil((event) => event._tag === 'TurnFinished'),
              Stream.runDrain,
              Effect.orDie,
            );

            const afterCrash = yield* readAll();
            const askedDuringCrash = models.asked.length;

            const result = yield* conversation.run().pipe(Effect.orDie);

            return {
              afterCrash: afterCrash.map((envelope) => envelope.record._tag),
              askedDuringCrash,
              askedTotal: models.asked.length,
              result,
            };
          }),
          models.layer,
        );

        // Durable through TurnFinished; Completed and a successful
        // RunSettled never got appended before the run was abandoned.
        expect(observed.afterCrash).toEqual([
          'RunStarted',
          'Text',
          'TurnFinished',
          'RunSettled',
        ]);
        expect(observed.askedDuringCrash).toBe(1);

        // Exactly zero further provider calls — the invariant this fix
        // exists for. Before the fix this was 2: a redundant call for a
        // turn whose text and usage were already fully durable.
        expect(observed.askedTotal).toBe(1);
        expect(observed.result.text).toBe('done');
        expect(observed.result.usage).toEqual({ input: 10, output: 4 });
      }),
  );

  // BUG, partially closed: a run interrupted strictly inside the
  // `ToolStarted`..`ToolOutcome` window — after the tool's own durable
  // outcome, but before its enclosing turn's `TurnFinished` — cannot
  // recover that turn's real provider usage from any durable record.
  // `TurnFinished` is the only record that carries it, and Effect AI defers
  // emitting the model's finish part (which is what carries usage) until
  // after automatic tool resolution completes (see CONTEXT.md's Turn
  // entry) — so at the crash point nothing in the process, durable or not,
  // yet knows that turn's cost. What is fixable, and is what this asserts,
  // is that the interrupted run stops caching that necessarily-incomplete
  // guess as a *verified* checkpoint: its own `RunSettled` carries no
  // `resume`, so a later open always re-derives `usage` fresh from durable
  // records (`usageFrom` over the whole log) instead of trusting a cached
  // snapshot that could drift from it or compound across a second crash.
  it.effect(
    "does not cache an interrupted run's own guessed usage as a verified checkpoint",
    () =>
      Effect.gen(function* () {
        const models = provider([callingTurn, answeringTurn]);

        const observed = yield* run(
          Effect.gen(function* () {
            yield* conversation.stream('where is order 42?').pipe(
              Stream.takeUntil(
                (event) =>
                  AgentEvents.isPart(event) &&
                  event.part.type === 'tool-result',
              ),
              Stream.runDrain,
              Effect.orDie,
            );

            const records = yield* readAll();
            const opened = yield* AgentLog.open(
              LogVocabulary.ConversationId.make(CONVERSATION),
              {
                compatibility: {
                  agent: 'test',
                  revision: LogVocabulary.AgentRevision.make('1'),
                },
              },
            );

            return { records, sessionUsage: opened.usage };
          }),
          models.layer,
        );

        const settled = required(
          observed.records.find(
            (envelope) => envelope.record._tag === 'RunSettled',
          ),
        ).record;
        if (settled._tag !== 'RunSettled') {
          throw new Error('expected a RunSettled record');
        }
        // The fix: an interrupted run's own settlement never becomes a
        // trusted resume boundary.
        expect(settled.resume).toBeUndefined();

        // What remains true, and is not this fix's to close: the model's
        // real turn-one usage never became durable at all, so a fresh fold
        // over every durable record gives the same (necessarily short)
        // figure a cached checkpoint would have — reopening no longer
        // trusts a wrong number instead of a merely incomplete one, which
        // is the difference this fix makes.
        expect(observed.sessionUsage).toEqual({ input: 0, output: 0 });
        expect(observed.sessionUsage).toEqual(
          AgentHistoryRuntime.usageFrom(observed.records),
        );
      }),
  );
});

describe('resuming an ordinary conversation', () => {
  it.effect('starts a new conversation when there is nothing to resume', () =>
    Effect.gen(function* () {
      const models = provider([answeringTurn]);

      const asked = yield* run(
        Effect.gen(function* () {
          yield* Conversation.make(agent, 'fresh')
            .run('hello')
            .pipe(Effect.orDie);
          return models.asked;
        }),
        models.layer,
      );

      expect(rolesOf(required(asked[0]))).toEqual(['system', 'user']);
      expect(textIn(required(asked[0]))).toContain('be terse');
    }),
  );

  // The property that makes this a conversation rather than a series of
  // unrelated runs.
  it.effect('carries the conversation into the next turn', () =>
    Effect.gen(function* () {
      const models = provider([answeringTurn, laterTurn]);

      const asked = yield* run(
        Effect.gen(function* () {
          yield* conversation.run('first').pipe(Effect.orDie);
          yield* conversation.run('second').pipe(Effect.orDie);
          return models.asked;
        }),
        models.layer,
      );

      expect(rolesOf(required(asked[1]))).toEqual([
        'system',
        'user',
        'assistant',
        'user',
      ]);
      expect(textIn(required(asked[1]))).toContain('first');
      expect(textIn(required(asked[1]))).toContain('done');
      expect(textIn(required(asked[1]))).toContain('second');
    }),
  );

  it.effect('streams the same continuation and lifetime usage as run', () =>
    Effect.gen(function* () {
      const models = provider([answeringTurn, laterTurn]);

      const observed = yield* run(
        Effect.gen(function* () {
          const first = yield* conversation.run('first').pipe(Effect.orDie);
          const events = yield* conversation
            .stream('second')
            .pipe(Stream.runCollect, Effect.orDie);
          const completed = Array.from(events).find(
            (event) => event._tag === 'Completed',
          );
          return { first, completed, asked: models.asked };
        }),
        models.layer,
      );

      expect(textIn(required(observed.asked[1]))).toContain('first');
      expect(textIn(required(observed.asked[1]))).toContain('second');
      expect(observed.completed).toMatchObject({
        _tag: 'Completed',
        usage: {
          input: observed.first.usage.input * 2,
          output: observed.first.usage.output * 2,
        },
      });
    }),
  );

  it.effect('keeps separate conversations separate', () =>
    Effect.gen(function* () {
      const models = provider([answeringTurn, laterTurn]);

      const asked = yield* run(
        Effect.gen(function* () {
          yield* Conversation.make(agent, 'conv-a')
            .run('first')
            .pipe(Effect.orDie);
          yield* Conversation.make(agent, 'conv-b')
            .run('first')
            .pipe(Effect.orDie);
          return models.asked;
        }),
        models.layer,
      );

      expect(rolesOf(required(asked[1]))).toEqual(['system', 'user']);
    }),
  );

  // A resumed conversation that reset the count would under-report every turn
  // after the first, which is the number anyone asking about cost wants.
  it.effect('accumulates usage across the life of the conversation', () =>
    Effect.gen(function* () {
      const models = provider([answeringTurn, laterTurn]);

      const totals = yield* run(
        Effect.gen(function* () {
          const first = yield* conversation.run('first').pipe(Effect.orDie);
          const second = yield* conversation.run('second').pipe(Effect.orDie);
          return { first: first.usage, second: second.usage };
        }),
        models.layer,
      );

      expect(totals.second.output).toBe(totals.first.output * 2);
      expect(totals.second.input).toBe(totals.first.input * 2);
    }),
  );

  it.effect(
    'restores the latest turn usage as the resumed estimator anchor',
    () =>
      Effect.gen(function* () {
        const models = provider([answeringTurn, laterTurn]);
        const seen: Array<ContextWindow.TurnUsage | undefined> = [];
        const heuristics: ContextWindow.Heuristics = {
          estimate: (_prompt, usage) => {
            seen.push(usage);
            return { tokens: 0, usageTokens: 0, trailingTokens: 0 };
          },
          shouldCompact: () => false,
        };
        const anchored = Agent.make({
          name: 'anchored',
          revision: '1',
          instructions: 'be terse',
          toolkit: Toolkit.make(),
          compaction: {
            contextWindow: 1_000,
            reserveTokens: 100,
            keepRecentTokens: 10,
            instructions: 'sum',
          },
        });

        yield* run(
          Effect.gen(function* () {
            const anchoredConversation = Conversation.make(
              anchored,
              CONVERSATION,
            );
            yield* anchoredConversation.run('first');
            yield* anchoredConversation.run('second');
          }).pipe(Effect.provideService(ContextWindow.Service, heuristics)),
          models.layer,
        );

        expect(seen).toEqual([undefined, { inputTokens: 10, outputTokens: 4 }]);
      }),
  );
});

describe('resuming provider-executed tool calls', () => {
  it.effect('keeps a provider-executed call without a Vesper outcome', () =>
    Effect.gen(function* () {
      const models = provider([providerExecutedTurn, answeringTurn, laterTurn]);

      const records = yield* run(
        Effect.gen(function* () {
          yield* conversation.run('search remotely');
          yield* conversation.run('continue');
          return yield* conversation.records().pipe(Stream.runCollect);
        }),
        models.layer,
      );

      const recordedCall = Array.from(records).find(
        (envelope) => envelope.record._tag === 'ToolCall',
      );
      expect(recordedCall?.record).toMatchObject({
        _tag: 'ToolCall',
        providerExecuted: true,
      });

      const assistant = required(models.asked[2]).content.find(
        (message) => message.role === 'assistant',
      );
      const toolCall = assistant?.content.find(
        (part) => part.type === 'tool-call' && part.id === 'provider-call-1',
      );
      expect(toolCall).toMatchObject({
        type: 'tool-call',
        providerExecuted: true,
      });
    }),
  );
});

describe('rebuilding a prompt from records', () => {
  it('groups a turn into an assistant message and its tool results', () => {
    const rebuilt = AgentHistory.messagesFrom(
      envelopes([
        {
          _tag: 'RunStarted',
          agent: 'test',
          formatVersion: 1,
          agentRevision: LogVocabulary.AgentRevision.make('1'),
          prompt: Prompt.make('hi').content,
        },
        { _tag: 'Text', step: 1, text: 'looking' },
        {
          _tag: 'ToolCall',
          step: 1,
          id: LogVocabulary.ToolCallId.make('call-1'),
          name: 'lookup',
          params: { id: '42' },
        },
        {
          _tag: 'ToolOutcome',
          step: 1,
          id: LogVocabulary.ToolCallId.make('call-1'),
          name: 'lookup',
          outcome: 'success',
          result: { status: 'shipped:42' },
        },
        { _tag: 'TurnFinished', step: 1, usage: { input: 1, output: 1 } },
      ]),
    );

    expect(rolesOf(rebuilt)).toEqual(['user', 'assistant', 'tool']);
    expect(rebuilt.content[1]?.content).toMatchObject([
      { type: 'text', text: 'looking' },
      { type: 'tool-call', id: 'call-1', name: 'lookup' },
    ]);
    expect(rebuilt.content[2]?.content).toMatchObject([
      { type: 'tool-result', id: 'call-1', isFailure: false },
    ]);
  });

  // The mid-turn crash. An assistant tool call with no matching result is not
  // a degraded prompt — providers reject the request outright — so it is
  // dropped and the model may ask again.
  it('drops a tool call whose outcome was never recorded', () => {
    const rebuilt = AgentHistory.messagesFrom(
      envelopes([
        {
          _tag: 'RunStarted',
          agent: 'test',
          formatVersion: 1,
          agentRevision: LogVocabulary.AgentRevision.make('1'),
          prompt: Prompt.make('hi').content,
        },
        { _tag: 'Text', step: 1, text: 'looking' },
        {
          _tag: 'ToolCall',
          step: 1,
          id: LogVocabulary.ToolCallId.make('call-1'),
          name: 'lookup',
          params: { id: '42' },
        },
        {
          _tag: 'ToolOutcome',
          step: 1,
          id: LogVocabulary.ToolCallId.make('call-1'),
          name: 'lookup',
          outcome: 'success',
          result: { status: 'shipped:42' },
        },
        {
          _tag: 'ToolCall',
          step: 1,
          id: LogVocabulary.ToolCallId.make('call-2'),
          name: 'lookup',
          params: { id: '43' },
        },
      ]),
    );

    expect(rolesOf(rebuilt)).toEqual(['user', 'assistant', 'tool']);
    expect(JSON.stringify(rebuilt.content)).toContain('call-1');
    expect(JSON.stringify(rebuilt.content)).not.toContain('call-2');
  });

  // A steer became a user message on the next turn, so it belongs after the
  // turn that consumed it — not before the model's own words, which is where
  // record order alone would put it.
  it('replays a steer as the user message it became', () => {
    const rebuilt = AgentHistory.messagesFrom(
      envelopes([
        {
          _tag: 'RunStarted',
          agent: 'test',
          formatVersion: 1,
          agentRevision: LogVocabulary.AgentRevision.make('1'),
          prompt: Prompt.make('hi').content,
        },
        { _tag: 'Text', step: 1, text: 'working' },
        {
          _tag: 'SignalReceived',
          kind: 'steer',
          text: 'also check the invoice',
          source: 'operator',
          step: 1,
          at: LogOffset.fromSeq(1n),
        },
        { _tag: 'TurnFinished', step: 1, usage: { input: 1, output: 1 } },
      ]),
    );

    expect(rolesOf(rebuilt)).toEqual(['user', 'assistant', 'user']);
    expect(JSON.stringify(rebuilt.content[2])).toContain(
      'also check the invoice',
    );
  });

  it('ignores a cancel, which changed no prompt', () => {
    const rebuilt = AgentHistory.messagesFrom(
      envelopes([
        {
          _tag: 'RunStarted',
          agent: 'test',
          formatVersion: 1,
          agentRevision: LogVocabulary.AgentRevision.make('1'),
          prompt: Prompt.make('hi').content,
        },
        {
          _tag: 'SignalReceived',
          kind: 'cancel',
          text: 'stop',
          source: 'operator',
          step: 1,
          at: LogOffset.fromSeq(1n),
        },
      ]),
    );

    expect(rolesOf(rebuilt)).toEqual(['user']);
  });

  // `Completed.text` repeats the final turn's `Text` records. Reading both
  // would say everything twice, which the model would read as a stutter.
  it('does not repeat the final answer from Completed', () => {
    const rebuilt = AgentHistory.messagesFrom(
      envelopes([
        {
          _tag: 'RunStarted',
          agent: 'test',
          formatVersion: 1,
          agentRevision: LogVocabulary.AgentRevision.make('1'),
          prompt: Prompt.make('hi').content,
        },
        { _tag: 'Text', step: 1, text: 'done' },
        { _tag: 'TurnFinished', step: 1, usage: { input: 1, output: 1 } },
        {
          _tag: 'Completed',
          text: 'done',
          steps: 1,
          usage: { input: 1, output: 1 },
        },
        {
          _tag: 'RunSettled',
          outcome: 'success',
          detail: '',
          steps: 1,
          usage: { input: 1, output: 1 },
        },
      ]),
    );

    expect(JSON.stringify(rebuilt.content).match(/done/g)).toHaveLength(1);
  });

  it('is empty for a conversation with no records', () => {
    expect(AgentHistory.messagesFrom([]).content).toEqual([]);
  });

  it('sums usage across runs rather than reporting the last one', () => {
    const usage = AgentHistory.usageFrom(
      envelopes([
        {
          _tag: 'RunStarted',
          agent: 'test',
          formatVersion: 1,
          agentRevision: LogVocabulary.AgentRevision.make('1'),
          prompt: [],
        },
        { _tag: 'TurnFinished', step: 1, usage: { input: 3, output: 1 } },
        {
          _tag: 'RunSettled',
          outcome: 'success',
          detail: '',
          steps: 1,
          usage: { input: 3, output: 1 },
        },
        {
          _tag: 'RunStarted',
          agent: 'test',
          formatVersion: 1,
          agentRevision: LogVocabulary.AgentRevision.make('1'),
          prompt: [],
        },
        { _tag: 'TurnFinished', step: 1, usage: { input: 5, output: 2 } },
        {
          _tag: 'RunSettled',
          outcome: 'success',
          detail: '',
          steps: 1,
          usage: { input: 5, output: 2 },
        },
      ]),
    );

    expect(usage).toEqual({ input: 8, output: 3 });
  });

  it('restores only the latest turn own usage from cumulative records', () => {
    const usage = AgentHistoryRuntime.latestTurnUsageFrom(
      envelopes([
        {
          _tag: 'RunStarted',
          agent: 'test',
          formatVersion: 1,
          agentRevision: LogVocabulary.AgentRevision.make('1'),
          prompt: [],
        },
        { _tag: 'TurnFinished', step: 1, usage: { input: 10, output: 4 } },
        { _tag: 'TurnFinished', step: 2, usage: { input: 25, output: 10 } },
        {
          _tag: 'RunSettled',
          outcome: 'success',
          detail: '',
          steps: 2,
          usage: { input: 25, output: 10 },
        },
      ]),
    );

    expect(usage).toEqual({ input: 15, output: 6 });
  });
});
