import { LogStoreMemory } from '@sunfall/vesper-log/layer-memory';
import { LogStore } from '@sunfall/vesper-log/log-store';
import { LogOffset } from '@sunfall/vesper-log/offset';
import type { ConversationRecord } from '@sunfall/vesper-log/record';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';
import * as NodeServices from '@effect/platform-node/NodeServices';
import { describe, expect, it } from '@effect/vitest';
import {
  Crypto,
  Deferred,
  Effect,
  Fiber,
  Layer,
  Ref,
  Schema,
  Stream,
} from 'effect';
import {
  LanguageModel,
  type Response,
  Tool,
  Toolkit,
} from 'effect/unstable/ai';

import { Agent } from '../src/agent.js';
import { Conversation } from '../src/conversation.js';
import { AgentLog } from '../src/log.js';

const testLogLayer = Layer.mergeAll(
  LogStoreMemory.layer.pipe(Layer.provide(NodeServices.layer)),
  NodeServices.layer,
);

// What these have to prove, beyond "records appear":
//
//   - text is coalesced, so the row count is a function of the conversation
//     and not of the provider's chunking;
//   - the append lands *before* the consumer sees the event it describes;
//   - an agent with no `LogStore` is unchanged, at the type level as well as
//     at runtime;
//   - `streamFrom` follows live rather than only replaying history.
//
// Each of those is mutation-checked; the mutations are named in the report
// for this change.

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

/** Turn one: three deltas of one sentence, then a tool call. */
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

const scripted = (
  turns: ReadonlyArray<ReadonlyArray<Response.StreamPartEncoded>>,
) =>
  Layer.effect(
    LanguageModel.LanguageModel,
    Effect.gen(function* () {
      const calls = yield* Ref.make(0);
      return yield* LanguageModel.make({
        generateText: () => Effect.succeed<Response.PartEncoded[]>([finish()]),
        streamText: () =>
          Stream.unwrap(
            Effect.gen(function* () {
              const index = yield* Ref.getAndUpdate(calls, (n) => n + 1);
              return Stream.fromIterable(
                turns[Math.min(index, turns.length - 1)]!,
              );
            }),
          ),
      });
    }),
  );

const lookup = Tool.make('lookup', {
  description: 'look an order up',
  parameters: Schema.Struct({ id: Schema.String }),
  success: Schema.Struct({ status: Schema.String }),
});

const agent = Agent.make({
  name: 'test',
  revision: '1',
  instructions: 'be terse',
  toolkit: Toolkit.make(lookup),
}).withHandlers({
  lookup: ({ id }) => Effect.succeed({ status: `shipped:${id}` }),
});

const transformedLookup = Tool.make('transformed_lookup', {
  description: 'look an order up at a specific time',
  parameters: Schema.Struct({ at: Schema.DateFromString }),
  success: Schema.Struct({ status: Schema.String }),
});

const transformedAgent = Agent.make({
  name: 'transformed-params',
  revision: '1',
  instructions: 'be terse',
  toolkit: Toolkit.make(transformedLookup),
}).withHandlers({
  transformed_lookup: ({ at }) =>
    Effect.succeed({ status: `at:${at.toISOString()}` }),
});

const CONVERSATION = LogVocabulary.ConversationId.make('conversation-1');
const PATH = AgentLog.pathFor(CONVERSATION);

const TRANSFORMED_CONVERSATION = LogVocabulary.ConversationId.make(
  'transformed-params-conversation',
);
const transformedCallingTurn: Response.StreamPartEncoded[] = [
  {
    type: 'tool-call',
    id: 'transformed-call-1',
    name: 'transformed_lookup',
    params: { at: '2026-01-02T03:04:05.000Z' },
  },
  finish('tool-calls'),
];

const runTransformed = <A, E>(
  effect: Effect.Effect<
    A,
    E,
    LogStore.Service | LanguageModel.LanguageModel | Crypto.Crypto
  >,
) =>
  effect.pipe(
    Effect.orDie,
    Effect.provide(scripted([transformedCallingTurn, answeringTurn])),
    Effect.provide(testLogLayer),
  );

/** Defects fail the test with their real cause rather than a cast. */
const run = <A, E>(
  effect: Effect.Effect<
    A,
    E,
    LogStore.Service | LanguageModel.LanguageModel | Crypto.Crypto
  >,
) =>
  effect.pipe(
    Effect.orDie,
    Effect.provide(scripted([callingTurn, answeringTurn])),
    Effect.provide(testLogLayer),
  );

const readAll = Effect.fn('test.readAll')(function* (path = PATH) {
  const store = yield* LogStore.Service;
  const page = yield* store.read(path, { limit: 1000 });
  return page.records;
});

const tags = (records: ReadonlyArray<ConversationRecord.Envelope>) =>
  records.map((envelope) => envelope.record._tag);

const textsOf = (records: ReadonlyArray<ConversationRecord.Envelope>) =>
  records.flatMap((envelope) =>
    envelope.record._tag === 'Text' ? [envelope.record] : [],
  );

describe('recording a run', () => {
  it.effect('serializes concurrent appends through one producer sequence', () =>
    Effect.gen(function* () {
      const written = yield* run(
        Effect.gen(function* () {
          const session = yield* AgentLog.open(CONVERSATION, {
            compatibility: {
              agent: 'test',
              revision: LogVocabulary.AgentRevision.make('1'),
            },
          });
          yield* Effect.all(
            Array.from({ length: 50 }, (_, index) =>
              session.append([{ _tag: 'Text', step: 1, text: String(index) }]),
            ),
            { concurrency: 'unbounded' },
          );
          return yield* readAll();
        }),
      );

      expect(written).toHaveLength(50);
      expect(new Set(textsOf(written).map((record) => record.text)).size).toBe(
        50,
      );
    }),
  );

  it.effect('writes one record per thing that happened, in order', () =>
    Effect.gen(function* () {
      const written = yield* run(
        Effect.gen(function* () {
          yield* Conversation.make(agent, CONVERSATION)
            .run('hi')
            .pipe(Effect.orDie);
          return yield* readAll();
        }),
      );

      expect(tags(written)).toEqual([
        'RunStarted',
        'ToolStarted',
        'Text',
        'ToolCall',
        'ToolOutcome',
        'TurnFinished',
        'Text',
        'TurnFinished',
        'Completed',
        'RunSettled',
      ]);
    }),
  );

  // The coalescing claim, stated as a number. Three deltas arrived; one row
  // was written. Appending per delta makes this eight rows of text.
  it.effect('coalesces contiguous deltas into one Text record', () =>
    Effect.gen(function* () {
      const written = yield* run(
        Effect.gen(function* () {
          yield* Conversation.make(agent, CONVERSATION)
            .run('hi')
            .pipe(Effect.orDie);
          return yield* readAll();
        }),
      );

      expect(textsOf(written)).toEqual([
        { _tag: 'Text', step: 1, text: 'hello' },
        { _tag: 'Text', step: 2, text: 'done' },
      ]);
    }),
  );

  it.effect('records what the tool was asked and what it answered', () =>
    Effect.gen(function* () {
      const written = yield* run(
        Effect.gen(function* () {
          yield* Conversation.make(agent, CONVERSATION)
            .run('hi')
            .pipe(Effect.orDie);
          return yield* readAll();
        }),
      );

      expect(written.map((envelope) => envelope.record)).toContainEqual({
        _tag: 'ToolCall',
        step: 1,
        id: 'call-1',
        name: 'lookup',
        params: { id: '42' },
      });
      expect(written.map((envelope) => envelope.record)).toContainEqual({
        _tag: 'ToolOutcome',
        step: 1,
        id: 'call-1',
        name: 'lookup',
        outcome: 'success',
        result: { status: 'shipped:42' },
      });
    }),
  );

  it.effect('persists transformed tool parameters in encoded form', () =>
    Effect.gen(function* () {
      const written = yield* runTransformed(
        Effect.gen(function* () {
          yield* Conversation.make(transformedAgent, TRANSFORMED_CONVERSATION)
            .run('hi')
            .pipe(Effect.orDie);
          return yield* readAll(AgentLog.pathFor(TRANSFORMED_CONVERSATION));
        }),
      );

      const toolCall = written.find(
        (envelope) => envelope.record._tag === 'ToolCall',
      )?.record;
      expect(toolCall).toMatchObject({
        _tag: 'ToolCall',
        name: 'transformed_lookup',
        params: { at: '2026-01-02T03:04:05.000Z' },
      });
    }),
  );

  it.effect('records the run’s input and the result the caller got', () =>
    Effect.gen(function* () {
      const observed = yield* run(
        Effect.gen(function* () {
          const result = yield* Conversation.make(agent, CONVERSATION)
            .run('where is order 42?')
            .pipe(Effect.orDie);
          return { result, written: yield* readAll() };
        }),
      );

      expect(observed.written[0]?.record).toMatchObject({
        _tag: 'RunStarted',
        agent: 'test',
        formatVersion: 1,
        agentRevision: '1',
      });
      expect(
        observed.written.map((envelope) => envelope.record),
      ).toContainEqual(
        expect.objectContaining({
          _tag: 'Completed',
          text: observed.result.text,
          steps: observed.result.steps,
          usage: observed.result.usage,
        }),
      );
    }),
  );

  // The ordering rule that replaced `@sunfall/vesper-durable`'s "withhold `finish`
  // until the checkpoint is durable": a consumer must never act on something
  // the durable record does not yet contain. Forking the append, or moving it
  // after the emit, breaks exactly this.
  it.effect(
    'has already written the record when the consumer sees the event',
    () =>
      Effect.gen(function* () {
        const atToolCall = yield* run(
          Effect.gen(function* () {
            const store = yield* LogStore.Service;
            const seen: string[] = [];

            yield* Conversation.make(agent, CONVERSATION)
              .stream('hi')
              .pipe(
                Stream.runForEach((event) =>
                  event._tag === 'Part' &&
                  (event.part as Response.StreamPartEncoded).type ===
                    'tool-call'
                    ? Effect.gen(function* () {
                        const page = yield* store.read(PATH, { limit: 1000 });
                        seen.push(...tags(page.records));
                      })
                    : Effect.void,
                ),
                Effect.orDie,
              );

            return seen;
          }),
        );

        expect(atToolCall).toEqual([
          'RunStarted',
          'ToolStarted',
          'Text',
          'ToolCall',
        ]);
      }),
  );
});

describe('logging is optional', () => {
  it.effect('runs an unrecorded agent with no LogStore in context', () =>
    Effect.gen(function* () {
      // No `LogStoreMemory.layer` anywhere in this pipeline. If `run` required
      // a `LogStore`, this would not compile — which is the assertion.
      const result = yield* agent
        .run('hi')
        .pipe(
          Effect.orDie,
          Effect.provide(scripted([callingTurn, answeringTurn])),
        );

      expect(result.text).toBe('done');
    }),
  );

  it.effect('writes nothing for a run that was never told where to write', () =>
    Effect.gen(function* () {
      const written = yield* run(
        Effect.gen(function* () {
          yield* agent.run('hi').pipe(Effect.orDie);
          const store = yield* LogStore.Service;
          return yield* store.meta(PATH);
        }),
      );

      // Not "an empty stream" — no stream at all. A `LogStore` being reachable
      // is not consent to write to it.
      expect(written._tag).toBe('None');
    }),
  );

  it('puts LogStore in the type for a conversation', () => {
    type EffR<T> =
      T extends Effect.Effect<unknown, unknown, infer R> ? R : never;
    type Plain = Agent.Requires<typeof agent>;
    type Recording = ReturnType<typeof Conversation.make>;

    // Both fail to compile if binding a conversation stops changing the
    // requirement channel, which is what keeps this from being a comment.
    const plainIsFree: LogStore.Service extends Plain ? false : true = true;
    type RecordingRequirements = EffR<ReturnType<Recording['run']>>;
    const recordingNeedsIt: LogStore.Service extends RecordingRequirements
      ? true
      : false = true;

    expect([plainIsFree, recordingNeedsIt]).toEqual([true, true]);
  });
});

describe('Conversation.records', () => {
  it.effect('replays what was recorded, oldest first', () =>
    Effect.gen(function* () {
      const replayed = yield* run(
        Effect.gen(function* () {
          const conversation = Conversation.make(agent, CONVERSATION);
          yield* conversation.run('hi').pipe(Effect.orDie);

          return yield* conversation
            .records()
            .pipe(Stream.take(9), Stream.runCollect, Effect.orDie);
        }),
      );

      expect(tags(replayed)).toEqual([
        'RunStarted',
        'ToolStarted',
        'Text',
        'ToolCall',
        'ToolOutcome',
        'TurnFinished',
        'Text',
        'TurnFinished',
        'Completed',
      ]);
    }),
  );

  it.effect('resumes after an offset rather than from the beginning', () =>
    Effect.gen(function* () {
      const replayed = yield* run(
        Effect.gen(function* () {
          const conversation = Conversation.make(agent, CONVERSATION);
          yield* conversation.run('hi').pipe(Effect.orDie);
          const written = yield* readAll();

          return yield* conversation
            .records(written[3]!.offset)
            .pipe(Stream.take(1), Stream.runCollect, Effect.orDie);
        }),
      );

      expect(tags(replayed)).toEqual(['ToolOutcome']);
    }),
  );

  // The half that makes it a tail and not a query. The reader is held open
  // until it has drained every historical record, and only then is a new one
  // appended — so this record cannot reach it through catch-up.
  it.effect('follows records appended after the reader caught up', () =>
    Effect.gen(function* () {
      const late = yield* run(
        Effect.gen(function* () {
          const conversation = Conversation.make(agent, CONVERSATION);
          yield* conversation.run('hi').pipe(Effect.orDie);
          const store = yield* LogStore.Service;
          const historical = (yield* readAll()).length;

          const caughtUp = yield* Deferred.make<void>();
          const seen = yield* Ref.make(0);

          const reader = yield* conversation.follow().pipe(
            Stream.tap(() =>
              Effect.gen(function* () {
                const count = yield* Ref.updateAndGet(seen, (n) => n + 1);
                if (count === historical) {
                  yield* Deferred.succeed(caughtUp, undefined);
                }
              }),
            ),
            Stream.take(historical + 1),
            Stream.runCollect,
            Effect.orDie,
            Effect.forkChild,
          );

          yield* Deferred.await(caughtUp);

          const claim = yield* store
            .acquire(PATH, LogVocabulary.ProducerId.make('late-producer'))
            .pipe(Effect.orDie);
          yield* store
            .append({
              path: PATH,
              producerId: claim.producerId,
              epoch: claim.epoch,
              sequence: claim.nextSequence,
              records: [
                {
                  conversationId: CONVERSATION,
                  timestamp: 1_700_000_000_000,
                  record: { _tag: 'Text', step: 3, text: 'appended later' },
                },
              ],
            })
            .pipe(Effect.orDie);

          return yield* Fiber.join(reader);
        }),
      );

      expect(late[late.length - 1]?.record).toMatchObject({
        text: 'appended later',
      });
    }),
  );

  it('starts from the beginning by default', () => {
    // The default is the sentinel, not offset zero: `after` is exclusive, so
    // starting at a real offset would skip the first record.
    expect(LogOffset.START).toBe('-1');
  });
});
