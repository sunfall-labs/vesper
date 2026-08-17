import { LogStoreMemory } from '@sunfall/vesper-log/layer-memory';
import { LogStore } from '@sunfall/vesper-log/log-store';
import { LogOffset } from '@sunfall/vesper-log/offset';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';
import { describe, expect, it } from '@effect/vitest';
import {
  Deferred,
  Effect,
  Exit,
  Fiber,
  Layer,
  Option,
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
import { Session } from '../src/internal.js';
import { AgentLog } from '../src/log.js';
import { AgentState } from '../src/state.js';

const compatibility = {
  agent: 'state-test',
  revision: LogVocabulary.AgentRevision.make('1'),
};
const conversation = LogVocabulary.ConversationId.make('state-test');
const Count = AgentState.make({
  id: 'count',
  version: '1',
  schema: Schema.Struct({ count: Schema.Number }),
  initial: { count: 0 },
});

const open = Effect.fnUntraced(function* () {
  const session = yield* AgentLog.open(conversation, { compatibility });
  if (session.history.length === 0 && (yield* session.recorded).length === 0) {
    yield* AgentLog.start(session, {
      agent: compatibility.agent,
      revision: compatibility.revision,
      input: 'state test',
    });
  }
  return session;
});

const finish: Response.StreamPartEncoded = {
  type: 'finish',
  reason: 'stop',
  usage: {
    inputTokens: { total: 1, uncached: 1, cacheRead: 0, cacheWrite: 0 },
    outputTokens: { total: 1 },
  },
};
const model = Layer.effect(
  LanguageModel.LanguageModel,
  Effect.gen(function* () {
    const calls = yield* Ref.make(0);
    return yield* LanguageModel.make({
      generateText: () => Effect.succeed([finish]),
      streamText: () =>
        Stream.unwrap(
          Effect.map(
            Ref.getAndUpdate(calls, (n) => n + 1),
            (call) =>
              Stream.fromIterable<Response.StreamPartEncoded>(
                call % 2 === 0
                  ? [
                      {
                        type: 'tool-call',
                        id: `increment-${call}`,
                        name: 'increment',
                        params: {},
                      },
                      { ...finish, reason: 'tool-calls' },
                    ]
                  : [
                      { type: 'text-start', id: `text-${call}` },
                      { type: 'text-delta', id: `text-${call}`, delta: 'ok' },
                      { type: 'text-end', id: `text-${call}` },
                      finish,
                    ],
              ),
          ),
        ),
    });
  }),
);

describe('durable agent state', () => {
  it('uses typed definition errors', () => {
    expect(() =>
      AgentState.make({
        id: '',
        version: '1',
        schema: Schema.Struct({}),
        initial: {},
      }),
    ).toThrow(
      expect.objectContaining({
        _tag: '@sunfall/vesper-agent/AgentStateError',
        reason: 'invalid-definition',
      }),
    );
  });

  it.effect(
    'provides state to handlers and restores it across agent runs',
    () => {
      const increment = Tool.make('increment', {
        description: 'Increment the durable count.',
        parameters: Schema.Struct({}),
        success: Schema.Struct({ count: Schema.Number }),
        failure: AgentState.error,
        dependencies: AgentState.dependencies(Count),
      });
      const agent = Agent.make({
        name: 'state-test',
        revision: '1',
        instructions: 'Use the tool once.',
        toolkit: Toolkit.make(increment),
      }).withHandlers({
        increment: () =>
          Effect.gen(function* () {
            const state = yield* Count;
            return yield* state.update(({ count }) => ({ count: count + 1 }));
          }),
      });

      return Effect.gen(function* () {
        yield* agent.recordingTo('agent-state').run('first');
        yield* agent.resume('agent-state', 'second');
        const session = yield* AgentLog.open(
          LogVocabulary.ConversationId.make('agent-state'),
          { compatibility },
        );
        expect(yield* (yield* AgentState.open(Count, session)).get).toEqual({
          count: 2,
        });
      }).pipe(
        Effect.provide(AgentState.layerDurable(Count)),
        Effect.provide(model),
        Effect.provide(LogStoreMemory.layer),
        Effect.orDie,
      );
    },
  );

  it.effect('supports ephemeral typed updates', () =>
    Effect.gen(function* () {
      const state = yield* Count;
      expect(yield* state.get).toEqual({ count: 0 });
      expect(
        yield* state.update(({ count }) => ({ count: count + 1 })),
      ).toEqual({
        count: 1,
      });
      expect(
        yield* state.modify(({ count }) => [count, { count: count + 1 }]),
      ).toBe(1);
      expect(yield* state.get).toEqual({ count: 2 });
    }).pipe(Effect.provide(AgentState.layerMemory(Count))),
  );

  it.effect('fails durable state outside a recorded session', () =>
    Effect.gen(function* () {
      const state = yield* Count;
      const exit = yield* Effect.exit(state.get);
      expect(exit._tag).toBe('Failure');
      if (exit._tag === 'Failure') {
        expect(Option.getOrThrow(Exit.findErrorOption(exit))).toMatchObject({
          reason: 'no-session',
          stateId: 'count',
        });
      }
    }).pipe(Effect.provide(AgentState.layerDurable(Count))),
  );

  it.effect(
    'isolates concurrent conversations sharing one durable layer',
    () => {
      const setCount = Tool.make('set_count', {
        description: 'Set the durable count.',
        parameters: Schema.Struct({ count: Schema.Number }),
        success: Schema.Struct({ count: Schema.Number }),
        failure: AgentState.error,
        dependencies: AgentState.dependencies(Count),
      });
      const agent = Agent.make({
        name: 'state-test',
        revision: '1',
        instructions: 'Set the requested count.',
        toolkit: Toolkit.make(setCount),
      }).withHandlers({
        set_count: ({ count }) =>
          Effect.gen(function* () {
            const state = yield* Count;
            yield* state.set({ count });
            return yield* state.get;
          }),
      });
      const layer = AgentState.layerDurable(Count);
      const setCountModel = Layer.effect(
        LanguageModel.LanguageModel,
        Effect.gen(function* () {
          const calls = yield* Ref.make(0);
          return yield* LanguageModel.make({
            generateText: () => Effect.succeed([finish]),
            streamText: (options) =>
              Stream.unwrap(
                Effect.map(
                  Ref.getAndUpdate(calls, (n) => n + 1),
                  (call) =>
                    call < 2
                      ? Stream.fromIterable<Response.StreamPartEncoded>([
                          {
                            type: 'tool-call',
                            id: `set-count-${call}`,
                            name: 'set_count',
                            params: {
                              count: JSON.stringify(options.prompt).includes(
                                'count to 1',
                              )
                                ? 1
                                : 2,
                            },
                          },
                          { ...finish, reason: 'tool-calls' },
                        ])
                      : Stream.fromIterable<Response.StreamPartEncoded>([
                          { type: 'text-start', id: `text-${call}` },
                          {
                            type: 'text-delta',
                            id: `text-${call}`,
                            delta: 'ok',
                          },
                          { type: 'text-end', id: `text-${call}` },
                          finish,
                        ]),
                ),
              ),
          });
        }),
      );

      return Effect.gen(function* () {
        yield* Effect.all(
          [
            agent.recordingTo('state-left').run('set count to 1'),
            agent.recordingTo('state-right').run('set count to 2'),
          ],
          { concurrency: 'unbounded' },
        );
        const left = yield* AgentLog.open(
          LogVocabulary.ConversationId.make('state-left'),
          { compatibility },
        );
        const right = yield* AgentLog.open(
          LogVocabulary.ConversationId.make('state-right'),
          { compatibility },
        );
        expect(yield* (yield* AgentState.open(Count, left)).get).toEqual({
          count: 1,
        });
        expect(yield* (yield* AgentState.open(Count, right)).get).toEqual({
          count: 2,
        });
      }).pipe(
        Effect.provide(layer),
        Effect.provide(setCountModel),
        Effect.provide(LogStoreMemory.layer),
        Effect.orDie,
      );
    },
  );

  it.effect(
    'serializes concurrent first access through the durable layer',
    () =>
      Effect.gen(function* () {
        const session = yield* open();
        const program = Effect.gen(function* () {
          const state = yield* Count;
          yield* Effect.all(
            Array.from({ length: 20 }, () =>
              state.update(({ count }) => ({ count: count + 1 })),
            ),
            { concurrency: 'unbounded' },
          );
          expect(yield* state.get).toEqual({ count: 20 });
        }).pipe(
          Effect.provideService(Session, session),
          Effect.provide(AgentState.layerDurable(Count)),
        );
        yield* program;
      }).pipe(Effect.provide(LogStoreMemory.layer)),
  );

  it.effect('restores immediate checkpoints after reopening', () =>
    Effect.gen(function* () {
      const first = yield* open();
      const state = yield* AgentState.open(Count, first);
      yield* state.set({ count: 3 });

      const reopened = yield* AgentLog.open(conversation, { compatibility });
      const restored = yield* AgentState.open(Count, reopened);
      expect(yield* restored.get).toEqual({ count: 3 });
      expect(reopened.stateHistory.at(-1)?.record).toMatchObject({
        _tag: 'StateCheckpoint',
        id: 'count',
        version: '1',
        value: { count: 3 },
      });
    }).pipe(Effect.provide(LogStoreMemory.layer)),
  );

  it.effect('serializes concurrent updates', () =>
    Effect.gen(function* () {
      const session = yield* open();
      const state = yield* AgentState.open(Count, session);
      yield* Effect.all(
        Array.from({ length: 20 }, () =>
          state.update(({ count }) => ({ count: count + 1 })),
        ),
        { concurrency: 'unbounded' },
      );
      expect(yield* state.get).toEqual({ count: 20 });
    }).pipe(Effect.provide(LogStoreMemory.layer)),
  );

  it.effect('restores a pre-compaction checkpoint after an orphaned run', () =>
    Effect.gen(function* () {
      const session = yield* open();
      yield* (yield* AgentState.open(Count, session)).set({ count: 7 });
      yield* session.append([{ _tag: 'Text', step: 1, text: 'kept' }]);
      const firstKept = (yield* session.recorded).at(-1)!.offset;
      yield* session.append([
        {
          _tag: 'Compacted',
          formatVersion: 1,
          agent: compatibility.agent,
          agentRevision: compatibility.revision,
          step: 1,
          summary: 'summary',
          firstKept,
          summarizedMessages: 1,
          keptMessages: 1,
        },
        {
          _tag: 'RunStarted',
          formatVersion: 1,
          agent: compatibility.agent,
          agentRevision: compatibility.revision,
          prompt: [],
        },
      ]);

      const reopened = yield* AgentLog.open(conversation, { compatibility });
      expect(yield* (yield* AgentState.open(Count, reopened)).get).toEqual({
        count: 7,
      });
    }).pipe(Effect.provide(LogStoreMemory.layer)),
  );

  it.effect('snapshots earlier records from the settlement batch', () =>
    Effect.gen(function* () {
      const session = yield* open();
      yield* session.append([
        {
          _tag: 'StateCheckpoint',
          id: Count.id,
          version: Count.version,
          value: { count: 9 },
        },
        {
          _tag: 'Completed',
          outcome: 'success',
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
      ]);

      const settled = (yield* session.recorded).find(
        ({ record }) => record._tag === 'RunSettled',
      )?.record;
      expect(settled).toMatchObject({
        resume: {
          completed: { text: 'done' },
          state: { id: 'count', version: '1', value: { count: 9 } },
        },
      });
      const reopened = yield* AgentLog.open(conversation, { compatibility });
      expect(reopened.completed).toMatchObject({ text: 'done' });
      expect(yield* (yield* AgentState.open(Count, reopened)).get).toEqual({
        count: 9,
      });
    }).pipe(Effect.provide(LogStoreMemory.layer)),
  );

  it.effect('serializes checkpoints with concurrent settlement snapshots', () =>
    Effect.gen(function* () {
      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const delayed = Layer.effect(
        LogStore.Service,
        Effect.gen(function* () {
          const store = yield* LogStore.Service;
          return {
            ...store,
            append: (input: LogStore.AppendInput) =>
              input.records.some(
                ({ record }) => record._tag === 'StateCheckpoint',
              )
                ? Effect.gen(function* () {
                    yield* Deferred.succeed(entered, undefined);
                    yield* Deferred.await(release);
                    return yield* store.append(input);
                  })
                : store.append(input),
          } satisfies LogStore.Interface;
        }),
      ).pipe(Layer.provide(LogStoreMemory.layer));

      yield* Effect.gen(function* () {
        const session = yield* open();
        const checkpoint = yield* session
          .append([
            {
              _tag: 'StateCheckpoint',
              id: Count.id,
              version: Count.version,
              value: { count: 11 },
            },
          ])
          .pipe(Effect.forkChild);
        yield* Deferred.await(entered);
        const settlement = yield* session
          .append([
            {
              _tag: 'RunSettled',
              outcome: 'interrupted',
              detail: 'concurrent settlement',
              steps: 0,
              usage: { input: 0, output: 0 },
            },
          ])
          .pipe(Effect.forkChild);
        yield* Effect.yieldNow;
        yield* Deferred.succeed(release, undefined);
        yield* Fiber.join(checkpoint);
        yield* Fiber.join(settlement);

        const reopened = yield* AgentLog.open(conversation, { compatibility });
        expect(yield* (yield* AgentState.open(Count, reopened)).get).toEqual({
          count: 11,
        });
      }).pipe(Effect.provide(delayed));
    }),
  );

  it.effect('rejects a different state identity before use', () =>
    Effect.gen(function* () {
      const session = yield* open();
      yield* (yield* AgentState.open(Count, session)).set({ count: 1 });
      const reopened = yield* AgentLog.open(conversation, { compatibility });
      const Other = AgentState.make({
        id: 'other',
        version: '1',
        schema: Schema.Struct({ count: Schema.Number }),
        initial: { count: 0 },
      });
      const result = yield* Effect.exit(AgentState.open(Other, reopened));
      expect(result._tag).toBe('Failure');
      if (result._tag === 'Failure') {
        const error = Exit.findErrorOption(result);
        expect(Option.getOrThrow(error)).toMatchObject({
          reason: 'incompatible',
          stateId: 'other',
          persistedId: 'count',
        });
      }
    }).pipe(Effect.provide(LogStoreMemory.layer)),
  );

  it.effect('reports mutation encoding failures without defects', () => {
    const Invalid = AgentState.make({
      id: 'invalid',
      version: '1',
      schema: Schema.Struct({ count: Schema.Number }),
      initial: { count: 0 },
    });
    return Effect.gen(function* () {
      const session = yield* open();
      const state = yield* AgentState.open(Invalid, session);
      const exit = yield* Effect.exit(
        state.set({ count: Number.POSITIVE_INFINITY }),
      );
      expect(exit._tag).toBe('Failure');
      if (exit._tag === 'Failure') {
        const error = Exit.findErrorOption(exit);
        expect(Option.isSome(error)).toBe(true);
        if (Option.isSome(error)) {
          expect(error.value).toMatchObject({
            reason: 'not-json-safe',
            message: 'State checkpoint is not JSON-safe',
            stateId: 'invalid',
            stateVersion: '1',
          });
        }
      }
    }).pipe(Effect.provide(LogStoreMemory.layer));
  });

  it.effect('branches and forks from the selected state checkpoint', () =>
    Effect.gen(function* () {
      const session = yield* open();
      const state = yield* AgentState.open(Count, session);
      yield* state.set({ count: 1 });
      const afterOne = (yield* session.recorded).at(-1)!.offset;
      yield* state.set({ count: 2 });

      const branched = yield* AgentLog.open(conversation, {
        compatibility,
        branchFrom: afterOne,
      });
      expect(yield* (yield* AgentState.open(Count, branched)).get).toEqual({
        count: 1,
      });
      yield* branched.append([
        {
          _tag: 'RunSettled',
          outcome: 'interrupted',
          detail: 'branch state regression',
          steps: 0,
          usage: { input: 0, output: 0 },
        },
      ]);
      const reopened = yield* AgentLog.open(conversation, { compatibility });
      expect(yield* (yield* AgentState.open(Count, reopened)).get).toEqual({
        count: 1,
      });

      const forked = yield* AgentLog.fork(
        conversation,
        afterOne,
        LogVocabulary.ConversationId.make('state-fork'),
        compatibility,
      );
      expect(yield* (yield* AgentState.open(Count, forked)).get).toEqual({
        count: 1,
      });
      expect(afterOne).not.toBe(LogOffset.START);
    }).pipe(Effect.provide(LogStoreMemory.layer)),
  );

  it.effect('restores state when branching behind a settled aggregate', () =>
    Effect.gen(function* () {
      const session = yield* open();
      yield* (yield* AgentState.open(Count, session)).set({ count: 13 });
      const checkpoint = (yield* session.recorded).at(-1)!.offset;
      yield* session.append([
        {
          _tag: 'RunSettled',
          outcome: 'interrupted',
          detail: 'settled before branch',
          steps: 0,
          usage: { input: 0, output: 0 },
        },
      ]);

      const branched = yield* AgentLog.open(conversation, {
        compatibility,
        branchFrom: checkpoint,
      });
      expect(yield* (yield* AgentState.open(Count, branched)).get).toEqual({
        count: 13,
      });
    }).pipe(Effect.provide(LogStoreMemory.layer)),
  );
});
