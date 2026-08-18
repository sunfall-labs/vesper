import { LogStoreMemory } from '@sunfall/vesper-log/layer-memory';
import { LogStore } from '@sunfall/vesper-log/log-store';
import { LogOffset } from '@sunfall/vesper-log/offset';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';
import * as NodeServices from '@effect/platform-node/NodeServices';
import { describe, expect, it } from '@effect/vitest';
import {
  Deferred,
  Context,
  Effect,
  Exit,
  Fiber,
  Layer,
  Option,
  Ref,
  Schema,
  SchemaIssue,
  SchemaTransformation,
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
import * as AgentLog from '../src/log.js';
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
  initial: () => ({ count: 0 }),
});
const SameIdCount = AgentState.make({
  id: 'count',
  version: '1',
  schema: Schema.Struct({ count: Schema.Number }),
  initial: () => ({ count: 10 }),
});
class StateCodecService extends Context.Service<
  StateCodecService,
  { readonly offset: number }
>()('state-test/StateCodecService') {}
const OffsetState = AgentState.make({
  id: 'offset-state',
  version: '1',
  schema: Schema.String.pipe(
    Schema.decodeTo(
      Schema.Number,
      SchemaTransformation.transformOrFail<
        number,
        string,
        StateCodecService,
        StateCodecService
      >({
        decode: (value) =>
          Effect.map(StateCodecService, ({ offset }) => Number(value) + offset),
        encode: (value) =>
          Effect.map(StateCodecService, ({ offset }) => String(value - offset)),
      }),
    ),
  ),
  initial: () => 0,
});
const EncodeFailureState = AgentState.make({
  id: 'encode-failure',
  version: '1',
  schema: Schema.String.pipe(
    Schema.decodeTo(
      Schema.Number,
      SchemaTransformation.transformOrFail<number, string>({
        decode: (value) => Effect.succeed(Number(value)),
        encode: () =>
          Effect.fail(
            new SchemaIssue.InvalidValue({ message: 'encode failed' }),
          ),
      }),
    ),
  ),
  initial: () => 0,
});
const DecodeFailureState = AgentState.make({
  id: 'decode-failure',
  version: '1',
  schema: Schema.Number,
  initial: () => 0,
});

const testLogLayer = Layer.mergeAll(
  LogStoreMemory.layer.pipe(Layer.provide(NodeServices.layer)),
  NodeServices.layer,
);

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

describe('recorded agent state', () => {
  it('uses typed definition errors', () => {
    expect(() =>
      AgentState.make({
        id: '',
        version: '1',
        schema: Schema.Struct({}),
        initial: () => ({}),
      }),
    ).toThrow(
      expect.objectContaining({
        _tag: 'StateDefinitionError',
      }),
    );
  });

  it.effect(
    'provides state to handlers and restores it across agent runs',
    () => {
      const increment = Tool.make('increment', {
        description: 'Increment the recorded count.',
        parameters: Schema.Struct({}),
        success: Schema.Struct({ count: Schema.Number }),
        failure: AgentState.Error,
        dependencies: AgentState.dependencies(Count),
      });
      const agent = Agent.make({
        name: 'state-test',
        revision: '1',
        instructions: 'Use the tool once.',
        toolkit: Toolkit.make(increment),
        state: Count,
      }).withHandlers({
        increment: () =>
          Effect.gen(function* () {
            const state = yield* Count;
            return yield* state.update(({ count }) => ({ count: count + 1 }));
          }),
      });

      return Effect.gen(function* () {
        const conversation = Conversation.make(agent, 'agent-state');
        yield* conversation.run('first');
        yield* conversation.run('second');
        const session = yield* AgentLog.open(
          LogVocabulary.ConversationId.make('agent-state'),
          { compatibility },
        );
        expect(yield* (yield* AgentState.open(Count, session)).get).toEqual({
          count: 2,
        });
      }).pipe(
        Effect.provide(model),
        Effect.provide(testLogLayer),
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
    }).pipe(Effect.provide(AgentState.layerEphemeral(Count))),
  );

  it.effect('opens isolated ephemeral handles', () =>
    Effect.gen(function* () {
      const left = yield* AgentState.open(Count, undefined);
      const right = yield* AgentState.open(Count, undefined);
      yield* left.set({ count: 1 });
      yield* right.set({ count: 2 });
      expect(yield* left.get).toEqual({ count: 1 });
      expect(yield* right.get).toEqual({ count: 2 });
    }),
  );

  it.effect('keeps same-id definitions distinct as Context services', () =>
    Effect.gen(function* () {
      expect(yield* (yield* Count).get).toEqual({ count: 0 });
      expect(yield* (yield* SameIdCount).get).toEqual({ count: 10 });
    }).pipe(
      Effect.provide(
        Layer.merge(
          AgentState.layerEphemeral(Count),
          AgentState.layerEphemeral(SameIdCount),
        ),
      ),
    ),
  );

  it.effect('preserves transformed codec service requirements at runtime', () =>
    Effect.gen(function* () {
      const session = yield* open();
      const codecService = { offset: 1 };
      const opened = yield* AgentState.open(OffsetState, session).pipe(
        Effect.provideService(StateCodecService, codecService),
      );
      yield* opened
        .set(41)
        .pipe(Effect.provideService(StateCodecService, codecService));
      expect((yield* session.recorded).at(-1)?.record).toMatchObject({
        _tag: 'StateCheckpoint',
        value: '40',
      });

      const reopened = yield* AgentLog.open(conversation, { compatibility });
      const restored = yield* AgentState.open(OffsetState, reopened).pipe(
        Effect.provideService(StateCodecService, codecService),
      );
      expect(yield* restored.get).toBe(41);
    }).pipe(Effect.provide(testLogLayer)),
  );

  it.effect('reports checkpoint decoding failures as structured errors', () =>
    Effect.gen(function* () {
      const session = yield* open();
      yield* session.append([
        {
          _tag: 'StateCheckpoint',
          id: DecodeFailureState.id,
          version: DecodeFailureState.version,
          value: 'not-a-number',
        },
      ]);
      const reopened = yield* AgentLog.open(conversation, { compatibility });
      const exit = yield* Effect.exit(
        AgentState.open(DecodeFailureState, reopened),
      );
      expect(exit._tag).toBe('Failure');
      if (exit._tag === 'Failure') {
        expect(Option.getOrThrow(Exit.findErrorOption(exit))).toMatchObject({
          _tag: 'StateDecodeError',
          stateId: DecodeFailureState.id,
          stateVersion: DecodeFailureState.version,
        });
      }
    }).pipe(Effect.provide(testLogLayer)),
  );

  it.effect('reports checkpoint encoding failures as structured errors', () =>
    Effect.gen(function* () {
      const session = yield* open();
      const state = yield* AgentState.open(EncodeFailureState, session);
      const exit = yield* Effect.exit(state.set(1));
      expect(exit._tag).toBe('Failure');
      if (exit._tag === 'Failure') {
        expect(Option.getOrThrow(Exit.findErrorOption(exit))).toMatchObject({
          _tag: 'StateEncodeError',
          stateId: EncodeFailureState.id,
          stateVersion: EncodeFailureState.version,
        });
      }
      expect(yield* state.get).toBe(0);
      expect(
        (yield* session.recorded).some(
          ({ record }) => record._tag === 'StateCheckpoint',
        ),
      ).toBe(false);
    }).pipe(Effect.provide(testLogLayer)),
  );

  it.effect('isolates concurrent ordinary runs', () =>
    Effect.gen(function* () {
      const seen = yield* Ref.make<ReadonlyArray<number>>([]);
      const increment = Tool.make('increment', {
        description: 'Increment the ephemeral count.',
        parameters: Schema.Struct({}),
        success: Schema.Struct({ count: Schema.Number }),
        failure: AgentState.Error,
        dependencies: AgentState.dependencies(Count),
      });
      const agent = Agent.make({
        name: 'ephemeral-state-test',
        revision: '1',
        instructions: 'Use the tool once.',
        toolkit: Toolkit.make(increment),
        state: Count,
      }).withHandlers({
        increment: () =>
          Effect.gen(function* () {
            const state = yield* Count;
            const value = yield* state.update(({ count }) => ({
              count: count + 1,
            }));
            yield* Ref.update(seen, (values) => [...values, value.count]);
            return value;
          }),
      });
      const ordinaryModel = Layer.effect(
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
                    call < 2
                      ? Stream.fromIterable<Response.StreamPartEncoded>([
                          {
                            type: 'tool-call',
                            id: `increment-${call}`,
                            name: 'increment',
                            params: {},
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

      yield* Effect.all([agent.run('left'), agent.run('right')], {
        concurrency: 'unbounded',
      }).pipe(Effect.provide(ordinaryModel));
      expect([...(yield* Ref.get(seen))].sort()).toEqual([1, 1]);
    }).pipe(Effect.orDie),
  );

  it.effect(
    'isolates concurrent conversations sharing one recorded layer',
    () => {
      const setCount = Tool.make('set_count', {
        description: 'Set the recorded count.',
        parameters: Schema.Struct({ count: Schema.Number }),
        success: Schema.Struct({ count: Schema.Number }),
        failure: AgentState.Error,
        dependencies: AgentState.dependencies(Count),
      });
      const agent = Agent.make({
        name: 'state-test',
        revision: '1',
        instructions: 'Set the requested count.',
        toolkit: Toolkit.make(setCount),
        state: Count,
      }).withHandlers({
        set_count: ({ count }) =>
          Effect.gen(function* () {
            const state = yield* Count;
            yield* state.set({ count });
            return yield* state.get;
          }),
      });
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
            Conversation.make(agent, 'state-left').run('set count to 1'),
            Conversation.make(agent, 'state-right').run('set count to 2'),
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
        Effect.provide(setCountModel),
        Effect.provide(testLogLayer),
        Effect.orDie,
      );
    },
  );

  it.effect('serializes concurrent first access for a recorded handle', () =>
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
    }).pipe(Effect.provide(testLogLayer)),
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
    }).pipe(Effect.provide(testLogLayer)),
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
    }).pipe(Effect.provide(testLogLayer)),
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
    }).pipe(Effect.provide(testLogLayer)),
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
    }).pipe(Effect.provide(testLogLayer)),
  );

  it.effect('serializes checkpoints with concurrent settlement snapshots', () =>
    Effect.gen(function* () {
      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const delayed = Layer.mergeAll(
        Layer.effect(
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
        ).pipe(Layer.provide(testLogLayer)),
        NodeServices.layer,
      );

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
        initial: () => ({ count: 0 }),
      });
      const result = yield* Effect.exit(AgentState.open(Other, reopened));
      expect(result._tag).toBe('Failure');
      if (result._tag === 'Failure') {
        const error = Exit.findErrorOption(result);
        expect(Option.getOrThrow(error)).toMatchObject({
          _tag: 'StateCompatibilityError',
          stateId: 'other',
          persistedId: 'count',
        });
      }
    }).pipe(Effect.provide(testLogLayer)),
  );

  it.effect('preserves state error identity at the agent boundary', () => {
    const Other = AgentState.make({
      id: 'other-agent-state',
      version: '7',
      schema: Schema.Struct({ count: Schema.Number }),
      initial: () => ({ count: 0 }),
    });
    const agent = Agent.make({
      name: compatibility.agent,
      revision: compatibility.revision,
      instructions: 'do nothing',
      toolkit: Toolkit.empty,
      state: Other,
    });

    return Effect.gen(function* () {
      const session = yield* open();
      yield* (yield* AgentState.open(Count, session)).set({ count: 1 });

      const result = yield* Effect.exit(
        Conversation.make(agent, conversation)
          .run('hello')
          .pipe(Effect.provide(model)),
      );
      expect(result._tag).toBe('Failure');
      if (result._tag === 'Failure') {
        expect(Option.getOrThrow(Exit.findErrorOption(result))).toMatchObject({
          _tag: 'AiError',
          reason: {
            _tag: 'InvalidRequestError',
            metadata: {
              vesper: {
                tag: 'StateCompatibilityError',
                stateId: Other.id,
                persistedId: Count.id,
                stateVersion: Other.version,
                persistedVersion: Count.version,
              },
            },
          },
        });
      }
    }).pipe(Effect.provide(testLogLayer));
  });

  it.effect('reports mutation encoding failures without defects', () => {
    const Invalid = AgentState.make({
      id: 'invalid',
      version: '1',
      schema: Schema.Struct({ count: Schema.Number }),
      initial: () => ({ count: 0 }),
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
            _tag: 'StateJsonError',
            message: 'State checkpoint is not JSON-safe',
            stateId: 'invalid',
            stateVersion: '1',
          });
        }
      }
    }).pipe(Effect.provide(testLogLayer));
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
    }).pipe(Effect.provide(testLogLayer)),
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
    }).pipe(Effect.provide(testLogLayer)),
  );
});
