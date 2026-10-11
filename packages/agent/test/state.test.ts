import * as NodeCrypto from '@effect/platform-node/NodeCrypto';
import { describe, expect, it } from '@effect/vitest';
import { LogStore } from '@sunfall/vesper-log/log-store';
import {
  Context,
  Effect,
  Exit,
  Layer,
  Option,
  Ref,
  Schema,
  SchemaIssue,
  SchemaTransformation,
  Stream,
} from 'effect';
import { Tool, Toolkit } from 'effect/ai';

import { Agent } from '../src/agent.js';
import { Conversation } from '../src/conversation.js';
import { AgentState } from '../src/state.js';
import { ScriptedModel } from '../src/testing.js';
import {
  finish,
  testLogLayer,
  textTurn,
  toolTurn,
} from './scenario-fixtures.js';

const Count = AgentState.make({
  id: 'count',
  version: '1',
  schema: Schema.Struct({ count: Schema.Finite }),
  initial: () => ({ count: 0 }),
});
const SameIdCount = AgentState.make({
  id: 'count',
  version: '1',
  schema: Schema.Struct({ count: Schema.Finite }),
  initial: () => ({ count: 10 }),
});

const increment = Tool.make('increment', {
  description: 'Increment the count.',
  parameters: Schema.Struct({}),
  success: Schema.Struct({ count: Schema.Finite }),
  failure: AgentState.Error,
  dependencies: AgentState.dependencies(Count),
});
const setCount = Tool.make('set_count', {
  description: 'Set the count.',
  parameters: Schema.Struct({ count: Schema.Finite }),
  success: Schema.Struct({ count: Schema.Finite }),
  failure: AgentState.Error,
  dependencies: AgentState.dependencies(Count),
});

const counter = Agent.make({
  name: 'state-test',
  revision: '1',
  instructions: 'Use the tool.',
  toolkit: Toolkit.make(increment, setCount),
  state: Count,
}).withHandlers({
  increment: () =>
    Effect.gen(function* () {
      const state = yield* Count;
      return yield* state.update(({ count }) => ({ count: count + 1 }));
    }),
  set_count: ({ count }) =>
    Effect.gen(function* () {
      const state = yield* Count;
      yield* state.set({ count });
      return yield* state.get;
    }),
});

/** One run that increments the count once, then answers. */
let runs = 0;
const incrementRun = () => {
  const n = String(runs++);
  return [toolTurn('increment', {}, `inc-${n}`), textTurn('ok', `t-${n}`)];
};

const checkpoints = (conversation: Conversation.Instance<typeof counter>) =>
  conversation.records().pipe(
    Stream.runCollect,
    Effect.map((all) =>
      Array.from(all).flatMap((envelope) =>
        envelope.record._tag === 'StateCheckpoint'
          ? [{ offset: envelope.offset, value: envelope.record.value }]
          : [],
      ),
    ),
  );

const promptText = (value: unknown): string =>
  Schema.encodeSync(Schema.fromJsonString(Schema.Unknown))(value);

const errorOf = <A, E>(exit: Exit.Exit<A, E>) =>
  exit._tag === 'Failure'
    ? Option.getOrThrow(Exit.findErrorOption(exit))
    : undefined;

describe('ephemeral agent state', () => {
  it('uses typed definition errors', () => {
    expect(() =>
      AgentState.make({
        id: '',
        version: '1',
        schema: Schema.Struct({}),
        initial: () => ({}),
      }),
    ).toThrow(expect.objectContaining({ _tag: 'StateDefinitionError' }));
  });

  it.effect(
    'supports typed updates and keeps same-id definitions distinct',
    () =>
      Effect.gen(function* () {
        const state = yield* Count;
        expect(yield* state.get).toEqual({ count: 0 });
        expect(
          yield* state.update(({ count }) => ({ count: count + 1 })),
        ).toEqual({ count: 1 });
        expect(
          yield* state.modify(({ count }) => [count, { count: count + 1 }]),
        ).toBe(1);
        expect(yield* state.get).toEqual({ count: 2 });
        expect(yield* (yield* SameIdCount).get).toEqual({ count: 10 });

        const left = yield* AgentState.open(Count, undefined);
        const right = yield* AgentState.open(Count, undefined);
        yield* left.set({ count: 1 });
        yield* right.set({ count: 2 });
        expect(yield* left.get).toEqual({ count: 1 });
        expect(yield* right.get).toEqual({ count: 2 });
      }).pipe(
        Effect.provide(
          Layer.merge(
            AgentState.layerEphemeral(Count),
            AgentState.layerEphemeral(SameIdCount),
          ),
        ),
      ),
  );

  it.effect('isolates concurrent ordinary runs', () =>
    Effect.gen(function* () {
      const seen = yield* Ref.make<ReadonlyArray<number>>([]);
      const recording = Agent.make({
        name: 'ephemeral-state-test',
        revision: '1',
        instructions: 'Use the tool.',
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
      // Both runs share one script: tool call, tool call, then two answers.
      const model = ScriptedModel.make([
        toolTurn('increment', {}, 'left'),
        toolTurn('increment', {}, 'right'),
        textTurn('ok', 'a'),
        textTurn('ok', 'b'),
      ]);

      yield* Effect.all([recording.run('left'), recording.run('right')], {
        concurrency: 'unbounded',
      }).pipe(Effect.provide(model.layer));

      expect(yield* Ref.get(seen)).toEqual([1, 1]);
    }).pipe(Effect.orDie),
  );
});

const providing = (model: ScriptedModel.Handle) =>
  Effect.provide(Layer.mergeAll(model.layer, testLogLayer, NodeCrypto.layer));

const failingAgent = <A, Encoded>(
  name: string,
  definition: AgentState.Definition<A, Encoded>,
  value: A,
) => {
  const poke = Tool.make('poke', {
    parameters: Schema.Struct({}),
    success: Schema.String,
    failure: AgentState.Error,
    dependencies: AgentState.dependencies(definition),
  });
  return Agent.make({
    name,
    revision: '1',
    instructions: 'x',
    toolkit: Toolkit.make(poke),
    state: definition,
  }).withHandlers({
    poke: () =>
      Effect.gen(function* () {
        const state = yield* definition;
        const outcome = yield* Effect.result(state.set(value));
        const failure =
          outcome._tag === 'Failure' ? outcome.failure._tag : 'none';
        return failure;
      }),
  });
};

describe('recorded agent state', () => {
  it.effect(
    'restores checkpoints across runs and snapshots them on settlement',
    () =>
      Effect.gen(function* () {
        const model = ScriptedModel.make([
          ...incrementRun(),
          ...incrementRun(),
        ]);
        yield* Effect.gen(function* () {
          const conversation = Conversation.make(counter, 'agent-state');
          yield* conversation.run('first');
          yield* conversation.run('second');
          const records = Array.from(
            yield* conversation.records().pipe(Stream.runCollect),
          );
          const settled = records.flatMap(({ record }) =>
            record._tag === 'RunSettled' ? [record] : [],
          );

          expect(
            (yield* checkpoints(conversation)).map((c) => c.value),
          ).toEqual([{ count: 1 }, { count: 2 }]);
          expect(settled.at(-1)).toMatchObject({
            resume: {
              state: { id: 'count', version: '1', value: { count: 2 } },
            },
          });
        }).pipe(providing(model));
      }).pipe(Effect.orDie),
  );

  it.effect(
    'isolates concurrent conversations sharing one recorded layer',
    () =>
      Effect.gen(function* () {
        const model = ScriptedModel.make([
          toolTurn('set_count', { count: 1 }, 'left'),
          toolTurn('set_count', { count: 2 }, 'right'),
          textTurn('ok', 'a'),
          textTurn('ok', 'b'),
        ]);
        yield* Effect.gen(function* () {
          const left = Conversation.make(counter, 'state-left');
          const right = Conversation.make(counter, 'state-right');
          yield* Effect.all([left.run('one'), right.run('two')], {
            concurrency: 'unbounded',
          });
          const values = (c: typeof left) =>
            Effect.map(checkpoints(c), (all) => all.map(({ value }) => value));
          // Whichever scripted call each conversation drew, its checkpoint is
          // its own: one conversation never sees the other's write.
          const both = [
            (yield* values(left)).at(-1),
            (yield* values(right)).at(-1),
          ];
          expect(both).toEqual(
            expect.arrayContaining([{ count: 1 }, { count: 2 }]),
          );
        }).pipe(providing(model));
      }).pipe(Effect.orDie),
  );

  it.effect('branches and forks from the selected state checkpoint', () =>
    Effect.gen(function* () {
      const model = ScriptedModel.make(
        Array.from({ length: 5 }, incrementRun).flat(),
      );
      yield* Effect.gen(function* () {
        const source = Conversation.make(counter, 'state-source');
        yield* source.run('one');
        yield* source.run('two');
        const settledFirst = Array.from(
          yield* source.records().pipe(Stream.runCollect),
        ).find(({ record }) => record._tag === 'RunSettled');
        const at = Option.getOrThrow(Option.fromNullishOr(settledFirst)).offset;

        // Branching behind the second increment restores count 1, so the
        // branch's own increment lands on 2, not 3.
        yield* source.branchFrom(at, 'branch');
        expect((yield* checkpoints(source)).at(-1)?.value).toEqual({
          count: 2,
        });
        // The next ordinary run continues from the branch, not the abandoned tail.
        yield* source.run('after branch');
        expect((yield* checkpoints(source)).at(-1)?.value).toEqual({
          count: 3,
        });

        yield* source.forkFrom(at, 'state-fork', 'fork');
        const fork = Conversation.make(counter, 'state-fork');
        expect((yield* checkpoints(fork)).at(-1)?.value).toEqual({ count: 2 });
        expect((yield* checkpoints(source)).at(-1)?.value).toEqual({
          count: 3,
        });
      }).pipe(providing(model));
    }).pipe(Effect.orDie),
  );

  it.effect('preserves state error identity at the agent boundary', () =>
    Effect.gen(function* () {
      const Other = AgentState.make({
        id: 'other-agent-state',
        version: '7',
        schema: Schema.Struct({ count: Schema.Finite }),
        initial: () => ({ count: 0 }),
      });
      const other = Agent.make({
        name: counter.name,
        revision: counter.revision,
        instructions: 'do nothing',
        toolkit: Toolkit.empty,
        state: Other,
      });
      const model = ScriptedModel.make([...incrementRun(), textTurn('unused')]);
      yield* Effect.gen(function* () {
        yield* Conversation.make(counter, 'state-identity').run('seed');
        const exit = yield* Effect.exit(
          Conversation.make(other, 'state-identity').run('hello'),
        );

        expect(errorOf(exit)).toMatchObject({
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
      }).pipe(providing(model));
    }).pipe(Effect.orDie),
  );

  it.effect('fails a resume whose persisted checkpoint no longer decodes', () =>
    Effect.gen(function* () {
      // Same id and version, incompatible schema: the stored { count } cannot
      // decode, and the run must fail typed rather than reset to initial.
      const Drifted = AgentState.make({
        id: Count.id,
        version: Count.version,
        schema: Schema.String,
        initial: () => 'reset',
      });
      const drifted = Agent.make({
        name: counter.name,
        revision: counter.revision,
        instructions: 'do nothing',
        toolkit: Toolkit.empty,
        state: Drifted,
      });
      const model = ScriptedModel.make([...incrementRun(), textTurn('unused')]);
      yield* Effect.gen(function* () {
        yield* Conversation.make(counter, 'state-decode').run('seed');
        const exit = yield* Effect.exit(
          Conversation.make(drifted, 'state-decode').run('hello'),
        );
        expect(errorOf(exit)).toMatchObject({
          _tag: 'AiError',
          reason: {
            metadata: {
              vesper: {
                tag: 'StateDecodeError',
                stateId: Count.id,
                stateVersion: Count.version,
              },
            },
          },
        });
      }).pipe(providing(model));
    }).pipe(Effect.orDie),
  );

  it.live('serializes slow checkpoint writes with the run settlement', () =>
    Effect.gen(function* () {
      const slowStore = Layer.effect(
        LogStore.Service,
        Effect.gen(function* () {
          const store = yield* LogStore.Service;
          return {
            ...store,
            append: (input: LogStore.AppendInput) =>
              input.records.some(
                ({ record }) => record._tag === 'StateCheckpoint',
              )
                ? Effect.sleep('20 millis').pipe(
                    Effect.andThen(store.append(input)),
                  )
                : store.append(input),
          } satisfies LogStore.Interface;
        }),
      ).pipe(Layer.provide(testLogLayer));
      // Two parallel increments in one turn, each writing a slow checkpoint.
      const model = ScriptedModel.make([
        [
          { type: 'tool-call', id: 'a', name: 'increment', params: {} },
          { type: 'tool-call', id: 'b', name: 'increment', params: {} },
          finish('tool-calls'),
        ],
        textTurn('done'),
      ]);
      yield* Effect.gen(function* () {
        const conversation = Conversation.make(counter, 'state-race');
        yield* conversation.run('go');
        const records = Array.from(
          yield* conversation.records().pipe(Stream.runCollect),
        );
        const settled = records.find(
          ({ record }) => record._tag === 'RunSettled',
        );
        const written = records.filter(
          ({ record }) => record._tag === 'StateCheckpoint',
        );
        // No lost update, and no checkpoint lands after the settlement.
        expect(written.map(({ record }) => record)).toMatchObject([
          { value: { count: 1 } },
          { value: { count: 2 } },
        ]);
        const settledAt = Option.getOrThrow(
          Option.fromNullishOr(settled),
        ).offset;
        expect(written.every(({ offset }) => offset < settledAt)).toBe(true);
        expect(settled?.record).toMatchObject({
          resume: { state: { value: { count: 2 } } },
        });
      }).pipe(
        Effect.provide(
          Layer.mergeAll(model.layer, slowStore, NodeCrypto.layer),
        ),
      );
    }).pipe(Effect.orDie),
  );

  it.effect('reports encode, JSON, and codec failures without defects', () =>
    Effect.gen(function* () {
      class Codec extends Context.Service<Codec, { readonly offset: number }>()(
        'state-test/Codec',
      ) {}
      const OffsetState = AgentState.make({
        id: 'offset-state',
        version: '1',
        schema: Schema.String.pipe(
          Schema.decodeTo(
            Schema.Finite,
            SchemaTransformation.transformEffect<number, string, Codec, Codec>({
              decode: (value) =>
                Effect.map(Codec, ({ offset }) => Number(value) + offset),
              encode: (value) =>
                Effect.map(Codec, ({ offset }) => String(value - offset)),
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
            Schema.Finite,
            SchemaTransformation.transformEffect<number, string>({
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
      const InfiniteState = AgentState.make({
        id: 'infinite',
        version: '1',
        schema: Schema.Struct({ count: Schema.Number }),
        initial: () => ({ count: 0 }),
      });
      // Codec services flow through to runtime encoding of checkpoints.
      const setOffset = Tool.make('set_offset', {
        parameters: Schema.Struct({ value: Schema.Finite }),
        success: Schema.Finite,
        failure: AgentState.Error,
        dependencies: AgentState.dependencies(OffsetState, Codec),
      });
      const offsetAgent = Agent.make({
        name: 'offset',
        revision: '1',
        instructions: 'x',
        toolkit: Toolkit.make(setOffset),
        state: OffsetState,
      }).withHandlers({
        set_offset: ({ value }) =>
          Effect.gen(function* () {
            const state = yield* OffsetState;
            yield* state.set(value);
            return yield* state.get;
          }),
      });
      const offsetModel = ScriptedModel.make([
        toolTurn('set_offset', { value: 41 }),
        textTurn('ok'),
      ]);
      yield* Effect.gen(function* () {
        const conversation = Conversation.make(offsetAgent, 'offset');
        yield* conversation.run('go');
        const stored = Array.from(
          yield* conversation.records().pipe(Stream.runCollect),
        ).flatMap(({ record }) =>
          record._tag === 'StateCheckpoint' ? [record.value] : [],
        );
        expect(stored).toEqual(['40']);
      }).pipe(
        Effect.provideService(Codec, { offset: 1 }),
        providing(offsetModel),
      );

      // Failing encodes surface as typed tool failures and write nothing.
      for (const [agent, expected] of [
        [failingAgent('encode', EncodeFailureState, 1), 'StateEncodeError'],
        [
          failingAgent('json', InfiniteState, { count: Infinity }),
          'StateJsonError',
        ],
      ] as const) {
        const model = ScriptedModel.make([
          toolTurn('poke', {}),
          textTurn('ok'),
        ]);
        yield* Effect.gen(function* () {
          const conversation = Conversation.make(agent, `poke-${agent.name}`);
          yield* conversation.run('go');
          const records = Array.from(
            yield* conversation.records().pipe(Stream.runCollect),
          );
          expect(
            records.some(({ record }) => record._tag === 'StateCheckpoint'),
          ).toBe(false);
        }).pipe(providing(model));
        expect(promptText((yield* model.requests).at(-1)?.prompt)).toContain(
          expected,
        );
      }
    }).pipe(Effect.orDie),
  );
});
