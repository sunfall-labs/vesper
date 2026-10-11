import * as NodeCrypto from '@effect/platform-node/NodeCrypto';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';
import { describe, expect, it } from '@effect/vitest';
import {
  Context,
  Deferred,
  Effect,
  ExecutionPlan,
  Fiber,
  Layer,
  Ref,
  Schema,
  Stream,
} from 'effect';
import {
  AiError,
  LanguageModel,
  type Response,
  Tool,
  Toolkit,
} from 'effect/ai';

import { Agent } from '../src/agent.js';
import { ModelPlan } from '../src/model-plan.js';
import { Skill } from '../src/skill.js';
import { Stop } from '../src/stop.js';
import { Subagent } from '../src/subagent.js';
import { ScriptedModel } from '../src/testing.js';
import { finish, textTurn, toolTurn } from './scenario-fixtures.js';

const present = <A>(value: A | undefined): A => {
  if (value === undefined) {
    throw new Error('Expected test fixture value to be present');
  }
  return value;
};

const ping = Tool.make('ping', {
  description: 'A tool the agent owns, to prove the merge is additive.',
  parameters: Schema.Struct({}),
  success: Schema.Struct({ pong: Schema.Boolean }),
});
const pingHandlers = Toolkit.make(ping).toLayer({
  ping: () => Effect.succeed({ pong: true }),
});

const plain = Agent.make({
  name: 'test',
  revision: '1',
  instructions: 'be terse',
  toolkit: Toolkit.make(),
});

const twoTurns = Agent.make({
  name: 'two',
  revision: '1',
  instructions: 'x',
  toolkit: Toolkit.make(),
  stopWhen: Stop.maxSteps(2),
});

describe('Agent run and stream', () => {
  it.effect('fails an in-band provider error instead of completing', () =>
    Effect.gen(function* () {
      const model = ScriptedModel.make([
        [{ type: 'error', error: new Error('provider stream failed') }],
      ]);
      const outcome = yield* plain
        .stream('hi')
        .pipe(Stream.runDrain, Effect.provide(model.layer), Effect.result);

      expect(outcome._tag).toBe('Failure');
    }),
  );

  it.effect('continues across turns, accumulates usage, and run folds it', () =>
    Effect.gen(function* () {
      const script = [textTurn('one', 'a'), textTurn('two', 'b')];
      const model = ScriptedModel.make(script);
      const events = yield* twoTurns
        .stream('go')
        .pipe(Stream.runCollect, Effect.provide(model.layer));
      const tags = events.map((event) => event._tag);

      expect(tags[0]).toBe('TurnStarted');
      expect(tags.filter((tag) => tag === 'TurnStarted')).toHaveLength(2);
      expect(tags.filter((tag) => tag === 'TurnFinished')).toHaveLength(2);
      expect(tags.filter((tag) => tag === 'Completed')).toHaveLength(1);
      expect(tags.at(-1)).toBe('Completed');
      const completed = present(
        events.find((event) => event._tag === 'Completed'),
      );
      expect(completed.usage).toEqual({ input: 6, output: 4 });

      // `run` folds `stream`, so the two cannot report different things.
      const ran = yield* twoTurns
        .run('go')
        .pipe(Effect.provide(ScriptedModel.make(script).layer));
      expect(ran).toMatchObject({
        text: completed.text,
        steps: completed.steps,
        usage: completed.usage,
      });
    }),
  );

  // The point of streaming: tokens reach the consumer while the turn is
  // still open, not in one batch when the run finishes.
  it.effect('delivers deltas before the turn completes', () =>
    Effect.gen(function* () {
      const gate = yield* Deferred.make<void>();
      const parts = textTurn(['he', 'llo']);
      const gated = Layer.effect(
        LanguageModel.LanguageModel,
        LanguageModel.make({
          generateText: () => Effect.succeed([finish()]),
          streamText: () =>
            Stream.fromIterable(parts).pipe(
              Stream.tap((part) =>
                part.type === 'finish' ? Deferred.await(gate) : Effect.void,
              ),
            ),
        }),
      );
      const seen: string[] = [];
      const consumer = yield* plain.stream('hi').pipe(
        Stream.runForEach((event) =>
          Effect.sync(() => {
            seen.push(event._tag);
          }),
        ),
        Effect.provide(gated),
        Effect.forkChild,
      );

      yield* Effect.repeat(Effect.yieldNow, { times: 20 });
      const midTurn = [...seen];
      yield* Deferred.succeed(gate, undefined);
      yield* Fiber.join(consumer);

      expect(midTurn).toContain('Part');
      expect(midTurn).not.toContain('Completed');
      expect(seen.at(-1)).toBe('Completed');
    }),
  );

  it.effect('re-encodes transformed response parts for observers', () =>
    Effect.gen(function* () {
      const timestamp = '2026-08-18T12:34:56.000Z';
      const model = ScriptedModel.make([
        [{ type: 'response-metadata', timestamp }, finish()],
      ]);
      const events = yield* plain
        .stream('go')
        .pipe(Stream.runCollect, Effect.provide(model.layer));
      const metadata = events.find(
        (event) =>
          event._tag === 'Part' &&
          event.encodedPart.type === 'response-metadata',
      );

      expect(
        metadata?._tag === 'Part' &&
          metadata.encodedPart.type === 'response-metadata' &&
          metadata.encodedPart.timestamp,
      ).toBe(timestamp);
    }),
  );
});

describe('ScriptedModel', () => {
  const lookup = Tool.make('lookup_order', {
    description: 'Look up one order.',
    parameters: Schema.Struct({ orderId: Schema.String }),
    success: Schema.Struct({ status: Schema.String }),
  });
  const agent = Agent.make({
    name: 'scripted-model-test',
    revision: '1',
    instructions: 'Use the tool, then answer.',
    toolkit: Toolkit.make(lookup),
  }).withHandlers({
    lookup_order: ({ orderId }) =>
      Effect.succeed({ status: `${orderId}:delivered` }),
  });

  it.effect('plays tool and text turns, captures calls, fails when out', () =>
    Effect.gen(function* () {
      const model = ScriptedModel.make([
        toolTurn('lookup_order', { orderId: 'order-1' }),
        textTurn('Order order-1 was delivered.'),
      ]);
      const result = yield* agent
        .run('Where is order-1?')
        .pipe(Effect.provide(model.layer));
      const requests = yield* model.requests;

      expect(result.text).toBe('Order order-1 was delivered.');
      expect(requests.map((request) => request.operation)).toEqual([
        'streamText',
        'streamText',
      ]);
      expect(present(requests[0]).tools).toEqual(['lookup_order']);
      expect(yield* model.remaining).toEqual({ generate: 0, stream: 0 });

      const error = yield* agent
        .run('again')
        .pipe(Effect.provide(model.layer), Effect.flip);
      expect(error).toMatchObject({
        _tag: 'AiError',
        module: 'ScriptedModel',
        method: 'streamText',
      });
    }),
  );

  it.effect('injects typed failures and scripts generateText separately', () =>
    Effect.gen(function* () {
      const injected = new AiError.AiError({
        module: 'fake-provider',
        method: 'streamText',
        reason: new AiError.RateLimitError({}),
      });
      const failing = ScriptedModel.make([injected]);
      expect(
        yield* agent
          .run('fail now')
          .pipe(Effect.provide(failing.layer), Effect.flip),
      ).toBe(injected);

      const model = ScriptedModel.make([textTurn('same')], {
        generate: [
          [{ type: 'text', text: 'Earlier history summary.' }, finish()],
        ],
        repeatLast: true,
      });
      const generated = yield* LanguageModel.generateText({
        prompt: 'Summarize.',
      }).pipe(Effect.provide(model.layer));
      expect(generated.text).toBe('Earlier history summary.');
      const first = yield* agent.run('1').pipe(Effect.provide(model.layer));
      const second = yield* agent.run('2').pipe(Effect.provide(model.layer));
      expect([first.text, second.text]).toEqual(['same', 'same']);
    }),
  );
});

describe('declarative definition', () => {
  const researcher = Agent.make({
    name: 'researcher',
    revision: '1',
    description: 'Looks things up.',
    instructions: 'Answer concisely.',
    toolkit: Toolkit.make(),
  });
  const refunds: Skill.Skill = {
    name: 'refunds',
    description: 'How to process a refund.',
    instructions: 'STEP 1: verify the order.',
  };
  const reservedSkill = Tool.make(Skill.TOOL_NAME, {
    description: 'reserved',
    parameters: Schema.Struct({}),
    success: Schema.Struct({}),
  });
  const reservedChild = Tool.make('task_researcher', {
    description: 'reserved',
    parameters: Schema.Struct({}),
    success: Schema.Struct({}),
  });

  // Literal tuples reject collisions before runtime.
  const compileTimeCollisionAssertions = (): void => {
    // @ts-expect-error every definition must declare its durable revision
    Agent.make({
      name: 'missing-revision',
      instructions: 'x',
      toolkit: Toolkit.make(),
    });
    // @ts-expect-error load_skill is generated when a literal skill tuple exists
    Agent.make({
      name: 'bad-skill',
      revision: '1',
      instructions: 'x',
      toolkit: Toolkit.make(reservedSkill),
      skills: [refunds],
    });
    // @ts-expect-error task_researcher is generated from the literal child tuple
    Agent.make({
      name: 'bad-child',
      revision: '1',
      instructions: 'x',
      toolkit: Toolkit.make(reservedChild),
      subagents: [researcher],
    });
    // @ts-expect-error duplicate child names generate the same tool name
    Agent.make({
      name: 'duplicate-child',
      revision: '1',
      instructions: 'x',
      toolkit: Toolkit.make(),
      subagents: [researcher, researcher],
    });
  };
  void compileTimeCollisionAssertions;

  it('rejects invalid definitions at construction', () => {
    expect(() =>
      Agent.make({
        name: 'empty-revision',
        revision: '   ',
        instructions: 'x',
        toolkit: Toolkit.make(),
      }),
    ).toThrow('revision must be non-empty');

    const handWritten = {
      name: 'hand-written',
      revision: LogVocabulary.AgentRevision.make('1'),
      run: () =>
        Effect.succeed({
          outcome: 'success',
          text: 'ok',
          steps: 1,
          usage: { input: 0, output: 0 },
        }),
    };
    expect(() =>
      Agent.make({
        name: 'parent',
        revision: '1',
        instructions: 'x',
        toolkit: Toolkit.make(),
        // @ts-expect-error hand-written children are not branded by Agent.make
        subagents: [handWritten],
      }),
    ).toThrow('was not created by Agent.make');

    // Widened arrays dodge the compile-time check, so the runtime must hold.
    const children: ReadonlyArray<Agent.Child> = [researcher, researcher];
    const skills: ReadonlyArray<Skill.Skill> = [refunds];
    expect(() =>
      Agent.make({
        name: 'duplicate-child',
        revision: '1',
        instructions: 'x',
        toolkit: Toolkit.make(),
        subagents: children,
      }),
    ).toThrow('duplicate tool "task_researcher"');
    expect(() =>
      Agent.make({
        name: 'own-child-collision',
        revision: '1',
        instructions: 'x',
        toolkit: Toolkit.make(reservedChild),
        subagents: children.slice(0, 1),
      }),
    ).toThrow('toolkit already defines it');
    expect(() =>
      Agent.make({
        name: 'own-skill-collision',
        revision: '1',
        instructions: 'x',
        toolkit: Toolkit.make(reservedSkill),
        skills,
      }),
    ).toThrow('toolkit already defines it');

    expect(Agent.isAgent(researcher)).toBe(true);
    expect(Agent.isAgent({ [Agent.TypeId]: 'forged' })).toBe(false);
  });

  it.effect(
    'wires subagent and skill tools and keeps the prefix cacheable',
    () =>
      Effect.gen(function* () {
        const parent = Agent.make({
          name: 'parent',
          revision: '1',
          instructions: 'delegate when useful',
          toolkit: Toolkit.make(ping),
          subagents: [researcher],
          skills: [refunds],
        });
        const model = ScriptedModel.make([textTurn('ok')]);
        yield* parent
          .run('go')
          .pipe(
            Effect.provide(
              Layer.mergeAll(pingHandlers, model.layer, NodeCrypto.layer),
            ),
          );
        const request = present((yield* model.requests)[0]);
        const system = request.prompt.content
          .filter((message) => message.role === 'system')
          .map((message) => message.content)
          .join('\n');

        // Own tools survive the merge; generated tools are offered.
        expect(request.tools).toContain('ping');
        expect(request.tools).toContain(Subagent.toolName('researcher'));
        expect(request.tools).toContain(Skill.TOOL_NAME);
        // Catalog visible, body not.
        expect(system).toContain('refunds');
        expect(system).toContain('How to process a refund.');
        expect(system).not.toContain('STEP 1');

        const untouched = ScriptedModel.make([textTurn('ok')]);
        yield* Agent.make({
          name: 'plain',
          revision: '1',
          instructions: 'x',
          toolkit: Toolkit.make(ping),
        })
          .run('go')
          .pipe(Effect.provide(Layer.merge(pingHandlers, untouched.layer)));
        expect(present((yield* untouched.requests)[0]).tools).toEqual(['ping']);
      }),
  );

  it.effect(
    'returns skill instructions only when loaded, errors otherwise',
    () =>
      Effect.gen(function* () {
        const helper = Agent.make({
          name: 'helper',
          revision: '1',
          instructions: 'be helpful',
          toolkit: Toolkit.make(),
          skills: [refunds],
        });
        const model = ScriptedModel.make([
          toolTurn(Skill.TOOL_NAME, { name: 'refunds' }, 'load-1'),
          toolTurn(Skill.TOOL_NAME, { name: 'nope' }, 'load-2'),
          textTurn('done'),
        ]);
        const result = yield* helper
          .run('go')
          .pipe(Effect.provide(Layer.merge(model.layer, NodeCrypto.layer)));
        const requests = yield* model.requests;
        const promptAt = (index: number) =>
          JSON.stringify(present(requests[index]).prompt);

        expect(result.text).toBe('done');
        expect(promptAt(0)).not.toContain('STEP 1');
        expect(promptAt(1)).toContain('STEP 1: verify the order.');
        // The unknown name comes back to the model as a usable error.
        expect(promptAt(2)).toContain('nope');
      }),
  );
});

const providerFailure = () =>
  AiError.make({
    module: 'ModelPlanTest',
    method: 'generate',
    reason: new AiError.InternalProviderError({
      description: 'primary unavailable',
    }),
  });

const counted = (
  calls: Ref.Ref<number>,
  stream: () => Stream.Stream<Response.StreamPartEncoded, AiError.AiError>,
  generate: () => Effect.Effect<Response.PartEncoded[], AiError.AiError>,
) =>
  Layer.effect(
    LanguageModel.LanguageModel,
    LanguageModel.make({
      generateText: () =>
        Ref.update(calls, (count) => count + 1).pipe(
          Effect.andThen(generate()),
        ),
      streamText: () =>
        stream().pipe(Stream.onStart(Ref.update(calls, (count) => count + 1))),
    }),
  );

describe('ModelPlan', () => {
  class RetryGate extends Context.Service<
    RetryGate,
    { readonly enabled: boolean }
  >()('ModelPlanTest/RetryGate') {}

  const succeeding = (calls: Ref.Ref<number>, text: string) =>
    counted(
      calls,
      () => Stream.fromIterable(textTurn(text, 'answer')),
      () => Effect.succeed([{ type: 'text', text }, finish()]),
    );
  const failing = (calls: Ref.Ref<number>) =>
    counted(
      calls,
      () => Stream.fail(providerFailure()),
      () => Effect.fail(providerFailure()),
    );

  const compileTimeAssertions = () => {
    const model = succeeding(Ref.makeUnsafe(0), 'ok');
    const wrongErrorPlan = ExecutionPlan.make({
      provide: model,
      while: (_error: 'not-an-ai-error') => true,
    });
    const noModelPlan = ExecutionPlan.make({ provide: Layer.empty });
    const effectful = ModelPlan.layer(
      ExecutionPlan.make({
        provide: model,
        attempts: 2,
        while: ModelPlan.when(() => Effect.as(RetryGate, true)),
      }),
    );
    const requiresRetryGate: Layer.Layer<
      LanguageModel.LanguageModel,
      never,
      RetryGate
    > = effectful;
    void requiresRetryGate;
    // @ts-expect-error predicates cannot replace AiError with another input
    ModelPlan.when((_error: 'not-an-ai-error') => true);
    // @ts-expect-error model plans only inspect semantic AI failures
    ModelPlan.layer(wrongErrorPlan);
    // @ts-expect-error every model plan step must provide LanguageModel
    ModelPlan.layer(noModelPlan);
  };
  void compileTimeAssertions;

  it.effect('falls back for text, objects, and streams before first part', () =>
    Effect.gen(function* () {
      const primary = yield* Ref.make(0);
      const fallback = yield* Ref.make(0);
      const planOf = (text: string) =>
        ModelPlan.layer(
          ExecutionPlan.make(
            { provide: failing(primary) },
            { provide: succeeding(fallback, text) },
          ),
        );

      const text = yield* LanguageModel.generateText({ prompt: 'a' }).pipe(
        Effect.provide(planOf('fallback')),
      );
      const object = yield* LanguageModel.generateObject({
        prompt: 'a',
        schema: Schema.Struct({ answer: Schema.String }),
      }).pipe(Effect.provide(planOf('{"answer":"fallback"}')));
      const parts = yield* LanguageModel.streamText({ prompt: 'a' }).pipe(
        Stream.runCollect,
        Effect.provide(planOf('fallback')),
      );

      expect(text.text).toBe('fallback');
      expect(object.value).toEqual({ answer: 'fallback' });
      expect(
        parts
          .filter((part) => part.type === 'text-delta')
          .map((part) => part.delta)
          .join(''),
      ).toBe('fallback');
      expect(yield* Ref.get(primary)).toBe(3);
      expect(yield* Ref.get(fallback)).toBe(3);
    }),
  );

  it.effect('never splices a fallback into a partially emitted stream', () =>
    Effect.gen(function* () {
      const primary = yield* Ref.make(0);
      const fallback = yield* Ref.make(0);
      const partial = counted(
        primary,
        () =>
          Stream.fromIterable<Response.StreamPartEncoded>([
            { type: 'text-start', id: 'partial' },
            { type: 'text-delta', id: 'partial', delta: 'visible' },
          ]).pipe(Stream.concat(Stream.fail(providerFailure()))),
        () => Effect.fail(providerFailure()),
      );
      const error = yield* LanguageModel.streamText({ prompt: 'a' }).pipe(
        Stream.runDrain,
        Effect.provide(
          ModelPlan.layer(
            ExecutionPlan.make(
              { provide: partial },
              { provide: succeeding(fallback, 'must-not-run') },
            ),
          ),
        ),
        Effect.flip,
      );

      expect(AiError.isAiError(error)).toBe(true);
      expect(yield* Ref.get(primary)).toBe(1);
      expect(yield* Ref.get(fallback)).toBe(0);
    }),
  );

  it.effect('preserves typed tool failures without trying another model', () =>
    Effect.gen(function* () {
      const primary = yield* Ref.make(0);
      const fallback = yield* Ref.make(0);
      const blocked = Tool.make('blocked', {
        parameters: Schema.Struct({}),
        success: Schema.String,
        failure: Schema.Literal('handler-blocked'),
        failureMode: 'error',
      });
      const toolkit = Toolkit.make(blocked);
      const typedHandlers = toolkit.toLayer(
        toolkit.of({ blocked: () => Effect.fail('handler-blocked') }),
      );
      const aiErrorHandlers = toolkit.toLayer(
        toolkit.of({ blocked: () => Effect.fail(providerFailure()) }),
      );
      const callsTool = counted(
        primary,
        () => Stream.fromIterable(toolTurn('blocked', {}, 'streaming-call')),
        () =>
          Effect.succeed([
            { type: 'tool-call', id: 'c', name: 'blocked', params: {} },
            finish(),
          ]),
      );
      const plan = ModelPlan.layer(
        ExecutionPlan.make(
          { provide: callsTool },
          { provide: succeeding(fallback, 'must-not-run') },
        ),
      );

      const typed = yield* LanguageModel.generateText({
        prompt: 'call the tool',
        toolkit,
      }).pipe(Effect.provide(Layer.merge(plan, typedHandlers)), Effect.flip);
      const exact: AiError.AiError | 'handler-blocked' = typed;
      const aiError = yield* LanguageModel.generateText({
        prompt: 'call the tool',
        toolkit,
      }).pipe(Effect.provide(Layer.merge(plan, aiErrorHandlers)), Effect.flip);
      const streamed = yield* LanguageModel.streamText({
        prompt: 'call the tool',
        toolkit,
      }).pipe(
        Stream.runDrain,
        Effect.provide(Layer.merge(plan, aiErrorHandlers)),
        Effect.flip,
      );

      expect(exact).toBe('handler-blocked');
      expect(AiError.isAiError(aiError)).toBe(true);
      expect(AiError.isAiError(streamed)).toBe(true);
      expect(yield* Ref.get(primary)).toBe(3);
      expect(yield* Ref.get(fallback)).toBe(0);
    }),
  );
});
