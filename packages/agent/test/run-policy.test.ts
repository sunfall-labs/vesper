import * as NodeServices from '@effect/platform-node/NodeServices';
import { describe, expect, it } from '@effect/vitest';
import { Deferred, Effect, Fiber, Layer, Ref, Schema, Stream } from 'effect';
import {
  LanguageModel,
  type Response,
  Tool,
  Toolkit,
} from 'effect/unstable/ai';

import { Agent } from '../src/agent.js';
import { RunPolicy } from '../src/run-policy.js';
import { RunPolicyRuntime } from '../src/run-policy-runtime.js';
import { Stop } from '../src/stop.js';
import { ScriptedModel } from '../src/testing.js';

const finish = (input = 3, output = 2): Response.FinishPartEncoded => ({
  type: 'finish',
  reason: 'stop',
  usage: {
    inputTokens: { total: input, uncached: input, cacheRead: 0, cacheWrite: 0 },
    outputTokens: { total: output },
  },
});

const model = (calls: Ref.Ref<number>) =>
  Layer.effect(
    LanguageModel.LanguageModel,
    LanguageModel.make({
      generateText: () => Effect.succeed([finish()]),
      streamText: () =>
        Stream.fromIterable([finish()]).pipe(
          Stream.onStart(Ref.update(calls, (value) => value + 1)),
        ),
    }),
  );

const failedWith = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.exit(effect).pipe(
    Effect.provide(NodeServices.layer),
    Effect.map((exit) => {
      expect(exit._tag).toBe('Failure');
      return String(exit);
    }),
  );

const calling = (
  id: string,
  name: string,
  params: unknown,
): Response.ToolCallPartEncoded => ({
  type: 'tool-call',
  id,
  name,
  params,
});

describe('hard run policy', () => {
  it('keeps exported defaults immutable', () => {
    expect(Object.isFrozen(RunPolicy.defaultLimits)).toBe(true);
  });

  it('rejects unknown runtime limit fields', () => {
    const limits = { maxTurns: 1, misspelledLimit: 1 };
    expect(() => RunPolicy.make(limits)).toThrow(
      'RunPolicy.misspelledLimit is not a recognized limit',
    );
  });

  it.effect('does not let a steer override the shared turn ceiling', () =>
    Effect.gen(function* () {
      const runtime = yield* RunPolicyRuntime.create(
        RunPolicy.make({ maxTurns: 1 }),
      );
      yield* runtime.turn;
      const rendered = yield* failedWith(runtime.turn);
      expect(rendered).toContain('turns');
    }),
  );

  it.effect('counts model calls independently of soft steps', () =>
    Effect.gen(function* () {
      const calls = yield* Ref.make(0);
      const guarded = Agent.make({
        name: 'guarded',
        revision: '1',
        instructions: 'continue',
        toolkit: Toolkit.make(),
        stopWhen: Stop.maxSteps(3),
        runPolicy: { maxModelCalls: 1 },
      });

      const rendered = yield* failedWith(
        guarded.run('go').pipe(Effect.provide(model(calls))),
      );
      expect(rendered).toContain('model_calls');
      expect(yield* Ref.get(calls)).toBe(1);
    }),
  );

  it.effect('cannot be bypassed by opening a descendant agent loop', () =>
    Effect.gen(function* () {
      const calls = yield* Ref.make(0);
      const child = Agent.make({
        name: 'child',
        revision: '1',
        instructions: 'answer',
        toolkit: Toolkit.make(),
      });
      const parent = Agent.make({
        name: 'parent',
        revision: '1',
        instructions: 'delegate',
        toolkit: Toolkit.make(),
        subagents: [child],
        runPolicy: { maxModelCalls: 1 },
      });
      const delegated: Response.StreamPartEncoded[] = [
        {
          type: 'tool-call',
          id: 'child-call',
          name: 'task_child',
          params: { prompt: 'work' },
        },
        { ...finish(), reason: 'tool-calls' },
      ];
      const shared = Layer.effect(
        LanguageModel.LanguageModel,
        LanguageModel.make({
          generateText: () => Effect.succeed([finish()]),
          streamText: () =>
            Stream.fromIterable(delegated).pipe(
              Stream.onStart(Ref.update(calls, (value) => value + 1)),
            ),
        }),
      );

      const rendered = yield* failedWith(
        parent.run('go').pipe(Effect.provide(shared)),
      );
      expect(rendered).toContain('model_calls');
      expect(yield* Ref.get(calls)).toBe(1);
    }),
  );

  it.effect(
    'fails after provider-reported cumulative tokens cross a hard limit',
    () =>
      Effect.gen(function* () {
        const calls = yield* Ref.make(0);
        const guarded = Agent.make({
          name: 'tokens',
          revision: '1',
          instructions: 'answer',
          toolkit: Toolkit.make(),
          runPolicy: { maxInputTokens: 2 },
        });

        const rendered = yield* failedWith(
          guarded.run('go').pipe(Effect.provide(model(calls))),
        );
        expect(rendered).toContain('input_tokens');
      }),
  );

  it.live('shares delegation counts and child concurrency across breadth', () =>
    Effect.gen(function* () {
      const runtime = yield* RunPolicyRuntime.create(
        RunPolicy.make({
          maxDelegatedTasks: 2,
          maxConcurrentChildren: 1,
        }),
      );
      const firstEntered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const active = yield* Ref.make(0);
      const peak = yield* Ref.make(0);
      const child = runtime.delegation(
        Effect.gen(function* () {
          const now = yield* Ref.updateAndGet(active, (value) => value + 1);
          yield* Ref.update(peak, (value) => Math.max(value, now));
          yield* Deferred.succeed(firstEntered, undefined);
          yield* Deferred.await(release);
          yield* Ref.update(active, (value) => value - 1);
        }),
      );

      const fiberA = yield* Effect.forkChild(child);
      yield* Deferred.await(firstEntered);
      const fiberB = yield* Effect.forkChild(child);
      yield* Deferred.succeed(release, undefined);
      yield* Fiber.join(fiberA);
      yield* Fiber.join(fiberB);
      expect(yield* Ref.get(peak)).toBe(1);

      const rendered = yield* failedWith(runtime.delegation(Effect.void));
      expect(rendered).toContain('delegated_tasks');
    }),
  );

  it.live(
    'interrupts a blocked model call at the wall-clock deadline',
    () =>
      Effect.gen(function* () {
        const blocked = Layer.effect(
          LanguageModel.LanguageModel,
          LanguageModel.make({
            generateText: () => Effect.never,
            streamText: () => Stream.never,
          }),
        );
        const guarded = Agent.make({
          name: 'deadline',
          revision: '1',
          instructions: 'wait',
          toolkit: Toolkit.make(),
          runPolicy: { wallClockMillis: 20 },
        });
        const rendered = yield* failedWith(
          guarded.run('go').pipe(Effect.provide(blocked)),
        );
        expect(rendered).toContain('deadline');
      }),
    2_000,
  );

  it.live(
    'holds one leaf permit across parent and child handler streams',
    () =>
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const active = yield* Ref.make(0);
        const peak = yield* Ref.make(0);
        const calls = yield* Ref.make(0);
        const work = Tool.make('work', {
          description: 'block until released',
          parameters: Schema.Struct({}),
          success: Schema.Struct({ done: Schema.Boolean }),
        });
        const handler = () =>
          Effect.gen(function* () {
            const now = yield* Ref.updateAndGet(active, (value) => value + 1);
            yield* Ref.update(peak, (value) => Math.max(value, now));
            yield* Deferred.succeed(entered, undefined);
            yield* Deferred.await(release);
            yield* Ref.update(active, (value) => value - 1);
            return { done: true };
          });
        const child = Agent.make({
          name: 'child-worker',
          revision: '1',
          instructions: 'work',
          toolkit: Toolkit.make(work),
        }).withHandlers({ work: handler });
        const parent = Agent.make({
          name: 'parent-worker',
          revision: '1',
          instructions: 'work and delegate',
          toolkit: Toolkit.make(work),
          subagents: [child],
          runPolicy: { maxToolConcurrency: 1 },
        }).withHandlers({ work: handler });
        const answering: Response.StreamPartEncoded[] = [
          { type: 'text-start', id: 'answer' },
          { type: 'text-delta', id: 'answer', delta: 'done' },
          { type: 'text-end', id: 'answer' },
          finish(),
        ];
        const provider = Layer.effect(
          LanguageModel.LanguageModel,
          LanguageModel.make({
            generateText: () => Effect.succeed([finish()]),
            streamText: () =>
              Stream.unwrap(
                Effect.gen(function* () {
                  const call = yield* Ref.getAndUpdate(
                    calls,
                    (value) => value + 1,
                  );
                  if (call === 0) {
                    return Stream.fromIterable<Response.StreamPartEncoded>([
                      calling('parent-leaf', 'work', {}),
                      calling('delegate', 'task_child-worker', {
                        prompt: 'work',
                      }),
                      { ...finish(), reason: 'tool-calls' },
                    ]);
                  }
                  if (call === 1) {
                    return Stream.fromIterable<Response.StreamPartEncoded>([
                      calling('child-leaf', 'work', {}),
                      { ...finish(), reason: 'tool-calls' },
                    ]);
                  }
                  return Stream.fromIterable(answering);
                }),
              ),
          }),
        );

        yield* Effect.gen(function* () {
          const running = yield* Effect.forkChild(
            parent
              .run('go')
              .pipe(Effect.provide(Layer.merge(provider, NodeServices.layer))),
          );
          yield* Deferred.await(entered);
          yield* Effect.sleep(20);
          expect(yield* Ref.get(peak)).toBe(1);
          yield* Deferred.succeed(release, undefined);
          yield* Fiber.join(running);
        });
        expect(yield* Ref.get(peak)).toBe(1);
      }),
    2_000,
  );

  it.live('releases a leaf permit after failure and interruption', () =>
    Effect.gen(function* () {
      const runtime = yield* RunPolicyRuntime.create(
        RunPolicy.make({ maxToolConcurrency: 1 }),
      );

      const failed = yield* runtime
        .toolStream(Stream.fail('failed'))
        .pipe(Stream.runDrain, Effect.exit);
      expect(failed._tag).toBe('Failure');
      yield* runtime
        .toolStream(Stream.make('after failure'))
        .pipe(Stream.runDrain, Effect.timeout('1 second'));

      const blocked = yield* Effect.forkChild(
        runtime.toolStream(Stream.never).pipe(Stream.runDrain),
      );
      yield* Effect.yieldNow;
      yield* Fiber.interrupt(blocked);
      yield* runtime
        .toolStream(Stream.make('after interruption'))
        .pipe(Stream.runDrain, Effect.timeout('1 second'));
    }),
  );

  it.effect('preserves handler stream backpressure', () =>
    Effect.gen(function* () {
      const runtime = yield* RunPolicyRuntime.create(
        RunPolicy.make({ maxToolConcurrency: 1 }),
      );
      const pulls = yield* Ref.make(0);
      const element = (value: number) =>
        Stream.fromEffect(
          Ref.update(pulls, (count) => count + 1).pipe(Effect.as(value)),
        );
      const guarded = runtime.toolStream(
        Stream.concat(element(1), Stream.concat(element(2), element(3))),
      );

      yield* guarded.pipe(Stream.runHead);
      expect(yield* Ref.get(pulls)).toBe(1);
    }),
  );

  it.effect(
    'bounds per-signal, per-boundary, and cumulative steer bytes explicitly',
    () =>
      Effect.gen(function* () {
        const runtime = yield* RunPolicyRuntime.create(
          RunPolicy.make({
            maxSignalBytes: 4,
            maxSignalsPerBoundary: 1,
            maxSteeredBytes: 3,
          }),
        );
        const cumulative = yield* runtime.signal('steer', 'four', 0);
        const oversized = yield* runtime.signal('steer', 'large', 0);
        const backlog = yield* runtime.signal('steer', 'x', 1);
        expect(cumulative).toMatchObject({
          accepted: false,
          exhaustion: { limit: 'steered_bytes' },
        });
        expect(oversized).toMatchObject({
          accepted: false,
          exhaustion: { limit: 'signal_bytes' },
        });
        expect(backlog).toMatchObject({
          accepted: false,
          exhaustion: { limit: 'signals_per_boundary' },
        });
      }),
  );
});

const finishToolCalls = (
  input: number,
  output: number,
): Response.FinishPartEncoded => ({
  type: 'finish',
  reason: 'tool-calls',
  usage: {
    inputTokens: { total: input, uncached: input, cacheRead: 0, cacheWrite: 0 },
    outputTokens: { total: output },
  },
});

const textTurn = (
  id: string,
  text: string,
  usage: Response.FinishPartEncoded = finish(),
): ReadonlyArray<Response.StreamPartEncoded> => [
  { type: 'text-start', id },
  { type: 'text-delta', id, delta: text },
  { type: 'text-end', id },
  usage,
];

const ping = Tool.make('ping', {
  description: 'ping',
  parameters: Schema.Struct({}),
  success: Schema.Struct({ ok: Schema.Boolean }),
});

describe('cost budget', () => {
  it.effect('accumulates cost across turns and exhausts the budget', () =>
    Effect.gen(function* () {
      // 1 micro-USD per token on both sides keeps the arithmetic legible:
      // each turn below costs exactly input + output micro-USD.
      const costModel: RunPolicy.CostModel = {
        inputMicrousdPerMillionTokens: 1_000_000,
        outputMicrousdPerMillionTokens: 1_000_000,
      };
      const scripted = ScriptedModel.make([
        [calling('call-1', 'ping', {}), finishToolCalls(10, 5)], // costs 15
        [calling('call-2', 'ping', {}), finishToolCalls(10, 5)], // totals 30
      ]);
      const guarded = Agent.make({
        name: 'cost-guarded',
        revision: '1',
        instructions: 'call ping until told to stop',
        toolkit: Toolkit.make(ping),
        runPolicy: { maxCostMicrousd: 20, costModel },
      }).withHandlers({ ping: () => Effect.succeed({ ok: true }) });

      const rendered = yield* failedWith(
        guarded.run('go').pipe(Effect.provide(scripted.layer)),
      );
      expect(rendered).toContain('maxCostMicrousd');

      const requests = yield* scripted.requests;
      expect(requests).toHaveLength(2);
    }),
  );

  it('fails Agent.make construction when maxCostMicrousd is set without a costModel', () => {
    expect(() =>
      Agent.make({
        name: 'no-cost-model',
        revision: '1',
        instructions: 'answer',
        toolkit: Toolkit.make(),
        runPolicy: { maxCostMicrousd: 1_000 },
      }),
    ).toThrow(RunPolicy.CostModelRequiredError);
  });

  it('fails RunPolicy.make construction when maxCostMicrousd is set without a costModel', () => {
    expect(() => RunPolicy.make({ maxCostMicrousd: 1_000 })).toThrow(
      /costModel/,
    );
  });
});

describe('onExhaustion', () => {
  it.effect(
    "'fail' (the default) still fails the run once a soft-fallback-eligible limit is exhausted",
    () =>
      Effect.gen(function* () {
        const scripted = ScriptedModel.make([
          [calling('call-1', 'ping', {}), finishToolCalls(3, 2)],
          textTurn('answer', 'never reached'),
        ]);
        const guarded = Agent.make({
          name: 'fail-mode',
          revision: '1',
          instructions: 'call ping then answer',
          toolkit: Toolkit.make(ping),
          runPolicy: { maxModelCalls: 1 },
        }).withHandlers({ ping: () => Effect.succeed({ ok: true }) });

        const rendered = yield* failedWith(
          guarded.run('go').pipe(Effect.provide(scripted.layer)),
        );
        expect(rendered).toContain('model_calls');

        const requests = yield* scripted.requests;
        expect(requests).toHaveLength(1);
      }),
  );

  it.effect(
    "'final-answer' makes exactly one extra no-tools model call and settles once maxModelCalls is exhausted",
    () =>
      Effect.gen(function* () {
        const scripted = ScriptedModel.make([
          [calling('call-1', 'ping', {}), finishToolCalls(3, 2)],
          textTurn('answer', 'the best I can do'),
        ]);
        const guarded = Agent.make({
          name: 'final-answer-mode',
          revision: '1',
          instructions: 'call ping then answer',
          toolkit: Toolkit.make(ping),
          runPolicy: { maxModelCalls: 1, onExhaustion: 'final-answer' },
        }).withHandlers({ ping: () => Effect.succeed({ ok: true }) });

        const result = yield* guarded
          .run('go')
          .pipe(Effect.provide(scripted.layer));

        expect(result.outcome).toBe('success');
        expect(result.text).toBe('the best I can do');
        expect(result).toMatchObject({
          exhausted: { limit: 'model_calls', used: 2, maximum: 1 },
        });

        const requests = yield* scripted.requests;
        expect(requests).toHaveLength(2);
        expect(requests[1]?.toolChoice).toBe('none');
      }),
  );
});
