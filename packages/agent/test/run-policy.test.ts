import { Deferred, Effect, Fiber, Layer, Ref, Schema, Stream } from 'effect';
import {
  LanguageModel,
  type Response,
  Tool,
  Toolkit,
} from 'effect/unstable/ai';
import { describe, expect, it } from 'vitest';

import { Agent } from '../src/agent.js';
import { RunPolicy } from '../src/run-policy.js';
import { Stop } from '../src/stop.js';

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

const failedWith = async <A, E>(
  effect: Effect.Effect<A, E>,
): Promise<string> => {
  const exit = await Effect.runPromise(Effect.exit(effect));
  expect(exit._tag).toBe('Failure');
  return String(exit);
};

describe('hard run policy', () => {
  it('does not let a steer override the shared turn ceiling', async () => {
    const runtime = await Effect.runPromise(
      RunPolicy.create(RunPolicy.make({ maxTurns: 1 })),
    );
    await Effect.runPromise(runtime.turn);
    const rendered = await failedWith(runtime.turn);
    expect(rendered).toContain('turns');
  });

  it('counts model calls independently of soft steps', async () => {
    const calls = await Effect.runPromise(Ref.make(0));
    const guarded = Agent.make({
      name: 'guarded',
      revision: '1',
      instructions: 'continue',
      toolkit: Toolkit.make(),
      stopWhen: Stop.maxSteps(3),
      runPolicy: { maxModelCalls: 1 },
    });

    const rendered = await failedWith(
      guarded.run('go').pipe(Effect.provide(model(calls))),
    );
    expect(rendered).toContain('model_calls');
    expect(await Effect.runPromise(Ref.get(calls))).toBe(1);
  });

  it('cannot be bypassed by opening a descendant agent loop', async () => {
    const calls = await Effect.runPromise(Ref.make(0));
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

    await failedWith(parent.run('go').pipe(Effect.provide(shared)));
    expect(await Effect.runPromise(Ref.get(calls))).toBe(1);
  });

  it('fails after provider-reported cumulative tokens cross a hard limit', async () => {
    const calls = await Effect.runPromise(Ref.make(0));
    const guarded = Agent.make({
      name: 'tokens',
      revision: '1',
      instructions: 'answer',
      toolkit: Toolkit.make(),
      runPolicy: { maxInputTokens: 2 },
    });

    const rendered = await failedWith(
      guarded.run('go').pipe(Effect.provide(model(calls))),
    );
    expect(rendered).toContain('input_tokens');
  });

  it('shares delegation counts and child concurrency across breadth', async () => {
    const runtime = await Effect.runPromise(
      RunPolicy.create(
        RunPolicy.make({
          maxDelegatedTasks: 2,
          maxConcurrentChildren: 1,
        }),
      ),
    );
    const firstEntered = await Effect.runPromise(Deferred.make<void>());
    const release = await Effect.runPromise(Deferred.make<void>());
    const active = await Effect.runPromise(Ref.make(0));
    const peak = await Effect.runPromise(Ref.make(0));
    const child = runtime.delegation(
      Effect.gen(function* () {
        const now = yield* Ref.updateAndGet(active, (value) => value + 1);
        yield* Ref.update(peak, (value) => Math.max(value, now));
        yield* Deferred.succeed(firstEntered, undefined);
        yield* Deferred.await(release);
        yield* Ref.update(active, (value) => value - 1);
      }),
    );

    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const fiberA = yield* Effect.forkChild(child);
          yield* Deferred.await(firstEntered);
          const fiberB = yield* Effect.forkChild(child);
          yield* Effect.sleep(10);
          expect(yield* Ref.get(peak)).toBe(1);
          yield* Deferred.succeed(release, undefined);
          yield* Fiber.join(fiberA);
          yield* Fiber.join(fiberB);
        }),
      ),
    );

    const rendered = await failedWith(runtime.delegation(Effect.void));
    expect(rendered).toContain('delegated_tasks');
  });

  it('interrupts a blocked model call at the wall-clock deadline', async () => {
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
    const rendered = await failedWith(
      guarded.run('go').pipe(Effect.provide(blocked)),
    );
    expect(rendered).toContain('deadline');
  });

  it('clamps unbounded requested tool concurrency to application policy', async () => {
    expect(RunPolicy.clampConcurrency('unbounded', 3)).toBe(3);
    expect(RunPolicy.clampConcurrency(12, 3)).toBe(3);
    expect(RunPolicy.clampConcurrency(2, 3)).toBe(2);
  });

  it('shares tool concurrency across every loop using the root runtime', async () => {
    const runtime = await Effect.runPromise(
      RunPolicy.create(RunPolicy.make({ maxToolConcurrency: 1 })),
    );
    const entered = await Effect.runPromise(Deferred.make<void>());
    const release = await Effect.runPromise(Deferred.make<void>());
    const active = await Effect.runPromise(Ref.make(0));
    const peak = await Effect.runPromise(Ref.make(0));
    const tool = runtime.tool(
      Effect.gen(function* () {
        const now = yield* Ref.updateAndGet(active, (value) => value + 1);
        yield* Ref.update(peak, (value) => Math.max(value, now));
        yield* Deferred.succeed(entered, undefined);
        yield* Deferred.await(release);
        yield* Ref.update(active, (value) => value - 1);
      }),
    );

    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const first = yield* Effect.forkChild(tool);
          yield* Deferred.await(entered);
          const second = yield* Effect.forkChild(tool);
          yield* Effect.sleep(10);
          expect(yield* Ref.get(peak)).toBe(1);
          yield* Deferred.succeed(release, undefined);
          yield* Fiber.join(first);
          yield* Fiber.join(second);
        }),
      ),
    );
  });

  it('holds one leaf permit across parent and child handler streams', async () => {
    const entered = await Effect.runPromise(Deferred.make<void>());
    const release = await Effect.runPromise(Deferred.make<void>());
    const active = await Effect.runPromise(Ref.make(0));
    const peak = await Effect.runPromise(Ref.make(0));
    const calls = await Effect.runPromise(Ref.make(0));
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
              const call = yield* Ref.getAndUpdate(calls, (value) => value + 1);
              if (call === 0) {
                return Stream.fromIterable<Response.StreamPartEncoded>([
                  calling('parent-leaf', 'work', {}),
                  calling('delegate', 'task_child-worker', { prompt: 'work' }),
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

    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const running = yield* Effect.forkChild(
            parent.run('go').pipe(Effect.provide(provider)),
          );
          yield* Deferred.await(entered);
          yield* Effect.sleep(20);
          expect(yield* Ref.get(peak)).toBe(1);
          yield* Deferred.succeed(release, undefined);
          yield* Fiber.join(running);
        }),
      ).pipe(Effect.timeout('2 seconds')),
    );
    expect(await Effect.runPromise(Ref.get(peak))).toBe(1);
  });

  it('releases a leaf permit after failure and interruption', async () => {
    const runtime = await Effect.runPromise(
      RunPolicy.create(RunPolicy.make({ maxToolConcurrency: 1 })),
    );

    await Effect.runPromise(
      runtime
        .toolStream(Stream.fail('failed'))
        .pipe(Stream.runDrain, Effect.exit),
    );
    await Effect.runPromise(
      runtime
        .toolStream(Stream.make('after failure'))
        .pipe(Stream.runDrain, Effect.timeout('1 second')),
    );

    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const blocked = yield* Effect.forkChild(
            runtime.toolStream(Stream.never).pipe(Stream.runDrain),
          );
          yield* Effect.yieldNow;
          yield* Fiber.interrupt(blocked);
          yield* runtime
            .toolStream(Stream.make('after interruption'))
            .pipe(Stream.runDrain, Effect.timeout('1 second'));
        }),
      ),
    );
  });

  it('preserves handler stream backpressure', async () => {
    const runtime = await Effect.runPromise(
      RunPolicy.create(RunPolicy.make({ maxToolConcurrency: 1 })),
    );
    const pulls = await Effect.runPromise(Ref.make(0));
    const element = (value: number) =>
      Stream.fromEffect(
        Ref.update(pulls, (count) => count + 1).pipe(Effect.as(value)),
      );
    const guarded = runtime.toolStream(
      Stream.concat(element(1), Stream.concat(element(2), element(3))),
    );

    await Effect.runPromise(guarded.pipe(Stream.runHead));
    expect(await Effect.runPromise(Ref.get(pulls))).toBe(1);
  });

  it('bounds per-signal, per-boundary, and cumulative steer bytes explicitly', async () => {
    const runtime = await Effect.runPromise(
      RunPolicy.create(
        RunPolicy.make({
          maxSignalBytes: 4,
          maxSignalsPerBoundary: 1,
          maxSteeredBytes: 3,
        }),
      ),
    );
    const cumulative = await Effect.runPromise(
      runtime.signal('steer', 'four', 0),
    );
    const oversized = await Effect.runPromise(
      runtime.signal('steer', 'large', 0),
    );
    const backlog = await Effect.runPromise(runtime.signal('steer', 'x', 1));
    expect(cumulative.exhaustion?.limit).toBe('steered_bytes');
    expect(oversized.exhaustion?.limit).toBe('signal_bytes');
    expect(backlog.exhaustion?.limit).toBe('signals_per_boundary');
  });
});
