import { Effect, Ref } from 'effect';
import type { Response } from 'effect/unstable/ai';
import { describe, expect, it } from 'vitest';

import { Stop } from '../src/stop.js';

// The stop conditions decide when an agent stops calling the model, which is
// to say when it stops spending money. Only `maxSteps` was exercised, and only
// indirectly through the loop — the combinators, the boundary cases, and the
// short-circuiting were all unpinned.

const call = (name: string): Response.ToolCallPartEncoded =>
  ({ type: 'tool-call', id: 'c1', name, params: {} }) as never;

const state = (over: Partial<Stop.State<Record<string, never>>> = {}) => ({
  step: 1,
  toolCalls: [] as ReadonlyArray<Response.ToolCallPartEncoded>,
  usage: { input: 0, output: 0 },
  ...over,
});

const decide = <T extends Record<string, never>>(
  condition: Stop.StopCondition<T>,
  over: Partial<Stop.State<T>> = {},
) => Effect.runPromise(condition(state(over) as Stop.State<T>));

describe('stop conditions', () => {
  it('stops when a turn requests no tools, and continues when it does', async () => {
    await expect(decide(Stop.noToolCalls())).resolves.toBe(true);
    await expect(
      decide(Stop.noToolCalls(), { toolCalls: [call('lookup')] }),
    ).resolves.toBe(false);
  });

  it('treats maxSteps as a ceiling, not a target', async () => {
    await expect(decide(Stop.maxSteps(3), { step: 2 })).resolves.toBe(false);
    await expect(decide(Stop.maxSteps(3), { step: 3 })).resolves.toBe(true);
  });

  it('counts output tokens only, since input is not what a loop grows', async () => {
    const usage = { input: 10_000, output: 50 };
    await expect(decide(Stop.maxOutputTokens(100), { usage })).resolves.toBe(
      false,
    );
    await expect(decide(Stop.maxOutputTokens(50), { usage })).resolves.toBe(
      true,
    );
  });

  it('fires on a terminal tool the moment it is called', async () => {
    await expect(
      decide(Stop.toolCalled('issue_refund'), {
        toolCalls: [call('lookup'), call('issue_refund')],
      }),
    ).resolves.toBe(true);
    await expect(
      decide(Stop.toolCalled('issue_refund'), { toolCalls: [call('lookup')] }),
    ).resolves.toBe(false);
  });

  // The boundary cases, which are the ones a composition accidentally hits:
  // an empty `any` must never stop the loop, and an empty `all` must stop it
  // immediately. Getting either backwards is an unbounded bill or a loop that
  // does nothing.
  it('is honest about empty compositions', async () => {
    await expect(decide(Stop.any())).resolves.toBe(false);
    await expect(decide(Stop.all())).resolves.toBe(true);
  });

  it('short-circuits `any` once something says stop', async () => {
    const seen = await Effect.runPromise(
      Effect.gen(function* () {
        const calls = yield* Ref.make(0);
        const counted: Stop.StopCondition<Record<string, never>> = () =>
          Ref.update(calls, (n) => n + 1).pipe(Effect.as(false));

        yield* Stop.any(
          Stop.maxSteps(1),
          counted,
        )(state() as Stop.State<Record<string, never>>);
        return yield* Ref.get(calls);
      }),
    );

    // `maxSteps(1)` already said stop at step 1, so the second never ran.
    expect(seen).toBe(0);
  });

  it('short-circuits `all` once something says continue', async () => {
    const seen = await Effect.runPromise(
      Effect.gen(function* () {
        const calls = yield* Ref.make(0);
        const counted: Stop.StopCondition<Record<string, never>> = () =>
          Ref.update(calls, (n) => n + 1).pipe(Effect.as(true));

        yield* Stop.all(
          Stop.maxSteps(99),
          counted,
        )(state() as Stop.State<Record<string, never>>);
        return yield* Ref.get(calls);
      }),
    );

    expect(seen).toBe(0);
  });

  // The ceiling exists because a model stuck in a two-tool cycle is an
  // unbounded bill, so the default must stop even when tools keep being called.
  it('defaults to stopping on no tool calls, or at 32 steps regardless', async () => {
    const busy = { toolCalls: [call('lookup')] };

    await expect(decide(Stop.defaultCondition(), busy)).resolves.toBe(false);
    await expect(
      decide(Stop.defaultCondition(), { ...busy, step: 32 }),
    ).resolves.toBe(true);
    await expect(decide(Stop.defaultCondition())).resolves.toBe(true);
  });
});
