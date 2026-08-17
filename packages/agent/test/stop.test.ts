import { describe, expect, it } from '@effect/vitest';
import { Effect, Ref } from 'effect';
import type { Response } from 'effect/unstable/ai';

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
) => condition(state(over) as Stop.State<T>);

describe('stop conditions', () => {
  it.effect(
    'stops when a turn requests no tools, and continues when it does',
    () =>
      Effect.gen(function* () {
        expect(yield* decide(Stop.noToolCalls())).toBe(true);
        expect(
          yield* decide(Stop.noToolCalls(), { toolCalls: [call('lookup')] }),
        ).toBe(false);
      }),
  );

  it.effect('treats maxSteps as a ceiling, not a target', () =>
    Effect.gen(function* () {
      expect(yield* decide(Stop.maxSteps(3), { step: 2 })).toBe(false);
      expect(yield* decide(Stop.maxSteps(3), { step: 3 })).toBe(true);
    }),
  );

  it.effect(
    'counts output tokens only, since input is not what a loop grows',
    () =>
      Effect.gen(function* () {
        const usage = { input: 10_000, output: 50 };
        expect(yield* decide(Stop.maxOutputTokens(100), { usage })).toBe(false);
        expect(yield* decide(Stop.maxOutputTokens(50), { usage })).toBe(true);
      }),
  );

  it.effect('fires on a terminal tool the moment it is called', () =>
    Effect.gen(function* () {
      expect(
        yield* decide(Stop.toolCalled('issue_refund'), {
          toolCalls: [call('lookup'), call('issue_refund')],
        }),
      ).toBe(true);
      expect(
        yield* decide(Stop.toolCalled('issue_refund'), {
          toolCalls: [call('lookup')],
        }),
      ).toBe(false);
    }),
  );

  // The boundary cases, which are the ones a composition accidentally hits:
  // an empty `any` must never stop the loop, and an empty `all` must stop it
  // immediately. Getting either backwards is an unbounded bill or a loop that
  // does nothing.
  it.effect('is honest about empty compositions', () =>
    Effect.gen(function* () {
      expect(yield* decide(Stop.any())).toBe(false);
      expect(yield* decide(Stop.all())).toBe(true);
    }),
  );

  it.effect('short-circuits `any` once something says stop', () =>
    Effect.gen(function* () {
      const calls = yield* Ref.make(0);
      const counted: Stop.StopCondition<Record<string, never>> = () =>
        Ref.update(calls, (n) => n + 1).pipe(Effect.as(false));

      yield* Stop.any(
        Stop.maxSteps(1),
        counted,
      )(state() as Stop.State<Record<string, never>>);
      const seen = yield* Ref.get(calls);

      // `maxSteps(1)` already said stop at step 1, so the second never ran.
      expect(seen).toBe(0);
    }),
  );

  it.effect('short-circuits `all` once something says continue', () =>
    Effect.gen(function* () {
      const calls = yield* Ref.make(0);
      const counted: Stop.StopCondition<Record<string, never>> = () =>
        Ref.update(calls, (n) => n + 1).pipe(Effect.as(true));

      yield* Stop.all(
        Stop.maxSteps(99),
        counted,
      )(state() as Stop.State<Record<string, never>>);
      const seen = yield* Ref.get(calls);

      expect(seen).toBe(0);
    }),
  );

  // The ceiling exists because a model stuck in a two-tool cycle is an
  // unbounded bill, so the default must stop even when tools keep being called.
  it.effect(
    'defaults to stopping on no tool calls, or at 32 steps regardless',
    () =>
      Effect.gen(function* () {
        const busy = { toolCalls: [call('lookup')] };

        expect(yield* decide(Stop.defaultCondition(), busy)).toBe(false);
        expect(
          yield* decide(Stop.defaultCondition(), { ...busy, step: 32 }),
        ).toBe(true);
        expect(yield* decide(Stop.defaultCondition())).toBe(true);
      }),
  );
});
