import { Agent } from '@sunfall/vesper-agent/agent';
import type { Effect } from 'effect';
import type { LanguageModel, Tool } from 'effect/unstable/ai';
import { expect, it } from 'vitest';

import { OrderRepo, supportAgent } from '../src/example.js';

// Type-level assertions about the public surface. These fail at `tsc`, not at
// runtime — the `it` block below only exists so a regression shows up in the
// same test run as everything else.
//
// Every assertion here has been mutation-checked: flipping its expected value
// breaks compilation. That matters because type-level tests are unusually easy
// to write vacuously.

/**
 * The only reliable any-detector: `0 extends 1 & T` holds precisely when `T`
 * is `any`. The usual `declare const x: T; const y: 'LIT' = x` probe reports
 * clean for both `any` *and* `never`.
 */
type IsAny<T> = 0 extends 1 & T ? 'ANY' : 'not-any';

/**
 * Union membership.
 *
 * The tuple wrapper is load-bearing: a naked `Member extends Union`
 * distributes over the union and yields `'yes' | 'no'`, which accepts either
 * answer and makes the assertion vacuous. A mutation test caught exactly that.
 */
type Has<Member, Union> = [Member] extends [Union] ? 'yes' : 'no';

type RunR =
  ReturnType<typeof supportAgent.run> extends Effect.Effect<
    infer _A,
    infer _E,
    infer R
  >
    ? R
    : never;

// The requirement channel is a real union, not `any`.
const _notAny: IsAny<RunR> = 'not-any';

// A handler's own dependencies stay the application's to provide...
const _repoStillRequired: Has<OrderRepo, RunR> = 'yes';
const _modelStillRequired: Has<LanguageModel.LanguageModel, RunR> = 'yes';

// ...but the handler term itself is discharged, which is the whole point of
// `withHandlers` — without it the method would compile and change nothing a
// caller can observe. Spelled as that term rather than as the whole
// requirement union, because removing exactly it is the claim being made.
const _handlersDischarged: Has<
  Tool.HandlersFor<Agent.Tools<typeof supportAgent>>,
  RunR
> = 'no';

// The utility types read back what was put in.
const _name: Agent.Name<typeof supportAgent> = 'support';

it('brands agents so they can be recognised structurally', () => {
  expect(Agent.isAgent(supportAgent)).toBe(true);
  expect(Agent.isAgent({ name: 'support' })).toBe(false);

  // `withHandlers` returns a whole agent, not a stripped record: the methods
  // survive, so handlers can be replaced later.
  expect(typeof supportAgent.withHandlers).toBe('function');
  expect(typeof supportAgent.of).toBe('function');
});
