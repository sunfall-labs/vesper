import { Context, Effect, Schema, type Stream } from 'effect';
import { LanguageModel, Tool, Toolkit } from 'effect/unstable/ai';
import { describe, expect, it } from 'vitest';

import { Agent } from '../src/agent.js';

// The eight narrowing assertions.
//
// `make` and `withHandlers` each hand back four entry points — `stream`,
// `run`, `streamIn`, `runIn` — and each is a hand-written `as`. TypeScript
// cannot verify them: `Layer.mergeAll` will not show inference that its
// output channel is discharged, so `Effect.provide` reports `any` and the
// assertion is what makes the result precise.
//
// That means an assertion that is too NARROW is invisible. It drops a service
// from the requirement channel, the call site compiles, and the run dies the
// first time it needs what was dropped. Two separate instances of exactly that
// shipped here: `make` discarded its subagents' services, and `withHandlers`
// discarded them again by naming its result instead of subtracting from it.
//
// So every one of the eight is pinned below, and each has been mutation-tested
// against the source rather than against itself.

class Notebook extends Context.Service<Notebook, { readonly n: string }>()(
  'assertions-test/Notebook',
) {}
class Db extends Context.Service<Db, { readonly q: string }>()(
  'assertions-test/Db',
) {}

const write = Tool.make('write', {
  description: 'child tool',
  parameters: Schema.Struct({ text: Schema.String }),
  success: Schema.Struct({ ok: Schema.Boolean }),
  dependencies: [Notebook],
});

const child = Agent.make({
  name: 'child',
  instructions: 'x',
  toolkit: Toolkit.make(write),
}).withHandlers({ write: () => Effect.succeed({ ok: true }) });

const own = Tool.make('own', {
  description: 'parent tool',
  parameters: Schema.Struct({}),
  success: Schema.Struct({ ok: Schema.Boolean }),
  dependencies: [Db],
});

// The fixture that separates the two concerns: a tool of its own needing one
// service, and a subagent needing a different one. A single-service fixture
// cannot tell "propagated correctly" from "happened to be the same type".
const parent = Agent.make({
  name: 'parent',
  instructions: 'x',
  toolkit: Toolkit.make(own),
  subagents: [child],
});

const handled = parent.withHandlers({
  own: () => Effect.succeed({ ok: true }),
});

type EffR<T> = T extends Effect.Effect<infer _A, infer _E, infer R> ? R : never;
type StrR<T> = T extends Stream.Stream<infer _A, infer _E, infer R> ? R : never;
type Has<M, U> = [M] extends [U] ? 'yes' : 'no';
type IsAny<T> = 0 extends 1 & T ? 'ANY' : 'not-any';

type Handlers = Tool.HandlersFor<Agent.Tools<typeof parent>>;

// Guard first: `[M] extends [any]` is true for every M, so every membership
// assertion below would pass vacuously against an `any` channel.
const _m1: IsAny<EffR<ReturnType<typeof parent.run>>> = 'not-any';
const _m2: IsAny<StrR<ReturnType<typeof parent.stream>>> = 'not-any';
const _h1: IsAny<EffR<ReturnType<typeof handled.run>>> = 'not-any';
const _h2: IsAny<StrR<ReturnType<typeof handled.stream>>> = 'not-any';

// --- make's four: own service, subagent's service, and the handler term ----

const _makeRun: Has<
  Db | Notebook | Handlers,
  EffR<ReturnType<typeof parent.run>>
> = 'yes';
const _makeRunIn: Has<
  Db | Notebook | Handlers,
  EffR<ReturnType<typeof parent.runIn>>
> = 'yes';
const _makeStream: Has<
  Db | Notebook | Handlers,
  StrR<ReturnType<typeof parent.stream>>
> = 'yes';
const _makeStreamIn: Has<
  Db | Notebook | Handlers,
  StrR<ReturnType<typeof parent.streamIn>>
> = 'yes';

// --- withHandlers' four: same services survive, handler term is gone -------
//
// Both halves matter. Only the first, and attaching handlers would be a no-op;
// only the second, and it would silently swallow real requirements — which is
// the bug this file was written for.

const _handledRun: Has<
  Db | Notebook,
  EffR<ReturnType<typeof handled.run>>
> = 'yes';
const _handledRunIn: Has<
  Db | Notebook,
  EffR<ReturnType<typeof handled.runIn>>
> = 'yes';
const _handledStream: Has<
  Db | Notebook,
  StrR<ReturnType<typeof handled.stream>>
> = 'yes';
const _handledStreamIn: Has<
  Db | Notebook,
  StrR<ReturnType<typeof handled.streamIn>>
> = 'yes';

const _dischargedRun: Has<
  Handlers,
  EffR<ReturnType<typeof handled.run>>
> = 'no';
const _dischargedRunIn: Has<
  Handlers,
  EffR<ReturnType<typeof handled.runIn>>
> = 'no';
const _dischargedStream: Has<
  Handlers,
  StrR<ReturnType<typeof handled.stream>>
> = 'no';
const _dischargedStreamIn: Has<
  Handlers,
  StrR<ReturnType<typeof handled.streamIn>>
> = 'no';

// The model is still required throughout — proof the channel is a real union
// and not something that collapsed to a single member.
const _model: Has<
  LanguageModel.LanguageModel,
  EffR<ReturnType<typeof handled.run>>
> = 'yes';

describe('the eight narrowing assertions', () => {
  it('keeps every entry point honest about what it still needs', () => {
    expect(_makeRun).toBe('yes');
    expect(_handledRun).toBe('yes');
    expect(_dischargedRun).toBe('no');
    expect(Object.keys(parent.toolkit.tools).sort()).toEqual([
      'own',
      'task_child',
    ]);
  });
});
