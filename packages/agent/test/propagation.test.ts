import { Context, Effect, Schema } from 'effect';
import { type LanguageModel, Tool, Toolkit } from 'effect/unstable/ai';
import { describe, expect, it } from '@effect/vitest';

import { Agent } from '../src/agent.js';

// A parent inherits its subagents' service requirements.
//
// This is the property the whole delegation design exists to give, and it was
// silently absent: `subagents` took `ReadonlyArray<Agent.Any>`, which erased
// each child's `R`, and `make` then asserted the parent's requirement channel
// to its own tools' requirements alone. A parent whose child read a database
// compiled without that database and died the first time the model delegated
// — the exact 2am failure the types were supposed to prevent.

class Notebook extends Context.Service<Notebook, { readonly note: string }>()(
  'propagation-test/Notebook',
) {}

const write = Tool.make('write', {
  description: 'write a note',
  parameters: Schema.Struct({ text: Schema.String }),
  success: Schema.Struct({ ok: Schema.Boolean }),
  // The child's tool declares a service. Nothing else in this file mentions
  // `Notebook` again — the point is that it arrives at the parent unaided.
  dependencies: [Notebook],
});

const scribe = Agent.make({
  name: 'scribe',
  revision: '1',
  instructions: 'write it down',
  toolkit: Toolkit.make(write),
}).withHandlers({ write: () => Effect.succeed({ ok: true }) });

const boss = Agent.make({
  name: 'boss',
  revision: '1',
  instructions: 'delegate',
  toolkit: Toolkit.make(),
  subagents: [scribe],
});

type R<T> = T extends Effect.Effect<infer _A, infer _E, infer X> ? X : never;
type BossR = R<ReturnType<typeof boss.run>>;

type IsAny<T> = 0 extends 1 & T ? 'ANY' : 'not-any';
type Has<M, U> = [M] extends [U] ? 'yes' : 'no';

// Load-bearing in this order: `[M] extends [any]` is true for *any* M, so the
// membership assertion below would pass vacuously if `BossR` were `any`.
const _notAny: IsAny<BossR> = 'not-any';

// The claim. Flipping this to 'no' fails to compile.
const _inherited: Has<Notebook, BossR> = 'yes';

// And the parent still needs a model, so the union really is a union.
const _model: Has<LanguageModel.LanguageModel, BossR> = 'yes';

// A childless agent inherits nothing — `Subagent.Services<readonly []>` must
// be `never`, not `unknown`. It was `unknown`, because `Children[number]` is
// `never` for an empty tuple and `never extends X` is *true*, so the true
// branch ran with no inference site for `R`. That `unknown` then swallowed
// the entire requirement channel of every agent without subagents.
const solo = Agent.make({
  name: 'solo',
  revision: '1',
  instructions: 'x',
  toolkit: Toolkit.make(),
});
type SoloR = R<ReturnType<typeof solo.run>>;
const _soloNotUnknown: Has<SoloR, LanguageModel.LanguageModel> = 'yes';

describe('subagent requirement propagation', () => {
  it('reaches the parent without anyone declaring it there', () => {
    // The assertions above are the test; these keep vitest honest about the
    // file having run at all.
    expect(_inherited).toBe('yes');
    expect(_notAny).toBe('not-any');
    expect(Object.keys(boss.toolkit.tools)).toContain('task_scribe');
  });
});
