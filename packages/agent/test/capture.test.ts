import { Context, Effect, Layer, Schema } from 'effect';
import { Tool, Toolkit } from 'effect/unstable/ai';
import { describe, expect, it } from 'vitest';

import { Agent } from '../src/agent.js';
import { handler } from '../src/subagent.js';

// Proof that capture actually propagates a subagent's services.
//
// `delegateTo` has to assert its handler record, because with a generic
// `Name` TypeScript cannot give a computed property a literal key — so a
// test written against it can be satisfied vacuously, and one earlier was.
// Everything here is concrete: literal tool name, literal handler key, no
// casts anywhere. That makes the assertions below load-bearing.

class Notebook extends Context.Service<Notebook, { readonly note: string }>()(
  'capture-test/Notebook',
) {}

const write = Tool.make('write', {
  description: 'write a note',
  parameters: Schema.Struct({ text: Schema.String }),
  success: Schema.Struct({ ok: Schema.Boolean }),
  // The child's own tool needs a service.
  dependencies: [Notebook],
});

const scribe = Agent.make({
  name: 'scribe',
  instructions: 'write it down',
  toolkit: Toolkit.make(write),
});

const delegation = Tool.make('task_scribe', {
  description: 'delegate to the scribe',
  parameters: Schema.Struct({ prompt: Schema.String }),
  success: Schema.Struct({ result: Schema.String, steps: Schema.Number }),
  failure: Schema.Struct({ refused: Schema.String }),
  failureMode: 'return',
});

const kit = Toolkit.make(delegation);

const layer = kit.toLayer(
  Effect.gen(function* () {
    // The capture. Remove it and the handler keeps `Notebook` in its own
    // requirement channel, which `HandlersFrom` rejects — this stops
    // compiling rather than silently degrading.
    const context =
      yield* Effect.context<
        Agent.WithOwnHandlers<{ readonly write: typeof write }>
      >();

    return {
      task_scribe: (input: { readonly prompt: string }) =>
        handler(scribe)(input).pipe(
          Effect.provide(context),
          Effect.map(({ result, steps }) => ({ result, steps })),
        ),
    };
  }),
);

/** A layer's input channel. All three positions are inferred deliberately:
 *  matching `Layer<unknown, unknown, infer R>` silently fails and yields
 *  `never`, which is how an earlier assertion reported the opposite of the
 *  truth. */
type LayerNeeds<L> =
  L extends Layer.Layer<infer Out, infer Err, infer R>
    ? [Out, Err] extends [unknown, unknown]
      ? R
      : never
    : never;

describe('capturing a subagent’s services', () => {
  it("puts the child's service on the delegation layer's input channel", () => {
    // All three positions inferred: matching against
    // `Layer<unknown, unknown, infer R>` silently fails and yields `never`,
    // which is how an earlier version of this assertion reported the
    // opposite of the truth.
    type Needs = LayerNeeds<typeof layer>;

    // `Notebook` reached the parent without anyone declaring it there.
    const requiresNotebook: Notebook extends Needs ? true : false = true;

    expect(requiresNotebook).toBe(true);
  });

  it('discharges the handler’s own requirements, leaving none', () => {
    // The handler is `R = never` because the captured context satisfied it.
    // That is what `HandlersFrom` demands, and why this file needs no cast.
    expect(kit.tools.task_scribe.name).toBe('task_scribe');
  });
});
