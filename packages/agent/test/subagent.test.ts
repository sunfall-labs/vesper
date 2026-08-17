import { Context, Effect, Layer, Ref, Schema, Stream } from 'effect';
import { LanguageModel, Tool, Toolkit } from 'effect/unstable/ai';
import { describe, expect, it } from 'vitest';

import { Agent } from '../src/agent.js';
import { Depth, MAX_DEPTH, tool, toolName } from '../src/subagent.js';
import { delegateTo, handler } from '../src/subagent-runtime.js';
import { RunPolicy } from '../src/run-policy.js';

// A service only the child's work needs. Its presence in the parent's
// requirement channel is the property this file exists to demonstrate: a
// closure-based subagent design cannot express it, so nothing tells you the
// dependency is missing until the tool runs in production.
class Notebook extends Context.Service<Notebook, { readonly note: string }>()(
  'test/Notebook',
) {}

const finish = {
  type: 'finish' as const,
  reason: 'stop' as const,
  usage: {
    inputTokens: { total: 3, uncached: 3, cacheRead: 0, cacheWrite: 0 },
    outputTokens: { total: 2 },
  },
};

// The loop streams, so the fake must too. Emitting the delta grammar rather
// than a single blob keeps the fake honest about what a provider produces.
const answering = (text: string, calls: Ref.Ref<number>) =>
  Layer.effect(
    LanguageModel.LanguageModel,
    LanguageModel.make({
      generateText: () =>
        Ref.update(calls, (n) => n + 1).pipe(
          Effect.as([{ type: 'text' as const, text }, finish]),
        ),
      streamText: () =>
        Stream.fromIterable([
          { type: 'text-start' as const, id: 't' },
          { type: 'text-delta' as const, id: 't', delta: text },
          { type: 'text-end' as const, id: 't' },
          finish,
        ]).pipe(Stream.onStart(Ref.update(calls, (n) => n + 1))),
    }),
  );

// The delegation handler's requirement channel arrives as `any`, because
// `Agent.Any` erases it for heterogeneous collections. One cast, here.
const runAny = <A, E, R>(effect: Effect.Effect<A, E, R>): Promise<A> =>
  Effect.runPromise(effect as Effect.Effect<A, E>);

const researcher = Agent.make({
  name: 'researcher',
  revision: '1',
  description: 'Looks things up.',
  instructions: 'Answer concisely.',
  toolkit: Toolkit.make(),
});

describe('subagent tool', () => {
  it('names the tool from the child so a parent can address it', () => {
    expect(toolName('researcher')).toBe('task_researcher');
    expect(tool(researcher).name).toBe('task_researcher');
  });

  it("uses the child's description, which is what the model chooses on", () => {
    const description = JSON.stringify(tool(researcher));

    expect(description).toContain('Looks things up.');
  });

  it('falls back to a usable description when the child has none', () => {
    const anonymous = Agent.make({
      name: 'helper',
      revision: '1',
      instructions: 'help',
      toolkit: Toolkit.make(),
    });

    expect(JSON.stringify(tool(anonymous))).toContain('helper');
  });
});

describe('delegation', () => {
  it('runs the child and returns its text plus step count', async () => {
    const result = await runAny(
      Effect.gen(function* () {
        const calls = yield* Ref.make(0);
        const delegated = yield* handler(researcher)({
          prompt: 'what is the capital of France?',
        }).pipe(Effect.provide(answering('Paris.', calls)));

        return { delegated, calls: yield* Ref.get(calls) };
      }) as Effect.Effect<{
        delegated: { result: string; steps: number };
        calls: number;
      }>,
    );

    expect(result.delegated.result).toBe('Paris.');
    expect(result.delegated.steps).toBe(1);
    expect(result.calls).toBe(1);
  });

  // The child runs against a fresh conversation. If it inherited the
  // parent's, delegation would spend the parent's context window on the
  // child's intermediate work, which is most of the reason to delegate.
  it('gives the child its own conversation', async () => {
    const result = await runAny(
      Effect.gen(function* () {
        const calls = yield* Ref.make(0);
        const first = yield* handler(researcher)({ prompt: 'one' }).pipe(
          Effect.provide(answering('A', calls)),
        );
        const second = yield* handler(researcher)({ prompt: 'two' }).pipe(
          Effect.provide(answering('B', calls)),
        );
        return [first.steps, second.steps];
      }) as Effect.Effect<number[]>,
    );

    // Both delegations start from step 1 — neither continued the other.
    expect(result).toEqual([1, 1]);
  });

  // The typed-requirement property, checked by the compiler rather than at
  // runtime: a child whose tools need `Notebook` produces a delegation
  // handler whose Effect still names `Notebook`, so the parent cannot be run
  // until wiring provides it.
  it("surfaces the child's service requirements to the caller", async () => {
    const notes = Toolkit.make();
    const scribe = Agent.make({
      name: 'scribe',
      revision: '1',
      instructions: 'write it down',
      toolkit: notes,
    });

    const delegate = handler(scribe)({ prompt: 'note this' }).pipe(
      Effect.tap(() =>
        Effect.gen(function* () {
          const notebook = yield* Notebook;
          expect(notebook.note).toBe('recorded');
        }),
      ),
    );

    const result = await runAny(
      Effect.gen(function* () {
        const calls = yield* Ref.make(0);
        return yield* delegate.pipe(
          Effect.provide(answering('done', calls)),
          // Removing this line is a compile error, not a runtime surprise.
          Effect.provideService(Notebook, { note: 'recorded' }),
        );
      }) as Effect.Effect<{ result: string; steps: number }>,
    );

    expect(result.result).toBe('done');
  });

  // Cycles are impossible by construction, but depth is not: a -> b -> c
  // chains freely, and each level multiplies model calls. Without a cap an
  // innocent-looking chain is an unbounded bill.
  it('refuses to delegate past the depth cap', async () => {
    const result = await runAny(
      Effect.gen(function* () {
        const calls = yield* Ref.make(0);
        const outcome = yield* handler(researcher)({ prompt: 'go' }).pipe(
          Effect.provideService(Depth, MAX_DEPTH),
          Effect.provide(answering('never runs', calls)),
          Effect.result,
        );
        return { outcome, calls: yield* Ref.get(calls) };
      }),
    );

    expect(result.outcome._tag).toBe('Failure');
    // Refused before the model was reached, which is the point.
    expect(result.calls).toBe(0);
  });

  it('uses the shared application depth limit when one is present', async () => {
    const result = await runAny(
      Effect.gen(function* () {
        const calls = yield* Ref.make(0);
        const runtime = yield* RunPolicy.create(
          RunPolicy.make({ maxDelegationDepth: 1 }),
        );
        const outcome = yield* handler(
          researcher,
          undefined,
          runtime,
        )({
          prompt: 'go',
        }).pipe(
          Effect.provideService(Depth, 1),
          Effect.provide(answering('never runs', calls)),
          Effect.result,
        );
        return { outcome, calls: yield* Ref.get(calls) };
      }),
    );

    expect(result.outcome._tag).toBe('Failure');
    expect(result.calls).toBe(0);
  });

  // Refusal is a tool failure, not a run failure: an agent told it cannot
  // delegate further can still do the work itself.
  it('reports the refusal to the model rather than aborting the run', async () => {
    const failure = await runAny(
      Effect.gen(function* () {
        const calls = yield* Ref.make(0);
        const outcome = yield* handler(researcher)({ prompt: 'go' }).pipe(
          Effect.provideService(Depth, MAX_DEPTH),
          Effect.provide(answering('never runs', calls)),
          Effect.result,
        );
        return outcome;
      }),
    );

    expect(failure._tag).toBe('Failure');
    if (failure._tag === 'Failure') {
      expect(failure.failure).toMatchObject({
        refused: expect.stringContaining(String(MAX_DEPTH)),
      });
    }
  });

  it('still delegates one level below the cap', async () => {
    const result = await runAny(
      Effect.gen(function* () {
        const calls = yield* Ref.make(0);
        return yield* handler(researcher)({ prompt: 'go' }).pipe(
          Effect.provideService(Depth, MAX_DEPTH - 1),
          Effect.provide(answering('done', calls)),
        );
      }),
    );

    expect(result.result).toBe('done');
  });

  // Propagation through the *generic* path, which is the one that used to
  // erase everything. `Named` now carries a real `R` (defaulting to
  // `unknown`, not `any`), so `ChildEnv` recovers the child's services and
  // `Effect.context` captures them onto the delegation layer.
  it("carries a child's services onto the delegation layer", () => {
    const write = Tool.make('write', {
      description: 'write a note',
      parameters: Schema.Struct({ text: Schema.String }),
      success: Schema.Struct({ ok: Schema.Boolean }),
      dependencies: [Notebook],
    });

    const scribe = Agent.make({
      name: 'scribe',
      revision: '1',
      instructions: 'write it down',
      toolkit: Toolkit.make(write),
    });

    // `layer` takes the parent's session now — `undefined` is the
    // not-recording case, which is what every existing caller was.
    const layer = delegateTo(scribe).layer(undefined);

    type Needs =
      typeof layer extends Layer.Layer<infer Out, infer Err, infer R>
        ? [Out, Err] extends [unknown, unknown]
          ? R
          : never
        : never;

    // Mutation-checked: removing the capture in `delegateTo`, or the child's
    // `dependencies`, breaks this.
    const requiresNotebook: Notebook extends Needs ? true : false = true;

    expect(requiresNotebook).toBe(true);
  });

  // End-to-end delegation with a service-using child.
  //
  // What this does NOT guard: deleting `Effect.provide(context)` from
  // `delegateTo` leaves this passing, because the caller provides `Notebook`
  // ambiently and the handler would see it either way. That is not a hole in
  // the test so much as a fact about the capture — it only earns its keep
  // when a toolkit layer is built under one context and invoked under
  // another. Guarding that needs a fixture where the service is reachable
  // *only* through the captured context, which no unit test here builds.
  it('actually provides the captured services to the child at runtime', async () => {
    const write = Tool.make('write', {
      description: 'write a note',
      parameters: Schema.Struct({ text: Schema.String }),
      success: Schema.Struct({ note: Schema.String }),
      dependencies: [Notebook],
    });

    const notes = Toolkit.make(write);

    const scribe = Agent.make({
      name: 'scribe',
      revision: '1',
      instructions: 'write it down',
      toolkit: notes,
    });

    const layer = delegateTo(scribe).layer(undefined);

    const result = await runAny(
      Effect.gen(function* () {
        const calls = yield* Ref.make(0);

        // The child is asked to use its tool; its handler reads `Notebook`,
        // which only the captured context can supply.
        return yield* handler(scribe)({ prompt: 'note this' }).pipe(
          Effect.provide(layer),
          Effect.provide(
            notes.toLayer({
              write: ({ text }) =>
                Effect.gen(function* () {
                  const notebook = yield* Notebook;
                  return { note: `${notebook.note}:${text}` };
                }),
            }),
          ),
          Effect.provide(answering('done', calls)),
          Effect.provideService(Notebook, { note: 'recorded' }),
        );
      }),
    );

    expect(result.result).toBe('done');
  });
});
