import { Deferred, Effect, Fiber, Layer, Ref, Stream } from 'effect';
import { LanguageModel, type Response, Toolkit } from 'effect/unstable/ai';
import { describe, expect, it } from 'vitest';

import { Agent } from '../src/agent.js';
import { Stop } from '../src/stop.js';

const finish = (reason: 'stop' | 'tool-calls' = 'stop') => ({
  type: 'finish' as const,
  reason,
  usage: {
    inputTokens: { total: 10, uncached: 10, cacheRead: 0, cacheWrite: 0 },
    outputTokens: { total: 4 },
  },
});

const textTurn = (
  id: string,
  deltas: ReadonlyArray<string>,
): Response.StreamPartEncoded[] => [
  { type: 'text-start' as const, id },
  ...deltas.map((delta) => ({ type: 'text-delta' as const, id, delta })),
  { type: 'text-end' as const, id },
  finish(),
];

/** A model that plays one scripted turn per call. */
const scripted = (
  turns: ReadonlyArray<ReadonlyArray<Response.StreamPartEncoded>>,
  calls: Ref.Ref<number>,
  gate?: Deferred.Deferred<void>,
) =>
  Layer.effect(
    LanguageModel.LanguageModel,
    LanguageModel.make({
      generateText: () => Effect.succeed<Response.PartEncoded[]>([finish()]),
      streamText: () =>
        Stream.unwrap(
          Effect.gen(function* () {
            const index = yield* Ref.getAndUpdate(calls, (n) => n + 1);
            const parts = turns[Math.min(index, turns.length - 1)]!;
            return Stream.fromIterable(parts).pipe(
              Stream.tap((part) =>
                // Park before the terminal part so a test can inspect what
                // the consumer has already received mid-turn.
                gate !== undefined && part.type === 'finish'
                  ? Deferred.await(gate)
                  : Effect.void,
              ),
            );
          }),
        ),
    }),
  );

// Defects fail the test with the real cause; casting the error channel to
// `never` would hide a genuine failure behind a type assertion.
const run = <A, E>(effect: Effect.Effect<A, E>): Promise<A> =>
  Effect.runPromise(Effect.orDie(effect));

const agent = Agent.make({
  name: 'test',
  instructions: 'be terse',
  toolkit: Toolkit.make(),
});

describe('Agent.stream', () => {
  it('emits turn boundaries around the model parts', async () => {
    const tags = await run(
      Effect.gen(function* () {
        const calls = yield* Ref.make(0);
        return yield* agent.stream('hi').pipe(
          Stream.map((event) => event._tag),
          Stream.runCollect,
          Effect.provide(scripted([textTurn('a', ['he', 'llo'])], calls)),
        );
      }),
    );

    expect(tags[0]).toBe('TurnStarted');
    expect(tags[tags.length - 1]).toBe('Completed');
    expect(tags).toContain('TurnFinished');
    expect(tags.filter((tag) => tag === 'Part')).toHaveLength(5);
  });

  // The point of streaming: tokens reach the consumer while the turn is
  // still open, not in one batch when the run finishes.
  it('delivers deltas before the turn completes', async () => {
    const observed = await run(
      Effect.scoped(
        Effect.gen(function* () {
          const calls = yield* Ref.make(0);
          const gate = yield* Deferred.make<void>();
          const seen: string[] = [];

          const consumer = yield* agent.stream('hi').pipe(
            Stream.runForEach((event) =>
              Effect.sync(() => {
                seen.push(event._tag);
              }),
            ),
            Effect.provide(
              scripted([textTurn('a', ['he', 'llo'])], calls, gate),
            ),
            Effect.forkChild,
          );

          yield* Effect.repeat(Effect.yieldNow, { times: 20 });
          const midTurn = [...seen];

          yield* Deferred.succeed(gate, undefined);
          yield* Fiber.join(consumer);

          return { midTurn, final: seen };
        }),
      ),
    );

    // Parts arrived while the turn was still parked on the gate.
    expect(observed.midTurn).toContain('Part');
    expect(observed.midTurn).not.toContain('Completed');
    expect(observed.final[observed.final.length - 1]).toBe('Completed');
  });

  it('continues across turns until the stop condition holds', async () => {
    const result = await run(
      Effect.gen(function* () {
        const calls = yield* Ref.make(0);
        const twoTurns = Agent.make({
          name: 'two',
          instructions: 'x',
          toolkit: Toolkit.make(),
          stopWhen: Stop.maxSteps(2),
        });

        const tags = yield* twoTurns.stream('go').pipe(
          Stream.map((event) => event._tag),
          Stream.runCollect,
          Effect.provide(
            scripted([textTurn('a', ['one']), textTurn('b', ['two'])], calls),
          ),
        );

        return { tags, calls: yield* Ref.get(calls) };
      }),
    );

    expect(result.calls).toBe(2);
    expect(result.tags.filter((tag) => tag === 'TurnStarted')).toHaveLength(2);
    expect(result.tags.filter((tag) => tag === 'Completed')).toHaveLength(1);
  });

  it('accumulates usage across every turn, not just the last', async () => {
    const completed = await run(
      Effect.gen(function* () {
        const calls = yield* Ref.make(0);
        const twoTurns = Agent.make({
          name: 'two',
          instructions: 'x',
          toolkit: Toolkit.make(),
          stopWhen: Stop.maxSteps(2),
        });

        return yield* twoTurns.stream('go').pipe(
          Stream.filter((event) => event._tag === 'Completed'),
          Stream.runCollect,
          Effect.provide(
            scripted([textTurn('a', ['one']), textTurn('b', ['two'])], calls),
          ),
        );
      }),
    );

    expect(completed[0]!.usage).toEqual({ input: 20, output: 8 });
  });
});

describe('Agent.run', () => {
  // `run` folds `stream`, so the two cannot report different things. This
  // asserts the fold rather than a second code path.
  it('reports the same text and usage the stream ended with', async () => {
    const both = await run(
      Effect.gen(function* () {
        const calls = yield* Ref.make(0);
        const model = scripted([textTurn('a', ['he', 'llo'])], calls);

        const streamed = yield* agent.stream('hi').pipe(
          Stream.filter((event) => event._tag === 'Completed'),
          Stream.runCollect,
          Effect.provide(model),
        );

        const ran = yield* agent.run('hi').pipe(Effect.provide(model));

        return { streamed: streamed[0], ran };
      }),
    );

    expect(both.ran.text).toBe('hello');
    expect(both.ran).toMatchObject({
      text: both.streamed!.text,
      steps: both.streamed!.steps,
      usage: both.streamed!.usage,
    });
  });
});
