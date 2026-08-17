import { Deferred, Effect, Fiber, Layer, Ref, Stream } from 'effect';
import { LanguageModel, type Response, Toolkit } from 'effect/unstable/ai';
import { describe, expect, it } from '@effect/vitest';

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

const agent = Agent.make({
  name: 'test',
  revision: '1',
  instructions: 'be terse',
  toolkit: Toolkit.make(),
});

describe('Agent.stream', () => {
  it.effect(
    'fails an in-band provider error instead of completing an empty turn',
    () =>
      Effect.gen(function* () {
        const calls = yield* Ref.make(0);
        const outcome = yield* agent.stream('hi').pipe(
          Stream.runDrain,
          Effect.provide(
            scripted(
              [
                [
                  {
                    type: 'error',
                    error: new Error('provider stream failed'),
                  },
                ],
              ],
              calls,
            ),
          ),
          Effect.result,
        );

        expect(outcome._tag).toBe('Failure');
      }),
  );

  it.effect('emits turn boundaries around the model parts', () =>
    Effect.gen(function* () {
      const calls = yield* Ref.make(0);
      const tags = yield* agent.stream('hi').pipe(
        Stream.map((event) => event._tag),
        Stream.runCollect,
        Effect.provide(scripted([textTurn('a', ['he', 'llo'])], calls)),
      );

      expect(tags[0]).toBe('TurnStarted');
      expect(tags[tags.length - 1]).toBe('Completed');
      expect(tags).toContain('TurnFinished');
      expect(tags.filter((tag) => tag === 'Part')).toHaveLength(5);
    }),
  );

  // The point of streaming: tokens reach the consumer while the turn is
  // still open, not in one batch when the run finishes.
  it.effect('delivers deltas before the turn completes', () =>
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
        Effect.provide(scripted([textTurn('a', ['he', 'llo'])], calls, gate)),
        Effect.forkChild,
      );

      yield* Effect.repeat(Effect.yieldNow, { times: 20 });
      const midTurn = [...seen];

      yield* Deferred.succeed(gate, undefined);
      yield* Fiber.join(consumer);

      // Parts arrived while the turn was still parked on the gate.
      expect(midTurn).toContain('Part');
      expect(midTurn).not.toContain('Completed');
      expect(seen[seen.length - 1]).toBe('Completed');
    }),
  );

  it.effect('continues across turns until the stop condition holds', () =>
    Effect.gen(function* () {
      const calls = yield* Ref.make(0);
      const twoTurns = Agent.make({
        name: 'two',
        revision: '1',
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

      const result = { tags, calls: yield* Ref.get(calls) };

      expect(result.calls).toBe(2);
      expect(result.tags.filter((tag) => tag === 'TurnStarted')).toHaveLength(
        2,
      );
      expect(result.tags.filter((tag) => tag === 'Completed')).toHaveLength(1);
    }),
  );

  it.effect('accumulates usage across every turn, not just the last', () =>
    Effect.gen(function* () {
      const calls = yield* Ref.make(0);
      const twoTurns = Agent.make({
        name: 'two',
        revision: '1',
        instructions: 'x',
        toolkit: Toolkit.make(),
        stopWhen: Stop.maxSteps(2),
      });

      const completed = yield* twoTurns.stream('go').pipe(
        Stream.filter((event) => event._tag === 'Completed'),
        Stream.runCollect,
        Effect.provide(
          scripted([textTurn('a', ['one']), textTurn('b', ['two'])], calls),
        ),
      );

      expect(completed[0]!.usage).toEqual({ input: 20, output: 8 });
    }),
  );
});

describe('Agent.run', () => {
  // `run` folds `stream`, so the two cannot report different things. This
  // asserts the fold rather than a second code path.
  it.effect('reports the same text and usage the stream ended with', () =>
    Effect.gen(function* () {
      const calls = yield* Ref.make(0);
      const model = scripted([textTurn('a', ['he', 'llo'])], calls);

      const streamed = yield* agent.stream('hi').pipe(
        Stream.filter((event) => event._tag === 'Completed'),
        Stream.runCollect,
        Effect.provide(model),
      );

      const ran = yield* agent.run('hi').pipe(Effect.provide(model));

      const both = { streamed: streamed[0], ran };

      expect(both.ran.text).toBe('hello');
      expect(both.ran).toMatchObject({
        text: both.streamed!.text,
        steps: both.streamed!.steps,
        usage: both.streamed!.usage,
      });
    }),
  );
});
