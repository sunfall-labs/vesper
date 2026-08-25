import { describe, expect, it } from '@effect/vitest';
import { Effect, Ref, Schema, Stream } from 'effect';
import { Prompt, type Response, Tool, Toolkit } from 'effect/unstable/ai';

import { Agent } from '../src/agent.js';
import { AgentEvents } from '../src/event.js';
import { Stop } from '../src/stop.js';
import { ScriptedModel } from '../src/testing.js';

// The stop conditions decide when an agent stops calling the model, which is
// to say when it stops spending money. Only `maxSteps` was exercised, and only
// indirectly through the loop — the combinators, the boundary cases, and the
// short-circuiting were all unpinned.

const call = (name: string): Response.ToolCallPartEncoded => ({
  type: 'tool-call',
  id: 'c1',
  name,
  params: {},
});

const toolResult = (isFailure: boolean): Response.ToolResultPartEncoded => ({
  type: 'tool-result',
  id: 'c1',
  name: 'finish',
  result: isFailure ? { message: 'invalid' } : { value: 42 },
  isFailure,
});

const finish = (reason: 'stop' | 'tool-calls' = 'stop') => ({
  type: 'finish' as const,
  reason,
  usage: {
    inputTokens: { total: 1, uncached: 1, cacheRead: 0, cacheWrite: 0 },
    outputTokens: { total: 1 },
  },
});

const searchTool = Tool.make('search', {
  description: 'Search for something.',
  parameters: Schema.Struct({ query: Schema.String }),
  success: Schema.Struct({ result: Schema.String }),
});

const searcher = Agent.make({
  name: 'searcher',
  revision: '1',
  instructions: 'x',
  toolkit: Toolkit.make(searchTool),
  stopWhen: Stop.toolCalledTimes('search', 3),
}).withHandlers({
  search: ({ query }) => Effect.succeed({ result: `found ${query}` }),
});

const searchCall = (
  id: string,
  query: string,
): Response.ToolCallPartEncoded => ({
  type: 'tool-call',
  id,
  name: 'search',
  params: { query },
});

const state = (over: Partial<Stop.State<Record<string, never>>> = {}) => ({
  step: 1,
  toolCalls: [] as ReadonlyArray<Response.ToolCallPartEncoded>,
  toolResults: [] as ReadonlyArray<Response.ToolResultPartEncoded>,
  response: Prompt.empty,
  finishReason: 'stop' as const,
  text: '',
  reasoning: '',
  usage: { input: 0, output: 0 },
  toolCallCounts: {} as Readonly<Record<string, number>>,
  ...over,
});

const decide = <T extends Record<string, never>>(
  condition: Stop.StopCondition<T>,
  over: Partial<Stop.State<T>> = {},
) => condition(state(over) as Stop.State<T>);

describe('stop conditions', () => {
  it.effect('rejects non-finite token usage', () =>
    Effect.gen(function* () {
      const result = yield* Schema.decodeEffect(Stop.Usage)({
        input: Number.NaN,
        output: Number.POSITIVE_INFINITY,
      }).pipe(Effect.result);

      expect(result._tag).toBe('Failure');
    }),
  );

  it.effect('rejects invalid lifecycle counters', () =>
    Effect.gen(function* () {
      const result = yield* Schema.decodeEffect(AgentEvents.Lifecycle)({
        _tag: 'SignalBacklog',
        step: -1,
        maximum: Number.POSITIVE_INFINITY,
      }).pipe(Effect.result);

      expect(result._tag).toBe('Failure');
    }),
  );

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

  it.effect(
    'distinguishes a successful terminal result from a failed call',
    () =>
      Effect.gen(function* () {
        expect(
          yield* decide(Stop.toolSucceeded('finish'), {
            toolResults: [toolResult(false)],
          }),
        ).toBe(true);
        expect(
          yield* decide(Stop.toolSucceeded('finish'), {
            toolResults: [toolResult(true)],
          }),
        ).toBe(false);
        expect(
          yield* decide(Stop.toolFailed('finish'), {
            toolResults: [toolResult(true)],
          }),
        ).toBe(true);
      }),
  );

  it.effect(
    'stops on the successful result of an ordinary Effect AI tool',
    () =>
      Effect.gen(function* () {
        const attempts = yield* Ref.make(0);
        const submit = Tool.make('submit', {
          description: 'Submit the final structured answer.',
          parameters: Schema.Struct({ value: Schema.Finite }),
          success: Schema.Struct({ accepted: Schema.Boolean }),
          failure: Schema.Struct({ reason: Schema.String }),
          failureMode: 'return',
        });
        const terminal = Agent.make({
          name: 'terminal-result',
          revision: '1',
          instructions: 'Submit a valid answer.',
          toolkit: Toolkit.make(submit),
          stopWhen: Stop.toolSucceeded('submit'),
        }).withHandlers({
          submit: () =>
            Ref.getAndUpdate(attempts, (count) => count + 1).pipe(
              Effect.flatMap((count) =>
                count === 0
                  ? Effect.fail({ reason: 'try again' })
                  : Effect.succeed({ accepted: true }),
              ),
            ),
        });
        const model = ScriptedModel.make([
          [
            {
              type: 'tool-call',
              id: 'bad',
              name: 'submit',
              params: { value: 1 },
            },
            finish('tool-calls'),
          ],
          [
            {
              type: 'tool-call',
              id: 'good',
              name: 'submit',
              params: { value: 2 },
            },
            finish('tool-calls'),
          ],
        ]);

        const result = yield* terminal
          .run('go')
          .pipe(Effect.provide(model.layer));
        const requests = yield* model.requests;

        expect(requests).toHaveLength(2);
        expect(result.steps).toBe(2);
        expect(result.response?.content[0]).toMatchObject({
          role: 'assistant',
          content: [{ type: 'tool-call', id: 'good', name: 'submit' }],
        });
        expect(result.response?.content[1]).toMatchObject({
          role: 'tool',
          content: [
            {
              type: 'tool-result',
              id: 'good',
              name: 'submit',
              isFailure: false,
            },
          ],
        });
      }),
  );

  // `toolCallCounts` is cumulative across the whole run, unlike `toolCalls`
  // which only ever holds the turn just requested — so this reads the
  // cumulative field rather than counting `toolCalls` itself.
  it.effect(
    'fires once a tool has been called `times` times in total, not per turn',
    () =>
      Effect.gen(function* () {
        expect(
          yield* decide(Stop.toolCalledTimes('search', 3), {
            toolCallCounts: { search: 2 },
          }),
        ).toBe(false);
        expect(
          yield* decide(Stop.toolCalledTimes('search', 3), {
            toolCallCounts: { search: 3 },
          }),
        ).toBe(true);
        // A tool never seen has no entry at all, not a zero.
        expect(
          yield* decide(Stop.toolCalledTimes('search', 1), {
            toolCallCounts: {},
          }),
        ).toBe(false);
      }),
  );

  it.effect('composes `toolCalledTimes` with `any` and `all`', () =>
    Effect.gen(function* () {
      const counts = { search: 3, issue_refund: 1 };

      expect(
        yield* decide(
          Stop.any(Stop.toolCalledTimes('search', 3), Stop.maxSteps(99)),
          { toolCallCounts: counts },
        ),
      ).toBe(true);
      expect(
        yield* decide(
          Stop.all(
            Stop.toolCalledTimes('search', 3),
            Stop.toolCalledTimes('issue_refund', 2),
          ),
          { toolCallCounts: counts },
        ),
      ).toBe(false);
      expect(
        yield* decide(
          Stop.all(
            Stop.toolCalledTimes('search', 3),
            Stop.toolCalledTimes('issue_refund', 1),
          ),
          { toolCallCounts: counts },
        ),
      ).toBe(true);
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

      yield* Stop.any(Stop.maxSteps(1), counted)(state());
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

      yield* Stop.all(Stop.maxSteps(99), counted)(state());
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

  // Exercises the loop, not just the pure function: `toolCallCounts` has to
  // be threaded turn to turn by `agent.ts`'s `decide` stage, and this is
  // what pins that wiring rather than only the arithmetic above. Two calls
  // to `search` land in the first turn and a third in the second, so a
  // per-turn count would never reach 3 and the run would never stop.
  it.effect('counts tool calls cumulatively across turns, not per turn', () =>
    Effect.gen(function* () {
      const model = ScriptedModel.make([
        [
          searchCall('c1', 'a'),
          searchCall('c2', 'b'),
          finish('tool-calls'),
        ] satisfies ReadonlyArray<Response.StreamPartEncoded>,
        [
          searchCall('c3', 'c'),
          finish('tool-calls'),
        ] satisfies ReadonlyArray<Response.StreamPartEncoded>,
      ]);

      const tags = yield* searcher.stream('go').pipe(
        Stream.map((event) => event._tag),
        Stream.runCollect,
        Effect.provide(model.layer),
      );
      const requests = yield* model.requests;

      // The third `search` call is what crosses the threshold, so the
      // model is asked for exactly two turns — not a third to observe the
      // tool result, and not stuck forever because a per-turn count of 1
      // never reaches 3.
      expect(requests).toHaveLength(2);
      expect(
        Array.from(tags).filter((tag) => tag === 'TurnStarted'),
      ).toHaveLength(2);
      expect(
        Array.from(tags).filter((tag) => tag === 'Completed'),
      ).toHaveLength(1);
    }),
  );
});

// Compile-time: `Stop.toolCalledTimes` is keyed to the toolkit the same way
// `Stop.toolCalled` is, so naming a tool the agent does not have is a type
// error rather than a condition that silently never fires.
const stopConditionToolNameAssertions = (): void => {
  const valid: Stop.StopCondition<Agent.Tools<typeof searcher>> =
    Stop.toolCalledTimes('search', 3);
  void valid;
  const misspelled: Stop.StopCondition<Agent.Tools<typeof searcher>> =
    // @ts-expect-error Tool names are checked against the toolkit.
    Stop.toolCalledTimes('serach', 3);
  void misspelled;
};
void stopConditionToolNameAssertions;
