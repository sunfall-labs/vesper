import { describe, expect, it } from '@effect/vitest';
import { Effect, Result, Schema, Stream } from 'effect';
import { AiError, Chat, Prompt, Tool, Toolkit } from 'effect/ai';

import { Agent } from '../src/agent.js';
import { Conversation } from '../src/conversation.js';
import { Interception } from '../src/interception.js';
import { ScriptedModel } from '../src/testing.js';
import {
  CALL_ID,
  callTurn,
  outcomesOf,
  promptText,
  provide,
  readRecords,
  runSettled,
  runStarted,
  seed,
  tagsOf,
  textTurn,
  toolCalled,
  toolOutcome,
  toolStarted,
} from './tool-fixtures.js';

// Interception: the three seams (beforeTurn, beforeModelCall, beforeToolCall),
// how they meet recovery, replacement, and `Interception.compose`.

const refused = new AiError.AiError({
  module: 'test',
  method: 'intercept',
  reason: new AiError.ContentPolicyError({ description: 'policy says no' }),
});

const lookup = Tool.make('lookup', {
  description: 'look an order up',
  parameters: Schema.Struct({ id: Schema.String }),
  success: Schema.Struct({ status: Schema.String }),
});

const agentWith = (ran: { count: number }) =>
  Agent.make({
    name: 'test',
    revision: '1',
    instructions: 'be terse',
    toolkit: Toolkit.make(lookup),
  }).withHandlers({
    lookup: ({ id }) =>
      Effect.sync(() => {
        ran.count += 1;
        return { status: `fresh:${id}` };
      }),
  });

const calling = callTurn({ id: CALL_ID, name: 'lookup', params: { id: '42' } });
/** One tool-calling turn, then an answer: the agent makes two provider calls. */
const script = () => ScriptedModel.make([calling, textTurn('done')]);

/** Run an intercepted agent against a fresh script, keeping the model for inspection. */
const exec = (agent: ReturnType<typeof agentWith>, input = 'hi') => {
  const model = script();
  return {
    model,
    result: provide(agent.run(input).pipe(Effect.result), model),
  };
};

describe('beforeTurn', () => {
  it.effect(
    'fires once per turn with the earlier turns’ totals and the normalized input',
    () => {
      const seen: unknown[] = [];
      return exec(
        agentWith({ count: 0 }).intercepting({
          beforeTurn: (context) =>
            Effect.sync(() => {
              seen.push([
                context.agent,
                context.step,
                context.usage,
                JSON.stringify(context.input.content),
                context.conversationId,
              ]);
              return Interception.proceed;
            }),
        }),
        'the original question',
      ).result.pipe(
        Effect.tap(() =>
          Effect.sync(() => {
            expect(seen).toEqual([
              [
                'test',
                1,
                { input: 0, output: 0 },
                expect.stringContaining('the original question'),
                undefined,
              ],
              // Later turns add nothing unless a steer arrived.
              ['test', 2, { input: 10, output: 4 }, '[]', undefined],
            ]);
          }),
        ),
      );
    },
  );

  it.effect(
    'replaces the turn’s input, and persists the rewrite for replay',
    () => {
      const model = script();
      const agent = agentWith({ count: 0 }).intercepting({
        beforeTurn: (context) =>
          Effect.succeed(
            context.step === 1
              ? Interception.proceedWith('rewritten by the interceptor')
              : Interception.proceed,
          ),
      });
      return provide(
        Effect.gen(function* () {
          yield* Conversation.make(agent, 'rewrite').run(
            'the original question',
          );
          const prompt = promptText(yield* model.requests, 0);
          const started = JSON.stringify(
            (yield* readRecords('rewrite')).find(
              (r) => r._tag === 'RunStarted',
            ),
          );

          expect(prompt).toContain('rewritten by the interceptor');
          expect(prompt).not.toContain('the original question');
          expect(started).toContain('rewritten by the interceptor');
          expect(started).not.toContain('the original question');
        }),
        model,
      );
    },
  );

  it.effect(
    'ends the run before the turn starts when it fails, after recording the input',
    () => {
      const agent = agentWith({ count: 0 }).intercepting({
        beforeTurn: () => Effect.fail(refused),
      });
      const model = script();
      return provide(
        Effect.gen(function* () {
          const events: string[] = [];
          const streamed = yield* agent.stream('hi').pipe(
            Stream.runForEach((event) =>
              Effect.sync(() => void events.push(event._tag)),
            ),
            Effect.result,
          );
          const recorded = yield* Conversation.make(agent, 'refused')
            .run('refused input')
            .pipe(Effect.result);

          expect(Result.isFailure(streamed)).toBe(true);
          // Not even TurnStarted: a refused turn leaves no trace of a turn that never ran.
          expect(events).toEqual([]);
          expect(Result.isFailure(recorded)).toBe(true);
          expect(tagsOf(yield* readRecords('refused'))).toEqual([
            'RunStarted',
            'RunSettled',
          ]);
          expect(yield* model.requests).toEqual([]);
        }),
        model,
      );
    },
  );
});

describe('beforeModelCall', () => {
  it.effect(
    'fires once per provider call, marking a post-compaction retry',
    () => {
      const attempts: Interception.Attempt[] = [];
      const agent = agentWith({ count: 0 }).intercepting({
        beforeModelCall: (context) =>
          Effect.sync(() => void attempts.push(context.attempt)),
      });
      const overflow = new AiError.AiError({
        module: 'test',
        method: 'streamText',
        reason: new AiError.InvalidRequestError({
          constraint: 'context-window',
        }),
      });
      const compacting = ScriptedModel.make([overflow, textTurn('done')], {
        generate: [[{ type: 'text', text: 'SUMMARY' }]],
      });
      return Effect.gen(function* () {
        yield* provide(agent.run('hi'), script());
        expect(attempts).toEqual(['initial', 'initial']);

        attempts.length = 0;
        yield* provide(
          Effect.gen(function* () {
            const chat = yield* Chat.fromPrompt(
              Prompt.make([
                { role: 'system', content: 'S' },
                { role: 'user', content: 'old'.repeat(20_000) },
                { role: 'assistant', content: 'answer'.repeat(20_000) },
              ]),
            );
            yield* agent.runIn(chat, 'hi');
          }),
          compacting,
        );
        expect(attempts).toEqual(['initial', 'after-compaction']);
      });
    },
  );

  it.effect('refuses the call before the provider is reached', () => {
    const { model, result } = exec(
      agentWith({ count: 0 }).intercepting({
        beforeModelCall: () => Effect.fail(refused),
      }),
    );
    return Effect.gen(function* () {
      expect(Result.isFailure(yield* result)).toBe(true);
      // Nothing was spent and nothing reached the conversation.
      expect(yield* model.requests).toEqual([]);
    });
  });
});

describe('beforeToolCall', () => {
  it.effect(
    'is handed the call and the conversation, and lets a dispatch run',
    () => {
      const seen: unknown[] = [];
      const ran = { count: 0 };
      const agent = agentWith(ran).intercepting({
        beforeToolCall: (context) =>
          Effect.sync(() => {
            seen.push([
              context.name,
              context.toolCallId,
              context.params,
              context.conversationId,
            ]);
            return Interception.dispatch;
          }),
      });
      return provide(
        Effect.gen(function* () {
          yield* agent.run('hi');
          yield* Conversation.make(agent, 'who-am-i').run('hi');
          expect(seen).toEqual([
            ['lookup', CALL_ID, { id: '42' }, undefined],
            ['lookup', CALL_ID, { id: '42' }, 'who-am-i'],
          ]);
          expect(ran.count).toBe(2);
        }),
        ScriptedModel.make([
          calling,
          textTurn('done'),
          calling,
          textTurn('done'),
        ]),
      );
    },
  );

  it.effect(
    'answers in the tool’s place: not run, shown to the model, recorded as an ordinary outcome',
    () => {
      const ran = { count: 0 };
      const model = script();
      const agent = agentWith(ran).intercepting({
        beforeToolCall: () =>
          Effect.succeed(Interception.refuse('lookup is not allowed here')),
      });
      return provide(
        Effect.gen(function* () {
          const events = Array.from(
            yield* Conversation.make(agent, 'refuse')
              .stream('hi')
              .pipe(Stream.runCollect),
          );
          const records = yield* readRecords('refuse');
          const prompt = promptText(yield* model.requests, 1);

          expect(ran.count).toBe(0);
          expect(prompt).toContain('lookup is not allowed here');
          expect(prompt).not.toContain('fresh:42');
          // Written down like any other, so a crashed run would recover the
          // answer the interceptor gave rather than re-ask it.
          expect(outcomesOf(records)).toEqual([
            expect.objectContaining({
              id: CALL_ID,
              outcome: 'failure',
              result: 'lookup is not allowed here',
            }),
          ]);
          expect(tagsOf(records)).not.toContain('ToolStarted');
          expect(
            events.find(
              (event) =>
                event._tag === 'Part' && event.part.type === 'tool-result',
            ),
          ).toMatchObject({
            part: { isFailure: true, resultSource: 'substituted' },
          });
        }),
        model,
      );
    },
  );

  it.effect(
    'does not recover a defect from decoding a substituted result',
    () => {
      const Defective = Schema.declareConstructor<{
        readonly status: string;
      }>()([], () => () => Effect.die(new Error('result decoder defect')));
      const agent = Agent.make({
        name: 'test-defective-result',
        revision: '1',
        instructions: 'be terse',
        toolkit: Toolkit.make(
          Tool.make('lookup', {
            parameters: Schema.Struct({ id: Schema.String }),
            success: Defective,
          }),
        ),
      })
        .withHandlers({ lookup: () => Effect.succeed({ status: 'never' }) })
        .intercepting({
          beforeToolCall: () =>
            Effect.succeed(Interception.answer({ status: 'denied' })),
        });
      return provide(agent.run('hi').pipe(Effect.exit), script()).pipe(
        Effect.tap((exit) =>
          Effect.sync(() => {
            expect(exit._tag).toBe('Failure');
            expect(String(exit)).toContain('result decoder defect');
          }),
        ),
      );
    },
  );

  it.effect('fails the run when it fails, without running the tool', () => {
    const ran = { count: 0 };
    return exec(
      agentWith(ran).intercepting({
        beforeToolCall: () => Effect.fail(refused),
      }),
    ).result.pipe(
      Effect.tap((result) =>
        Effect.sync(() => {
          expect(Result.isFailure(result)).toBe(true);
          expect(ran.count).toBe(0);
        }),
      ),
    );
  });

  describe('against a log that already has an opinion', () => {
    const crashed = [
      runStarted('test'),
      toolCalled('lookup', { id: '42' }),
      toolOutcome('lookup', { status: 'from-log' }),
    ];

    it.effect(
      'serves the recovered outcome without consulting the interceptor, until the earlier run settles',
      () => {
        const consulted: string[] = [];
        const ran = { count: 0 };
        const agent = agentWith(ran).intercepting({
          beforeToolCall: (context) =>
            Effect.sync(() => {
              consulted.push(context.name);
              return Interception.refuse('denied');
            }),
        });
        return provide(
          Effect.gen(function* () {
            yield* seed('recovered', crashed);
            yield* Conversation.make(agent, 'recovered').run('hi');
            // The call already ran in the crashed run; refusing it now would
            // show the model a refusal for work that happened.
            expect(consulted).toEqual([]);
            expect(ran.count).toBe(0);
            expect(
              outcomesOf(yield* readRecords('recovered')).at(-1)?.result,
            ).toEqual({ status: 'from-log' });

            yield* seed('settled', [...crashed, runSettled]);
            yield* Conversation.make(agent, 'settled').run('hi');
            expect(consulted).toEqual(['lookup']);
            expect(ran.count).toBe(0);
          }),
          ScriptedModel.make([
            calling,
            textTurn('done'),
            calling,
            textTurn('done'),
          ]),
        );
      },
    );
  });
});

const eventTags = (agent: ReturnType<typeof agentWith>) =>
  agent.stream('hi').pipe(
    Stream.runCollect,
    Effect.map((events) =>
      Array.from(events, (event) => event._tag).filter((tag) => tag !== 'Part'),
    ),
  );

describe('an agent that is not intercepted', () => {
  it.effect(
    'emits exactly what it always did, and an empty interceptor changes nothing',
    () => {
      const ran = { count: 0 };
      return provide(
        Effect.gen(function* () {
          const plain = yield* eventTags(agentWith(ran));
          const empty = yield* eventTags(agentWith(ran).intercepting({}));
          expect(plain).toEqual([
            'TurnStarted',
            'TurnFinished',
            'TurnStarted',
            'TurnFinished',
            'Completed',
          ]);
          expect(empty).toEqual(plain);
          expect(ran.count).toBe(2);
        }),
        ScriptedModel.make([
          calling,
          textTurn('done'),
          calling,
          textTurn('done'),
        ]),
      );
    },
  );

  it.effect(
    'calling intercepting again replaces the first interceptor outright',
    () => {
      const seen: string[] = [];
      const ran = { count: 0 };
      const note = (label: string, decision: Interception.TurnDecision) => () =>
        Effect.sync(() => {
          seen.push(label);
          return decision;
        });
      return exec(
        agentWith(ran)
          .intercepting({
            beforeTurn: note('first:beforeTurn', Interception.proceed),
            beforeToolCall: () =>
              Effect.sync(() => {
                seen.push('first:beforeToolCall');
                return Interception.dispatch;
              }),
          })
          .intercepting({
            beforeTurn: note('second:beforeTurn', Interception.proceed),
          }),
      ).result.pipe(
        Effect.tap(() =>
          Effect.sync(() => {
            // Neither of the first interceptor's seams fires, not even the one
            // the second does not declare: a stacking implementation would.
            expect(seen).toEqual(['second:beforeTurn', 'second:beforeTurn']);
            expect(ran.count).toBe(1);
          }),
        ),
      );
    },
  );
});

const decide = (spec: string, input: string) =>
  spec === 'proceed'
    ? Interception.proceed
    : spec === 'echo'
      ? Interception.proceedWith(`${input} then second`)
      : Interception.proceedWith(spec.slice('proceedWith:'.length));

describe('Interception.compose', () => {
  it.effect(
    'beforeModelCall observes both operands in order, and a failing first stops the second',
    () => {
      const seen: string[] = [];
      const observe = (label: string) => ({
        beforeModelCall: (context: { readonly attempt: string }) =>
          Effect.sync(() => void seen.push(`${label}:${context.attempt}`)),
      });
      return Effect.gen(function* () {
        yield* exec(
          agentWith({ count: 0 }).intercepting(
            Interception.compose(observe('first'), observe('second')),
          ),
        ).result;
        expect(seen).toEqual([
          'first:initial',
          'second:initial',
          'first:initial',
          'second:initial',
        ]);

        seen.length = 0;
        const failed = yield* exec(
          agentWith({ count: 0 }).intercepting(
            Interception.compose(
              { beforeModelCall: () => Effect.fail(refused) },
              observe('second'),
            ),
          ),
        ).result;
        expect(Result.isFailure(failed)).toBe(true);
        expect(seen).toEqual([]);
      });
    },
  );

  const turnCases = [
    [
      'feeds the first rewrite into the second',
      'proceedWith:from first',
      'echo',
      ['from first', 'then second'],
      ['the original question'],
    ],
    [
      'keeps the first rewrite when the second has none',
      'proceedWith:from first',
      'proceed',
      ['from first'],
      ['the original question'],
    ],
    [
      'keeps the original input when neither rewrites',
      'proceed',
      'proceed',
      ['the original question'],
      [],
    ],
  ] as const;

  for (const [name, first, second, present, absent] of turnCases) {
    it.effect(`beforeTurn ${name}`, () => {
      const { model, result } = exec(
        agentWith({ count: 0 }).intercepting(
          Interception.compose(
            { beforeTurn: () => Effect.succeed(decide(first, '')) },
            {
              beforeTurn: (context) =>
                Effect.sync(() =>
                  decide(second, JSON.stringify(context.input.content)),
                ),
            },
          ),
        ),
        'the original question',
      );
      return result.pipe(
        Effect.andThen(model.requests),
        Effect.tap((requests) =>
          Effect.sync(() => {
            const prompt = promptText(requests, 0);
            for (const text of present) {
              expect(prompt).toContain(text);
            }
            for (const text of absent) {
              expect(prompt).not.toContain(text);
            }
          }),
        ),
      );
    });
  }

  const toolCases = [
    [
      'the first answers, the second is not consulted',
      'refuse',
      'dispatch',
      ['first'],
      0,
      'denied by first',
    ],
    [
      'the first dispatches, the second is consulted',
      'dispatch',
      'refuse',
      ['first', 'second'],
      0,
      'denied by second',
    ],
    [
      'neither answers, the tool runs',
      'dispatch',
      'dispatch',
      ['first', 'second'],
      1,
      'fresh:42',
    ],
  ] as const;

  for (const [name, first, second, consulted, runs, shown] of toolCases) {
    it.effect(`beforeToolCall: ${name}`, () => {
      const seen: string[] = [];
      const ran = { count: 0 };
      const seam = (label: string, decision: 'refuse' | 'dispatch') => ({
        beforeToolCall: () =>
          Effect.sync(() => {
            seen.push(label);
            return decision === 'refuse'
              ? Interception.refuse(`denied by ${label}`)
              : Interception.dispatch;
          }),
      });
      const { model, result } = exec(
        agentWith(ran).intercepting(
          Interception.compose(seam('first', first), seam('second', second)),
        ),
      );
      return result.pipe(
        Effect.andThen(model.requests),
        Effect.tap((requests) =>
          Effect.sync(() => {
            expect(seen).toEqual(consulted);
            expect(ran.count).toBe(runs);
            expect(promptText(requests, 1)).toContain(shown);
          }),
        ),
      );
    });
  }

  it.effect(
    'onIndeterminateToolCall has no neutral decision, so only the first operand is consulted',
    () => {
      const consulted: string[] = [];
      const ran = { count: 0 };
      const agent = agentWith(ran).intercepting(
        Interception.compose(
          {
            onIndeterminateToolCall: () =>
              Effect.sync(() => {
                consulted.push('first');
                return Interception.reconcile({ status: 'resolved-by-first' });
              }),
          },
          {
            onIndeterminateToolCall: () =>
              Effect.sync(() => {
                consulted.push('second');
                return Interception.retry;
              }),
          },
        ),
      );
      return provide(
        Effect.gen(function* () {
          yield* seed('indeterminate', [
            runStarted('test'),
            toolCalled('lookup', { id: '42' }),
            toolStarted('lookup'),
          ]);
          yield* Conversation.make(agent, 'indeterminate').run('hi');
          // Reconciled rather than retried: the handler did not run again.
          expect(consulted).toEqual(['first']);
          expect(ran.count).toBe(0);
        }),
        script(),
      );
    },
  );
});
