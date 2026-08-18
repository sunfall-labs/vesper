import { LogStoreMemory } from '@sunfall/vesper-log/layer-memory';
import { LogStore } from '@sunfall/vesper-log/log-store';
import type { ConversationRecord } from '@sunfall/vesper-log/record';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';
import { Context, Effect, Layer, Ref, Schema, Stream } from 'effect';
import {
  AiError,
  Chat,
  LanguageModel,
  Prompt,
  type Response,
  Tool,
  Toolkit,
} from 'effect/unstable/ai';
import { describe, expect, it, test } from '@effect/vitest';

import { Agent } from '../src/agent.js';
import { Conversation } from '../src/conversation.js';
import { AgentEvents } from '../src/event.js';
import { Interception } from '../src/interception.js';
import { AgentLog } from '../src/log.js';

// The three seams, and what each is allowed to do.
//
// Every assertion below is mutation-checked against the source — the seam it
// exercises was removed or inverted in `agent.ts`/`dispatch.ts` and the test
// confirmed to fail — because a test that watches an optional callback is the
// easy kind to write vacuously: an interceptor whose seam is never reached
// records nothing, and "records nothing" is also what a passing observation
// test looks like if the assertion is on the wrong side.

const finish = (reason: 'stop' | 'tool-calls' = 'stop') => ({
  type: 'finish' as const,
  reason,
  usage: {
    inputTokens: { total: 10, uncached: 10, cacheRead: 0, cacheWrite: 0 },
    outputTokens: { total: 4 },
  },
});

const CALL_ID = LogVocabulary.ToolCallId.make('call-1');
const quiet = test.extend({ disableErrorReporting: true });

const callingTurn: Response.StreamPartEncoded[] = [
  {
    type: 'tool-call' as const,
    id: CALL_ID,
    name: 'lookup',
    params: { id: '42' },
  },
  finish('tool-calls'),
];

const answeringTurn: Response.StreamPartEncoded[] = [
  { type: 'text-start' as const, id: 'b' },
  { type: 'text-delta' as const, id: 'b', delta: 'done' },
  { type: 'text-end' as const, id: 'b' },
  finish(),
];

const overflow = new AiError.AiError({
  module: 'test',
  method: 'streamText',
  reason: new AiError.InvalidRequestError({ constraint: 'context-window' }),
});

const refused = new AiError.AiError({
  module: 'test',
  method: 'intercept',
  reason: new AiError.ContentPolicyError({ description: 'policy says no' }),
});

/** One provider call: either the parts it streams, or the error it fails with. */
type Call = ReadonlyArray<Response.StreamPartEncoded> | AiError.AiError;

/**
 * A model driven by a script, recording the prompt it was handed per call.
 *
 * The last entry repeats, so a script only has to describe the calls a test
 * cares about.
 */
const scripted = (calls: ReadonlyArray<Call>, prompts: string[] = []) =>
  Layer.effect(
    LanguageModel.LanguageModel,
    Effect.gen(function* () {
      const made = yield* Ref.make(0);
      return yield* LanguageModel.make({
        generateText: () => Effect.succeed<Response.PartEncoded[]>([finish()]),
        streamText: (options) =>
          Stream.unwrap(
            Effect.gen(function* () {
              const index = yield* Ref.getAndUpdate(made, (n) => n + 1);
              prompts.push(JSON.stringify(options.prompt));
              const call = calls[Math.min(index, calls.length - 1)]!;
              return Array.isArray(call)
                ? Stream.fromIterable(call)
                : Stream.fail(call as AiError.AiError);
            }),
          ),
      });
    }),
  );

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

const run = <A, E>(
  effect: Effect.Effect<A, E, LogStore.Service | LanguageModel.LanguageModel>,
  calls: ReadonlyArray<Call> = [callingTurn, answeringTurn],
  prompts: string[] = [],
) =>
  effect.pipe(
    Effect.orDie,
    Effect.provide(scripted(calls, prompts)),
    Effect.provide(LogStoreMemory.layer),
    Effect.scoped,
  );

const runQuiet = <A>(effect: Effect.Effect<A>) => Effect.exit(effect);

// ---------------------------------------------------------------- beforeTurn

describe('beforeTurn', () => {
  it.effect(
    'fires once per turn, with the totals the earlier turns produced',
    () =>
      Effect.gen(function* () {
        const seen: ReadonlyArray<unknown>[] = [];
        const ran = { count: 0 };

        yield* run(
          agentWith(ran)
            .intercepting({
              beforeTurn: (context) =>
                Effect.sync(() => {
                  seen.push([context.agent, context.step, context.usage]);
                  return Interception.proceed;
                }),
            })
            .run('hi'),
        );

        expect(seen).toEqual([
          ['test', 1, { input: 0, output: 0 }],
          ['test', 2, { input: 10, output: 4 }],
        ]);
      }),
  );

  it.effect('replaces the turn’s input when it says so', () =>
    Effect.gen(function* () {
      const prompts: string[] = [];
      const ran = { count: 0 };

      yield* run(
        agentWith(ran)
          .intercepting({
            beforeTurn: (context) =>
              Effect.succeed(
                context.step === 1
                  ? Interception.proceedWith('rewritten by the interceptor')
                  : Interception.proceed,
              ),
          })
          .run('the original question'),
        [callingTurn, answeringTurn],
        prompts,
      );

      expect(prompts[0]).toContain('rewritten by the interceptor');
      expect(prompts[0]).not.toContain('the original question');
    }),
  );

  it.effect('persists the rewritten input reconstruction will replay', () =>
    Effect.gen(function* () {
      const ran = { count: 0 };

      const written = yield* run(
        Effect.gen(function* () {
          yield* Conversation.make(
            agentWith(ran).intercepting({
              beforeTurn: () =>
                Effect.succeed(Interception.proceedWith('persisted rewrite')),
            }),
            CONVERSATION,
          ).run('discarded original');
          return yield* readAll();
        }),
      );

      const started = written.find(
        (envelope) => envelope.record._tag === 'RunStarted',
      )?.record;
      expect(JSON.stringify(started)).toContain('persisted rewrite');
      expect(JSON.stringify(started)).not.toContain('discarded original');
    }),
  );

  it.effect('is handed the input it is being asked about', () =>
    Effect.gen(function* () {
      const seen: string[] = [];
      const ran = { count: 0 };

      yield* run(
        agentWith(ran)
          .intercepting({
            beforeTurn: (context) =>
              Effect.sync(() => {
                seen.push(JSON.stringify(context.input.content));
                return Interception.proceed;
              }),
          })
          .run('the original question'),
      );

      // Normalised to a `Prompt`, so a `RawInput` string arrives as a message
      // rather than as a string an interceptor would have to re-normalise.
      expect(seen[0]).toContain('the original question');
      // Later turns add nothing unless a steer arrived.
      expect(seen[1]).toBe('[]');
    }),
  );

  quiet(
    'ends the run without starting the turn when it fails',
    ({ disableErrorReporting: _disableErrorReporting }) =>
      runQuiet(
        Effect.gen(function* () {
          const events: string[] = [];
          const ran = { count: 0 };

          const exit = yield* agentWith(ran)
            .intercepting({ beforeTurn: () => Effect.fail(refused) })
            .stream('hi')
            .pipe(
              Stream.runForEach((event) =>
                Effect.sync(() => {
                  events.push(event._tag);
                }),
              ),
              Effect.result,
              Effect.provide(scripted([callingTurn, answeringTurn])),
              Effect.scoped,
            );

          expect(exit._tag).toBe('Failure');
          // Not even `TurnStarted`: the seam is *before* a turn, so a refused turn
          // leaves no trace of a turn that never ran.
          expect(events).toEqual([]);
        }),
      ),
  );

  quiet(
    'settles a refused recorded run only after recording its input',
    ({ disableErrorReporting: _disableErrorReporting }) =>
      runQuiet(
        Effect.gen(function* () {
          const ran = { count: 0 };

          const tags = yield* run(
            Effect.gen(function* () {
              yield* Conversation.make(
                agentWith(ran).intercepting({
                  beforeTurn: () => Effect.fail(refused),
                }),
                CONVERSATION,
              )
                .run('refused input')
                .pipe(Effect.result);
              return (yield* readAll()).map((envelope) => envelope.record._tag);
            }),
          );

          expect(tags).toEqual(['RunStarted', 'RunSettled']);
        }),
      ),
  );
});

// ----------------------------------------------------------- beforeModelCall

describe('beforeModelCall', () => {
  it.effect('fires once per provider call', () =>
    Effect.gen(function* () {
      const seen: Interception.Attempt[] = [];
      const ran = { count: 0 };

      yield* run(
        agentWith(ran)
          .intercepting({
            beforeModelCall: (context) =>
              Effect.sync(() => {
                seen.push(context.attempt);
              }),
          })
          .run('hi'),
      );

      expect(seen).toEqual(['initial', 'initial']);
    }),
  );

  it.effect(
    'fires again, marked as a retry, when compaction retries the turn',
    () =>
      Effect.gen(function* () {
        const seen: Interception.Attempt[] = [];
        const ran = { count: 0 };

        const intercepted = agentWith(ran).intercepting({
          beforeModelCall: (context) =>
            Effect.sync(() => {
              seen.push(context.attempt);
            }),
        });

        yield* run(
          Effect.gen(function* () {
            const chat = yield* Chat.fromPrompt(
              Prompt.make([
                { role: 'system', content: 'S' },
                { role: 'user', content: 'old'.repeat(20_000) },
                { role: 'assistant', content: 'answer'.repeat(20_000) },
              ]),
            );
            yield* intercepted.runIn(chat, 'hi');
          }),
          [overflow, answeringTurn],
        );

        expect(seen).toEqual(['initial', 'after-compaction']);
      }),
  );

  quiet(
    'refuses the call before the provider is reached',
    ({ disableErrorReporting: _disableErrorReporting }) =>
      runQuiet(
        Effect.gen(function* () {
          const prompts: string[] = [];
          const ran = { count: 0 };

          const exit = yield* agentWith(ran)
            .intercepting({ beforeModelCall: () => Effect.fail(refused) })
            .run('hi')
            .pipe(
              Effect.result,
              Effect.provide(scripted([callingTurn, answeringTurn], prompts)),
              Effect.scoped,
            );

          expect(exit._tag).toBe('Failure');
          // The point of a seam *before* the call: the provider was never asked, so
          // nothing was spent and nothing reached the conversation.
          expect(prompts).toEqual([]);
        }),
      ),
  );
});

// ------------------------------------------------------------ beforeToolCall

describe('beforeToolCall', () => {
  it.effect('is handed the call the model asked for', () =>
    Effect.gen(function* () {
      const seen: unknown[] = [];
      const ran = { count: 0 };

      yield* run(
        agentWith(ran)
          .intercepting({
            beforeToolCall: (context) =>
              Effect.sync(() => {
                seen.push([context.name, context.toolCallId, context.params]);
                return Interception.dispatch;
              }),
          })
          .run('hi'),
      );

      expect(seen).toEqual([['lookup', CALL_ID, { id: '42' }]]);
      expect(ran.count).toBe(1);
    }),
  );

  it.effect('answers in the tool’s place without running it', () =>
    Effect.gen(function* () {
      const prompts: string[] = [];
      const ran = { count: 0 };

      yield* run(
        agentWith(ran)
          .intercepting({
            beforeToolCall: () =>
              Effect.succeed(Interception.refuse('lookup is not allowed here')),
          })
          .run('hi'),
        [callingTurn, answeringTurn],
        prompts,
      );

      expect(ran.count).toBe(0);
      // What the model is shown is the substituted result, in the same place a
      // real one would have been.
      expect(prompts[1]).toContain('lookup is not allowed here');
      expect(prompts[1]).not.toContain('fresh:42');
    }),
  );

  it.effect('records a substituted answer as an ordinary tool outcome', () =>
    Effect.gen(function* () {
      const ran = { count: 0 };

      const written = yield* run(
        Effect.gen(function* () {
          yield* Conversation.make(
            agentWith(ran).intercepting({
              beforeToolCall: () =>
                Effect.succeed(Interception.refuse('denied')),
            }),
            CONVERSATION,
          )
            .run('hi')
            .pipe(Effect.orDie);
          return yield* readAll();
        }),
      );

      // The claim in `dispatch.ts`: an answered call is written down like any
      // other, so if *this* run crashed a later one would recover the answer the
      // interceptor gave rather than re-asking it.
      expect(outcomesOf(written)).toEqual([
        expect.objectContaining({
          id: CALL_ID,
          name: 'lookup',
          outcome: 'failure',
          result: 'denied',
        }),
      ]);
      expect(written.some(({ record }) => record._tag === 'ToolStarted')).toBe(
        false,
      );
    }),
  );

  quiet(
    'fails the run when it fails',
    ({ disableErrorReporting: _disableErrorReporting }) =>
      runQuiet(
        Effect.gen(function* () {
          const ran = { count: 0 };

          const exit = yield* agentWith(ran)
            .intercepting({ beforeToolCall: () => Effect.fail(refused) })
            .run('hi')
            .pipe(
              Effect.result,
              Effect.provide(scripted([callingTurn, answeringTurn])),
              Effect.scoped,
            );

          expect(exit._tag).toBe('Failure');
          expect(ran.count).toBe(0);
        }),
      ),
  );
});

// ------------------------------------------------- interception vs. recovery

const CONVERSATION = LogVocabulary.ConversationId.make(
  'interception-conversation',
);
const PATH = AgentLog.pathFor(CONVERSATION);

const seed = Effect.fn('test.seed')(function* (
  records: ReadonlyArray<ConversationRecord.Record>,
) {
  const store = yield* LogStore.Service;
  yield* store.create(PATH, CONVERSATION).pipe(Effect.orDie);
  const claim = yield* store
    .acquire(PATH, LogVocabulary.ProducerId.make('previous-run'))
    .pipe(Effect.orDie);
  yield* store
    .append({
      path: PATH,
      producerId: claim.producerId,
      epoch: claim.epoch,
      sequence: claim.nextSequence,
      records: records.map((record) => ({
        conversationId: CONVERSATION,
        timestamp: 1_700_000_000_000,
        record,
      })),
    })
    .pipe(Effect.orDie);
});

const readAll = Effect.fn('test.readAll')(function* () {
  const store = yield* LogStore.Service;
  const page = yield* store.read(PATH, { limit: 1000 }).pipe(Effect.orDie);
  return page.records;
});

const outcomesOf = (records: ReadonlyArray<ConversationRecord.Envelope>) =>
  records.flatMap((envelope) =>
    envelope.record._tag === 'ToolOutcome' ? [envelope.record] : [],
  );

const crashed: ReadonlyArray<ConversationRecord.Record> = [
  {
    _tag: 'RunStarted',
    agent: 'test',
    formatVersion: 1,
    agentRevision: LogVocabulary.AgentRevision.make('1'),
    prompt: [],
  },
  {
    _tag: 'ToolCall',
    step: 1,
    id: CALL_ID,
    name: 'lookup',
    params: { id: '42' },
  },
  {
    _tag: 'ToolOutcome',
    step: 1,
    id: CALL_ID,
    name: 'lookup',
    outcome: 'success',
    result: { status: 'from-log' },
  },
];

describe('when the log and an interceptor both have an opinion', () => {
  it.effect(
    'serves the recovered outcome and does not consult the interceptor',
    () =>
      Effect.gen(function* () {
        const consulted: string[] = [];
        const ran = { count: 0 };

        const written = yield* run(
          Effect.gen(function* () {
            yield* seed(crashed);
            yield* Conversation.make(
              agentWith(ran).intercepting({
                beforeToolCall: (context) =>
                  Effect.sync(() => {
                    consulted.push(context.name);
                    return Interception.refuse('denied');
                  }),
              }),
              CONVERSATION,
            )
              .run('hi')
              .pipe(Effect.orDie);
            return yield* readAll();
          }),
        );

        // The call already ran, in the run that crashed. Refusing it now would
        // show the model a refusal for work that actually happened.
        expect(consulted).toEqual([]);
        expect(ran.count).toBe(0);
        expect(outcomesOf(written).at(-1)?.result).toEqual({
          status: 'from-log',
        });
        expect(
          written.some(({ record }) => record._tag === 'ToolStarted'),
        ).toBe(false);
      }),
  );

  it.effect('consults the interceptor once the earlier run has settled', () =>
    Effect.gen(function* () {
      const consulted: string[] = [];
      const ran = { count: 0 };

      yield* run(
        Effect.gen(function* () {
          yield* seed([
            ...crashed,
            {
              _tag: 'RunSettled',
              outcome: 'success',
              detail: '',
              steps: 2,
              usage: { input: 0, output: 0 },
            },
          ]);
          yield* Conversation.make(
            agentWith(ran).intercepting({
              beforeToolCall: (context) =>
                Effect.sync(() => {
                  consulted.push(context.name);
                  return Interception.dispatch;
                }),
            }),
            CONVERSATION,
          )
            .run('hi')
            .pipe(Effect.orDie);
        }),
      );

      // A settled conversation has an empty recovery index, so the ordering
      // above never comes up and the seam is back in charge.
      expect(consulted).toEqual(['lookup']);
      expect(ran.count).toBe(1);
    }),
  );

  it.effect('tells the interceptor which conversation it is in', () =>
    Effect.gen(function* () {
      const seen: Array<string | undefined> = [];
      const ran = { count: 0 };

      yield* run(
        Effect.gen(function* () {
          yield* Conversation.make(
            agentWith(ran).intercepting({
              beforeToolCall: (context) =>
                Effect.sync(() => {
                  seen.push(context.conversationId);
                  return Interception.dispatch;
                }),
              beforeTurn: (context) =>
                Effect.sync(() => {
                  seen.push(context.conversationId);
                  return Interception.proceed;
                }),
            }),
            CONVERSATION,
          )
            .run('hi')
            .pipe(Effect.orDie);
        }),
      );

      expect(new Set(seen)).toEqual(new Set([CONVERSATION]));
    }),
  );

  it.effect('says so when the run is not recording', () =>
    Effect.gen(function* () {
      const seen: Array<string | undefined> = [];
      const ran = { count: 0 };

      yield* run(
        agentWith(ran)
          .intercepting({
            beforeTurn: (context) =>
              Effect.sync(() => {
                seen.push(context.conversationId);
                return Interception.proceed;
              }),
          })
          .run('hi'),
      );

      expect(seen).toEqual([undefined, undefined]);
    }),
  );
});

// ------------------------------------------------------------- opting out

describe('an agent that is not intercepted', () => {
  it.effect('emits exactly what it emitted before', () =>
    Effect.gen(function* () {
      const ran = { count: 0 };
      const tags: string[] = [];

      yield* agentWith(ran)
        .stream('hi')
        .pipe(
          Stream.runForEach((event) =>
            Effect.sync(() => {
              tags.push(AgentEvents.isPart(event) ? 'Part' : event._tag);
            }),
          ),
          Effect.orDie,
          Effect.provide(scripted([callingTurn, answeringTurn])),
          Effect.scoped,
        );

      expect(tags.filter((tag) => tag !== 'Part')).toEqual([
        'TurnStarted',
        'TurnFinished',
        'TurnStarted',
        'TurnFinished',
        'Completed',
      ]);
      expect(ran.count).toBe(1);
    }),
  );

  it.effect('an interceptor with no seams changes nothing', () =>
    Effect.gen(function* () {
      const ran = { count: 0 };

      const result = yield* run(agentWith(ran).intercepting({}).run('hi'));

      expect(result.text).toBe('done');
      expect(ran.count).toBe(1);
    }),
  );
});

// ------------------------------------------------------- the requirement type

class Policy extends Context.Service<Policy, { readonly allow: boolean }>()(
  'interception-test/Policy',
) {}
class Reconciler extends Context.Service<
  Reconciler,
  { readonly retry: boolean }
>()('interception-test/Reconciler') {}

const policed = agentWith({ count: 0 }).intercepting({
  beforeToolCall: () =>
    Effect.gen(function* () {
      const policy = yield* Policy;
      return policy.allow ? Interception.dispatch : Interception.refuse('no');
    }),
});
const recoverable = agentWith({ count: 0 }).intercepting({
  onIndeterminateToolCall: () =>
    Effect.gen(function* () {
      const policy = yield* Reconciler;
      return policy.retry
        ? Interception.retry
        : Interception.reconcileFailure('not committed');
    }),
});

type EffR<T> = T extends Effect.Effect<infer _A, infer _E, infer R> ? R : never;
type Has<M, U> = [M] extends [U] ? 'yes' : 'no';
type IsAny<T> = 0 extends 1 & T ? 'ANY' : 'not-any';

// Guard first, exactly as `assertions.test.ts` does: `[M] extends [any]` holds
// for every M, so the membership assertion below would pass vacuously against
// an `any` channel.
const _guard: IsAny<EffR<ReturnType<typeof policed.run>>> = 'not-any';

// The assertion `intercepting`'s one cast exists to justify. Erasing the
// interceptor's `R` internally is only sound because it reappears here; a
// version that erased it in both places would compile and would fail at run
// time with a missing service. Read as `Has<members, channel>`, matching
// `assertions.test.ts`.
const _widened: Has<Policy, EffR<ReturnType<typeof policed.run>>> = 'yes';

// And it is additive: what the agent already required is still required.
const _kept: Has<
  LanguageModel.LanguageModel,
  EffR<ReturnType<typeof policed.run>>
> = 'yes';

// The other half, without which the first is nearly free — an agent that was
// never intercepted does not inherit the interceptor's services from anywhere.
const _untaxed: Has<
  Policy,
  EffR<ReturnType<ReturnType<typeof agentWith>['run']>>
> = 'no';
const _recoveryWidened: Has<
  Reconciler,
  EffR<ReturnType<typeof recoverable.run>>
> = 'yes';

describe('the requirement channel', () => {
  it.effect(
    'names the interceptor’s services, and only the intercepted agent’s',
    () =>
      Effect.sync(() => {
        expect([_guard, _widened, _kept, _untaxed, _recoveryWidened]).toEqual([
          'not-any',
          'yes',
          'yes',
          'no',
          'yes',
        ]);
      }),
  );
});
