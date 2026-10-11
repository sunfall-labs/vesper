import { LogStore } from '@sunfall/vesper-log/log-store';
import * as NodeServices from '@effect/platform-node/NodeServices';
import {
  Crypto,
  Deferred,
  Effect,
  Fiber,
  Layer,
  Ref,
  Schema,
  Stream,
} from 'effect';
import { LanguageModel, type Response, Tool, Toolkit } from 'effect/ai';
import { Queue } from 'effect';
import { describe, expect, it } from '@effect/vitest';

import { Agent } from '../src/agent.js';
import { Conversation, type Signal } from '../src/conversation.js';
import { AgentEvents } from '../src/event.js';
import { Stop } from '../src/stop.js';
import { ScriptedModel } from '../src/testing.js';
import { TurnControl } from '../src/turn-control.js';
import {
  callTool,
  finish,
  model,
  type Model,
  received,
  required,
  says,
  scriptedLayer,
  settlement,
  tagsOf,
  testLogLayer,
} from './signals-fixtures.js';

// Signals (steer / cancel), stop conditions, and next-turn control: how a run
// is redirected or ended. Everything drives the public API against a scripted
// model and the in-memory log.

const run = <A, E>(
  effect: Effect.Effect<
    A,
    E,
    LogStore.Service | LanguageModel.LanguageModel | Crypto.Crypto
  >,
  scripted: Model,
) =>
  effect.pipe(
    Effect.orDie,
    Effect.provide(scriptedLayer(scripted)),
    Effect.scoped,
  );

const CONVERSATION = 'signalled-conversation';

const agent = Agent.make({
  name: 'test',
  revision: '1',
  instructions: 'be terse',
  toolkit: Toolkit.make(),
});
const conversation = Conversation.make(agent, CONVERSATION);

const steer = (text: string, requestId?: string): Signal => ({
  kind: 'steer',
  text,
  source: 'operator',
  ...(requestId === undefined ? {} : { requestId }),
});
const cancel = (text = 'stop'): Signal => ({
  kind: 'cancel',
  text,
  source: 'ui',
});

const readAll = (target: { readonly records: typeof conversation.records }) =>
  target.records().pipe(
    Stream.runCollect,
    Effect.map((records) => Array.from(records)),
    Effect.orDie,
  );

/** Signals a conversation's run actually consumed, after sending `sends`. */
const deliveredAfter = (id: string, sends: ReadonlyArray<Signal>) =>
  run(
    Effect.gen(function* () {
      const bound = Conversation.make(agent, id);
      for (const signal of sends) {
        yield* bound.send(signal);
      }
      yield* bound.run('hi');
      return received(yield* readAll(bound)).map((r) => r.text);
    }),
    model(),
  );

const fixedAgent = (name: string, runPolicy: object) =>
  Agent.make({
    name,
    revision: '1',
    instructions: 'answer',
    toolkit: Toolkit.make(),
    runPolicy,
  });

const occurrences = (haystack: string, needle: string): number =>
  haystack.split(needle).length - 1;

const work = Tool.make('work', {
  description: 'do some work',
  parameters: Schema.Struct({}),
  success: Schema.Struct({ ok: Schema.Boolean }),
  dependencies: [LogStore.Service, Crypto.Crypto],
});

/** A model that calls `work` for the first `toolTurns` turns, then talks. */
const workThenTalk = (toolTurns: number): Model =>
  model((index) =>
    index < toolTurns
      ? [callTool(`call-${String(index + 1)}`, 'work'), finish('tool-calls')]
      : says(`turn ${String(index + 1)}`),
  );

/** A tool that sends signals to its own conversation, keyed by tool call. */
const signallingAgent = (sendOn: ReadonlyMap<number, Signal>) => {
  const turns = { count: 0 };
  return Agent.make({
    name: 'test',
    revision: '1',
    instructions: 'be terse',
    toolkit: Toolkit.make(work),
    stopWhen: Stop.any(Stop.noToolCalls(), Stop.maxSteps(8)),
  }).withHandlers({
    work: () =>
      Effect.gen(function* () {
        turns.count += 1;
        const signal = sendOn.get(turns.count);
        if (signal !== undefined) {
          yield* Conversation.make(agent, CONVERSATION)
            .send(signal)
            .pipe(Effect.orDie);
        }
        return { ok: true };
      }),
  });
};

const turn = (
  ...calls: ReadonlyArray<readonly [string, unknown?]>
): ReadonlyArray<Response.StreamPartEncoded> => [
  ...calls.map(([name, params], i) =>
    callTool(`${name}-${String(i)}`, name, params ?? {}),
  ),
  finish('tool-calls'),
];
const usage = (output: number) => ({
  ...finish('tool-calls'),
  usage: {
    inputTokens: {
      total: 10_000,
      uncached: 10_000,
      cacheRead: 0,
      cacheWrite: 0,
    },
    outputTokens: { total: output },
  },
});

const textTurn = (id: string, text: string) => [
  { type: 'text-start' as const, id },
  { type: 'text-delta' as const, id, delta: text },
  { type: 'text-end' as const, id },
  finish(),
];
const lastUserText = (request: ScriptedModel.Request) => {
  const message = request.prompt.content.at(-1);
  return message?.role === 'user'
    ? message.content
        .flatMap((part) => (part.type === 'text' ? [part.text] : []))
        .join('')
    : undefined;
};

describe('steering', () => {
  it.effect('retries with one requestId deliver once, across bindings', () =>
    Effect.gen(function* () {
      const signal = steer('once', 'req-1');
      const delivered = yield* run(
        Effect.gen(function* () {
          const bound = Conversation.make(agent, 'keyed');
          yield* bound.send(signal);
          yield* bound.send(signal);
          // A fresh binding stands in for a restarted process.
          yield* Conversation.make(agent, 'keyed').send(signal);
          yield* Effect.all(
            Array.from({ length: 8 }, () =>
              Conversation.make(agent, 'keyed').send(signal),
            ),
            { concurrency: 'unbounded' },
          );
          yield* bound.run('hi');
          return received(yield* readAll(bound));
        }),
        model(),
      );
      expect(delivered).toHaveLength(1);
    }),
  );

  it.effect('a requestId with a different payload is a typed conflict', () =>
    Effect.gen(function* () {
      const [error, delivered] = yield* run(
        Effect.gen(function* () {
          const bound = Conversation.make(agent, 'conflict');
          yield* bound.send(steer('a', 'req-1'));
          const failure = yield* bound
            .send(steer('b', 'req-1'))
            .pipe(Effect.flip);
          yield* bound.run('hi');
          return [failure, received(yield* readAll(bound))] as const;
        }),
        model(),
      );
      expect(error._tag).toBe('SignalRequestConflictError');
      expect(delivered.map((r) => r.text)).toEqual(['a']);
    }),
  );

  it.effect('scopes keys to their conversation; unkeyed sends all land', () =>
    Effect.gen(function* () {
      expect(yield* deliveredAfter('scope-a', [steer('x', 'shared')])).toEqual([
        'x',
      ]);
      expect(yield* deliveredAfter('scope-b', [steer('x', 'shared')])).toEqual([
        'x',
      ]);
      expect(
        yield* deliveredAfter('unkeyed', [steer('x'), steer('x')]),
      ).toEqual(['x', 'x']);
    }),
  );

  it.effect('rejects oversized and malformed signals with typed errors', () =>
    Effect.gen(function* () {
      const bound = Conversation.make(agent, 'invalid');
      const oversized = yield* bound
        .send(steer('x'.repeat(256 * 1024 + 1)))
        .pipe(Effect.provide(testLogLayer), Effect.flip);
      expect(oversized).toMatchObject({ reason: 'encoding' });
      expect(Reflect.get(oversized, 'detail')).toContain('maximum is');

      // @ts-expect-error typed callers must supply the signal text
      const invalid = bound.send({ kind: 'steer', source: 'operator' });
      const malformed = yield* invalid.pipe(
        Effect.provide(testLogLayer),
        Effect.flip,
      );
      expect(malformed).toMatchObject({ reason: 'encoding' });
      expect(Reflect.get(malformed, 'detail')).toContain('signal must contain');
    }),
  );

  it.effect(
    'records and emits a policy-oversized steer without injecting it',
    () =>
      Effect.gen(function* () {
        const scripted = model();
        const bounded = Conversation.make(
          fixedAgent('bounded-signals', { maxSignalBytes: 4 }),
          CONVERSATION,
        );
        const result = yield* run(
          Effect.gen(function* () {
            yield* bounded.send(steer('too large'));
            const events = yield* bounded.stream('hi').pipe(Stream.runCollect);
            return { events, records: yield* readAll(bounded) };
          }),
          scripted,
        );
        expect(result.events).toContainEqual(
          expect.objectContaining({
            _tag: 'SignalRejected',
            reason: 'signal_bytes',
          }),
        );
        expect(scripted.prompts).toHaveLength(1);
        expect(scripted.prompts[0]).not.toContain('too large');
        expect(received(result.records)).toContainEqual(
          expect.objectContaining({ disposition: 'rejected' }),
        );
      }),
  );

  it.effect(
    'announces a bounded-drain backlog and delivers it next boundary',
    () =>
      Effect.gen(function* () {
        const scripted = model();
        const bounded = Conversation.make(
          fixedAgent('bounded-backlog', { maxSignalsPerBoundary: 1 }),
          CONVERSATION,
        );
        const events = yield* run(
          Effect.gen(function* () {
            yield* bounded.send(steer('first'));
            yield* bounded.send(steer('second'));
            return yield* bounded.stream('hi').pipe(Stream.runCollect);
          }),
          scripted,
        );
        expect(events).toContainEqual(
          expect.objectContaining({ _tag: 'SignalBacklog', maximum: 1 }),
        );
        expect(scripted.prompts.some((p) => p.includes('second'))).toBe(true);
      }),
  );

  it.effect('reaches the model, overrides stop, and is recorded in place', () =>
    Effect.gen(function* () {
      const scripted = model();
      const { result, written, observed } = yield* run(
        Effect.gen(function* () {
          yield* conversation.send(steer('also check the invoice'));
          const signalled = yield* conversation.stream('hi').pipe(
            Stream.filter((event) => event._tag === 'Signalled'),
            Stream.runCollect,
          );
          const later = yield* conversation.run('again');
          return {
            result: later,
            written: yield* readAll(conversation),
            observed: signalled,
          };
        }),
        scripted,
      );

      // Every turn this model produces satisfies the default stop condition:
      // run 1's second turn exists only because the steer overrode it, and
      // the steer is delivered once, not to the later run.
      expect(scripted.prompts).toHaveLength(3);
      expect(scripted.prompts[1]).toContain('also check the invoice');
      expect(result.text).toBe('turn 3');
      expect(observed).toMatchObject([
        { _tag: 'Signalled', step: 1, kind: 'steer' },
      ]);
      expect(received(written)).toMatchObject([
        {
          _tag: 'SignalReceived',
          kind: 'steer',
          text: 'also check the invoice',
          source: 'operator',
          step: 1,
        },
      ]);
      // After the text the model had already produced, before the boundary.
      expect(tagsOf(written).slice(0, 8)).toEqual([
        'RunStarted',
        'Text',
        'SignalReceived',
        'TurnFinished',
        'Text',
        'TurnFinished',
        'Completed',
        'RunSettled',
      ]);
    }),
  );
});

describe('cancelling', () => {
  // This agent is told to keep going for five steps, so anything that ends it
  // sooner ended it because it was told to.
  const persistent = Agent.make({
    name: 'persistent',
    revision: '1',
    instructions: 'keep going',
    toolkit: Toolkit.make(),
    stopWhen: Stop.maxSteps(5),
  });

  it.effect(
    'ends a durable-cancelled run before the model, and outranks steers',
    () =>
      Effect.gen(function* () {
        const scripted = model();
        const { control, cancelled, mixed, written } = yield* run(
          Effect.gen(function* () {
            const uncancelled = yield* Conversation.make(
              persistent,
              'control',
            ).run('hi');
            const bound = Conversation.make(persistent, CONVERSATION);
            yield* bound.send(cancel('user closed the tab'));
            const ended = yield* bound.run('hi');
            const records = yield* readAll(bound);
            const other = Conversation.make(persistent, 'mixed');
            yield* other.send(steer('keep going'));
            yield* other.send(cancel('never mind'));
            return {
              control: uncancelled,
              cancelled: ended,
              mixed: yield* other.run('hi'),
              written: records,
            };
          }),
          scripted,
        );
        // The control proves the stop condition alone would have run 5 steps.
        expect(control.steps).toBe(5);
        expect(scripted.prompts).toHaveLength(5);
        expect(cancelled).toMatchObject({
          text: '',
          steps: 0,
          outcome: 'cancelled',
        });
        expect(mixed).toMatchObject({ steps: 0, outcome: 'cancelled' });
        expect(settlement(written)).toMatchObject([{ outcome: 'cancelled' }]);
      }),
  );

  it.live(
    'interrupts an in-flight provider stream and keeps partial text',
    () =>
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>();
        const stopped = yield* Deferred.make<void>();
        const blocking = Layer.succeed(
          LanguageModel.LanguageModel,
          yield* LanguageModel.make({
            generateText: () =>
              Effect.succeed<Response.PartEncoded[]>([finish()]),
            streamText: () =>
              Stream.concat(
                Stream.fromIterable<Response.StreamPartEncoded>([
                  { type: 'text-start', id: 'partial' },
                  {
                    type: 'text-delta',
                    id: 'partial',
                    delta: 'partial answer',
                  },
                ]),
                Stream.concat(
                  Stream.fromEffect(Deferred.succeed(entered, undefined)).pipe(
                    Stream.drain,
                  ),
                  Stream.never,
                ),
              ).pipe(Stream.ensuring(Deferred.succeed(stopped, undefined))),
          }),
        );
        const result = yield* Effect.gen(function* () {
          const running = yield* Effect.forkChild(
            conversation.run('hi').pipe(Effect.provide(blocking)),
          );
          yield* Deferred.await(entered);
          yield* conversation
            .send(steer('change direction'))
            .pipe(Effect.orDie);
          yield* conversation.send(cancel('stop now')).pipe(Effect.orDie);
          const completed = yield* Fiber.join(running);
          yield* Deferred.await(stopped);
          return { completed, records: yield* readAll(conversation) };
        }).pipe(Effect.provide(testLogLayer));

        expect(result.completed).toMatchObject({
          text: 'partial answer',
          steps: 1,
          outcome: 'cancelled',
        });
        expect(tagsOf(result.records)).toEqual([
          'RunStarted',
          'Text',
          'SignalReceived',
          'SignalReceived',
          'TurnFinished',
          'Completed',
          'RunSettled',
        ]);
        expect(received(result.records).map((r) => r.kind)).toEqual([
          'steer',
          'cancel',
        ]);
      }),
    2_000,
  );

  it.live(
    'does not let an oversized or backlogged cancel preempt a blocked stream',
    () =>
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>();
        const blockedLayer = Layer.succeed(
          LanguageModel.LanguageModel,
          yield* LanguageModel.make({
            generateText: () =>
              Effect.succeed<Response.PartEncoded[]>([finish()]),
            streamText: () =>
              Stream.unwrap(
                Deferred.succeed(entered, undefined).pipe(
                  Effect.as(Stream.never),
                ),
              ),
          }),
        );
        const blocked: Model = { prompts: [], layer: blockedLayer };

        const oversized = Conversation.make(
          fixedAgent('bounded-cancel', {
            maxSignalBytes: 4,
            wallClockMillis: 100,
          }),
          CONVERSATION,
        );
        const outcome = yield* run(
          Effect.gen(function* () {
            const running = yield* Effect.forkChild(oversized.run('hi'));
            yield* Deferred.await(entered);
            yield* oversized.send(cancel('too large')).pipe(Effect.orDie);
            return yield* Fiber.join(running);
          }).pipe(Effect.result),
          blocked,
        );
        expect(String(outcome)).toContain('deadline');

        const backlogged = Conversation.make(
          fixedAgent('backlogged-cancel', {
            maxSignalsPerBoundary: 1,
            wallClockMillis: 100,
          }),
          CONVERSATION,
        );
        const second = yield* run(
          Effect.gen(function* () {
            yield* backlogged.send(steer('older'));
            yield* backlogged.send(cancel('newer'));
            return yield* backlogged.run('hi');
          }).pipe(Effect.result),
          blocked,
        );
        expect(String(second)).toContain('deadline');
      }),
  );

  it.effect(
    'falls back to boundary cancellation when the change feed fails',
    () =>
      Effect.gen(function* () {
        const scripted = model();
        const failingFeed = Layer.effect(
          LogStore.Service,
          Effect.map(LogStore.Service, (store) =>
            LogStore.Service.of({
              ...store,
              changes: (path) =>
                Stream.fail(
                  LogStore.makeError(path, 'changes', 'storage', 'injected'),
                ),
            }),
          ),
        ).pipe(Layer.provide(testLogLayer));

        const result = yield* Effect.gen(function* () {
          yield* conversation
            .send(cancel('stop at boundary'))
            .pipe(Effect.orDie);
          return yield* conversation.run('hi');
        }).pipe(
          Effect.provide(
            Layer.merge(
              scripted.layer,
              Layer.merge(failingFeed, NodeServices.layer),
            ),
          ),
        );
        expect(result.text).toBe('turn 1');
        expect(result.outcome).toBe('cancelled');
        expect(scripted.prompts).toHaveLength(1);
      }),
  );

  it.live(
    'survives a watcher read failure and tears the watcher down',
    () =>
      Effect.gen(function* () {
        const readFailed = yield* Deferred.make<void>();
        const stopped = yield* Deferred.make<void>();
        let watched: string | undefined;
        let failNextRead = true;
        const instrumented = Layer.mergeAll(
          Layer.effect(
            LogStore.Service,
            Effect.map(LogStore.Service, (store) =>
              LogStore.Service.of({
                ...store,
                read: (path, options) =>
                  path === watched && failNextRead
                    ? Effect.sync(() => {
                        failNextRead = false;
                      }).pipe(
                        Effect.andThen(Deferred.succeed(readFailed, undefined)),
                        Effect.andThen(
                          Effect.fail(
                            LogStore.makeError(
                              path,
                              'read',
                              'storage',
                              'injected watcher read failure',
                            ),
                          ),
                        ),
                      )
                    : store.read(path, options),
                changes: (path) => {
                  watched = path;
                  return store
                    .changes(path)
                    .pipe(
                      Stream.ensuring(Deferred.succeed(stopped, undefined)),
                    );
                },
              }),
            ),
          ).pipe(Layer.provide(testLogLayer)),
          NodeServices.layer,
        );
        const provider = Layer.effect(
          LanguageModel.LanguageModel,
          LanguageModel.make({
            generateText: () =>
              Effect.succeed<Response.PartEncoded[]>([finish()]),
            streamText: () =>
              Stream.unwrap(
                Effect.timeoutOption(
                  Deferred.await(readFailed),
                  '200 millis',
                ).pipe(Effect.as(Stream.fromIterable(says('after watcher')))),
              ),
          }),
        );
        const result = yield* conversation
          .run('hi')
          .pipe(Effect.provide(Layer.merge(provider, instrumented)));
        // The watcher is scoped to the provider stream: it is gone once the run is.
        yield* Deferred.await(stopped);
        expect(result.text).toBe('after watcher');
      }),
    2_000,
  );
});

// The production case is a signal sent while the run is mid-tool-call, which
// pre-seeded signals cannot reach: the first drain meets a signal stream that
// does not exist yet, drains happen at every boundary, and the cursor advances
// within a run.
describe('a signal that arrives while the run is in flight', () => {
  it.effect('steers land at the next boundary, once, across runs', () =>
    Effect.gen(function* () {
      const scripted = workThenTalk(3);
      const written = yield* run(
        Effect.gen(function* () {
          const bound = Conversation.make(
            signallingAgent(
              new Map([
                [1, steer('first instruction')],
                [3, steer('second instruction')],
              ]),
            ),
            CONVERSATION,
          );
          yield* bound.run('hi');
          yield* bound.run('again');
          return yield* readAll(bound);
        }),
        scripted,
      );

      expect(received(written).map((r) => r.text)).toEqual([
        'first instruction',
        'second instruction',
      ]);
      // Four turns: tool x3, answer; each steer reaches the turn after it was
      // sent and appears exactly once (a stuck cursor would duplicate it).
      const [p0, p1, p2, p3] = scripted.prompts.map((p) => p);
      expect(occurrences(required(p0), 'first instruction')).toBe(0);
      expect(occurrences(required(p1), 'first instruction')).toBe(1);
      expect(occurrences(required(p2), 'first instruction')).toBe(1);
      expect(occurrences(required(p3), 'first instruction')).toBe(1);
      expect(occurrences(required(p2), 'second instruction')).toBe(0);
      expect(occurrences(required(p3), 'second instruction')).toBe(1);
    }),
  );

  it.effect('a mid-run cancel ends the run; no signal runs to completion', () =>
    Effect.gen(function* () {
      const cancelled = workThenTalk(4);
      const result = yield* run(
        Conversation.make(
          signallingAgent(new Map([[1, cancel('user closed the tab')]])),
          CONVERSATION,
        ).run('hi'),
        cancelled,
      );
      // The model asks for a tool on four turns, so only the cancel ends it.
      expect(result.steps).toBe(1);
      expect(cancelled.prompts).toHaveLength(1);

      const control = yield* run(
        Conversation.make(signallingAgent(new Map()), CONVERSATION).run('hi'),
        workThenTalk(4),
      );
      expect(control.steps).toBe(5);
    }),
  );

  it.live(
    'a cancel preempts a stalled provider without starting its tool call',
    () =>
      Effect.gen(function* () {
        const ran = { count: 0 };
        const result = yield* Effect.gen(function* () {
          const entered = yield* Deferred.make<void>();
          const stalled = Layer.succeed(
            LanguageModel.LanguageModel,
            yield* LanguageModel.make({
              generateText: () =>
                Effect.succeed<Response.PartEncoded[]>([finish()]),
              streamText: () =>
                Stream.concat(
                  Stream.make(callTool('call-1', 'work')),
                  Stream.concat(
                    Stream.fromEffect(
                      Deferred.succeed(entered, undefined),
                    ).pipe(Stream.drain),
                    Stream.never,
                  ),
                ),
            }),
          );
          const counting = Conversation.make(
            Agent.make({
              name: 'test',
              revision: '1',
              instructions: 'be terse',
              toolkit: Toolkit.make(work),
            }).withHandlers({
              work: () =>
                Effect.sync(() => {
                  ran.count += 1;
                  return { ok: true };
                }),
            }),
            CONVERSATION,
          );
          const running = yield* Effect.forkChild(
            counting.run('hi').pipe(Effect.provide(stalled)),
          );
          yield* Deferred.await(entered);
          yield* counting.send(cancel('stop now')).pipe(Effect.orDie);
          return {
            completed: yield* Fiber.join(running),
            records: yield* readAll(counting),
          };
        }).pipe(Effect.provide(testLogLayer));

        expect(result.completed.outcome).toBe('cancelled');
        expect(ran.count).toBe(0);
        const tags = tagsOf(result.records);
        expect(tags).not.toContain('ToolStarted');
        expect(tags).not.toContain('ToolOutcome');
        expect(settlement(result.records)).toMatchObject([
          { outcome: 'cancelled' },
        ]);
      }),
  );
});

describe('an agent that is not recording', () => {
  it.effect('is unaffected by a signal addressed to a conversation', () =>
    Effect.gen(function* () {
      const scripted = model();
      const result = yield* run(
        Effect.gen(function* () {
          yield* conversation.send(cancel('never mind'));
          return yield* agent.run('hi');
        }),
        scripted,
      );
      expect(result.text).toBe('turn 1');
      expect(scripted.prompts).toHaveLength(1);
    }),
  );
});

// Stop conditions decide when an agent stops calling the model, i.e. spending.
describe('stop conditions', () => {
  const lookup = Tool.make('lookup', {
    description: 'Look something up.',
    parameters: Schema.Struct({}),
    success: Schema.Struct({ ok: Schema.Boolean }),
  });
  const search = Tool.make('search', {
    description: 'Search for something.',
    parameters: Schema.Struct({ query: Schema.String }),
    success: Schema.Struct({ result: Schema.String }),
  });
  const issueRefund = Tool.make('issue_refund', {
    description: 'Refund.',
    parameters: Schema.Struct({}),
    success: Schema.Struct({ ok: Schema.Boolean }),
  });
  const submit = Tool.make('submit', {
    description: 'Submit the final structured answer.',
    parameters: Schema.Struct({ value: Schema.Finite }),
    success: Schema.Struct({ accepted: Schema.Boolean }),
    failure: Schema.Struct({ reason: Schema.String }),
    failureMode: 'return',
  });
  const toolkit = Toolkit.make(lookup, search, issueRefund, submit);
  const probe = Agent.make({
    name: 'stopper',
    revision: '1',
    instructions: 'x',
    toolkit,
  });
  type StopperCondition = Stop.StopCondition<Agent.Tools<typeof probe>>;
  const stopper = (stopWhen?: StopperCondition, firstFails = false) => {
    const attempts = { count: 0 };
    return Agent.make({
      name: 'stopper',
      revision: '1',
      instructions: 'x',
      toolkit,
      ...(stopWhen === undefined ? {} : { stopWhen }),
    }).withHandlers({
      lookup: () => Effect.succeed({ ok: true }),
      issue_refund: () => Effect.succeed({ ok: true }),
      search: ({ query }) => Effect.succeed({ result: `found ${query}` }),
      submit: () =>
        firstFails && attempts.count++ === 0
          ? Effect.fail({ reason: 'try again' })
          : Effect.succeed({ accepted: true }),
    });
  };

  const stepsWith = (
    stopWhen: StopperCondition | undefined,
    script: ReadonlyArray<ReadonlyArray<Response.StreamPartEncoded>>,
    options?: { readonly repeatLast?: boolean; readonly firstFails?: boolean },
  ) =>
    Effect.gen(function* () {
      const scripted = ScriptedModel.make(script, {
        repeatLast: options?.repeatLast ?? false,
      });
      const result = yield* stopper(stopWhen, options?.firstFails)
        .run('go')
        .pipe(Effect.provide(scripted.layer));
      return { result, requests: yield* scripted.requests };
    });

  it.effect('default: stops on no tool calls, else at 32 steps', () =>
    Effect.gen(function* () {
      const quiet = yield* stepsWith(undefined, [
        turn(['lookup']),
        [...says('done')],
      ]);
      expect(quiet.result.steps).toBe(2);
      const busy = yield* stepsWith(undefined, [turn(['lookup'])], {
        repeatLast: true,
      });
      expect(busy.result.steps).toBe(32);
    }),
  );

  it.effect('maxSteps is a ceiling; maxOutputTokens counts output only', () =>
    Effect.gen(function* () {
      const steps = yield* stepsWith(Stop.maxSteps(3), [turn(['lookup'])], {
        repeatLast: true,
      });
      expect(steps.result.steps).toBe(3);
      const tokens = yield* stepsWith(
        Stop.maxOutputTokens(50),
        [[callTool('c', 'lookup'), usage(30)]],
        { repeatLast: true },
      );
      // 10k input per turn never counts; 30 + 30 output crosses 50.
      expect(tokens.result.steps).toBe(2);
    }),
  );

  it.effect('terminal-tool conditions stop on call, success, or failure', () =>
    Effect.gen(function* () {
      const called = yield* stepsWith(Stop.toolCalled('issue_refund'), [
        turn(['lookup']),
        turn(['lookup'], ['issue_refund']),
        turn(['lookup']),
      ]);
      expect(called.result.steps).toBe(2);

      const succeeded = yield* stepsWith(
        Stop.toolSucceeded('submit'),
        [turn(['submit', { value: 1 }]), turn(['submit', { value: 2 }])],
        { firstFails: true },
      );
      // The failed first attempt does not satisfy it; the second does.
      expect(succeeded.requests).toHaveLength(2);
      expect(succeeded.result.steps).toBe(2);
      expect(succeeded.result.response?.content[1]).toMatchObject({
        role: 'tool',
        content: [{ type: 'tool-result', name: 'submit', isFailure: false }],
      });

      const failed = yield* stepsWith(
        Stop.toolFailed('submit'),
        [turn(['lookup']), turn(['submit', { value: 1 }]), turn(['lookup'])],
        { firstFails: true },
      );
      expect(failed.result.steps).toBe(2);
    }),
  );

  it.effect('counts tool calls cumulatively across turns and composes', () =>
    Effect.gen(function* () {
      const script = [
        turn(['search', { query: 'a' }], ['search', { query: 'b' }]),
        turn(['search', { query: 'c' }]),
        turn(['issue_refund']),
        turn(['issue_refund']),
      ];
      const search3 = Stop.toolCalledTimes('search', 3);
      const cumulative = yield* stepsWith(search3, script);
      // Two calls in turn 1 and a third in turn 2: a per-turn count never fires.
      expect(cumulative.requests).toHaveLength(2);

      const anyOf = yield* stepsWith(
        Stop.any(search3, Stop.maxSteps(99)),
        script,
      );
      expect(anyOf.result.steps).toBe(2);

      const allOf = yield* stepsWith(
        Stop.all(search3, Stop.toolCalledTimes('issue_refund', 2)),
        script,
      );
      expect(allOf.result.steps).toBe(4);
    }),
  );

  it.effect('treats empty compositions honestly and short-circuits', () =>
    Effect.gen(function* () {
      // An empty `all` stops immediately.
      const empty = yield* stepsWith(Stop.all(), [turn(['lookup'])]);
      expect(empty.result.steps).toBe(1);
      // An empty `any` never stops: the script runs dry and the run fails.
      const never = yield* stepsWith(Stop.any(), [turn(['lookup'])]).pipe(
        Effect.exit,
      );
      expect(never._tag).toBe('Failure');

      const calls = yield* Ref.make(0);
      const counted =
        (verdict: boolean): StopperCondition =>
        () =>
          Ref.update(calls, (n) => n + 1).pipe(Effect.as(verdict));
      yield* stepsWith(Stop.any(Stop.maxSteps(1), counted(false)), [
        turn(['lookup']),
      ]);
      yield* stepsWith(Stop.all(Stop.maxSteps(99), counted(true)), [
        turn(['lookup']),
        [...says('done')],
      ]).pipe(Effect.exit);
      // `any` stopped at maxSteps(1); `all` hit a continue first: neither ran it.
      expect(yield* Ref.get(calls)).toBe(0);
    }),
  );

  it.effect('rejects non-finite usage and invalid lifecycle counters', () =>
    Effect.gen(function* () {
      const usageResult = yield* Schema.decodeEffect(Stop.Usage)({
        input: Number.NaN,
        output: Number.POSITIVE_INFINITY,
      }).pipe(Effect.result);
      const lifecycle = yield* Schema.decodeEffect(AgentEvents.Lifecycle)({
        _tag: 'SignalBacklog',
        step: -1,
        maximum: Number.POSITIVE_INFINITY,
      }).pipe(Effect.result);
      expect([usageResult._tag, lifecycle._tag]).toEqual([
        'Failure',
        'Failure',
      ]);
    }),
  );
});

// Compile-time: tool names are checked against the toolkit.
const stopConditionToolNameAssertions = (): void => {
  const probe = Agent.make({
    name: 'probe',
    revision: '1',
    instructions: 'x',
    toolkit: Toolkit.make(
      Tool.make('search', {
        parameters: Schema.Struct({}),
        success: Schema.Struct({}),
      }),
    ),
  });
  const valid: Stop.StopCondition<Agent.Tools<typeof probe>> =
    Stop.toolCalledTimes('search', 3);
  void valid;
  const misspelled: Stop.StopCondition<Agent.Tools<typeof probe>> =
    // @ts-expect-error Tool names are checked against the toolkit.
    Stop.toolCalledTimes('serach', 3);
  void misspelled;
};
void stopConditionToolNameAssertions;

describe('next-turn control', () => {
  it.effect(
    'follows up on a different model, and returns canonical messages',
    () =>
      Effect.gen(function* () {
        const primary = ScriptedModel.make([
          [
            { type: 'reasoning-start', id: 'r' },
            { type: 'reasoning-delta', id: 'r', delta: 'considered' },
            { type: 'reasoning-end', id: 'r' },
            ...textTurn('one', 'first'),
          ],
        ]);
        const followUp = ScriptedModel.make([textTurn('two', 'second')]);
        const decisions = yield* Ref.make(0);
        const controlled = Agent.make({
          name: 'controlled',
          revision: '1',
          instructions: 'be terse',
          toolkit: Toolkit.make(),
          nextTurn: () =>
            Effect.gen(function* () {
              const count = yield* Ref.getAndUpdate(decisions, (n) => n + 1);
              if (count > 0) {
                return TurnControl.keep;
              }
              return TurnControl.continueWith('check your answer', {
                model: yield* followUp.service,
              });
            }),
        });

        const result = yield* controlled
          .run('start')
          .pipe(Effect.provide(primary.layer));
        expect(result).toMatchObject({ text: 'second', steps: 2 });
        expect(yield* primary.requests).toHaveLength(1);
        const followUps = yield* followUp.requests;
        expect(followUps).toHaveLength(1);
        expect(lastUserText(required(followUps[0]))).toBe('check your answer');

        const plain = Agent.make({
          name: 'response',
          revision: '1',
          instructions: 'be terse',
          toolkit: Toolkit.make(),
        });
        const canonical = yield* plain
          .run('start')
          .pipe(
            Effect.provide(
              ScriptedModel.make([
                [
                  { type: 'reasoning-start', id: 'r' },
                  { type: 'reasoning-delta', id: 'r', delta: 'considered' },
                  { type: 'reasoning-end', id: 'r' },
                  ...textTurn('t', 'answered'),
                ],
              ]).layer,
            ),
          );
        expect(canonical.response?.content[0]).toMatchObject({
          role: 'assistant',
          content: [
            { type: 'reasoning', text: 'considered' },
            { type: 'text', text: 'answered' },
          ],
        });
      }),
  );

  it.effect('drains follow-ups only when the agent would otherwise stop', () =>
    Effect.gen(function* () {
      const queue = yield* Queue.unbounded<string>();
      yield* Queue.offerAll(queue, ['first follow-up', 'second follow-up']);
      const service = ScriptedModel.make([
        textTurn('one', 'initial'),
        textTurn('two', 'after first'),
        textTurn('three', 'after second'),
      ]);
      const followed = Agent.make({
        name: 'follow-ups',
        revision: '1',
        instructions: 'be terse',
        toolkit: Toolkit.make(),
        nextTurn: TurnControl.followUps(queue),
      });
      const result = yield* followed
        .run('start')
        .pipe(Effect.provide(service.layer));
      expect(result.steps).toBe(3);
      expect((yield* service.requests).map(lastUserText)).toEqual([
        'start',
        'first follow-up',
        'second follow-up',
      ]);
    }),
  );
});
