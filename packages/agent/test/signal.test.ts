import { LogStoreMemory } from '@sunfall/vesper-log/layer-memory';
import { LogStore } from '@sunfall/vesper-log/log-store';
import type { ConversationRecord } from '@sunfall/vesper-log/record';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';
import { Deferred, Effect, Fiber, Layer, Ref, Schema, Stream } from 'effect';
import {
  LanguageModel,
  type Response,
  Tool,
  Toolkit,
} from 'effect/unstable/ai';
import { describe, expect, it } from '@effect/vitest';

import { Agent } from '../src/agent.js';
import { AgentLog } from '../src/log.js';
import { AgentSignals } from '../src/signal.js';
import { Stop } from '../src/stop.js';

// Signals: out-of-band input to a running conversation.
//
// What these have to prove:
//
//   - a steer reaches the model as input on the next turn, and is recorded as
//     delivered;
//   - a steer outranks the stop condition, because a run that consumed an
//     instruction and stopped anyway has silently ignored it;
//   - a cancel ends the run at the turn boundary and settles it as cancelled,
//     without another model call;
//   - a signal queued before the run began is still delivered;
//   - a signal already delivered is not delivered again to the next run,
//     which is the property the `SignalReceived` cursor exists for;
//   - an agent that is not recording is unaffected.

const finish = () => ({
  type: 'finish' as const,
  reason: 'stop' as const,
  usage: {
    inputTokens: { total: 5, uncached: 5, cacheRead: 0, cacheWrite: 0 },
    outputTokens: { total: 2 },
  },
});

const says = (text: string): Response.StreamPartEncoded[] => [
  { type: 'text-start' as const, id: text },
  { type: 'text-delta' as const, id: text, delta: text },
  { type: 'text-end' as const, id: text },
  finish(),
];

interface Model {
  readonly layer: Layer.Layer<LanguageModel.LanguageModel>;
  readonly prompts: string[];
}

/**
 * A model that always wants to stop, and remembers what it was asked.
 *
 * "Always wants to stop" is the point: every turn ends with no tool calls, so
 * the default stop condition fires at every boundary and a second turn can
 * only happen because a steer overrode it.
 */
const model = (): Model => {
  const prompts: string[] = [];
  return {
    prompts,
    layer: Layer.effect(
      LanguageModel.LanguageModel,
      Effect.gen(function* () {
        const calls = yield* Ref.make(0);
        return yield* LanguageModel.make({
          generateText: () =>
            Effect.succeed<Response.PartEncoded[]>([finish()]),
          streamText: (options) =>
            Stream.unwrap(
              Effect.gen(function* () {
                const index = yield* Ref.getAndUpdate(calls, (n) => n + 1);
                prompts.push(JSON.stringify(options.prompt));
                return Stream.fromIterable(says(`turn ${index + 1}`));
              }),
            ),
        });
      }),
    ),
  };
};

const CONVERSATION = 'signalled-conversation';

const agent = Agent.make({
  name: 'test',
  revision: '1',
  instructions: 'be terse',
  toolkit: Toolkit.make(),
});

const run = <A, E>(
  effect: Effect.Effect<A, E, LogStore.Service | LanguageModel.LanguageModel>,
  scripted: Model,
) =>
  effect.pipe(
    Effect.orDie,
    Effect.provide(scripted.layer),
    Effect.provide(LogStoreMemory.layer),
    Effect.scoped,
  );

const readAll = Effect.fn('test.readAll')(function* () {
  const store = yield* LogStore.Service;
  const page = yield* store
    .read(AgentLog.pathFor(LogVocabulary.ConversationId.make(CONVERSATION)), {
      limit: 1000,
    })
    .pipe(Effect.orDie);
  return page.records;
});

const received = (records: ReadonlyArray<ConversationRecord.Envelope>) =>
  records.flatMap((envelope) =>
    envelope.record._tag === 'SignalReceived' ? [envelope.record] : [],
  );

const settlement = (records: ReadonlyArray<ConversationRecord.Envelope>) =>
  records.flatMap((envelope) =>
    envelope.record._tag === 'RunSettled' ? [envelope.record] : [],
  );

describe('steering', () => {
  it.effect(
    'records and emits an oversized signal rejection without injecting it',
    () =>
      Effect.gen(function* () {
        const scripted = model();
        const bounded = Agent.make({
          name: 'bounded-signals',
          revision: '1',
          instructions: 'answer',
          toolkit: Toolkit.make(),
          runPolicy: { maxSignalBytes: 4 },
        });

        const result = yield* run(
          Effect.gen(function* () {
            yield* AgentSignals.send(CONVERSATION, {
              kind: 'steer',
              text: 'too large',
              source: 'operator',
            }).pipe(Effect.orDie);
            const events = yield* bounded
              .recordingTo(CONVERSATION)
              .stream('hi')
              .pipe(Stream.runCollect, Effect.orDie);
            return { events, records: yield* readAll() };
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
    'announces a bounded-drain backlog and leaves it for the next boundary',
    () =>
      Effect.gen(function* () {
        const scripted = model();
        const bounded = Agent.make({
          name: 'bounded-backlog',
          revision: '1',
          instructions: 'answer',
          toolkit: Toolkit.make(),
          runPolicy: { maxSignalsPerBoundary: 1 },
        });

        const events = yield* run(
          Effect.gen(function* () {
            for (const text of ['first', 'second']) {
              yield* AgentSignals.send(CONVERSATION, {
                kind: 'steer',
                text,
                source: 'operator',
              }).pipe(Effect.orDie);
            }
            return yield* bounded
              .recordingTo(CONVERSATION)
              .stream('hi')
              .pipe(Stream.runCollect, Effect.orDie);
          }),
          scripted,
        );

        expect(events).toContainEqual(
          expect.objectContaining({ _tag: 'SignalBacklog', maximum: 1 }),
        );
        expect(
          scripted.prompts.some((prompt) => prompt.includes('second')),
        ).toBe(true);
      }),
  );

  it.effect('reaches the model as input on the next turn', () =>
    Effect.gen(function* () {
      const scripted = model();

      yield* run(
        Effect.gen(function* () {
          yield* AgentSignals.send(CONVERSATION, {
            kind: 'steer',
            text: 'also check the invoice',
            source: 'operator',
          }).pipe(Effect.orDie);

          yield* agent.recordingTo(CONVERSATION).run('hi').pipe(Effect.orDie);
        }),
        scripted,
      );

      expect(scripted.prompts).toHaveLength(2);
      expect(scripted.prompts[1]).toContain('also check the invoice');
    }),
  );

  it.effect('overrides a stop condition that would have ended the run', () =>
    Effect.gen(function* () {
      const scripted = model();

      const result = yield* run(
        Effect.gen(function* () {
          yield* AgentSignals.send(CONVERSATION, {
            kind: 'steer',
            text: 'keep going',
            source: 'operator',
          }).pipe(Effect.orDie);

          return yield* agent
            .recordingTo(CONVERSATION)
            .run('hi')
            .pipe(Effect.orDie);
        }),
        scripted,
      );

      // Every turn this model produces satisfies the default stop condition.
      // Two steps happened, so the steer is what carried it past the first.
      expect(result.steps).toBe(2);
      expect(result.text).toBe('turn 2');
    }),
  );

  it.effect('records the delivery, with the offset it consumed', () =>
    Effect.gen(function* () {
      const scripted = model();

      const written = yield* run(
        Effect.gen(function* () {
          yield* AgentSignals.send(CONVERSATION, {
            kind: 'steer',
            text: 'keep going',
            source: 'operator',
          }).pipe(Effect.orDie);

          yield* agent.recordingTo(CONVERSATION).run('hi').pipe(Effect.orDie);
          return yield* readAll();
        }),
        scripted,
      );

      expect(received(written)).toEqual([
        {
          _tag: 'SignalReceived',
          kind: 'steer',
          text: 'keep going',
          source: 'operator',
          step: 1,
          at: expect.any(String),
        },
      ]);

      // And in the right place: after the text the model had already produced,
      // before the turn boundary. A rebuilt prompt then reads as the model's
      // own words followed by the instruction that redirected it, which is the
      // order they happened in.
      expect(written.map((envelope) => envelope.record._tag)).toEqual([
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

  it.effect('is delivered once, not to every later run', () =>
    Effect.gen(function* () {
      const scripted = model();

      const written = yield* run(
        Effect.gen(function* () {
          yield* AgentSignals.send(CONVERSATION, {
            kind: 'steer',
            text: 'keep going',
            source: 'operator',
          }).pipe(Effect.orDie);

          yield* agent.recordingTo(CONVERSATION).run('hi').pipe(Effect.orDie);
          yield* agent
            .recordingTo(CONVERSATION)
            .run('again')
            .pipe(Effect.orDie);
          return yield* readAll();
        }),
        scripted,
      );

      // Two turns in the first run, one in the second: the second run resumed
      // draining past what the first recorded taking, rather than re-reading
      // the signal stream from the beginning.
      expect(scripted.prompts).toHaveLength(3);
      expect(received(written)).toHaveLength(1);
    }),
  );

  it.effect('is visible to a consumer of the event stream', () =>
    Effect.gen(function* () {
      const scripted = model();

      const observed = yield* run(
        Effect.gen(function* () {
          yield* AgentSignals.send(CONVERSATION, {
            kind: 'steer',
            text: 'keep going',
            source: 'operator',
          }).pipe(Effect.orDie);

          return yield* agent
            .recordingTo(CONVERSATION)
            .stream('hi')
            .pipe(
              Stream.filter((event) => event._tag === 'Signalled'),
              Stream.runCollect,
              Effect.orDie,
            );
        }),
        scripted,
      );

      expect(observed).toMatchObject([
        { _tag: 'Signalled', step: 1, kind: 'steer', text: 'keep going' },
      ]);
    }),
  );
});

describe('cancelling', () => {
  // The agent that shows cancel doing something the stop condition would not:
  // this one is told to keep going for five steps, so anything that ends it
  // sooner ended it because it was told to.
  const persistent = Agent.make({
    name: 'persistent',
    revision: '1',
    instructions: 'keep going',
    toolkit: Toolkit.make(),
    stopWhen: Stop.maxSteps(5),
  });

  it.effect(
    'ends a run before the model when the cancel is already durable',
    () =>
      Effect.gen(function* () {
        const scripted = model();

        const result = yield* run(
          Effect.gen(function* () {
            yield* AgentSignals.send(CONVERSATION, {
              kind: 'cancel',
              text: 'user closed the tab',
              source: 'ui',
            }).pipe(Effect.orDie);

            return yield* persistent
              .recordingTo(CONVERSATION)
              .run('hi')
              .pipe(Effect.orDie);
          }),
          scripted,
        );

        expect(scripted.prompts).toHaveLength(0);
        // Cancellation ends a run; it does not fail one or add a model call.
        expect(result.text).toBe('');
        expect(result.steps).toBe(0);
        expect(result.outcome).toBe('cancelled');
      }),
  );

  it.effect('leaves the run alone when nobody cancelled it', () =>
    Effect.gen(function* () {
      const scripted = model();

      const result = yield* run(
        persistent.recordingTo(CONVERSATION).run('hi').pipe(Effect.orDie),
        scripted,
      );

      // The control for the case above: five steps, because the stop condition
      // is what ends this agent and it was not asked to stop early.
      expect(result.steps).toBe(5);
    }),
  );

  it.effect('settles the run as cancelled', () =>
    Effect.gen(function* () {
      const scripted = model();

      const written = yield* run(
        Effect.gen(function* () {
          yield* AgentSignals.send(CONVERSATION, {
            kind: 'cancel',
            text: 'user closed the tab',
            source: 'ui',
          }).pipe(Effect.orDie);

          yield* agent.recordingTo(CONVERSATION).run('hi').pipe(Effect.orDie);
          return yield* readAll();
        }),
        scripted,
      );

      expect(settlement(written)).toMatchObject([{ outcome: 'cancelled' }]);
    }),
  );

  it.effect('outranks a steer delivered in the same batch', () =>
    Effect.gen(function* () {
      const scripted = model();

      const result = yield* run(
        Effect.gen(function* () {
          yield* AgentSignals.send(CONVERSATION, {
            kind: 'steer',
            text: 'keep going',
            source: 'operator',
          }).pipe(Effect.orDie);
          yield* AgentSignals.send(CONVERSATION, {
            kind: 'cancel',
            text: 'never mind',
            source: 'ui',
          }).pipe(Effect.orDie);

          return yield* agent
            .recordingTo(CONVERSATION)
            .run('hi')
            .pipe(Effect.orDie);
        }),
        scripted,
      );

      expect(result.steps).toBe(0);
      expect(result.outcome).toBe('cancelled');
    }),
  );

  it.live(
    'interrupts an in-flight provider stream and preserves partial text',
    () =>
      Effect.gen(function* () {
        const result = yield* Effect.gen(function* () {
          const entered = yield* Deferred.make<void>();
          const stopped = yield* Deferred.make<void>();
          const blocking = Layer.succeed(
            LanguageModel.LanguageModel,
            yield* LanguageModel.make({
              generateText: () =>
                Effect.succeed<Response.PartEncoded[]>([finish()]),
              streamText: () =>
                Stream.unwrap(
                  Deferred.succeed(entered, undefined).pipe(
                    Effect.as(
                      Stream.concat(
                        Stream.fromIterable([
                          { type: 'text-start' as const, id: 'partial' },
                          {
                            type: 'text-delta' as const,
                            id: 'partial',
                            delta: 'partial answer',
                          },
                        ]),
                        Stream.never,
                      ).pipe(
                        Stream.ensuring(Deferred.succeed(stopped, undefined)),
                      ),
                    ),
                  ),
                ),
            }),
          );

          const running = yield* Effect.forkChild(
            agent
              .recordingTo(CONVERSATION)
              .run('hi')
              .pipe(Effect.provide(blocking)),
          );
          yield* Deferred.await(entered);
          yield* AgentSignals.send(CONVERSATION, {
            kind: 'steer',
            text: 'change direction',
            source: 'operator',
          }).pipe(Effect.orDie);
          yield* AgentSignals.send(CONVERSATION, {
            kind: 'cancel',
            text: 'stop now',
            source: 'ui',
          }).pipe(Effect.orDie);

          const completed = yield* Fiber.join(running);
          yield* Deferred.await(stopped);
          return { completed, records: yield* readAll() };
        }).pipe(Effect.provide(LogStoreMemory.layer));

        expect(result.completed.text).toBe('partial answer');
        expect(result.completed.steps).toBe(1);
        expect(result.completed.outcome).toBe('cancelled');
        expect(result.records.map(({ record }) => record._tag)).toEqual([
          'RunStarted',
          'Text',
          'SignalReceived',
          'SignalReceived',
          'TurnFinished',
          'Completed',
          'RunSettled',
        ]);
        expect(received(result.records).map((signal) => signal.kind)).toEqual([
          'steer',
          'cancel',
        ]);
        expect(settlement(result.records)).toMatchObject([
          { outcome: 'cancelled' },
        ]);
      }),
    2_000,
  );

  it.live('does not let an oversized cancel preempt a blocked stream', () =>
    Effect.gen(function* () {
      const entered = yield* Deferred.make<void>();
      const bounded = Agent.make({
        name: 'bounded-cancel',
        revision: '1',
        instructions: 'answer',
        toolkit: Toolkit.make(),
        runPolicy: { maxSignalBytes: 4, wallClockMillis: 100 },
      });
      const blocking: Model = {
        prompts: [],
        layer: Layer.succeed(
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
        ),
      };

      const outcome = yield* run(
        Effect.gen(function* () {
          const running = yield* Effect.forkChild(
            bounded.recordingTo(CONVERSATION).run('hi'),
          );
          yield* Deferred.await(entered);
          yield* AgentSignals.send(CONVERSATION, {
            kind: 'cancel',
            text: 'too large',
            source: 'ui',
          }).pipe(Effect.orDie);
          return yield* Fiber.join(running);
        }).pipe(Effect.result),
        blocking,
      );
      expect(String(outcome)).toContain('deadline');
    }),
  );

  it.live(
    'does not let a cancel behind the bounded page leapfrog backlog',
    () =>
      Effect.gen(function* () {
        const bounded = Agent.make({
          name: 'backlogged-cancel',
          revision: '1',
          instructions: 'answer',
          toolkit: Toolkit.make(),
          runPolicy: { maxSignalsPerBoundary: 1, wallClockMillis: 100 },
        });
        const blocked: Model = {
          prompts: [],
          layer: Layer.effect(
            LanguageModel.LanguageModel,
            LanguageModel.make({
              generateText: () =>
                Effect.succeed<Response.PartEncoded[]>([finish()]),
              streamText: () => Stream.never,
            }),
          ),
        };

        const outcome = yield* run(
          Effect.gen(function* () {
            yield* AgentSignals.send(CONVERSATION, {
              kind: 'steer',
              text: 'older',
              source: 'operator',
            }).pipe(Effect.orDie);
            yield* AgentSignals.send(CONVERSATION, {
              kind: 'cancel',
              text: 'newer',
              source: 'ui',
            }).pipe(Effect.orDie);
            return yield* bounded.recordingTo(CONVERSATION).run('hi');
          }).pipe(Effect.result),
          blocked,
        );
        expect(String(outcome)).toContain('deadline');
      }),
  );

  it.effect(
    'falls back to boundary cancellation when the change feed fails',
    () =>
      Effect.gen(function* () {
        const scripted = model();

        const result = yield* Effect.gen(function* () {
          yield* AgentSignals.send(CONVERSATION, {
            kind: 'cancel',
            text: 'stop at boundary',
            source: 'ui',
          }).pipe(Effect.orDie);
          return yield* agent.recordingTo(CONVERSATION).run('hi');
        }).pipe(
          Effect.provide(scripted.layer),
          Effect.provide(
            LogStoreMemory.layerFailingChanges(
              AgentSignals.pathFor(
                LogVocabulary.ConversationId.make(CONVERSATION),
              ),
            ),
          ),
        );

        expect(result.text).toBe('turn 1');
        expect(result.outcome).toBe('cancelled');
        expect(scripted.prompts).toHaveLength(1);
      }),
  );

  it.live(
    'keeps watcher read failures typed and tears the watcher down',
    () =>
      Effect.gen(function* () {
        const readFailed = yield* Deferred.make<void>();
        const stopped = yield* Deferred.make<void>();
        let failNextSignalRead = true;
        const instrumented = Layer.effect(
          LogStore.Service,
          Effect.map(LogStore.Service, (store) =>
            LogStore.Service.of({
              ...store,
              read: (path, options) =>
                path ===
                  AgentSignals.pathFor(
                    LogVocabulary.ConversationId.make(CONVERSATION),
                  ) && failNextSignalRead
                  ? Effect.sync(() => {
                      failNextSignalRead = false;
                    }).pipe(
                      Effect.andThen(Deferred.succeed(readFailed, undefined)),
                      Effect.andThen(
                        Effect.fail(
                          new LogStore.LogStoreError({
                            path,
                            operation: 'read',
                            reason: 'storage',
                            detail: 'injected watcher read failure',
                          }),
                        ),
                      ),
                    )
                  : store.read(path, options),
              changes: (path) =>
                store
                  .changes(path)
                  .pipe(Stream.ensuring(Deferred.succeed(stopped, undefined))),
            }),
          ),
        ).pipe(Layer.provide(LogStoreMemory.layer));
        const calls = { count: 0 };
        const waitingProvider = Layer.effect(
          LanguageModel.LanguageModel,
          LanguageModel.make({
            generateText: () =>
              Effect.succeed<Response.PartEncoded[]>([finish()]),
            streamText: () =>
              Stream.unwrap(
                Deferred.await(readFailed).pipe(
                  Effect.tap(() =>
                    Effect.sync(() => {
                      calls.count += 1;
                    }),
                  ),
                  Effect.as(Stream.fromIterable(says('after watcher failure'))),
                ),
              ),
          }),
        );

        const result = yield* agent
          .recordingTo(CONVERSATION)
          .run('hi')
          .pipe(Effect.provide(waitingProvider), Effect.provide(instrumented));
        yield* Deferred.await(stopped);

        expect(result.text).toBe('after watcher failure');
        expect(calls.count).toBe(1);
      }),
    2_000,
  );

  it.live(
    'scopes the change-feed watcher to the provider stream',
    () =>
      Effect.gen(function* () {
        const scripted = model();

        yield* Effect.gen(function* () {
          const stopped = yield* Deferred.make<void>();
          const instrumented = Layer.effect(
            LogStore.Service,
            Effect.map(LogStore.Service, (store) =>
              LogStore.Service.of({
                ...store,
                changes: (path) =>
                  store
                    .changes(path)
                    .pipe(
                      Stream.ensuring(Deferred.succeed(stopped, undefined)),
                    ),
              }),
            ),
          ).pipe(Layer.provide(LogStoreMemory.layer));

          yield* agent
            .recordingTo(CONVERSATION)
            .run('hi')
            .pipe(Effect.provide(scripted.layer), Effect.provide(instrumented));
          yield* Deferred.await(stopped);
        });
      }),
    2_000,
  );
});

// Everything above sends its signal before the run exists, which is the easy
// half: the signal stream is already there when the session opens, and the
// very first drain finds it. The production case is the other one — an
// operator types into a thread while the agent is mid-tool-call — and it
// exercises paths the pre-seeded tests cannot reach:
//
//   - the first drain runs against a signal stream that **does not exist yet**
//     and must come back empty rather than dying, and the cursor it leaves
//     behind must still be usable by the drain that follows;
//   - the drain happens at *every* turn boundary, not once at the start;
//   - the cursor advances *within* one run, so a steer taken at one boundary
//     is not taken again at the next.
//
// The signal is sent from a tool handler, because that is where a run
// genuinely is when somebody steers it: inside a turn, with the model waiting.

const CALL = (id: string, name: string): Response.StreamPartEncoded => ({
  type: 'tool-call',
  id,
  name,
  params: {},
});

/**
 * How many times an instruction appears in a rebuilt prompt.
 *
 * Not `toContain`: a prompt is the whole conversation so far, so a steer
 * delivered at turn 2 is legitimately still in the prompt at turn 4. The
 * failure worth catching is a *second copy* of it, which is what a cursor that
 * did not advance produces — and which `toContain` cannot see.
 */
const occurrences = (haystack: string, needle: string): number =>
  haystack.split(needle).length - 1;

const work = Tool.make('work', {
  description: 'do some work',
  parameters: Schema.Struct({}),
  success: Schema.Struct({ ok: Schema.Boolean }),
  // The handler sends a signal, which needs the store. Declared rather than
  // captured, so the requirement is visible on the tool the way any other
  // service-using tool declares it.
  dependencies: [LogStore.Service],
});

/**
 * A model that calls `work` for the first `toolTurns` turns, then talks.
 *
 * The tool calls are what keep the loop going without a steer: the default
 * stop condition is "no tool calls", so every turn that asks for one continues
 * on its own merits and nothing here is measuring the steer's stop-override.
 * What is being measured is the *content* of the next prompt, and when the run
 * ends.
 */
const workThenTalk = (toolTurns: number): Model => {
  const prompts: string[] = [];
  return {
    prompts,
    layer: Layer.effect(
      LanguageModel.LanguageModel,
      Effect.gen(function* () {
        const calls = yield* Ref.make(0);
        return yield* LanguageModel.make({
          generateText: () =>
            Effect.succeed<Response.PartEncoded[]>([finish()]),
          streamText: (options) =>
            Stream.unwrap(
              Effect.gen(function* () {
                const index = yield* Ref.getAndUpdate(calls, (n) => n + 1);
                prompts.push(JSON.stringify(options.prompt));
                return Stream.fromIterable(
                  index < toolTurns
                    ? [
                        CALL(`call-${index + 1}`, 'work'),
                        { ...finish(), reason: 'tool-calls' as const },
                      ]
                    : says(`turn ${index + 1}`),
                );
              }),
            ),
        });
      }),
    ),
  };
};

/**
 * An agent whose tool sends signals to its own conversation while it runs.
 *
 * `sendOn` is keyed by turn so a test can put a signal at a chosen boundary
 * without the model script and the signal script having to agree twice.
 */
const signallingAgent = (sendOn: ReadonlyMap<number, AgentSignals.Signal>) => {
  const turns = { count: 0 };
  return Agent.make({
    name: 'test',
    revision: '1',
    instructions: 'be terse',
    toolkit: Toolkit.make(work),
    // The default policy with a lower ceiling: a turn that asks for a tool
    // continues, a turn that does not stops, and nothing runs away.
    stopWhen: Stop.any(Stop.noToolCalls(), Stop.maxSteps(8)),
  }).withHandlers({
    work: () =>
      Effect.gen(function* () {
        turns.count += 1;
        const signal = sendOn.get(turns.count);
        if (signal !== undefined) {
          yield* AgentSignals.send(CONVERSATION, signal).pipe(Effect.orDie);
        }
        return { ok: true };
      }),
  });
};

describe('a signal that arrives while the run is in flight', () => {
  it.live('is delivered at the next turn boundary, not only at the first', () =>
    Effect.gen(function* () {
      const scripted = workThenTalk(2);

      const written = yield* run(
        Effect.gen(function* () {
          yield* signallingAgent(
            new Map([
              [
                2,
                {
                  kind: 'steer',
                  text: 'also check the invoice',
                  source: 'operator',
                } satisfies AgentSignals.Signal,
              ],
            ]),
          )
            .recordingTo(CONVERSATION)
            .run('hi')
            .pipe(Effect.orDie);

          return yield* readAll();
        }),
        scripted,
      );

      // Turn 1's boundary drained a signal stream that did not exist yet and
      // survived it; turn 2's tool sent one; turn 3 is where it lands.
      expect(scripted.prompts).toHaveLength(3);
      expect(occurrences(scripted.prompts[0]!, 'also check the invoice')).toBe(
        0,
      );
      expect(occurrences(scripted.prompts[1]!, 'also check the invoice')).toBe(
        0,
      );
      expect(occurrences(scripted.prompts[2]!, 'also check the invoice')).toBe(
        1,
      );

      expect(received(written)).toMatchObject([
        { kind: 'steer', text: 'also check the invoice', step: 2 },
      ]);
    }),
  );

  it.effect('ends the run at the boundary when it is a cancel', () =>
    Effect.gen(function* () {
      const scripted = workThenTalk(4);

      const result = yield* run(
        signallingAgent(
          new Map([
            [
              1,
              {
                kind: 'cancel',
                text: 'user closed the tab',
                source: 'ui',
              } satisfies AgentSignals.Signal,
            ],
          ]),
        )
          .recordingTo(CONVERSATION)
          .run('hi')
          .pipe(Effect.orDie),
        scripted,
      );

      // The control is in the turn script: this model asks for a tool on the
      // first four turns, so the stop condition cannot fire at step 1 and the
      // step ceiling is 6. One step means the cancel ended it.
      expect(result.steps).toBe(1);
      expect(scripted.prompts).toHaveLength(1);
    }),
  );

  it.effect(
    'preempts a stalled provider after dispatch commits and its outcome is durable',
    () =>
      Effect.gen(function* () {
        const scripted: Model = {
          prompts: [],
          layer: Layer.effect(
            LanguageModel.LanguageModel,
            LanguageModel.make({
              generateText: () =>
                Effect.succeed<Response.PartEncoded[]>([finish()]),
              streamText: (options) => {
                scripted.prompts.push(JSON.stringify(options.prompt));
                return Stream.concat(
                  Stream.make(CALL('call-1', 'work')),
                  Stream.never,
                );
              },
            }),
          ),
        };

        const observed = yield* run(
          Effect.gen(function* () {
            const result = yield* signallingAgent(
              new Map([
                [
                  1,
                  {
                    kind: 'cancel',
                    text: 'stop after work',
                    source: 'ui',
                  } satisfies AgentSignals.Signal,
                ],
              ]),
            )
              .recordingTo(CONVERSATION)
              .run('hi');
            return { result, records: yield* readAll() };
          }),
          scripted,
        );

        expect(observed.result.steps).toBe(1);
        const tags = observed.records.map(({ record }) => record._tag);
        expect(tags.indexOf('ToolOutcome')).toBeLessThan(
          tags.indexOf('SignalReceived'),
        );
        expect(settlement(observed.records)).toMatchObject([
          { outcome: 'cancelled' },
        ]);
      }),
    2_000,
  );

  it.effect('leaves the run alone when nothing is sent mid-flight', () =>
    Effect.gen(function* () {
      const scripted = workThenTalk(4);

      const result = yield* run(
        signallingAgent(new Map())
          .recordingTo(CONVERSATION)
          .run('hi')
          .pipe(Effect.orDie),
        scripted,
      );

      // The other half of the control above: four tool turns then an answer, so
      // an unsignalled run of this agent takes five steps.
      expect(result.steps).toBe(5);
    }),
  );

  // The cursor is per-run state advanced by each drain. Not advancing it — or
  // advancing it from the wrong page — re-delivers a steer the agent has
  // already acted on, at every remaining boundary of the same run.
  it.effect(
    'advances its cursor within one run, so no steer is taken twice',
    () =>
      Effect.gen(function* () {
        const scripted = workThenTalk(3);

        const written = yield* run(
          Effect.gen(function* () {
            yield* signallingAgent(
              new Map([
                [
                  1,
                  {
                    kind: 'steer',
                    text: 'first instruction',
                    source: 'operator',
                  } satisfies AgentSignals.Signal,
                ],
                [
                  3,
                  {
                    kind: 'steer',
                    text: 'second instruction',
                    source: 'operator',
                  } satisfies AgentSignals.Signal,
                ],
              ]),
            )
              .recordingTo(CONVERSATION)
              .run('hi')
              .pipe(Effect.orDie);

            return yield* readAll();
          }),
          scripted,
        );

        expect(received(written).map((record) => record.text)).toEqual([
          'first instruction',
          'second instruction',
        ]);

        // Four turns: tool, tool, tool, answer. Each instruction reaches the turn
        // after it was sent and appears exactly once thereafter — a second copy is
        // what a cursor stuck at its starting offset would produce.
        expect(scripted.prompts).toHaveLength(4);
        expect(occurrences(scripted.prompts[0]!, 'first instruction')).toBe(0);
        expect(occurrences(scripted.prompts[1]!, 'first instruction')).toBe(1);
        expect(occurrences(scripted.prompts[2]!, 'first instruction')).toBe(1);
        expect(occurrences(scripted.prompts[3]!, 'first instruction')).toBe(1);
        expect(occurrences(scripted.prompts[3]!, 'second instruction')).toBe(1);
      }),
  );

  // A run that took a steer records how far it drained. The next run has to
  // start from there and no earlier, or the instruction is injected a second
  // time into a conversation that already followed it.
  it.effect('does not re-deliver a mid-run steer to the run after it', () =>
    Effect.gen(function* () {
      const scripted = workThenTalk(1);

      const written = yield* run(
        Effect.gen(function* () {
          const sending = signallingAgent(
            new Map([
              [
                1,
                {
                  kind: 'steer',
                  text: 'mid-run instruction',
                  source: 'operator',
                } satisfies AgentSignals.Signal,
              ],
            ]),
          );

          yield* sending.recordingTo(CONVERSATION).run('hi').pipe(Effect.orDie);
          yield* sending
            .recordingTo(CONVERSATION)
            .run('again')
            .pipe(Effect.orDie);
          return yield* readAll();
        }),
        scripted,
      );

      expect(received(written)).toHaveLength(1);
    }),
  );
});

describe('an agent that is not recording', () => {
  it.effect('has no conversation to signal, and is unaffected by one', () =>
    Effect.gen(function* () {
      const scripted = model();

      const result = yield* run(
        Effect.gen(function* () {
          yield* AgentSignals.send(CONVERSATION, {
            kind: 'cancel',
            text: 'never mind',
            source: 'ui',
          }).pipe(Effect.orDie);

          return yield* agent.run('hi').pipe(Effect.orDie);
        }),
        scripted,
      );

      // Signals are addressed to a conversation, and a run only has one when it
      // is recording. This is a real limit, not an oversight: without an
      // identity there is nothing for a sender to address.
      expect(result.text).toBe('turn 1');
      expect(scripted.prompts).toHaveLength(1);
    }),
  );
});
