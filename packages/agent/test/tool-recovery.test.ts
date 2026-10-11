import { describe, expect, it } from '@effect/vitest';
import * as NodeServices from '@effect/platform-node/NodeServices';
import { LogStoreMemory } from '@sunfall/vesper-log/layer-memory';
import { LogStore } from '@sunfall/vesper-log/log-store';
import { type Crypto, Effect, Layer, Result, Schema, Stream } from 'effect';
import { AiError, type LanguageModel, Tool, Toolkit } from 'effect/ai';

import { Agent } from '../src/agent.js';
import { Conversation } from '../src/conversation.js';
import { Interception } from '../src/interception.js';
import { ScriptedModel } from '../src/testing.js';
import {
  CALL_2,
  CALL_ID,
  RECORDED,
  RECORDED_ISO,
  date,
  digestOf,
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

// Recovering tool calls from the log. Each scenario writes the exact records a
// previous run left behind, then runs the same conversation again.

const ID = 'recovery';
const WHEN = date('2026-01-02T03:04:05.000Z');

const lookupDef = (
  failure?: Schema.Struct<{ readonly code: typeof Schema.String }>,
) =>
  Tool.make('lookup', {
    description: 'look an order up',
    parameters: Schema.Struct({ id: Schema.String }),
    success: Schema.Struct({
      status: Schema.String,
      at: Schema.DateFromString,
    }),
    ...(failure === undefined ? {} : { failure }),
  });
const lookup = lookupDef();

const lookupAgent = (ran: { count: number }) =>
  Agent.make({
    name: 'test',
    revision: '1',
    instructions: 'be terse',
    toolkit: Toolkit.make(lookup),
  }).withHandlers({
    lookup: ({ id }) =>
      Effect.sync(() => {
        ran.count += 1;
        return { status: `fresh:${id}`, at: WHEN };
      }),
  });

const script = () =>
  ScriptedModel.make([
    callTurn({ id: CALL_ID, name: 'lookup', params: { id: '42' } }),
    textTurn('done'),
  ]);

const served = (status: string) =>
  toolOutcome('lookup', { status, at: RECORDED_ISO });
const crashed = [runStarted('test'), toolCalled('lookup', { id: '42' })];
const orphan = [...crashed, toolStarted('lookup')];

type Services = LogStore.Service | LanguageModel.LanguageModel | Crypto.Crypto;

/** Seed a previous run, then run the same conversation again. */
const resumeRun = <E>(
  previous: Parameters<typeof seed>[1],
  run: Effect.Effect<unknown, E, Services>,
  model = script(),
) =>
  provide(
    Effect.gen(function* () {
      yield* seed(ID, previous);
      const result = yield* run.pipe(Effect.result);
      return { result, records: yield* readRecords(ID) };
    }),
    model,
  );

const resume = (
  previous: Parameters<typeof seed>[1],
  agent: ReturnType<typeof lookupAgent>,
  model = script(),
) => resumeRun(previous, Conversation.make(agent, ID).run('hi'), model);

describe('a call whose earlier run already settled it', () => {
  it.effect(
    'is served from the log: not re-run, shown to the model, recorded as served, decoded for the stream',
    () => {
      const ran = { count: 0 };
      const model = script();
      return provide(
        Effect.gen(function* () {
          yield* seed(ID, [...crashed, served('from-log')]);
          const events = yield* Conversation.make(lookupAgent(ran), ID)
            .stream('hi')
            .pipe(
              Stream.runCollect,
              Effect.map((all) => Array.from(all)),
              Effect.orDie,
            );

          expect(ran.count).toBe(0);
          const prompt = promptText(yield* model.requests, 1);
          expect(prompt).toContain('from-log');
          expect(prompt).not.toContain('fresh:');
          expect(outcomesOf(yield* readRecords(ID)).at(-1)).toMatchObject({
            id: CALL_ID,
            result: { status: 'from-log' },
          });
          // The live stream carries the decoded half: a Date, not the stored string.
          expect(
            events.find(
              (event) =>
                event._tag === 'Part' && event.part.type === 'tool-result',
            ),
          ).toMatchObject({
            part: {
              result: { status: 'from-log', at: RECORDED },
              resultSource: 'substituted',
            },
          });
        }),
        model,
      );
    },
  );

  it.effect(
    'serves a spilled-result pointer by its shape, not the tool schema',
    () => {
      const ran = { count: 0 };
      const pointer = {
        _tag: 'ToolResultOverflow',
        attachmentId: `sha256:${'a'.repeat(64)}`,
        byteLength: 5_000,
        mediaType: 'text/plain; charset=utf-8',
        preview: 'the record this settled call actually produced',
      };
      return resume(
        [...crashed, toolOutcome('lookup', pointer)],
        lookupAgent(ran),
      ).pipe(
        Effect.tap(({ records }) =>
          Effect.sync(() => {
            // Decoding the pointer against `lookup`'s schema would fail and
            // make any conversation holding a spilled result unresumable.
            expect(ran.count).toBe(0);
            expect(outcomesOf(records).at(-1)?.result).toMatchObject({
              _tag: 'ToolResultOverflow',
            });
          }),
        ),
      );
    },
  );

  it.effect(
    'serves a recorded failure even for a tool whose own failure schema cannot decode it',
    () => {
      const ran = { count: 0 };
      const denied = new AiError.AiError({
        module: 'test',
        method: 'deny',
        reason: new AiError.UnknownError({ description: 'denied' }),
      });
      return Effect.gen(function* () {
        const encoded = yield* Schema.encodeUnknownEffect(AiError.AiError)(
          denied,
        );
        const { records } = yield* resume(
          [...crashed, toolOutcome('lookup', encoded, 'failure')],
          lookupAgent(ran),
        );
        expect(ran.count).toBe(0);
        expect(outcomesOf(records).at(-1)).toMatchObject({
          outcome: 'failure',
          result: encoded,
        });
      });
    },
  );

  it.effect(
    'fails safely when the recorded result no longer matches the schema',
    () => {
      const ran = { count: 0 };
      return resume(
        [...crashed, toolOutcome('lookup', { staleShape: true })],
        lookupAgent(ran),
      ).pipe(
        Effect.tap(({ result }) =>
          Effect.sync(() => {
            expect(Result.isFailure(result)).toBe(true);
            expect(String(result)).toContain(
              'does not match its current result schema',
            );
            expect(ran.count).toBe(0);
          }),
        ),
      );
    },
  );

  // A recording policy may have replaced the recorded parameters. The call is
  // still the same one: identity is the digest taken before the policy ran.
  it.effect(
    'is served even when a recording policy redacted its parameters',
    () => {
      const ran = { count: 0 };
      return resume(
        [
          runStarted('test'),
          {
            _tag: 'StepOpened',
            step: 1,
            id: CALL_ID,
            name: 'lookup',
            inputDigest: digestOf('lookup', { id: '42' }),
            params: { redacted: true },
          },
          served('from-log'),
        ],
        lookupAgent(ran),
      ).pipe(
        Effect.tap(({ records }) =>
          Effect.sync(() => {
            expect(ran.count).toBe(0);
            expect(outcomesOf(records).at(-1)?.result).toMatchObject({
              status: 'from-log',
            });
          }),
        ),
      );
    },
  );

  const rerun = [
    ['the earlier run has settled', [...crashed, served('old'), runSettled]],
    [
      'the outcome was recorded under another tool name',
      [...crashed, { ...served('old'), name: 'something_else' }],
    ],
  ] as const;
  for (const [name, previous] of rerun) {
    it.effect(`is run again when ${name}`, () => {
      const ran = { count: 0 };
      return resume(previous, lookupAgent(ran)).pipe(
        Effect.tap(({ records }) =>
          Effect.sync(() => {
            // Serving a finished conversation's outcomes would answer a new
            // question with an old answer.
            expect(ran.count).toBe(1);
            expect(outcomesOf(records).at(-1)?.result).toMatchObject({
              status: 'fresh:42',
            });
          }),
        ),
      );
    });
  }
});

describe('a call that was started but never settled', () => {
  it.effect(
    'is not re-run without a decision, even when beforeToolCall would dispatch',
    () => {
      const ran = { count: 0 };
      const model = script();
      return resume(
        orphan,
        lookupAgent(ran).intercepting({
          beforeToolCall: () => Effect.succeed(Interception.dispatch),
        }),
        model,
      ).pipe(
        Effect.tap(({ result, records }) =>
          Effect.gen(function* () {
            expect(Result.isFailure(result)).toBe(true);
            expect(ran.count).toBe(0);
            expect(yield* model.requests).toEqual([]);
            expect(tagsOf(records)).not.toContain('RunSettled');
          }),
        ),
      );
    },
  );

  it.effect('fails safely when StepStarted has no original StepOpened', () => {
    const ran = { count: 0 };
    const asked: string[] = [];
    const model = script();
    return resume(
      [runStarted('test'), toolStarted('lookup')],
      lookupAgent(ran).intercepting({
        onIndeterminateToolCall: () => {
          asked.push('asked');
          return Effect.succeed(Interception.retry);
        },
      }),
      model,
    ).pipe(
      Effect.tap(({ result }) =>
        Effect.gen(function* () {
          expect(String(result)).toContain('StepStarted has no StepOpened');
          expect(asked).toEqual([]);
          expect(ran.count).toBe(0);
          expect(yield* model.requests).toEqual([]);
        }),
      ),
    );
  });

  it.effect(
    'asks about each orphan in StepOpened order, wherever its StepStarted sits',
    () => {
      const seen: string[] = [];
      return resume(
        [
          runStarted('test'),
          toolStarted('lookup', CALL_2),
          toolCalled('lookup', { id: '42' }),
          toolCalled('lookup', { id: '43' }, CALL_2),
          toolStarted('lookup'),
        ],
        lookupAgent({ count: 0 }).intercepting({
          onIndeterminateToolCall: (call) => {
            seen.push(String(call.toolCallId));
            return Effect.succeed(
              Interception.reconcile({
                status: 'confirmed',
                at: RECORDED_ISO,
              }),
            );
          },
        }),
      ).pipe(
        Effect.tap(() =>
          Effect.sync(() => {
            expect(seen).toEqual([CALL_ID, CALL_2]);
          }),
        ),
      );
    },
  );

  it.effect('runs the tool once, and only, on an explicit Retry', () => {
    const ran = { count: 0 };
    const asked: unknown[] = [];
    const model = script();
    return resume(
      orphan,
      lookupAgent(ran).intercepting({
        onIndeterminateToolCall: (call) => {
          asked.push([call.name, call.toolCallId, call.params]);
          return Effect.succeed(Interception.retry);
        },
      }),
      model,
    ).pipe(
      Effect.tap(({ records }) =>
        Effect.gen(function* () {
          expect(ran.count).toBe(1);
          expect(asked).toEqual([['lookup', CALL_ID, { id: '42' }]]);
          expect(promptText(yield* model.requests, 1)).toContain('fresh:42');
          expect(
            tagsOf(records).filter((tag) => tag === 'StepStarted'),
          ).toHaveLength(2);
          expect(tagsOf(records).at(-1)).toBe('RunSettled');
        }),
      ),
    );
  });

  it.effect('reconciles with an explicit Answer without dispatching', () => {
    const ran = { count: 0 };
    const model = script();
    return resume(
      orphan,
      lookupAgent(ran).intercepting({
        onIndeterminateToolCall: () =>
          Effect.succeed(
            Interception.reconcile({
              status: 'confirmed',
              at: RECORDED_ISO,
            }),
          ),
      }),
      model,
    ).pipe(
      Effect.tap(({ records }) =>
        Effect.gen(function* () {
          expect(ran.count).toBe(0);
          expect(promptText(yield* model.requests, 1)).toContain('confirmed');
          expect(outcomesOf(records).at(-1)?.result).toMatchObject({
            status: 'confirmed',
          });
          expect(tagsOf(records).at(-1)).toBe('RunSettled');
        }),
      ),
    );
  });

  it.effect(
    'rejects a reconciliation answer outside the tool result schema',
    () => {
      const ran = { count: 0 };
      return resume(
        orphan,
        lookupAgent(ran).intercepting({
          onIndeterminateToolCall: () =>
            Effect.succeed(Interception.reconcile({ staleShape: true })),
        }),
      ).pipe(
        Effect.tap(({ result }) =>
          Effect.sync(() => {
            expect(String(result)).toContain(
              'does not match its current result schema',
            );
            expect(ran.count).toBe(0);
          }),
        ),
      );
    },
  );

  it.effect('normalizes a typed handler failure during retry', () => {
    const failing = Agent.make({
      name: 'test',
      revision: '1',
      instructions: 'be terse',
      toolkit: Toolkit.make(lookupDef(Schema.Struct({ code: Schema.String }))),
    }).withHandlers({ lookup: () => Effect.fail({ code: 'typed-failure' }) });
    return resumeRun(
      orphan,
      Conversation.make(
        failing.intercepting({
          onIndeterminateToolCall: () => Effect.succeed(Interception.retry),
        }),
        ID,
      ).run('hi'),
    ).pipe(
      Effect.tap(({ result }) =>
        Effect.sync(() => {
          expect(
            Result.isFailure(result) && AiError.isAiError(result.failure),
          ).toBe(true);
        }),
      ),
    );
  });

  it.effect(
    'leaves a crashing dispatch orphaned: StepStarted is durable before the handler runs',
    () => {
      const trackedLookup = Tool.make('lookup', {
        description: 'look an order up',
        parameters: Schema.Struct({ id: Schema.String }),
        success: Schema.Struct({ status: Schema.String }),
        dependencies: [LogStore.Service],
      });
      const observed = { startedFirst: false, mutations: 0 };
      const crashing = Agent.make({
        name: 'test',
        revision: '1',
        instructions: 'be terse',
        toolkit: Toolkit.make(trackedLookup),
      }).withHandlers({
        lookup: () =>
          Effect.gen(function* () {
            observed.startedFirst = tagsOf(yield* readRecords(ID)).includes(
              'StepStarted',
            );
            observed.mutations += 1;
            return yield* Effect.die(
              new Error('process lost after dispatch commits'),
            );
          }),
      });
      return provide(
        Effect.gen(function* () {
          yield* Conversation.make(crashing, ID).run('hi').pipe(Effect.exit);
          const tags = tagsOf(yield* readRecords(ID));
          expect(observed).toEqual({ startedFirst: true, mutations: 1 });
          expect(tags).not.toContain('StepSettled');
          expect(tags).not.toContain('RunSettled');
        }),
        script(),
      );
    },
  );

  it.effect('keeps a crashed Retry indeterminate for the next resume', () => {
    const mutations = { count: 0 };
    const crashing = Agent.make({
      name: 'test',
      revision: '1',
      instructions: 'be terse',
      toolkit: Toolkit.make(lookup),
    }).withHandlers({
      lookup: () =>
        Effect.sync(() => {
          mutations.count += 1;
          throw new Error('lost after retry mutation');
        }),
    });
    return provide(
      Effect.gen(function* () {
        yield* seed(ID, orphan);
        yield* Conversation.make(
          crashing.intercepting({
            onIndeterminateToolCall: () => Effect.succeed(Interception.retry),
          }),
          ID,
        )
          .run('hi')
          .pipe(Effect.exit);
        yield* Conversation.make(
          lookupAgent({ count: 0 }).intercepting({
            onIndeterminateToolCall: (call) =>
              Effect.succeed(
                Interception.reconcile({
                  status: `externally-confirmed:${String(call.toolCallId)}`,
                  at: RECORDED_ISO,
                }),
              ),
          }),
          ID,
        ).run('hi');
        expect(mutations.count).toBe(1);
        expect(outcomesOf(yield* readRecords(ID)).at(-1)?.result).toMatchObject(
          {
            status: `externally-confirmed:${CALL_ID}`,
          },
        );
      }),
      script(),
    );
  });
});

describe('cancelling and deadlines while a call is indeterminate', () => {
  it.effect('lets a queued cancel win before a retry has side effects', () => {
    const ran = { count: 0 };
    const model = script();
    return provide(
      Effect.gen(function* () {
        yield* seed(ID, orphan);
        yield* Conversation.make(lookupAgent(ran), ID).send({
          kind: 'cancel',
          text: 'stop before retry',
          source: 'test',
        });
        const result = yield* Conversation.make(
          lookupAgent(ran).intercepting({
            onIndeterminateToolCall: () => Effect.succeed(Interception.retry),
          }),
          ID,
        ).run('hi');
        expect(result.outcome).toBe('cancelled');
        expect(ran.count).toBe(0);
        expect(yield* model.requests).toEqual([]);
      }),
      model,
    );
  });

  it.effect(
    'lets a cancel arriving during recovery win before dispatch commits',
    () => {
      const ran = { count: 0 };
      return provide(
        Effect.gen(function* () {
          yield* seed(ID, orphan);
          const result = yield* Conversation.make(
            lookupAgent(ran).intercepting({
              onIndeterminateToolCall: () =>
                Conversation.make(lookupAgent(ran), ID)
                  .send({
                    kind: 'cancel',
                    text: 'stop during recovery',
                    source: 'test',
                  })
                  // Stay in recovery until the signal watcher observes the
                  // durable cancel; returning Retry would reintroduce a race.
                  .pipe(Effect.orDie, Effect.andThen(Effect.never)),
            }),
            ID,
          ).run('hi');
          expect(result.outcome).toBe('cancelled');
          expect(ran.count).toBe(0);
        }),
        script(),
      );
    },
  );

  it.live('interrupts a retry at the root deadline', () => {
    const entered = { count: 0 };
    const hanging = Agent.make({
      name: 'test',
      revision: '1',
      instructions: 'be terse',
      toolkit: Toolkit.make(lookup),
      runPolicy: { wallClockMillis: 100 },
    }).withHandlers({
      lookup: () =>
        Effect.sync(() => void (entered.count += 1)).pipe(
          Effect.andThen(Effect.never),
        ),
    });
    return resume(
      orphan,
      hanging.intercepting({
        onIndeterminateToolCall: () => Effect.succeed(Interception.retry),
      }),
    ).pipe(
      Effect.tap(({ result }) =>
        Effect.sync(() => {
          expect(Result.isFailure(result)).toBe(true);
          expect(String(result)).toContain('deadline');
          expect(entered.count).toBe(1);
        }),
      ),
    );
  });
});

describe('a dispatch whose durable start cannot be recorded', () => {
  it.effect(
    'never runs the handler, leaves no orphan, and does not wedge the conversation',
    () => {
      const ran = { count: 0 };
      const fault = { active: true };
      // Fault injection at the public store seam: refuse the batch that holds StepStarted.
      const flaky = Layer.effect(
        LogStore.Service,
        Effect.gen(function* () {
          const real = yield* LogStore.Service;
          return LogStore.Service.of({
            ...real,
            append: (input) =>
              fault.active &&
              input.records.some(({ record }) => record._tag === 'StepStarted')
                ? Effect.fail(
                    new LogStore.LogStoreError({
                      path: input.path,
                      operation: 'append',
                      reason: 'storage',
                      detail: 'StepStarted append failed',
                    }),
                  )
                : real.append(input),
          });
        }),
      ).pipe(
        Layer.provide(LogStoreMemory.layer),
        Layer.provideMerge(NodeServices.layer),
      );
      const model = script();
      return Effect.gen(function* () {
        const context = yield* Layer.build(Layer.merge(model.layer, flaky));
        const agent = lookupAgent(ran);
        const failed = yield* Conversation.make(agent, ID)
          .run('hi')
          .pipe(Effect.result, Effect.provideContext(context));

        expect(Result.isFailure(failed)).toBe(true);
        expect(ran.count).toBe(0);
        const during = yield* readRecords(ID).pipe(
          Effect.provideContext(context),
        );
        expect(tagsOf(during)).not.toContain('StepStarted');

        // The permit was released: a cancel is not held back, and a fresh run completes.
        fault.active = false;
        const cancel = yield* Conversation.make(agent, ID)
          .send({ kind: 'cancel', text: 'stop', source: 'test' })
          .pipe(
            Effect.timeout('1 second'),
            Effect.exit,
            Effect.provideContext(context),
          );
        expect(cancel._tag).toBe('Success');
      }).pipe(Effect.scoped);
    },
  );
});
