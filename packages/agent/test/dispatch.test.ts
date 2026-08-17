import { LogStoreMemory } from '@sunfall/vesper-log/layer-memory';
import { LogStore } from '@sunfall/vesper-log/log-store';
import type { ConversationRecord } from '@sunfall/vesper-log/record';
import { Effect, Fiber, Layer, Ref, Schema, Stream } from 'effect';
import {
  LanguageModel,
  type Response,
  Tool,
  Toolkit,
} from 'effect/unstable/ai';
import { describe, expect, it } from 'vitest';

import { Agent } from '../src/agent.js';
import { Interception } from '../src/interception.js';
import { AgentLog } from '../src/log.js';
import { ToolDispatch } from '../src/dispatch.js';
import { AgentSignals } from '../src/signal.js';

// The tool-dispatch seam.
//
// What these have to prove, and each is mutation-checked:
//
//   - a tool whose outcome a *crashed* run already recorded is not run again,
//     and the model is shown the recorded answer;
//   - a tool whose outcome a *settled* run recorded IS run again, because a
//     conversation that finished has nothing to recover;
//   - a fresh conversation dispatches normally, so the seam costs nothing on
//     the ordinary path;
//   - what is stored and served is the toolkit's *encoding* of the result,
//     which is the form the provider is shown.

const finish = (reason: 'stop' | 'tool-calls' = 'stop') => ({
  type: 'finish' as const,
  reason,
  usage: {
    inputTokens: { total: 10, uncached: 10, cacheRead: 0, cacheWrite: 0 },
    outputTokens: { total: 4 },
  },
});

const CALL_ID = 'call-1';

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

/** A model that also records the prompt it was handed, per turn. */
const scripted = (prompts: string[]) =>
  Layer.effect(
    LanguageModel.LanguageModel,
    Effect.gen(function* () {
      const calls = yield* Ref.make(0);
      const turns = [callingTurn, answeringTurn];
      return yield* LanguageModel.make({
        generateText: () => Effect.succeed<Response.PartEncoded[]>([finish()]),
        streamText: (options) =>
          Stream.unwrap(
            Effect.gen(function* () {
              const index = yield* Ref.getAndUpdate(calls, (n) => n + 1);
              prompts.push(JSON.stringify(options.prompt));
              return Stream.fromIterable(
                turns[Math.min(index, turns.length - 1)]!,
              );
            }),
          ),
      });
    }),
  );

const differentCallProvider = (calls: { count: number }) =>
  Layer.effect(
    LanguageModel.LanguageModel,
    LanguageModel.make({
      generateText: () => Effect.succeed<Response.PartEncoded[]>([finish()]),
      streamText: () => {
        calls.count += 1;
        return Stream.fromIterable<Response.StreamPartEncoded>([
          {
            type: 'tool-call',
            id: 'different-call-id',
            name: 'lookup',
            params: { id: 'different' },
          },
          finish('tool-calls'),
        ]);
      },
    }),
  );

const lookup = Tool.make('lookup', {
  description: 'look an order up',
  parameters: Schema.Struct({ id: Schema.String }),
  success: Schema.Struct({ status: Schema.String, at: Schema.DateFromString }),
});

const CONVERSATION = 'dispatch-conversation';
const PATH = AgentLog.pathFor(CONVERSATION);
const WHEN = new Date('2026-01-02T03:04:05.000Z');
/** Deliberately not `WHEN`: a served result must be distinguishable. */
const RECORDED = new Date('2020-05-06T07:08:09.000Z');

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
        return { status: `fresh:${id}`, at: WHEN };
      }),
  });

const run = <A, E>(
  effect: Effect.Effect<A, E, LogStore.Service | LanguageModel.LanguageModel>,
  prompts: string[] = [],
): Promise<A> =>
  Effect.runPromise(
    effect.pipe(
      Effect.orDie,
      Effect.provide(scripted(prompts)),
      Effect.provide(LogStoreMemory.layer),
      Effect.scoped,
    ),
  );

/**
 * Write a previous run's records straight into the conversation.
 *
 * Deliberately not "run the agent and kill it": a crash has to leave the log
 * in an exact state for this to be testing the gate rather than testing
 * whatever a killed fiber happens to flush.
 */
const seed = Effect.fn('test.seed')(function* (
  records: ReadonlyArray<ConversationRecord.Record>,
) {
  const store = yield* LogStore.Service;
  yield* store.create(PATH, CONVERSATION).pipe(Effect.orDie);
  const claim = yield* store.acquire(PATH, 'previous-run').pipe(Effect.orDie);
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

const started: ConversationRecord.Record = {
  _tag: 'RunStarted',
  agent: 'test',
  formatVersion: 1,
  agentRevision: '1',
  prompt: [],
};

const called: ConversationRecord.Record = {
  _tag: 'ToolCall',
  step: 1,
  id: CALL_ID,
  name: 'lookup',
  params: { id: '42' },
};

const toolStarted: ConversationRecord.Record = {
  _tag: 'ToolStarted',
  id: CALL_ID,
  name: 'lookup',
};

const outcome: ConversationRecord.Record = {
  _tag: 'ToolOutcome',
  step: 1,
  id: CALL_ID,
  name: 'lookup',
  outcome: 'success',
  result: { status: 'from-log', at: RECORDED.toISOString() },
};

const settledRun: ConversationRecord.Record = {
  _tag: 'RunSettled',
  outcome: 'success',
  detail: '',
  steps: 2,
  usage: { input: 0, output: 0 },
};

const outcomesOf = (records: ReadonlyArray<ConversationRecord.Envelope>) =>
  records.flatMap((envelope) =>
    envelope.record._tag === 'ToolOutcome' ? [envelope.record] : [],
  );

const readAll = Effect.fn('test.readAll')(function* () {
  const store = yield* LogStore.Service;
  const page = yield* store.read(PATH, { limit: 1000 }).pipe(Effect.orDie);
  return page.records;
});

describe('responsive cancellation arbitration', () => {
  it('prevents dispatch when cancellation wins first', async () => {
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const arbitration = yield* ToolDispatch.makeTurnArbitration;
        yield* arbitration.cancel;
        return {
          dispatched: yield* arbitration.dispatchCommits,
        };
      }),
    );

    expect(result).toEqual({ dispatched: false });
  });

  it('defers cancellation when dispatch commits first', async () => {
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const arbitration = yield* ToolDispatch.makeTurnArbitration;
        const dispatched = yield* arbitration.dispatchCommits;
        const cancelled = yield* Ref.make(false);
        const cancelling = yield* Effect.forkChild(
          arbitration.cancel.pipe(Effect.andThen(Ref.set(cancelled, true))),
        );
        yield* Effect.yieldNow;
        const beforeSettlement = yield* Ref.get(cancelled);
        yield* arbitration.settled;
        yield* Fiber.join(cancelling);
        return {
          dispatched,
          beforeSettlement,
        };
      }),
    );

    expect(result.dispatched).toBe(true);
    expect(result.beforeSettlement).toBe(false);
  });
});

describe('recovering a tool call from the log', () => {
  it('does not re-run a tool an unsettled run already settled', async () => {
    const ran = { count: 0 };

    const written = await run(
      Effect.gen(function* () {
        yield* seed([started, called, outcome]);
        yield* agentWith(ran)
          .recordingTo(CONVERSATION)
          .run('hi')
          .pipe(Effect.orDie);
        return yield* readAll();
      }),
    );

    expect(ran.count).toBe(0);
    // The new run recorded the outcome it was served, not one it produced.
    expect(outcomesOf(written).at(-1)).toMatchObject({
      id: CALL_ID,
      result: { status: 'from-log' },
    });
  });

  it('shows the model the recovered result, not a fresh one', async () => {
    const ran = { count: 0 };
    const prompts: string[] = [];

    await run(
      Effect.gen(function* () {
        yield* seed([started, called, outcome]);
        yield* agentWith(ran)
          .recordingTo(CONVERSATION)
          .run('hi')
          .pipe(Effect.orDie);
      }),
      prompts,
    );

    // The second turn's prompt carries the tool result message. It is the
    // encoded result that lands there, which is why the log stores that form.
    expect(prompts[1]).toContain('from-log');
    expect(prompts[1]).not.toContain('fresh:');
  });

  it('re-runs the tool once the earlier run has settled', async () => {
    const ran = { count: 0 };

    const written = await run(
      Effect.gen(function* () {
        yield* seed([started, called, outcome, settledRun]);
        yield* agentWith(ran)
          .recordingTo(CONVERSATION)
          .run('hi')
          .pipe(Effect.orDie);
        return yield* readAll();
      }),
    );

    // A conversation whose last run finished has nothing to recover. Serving
    // its outcomes to a later run would answer a *new* question with an old
    // answer, which is worse than running the tool twice.
    expect(ran.count).toBe(1);
    expect(outcomesOf(written).at(-1)).toMatchObject({
      result: { status: 'fresh:42' },
    });
  });

  it('dispatches normally on a conversation with no history', async () => {
    const ran = { count: 0 };

    const written = await run(
      Effect.gen(function* () {
        yield* agentWith(ran)
          .recordingTo(CONVERSATION)
          .run('hi')
          .pipe(Effect.orDie);
        return yield* readAll();
      }),
    );

    expect(ran.count).toBe(1);
    expect(outcomesOf(written)).toHaveLength(1);
  });

  it('will not serve an outcome recorded under a different tool name', async () => {
    const ran = { count: 0 };

    await run(
      Effect.gen(function* () {
        yield* seed([
          started,
          called,
          { ...outcome, _tag: 'ToolOutcome', name: 'something_else' },
        ]);
        yield* agentWith(ran)
          .recordingTo(CONVERSATION)
          .run('hi')
          .pipe(Effect.orDie);
      }),
    );

    expect(ran.count).toBe(1);
  });
});

describe('recovering indeterminate tool execution', () => {
  it('lets a queued cancel win before an indeterminate retry has side effects', async () => {
    const ran = { count: 0 };
    const prompts: string[] = [];

    const result = await run(
      Effect.gen(function* () {
        yield* seed([started, called, toolStarted]);
        yield* AgentSignals.send(CONVERSATION, {
          kind: 'cancel',
          text: 'stop before retry',
          source: 'test',
        });
        return yield* agentWith(ran)
          .intercepting({
            onIndeterminateToolCall: () => Effect.succeed(Interception.retry),
          })
          .resume(CONVERSATION, 'hi');
      }),
      prompts,
    );

    expect(result.outcome).toBe('cancelled');
    expect(ran.count).toBe(0);
    expect(prompts).toEqual([]);
  });

  it('lets a cancel arriving during recovery win before dispatch commits', async () => {
    const ran = { count: 0 };

    const result = await run(
      Effect.gen(function* () {
        yield* seed([started, called, toolStarted]);
        return yield* agentWith(ran)
          .intercepting({
            onIndeterminateToolCall: () =>
              AgentSignals.send(CONVERSATION, {
                kind: 'cancel',
                text: 'stop during recovery',
                source: 'test',
              }).pipe(
                Effect.orDie,
                Effect.andThen(Effect.sleep(20)),
                Effect.as(Interception.retry),
              ),
          })
          .resume(CONVERSATION, 'hi');
      }),
    );

    expect(result.outcome).toBe('cancelled');
    expect(ran.count).toBe(0);
  });

  it('interrupts an indeterminate retry at the root deadline', async () => {
    const entered = { count: 0 };
    const hanging = Agent.make({
      name: 'test',
      revision: '1',
      instructions: 'be terse',
      toolkit: Toolkit.make(lookup),
      runPolicy: { wallClockMillis: 100 },
    }).withHandlers({
      lookup: () =>
        Effect.sync(() => {
          entered.count += 1;
        }).pipe(Effect.andThen(Effect.never)),
    });

    const exit = await run(
      Effect.gen(function* () {
        yield* seed([started, called, toolStarted]);
        return yield* hanging
          .intercepting({
            onIndeterminateToolCall: () => Effect.succeed(Interception.retry),
          })
          .resume(CONVERSATION, 'hi')
          .pipe(Effect.exit);
      }),
    );

    expect(exit._tag).toBe('Failure');
    expect(String(exit)).toContain('deadline');
    expect(entered.count).toBe(1);
  });

  it('fails before a provider can emit a different call id without a resolver', async () => {
    const calls = { count: 0 };
    const ran = { count: 0 };

    const exit = await Effect.runPromise(
      Effect.gen(function* () {
        yield* seed([started, called, toolStarted]);
        return yield* agentWith(ran)
          .resume(CONVERSATION, 'hi')
          .pipe(Effect.exit);
      }).pipe(
        Effect.provide(differentCallProvider(calls)),
        Effect.provide(LogStoreMemory.layer),
        Effect.scoped,
      ),
    );

    expect(exit._tag).toBe('Failure');
    expect(calls.count).toBe(0);
    expect(ran.count).toBe(0);
  });

  it('fails safely when ToolStarted has no original ToolCall', async () => {
    const calls = { count: 0 };
    const ran = { count: 0 };
    const resolved = { count: 0 };

    const exit = await Effect.runPromise(
      Effect.gen(function* () {
        yield* seed([started, toolStarted]);
        return yield* agentWith(ran)
          .intercepting({
            onIndeterminateToolCall: () => {
              resolved.count += 1;
              return Effect.succeed(Interception.retry);
            },
          })
          .resume(CONVERSATION, 'hi')
          .pipe(Effect.exit);
      }).pipe(
        Effect.provide(differentCallProvider(calls)),
        Effect.provide(LogStoreMemory.layer),
        Effect.scoped,
      ),
    );

    expect(exit._tag).toBe('Failure');
    expect(String(exit)).toContain('has no matching ToolCall');
    expect(calls.count).toBe(0);
    expect(ran.count).toBe(0);
    expect(resolved.count).toBe(0);
  });

  it('recovers when durable ToolStarted precedes its matching ToolCall', async () => {
    const resolved: string[] = [];

    await run(
      Effect.gen(function* () {
        yield* seed([started, toolStarted, called]);
        yield* agentWith({ count: 0 })
          .intercepting({
            onIndeterminateToolCall: (call) => {
              resolved.push(call.toolCallId!);
              return Effect.succeed(
                Interception.reconcile({
                  status: 'confirmed',
                  at: RECORDED.toISOString(),
                }),
              );
            },
          })
          .resume(CONVERSATION, 'hi');
      }),
    );

    expect(resolved).toEqual([CALL_ID]);
  });

  it('resolves multiple orphaned calls in original ToolCall order', async () => {
    const seen: string[] = [];
    const secondCall: ConversationRecord.Record = {
      ...called,
      id: 'call-2',
      params: { id: '43' },
    };
    const secondStart: ConversationRecord.Record = {
      ...toolStarted,
      id: 'call-2',
    };

    await run(
      Effect.gen(function* () {
        yield* seed([started, called, secondCall, secondStart, toolStarted]);
        yield* agentWith({ count: 0 })
          .intercepting({
            onIndeterminateToolCall: (call) => {
              seen.push(call.toolCallId!);
              return Effect.succeed(
                Interception.reconcile({
                  status: `confirmed:${String((call.params as { id: string }).id)}`,
                  at: RECORDED.toISOString(),
                }),
              );
            },
          })
          .resume(CONVERSATION, 'hi');
      }),
    );

    expect(seen).toEqual([CALL_ID, 'call-2']);
  });

  it('persists ToolStarted after dispatch commits and leaves a crashing dispatch orphaned', async () => {
    const mutations = { count: 0 };
    const sawStarted = { value: false };
    const trackedLookup = Tool.make('lookup', {
      description: 'look an order up',
      parameters: Schema.Struct({ id: Schema.String }),
      success: Schema.Struct({
        status: Schema.String,
        at: Schema.DateFromString,
      }),
      dependencies: [LogStore.Service],
    });
    const crashing = Agent.make({
      name: 'test',
      revision: '1',
      instructions: 'be terse',
      toolkit: Toolkit.make(trackedLookup),
    }).withHandlers({
      lookup: () =>
        Effect.gen(function* () {
          const records = yield* readAll();
          sawStarted.value = records.some(
            ({ record }) =>
              record._tag === 'ToolStarted' && record.id === CALL_ID,
          );
          mutations.count += 1;
          return yield* Effect.die(
            new Error('process lost after dispatch commits'),
          );
        }),
    });

    const records = await run(
      Effect.gen(function* () {
        yield* crashing.recordingTo(CONVERSATION).run('hi').pipe(Effect.exit);
        return yield* readAll();
      }),
    );

    expect(sawStarted.value).toBe(true);
    expect(mutations.count).toBe(1);
    expect(records.some(({ record }) => record._tag === 'ToolOutcome')).toBe(
      false,
    );
    expect(records.some(({ record }) => record._tag === 'RunSettled')).toBe(
      false,
    );
  });

  it('does not re-run by default, even when beforeToolCall dispatches', async () => {
    const ran = { count: 0 };

    const result = await run(
      Effect.gen(function* () {
        yield* seed([started, called, toolStarted]);
        const exit = yield* agentWith(ran)
          .intercepting({
            beforeToolCall: () => Effect.succeed(Interception.dispatch),
          })
          .resume(CONVERSATION, 'hi')
          .pipe(Effect.exit);
        return { exit, records: yield* readAll() };
      }),
    );

    expect(result.exit._tag).toBe('Failure');
    expect(ran.count).toBe(0);
    expect(
      result.records.some(({ record }) => record._tag === 'RunSettled'),
    ).toBe(false);
  });

  it('runs only after an explicit Retry decision', async () => {
    const ran = { count: 0 };
    const resolved = { count: 0 };
    const prompts: string[] = [];

    const records = await run(
      Effect.gen(function* () {
        yield* seed([started, called, toolStarted]);
        yield* agentWith(ran)
          .intercepting({
            onIndeterminateToolCall: (call) => {
              resolved.count += 1;
              expect(call).toMatchObject({
                name: 'lookup',
                toolCallId: CALL_ID,
                params: { id: '42' },
              });
              return Effect.succeed(Interception.retry);
            },
          })
          .resume(CONVERSATION, 'hi');
        return yield* readAll();
      }),
      prompts,
    );

    expect(ran.count).toBe(1);
    expect(resolved.count).toBe(1);
    expect(prompts[0]).toContain('fresh:42');
    expect(
      records.filter(({ record }) => record._tag === 'ToolStarted'),
    ).toHaveLength(2);
    expect(records.at(-1)?.record._tag).toBe('RunSettled');
  });

  it('reconciles with an explicit Answer without dispatching', async () => {
    const ran = { count: 0 };
    const prompts: string[] = [];

    const records = await run(
      Effect.gen(function* () {
        yield* seed([started, called, toolStarted]);
        yield* agentWith(ran)
          .intercepting({
            onIndeterminateToolCall: (call) => {
              expect(call.toolCallId).toBe(CALL_ID);
              expect(call.params).toEqual({ id: '42' });
              return Effect.succeed(
                Interception.reconcile({
                  status: 'confirmed',
                  at: RECORDED.toISOString(),
                }),
              );
            },
          })
          .resume(CONVERSATION, 'hi');
        return yield* readAll();
      }),
      prompts,
    );

    expect(ran.count).toBe(0);
    expect(prompts[0]).toContain('confirmed');
    expect(outcomesOf(records).at(-1)?.result).toMatchObject({
      status: 'confirmed',
    });
    expect(records.at(-1)?.record._tag).toBe('RunSettled');
  });

  it('rejects a reconciliation answer outside the tool result schema', async () => {
    const ran = { count: 0 };

    await expect(
      run(
        Effect.gen(function* () {
          yield* seed([started, called, toolStarted]);
          yield* agentWith(ran)
            .intercepting({
              onIndeterminateToolCall: () =>
                Effect.succeed(Interception.reconcile({ staleShape: true })),
            })
            .resume(CONVERSATION, 'hi');
        }),
      ),
    ).rejects.toThrow('does not match its current result schema');
    expect(ran.count).toBe(0);
  });

  it('keeps a crashed explicit retry indeterminate for the next resume', async () => {
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

    const records = await run(
      Effect.gen(function* () {
        yield* seed([started, called, toolStarted]);
        yield* crashing
          .intercepting({
            onIndeterminateToolCall: () => Effect.succeed(Interception.retry),
          })
          .resume(CONVERSATION, 'hi')
          .pipe(Effect.exit);
        yield* agentWith({ count: 0 })
          .intercepting({
            onIndeterminateToolCall: (call) => {
              expect(call.toolCallId).toBe(CALL_ID);
              return Effect.succeed(
                Interception.reconcile({
                  status: 'externally-confirmed',
                  at: RECORDED.toISOString(),
                }),
              );
            },
          })
          .resume(CONVERSATION, 'hi');
        return yield* readAll();
      }),
    );

    expect(mutations.count).toBe(1);
    expect(outcomesOf(records).at(-1)?.id).toBe(CALL_ID);
  });
});

describe('what a tool outcome stores', () => {
  it('settles normally after the durable outcome resolves its start', async () => {
    const ran = { count: 0 };

    const records = await run(
      Effect.gen(function* () {
        yield* agentWith(ran).recordingTo(CONVERSATION).run('hi');
        return yield* readAll();
      }),
    );

    const tags = records.map(({ record }) => record._tag);
    expect(tags.indexOf('ToolStarted')).toBeLessThan(
      tags.indexOf('ToolOutcome'),
    );
    expect(tags.at(-1)).toBe('RunSettled');
  });

  it('stores the toolkit’s encoding, not the handler’s value', async () => {
    const ran = { count: 0 };

    const written = await run(
      Effect.gen(function* () {
        yield* agentWith(ran)
          .recordingTo(CONVERSATION)
          .run('hi')
          .pipe(Effect.orDie);
        return yield* readAll();
      }),
    );

    // `at` is a `Date` in the handler and an ISO string in the log, because
    // the encoded form is the one the provider is shown and the only one a
    // resuming dispatch can serve back unchanged.
    expect(outcomesOf(written)[0]?.result).toEqual({
      status: 'fresh:42',
      at: WHEN.toISOString(),
    });
  });

  it('decodes a served result back through the tool’s schema', async () => {
    const ran = { count: 0 };
    const seen: unknown[] = [];
    const sources: unknown[] = [];

    await run(
      Effect.gen(function* () {
        yield* seed([started, called, outcome]);
        yield* agentWith(ran)
          .recordingTo(CONVERSATION)
          .stream('hi')
          .pipe(
            Stream.runForEach((event) =>
              Effect.sync(() => {
                if (
                  event._tag === 'Part' &&
                  (event.part as Response.StreamPartEncoded).type ===
                    'tool-result'
                ) {
                  sources.push(
                    (event.part as { readonly resultSource?: unknown })
                      .resultSource,
                  );
                  seen.push(
                    (event.part as unknown as { result: { at: unknown } })
                      .result.at,
                  );
                }
              }),
            ),
            Effect.orDie,
          );
      }),
    );

    // A consumer of the live stream reads the decoded half of a tool result.
    // Serving the stored JSON there would hand it a string where its type
    // says `Date` — so the stored value goes back through the tool's own
    // codec first.
    expect(seen).toEqual([RECORDED]);
    expect(sources).toEqual(['substituted']);
  });

  it('fails safely when a recovered result no longer matches the schema', async () => {
    const ran = { count: 0 };
    const encoded = { staleShape: true };

    await expect(
      run(
        Effect.gen(function* () {
          yield* seed([started, called, { ...outcome, result: encoded }]);
          yield* agentWith(ran)
            .recordingTo(CONVERSATION)
            .stream('hi')
            .pipe(Stream.runDrain);
        }),
      ),
    ).rejects.toThrow('does not match its current result schema');

    expect(ran.count).toBe(0);
  });
});
