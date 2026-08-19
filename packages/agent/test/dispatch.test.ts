import { LogStoreMemory } from '@sunfall/vesper-log/layer-memory';
import { LogStore } from '@sunfall/vesper-log/log-store';
import type { ConversationRecord } from '@sunfall/vesper-log/record';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';
import * as NodeServices from '@effect/platform-node/NodeServices';
import { describe, expect, it, test } from '@effect/vitest';
import {
  Crypto,
  Effect,
  Fiber,
  Layer,
  Option,
  Ref,
  Schema,
  Stream,
} from 'effect';
import {
  AiError,
  LanguageModel,
  type Response,
  Tool,
  Toolkit,
} from 'effect/unstable/ai';

import { Agent } from '../src/agent.js';
import { Conversation } from '../src/conversation.js';
import { Interception } from '../src/interception.js';
import * as AgentLog from '../src/log.js';
import { ToolDispatch } from '../src/dispatch.js';

const testLogLayer = Layer.mergeAll(
  LogStoreMemory.layer.pipe(Layer.provide(NodeServices.layer)),
  NodeServices.layer,
);

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

const lookupWithTypedFailure = Tool.make('lookup', {
  description: 'look an order up',
  parameters: Schema.Struct({ id: Schema.String }),
  success: Schema.Struct({ status: Schema.String, at: Schema.DateFromString }),
  failure: Schema.Struct({ code: Schema.String }),
});

const CONVERSATION = LogVocabulary.ConversationId.make('dispatch-conversation');
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
  effect: Effect.Effect<
    A,
    E,
    LogStore.Service | LanguageModel.LanguageModel | Crypto.Crypto
  >,
  prompts: string[] = [],
): Effect.Effect<A> =>
  effect.pipe(
    Effect.orDie,
    Effect.provide(scripted(prompts)),
    Effect.provide(testLogLayer),
    Effect.scoped,
  );

const runQuiet = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.exit(effect);

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

const started: ConversationRecord.Record = {
  _tag: 'RunStarted',
  agent: 'test',
  formatVersion: 1,
  agentRevision: LogVocabulary.AgentRevision.make('1'),
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
  it.effect('prevents dispatch when cancellation wins first', () =>
    Effect.gen(function* () {
      const arbitration = yield* ToolDispatch.makeTurnArbitration;
      yield* arbitration.cancel;
      const result = {
        dispatched: Option.isSome(yield* arbitration.commit),
      };

      expect(result).toEqual({ dispatched: false });
    }),
  );

  it.effect('defers cancellation when dispatch commits first', () =>
    Effect.gen(function* () {
      const arbitration = yield* ToolDispatch.makeTurnArbitration;
      const permit = yield* arbitration.commit;
      const cancelled = yield* Ref.make(false);
      const cancelling = yield* Effect.forkChild(
        arbitration.cancel.pipe(Effect.andThen(Ref.set(cancelled, true))),
      );
      yield* Effect.yieldNow;
      const beforeSettlement = yield* Ref.get(cancelled);
      expect(Option.isSome(permit)).toBe(true);
      if (Option.isSome(permit)) yield* permit.value.settle;
      yield* Fiber.join(cancelling);
      const result = {
        dispatched: Option.isSome(permit),
        beforeSettlement,
      };

      expect(result.dispatched).toBe(true);
      expect(result.beforeSettlement).toBe(false);
    }),
  );

  it.effect('settles each committed dispatch at most once', () =>
    Effect.gen(function* () {
      const arbitration = yield* ToolDispatch.makeTurnArbitration;
      const first = yield* arbitration.commit;
      const second = yield* arbitration.commit;
      expect(Option.isSome(first)).toBe(true);
      expect(Option.isSome(second)).toBe(true);
      if (Option.isNone(first) || Option.isNone(second)) return;

      yield* first.value.settle;
      yield* first.value.settle;

      const cancelled = yield* Ref.make(false);
      const cancelling = yield* Effect.forkChild(
        arbitration.cancel.pipe(Effect.andThen(Ref.set(cancelled, true))),
      );
      yield* Effect.yieldNow;
      expect(yield* Ref.get(cancelled)).toBe(false);

      yield* second.value.settle;
      yield* Fiber.join(cancelling);
      expect(yield* Ref.get(cancelled)).toBe(true);
    }),
  );

  it.effect('settles a dispatch whose durable start append fails', () =>
    Effect.gen(function* () {
      const arbitration = yield* ToolDispatch.makeTurnArbitration;
      const toolkit = Toolkit.make(lookup);
      const opened = yield* AgentLog.open(CONVERSATION, {
        compatibility: {
          agent: 'test',
          revision: LogVocabulary.AgentRevision.make('1'),
        },
      });
      const session: AgentLog.Session = {
        ...opened,
        append: () => Effect.die('ToolStarted append failed'),
      };
      const gated = yield* ToolDispatch.gate(toolkit, {
        agent: 'test',
        arbitration,
        session,
      }).pipe(
        Effect.provide(
          toolkit.toLayer({
            lookup: () => Effect.succeed({ status: 'ok', at: WHEN }),
          }),
        ),
      );
      const failed = yield* gated
        .handle('lookup', { id: '42' }, 'append-fails')
        .pipe(Effect.exit);
      expect(failed._tag).toBe('Failure');

      const cancelled = yield* arbitration.cancel.pipe(
        Effect.timeout('100 millis'),
        Effect.exit,
      );
      expect(cancelled._tag).toBe('Success');
    }).pipe(Effect.provide(testLogLayer)),
  );
});

describe('recovering a tool call from the log', () => {
  it.effect('does not re-run a tool an unsettled run already settled', () =>
    Effect.gen(function* () {
      const ran = { count: 0 };

      const written = yield* run(
        Effect.gen(function* () {
          yield* seed([started, called, outcome]);
          yield* Conversation.make(agentWith(ran), CONVERSATION)
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
    }),
  );

  it.effect(
    'serves a spilled result on recovery by its pointer shape, not the tool schema',
    () =>
      Effect.gen(function* () {
        const ran = { count: 0 };
        // `lookup`'s own success schema is `{ status, at }` — nothing like a
        // pointer. `ResultOverflow.wrap` is what would have produced this
        // shape live; seeding it directly exercises recovery without
        // depending on the overflow module being wired into this agent.
        const pointerOutcome: ConversationRecord.Record = {
          _tag: 'ToolOutcome',
          step: 1,
          id: CALL_ID,
          name: 'lookup',
          outcome: 'success',
          result: {
            _tag: 'ToolResultOverflow',
            attachmentId: `sha256:${'a'.repeat(64)}`,
            byteLength: 5_000,
            mediaType: 'text/plain; charset=utf-8',
            preview: 'the record this settled call actually produced',
          },
        };

        const written = yield* run(
          Effect.gen(function* () {
            yield* seed([started, called, pointerOutcome]);
            yield* Conversation.make(agentWith(ran), CONVERSATION)
              .run('hi')
              .pipe(Effect.orDie);
            return yield* readAll();
          }),
        );

        // Decoding the pointer against `lookup`'s schema would fail, which
        // would make a conversation containing a spilled result unresumable.
        expect(ran.count).toBe(0);
        expect(outcomesOf(written).at(-1)).toMatchObject({
          id: CALL_ID,
          result: { _tag: 'ToolResultOverflow' },
        });
      }),
  );

  it.effect('shows the model the recovered result, not a fresh one', () =>
    Effect.gen(function* () {
      const ran = { count: 0 };
      const prompts: string[] = [];

      yield* run(
        Effect.gen(function* () {
          yield* seed([started, called, outcome]);
          yield* Conversation.make(agentWith(ran), CONVERSATION)
            .run('hi')
            .pipe(Effect.orDie);
        }),
        prompts,
      );

      // The second turn's prompt carries the tool result message. It is the
      // encoded result that lands there, which is why the log stores that form.
      expect(prompts[1]).toContain('from-log');
      expect(prompts[1]).not.toContain('fresh:');
    }),
  );

  it.effect(
    'serves a recovered failure as an AiError even for a tool without failureMode: return',
    () =>
      Effect.gen(function* () {
        const ran = { count: 0 };
        // `lookup` defaults to `failureMode: 'error'`, so its own declared
        // failure schema is never part of what a served result may decode
        // as. A recorded failure that reached the log some other way than
        // the handler itself — here, standing in for a denied `needsApproval`
        // gate or an interceptor's `Answer` — still has to be servable.
        const denied = yield* Schema.encodeUnknownEffect(AiError.AiError)(
          new AiError.AiError({
            module: 'test',
            method: 'deny',
            reason: new AiError.UnknownError({ description: 'denied' }),
          }),
        );

        const written = yield* run(
          Effect.gen(function* () {
            yield* seed([
              started,
              called,
              { ...outcome, outcome: 'failure', result: denied },
            ]);
            yield* Conversation.make(agentWith(ran), CONVERSATION)
              .run('hi')
              .pipe(Effect.orDie);
            return yield* readAll();
          }),
        );

        // Not re-run, and the recovered failure round-tripped rather than
        // failing to decode — the new run recorded the outcome it was
        // served, not a decode error.
        expect(ran.count).toBe(0);
        expect(outcomesOf(written).at(-1)).toMatchObject({
          id: CALL_ID,
          outcome: 'failure',
          result: denied,
        });
      }),
  );

  it.effect('re-runs the tool once the earlier run has settled', () =>
    Effect.gen(function* () {
      const ran = { count: 0 };

      const written = yield* run(
        Effect.gen(function* () {
          yield* seed([started, called, outcome, settledRun]);
          yield* Conversation.make(agentWith(ran), CONVERSATION)
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
    }),
  );

  it.effect('dispatches normally on a conversation with no history', () =>
    Effect.gen(function* () {
      const ran = { count: 0 };

      const written = yield* run(
        Effect.gen(function* () {
          yield* Conversation.make(agentWith(ran), CONVERSATION)
            .run('hi')
            .pipe(Effect.orDie);
          return yield* readAll();
        }),
      );

      expect(ran.count).toBe(1);
      expect(outcomesOf(written)).toHaveLength(1);
    }),
  );

  it.effect(
    'will not serve an outcome recorded under a different tool name',
    () =>
      Effect.gen(function* () {
        const ran = { count: 0 };

        yield* run(
          Effect.gen(function* () {
            yield* seed([
              started,
              called,
              { ...outcome, _tag: 'ToolOutcome', name: 'something_else' },
            ]);
            yield* Conversation.make(agentWith(ran), CONVERSATION)
              .run('hi')
              .pipe(Effect.orDie);
          }),
        );

        expect(ran.count).toBe(1);
      }),
  );
});

describe('recovering indeterminate tool execution', () => {
  it.effect(
    'lets a queued cancel win before an indeterminate retry has side effects',
    () =>
      Effect.gen(function* () {
        const ran = { count: 0 };
        const prompts: string[] = [];

        const result = yield* run(
          Effect.gen(function* () {
            yield* seed([started, called, toolStarted]);
            const conversation = Conversation.make(
              agentWith(ran),
              CONVERSATION,
            );
            yield* conversation.send({
              kind: 'cancel',
              text: 'stop before retry',
              source: 'test',
            });
            return yield* Conversation.make(
              agentWith(ran).intercepting({
                onIndeterminateToolCall: () =>
                  Effect.succeed(Interception.retry),
              }),
              CONVERSATION,
            ).run('hi');
          }),
          prompts,
        );

        expect(result.outcome).toBe('cancelled');
        expect(ran.count).toBe(0);
        expect(prompts).toEqual([]);
      }),
  );

  it.effect(
    'lets a cancel arriving during recovery win before dispatch commits',
    () =>
      Effect.gen(function* () {
        const ran = { count: 0 };

        const result = yield* run(
          Effect.gen(function* () {
            yield* seed([started, called, toolStarted]);
            const conversation = Conversation.make(
              agentWith(ran).intercepting({
                onIndeterminateToolCall: () =>
                  Conversation.make(agentWith(ran), CONVERSATION)
                    .send({
                      kind: 'cancel',
                      text: 'stop during recovery',
                      source: 'test',
                    })
                    .pipe(
                      Effect.orDie,
                      // Stay inside recovery until the signal watcher observes
                      // the durable cancel and interrupts the run. Returning a
                      // retry here would reintroduce a timing race in the test.
                      Effect.andThen(Effect.never),
                    ),
              }),
              CONVERSATION,
            );
            return yield* conversation.run('hi');
          }),
        );

        expect(result.outcome).toBe('cancelled');
        expect(ran.count).toBe(0);
      }),
  );

  it.live('interrupts an indeterminate retry at the root deadline', () =>
    Effect.gen(function* () {
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

      const exit = yield* run(
        Effect.gen(function* () {
          yield* seed([started, called, toolStarted]);
          return yield* Conversation.make(
            hanging.intercepting({
              onIndeterminateToolCall: () => Effect.succeed(Interception.retry),
            }),
            CONVERSATION,
          )
            .run('hi')
            .pipe(Effect.result);
        }),
      );

      expect(exit._tag).toBe('Failure');
      expect(String(exit)).toContain('deadline');
      expect(entered.count).toBe(1);
    }),
  );

  it.effect(
    'fails before a provider can emit a different call id without a resolver',
    () =>
      Effect.gen(function* () {
        const calls = { count: 0 };
        const ran = { count: 0 };

        const exit = yield* Effect.gen(function* () {
          yield* seed([started, called, toolStarted]);
          return yield* Conversation.make(agentWith(ran), CONVERSATION)
            .run('hi')
            .pipe(Effect.result);
        }).pipe(
          Effect.provide(differentCallProvider(calls)),
          Effect.provide(testLogLayer),
          Effect.scoped,
        );

        expect(exit._tag).toBe('Failure');
        expect(calls.count).toBe(0);
        expect(ran.count).toBe(0);
      }),
  );

  it.effect('fails safely when ToolStarted has no original ToolCall', () =>
    Effect.gen(function* () {
      const calls = { count: 0 };
      const ran = { count: 0 };
      const resolved = { count: 0 };

      const exit = yield* Effect.gen(function* () {
        yield* seed([started, toolStarted]);
        return yield* Conversation.make(
          agentWith(ran).intercepting({
            onIndeterminateToolCall: () => {
              resolved.count += 1;
              return Effect.succeed(Interception.retry);
            },
          }),
          CONVERSATION,
        )
          .run('hi')
          .pipe(Effect.result);
      }).pipe(
        Effect.provide(differentCallProvider(calls)),
        Effect.provide(testLogLayer),
        Effect.scoped,
      );

      expect(exit._tag).toBe('Failure');
      expect(String(exit)).toContain('has no matching ToolCall');
      expect(calls.count).toBe(0);
      expect(ran.count).toBe(0);
      expect(resolved.count).toBe(0);
    }),
  );

  it.effect(
    'recovers when durable ToolStarted precedes its matching ToolCall',
    () =>
      Effect.gen(function* () {
        const resolved: string[] = [];

        yield* run(
          Effect.gen(function* () {
            yield* seed([started, toolStarted, called]);
            yield* Conversation.make(
              agentWith({ count: 0 }).intercepting({
                onIndeterminateToolCall: (call) => {
                  resolved.push(call.toolCallId!);
                  return Effect.succeed(
                    Interception.reconcile({
                      status: 'confirmed',
                      at: RECORDED.toISOString(),
                    }),
                  );
                },
              }),
              CONVERSATION,
            ).run('hi');
          }),
        );

        expect(resolved).toEqual([CALL_ID]);
      }),
  );

  it.effect('resolves multiple orphaned calls in original ToolCall order', () =>
    Effect.gen(function* () {
      const seen: string[] = [];
      const secondCall: ConversationRecord.Record = {
        ...called,
        id: LogVocabulary.ToolCallId.make('call-2'),
        params: { id: '43' },
      };
      const secondStart: ConversationRecord.Record = {
        ...toolStarted,
        id: LogVocabulary.ToolCallId.make('call-2'),
      };

      yield* run(
        Effect.gen(function* () {
          yield* seed([started, called, secondCall, secondStart, toolStarted]);
          yield* Conversation.make(
            agentWith({ count: 0 }).intercepting({
              onIndeterminateToolCall: (call) => {
                seen.push(call.toolCallId!);
                return Effect.succeed(
                  Interception.reconcile({
                    status: `confirmed:${String((call.params as { id: string }).id)}`,
                    at: RECORDED.toISOString(),
                  }),
                );
              },
            }),
            CONVERSATION,
          ).run('hi');
        }),
      );

      expect(seen).toEqual([CALL_ID, LogVocabulary.ToolCallId.make('call-2')]);
    }),
  );

  quiet(
    'persists ToolStarted after dispatch commits and leaves a crashing dispatch orphaned',
    ({ disableErrorReporting: _disableErrorReporting }) =>
      runQuiet(
        Effect.gen(function* () {
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

          const records = yield* run(
            Effect.gen(function* () {
              yield* Conversation.make(crashing, CONVERSATION)
                .run('hi')
                .pipe(Effect.result);
              return yield* readAll();
            }),
          );

          expect(sawStarted.value).toBe(true);
          expect(mutations.count).toBe(1);
          expect(
            records.some(({ record }) => record._tag === 'ToolOutcome'),
          ).toBe(false);
          expect(
            records.some(({ record }) => record._tag === 'RunSettled'),
          ).toBe(false);
        }),
      ),
  );

  it.effect(
    'does not re-run by default, even when beforeToolCall dispatches',
    () =>
      Effect.gen(function* () {
        const ran = { count: 0 };

        const result = yield* run(
          Effect.gen(function* () {
            yield* seed([started, called, toolStarted]);
            const exit = yield* Conversation.make(
              agentWith(ran).intercepting({
                beforeToolCall: () => Effect.succeed(Interception.dispatch),
              }),
              CONVERSATION,
            )
              .run('hi')
              .pipe(Effect.result);
            return { exit, records: yield* readAll() };
          }),
        );

        expect(result.exit._tag).toBe('Failure');
        expect(ran.count).toBe(0);
        expect(
          result.records.some(({ record }) => record._tag === 'RunSettled'),
        ).toBe(false);
      }),
  );

  it.effect('runs only after an explicit Retry decision', () =>
    Effect.gen(function* () {
      const ran = { count: 0 };
      const resolved = { count: 0 };
      const prompts: string[] = [];

      const records = yield* run(
        Effect.gen(function* () {
          yield* seed([started, called, toolStarted]);
          yield* Conversation.make(
            agentWith(ran).intercepting({
              onIndeterminateToolCall: (call) => {
                resolved.count += 1;
                expect(call).toMatchObject({
                  name: 'lookup',
                  toolCallId: CALL_ID,
                  params: { id: '42' },
                });
                return Effect.succeed(Interception.retry);
              },
            }),
            CONVERSATION,
          ).run('hi');
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
    }),
  );

  it.effect('normalizes a typed handler failure during retry', () =>
    Effect.gen(function* () {
      const failing = Agent.make({
        name: 'test',
        revision: '1',
        instructions: 'be terse',
        toolkit: Toolkit.make(lookupWithTypedFailure),
      }).withHandlers({
        lookup: () => Effect.fail({ code: 'typed-failure' }),
      });

      const exit = yield* run(
        Effect.gen(function* () {
          yield* seed([started, called, toolStarted]);
          return yield* Conversation.make(
            failing.intercepting({
              onIndeterminateToolCall: () => Effect.succeed(Interception.retry),
            }),
            CONVERSATION,
          )
            .run('hi')
            .pipe(Effect.result);
        }),
      );

      expect(exit._tag).toBe('Failure');
      if (exit._tag === 'Failure') {
        expect(AiError.isAiError(exit.failure)).toBe(true);
      }
    }),
  );

  it.effect('reconciles with an explicit Answer without dispatching', () =>
    Effect.gen(function* () {
      const ran = { count: 0 };
      const prompts: string[] = [];

      const records = yield* run(
        Effect.gen(function* () {
          yield* seed([started, called, toolStarted]);
          yield* Conversation.make(
            agentWith(ran).intercepting({
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
            }),
            CONVERSATION,
          ).run('hi');
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
    }),
  );

  quiet(
    'rejects a reconciliation answer outside the tool result schema',
    ({ disableErrorReporting: _disableErrorReporting }) =>
      runQuiet(
        Effect.gen(function* () {
          const ran = { count: 0 };

          const result = yield* run(
            Effect.gen(function* () {
              yield* seed([started, called, toolStarted]);
              yield* Conversation.make(
                agentWith(ran).intercepting({
                  onIndeterminateToolCall: () =>
                    Effect.succeed(
                      Interception.reconcile({ staleShape: true }),
                    ),
                }),
                CONVERSATION,
              ).run('hi');
            }),
          ).pipe(Effect.result);
          expect(String(result)).toContain(
            'does not match its current result schema',
          );
          expect(ran.count).toBe(0);
        }),
      ),
  );

  quiet(
    'keeps a crashed explicit retry indeterminate for the next resume',
    ({ disableErrorReporting: _disableErrorReporting }) =>
      runQuiet(
        Effect.gen(function* () {
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

          const records = yield* run(
            Effect.gen(function* () {
              yield* seed([started, called, toolStarted]);
              yield* Conversation.make(
                crashing.intercepting({
                  onIndeterminateToolCall: () =>
                    Effect.succeed(Interception.retry),
                }),
                CONVERSATION,
              )
                .run('hi')
                .pipe(Effect.result);
              yield* Conversation.make(
                agentWith({ count: 0 }).intercepting({
                  onIndeterminateToolCall: (call) => {
                    expect(call.toolCallId).toBe(CALL_ID);
                    return Effect.succeed(
                      Interception.reconcile({
                        status: 'externally-confirmed',
                        at: RECORDED.toISOString(),
                      }),
                    );
                  },
                }),
                CONVERSATION,
              ).run('hi');
              return yield* readAll();
            }),
          );

          expect(mutations.count).toBe(1);
          expect(outcomesOf(records).at(-1)?.id).toBe(CALL_ID);
        }),
      ),
  );
});

describe('what a tool outcome stores', () => {
  it.effect(
    'settles normally after the durable outcome resolves its start',
    () =>
      Effect.gen(function* () {
        const ran = { count: 0 };

        const records = yield* run(
          Effect.gen(function* () {
            yield* Conversation.make(agentWith(ran), CONVERSATION).run('hi');
            return yield* readAll();
          }),
        );

        const tags = records.map(({ record }) => record._tag);
        expect(tags.indexOf('ToolStarted')).toBeLessThan(
          tags.indexOf('ToolOutcome'),
        );
        expect(tags.at(-1)).toBe('RunSettled');
      }),
  );

  it.effect('stores the toolkit’s encoding, not the handler’s value', () =>
    Effect.gen(function* () {
      const ran = { count: 0 };

      const written = yield* run(
        Effect.gen(function* () {
          yield* Conversation.make(agentWith(ran), CONVERSATION)
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
    }),
  );

  it.effect('decodes a served result back through the tool’s schema', () =>
    Effect.gen(function* () {
      const ran = { count: 0 };
      const seen: unknown[] = [];
      const sources: unknown[] = [];

      yield* run(
        Effect.gen(function* () {
          yield* seed([started, called, outcome]);
          yield* Conversation.make(agentWith(ran), CONVERSATION)
            .stream('hi')
            .pipe(
              Stream.runForEach((event) =>
                Effect.sync(() => {
                  if (
                    event._tag === 'Part' &&
                    event.part.type === 'tool-result' &&
                    typeof event.part.result === 'object' &&
                    event.part.result !== null &&
                    'at' in event.part.result
                  ) {
                    sources.push(
                      'resultSource' in event.part
                        ? event.part.resultSource
                        : undefined,
                    );
                    seen.push(event.part.result.at);
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
    }),
  );

  quiet(
    'fails safely when a recovered result no longer matches the schema',
    ({ disableErrorReporting: _disableErrorReporting }) =>
      runQuiet(
        Effect.gen(function* () {
          const ran = { count: 0 };
          const encoded = { staleShape: true };

          const result = yield* run(
            Effect.gen(function* () {
              yield* seed([started, called, { ...outcome, result: encoded }]);
              yield* Conversation.make(agentWith(ran), CONVERSATION)
                .stream('hi')
                .pipe(Stream.runDrain);
            }),
          ).pipe(Effect.result);
          expect(String(result)).toContain(
            'does not match its current result schema',
          );

          expect(ran.count).toBe(0);
        }),
      ),
  );
});
