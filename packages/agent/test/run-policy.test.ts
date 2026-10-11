import type { LogStore } from '@sunfall/vesper-log/log-store';
import type { ConversationRecord } from '@sunfall/vesper-log/record';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';
import { describe, expect, it } from '@effect/vitest';
import {
  type Crypto,
  Deferred,
  Effect,
  Fiber,
  Layer,
  Ref,
  Schema,
  Stream,
} from 'effect';
import * as NodeServices from '@effect/platform-node/NodeServices';
import { LanguageModel, Tool, Toolkit } from 'effect/ai';

import { Agent } from '../src/agent.js';
import { Conversation } from '../src/conversation.js';
import { RunPolicy } from '../src/run-policy.js';
import { Stop } from '../src/stop.js';
import {
  callTool,
  finish,
  model,
  type Model,
  says,
  scriptedLayer,
} from './signals-fixtures.js';

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

// Hard run policy and its audit. Limits are shared by a root run and every
// descendant loop; the audit recomputes consumption from the log alone.

/** Run and render the failure (asserting there is one). */
const failedWith = <A, E>(
  effect: Effect.Effect<A, E, LanguageModel.LanguageModel | Crypto.Crypto>,
  scripted: ReturnType<typeof model>,
) =>
  Effect.exit(effect).pipe(
    Effect.provide(Layer.merge(scripted.layer, NodeServices.layer)),
    Effect.map((exit) => {
      expect(exit._tag).toBe('Failure');
      return String(exit);
    }),
  );

const plain = (name: string, runPolicy: Partial<RunPolicy.Limits>) =>
  Agent.make({
    name,
    revision: '1',
    instructions: 'answer',
    toolkit: Toolkit.make(),
    runPolicy,
  });

const work = Tool.make('work', {
  description: 'block until released',
  parameters: Schema.Struct({}),
  success: Schema.Struct({ done: Schema.Boolean }),
});
/** Handler that records how many run at once. */
const gauge = () => {
  const active = { now: 0, peak: 0 };
  const entered = Deferred.makeUnsafe<void>();
  return {
    active,
    entered,
    handler: (release: Deferred.Deferred<void> | undefined) => () =>
      Effect.gen(function* () {
        active.now += 1;
        active.peak = Math.max(active.peak, active.now);
        yield* Deferred.succeed(entered, undefined);
        yield* release === undefined
          ? Effect.sleep(30)
          : Deferred.await(release);
        active.now -= 1;
        return { done: true };
      }),
  };
};

const delegation = (index: number) =>
  index === 0
    ? [
        callTool('call-a', 'task_researcher', { prompt: 'look it up' }),
        finish('tool-calls'),
      ]
    : says(index === 1 ? 'found' : 'done');

const edit = (
  records: ReadonlyArray<ConversationRecord.Envelope>,
  change: (record: ConversationRecord.Record) => ConversationRecord.Record,
) =>
  records.map(({ offset, conversationId, timestamp, record }) => ({
    offset,
    conversationId,
    timestamp,
    record: change(record),
  }));

const limitOf = (
  audited: RunPolicy.RunAudit,
  limit: RunPolicy.AuditedLimit,
) => {
  const found = audited.limits.find((entry) => entry.limit === limit);
  if (found === undefined) {
    throw new Error(`audit has no ${limit} entry`);
  }
  return found;
};
const onlyRun = (report: RunPolicy.Audit): RunPolicy.RunAudit => {
  expect(report.runs).toHaveLength(1);
  const first = report.runs.at(0);
  if (first === undefined) {
    throw new Error('audit has no runs');
  }
  return first;
};

describe('hard run policy', () => {
  it('rejects unknown runtime limit fields when an agent is defined', () => {
    const limits = { maxTurns: 1, misspelledLimit: 1 };
    expect(() =>
      Agent.make({
        name: 'bad',
        revision: '1',
        instructions: 'x',
        toolkit: Toolkit.make(),
        runPolicy: limits,
      }),
    ).toThrow('RunPolicy.misspelledLimit is not a recognized limit');
  });

  it.effect('does not let a steer override the turn ceiling', () =>
    Effect.gen(function* () {
      const bound = Conversation.make(plain('turns', { maxTurns: 1 }), 'turns');
      const scripted = model();
      const rendered = yield* run(
        Effect.gen(function* () {
          yield* bound.send({ kind: 'steer', text: 'more', source: 'op' });
          return yield* bound.run('go').pipe(
            Effect.exit,
            Effect.map((exit) => String(exit)),
          );
        }),
        scripted,
      );
      expect(rendered).toContain('turns');
      expect(scripted.prompts).toHaveLength(1);
    }),
  );

  it.effect('counts model calls independently of soft steps', () =>
    Effect.gen(function* () {
      const scripted = model();
      const guarded = Agent.make({
        name: 'guarded',
        revision: '1',
        instructions: 'continue',
        toolkit: Toolkit.make(),
        stopWhen: Stop.maxSteps(3),
        runPolicy: { maxModelCalls: 1 },
      });
      expect(yield* failedWith(guarded.run('go'), scripted)).toContain(
        'model_calls',
      );
      expect(scripted.prompts).toHaveLength(1);
    }),
  );

  it.effect(
    'fails once provider-reported cumulative tokens cross a limit',
    () =>
      Effect.gen(function* () {
        const rendered = yield* failedWith(
          plain('tokens', { maxInputTokens: 2 }).run('go'),
          model(),
        );
        expect(rendered).toContain('input_tokens');
      }),
  );

  it.live(
    'interrupts a blocked model call at the wall-clock deadline',
    () =>
      Effect.gen(function* () {
        const blocked = Layer.effect(
          LanguageModel.LanguageModel,
          LanguageModel.make({
            generateText: () => Effect.never,
            streamText: () => Stream.never,
          }),
        );
        const rendered = yield* plain('deadline', { wallClockMillis: 20 })
          .run('go')
          .pipe(
            Effect.provide(blocked),
            Effect.exit,
            Effect.map((exit) => String(exit)),
          );
        expect(rendered).toContain('deadline');
      }),
    2_000,
  );

  it.effect('bounds per-signal, per-boundary, and cumulative steer bytes', () =>
    Effect.gen(function* () {
      const reasonFor = (
        id: string,
        runPolicy: Partial<RunPolicy.Limits>,
        sends: ReadonlyArray<string>,
      ) =>
        run(
          Effect.gen(function* () {
            const bound = Conversation.make(plain(id, runPolicy), id);
            for (const text of sends) {
              yield* bound.send({ kind: 'steer', text, source: 'op' });
            }
            const events = yield* bound.stream('go').pipe(Stream.runCollect);
            return Array.from(events).flatMap((event) =>
              event._tag === 'SignalRejected' ? [event.reason] : [],
            );
          }),
          model(),
        );
      expect(
        yield* reasonFor('cumulative', { maxSteeredBytes: 3 }, ['four']),
      ).toEqual(['steered_bytes']);
      expect(
        yield* reasonFor('oversized', { maxSignalBytes: 4 }, ['large']),
      ).toEqual(['signal_bytes']);
      expect(
        yield* reasonFor('backlog', { maxSignalsPerBoundary: 1 }, ['x', 'y']),
      ).not.toContain('signal_bytes');
    }),
  );
});

describe('hard run policy across delegation', () => {
  const child = Agent.make({
    name: 'child',
    revision: '1',
    instructions: 'answer',
    toolkit: Toolkit.make(),
  });

  it.effect('cannot be bypassed by opening a descendant agent loop', () =>
    Effect.gen(function* () {
      const parent = Agent.make({
        name: 'parent',
        revision: '1',
        instructions: 'delegate',
        toolkit: Toolkit.make(),
        subagents: [child],
        runPolicy: { maxModelCalls: 1 },
      });
      const scripted = model(() => [
        callTool('child-call', 'task_child', { prompt: 'work' }),
        finish('tool-calls'),
      ]);
      expect(yield* failedWith(parent.run('go'), scripted)).toContain(
        'model_calls',
      );
      expect(scripted.prompts).toHaveLength(1);
    }),
  );

  it.live(
    'shares delegation counts and child concurrency across breadth',
    () =>
      Effect.gen(function* () {
        const meter = gauge();
        const worker = Agent.make({
          name: 'child-worker',
          revision: '1',
          instructions: 'ROLE-CHILD',
          toolkit: Toolkit.make(work),
        }).withHandlers({ work: meter.handler(undefined) });
        const plan = { tasks: 2 };
        const breadth = Agent.make({
          name: 'breadth',
          revision: '1',
          instructions: 'ROLE-PARENT',
          toolkit: Toolkit.make(),
          subagents: [worker],
          runPolicy: { maxDelegatedTasks: 2, maxConcurrentChildren: 1 },
        });
        const scripted = model((_, prompt) => {
          if (prompt.includes('ROLE-CHILD')) {
            return prompt.includes('tool-result')
              ? says('child done')
              : [
                  callTool(`w-${String(prompt.length)}`, 'work'),
                  finish('tool-calls'),
                ];
          }
          return prompt.includes('tool-result')
            ? says('parent done')
            : [
                ...Array.from({ length: plan.tasks }, (_unused, i) =>
                  callTool(`d${String(i)}`, 'task_child-worker', {
                    prompt: 'x',
                  }),
                ),
                finish('tool-calls'),
              ];
        });
        const attempt = (tasks: number) => {
          plan.tasks = tasks;
          return breadth
            .run('go')
            .pipe(
              Effect.provide(Layer.merge(scripted.layer, NodeServices.layer)),
              Effect.exit,
            );
        };
        // Two sibling children share one slot: they run one at a time...
        expect((yield* attempt(2))._tag).toBe('Success');
        expect(meter.active.peak).toBe(1);
        // ...and a third delegation exceeds the shared task count.
        expect(String(yield* attempt(3))).toContain('delegated_tasks');
      }),
    5_000,
  );

  it.live(
    'holds one leaf permit across parent and child handlers',
    () =>
      Effect.gen(function* () {
        const meter = gauge();
        const release = yield* Deferred.make<void>();
        const handler = meter.handler(release);
        const worker = Agent.make({
          name: 'child-worker',
          revision: '1',
          instructions: 'work',
          toolkit: Toolkit.make(work),
        }).withHandlers({ work: handler });
        const parent = Agent.make({
          name: 'parent-worker',
          revision: '1',
          instructions: 'work and delegate',
          toolkit: Toolkit.make(work),
          subagents: [worker],
          runPolicy: { maxToolConcurrency: 1 },
        }).withHandlers({ work: handler });
        const scripted = model((index) =>
          index === 0
            ? [
                callTool('parent-leaf', 'work'),
                callTool('delegate', 'task_child-worker', { prompt: 'work' }),
                finish('tool-calls'),
              ]
            : index === 1
              ? [callTool('child-leaf', 'work'), finish('tool-calls')]
              : says('done'),
        );
        yield* Effect.gen(function* () {
          const running = yield* Effect.forkChild(parent.run('go'));
          yield* Deferred.await(meter.entered);
          yield* Effect.sleep(20);
          expect(meter.active.peak).toBe(1);
          yield* Deferred.succeed(release, undefined);
          yield* Fiber.join(running);
        }).pipe(
          Effect.provide(Layer.merge(scripted.layer, NodeServices.layer)),
        );
        expect(meter.active.peak).toBe(1);
      }),
    2_000,
  );

  it.effect('releases the leaf permit after a failed tool call', () =>
    Effect.gen(function* () {
      const flaky = Tool.make('flaky', {
        description: 'fails',
        parameters: Schema.Struct({}),
        success: Schema.Struct({ done: Schema.Boolean }),
        failure: Schema.Struct({ reason: Schema.String }),
        failureMode: 'return',
      });
      const ran = yield* Ref.make(0);
      const limited = Agent.make({
        name: 'limited',
        revision: '1',
        instructions: 'x',
        toolkit: Toolkit.make(flaky, work),
        runPolicy: { maxToolConcurrency: 1 },
      }).withHandlers({
        flaky: () => Effect.fail({ reason: 'nope' }),
        work: () =>
          Ref.update(ran, (n) => n + 1).pipe(Effect.as({ done: true })),
      });
      const scripted = model((index) =>
        index === 0
          ? [
              callTool('a', 'flaky'),
              callTool('b', 'work'),
              callTool('c', 'work'),
              finish('tool-calls'),
            ]
          : says('done'),
      );
      const result = yield* limited
        .run('go')
        .pipe(
          Effect.provide(Layer.merge(scripted.layer, NodeServices.layer)),
          Effect.timeout('2 seconds'),
        );
      expect(result.text).toBe('done');
      expect(yield* Ref.get(ran)).toBe(2);
    }),
  );
});

describe('streaming tool handlers', () => {
  const chatty = Tool.make('chatty', {
    description: 'report progress, then keep working',
    parameters: Schema.Struct({}),
    success: Schema.Struct({ done: Schema.Boolean }),
  });

  // Effect AI buffers a handler's preliminary results in its own unbounded
  // queue, so backpressure is observable only as the run not advancing past a
  // stalled consumer, never as the handler being paused mid-emission.
  it.live(
    'a stalled consumer holds the run open and interruption releases the handler',
    () =>
      Effect.gen(function* () {
        const released = yield* Deferred.make<void>();
        const gate = yield* Deferred.make<void>();
        const seen = yield* Deferred.make<void>();
        const agent = Agent.make({
          name: 'streamer',
          revision: '1',
          instructions: 'go',
          toolkit: Toolkit.make(chatty),
        }).withHandlers({
          chatty: (_, context) =>
            context
              .preliminary({ done: false })
              .pipe(
                Effect.andThen(Effect.never),
                Effect.ensuring(Deferred.succeed(released, undefined)),
              ),
        });
        const scripted = model(() => [
          callTool('c1', 'chatty'),
          finish('tool-calls'),
        ]);
        const consumer = yield* run(
          agent.stream('go').pipe(
            Stream.mapEffect((event) =>
              event._tag === 'Part' &&
              event.part.type === 'tool-result' &&
              event.part.preliminary
                ? Deferred.succeed(seen, undefined).pipe(
                    Effect.andThen(Deferred.await(gate)),
                  )
                : Effect.void,
            ),
            Stream.runDrain,
          ),
          scripted,
        ).pipe(Effect.forkChild);

        yield* Deferred.await(seen);
        yield* Effect.sleep(50);
        expect(scripted.prompts).toHaveLength(1);
        expect(yield* Deferred.isDone(released)).toBe(false);

        yield* Fiber.interrupt(consumer);
        yield* Deferred.await(released).pipe(Effect.timeout('2 seconds'));
      }),
    5_000,
  );
});

// Budget audit: the ceiling is recorded into `RunStarted`, and a reader with
// only the log recomputes consumption and judges it. Real conversations give
// the recorded fields the runtime writes; edited copies of their records give
// the shapes a real run cannot produce.
describe('budget audit', () => {
  const ROOT = LogVocabulary.ConversationId.make('audited-root');
  const researcher = Agent.make({
    name: 'researcher',
    revision: '1',
    instructions: 'Answer concisely.',
    toolkit: Toolkit.make(),
  });
  const supervisorWith = (runPolicy: Partial<RunPolicy.Limits>) =>
    Agent.make({
      name: 'supervisor',
      revision: '1',
      instructions: 'delegate',
      toolkit: Toolkit.make(),
      subagents: [researcher],
      runPolicy,
    });
  const runTree = (runPolicy: Partial<RunPolicy.Limits>) =>
    run(
      Effect.gen(function* () {
        yield* Conversation.make(supervisorWith(runPolicy), ROOT)
          .run('go')
          .pipe(Effect.exit);
        return yield* RunPolicy.auditStored(ROOT);
      }),
      model(delegation),
    );

  /** The root conversation's own records from a single-agent run. */
  const recordsOf = (runPolicy: Partial<RunPolicy.Limits>) =>
    run(
      Effect.gen(function* () {
        const bound = Conversation.make(plain('solo', runPolicy), ROOT);
        yield* bound.run('go').pipe(Effect.exit);
        return Array.from(yield* bound.records().pipe(Stream.runCollect));
      }),
      model(),
    );

  it.effect(
    'recomputes exact totals across the tree and records the limits',
    () =>
      Effect.gen(function* () {
        const report = yield* runTree({ maxInputTokens: 100, maxTurns: 9 });
        const audited = onlyRun(report);
        expect(report.verdict).toBe('compliant');
        expect(audited.gaps).toEqual([]);
        // Parent and child draw from one budget recorded on every run.
        expect(audited.conversations).toHaveLength(2);
        // Parent turn, child turn, parent turn: 5 in / 2 out each.
        expect(limitOf(audited, 'input_tokens')).toMatchObject({
          ceiling: 100,
          used: 15,
          overshoot: 0,
          status: 'within',
        });
        expect(limitOf(audited, 'output_tokens')).toMatchObject({
          ceiling: RunPolicy.defaultLimits.maxOutputTokens,
          used: 6,
        });
        expect(limitOf(audited, 'turns')).toMatchObject({
          ceiling: 9,
          used: 3,
        });
        expect(limitOf(audited, 'delegated_tasks')).toMatchObject({ used: 1 });
        expect(audited.unaudited).toContain('deadline');
      }),
  );

  it.effect('counts subagent usage toward the root and shows the stop', () =>
    Effect.gen(function* () {
      // Parent 5 + child 5 = 10 > 8, though neither exceeds 8 alone.
      const audited = onlyRun(yield* runTree({ maxInputTokens: 8 }));
      expect(audited.outcome).toBe('failure');
      expect(audited.stoppedBy).toEqual(['input_tokens']);
      expect(limitOf(audited, 'input_tokens')).toMatchObject({
        used: 10,
        overshoot: 2,
        // One in-flight turn per conversation may land past the ceiling.
        overshootBound: 10,
        status: 'stopped',
      });
      expect(audited.gaps).toEqual([]);
      expect(audited.verdict).toBe('compliant');
    }),
  );

  it.effect('never counts a missing child log as compliance', () =>
    Effect.gen(function* () {
      const rootOnly = yield* run(
        Effect.gen(function* () {
          const bound = Conversation.make(
            supervisorWith({ maxInputTokens: 8 }),
            ROOT,
          );
          yield* bound.run('go').pipe(Effect.exit);
          return Array.from(yield* bound.records().pipe(Stream.runCollect));
        }),
        model(delegation),
      );
      const report = RunPolicy.audit(rootOnly);
      expect(limitOf(onlyRun(report), 'input_tokens').used).toBe(5);
      expect(onlyRun(report).gaps.map((gap) => gap.kind)).toContain(
        'child-log-missing',
      );
      expect(report.verdict).toBe('unprovable');
    }),
  );

  it.effect(
    'judges edited logs: violations, missing evidence, false claims',
    () =>
      Effect.gen(function* () {
        const within = yield* recordsOf({ maxInputTokens: 100 });
        const stopped = yield* recordsOf({ maxInputTokens: 3 });
        expect(RunPolicy.audit(within).verdict).toBe('compliant');

        // Spend above a ceiling in a run that was never stopped.
        const lowered = RunPolicy.audit(
          edit(within, (record) =>
            record._tag === 'RunStarted'
              ? {
                  ...record,
                  budget: {
                    ...record.budget,
                    limits: { ...record.budget.limits, maxInputTokens: 3 },
                  },
                }
              : record,
          ),
        );
        expect(limitOf(onlyRun(lowered), 'input_tokens')).toMatchObject({
          used: 5,
          overshoot: 2,
          status: 'violated',
        });
        expect(lowered.verdict).toBe('breached');

        // A stop is read from the structured evidence, not the failure message.
        const unstructured = RunPolicy.audit(
          edit(stopped, (record) => {
            if (record._tag !== 'RunSettled') {
              return record;
            }
            const { stoppedBy: _dropped, ...rest } = record;
            return rest;
          }),
        );
        expect(onlyRun(unstructured).stoppedBy).toEqual([]);
        expect(limitOf(onlyRun(unstructured), 'input_tokens').status).toBe(
          'violated',
        );

        // A stop that claims more usage than the log holds.
        const inflated = RunPolicy.audit(
          edit(stopped, (record) =>
            record._tag === 'RunSettled' && record.stoppedBy !== undefined
              ? { ...record, stoppedBy: { ...record.stoppedBy, used: 30 } }
              : record,
          ),
        );
        expect(onlyRun(inflated).gaps).toEqual([
          {
            kind: 'unlogged-usage',
            limit: 'input_tokens',
            claimed: 30,
            recomputed: 5,
          },
        ]);
        expect(inflated.verdict).toBe('unprovable');

        // A stop excuses overshoot only up to what one turn could spend.
        const twoTurns = yield* run(
          Effect.gen(function* () {
            const bound = Conversation.make(
              Agent.make({
                name: 'two-turns',
                revision: '1',
                instructions: 'go',
                toolkit: Toolkit.make(work),
              }).withHandlers({ work: () => Effect.succeed({ done: true }) }),
              ROOT,
            );
            yield* bound.run('go').pipe(Effect.exit);
            return Array.from(yield* bound.records().pipe(Stream.runCollect));
          }),
          model((index) =>
            index === 0
              ? [callTool('c1', 'work'), finish('tool-calls')]
              : says('done'),
          ),
        );
        const stoppedAt = (ceiling: number) =>
          RunPolicy.audit(
            edit(twoTurns, (record) => {
              if (record._tag === 'RunStarted') {
                return {
                  ...record,
                  budget: {
                    ...record.budget,
                    limits: {
                      ...record.budget.limits,
                      maxInputTokens: ceiling,
                    },
                  },
                };
              }
              return record._tag === 'RunSettled'
                ? {
                    ...record,
                    outcome: 'failure',
                    stoppedBy: {
                      limit: 'input_tokens',
                      used: 10,
                      ceiling,
                    },
                  }
                : record;
            }),
          );
        // Two turns of 5: ceiling 6 overshoots by 4, within one turn's 5.
        expect(limitOf(onlyRun(stoppedAt(6)), 'input_tokens')).toMatchObject({
          overshoot: 4,
          overshootBound: 5,
          status: 'stopped',
        });
        // Ceiling 3 overshoots by 7: no single turn explains it.
        expect(limitOf(onlyRun(stoppedAt(3)), 'input_tokens')).toMatchObject({
          overshoot: 7,
          overshootBound: 5,
          status: 'violated',
        });

        // An unsettled run is a gap, not a pass; no records is unprovable.
        const unsettled = RunPolicy.audit(
          within.filter(({ record }) => record._tag !== 'RunSettled'),
        );
        expect(onlyRun(unsettled).gaps).toEqual([
          { kind: 'run-unsettled', conversationId: ROOT },
        ]);
        expect(unsettled.verdict).toBe('unprovable');
        expect(RunPolicy.audit([]).verdict).toBe('unprovable');
      }),
  );
});
