import { LogStore } from '@sunfall/vesper-log/log-store';
import { LogOffset } from '@sunfall/vesper-log/offset';
import { LogStoreMemory } from '@sunfall/vesper-log/layer-memory';
import { ConversationRecord, FORMAT_VERSION } from '@sunfall/vesper-log/record';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';
import * as NodeCrypto from '@effect/platform-node/NodeCrypto';
import * as NodeServices from '@effect/platform-node/NodeServices';
import { describe, expect, it } from '@effect/vitest';
import { Effect, Layer, Ref, Schema, Stream } from 'effect';
import { LanguageModel, type Response, Toolkit } from 'effect/ai';

import { Agent } from '../src/agent.js';
import { Conversation } from '../src/conversation.js';
import * as AgentLog from '../src/log.js';
import { RunPolicy } from '../src/run-policy.js';

// Budget audit: the ceiling is recorded into `RunStarted`, and a reader with
// only the log recomputes consumption and judges it. These run real
// conversations (so the recorded fields are the ones the runtime writes) and
// hand-built logs (so the shapes a real run cannot produce are still judged).

const finish = (reason: 'stop' | 'tool-calls' = 'stop') => ({
  type: 'finish' as const,
  reason,
  usage: {
    inputTokens: { total: 5, uncached: 5, cacheRead: 0, cacheWrite: 0 },
    outputTokens: { total: 2 },
  },
});

const says = (text: string): Response.StreamPartEncoded[] => [
  { type: 'text-start', id: text },
  { type: 'text-delta', id: text, delta: text },
  { type: 'text-end', id: text },
  finish(),
];

const delegates = (id: string): Response.StreamPartEncoded[] => [
  {
    type: 'tool-call',
    id,
    name: 'task_researcher',
    params: { prompt: 'look it up' },
  },
  finish('tool-calls'),
];

/** One model for the whole tree, replying in script order. */
const scripted = (turns: ReadonlyArray<Response.StreamPartEncoded[]>) =>
  Layer.effect(
    LanguageModel.LanguageModel,
    Effect.gen(function* () {
      const calls = yield* Ref.make(0);
      return yield* LanguageModel.make({
        generateText: () => Effect.succeed<Response.PartEncoded[]>([finish()]),
        streamText: () =>
          Stream.unwrap(
            Effect.map(
              Ref.getAndUpdate(calls, (n) => n + 1),
              (index) =>
                Stream.fromIterable(
                  turns[Math.min(index, turns.length - 1)] ?? [],
                ),
            ),
          ),
      });
    }),
  );

const logLayer = Layer.mergeAll(
  LogStoreMemory.layer.pipe(Layer.provide(NodeCrypto.layer)),
  NodeServices.layer,
);

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

const ROOT = LogVocabulary.ConversationId.make('audited-root');

const delegation = [delegates('call-a'), says('found'), says('done')];

const runTree = (
  runPolicy: Partial<RunPolicy.Limits>,
  turns: ReadonlyArray<Response.StreamPartEncoded[]> = delegation,
) =>
  Effect.gen(function* () {
    yield* Conversation.make(supervisorWith(runPolicy), ROOT)
      .run('go')
      .pipe(Effect.exit);
    return yield* RunPolicy.auditStored(ROOT);
  }).pipe(
    Effect.orDie,
    Effect.provide(Layer.merge(scripted(turns), logLayer)),
    Effect.scoped,
  );

const limitOf = (run: RunPolicy.RunAudit, limit: RunPolicy.AuditedLimit) => {
  const found = run.limits.find((entry) => entry.limit === limit);
  if (found === undefined) {
    throw new Error(`audit has no ${limit} entry`);
  }
  return found;
};

const onlyRun = (report: RunPolicy.Audit): RunPolicy.RunAudit => {
  expect(report.runs).toHaveLength(1);
  const run = report.runs.at(0);
  if (run === undefined) {
    throw new Error('audit has no runs');
  }
  return run;
};

// Hand-built logs.
let seq = 0n;
const envelope = (
  conversationId: string,
  record: ConversationRecord.Record,
): ConversationRecord.Envelope => ({
  offset: LogOffset.fromSeq((seq += 1n)),
  conversationId: LogVocabulary.ConversationId.make(conversationId),
  timestamp: 0,
  record,
});

const started = (
  budget: ConversationRecord.RunBudget,
): ConversationRecord.Record => ({
  _tag: 'RunStarted',
  agent: 'a',
  formatVersion: FORMAT_VERSION,
  agentRevision: LogVocabulary.AgentRevision.make('1'),
  prompt: [],
  budget,
});

const turnFinished = (
  input: number,
  output = 0,
): ConversationRecord.Record => ({
  _tag: 'TurnFinished',
  step: 1,
  usage: { input, output },
});

const settled = (
  input: number,
  stoppedBy?: { limit: string; used: number; ceiling: number },
): ConversationRecord.Record => ({
  _tag: 'RunSettled',
  outcome: stoppedBy === undefined ? 'success' : 'failure',
  detail: stoppedBy === undefined ? '' : 'budget exhausted',
  ...(stoppedBy === undefined ? {} : { stoppedBy }),
  steps: 1,
  usage: { input, output: 0 },
});

const budget = (maxInputTokens: number): ConversationRecord.RunBudget => ({
  id: 'budget-1',
  root: LogVocabulary.ConversationId.make('c'),
  limits: { ...RunPolicy.defaultLimits, maxInputTokens },
});

describe('budget audit', () => {
  it.effect('records the effective limits on every run of the tree', () =>
    Effect.gen(function* () {
      const records = yield* Effect.gen(function* () {
        yield* Conversation.make(supervisorWith({ maxTurns: 9 }), ROOT)
          .run('go')
          .pipe(Effect.orDie);
        const store = yield* LogStore.Service;
        const read = (id: LogVocabulary.ConversationId) =>
          store.read(AgentLog.pathFor(id), { limit: 100 }).pipe(Effect.orDie);
        const root = (yield* read(ROOT)).records;
        const child = (yield* read(
          AgentLog.childIdFor(ROOT, LogVocabulary.ToolCallId.make('call-a')),
        )).records;
        return [...root, ...child];
      }).pipe(
        Effect.provide(Layer.merge(scripted(delegation), logLayer)),
        Effect.scoped,
      );

      const budgets = records.flatMap(({ record }) =>
        record._tag === 'RunStarted' ? [record.budget] : [],
      );
      expect(budgets).toHaveLength(2);
      expect(budgets[0]?.limits['maxTurns']).toBe(9);
      expect(budgets[0]?.root).toBe(ROOT);
      // The child draws from the same budget, not a fresh one.
      expect(budgets[1]).toEqual(budgets[0]);
    }),
  );

  it.effect('reports a run within budget with exact recomputed totals', () =>
    Effect.gen(function* () {
      const report = yield* runTree({ maxInputTokens: 100 });
      const run = onlyRun(report);
      expect(report.runs).toHaveLength(1);
      expect(report.verdict).toBe('compliant');
      expect(run.verdict).toBe('compliant');
      expect(run.gaps).toEqual([]);
      expect(run.conversations).toHaveLength(2);
      // Parent turn, child turn, parent turn: 5 in / 2 out each.
      expect(limitOf(run, 'input_tokens')).toMatchObject({
        ceiling: 100,
        used: 15,
        overshoot: 0,
        status: 'within',
      });
      expect(limitOf(run, 'output_tokens')).toMatchObject({
        ceiling: RunPolicy.defaultLimits.maxOutputTokens,
        used: 6,
        status: 'within',
      });
      expect(limitOf(run, 'turns')).toMatchObject({
        used: 3,
        status: 'within',
      });
      expect(limitOf(run, 'delegated_tasks')).toMatchObject({
        used: 1,
        status: 'within',
      });
      expect(run.unaudited).toContain('deadline');
    }),
  );

  it.effect('counts subagent usage toward the root and shows the stop', () =>
    Effect.gen(function* () {
      // Parent 5 + child 5 = 10 > 8, though neither exceeds 8 alone.
      const report = yield* runTree({ maxInputTokens: 8 });
      const run = onlyRun(report);
      expect(run.outcome).toBe('failure');
      expect(run.stoppedBy).toEqual(['input_tokens']);
      const input = limitOf(run, 'input_tokens');
      expect(input.used).toBe(10);
      expect(input.overshoot).toBe(2);
      // One in-flight turn per conversation may land past the ceiling.
      expect(input.overshootBound).toBe(10);
      expect(input.status).toBe('stopped');
      expect(run.gaps).toEqual([]);
      expect(run.verdict).toBe('compliant');
    }),
  );

  it.effect('never counts a missing child log as compliance', () =>
    Effect.gen(function* () {
      const store = yield* Effect.gen(function* () {
        yield* Conversation.make(supervisorWith({ maxInputTokens: 8 }), ROOT)
          .run('go')
          .pipe(Effect.exit);
        const service = yield* LogStore.Service;
        return (yield* service
          .read(AgentLog.pathFor(ROOT), { limit: 100 })
          .pipe(Effect.orDie)).records;
      }).pipe(
        Effect.provide(Layer.merge(scripted(delegation), logLayer)),
        Effect.scoped,
      );

      const report = RunPolicy.audit(store);
      const run = onlyRun(report);
      expect(limitOf(run, 'input_tokens').used).toBe(5);
      expect(run.gaps.map((gap) => gap.kind)).toContain('child-log-missing');
      expect(report.verdict).toBe('unprovable');
    }),
  );

  it('rejects a RunStarted that records no budget as malformed', () => {
    expect(() =>
      Schema.decodeUnknownSync(ConversationRecord.Record)({
        _tag: 'RunStarted',
        agent: 'a',
        formatVersion: FORMAT_VERSION,
        agentRevision: '1',
        prompt: [],
      }),
    ).toThrow(/budget/);
  });

  it('records the structured stop when the budget ends a run', () => {
    const stop = { limit: 'input_tokens', used: 11, ceiling: 8 };
    const report = RunPolicy.audit([
      envelope('c', started(budget(8))),
      envelope('c', turnFinished(11)),
      envelope('c', settled(11, stop)),
    ]);
    const run = onlyRun(report);
    expect(run.stoppedBy).toEqual(['input_tokens']);
    expect(limitOf(run, 'input_tokens').status).toBe('stopped');
    expect(report.verdict).toBe('compliant');
  });

  it('does not read stop evidence from the failure message', () => {
    const report = RunPolicy.audit([
      envelope('c', started(budget(10))),
      envelope('c', turnFinished(12)),
      envelope('c', {
        _tag: 'RunSettled',
        outcome: 'failure',
        detail:
          'RunPolicyExhausted: Hard run budget input_tokens exhausted (12/10)',
        steps: 1,
        usage: { input: 12, output: 0 },
      }),
    ]);
    const run = onlyRun(report);
    expect(run.stoppedBy).toEqual([]);
    expect(limitOf(run, 'input_tokens').status).toBe('violated');
  });

  it('reports no records as unprovable rather than compliant', () => {
    expect(RunPolicy.audit([]).verdict).toBe('unprovable');
  });

  it('flags usage above the ceiling in a run that was never stopped', () => {
    const report = RunPolicy.audit([
      envelope('c', started(budget(10))),
      envelope('c', turnFinished(5)),
      envelope('c', turnFinished(40)),
      envelope('c', settled(40)),
    ]);
    const input = limitOf(onlyRun(report), 'input_tokens');
    expect(input).toMatchObject({
      used: 40,
      overshoot: 30,
      status: 'violated',
    });
    expect(report.verdict).toBe('breached');
  });

  it('rejects an overshoot larger than any single turn could explain', () => {
    const stop = { limit: 'input_tokens', used: 40, ceiling: 10 };
    const report = RunPolicy.audit([
      envelope('c', started(budget(10))),
      envelope('c', turnFinished(20)),
      envelope('c', turnFinished(40)),
      envelope('c', settled(40, stop)),
    ]);
    // Each turn spent 20; 30 over the ceiling cannot be one turn's worth.
    expect(limitOf(onlyRun(report), 'input_tokens')).toMatchObject({
      overshoot: 30,
      overshootBound: 20,
      status: 'violated',
    });
  });

  it('treats an unsettled run as a gap, not a pass', () => {
    const report = RunPolicy.audit([
      envelope('c', started(budget(100))),
      envelope('c', turnFinished(5)),
    ]);
    expect(onlyRun(report).gaps).toEqual([
      { kind: 'run-unsettled', conversationId: 'c' },
    ]);
    expect(report.verdict).toBe('unprovable');
  });

  it('reports a stop that claims more usage than the log holds', () => {
    const stop = { limit: 'input_tokens', used: 30, ceiling: 10 };
    const report = RunPolicy.audit([
      envelope('c', started(budget(10))),
      envelope('c', turnFinished(5)),
      envelope('c', settled(5, stop)),
    ]);
    expect(onlyRun(report).gaps).toEqual([
      {
        kind: 'unlogged-usage',
        limit: 'input_tokens',
        claimed: 30,
        recomputed: 5,
      },
    ]);
    expect(report.verdict).toBe('unprovable');
  });
});
