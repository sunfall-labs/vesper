import { LogStoreMemory } from '@sunfall/vesper-log/layer-memory';
import { LogStore } from '@sunfall/vesper-log/log-store';
import { LogOffset } from '@sunfall/vesper-log/offset';
import type { ConversationRecord } from '@sunfall/vesper-log/record';
import { Effect, Layer } from 'effect';
import { describe, expect, it } from 'vitest';

import { AgentLog } from '../src/log.js';

interface Reads {
  calls: number;
  records: number;
}

const counted = (reads: Reads): Layer.Layer<LogStore.Service> =>
  Layer.effect(
    LogStore.Service,
    Effect.gen(function* () {
      const store = yield* LogStore.Service;
      return LogStore.Service.of({
        ...store,
        readBackwards: (path, options) =>
          store.readBackwards(path, options).pipe(
            Effect.tap((page) =>
              Effect.sync(() => {
                reads.calls += 1;
                reads.records += page.records.length;
              }),
            ),
          ),
      });
    }),
  ).pipe(Layer.provide(LogStoreMemory.layer));

const text = (
  step: number,
  value = `text-${step}`,
): ConversationRecord.Record => ({ _tag: 'Text', step, text: value });

const settled = (
  usage = { input: 10, output: 5 },
  signalCursor = LogOffset.START,
): ConversationRecord.Record => ({
  _tag: 'RunSettled',
  outcome: 'success',
  detail: '',
  steps: 1,
  usage,
  resume: {
    formatVersion: 1,
    agent: 'test',
    agentRevision: '1',
    usage,
    signalCursor,
  },
});

const compacted = (firstKept: LogOffset.Offset): ConversationRecord.Record => ({
  _tag: 'Compacted',
  formatVersion: 1,
  agent: 'test',
  agentRevision: '1',
  step: 1,
  summary: 'summary',
  firstKept,
  summarizedMessages: 9_980,
  keptMessages: 10,
});

const seed = Effect.fn('test.seedScaling')(function* (
  id: string,
  records: ReadonlyArray<ConversationRecord.Record>,
) {
  const store = yield* LogStore.Service;
  const path = AgentLog.pathFor(id);
  yield* store.create(path, id);
  const claim = yield* store.acquire(path, 'fixture');
  yield* store.append({
    path,
    producerId: claim.producerId,
    epoch: claim.epoch,
    sequence: 0,
    records: records.map((record) => ({
      conversationId: id,
      timestamp: 1_700_000_000_000,
      record,
    })),
  });
});

const reset = (reads: Reads): void => {
  reads.calls = 0;
  reads.records = 0;
};

describe('long conversation open structure', () => {
  it('opens 10k compacted lifetime records from bounded suffix pages', async () => {
    const reads = { calls: 0, records: 0 };
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const old = Array.from({ length: 9_980 }, (_, index) => text(index));
        const kept = Array.from({ length: 10 }, (_, index) =>
          text(9_980 + index),
        );
        yield* seed('fixed-tail', [
          ...old,
          ...kept,
          compacted(LogOffset.fromSeq(9_980n)),
          ...Array.from({ length: 5 }, (_, index) => text(10_000 + index)),
          settled(),
        ]);
        reset(reads);
        const session = yield* AgentLog.open('fixed-tail', {
          compatibility: { agent: 'test', revision: '1' },
        });
        return { retained: session.history.length, reads: { ...reads } };
      }).pipe(Effect.provide(counted(reads))),
    );

    expect(result.retained).toBe(17);
    // One bounded active-path read validates before producer acquisition; the
    // second rebuilds from the head after fencing any compatible predecessor.
    expect(result.reads.calls).toBe(3);
    expect(result.reads.records).toBeLessThanOrEqual(128);
  });

  it('jumps over an abandoned 10k-record branch to an older compaction', async () => {
    const reads = { calls: 0, records: 0 };
    const retained = await Effect.runPromise(
      Effect.gen(function* () {
        const prefix = [
          ...Array.from({ length: 100 }, (_, index) => text(index)),
          ...Array.from({ length: 10 }, (_, index) => text(100 + index)),
          compacted(LogOffset.fromSeq(100n)),
          text(111, 'kept-after-summary'),
          settled(),
        ];
        const abandoned = Array.from({ length: 10_000 }, (_, index) =>
          text(1_000 + index, 'abandoned'),
        );
        const laterUsage = { input: 20, output: 10 };
        yield* seed('branch-jump', [
          ...prefix,
          ...abandoned,
          settled(laterUsage),
          { _tag: 'BranchedFrom', at: LogOffset.fromSeq(111n) },
        ]);
        reset(reads);
        return (yield* AgentLog.open('branch-jump', {
          compatibility: { agent: 'test', revision: '1' },
        })).history;
      }).pipe(Effect.provide(counted(reads))),
    );

    expect(
      retained.some(
        ({ record }) => record._tag === 'Text' && record.text === 'abandoned',
      ),
    ).toBe(false);
    expect(reads.calls).toBeLessThanOrEqual(8);
    expect(reads.records).toBeLessThanOrEqual(256);
  });

  it('retains an orphan tool suffix after the latest resume aggregate', async () => {
    const reads = { calls: 0, records: 0 };
    const recovery = await Effect.runPromise(
      Effect.gen(function* () {
        yield* seed('orphan-suffix', [
          ...Array.from({ length: 9_990 }, (_, index) => text(index)),
          ...Array.from({ length: 5 }, (_, index) => text(9_990 + index)),
          compacted(LogOffset.fromSeq(9_990n)),
          settled(),
          {
            _tag: 'RunStarted',
            agent: 'test',
            formatVersion: 1,
            agentRevision: '1',
            prompt: [],
          },
          { _tag: 'ToolStarted', id: 'call-1', name: 'lookup' },
          { _tag: 'ToolStarted', id: 'call-2', name: 'lookup' },
          {
            _tag: 'ToolOutcome',
            step: 1,
            id: 'call-2',
            name: 'lookup',
            outcome: 'success',
            result: { status: 'durable' },
          },
        ]);
        reset(reads);
        const session = yield* AgentLog.open('orphan-suffix', {
          compatibility: { agent: 'test', revision: '1' },
        });
        return {
          indeterminate: session.recovery('lookup', 'call-1'),
          outcome: session.recovery('lookup', 'call-2'),
        };
      }).pipe(Effect.provide(counted(reads))),
    );

    expect(recovery.indeterminate).toMatchObject({
      _tag: 'Some',
      value: { _tag: 'Indeterminate' },
    });
    expect(recovery.outcome).toMatchObject({
      _tag: 'Some',
      value: {
        _tag: 'Settled',
        outcome: 'success',
        result: { status: 'durable' },
      },
    });
    expect(reads.calls).toBe(3);
    expect(reads.records).toBeLessThanOrEqual(128);
  });

  it('full-scans compatible history without an aggregate until a run settles', async () => {
    const reads = { calls: 0, records: 0 };
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const old = Array.from({ length: 9_980 }, (_, index) => text(index));
        const kept = Array.from({ length: 10 }, (_, index) =>
          text(9_980 + index),
        );
        yield* seed('no-aggregate', [
          {
            _tag: 'RunStarted',
            agent: 'test',
            formatVersion: 1,
            agentRevision: '1',
            prompt: [],
          },
          ...old,
          ...kept,
          compacted(LogOffset.fromSeq(9_981n)),
          {
            _tag: 'RunSettled',
            outcome: 'success',
            detail: '',
            steps: 1,
            usage: { input: 10, output: 5 },
          },
        ]);
        const store = yield* LogStore.Service;
        const path = AgentLog.pathFor('no-aggregate');
        const before = (yield* store.meta(path)).pipe((option) =>
          option._tag === 'Some' ? option.value.records : 0,
        );

        reset(reads);
        yield* AgentLog.open('no-aggregate', {
          compatibility: { agent: 'test', revision: '1' },
        });
        const first = { ...reads };

        reset(reads);
        yield* AgentLog.open('no-aggregate', {
          compatibility: { agent: 'test', revision: '1' },
        });
        const second = { ...reads };
        const after = (yield* store.meta(path)).pipe((option) =>
          option._tag === 'Some' ? option.value.records : 0,
        );
        return { before, after, first, second };
      }).pipe(Effect.provide(counted(reads))),
    );

    expect(result.first.records).toBeGreaterThan(19_000);
    expect(result.second.records).toBeGreaterThan(19_000);
    expect(result.after).toBe(result.before);
  });

  it('rejects unrevisioned history instead of self-upgrading it', async () => {
    const reads = { calls: 0, records: 0 };
    await expect(
      Effect.runPromise(
        Effect.gen(function* () {
          yield* seed('legacy', [
            { _tag: 'RunStarted', agent: 'test', prompt: [] },
            ...Array.from({ length: 9_990 }, (_, index) => text(index)),
            {
              _tag: 'RunSettled',
              outcome: 'success',
              detail: '',
              steps: 1,
              usage: { input: 10, output: 5 },
            },
          ]);
          reset(reads);
          yield* AgentLog.open('legacy', {
            compatibility: { agent: 'test', revision: '1' },
          });
        }).pipe(Effect.provide(counted(reads))),
      ),
    ).rejects.toThrow('predates explicit compatibility metadata');
    expect(reads.records).toBeGreaterThan(9_000);
  });
});
