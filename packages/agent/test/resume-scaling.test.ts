import { LogStore } from '@sunfall/vesper-log/log-store';
import { LogOffset } from '@sunfall/vesper-log/offset';
import type { ConversationRecord } from '@sunfall/vesper-log/record';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';
import * as NodeServices from '@effect/platform-node/NodeServices';
import { describe, expect, it } from '@effect/vitest';
import { Effect, Layer } from 'effect';

import * as AgentLog from '../src/log.js';
import {
  callId,
  lookupOutcome,
  lookupStarted,
  openSession,
  said,
  seed,
  settled,
  started,
  testLogLayer,
} from './durability-fixtures.js';

// Opening a long conversation must read a bounded suffix of the log, not the
// whole of it. Each scenario seeds a 10k-record history, counts the pages the
// store is asked for while the conversation opens, and checks the retained
// view is still the right one.

interface Reads {
  calls: number;
  records: number;
}

const counted = (reads: Reads) =>
  Layer.mergeAll(
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
    ).pipe(Layer.provide(testLogLayer)),
    NodeServices.layer,
  );

const reset = (reads: Reads): void => {
  reads.calls = 0;
  reads.records = 0;
};

const texts = (from: number, count: number, text?: string) =>
  Array.from({ length: count }, (_, index) =>
    said(text ?? `text-${String(from + index)}`, from + index),
  );

const aggregated = (
  usage = { input: 10, output: 5 },
): ConversationRecord.Record => ({
  ...settled(usage),
  resume: {
    formatVersion: 1,
    agent: 'test',
    agentRevision: LogVocabulary.AgentRevision.make('1'),
    usage,
    signalCursor: LogOffset.START,
  },
});

const compacted = (firstKept: LogOffset.Offset): ConversationRecord.Record => ({
  _tag: 'Compacted',
  formatVersion: 1,
  agent: 'test',
  agentRevision: LogVocabulary.AgentRevision.make('1'),
  step: 1,
  summary: 'summary',
  firstKept,
  summarizedMessages: 9_980,
  keptMessages: 10,
});

describe('long conversation open structure', () => {
  it.effect(
    'opens 10k compacted lifetime records from bounded suffix pages',
    () =>
      Effect.gen(function* () {
        const reads = { calls: 0, records: 0 };
        const retained = yield* Effect.gen(function* () {
          yield* seed('fixed-tail', [
            ...texts(0, 9_990),
            compacted(LogOffset.fromSeq(9_980n)),
            ...texts(10_000, 5),
            aggregated(),
          ]);
          reset(reads);
          return (yield* openSession('fixed-tail')).history.length;
        }).pipe(Effect.provide(counted(reads)));

        expect(retained).toBe(17);
        // One bounded active-path read validates before producer acquisition;
        // the second rebuilds from the head after fencing any compatible
        // predecessor.
        expect(reads.calls).toBe(3);
        expect(reads.records).toBeLessThanOrEqual(128);
      }),
  );

  it.effect(
    'jumps over an abandoned 10k-record branch to an older compaction',
    () =>
      Effect.gen(function* () {
        const reads = { calls: 0, records: 0 };
        const retained = yield* Effect.gen(function* () {
          yield* seed('branch-jump', [
            ...texts(0, 110),
            compacted(LogOffset.fromSeq(100n)),
            said('kept-after-summary', 111),
            aggregated(),
            ...texts(1_000, 10_000, 'abandoned'),
            aggregated({ input: 20, output: 10 }),
            { _tag: 'BranchedFrom', at: LogOffset.fromSeq(111n) },
          ]);
          reset(reads);
          return (yield* openSession('branch-jump')).history;
        }).pipe(Effect.provide(counted(reads)));

        expect(
          retained.some(
            ({ record }) =>
              record._tag === 'Text' && record.text === 'abandoned',
          ),
        ).toBe(false);
        expect(reads.calls).toBeLessThanOrEqual(8);
        expect(reads.records).toBeLessThanOrEqual(256);
      }),
  );

  it.effect(
    'retains an orphan tool suffix after the latest resume aggregate',
    () =>
      Effect.gen(function* () {
        const reads = { calls: 0, records: 0 };
        const session = yield* Effect.gen(function* () {
          yield* seed('orphan-suffix', [
            ...texts(0, 9_995),
            compacted(LogOffset.fromSeq(9_990n)),
            aggregated(),
            started(),
            lookupStarted('call-1'),
            lookupStarted('call-2'),
            lookupOutcome('call-2', 'durable'),
          ]);
          reset(reads);
          return yield* openSession('orphan-suffix');
        }).pipe(Effect.provide(counted(reads)));

        expect(session.recovery('lookup', callId('call-1'))).toMatchObject({
          _tag: 'Some',
          value: { _tag: 'Indeterminate' },
        });
        expect(session.recovery('lookup', callId('call-2'))).toMatchObject({
          _tag: 'Some',
          value: {
            _tag: 'Settled',
            outcome: 'success',
            result: { status: 'durable' },
          },
        });
        expect(reads.calls).toBe(3);
        expect(reads.records).toBeLessThanOrEqual(128);
      }),
  );

  // Without a resume aggregate the only safe move is a full scan, and it stays
  // that way (opening twice reads the whole history twice, and writes nothing)
  // until a run settles and writes one.
  it.effect(
    'full-scans compatible history without an aggregate until a run settles',
    () =>
      Effect.gen(function* () {
        const reads = { calls: 0, records: 0 };
        const id = LogVocabulary.ConversationId.make('no-aggregate');
        const observed = yield* Effect.gen(function* () {
          yield* seed('no-aggregate', [
            started(),
            ...texts(0, 9_990),
            compacted(LogOffset.fromSeq(9_981n)),
            settled({ input: 10, output: 5 }),
          ]);
          const store = yield* LogStore.Service;
          const count = store
            .meta(AgentLog.pathFor(id))
            .pipe(
              Effect.map((meta) =>
                meta._tag === 'Some' ? meta.value.records : 0,
              ),
            );
          const before = yield* count;
          const openedReads: number[] = [];
          for (let attempt = 0; attempt < 2; attempt += 1) {
            reset(reads);
            yield* openSession('no-aggregate');
            openedReads.push(reads.records);
          }
          return { before, after: yield* count, openedReads };
        }).pipe(Effect.provide(counted(reads)));

        for (const read of observed.openedReads) {
          expect(read).toBeGreaterThan(9_000);
        }
        expect(observed.after).toBe(observed.before);
      }),
  );
});
