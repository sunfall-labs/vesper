import { describe, expect, it } from '@effect/vitest';
import { Deferred, Effect, Fiber, Option, Stream, type Layer } from 'effect';

import { LogOffset } from './offset.js';
import type { ConversationRecord } from './record.js';
import { RecordBatch } from './record-batch.js';
import { LogStore } from './log-store.js';
import { Tail } from './tail.js';
import { LogVocabulary } from './vocabulary.js';

// The behaviour every `LogStore` backend must have, expressed once and
// published so a third-party adapter can certify against it without
// vendoring this package's test tree. `@sunfall/vesper-log-sqlite` and
// `@sunfall/vesper-log-pg` run this suite against their own layers; an
// out-of-tree adapter does the same by importing this subpath and calling
// `LogStoreConformance.register` with its own `Layer<LogStore.Service, E>`.
//
// This is a deliberate narrowing of the "no shared testkit package" rule in
// `docs/contributing.md`: that rule exists to avoid a package that would
// have to depend on the adapters that depend on it. This module carries no
// such cycle — it depends only on this package's own interface, exactly
// like `@sunfall/vesper-agent/testing` — but it does add a dependency this
// package did not previously have outside its test tree: `@effect/vitest`.
// That dependency is scoped to this subpath. It is a peer dependency,
// marked optional, so installing `@sunfall/vesper-log` for its runtime
// modules never pulls in a test framework; only a consumer that imports
// `/testing` needs it, the same way it already needs `@effect/vitest` to
// run its own suite.
//
// Most of what is checked below is invisible from the interface and only
// ever fails in production: that a rejected batch left nothing behind, that
// a retried append converges instead of duplicating, that a reader can
// resume from an offset in the middle of a batch, and that a wake-up
// actually reaches a tail that has already caught up. Those are exactly the
// properties a second backend gets wrong.

const TIMESTAMP = 1_700_000_000_000;
const defaultConversationId =
  LogVocabulary.ConversationId.make('conversation-1');
const childConversationId =
  LogVocabulary.ConversationId.make('conversation-1/c1');
const producer1 = LogVocabulary.ProducerId.make('producer-1');
const producer2 = LogVocabulary.ProducerId.make('producer-2');
const producer3 = LogVocabulary.ProducerId.make('producer-3');
const producerImpostor = LogVocabulary.ProducerId.make('producer-impostor');
const toolCallId = LogVocabulary.ToolCallId.make('c1');
const agentRevision = LogVocabulary.AgentRevision.make('1');
const producerSequence = (value: number): LogVocabulary.ProducerSequence =>
  LogVocabulary.ProducerSequence.make(value);

const at = <T>(values: ReadonlyArray<T>, index: number): T => {
  const value = values[index];
  if (value === undefined) {
    throw new Error(`missing value at index ${String(index)}`);
  }
  return value;
};

const text = (
  value: string,
  conversationId: LogVocabulary.ConversationId = defaultConversationId,
): ConversationRecord.Entry => ({
  conversationId,
  timestamp: TIMESTAMP,
  record: { _tag: 'Text', step: 1, text: value },
});

const texts = (envelopes: ReadonlyArray<ConversationRecord.Envelope>) =>
  envelopes.map((envelope) =>
    envelope.record._tag === 'Text'
      ? envelope.record.text
      : envelope.record._tag,
  );

/** A created stream with a producer already holding the current epoch. */
const open = (store: LogStore.Interface, path: string) =>
  Effect.gen(function* () {
    yield* store.create(path, `identity:${path}`);
    const claim = yield* store.acquire(path, producer1);
    const append = (
      sequence: number,
      records: ReadonlyArray<ConversationRecord.Entry>,
    ) =>
      store.append({
        path,
        producerId: claim.producerId,
        epoch: claim.epoch,
        sequence: producerSequence(sequence),
        records,
      });
    return { claim, append };
  });

/**
 * One behaviour a `LogStore` backend must have, expressed against the
 * resolved service rather than a raw `Layer`, so a case can be run directly
 * in a bespoke harness as well as through {@link register}.
 *
 * `concurrent` marks a case that needs two independent connections to the
 * same underlying storage to be meaningful — a single in-process `Layer`
 * instance cannot demonstrate it. No case in {@link cases} currently needs
 * this; it exists so a future case can be added without another breaking
 * change to the registration shape, and so an adapter that genuinely cannot
 * offer a second independent connection — SQLite's single-file, single
 * connection layer, for instance — has a documented way to opt out via
 * {@link register}'s `concurrent` option instead of silently failing.
 */
export interface ConformanceCase {
  readonly name: string;
  readonly concurrent?: boolean;
  readonly run: (
    store: LogStore.Interface,
  ) => Effect.Effect<void, unknown, LogStore.Service>;
}

export const cases: ReadonlyArray<ConformanceCase> = [
  {
    name: 'round-trips an appended batch',
    run: (store) =>
      Effect.gen(function* () {
        const { append } = yield* open(store, 's1');
        yield* append(0, [text('one'), text('two')]);
        const page = yield* store.read('s1');

        expect(texts(page.records)).toEqual(['one', 'two']);
        expect(page.upToDate).toBe(true);
        expect(page.cursor).toBe(page.records[1]?.offset);
      }),
  },

  // One of every case, through whatever the backend actually stores.
  //
  // A backend that persists JSON — the Postgres one does — round-trips a
  // record through `encodeEntry` and `decodeEnvelope`, and a case whose
  // fields do not survive that is not discoverable from any other test
  // here: the rest of the suite appends `Text`, which is three primitives
  // and would encode under almost any mistake. This is the test that fails
  // when the record union grows a case the codec cannot carry.
  {
    name: 'round-trips every record case',
    run: (store) =>
      Effect.gen(function* () {
        const casesByTag = {
          RunStarted: {
            _tag: 'RunStarted',
            agent: 'a',
            formatVersion: 1,
            agentRevision,
            prompt: [{ role: 'user' }],
          },
          Text: { _tag: 'Text', step: 1, text: 'hello' },
          ToolCall: {
            _tag: 'ToolCall',
            step: 1,
            id: toolCallId,
            name: 't',
            params: { a: 1 },
          },
          ToolStarted: {
            _tag: 'ToolStarted',
            id: toolCallId,
            name: 't',
          },
          ToolSuspended: {
            _tag: 'ToolSuspended',
            id: toolCallId,
            name: 't',
            wait: 'review',
            token: 'workflow-token',
            request: { question: 'continue?' },
          },
          ToolResumed: {
            _tag: 'ToolResumed',
            id: toolCallId,
            name: 't',
            token: 'workflow-token',
          },
          ToolWaitCompleted: {
            _tag: 'ToolWaitCompleted',
            id: toolCallId,
            name: 't',
            wait: 'review',
            token: 'workflow-token',
            outcome: 'success',
            result: { _tag: 'Success', value: { approved: true } },
          },
          ToolWaitRestarted: {
            _tag: 'ToolWaitRestarted',
            id: toolCallId,
            name: 't',
            wait: 'review',
            priorToken: 'workflow-token',
          },
          ToolOutcome: {
            _tag: 'ToolOutcome',
            step: 1,
            id: toolCallId,
            name: 't',
            outcome: 'failure',
            result: { why: 'no' },
          },
          TurnFinished: {
            _tag: 'TurnFinished',
            step: 1,
            usage: { input: 1, output: 2 },
          },
          Compacted: {
            _tag: 'Compacted',
            formatVersion: 1,
            agent: 'test',
            agentRevision,
            step: 1,
            summary: 'the user asked about order 42',
            firstKept: LogOffset.fromSeq(3n),
            summarizedMessages: 4,
            keptMessages: 2,
          },
          BranchedFrom: {
            _tag: 'BranchedFrom',
            at: LogOffset.fromSeq(2n),
          },
          Completed: {
            _tag: 'Completed',
            text: 'done',
            steps: 2,
            usage: { input: 1, output: 2 },
          },
          ChildSession: {
            _tag: 'ChildSession',
            toolCallId,
            agent: 'child',
            parentConversationId: defaultConversationId,
            childConversationId,
            depth: 1,
          },
          Signal: {
            _tag: 'Signal',
            kind: 'steer',
            text: 'go on',
            source: 'operator',
          },
          SignalReceived: {
            _tag: 'SignalReceived',
            kind: 'cancel',
            text: 'stop',
            source: 'ui',
            step: 3,
            at: LogOffset.fromSeq(7n),
          },
          RunSettled: {
            _tag: 'RunSettled',
            outcome: 'interrupted',
            detail: 'the run was interrupted',
            steps: 3,
            usage: { input: 9, output: 8 },
            resume: {
              formatVersion: 1,
              agent: 'test',
              agentRevision,
              usage: { input: 20, output: 10 },
              signalCursor: LogOffset.fromSeq(7n),
            },
          },
          StateCheckpoint: {
            _tag: 'StateCheckpoint',
            id: 'test-state',
            version: '1',
            value: { count: 1 },
          },
          CodeStateCheckpoint: {
            _tag: 'CodeStateCheckpoint',
            state: { remembered: '42' },
          },
        } satisfies {
          [Tag in ConversationRecord.Record['_tag']]: ConversationRecord.RecordOf<Tag>;
        };
        const recordCases: ReadonlyArray<ConversationRecord.Record> =
          Object.values(casesByTag);

        const { append } = yield* open(store, 'every-case');
        yield* append(
          0,
          recordCases.map((record) => ({
            conversationId: defaultConversationId,
            timestamp: TIMESTAMP,
            record,
          })),
        );
        const page = yield* store.read('every-case');

        expect(page.records.map((envelope) => envelope.record)).toEqual(
          recordCases,
        );
      }),
  },

  {
    name: 'returns records that decode as envelopes',
    run: (store) =>
      Effect.gen(function* () {
        const { append } = yield* open(store, 'decodes');
        yield* append(0, [text('one')]);
        const page = yield* store.read('decodes');
        const decoded = yield* RecordBatch.decodeEnvelope(page.records[0]);

        expect(decoded).toMatchObject({
          conversationId: 'conversation-1',
          timestamp: TIMESTAMP,
          record: { _tag: 'Text', text: 'one' },
        });
      }),
  },

  // The whole reason offsets are per record rather than per batch. One
  // offset per batch means a reader that died halfway through one can only
  // restart before it or after it.
  {
    name: 'gives every record in a batch its own offset',
    run: (store) =>
      Effect.gen(function* () {
        const { append } = yield* open(store, 'per-record');
        const last = yield* append(0, [
          text('one'),
          text('two'),
          text('three'),
        ]);
        const page = yield* store.read('per-record');
        const offsets = page.records.map((record) => record.offset);

        expect(new Set(offsets).size).toBe(3);
        expect(last).toBe(offsets[2]);
      }),
  },

  {
    name: 'orders offsets by plain string comparison',
    run: (store) =>
      Effect.gen(function* () {
        const { append } = yield* open(store, 'ordered');
        yield* append(0, [text('one'), text('two')]);
        yield* append(1, [text('three')]);
        const page = yield* store.read('ordered');
        const offsets = page.records.map((record) => record.offset);

        const sorted = offsets.reduce<LogOffset.Offset[]>((result, offset) => {
          const index = result.findIndex((existing) => existing > offset);
          result.splice(index === -1 ? result.length : index, 0, offset);
          return result;
        }, []);
        expect(sorted).toEqual(offsets);
        expect(LogOffset.isAfter(at(offsets, 1), at(offsets, 0))).toBe(true);
        expect(LogOffset.isAfter(at(offsets, 0), LogOffset.START)).toBe(true);
      }),
  },

  {
    name: 'reads exclusively after the supplied offset',
    run: (store) =>
      Effect.gen(function* () {
        const { append } = yield* open(store, 'exclusive');
        yield* append(0, [text('one'), text('two')]);
        const all = yield* store.read('exclusive');
        const page = yield* store.read('exclusive', {
          after: at(all.records, 0).offset,
        });

        expect(texts(page.records)).toEqual(['two']);
      }),
  },

  {
    name: 'resumes from an offset in the middle of a batch',
    run: (store) =>
      Effect.gen(function* () {
        const { append } = yield* open(store, 'mid-batch');
        yield* append(0, [text('one'), text('two'), text('three')]);
        const all = yield* store.read('mid-batch');
        const page = yield* store.read('mid-batch', {
          after: at(all.records, 1).offset,
        });

        expect(texts(page.records)).toEqual(['three']);
      }),
  },

  {
    name: 'reports a partial page as not up to date',
    run: (store) =>
      Effect.gen(function* () {
        const { append } = yield* open(store, 'paged');
        yield* append(0, [text('one'), text('two'), text('three')]);
        const first = yield* store.read('paged', { limit: 2 });

        expect(texts(first.records)).toEqual(['one', 'two']);
        expect(first.upToDate).toBe(false);
        expect(first.cursor).toBe(first.records[1]?.offset);
      }),
  },

  {
    name: 'reports an empty read at the head as up to date',
    run: (store) =>
      Effect.gen(function* () {
        const { append } = yield* open(store, 'at-head');
        const last = yield* append(0, [text('one')]);
        const page = yield* store.read('at-head', { after: last });

        expect(page.records).toEqual([]);
        expect(page.upToDate).toBe(true);
      }),
  },

  {
    name: 'readBackwards: reports backwards reads in not-found errors',
    run: (store) =>
      Effect.gen(function* () {
        const outcome = yield* store
          .readBackwards('missing-backwards')
          .pipe(Effect.result);

        expect(outcome).toMatchObject({
          _tag: 'Failure',
          failure: { operation: 'readBackwards', reason: 'not_found' },
        });
      }),
  },

  {
    name: 'readBackwards: reads newest first and resumes before its oldest record',
    run: (store) =>
      Effect.gen(function* () {
        const { append } = yield* open(store, 'backwards');
        yield* append(0, [text('one'), text('two'), text('three')]);
        const first = yield* store.readBackwards('backwards', { limit: 2 });
        const second = yield* store.readBackwards('backwards', {
          before: first.cursor,
          limit: 2,
        });

        expect(texts(first.records)).toEqual(['three', 'two']);
        expect(first.upToDate).toBe(false);
        expect(texts(second.records)).toEqual(['one']);
        expect(second.upToDate).toBe(true);
      }),
  },

  {
    name: 'readBackwards: uses an exclusive upper bound, including in the middle of a batch',
    run: (store) =>
      Effect.gen(function* () {
        const { append } = yield* open(store, 'backwards-exclusive');
        yield* append(0, [text('one'), text('two'), text('three')]);
        const all = yield* store.read('backwards-exclusive');
        const page = yield* store.readBackwards('backwards-exclusive', {
          before: at(all.records, 2).offset,
        });

        expect(texts(page.records)).toEqual(['two', 'one']);
      }),
  },

  {
    name: 'readBackwards: validates backwards limits and offsets',
    run: (store) =>
      Effect.gen(function* () {
        yield* open(store, 'invalid-backwards');
        const outcomes = yield* Effect.all([
          store
            .readBackwards('invalid-backwards', { limit: 0 })
            .pipe(Effect.result),
          store
            .readBackwards('invalid-backwards', {
              // @ts-expect-error Exercise runtime validation at the untrusted boundary.
              before: 'malformed',
            })
            .pipe(Effect.result),
        ]);

        for (const outcome of outcomes) {
          expect(outcome._tag).toBe('Failure');
          if (outcome._tag === 'Failure') {
            expect(outcome.failure).toMatchObject({
              operation: 'readBackwards',
              reason: 'invalid',
            });
          }
        }
      }),
  },

  // Atomicity. A rejected batch must leave the log exactly as it was — a
  // half-written turn is worse than no turn, because replay would produce a
  // tool call with no outcome and no way to tell that is what happened.
  {
    name: 'writes nothing when an append is rejected',
    run: (store) =>
      Effect.gen(function* () {
        const { append } = yield* open(store, 'atomic');
        yield* append(0, [text('one'), text('two')]);

        const rejected = yield* append(9, [
          text('three'),
          text('four'),
          text('five'),
        ]).pipe(Effect.result);

        const page = yield* store.read('atomic');

        expect(rejected._tag).toBe('Failure');
        expect(texts(page.records)).toEqual(['one', 'two']);
      }),
  },

  {
    name: 'rejects an append with no records',
    run: (store) =>
      Effect.gen(function* () {
        const { append } = yield* open(store, 'empty-batch');
        const outcome = yield* append(0, []).pipe(Effect.result);

        expect(outcome._tag).toBe('Failure');
        if (outcome._tag === 'Failure') {
          expect(outcome.failure).toMatchObject({ reason: 'empty' });
        }
      }),
  },

  {
    name: 'producer fencing: compare-and-acquire does not fence a producer after an observed read changes',
    run: (store) =>
      Effect.gen(function* () {
        const { claim, append } = yield* open(store, 'cas-acquire');
        const before = yield* store.meta('cas-acquire');
        yield* store.read('cas-acquire');
        yield* append(0, [text('landed')]);
        const attempted = yield* store
          .acquire('cas-acquire', producer2, {
            epoch: Option.getOrThrow(before).epoch,
            head: Option.getOrThrow(before).head,
          })
          .pipe(Effect.result);
        const stillWritable = yield* store.append({
          path: 'cas-acquire',
          producerId: claim.producerId,
          epoch: claim.epoch,
          sequence: producerSequence(1),
          records: [text('still-owner')],
        });

        expect(attempted).toMatchObject({
          _tag: 'Failure',
          failure: { operation: 'acquire', reason: 'conflict' },
        });
        expect(stillWritable).not.toBe(LogOffset.START);
      }),
  },

  {
    name: 'producer fencing: rejects a stale epoch even when the head still matches',
    run: (store) =>
      Effect.gen(function* () {
        yield* open(store, 'cas-stale-epoch');
        const before = Option.getOrThrow(yield* store.meta('cas-stale-epoch'));
        const current = yield* store.acquire('cas-stale-epoch', producer2);
        const attempted = yield* store
          .acquire('cas-stale-epoch', producer3, {
            epoch: before.epoch,
            head: before.head,
          })
          .pipe(Effect.result);
        const stillWritable = yield* store.append({
          path: 'cas-stale-epoch',
          producerId: current.producerId,
          epoch: current.epoch,
          sequence: producerSequence(0),
          records: [text('still-owner')],
        });

        expect(attempted).toMatchObject({
          _tag: 'Failure',
          failure: { operation: 'acquire', reason: 'conflict' },
        });
        expect(stillWritable).not.toBe(LogOffset.START);
      }),
  },

  {
    name: 'producer fencing: compare-and-acquire fences normally when the expected position matches',
    run: (store) =>
      Effect.gen(function* () {
        const { claim } = yield* open(store, 'cas-match');
        const before = Option.getOrThrow(yield* store.meta('cas-match'));
        yield* store.acquire('cas-match', producer2, {
          epoch: before.epoch,
          head: before.head,
        });
        const outcome = yield* store
          .append({
            path: 'cas-match',
            producerId: claim.producerId,
            epoch: claim.epoch,
            sequence: producerSequence(0),
            records: [text('stale')],
          })
          .pipe(Effect.result);

        expect(outcome).toMatchObject({
          _tag: 'Failure',
          failure: { operation: 'append', reason: 'fenced' },
        });
      }),
  },

  {
    name: 'producer fencing: keeps legacy acquire fencing semantics without an expected position',
    run: (store) =>
      Effect.gen(function* () {
        const { claim, append } = yield* open(store, 'legacy-acquire');
        yield* append(0, [text('landed')]);
        yield* store.acquire('legacy-acquire', producer2);
        const outcome = yield* store
          .append({
            path: 'legacy-acquire',
            producerId: claim.producerId,
            epoch: claim.epoch,
            sequence: producerSequence(1),
            records: [text('stale')],
          })
          .pipe(Effect.result);

        expect(outcome).toMatchObject({
          _tag: 'Failure',
          failure: { operation: 'append', reason: 'fenced' },
        });
      }),
  },

  // A producer whose epoch has been superseded must be told, not allowed to
  // interleave writes with the producer that replaced it.
  {
    name: 'producer fencing: fences a producer holding a stale epoch',
    run: (store) =>
      Effect.gen(function* () {
        const { claim } = yield* open(store, 'fenced');
        yield* store.acquire('fenced', producer2);
        const outcome = yield* store
          .append({
            path: 'fenced',
            producerId: claim.producerId,
            epoch: claim.epoch,
            sequence: producerSequence(0),
            records: [text('stale')],
          })
          .pipe(Effect.result);

        expect(outcome._tag).toBe('Failure');
        if (outcome._tag === 'Failure') {
          expect(outcome.failure).toMatchObject({
            operation: 'append',
            reason: 'fenced',
          });
        }
      }),
  },

  // The producer that wrote a batch and crashed before hearing the answer
  // retries it; it must converge on the original offset rather than writing
  // the batch twice.
  {
    name: 'producer fencing: returns the original offset for an exact retry',
    run: (store) =>
      Effect.gen(function* () {
        const { append } = yield* open(store, 'retry');
        const first = yield* append(0, [text('one'), text('two')]);
        const again = yield* append(0, [text('one'), text('two')]);
        const page = yield* store.read('retry');

        expect(again).toBe(first);
        expect(page.records.length).toBe(2);
      }),
  },

  // Reusing a sequence for a different batch is not a retry. Returning the
  // old offset would drop the new records silently.
  {
    name: 'producer fencing: rejects a retry that carries a different batch',
    run: (store) =>
      Effect.gen(function* () {
        const { append } = yield* open(store, 'conflict');
        yield* append(0, [text('one'), text('two')]);
        const outcome = yield* append(0, [text('one')]).pipe(Effect.result);

        expect(outcome._tag).toBe('Failure');
        if (outcome._tag === 'Failure') {
          expect(outcome.failure).toMatchObject({ reason: 'conflict' });
        }
      }),
  },

  // The version a record count cannot catch, and the reason sameness is
  // decided by a digest of the encoded batch. Same producer, same epoch,
  // same sequence, same number of records, different content — answering
  // that with the earlier offset is silent data loss.
  {
    name: 'producer fencing: rejects a retry of the same length with different content',
    run: (store) =>
      Effect.gen(function* () {
        const { append } = yield* open(store, 'same-length');
        yield* append(0, [text('one'), text('two')]);
        const outcome = yield* append(0, [text('one'), text('CHANGED')]).pipe(
          Effect.result,
        );

        expect(outcome._tag).toBe('Failure');
        if (outcome._tag === 'Failure') {
          expect(outcome.failure).toMatchObject({ reason: 'conflict' });
        }
      }),
  },

  {
    name: 'producer fencing: rejects an append from a producer that does not hold the epoch',
    run: (store) =>
      Effect.gen(function* () {
        const { claim } = yield* open(store, 'impostor');
        const outcome = yield* store
          .append({
            path: 'impostor',
            producerId: producerImpostor,
            epoch: claim.epoch,
            sequence: producerSequence(0),
            records: [text('one')],
          })
          .pipe(Effect.result);

        expect(outcome._tag).toBe('Failure');
        if (outcome._tag === 'Failure') {
          expect(outcome.failure).toMatchObject({ reason: 'conflict' });
        }
      }),
  },

  // A skipped sequence means writes were lost. Accepting it would leave a
  // hole nobody notices until a replay reads through it.
  {
    name: 'producer fencing: rejects a sequence gap',
    run: (store) =>
      Effect.gen(function* () {
        const { append } = yield* open(store, 'gap');
        yield* append(0, [text('one')]);
        const outcome = yield* append(3, [text('four')]).pipe(Effect.result);

        expect(outcome._tag).toBe('Failure');
        if (outcome._tag === 'Failure') {
          expect(outcome.failure).toMatchObject({ reason: 'gap' });
        }
      }),
  },

  {
    name: 'producer fencing: resets the producer sequence on acquire but not the offsets',
    run: (store) =>
      Effect.gen(function* () {
        const { append } = yield* open(store, 'handover');
        const first = yield* append(0, [text('one')]);

        const second = yield* store.acquire('handover', producer2);
        const next = yield* store.append({
          path: 'handover',
          producerId: producer2,
          epoch: second.epoch,
          sequence: second.nextSequence,
          records: [text('two')],
        });

        expect(second.nextSequence).toBe(0);
        expect(LogOffset.isAfter(next, first)).toBe(true);
      }),
  },

  // The opening tick is the only way a reader can learn that its
  // subscription took effect, and `Tail` depends on it to start at all. A
  // backend that omits it produces a tail that never emits its own
  // history — a hang, not a failure, and therefore worth its own case
  // rather than being diagnosed through the wake-up case below.
  {
    name: 'emits an opening wake-up once subscribed',
    run: (store) =>
      Effect.gen(function* () {
        yield* store.create('opening', 'identity');
        const ticks = yield* store
          .changes('opening')
          .pipe(Stream.take(1), Stream.runCollect);

        expect(ticks.length).toBe(1);
      }),
  },

  // The wake-up path, tested through `Tail` because that is the only thing
  // that consumes it.
  {
    name: 'wakes a caught-up tail when a record arrives',
    run: (store) =>
      Effect.gen(function* () {
        const { append } = yield* open(store, 'tailed');
        yield* append(0, [text('one')]);

        const emitted = yield* Deferred.make<void>();
        const fiber = yield* Tail.from('tailed', LogOffset.START).pipe(
          Stream.tap(() => Deferred.succeed(emitted, undefined)),
          Stream.take(2),
          Stream.runCollect,
          Effect.forkChild,
        );

        // Only append the second record once the tail has emitted the
        // first, so the record cannot be picked up by the initial catch-up.
        // Reaching it requires a wake-up.
        yield* Deferred.await(emitted);
        yield* append(1, [text('two')]);

        const collected = yield* Fiber.join(fiber);
        expect(texts(collected)).toEqual(['one', 'two']);
      }),
  },

  // A payload a backend could not have written must fail the append, not be
  // dropped or stored half-formed. `Schema.Unknown` is the hole through
  // which one arrives.
  {
    name: 'rejects records that cannot be encoded for persistence',
    run: (store) =>
      Effect.gen(function* () {
        const { append } = yield* open(store, 'unencodable');
        const outcome = yield* append(0, [
          {
            conversationId: defaultConversationId,
            timestamp: TIMESTAMP,
            record: {
              _tag: 'ToolCall',
              step: 1,
              id: LogVocabulary.ToolCallId.make('call-1'),
              name: 'search',
              params: { limit: 1n },
            },
          },
        ]).pipe(Effect.result);

        expect(outcome._tag).toBe('Failure');
        if (outcome._tag === 'Failure') {
          expect(outcome.failure).toMatchObject({
            operation: 'append',
            reason: 'encoding',
          });
        }
      }),
  },

  {
    name: 'rejects every value JSON would alter or discard',
    run: (store) =>
      Effect.gen(function* () {
        const cyclic: { self?: unknown } = {};
        cyclic.self = cyclic;
        const sparse: unknown[] = [];
        sparse.length = 1;
        const symbolProperty: Record<PropertyKey, unknown> = { okay: true };
        symbolProperty[Symbol('hidden')] = true;
        const lossy: ReadonlyArray<unknown> = [
          undefined,
          () => undefined,
          Symbol('value'),
          1n,
          Number.NaN,
          Number.POSITIVE_INFINITY,
          Number.NEGATIVE_INFINITY,
          -0,
          sparse,
          cyclic,
          symbolProperty,
          new Date(0),
        ];

        const { append } = yield* open(store, 'all-lossy-values');
        const outcomes = yield* Effect.forEach(lossy, (params) =>
          append(0, [
            {
              conversationId: defaultConversationId,
              timestamp: TIMESTAMP,
              record: {
                _tag: 'ToolCall',
                step: 1,
                id: LogVocabulary.ToolCallId.make('call-1'),
                name: 'search',
                params,
              },
            },
          ]).pipe(Effect.result),
        );

        expect(outcomes).toHaveLength(lossy.length);
        for (const outcome of outcomes) {
          expect(outcome._tag).toBe('Failure');
          if (outcome._tag === 'Failure') {
            expect(outcome.failure).toMatchObject({ reason: 'encoding' });
          }
        }
      }),
  },

  {
    name: 'stores an encoded clone rather than caller-owned objects',
    run: (store) =>
      Effect.gen(function* () {
        const params = {
          nested: { value: 'before' },
          ['__proto__']: { retained: true },
        };
        const { append } = yield* open(store, 'encoded-clone');
        yield* append(0, [
          {
            conversationId: defaultConversationId,
            timestamp: TIMESTAMP,
            record: {
              _tag: 'ToolCall',
              step: 1,
              id: LogVocabulary.ToolCallId.make('call-1'),
              name: 'search',
              params,
            },
          },
        ]);
        params.nested.value = 'after';
        const read = yield* store.read('encoded-clone');

        expect(read.records[0]?.record).toMatchObject({
          params: { nested: { value: 'before' } },
        });
        const record = read.records[0]?.record;
        if (record === undefined) {
          throw new Error('encoded-clone read returned no record');
        }
        expect(record._tag).toBe('ToolCall');
        if (
          record._tag === 'ToolCall' &&
          typeof record.params === 'object' &&
          record.params !== null
        ) {
          const stored = record.params;
          expect(Object.hasOwn(stored, '__proto__')).toBe(true);
          const protoValue: unknown = Object.getOwnPropertyDescriptor(
            stored,
            '__proto__',
          )?.value;
          expect(protoValue).toEqual({ retained: true });
        }
      }),
  },

  {
    name: 'treats object insertion order as irrelevant to retry identity',
    run: (store) =>
      Effect.gen(function* () {
        const { append } = yield* open(store, 'canonical-key-order');
        const entry = (params: Record<string, number>) => ({
          conversationId: defaultConversationId,
          timestamp: TIMESTAMP,
          record: {
            _tag: 'ToolCall' as const,
            step: 1,
            id: LogVocabulary.ToolCallId.make('call-1'),
            name: 'search',
            params,
          },
        });
        const first = yield* append(0, [entry({ a: 1, b: 2 })]);
        const retry = yield* append(0, [entry({ b: 2, a: 1 })]);

        expect(retry).toBe(first);
      }),
  },

  {
    name: 'rejects invalid read limits and malformed offsets',
    run: (store) =>
      Effect.gen(function* () {
        yield* open(store, 'invalid-read-options');
        const outcomes = yield* Effect.all([
          store.read('invalid-read-options', { limit: 0 }).pipe(Effect.result),
          store
            .read('invalid-read-options', { limit: 1.5 })
            .pipe(Effect.result),
          store
            .read('invalid-read-options', {
              limit: LogStore.MAX_READ_LIMIT + 1,
            })
            .pipe(Effect.result),
          store
            .read('invalid-read-options', {
              // @ts-expect-error Exercise runtime validation at the untrusted boundary.
              after: 'malformed',
            })
            .pipe(Effect.result),
        ]);

        for (const outcome of outcomes) {
          expect(outcome._tag).toBe('Failure');
          if (outcome._tag === 'Failure') {
            expect(outcome.failure).toMatchObject({
              operation: 'read',
              reason: 'invalid',
            });
          }
        }
      }),
  },

  {
    name: 'stream lifecycle: reports meta for a created stream',
    run: (store) =>
      Effect.gen(function* () {
        const { append } = yield* open(store, 'meta');
        yield* append(0, [text('one'), text('two')]);
        const meta = yield* store.meta('meta');

        expect(Option.isSome(meta)).toBe(true);
        expect(Option.getOrThrow(meta)).toMatchObject({
          path: 'meta',
          identity: 'identity:meta',
          records: 2,
        });
      }),
  },

  {
    name: 'stream lifecycle: returns None for a stream that was never created',
    run: (store) =>
      Effect.gen(function* () {
        const meta = yield* store.meta('absent');
        expect(Option.isNone(meta)).toBe(true);
      }),
  },

  {
    name: 'stream lifecycle: rejects creating a stream twice',
    run: (store) =>
      Effect.gen(function* () {
        yield* store.create('twice', 'identity');
        const outcome = yield* store
          .create('twice', 'identity')
          .pipe(Effect.result);

        expect(outcome._tag).toBe('Failure');
        if (outcome._tag === 'Failure') {
          expect(outcome.failure).toMatchObject({
            operation: 'create',
            reason: 'conflict',
          });
        }
      }),
  },

  // A typo in a conversation id must not quietly start a second, parallel
  // history. That is why creation is explicit.
  {
    name: 'stream lifecycle: fails to read a stream that was never created',
    run: (store) =>
      Effect.gen(function* () {
        const outcome = yield* store.read('never').pipe(Effect.result);

        expect(outcome._tag).toBe('Failure');
        if (outcome._tag === 'Failure') {
          expect(outcome.failure).toMatchObject({
            operation: 'read',
            reason: 'not_found',
          });
        }
      }),
  },

  {
    name: 'stream lifecycle: fails to acquire a stream that was never created',
    run: (store) =>
      Effect.gen(function* () {
        const outcome = yield* store
          .acquire('never', producer1)
          .pipe(Effect.result);

        expect(outcome._tag).toBe('Failure');
        if (outcome._tag === 'Failure') {
          expect(outcome.failure).toMatchObject({
            operation: 'acquire',
            reason: 'not_found',
          });
        }
      }),
  },
];

export interface RegisterOptions {
  /**
   * Include cases marked {@link ConformanceCase.concurrent}. Defaults to
   * `true`. Pass `false` for an adapter whose layer cannot stand up two
   * independent connections to the same underlying storage — a single-file
   * SQLite connection, for instance.
   */
  readonly concurrent?: boolean;
}

/**
 * Register {@link cases} against `layer` as an `@effect/vitest` suite named
 * `` `LogStore contract: ${name}` ``.
 *
 * Each case gets its own `it.effect`, and each `it.effect` provides `layer`
 * fresh, so cases never share state through the store even when the layer
 * itself is cheap to build twice — the same isolation the built-in adapters'
 * own suites already rely on.
 */
export const register = <E>(
  name: string,
  layer: Layer.Layer<LogStore.Service, E>,
  options: RegisterOptions = {},
): void => {
  const includeConcurrent = options.concurrent ?? true;
  describe(`LogStore contract: ${name}`, () => {
    for (const conformanceCase of cases) {
      if (conformanceCase.concurrent === true && !includeConcurrent) {
        continue;
      }
      it.effect(conformanceCase.name, () =>
        Effect.flatMap(LogStore.Service, conformanceCase.run).pipe(
          Effect.provide(layer),
        ),
      );
    }
  });
};

/**
 * The dead-change-feed case, kept separate from {@link cases} and
 * {@link register} because it needs a backend that can fake a failing
 * `changes` subscription on demand — something only the in-process memory
 * adapter can construct today, by building a store around a channel it
 * controls rather than a real socket. It is the only way to exercise the
 * case that matters most about `changes` having an error channel at all: a
 * feed that dies must reach the consumer as a failure rather than as a tail
 * that looks healthy and delivers nothing, because a working backend cannot
 * produce that state on demand. An adapter that cannot fake this — Postgres,
 * SQLite — simply does not call this export; {@link register} alone still
 * certifies it against everything else.
 */
export const registerDeadChangeFeed = <E>(
  name: string,
  layerWithFailingChanges: (path: string) => Layer.Layer<LogStore.Service, E>,
): void => {
  describe(`LogStore contract: ${name}`, () => {
    it.effect('surfaces a dead change feed to a tail rather than hanging', () =>
      Effect.gen(function* () {
        const store = yield* LogStore.Service;
        yield* store.create('dead-feed', 'identity');
        const outcome = yield* Tail.from('dead-feed', LogOffset.START).pipe(
          Stream.runCollect,
          Effect.result,
        );

        expect(outcome._tag).toBe('Failure');
        if (outcome._tag === 'Failure') {
          expect(outcome.failure).toMatchObject({ operation: 'changes' });
        }
      }).pipe(Effect.provide(layerWithFailingChanges('dead-feed'))),
    );
  });
};

export * as LogStoreConformance from './testing.js';
