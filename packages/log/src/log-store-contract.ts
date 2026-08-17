import { Deferred, Effect, Fiber, Layer, Option, Stream } from 'effect';
import { describe, expect, it } from 'vitest';

import { LogStore } from './log-store.js';
import { LogOffset } from './offset.js';
import { ConversationRecord } from './record.js';
import { Tail } from './tail.js';
import { LogVocabulary } from './vocabulary.js';

// The behaviour every LogStore backend must have, expressed once.
//
// Per `docs/contributing.md` this lives in the package that owns the
// interface. Anything implementing `LogStore` already depends on
// `@sunfall/vesper-log`, so importing the suite from here cannot cycle, whereas a
// shared testkit package would have to depend on the packages that depend on
// it.
//
// Run it against every backend:
//
//   logStoreContract('memory', { layer: LogStoreMemory.layer })
//   logStoreContract('postgres', { layer: LogStorePostgres.layer })
//
// Most of what is checked below is invisible from the interface and only
// ever fails in production: that a rejected batch left nothing behind, that
// a retried append converges instead of duplicating, that a reader can
// resume from an offset in the middle of a batch, that a wake-up actually
// reaches a tail that has already caught up, and that a change feed which
// dies says so instead of going quiet. Those are exactly the properties a
// second backend gets wrong.

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
const producerSequence = LogVocabulary.ProducerSequence.make;

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

export interface ContractOptions<E, R> {
  readonly layer: Layer.Layer<LogStore.Service, E, R>;
  /**
   * The same backend, with `changes(path)` failing.
   *
   * Supply it when the backend can fake a dead notification channel. It is
   * the only way to exercise the case that matters most about `changes`
   * having an error channel at all — a feed that dies must reach the
   * consumer as a failure rather than as a tail that looks healthy and
   * delivers nothing — because a working backend cannot produce that state
   * on demand. Backends that omit it skip the case, and the suite says so in
   * the test name.
   */
  readonly layerWithFailingChanges?: (
    path: string,
  ) => Layer.Layer<LogStore.Service, E, R>;
}

export const logStoreContract = <E, R>(
  name: string,
  options: ContractOptions<E, R>,
): void => {
  const runIn = <A>(
    layer: Layer.Layer<LogStore.Service, E, R>,
    effect: Effect.Effect<A, unknown, LogStore.Service>,
  ): Promise<A> =>
    Effect.runPromise(
      effect.pipe(
        Effect.provide(layer as Layer.Layer<LogStore.Service>),
      ) as Effect.Effect<A>,
    );

  const run = <A>(
    effect: Effect.Effect<A, unknown, LogStore.Service>,
  ): Promise<A> => runIn(options.layer, effect);

  /** A created stream with a producer already holding the current epoch. */
  const open = (path: string) =>
    Effect.gen(function* () {
      const store = yield* LogStore.Service;
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
      return { store, claim, append };
    });

  describe(`LogStore contract: ${name}`, () => {
    it('round-trips an appended batch', async () => {
      const page = await run(
        Effect.gen(function* () {
          const { store, append } = yield* open('s1');
          yield* append(0, [text('one'), text('two')]);
          return yield* store.read('s1');
        }),
      );

      expect(texts(page.records)).toEqual(['one', 'two']);
      expect(page.upToDate).toBe(true);
      expect(page.cursor).toBe(page.records[1]?.offset);
    });

    // One of every case, through whatever the backend actually stores.
    //
    // A backend that persists JSON — the Postgres one does — round-trips a
    // record through `encodeEntry` and `decodeEnvelope`, and a case whose
    // fields do not survive that is not discoverable from any other test
    // here: the rest of the suite appends `Text`, which is three primitives
    // and would encode under almost any mistake. This is the test that fails
    // when the record union grows a case the codec cannot carry.
    it('round-trips every record case', async () => {
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
      } satisfies {
        [Tag in ConversationRecord.Record['_tag']]: ConversationRecord.RecordOf<Tag>;
      };
      const cases: ReadonlyArray<ConversationRecord.Record> =
        Object.values(casesByTag);

      const page = await run(
        Effect.gen(function* () {
          const { store, append } = yield* open('every-case');
          yield* append(
            0,
            cases.map((record) => ({
              conversationId: defaultConversationId,
              timestamp: TIMESTAMP,
              record,
            })),
          );
          return yield* store.read('every-case');
        }),
      );

      expect(page.records.map((envelope) => envelope.record)).toEqual(cases);
    });

    it('returns records that decode as envelopes', async () => {
      const decoded = await run(
        Effect.gen(function* () {
          const { store, append } = yield* open('decodes');
          yield* append(0, [text('one')]);
          const page = yield* store.read('decodes');
          return yield* ConversationRecord.decodeEnvelope(page.records[0]);
        }),
      );

      expect(decoded).toMatchObject({
        conversationId: 'conversation-1',
        timestamp: TIMESTAMP,
        record: { _tag: 'Text', text: 'one' },
      });
    });

    // The whole reason offsets are per record rather than per batch. One
    // offset per batch means a reader that died halfway through one can only
    // restart before it or after it.
    it('gives every record in a batch its own offset', async () => {
      const result = await run(
        Effect.gen(function* () {
          const { store, append } = yield* open('per-record');
          const last = yield* append(0, [
            text('one'),
            text('two'),
            text('three'),
          ]);
          const page = yield* store.read('per-record');
          return { last, offsets: page.records.map((r) => r.offset) };
        }),
      );

      expect(new Set(result.offsets).size).toBe(3);
      expect(result.last).toBe(result.offsets[2]);
    });

    it('orders offsets by plain string comparison', async () => {
      const offsets = await run(
        Effect.gen(function* () {
          const { store, append } = yield* open('ordered');
          yield* append(0, [text('one'), text('two')]);
          yield* append(1, [text('three')]);
          const page = yield* store.read('ordered');
          return page.records.map((r) => r.offset);
        }),
      );

      expect([...offsets].sort()).toEqual(offsets);
      expect(LogOffset.isAfter(offsets[1]!, offsets[0]!)).toBe(true);
      expect(LogOffset.isAfter(offsets[0]!, LogOffset.START)).toBe(true);
    });

    it('reads exclusively after the supplied offset', async () => {
      const page = await run(
        Effect.gen(function* () {
          const { store, append } = yield* open('exclusive');
          yield* append(0, [text('one'), text('two')]);
          const all = yield* store.read('exclusive');
          return yield* store.read('exclusive', {
            after: all.records[0]!.offset,
          });
        }),
      );

      expect(texts(page.records)).toEqual(['two']);
    });

    it('resumes from an offset in the middle of a batch', async () => {
      const page = await run(
        Effect.gen(function* () {
          const { store, append } = yield* open('mid-batch');
          yield* append(0, [text('one'), text('two'), text('three')]);
          const all = yield* store.read('mid-batch');
          return yield* store.read('mid-batch', {
            after: all.records[1]!.offset,
          });
        }),
      );

      expect(texts(page.records)).toEqual(['three']);
    });

    it('reports a partial page as not up to date', async () => {
      const first = await run(
        Effect.gen(function* () {
          const { store, append } = yield* open('paged');
          yield* append(0, [text('one'), text('two'), text('three')]);
          return yield* store.read('paged', { limit: 2 });
        }),
      );

      expect(texts(first.records)).toEqual(['one', 'two']);
      expect(first.upToDate).toBe(false);
      expect(first.cursor).toBe(first.records[1]?.offset);
    });

    it('reports an empty read at the head as up to date', async () => {
      const page = await run(
        Effect.gen(function* () {
          const { store, append } = yield* open('at-head');
          const last = yield* append(0, [text('one')]);
          return yield* store.read('at-head', { after: last });
        }),
      );

      expect(page.records).toEqual([]);
      expect(page.upToDate).toBe(true);
    });

    describe('backwards paging', () => {
      it('reports backwards reads in not-found errors', async () => {
        const outcome = await run(
          Effect.flatMap(LogStore.Service, (store) =>
            store.readBackwards('missing-backwards').pipe(Effect.result),
          ),
        );
        expect(outcome).toMatchObject({
          _tag: 'Failure',
          failure: {
            operation: 'readBackwards',
            reason: 'not_found',
          },
        });
      });

      it('reads newest first and resumes before its oldest record', async () => {
        const pages = await run(
          Effect.gen(function* () {
            const { store, append } = yield* open('backwards');
            yield* append(0, [text('one'), text('two'), text('three')]);
            const first = yield* store.readBackwards('backwards', { limit: 2 });
            const second = yield* store.readBackwards('backwards', {
              before: first.cursor,
              limit: 2,
            });
            return { first, second };
          }),
        );
        expect(texts(pages.first.records)).toEqual(['three', 'two']);
        expect(pages.first.upToDate).toBe(false);
        expect(texts(pages.second.records)).toEqual(['one']);
        expect(pages.second.upToDate).toBe(true);
      });

      it('uses an exclusive upper bound, including in the middle of a batch', async () => {
        const page = await run(
          Effect.gen(function* () {
            const { store, append } = yield* open('backwards-exclusive');
            yield* append(0, [text('one'), text('two'), text('three')]);
            const all = yield* store.read('backwards-exclusive');
            return yield* store.readBackwards('backwards-exclusive', {
              before: all.records[2]!.offset,
            });
          }),
        );
        expect(texts(page.records)).toEqual(['two', 'one']);
      });

      it('validates backwards limits and offsets', async () => {
        const outcomes = await run(
          Effect.gen(function* () {
            const { store } = yield* open('invalid-backwards');
            return yield* Effect.all([
              store
                .readBackwards('invalid-backwards', { limit: 0 })
                .pipe(Effect.result),
              store
                .readBackwards('invalid-backwards', {
                  before: 'malformed' as unknown as LogOffset.Offset,
                })
                .pipe(Effect.result),
            ]);
          }),
        );
        for (const outcome of outcomes) {
          expect(outcome._tag).toBe('Failure');
          if (outcome._tag === 'Failure') {
            expect(outcome.failure).toMatchObject({
              operation: 'readBackwards',
              reason: 'invalid',
            });
          }
        }
      });
    });

    // Atomicity. A rejected batch must leave the log exactly as it was — a
    // half-written turn is worse than no turn, because replay would produce
    // a tool call with no outcome and no way to tell that is what happened.
    it('writes nothing when an append is rejected', async () => {
      const result = await run(
        Effect.gen(function* () {
          const { store, append } = yield* open('atomic');
          yield* append(0, [text('one'), text('two')]);

          const rejected = yield* append(9, [
            text('three'),
            text('four'),
            text('five'),
          ]).pipe(Effect.result);

          const page = yield* store.read('atomic');
          return { rejected, texts: texts(page.records) };
        }),
      );

      expect(result.rejected._tag).toBe('Failure');
      expect(result.texts).toEqual(['one', 'two']);
    });

    it('rejects an append with no records', async () => {
      const outcome = await run(
        Effect.gen(function* () {
          const { append } = yield* open('empty-batch');
          return yield* append(0, []).pipe(Effect.result);
        }),
      );

      expect(outcome._tag).toBe('Failure');
      if (outcome._tag === 'Failure') {
        expect(outcome.failure).toMatchObject({ reason: 'empty' });
      }
    });

    describe('producer fencing', () => {
      it('compare-and-acquire does not fence a producer after an observed read changes', async () => {
        const result = await run(
          Effect.gen(function* () {
            const { store, claim, append } = yield* open('cas-acquire');
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
            return { attempted, stillWritable };
          }),
        );

        expect(result.attempted).toMatchObject({
          _tag: 'Failure',
          failure: { operation: 'acquire', reason: 'conflict' },
        });
        expect(result.stillWritable).not.toBe(LogOffset.START);
      });

      it('rejects a stale epoch even when the head still matches', async () => {
        const result = await run(
          Effect.gen(function* () {
            const { store } = yield* open('cas-stale-epoch');
            const before = Option.getOrThrow(
              yield* store.meta('cas-stale-epoch'),
            );
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
            return { attempted, stillWritable };
          }),
        );

        expect(result.attempted).toMatchObject({
          _tag: 'Failure',
          failure: { operation: 'acquire', reason: 'conflict' },
        });
        expect(result.stillWritable).not.toBe(LogOffset.START);
      });

      it('compare-and-acquire fences normally when the expected position matches', async () => {
        const outcome = await run(
          Effect.gen(function* () {
            const { store, claim } = yield* open('cas-match');
            const before = Option.getOrThrow(yield* store.meta('cas-match'));
            yield* store.acquire('cas-match', producer2, {
              epoch: before.epoch,
              head: before.head,
            });
            return yield* store
              .append({
                path: 'cas-match',
                producerId: claim.producerId,
                epoch: claim.epoch,
                sequence: producerSequence(0),
                records: [text('stale')],
              })
              .pipe(Effect.result);
          }),
        );

        expect(outcome).toMatchObject({
          _tag: 'Failure',
          failure: { operation: 'append', reason: 'fenced' },
        });
      });

      it('keeps legacy acquire fencing semantics without an expected position', async () => {
        const outcome = await run(
          Effect.gen(function* () {
            const { store, claim, append } = yield* open('legacy-acquire');
            yield* append(0, [text('landed')]);
            yield* store.acquire('legacy-acquire', producer2);
            return yield* store
              .append({
                path: 'legacy-acquire',
                producerId: claim.producerId,
                epoch: claim.epoch,
                sequence: producerSequence(1),
                records: [text('stale')],
              })
              .pipe(Effect.result);
          }),
        );

        expect(outcome).toMatchObject({
          _tag: 'Failure',
          failure: { operation: 'append', reason: 'fenced' },
        });
      });

      // Outcome 1. A producer whose epoch has been superseded must be told,
      // not allowed to interleave writes with the producer that replaced it.
      it('fences a producer holding a stale epoch', async () => {
        const outcome = await run(
          Effect.gen(function* () {
            const { store, claim } = yield* open('fenced');
            yield* store.acquire('fenced', producer2);

            return yield* store
              .append({
                path: 'fenced',
                producerId: claim.producerId,
                epoch: claim.epoch,
                sequence: producerSequence(0),
                records: [text('stale')],
              })
              .pipe(Effect.result);
          }),
        );

        expect(outcome._tag).toBe('Failure');
        if (outcome._tag === 'Failure') {
          expect(outcome.failure).toMatchObject({
            operation: 'append',
            reason: 'fenced',
          });
        }
      });

      // Outcome 2. The producer that wrote a batch and crashed before
      // hearing the answer retries it; it must converge on the original
      // offset rather than writing the batch twice.
      it('returns the original offset for an exact retry', async () => {
        const result = await run(
          Effect.gen(function* () {
            const { store, append } = yield* open('retry');
            const first = yield* append(0, [text('one'), text('two')]);
            const again = yield* append(0, [text('one'), text('two')]);
            const page = yield* store.read('retry');
            return { first, again, count: page.records.length };
          }),
        );

        expect(result.again).toBe(result.first);
        expect(result.count).toBe(2);
      });

      // Outcome 3. Reusing a sequence for a different batch is not a retry.
      // Returning the old offset would drop the new records silently.
      it('rejects a retry that carries a different batch', async () => {
        const outcome = await run(
          Effect.gen(function* () {
            const { append } = yield* open('conflict');
            yield* append(0, [text('one'), text('two')]);
            return yield* append(0, [text('one')]).pipe(Effect.result);
          }),
        );

        expect(outcome._tag).toBe('Failure');
        if (outcome._tag === 'Failure') {
          expect(outcome.failure).toMatchObject({ reason: 'conflict' });
        }
      });

      // The version a record count cannot catch, and the reason sameness is
      // decided by a digest of the encoded batch. Same producer, same epoch,
      // same sequence, same number of records, different content — answering
      // that with the earlier offset is silent data loss.
      it('rejects a retry of the same length with different content', async () => {
        const outcome = await run(
          Effect.gen(function* () {
            const { append } = yield* open('same-length');
            yield* append(0, [text('one'), text('two')]);
            return yield* append(0, [text('one'), text('CHANGED')]).pipe(
              Effect.result,
            );
          }),
        );

        expect(outcome._tag).toBe('Failure');
        if (outcome._tag === 'Failure') {
          expect(outcome.failure).toMatchObject({ reason: 'conflict' });
        }
      });

      it('rejects an append from a producer that does not hold the epoch', async () => {
        const outcome = await run(
          Effect.gen(function* () {
            const { store, claim } = yield* open('impostor');
            return yield* store
              .append({
                path: 'impostor',
                producerId: producerImpostor,
                epoch: claim.epoch,
                sequence: producerSequence(0),
                records: [text('one')],
              })
              .pipe(Effect.result);
          }),
        );

        expect(outcome._tag).toBe('Failure');
        if (outcome._tag === 'Failure') {
          expect(outcome.failure).toMatchObject({ reason: 'conflict' });
        }
      });

      // Outcome 4. A skipped sequence means writes were lost. Accepting it
      // would leave a hole nobody notices until a replay reads through it.
      it('rejects a sequence gap', async () => {
        const outcome = await run(
          Effect.gen(function* () {
            const { append } = yield* open('gap');
            yield* append(0, [text('one')]);
            return yield* append(3, [text('four')]).pipe(Effect.result);
          }),
        );

        expect(outcome._tag).toBe('Failure');
        if (outcome._tag === 'Failure') {
          expect(outcome.failure).toMatchObject({ reason: 'gap' });
        }
      });

      it('resets the producer sequence on acquire but not the offsets', async () => {
        const result = await run(
          Effect.gen(function* () {
            const { store, append } = yield* open('handover');
            const first = yield* append(0, [text('one')]);

            const second = yield* store.acquire('handover', producer2);
            const next = yield* store.append({
              path: 'handover',
              producerId: producer2,
              epoch: second.epoch,
              sequence: second.nextSequence,
              records: [text('two')],
            });

            return { first, next, nextSequence: second.nextSequence };
          }),
        );

        expect(result.nextSequence).toBe(0);
        expect(LogOffset.isAfter(result.next, result.first)).toBe(true);
      });
    });

    // The opening tick is the only way a reader can learn that its
    // subscription took effect, and `Tail` depends on it to start at all. A
    // backend that omits it produces a tail that never emits its own
    // history — a hang, not a failure, and therefore worth its own case
    // rather than being diagnosed through the test below.
    it('emits an opening wake-up once subscribed', async () => {
      const ticks = await run(
        Effect.gen(function* () {
          const store = yield* LogStore.Service;
          yield* store.create('opening', 'identity');
          return yield* store
            .changes('opening')
            .pipe(Stream.take(1), Stream.runCollect);
        }),
      );

      expect(ticks.length).toBe(1);
    });

    // The wake-up path, tested through `Tail` because that is the only thing
    // that consumes it — and because a backend whose `changes` never fires
    // produces a tail that hangs rather than a read that fails.
    it('wakes a caught-up tail when a record arrives', async () => {
      const collected = await run(
        Effect.gen(function* () {
          const { append } = yield* open('tailed');
          yield* append(0, [text('one')]);

          const emitted = yield* Deferred.make<void>();
          const fiber = yield* Tail.from('tailed', LogOffset.START).pipe(
            Stream.tap(() => Deferred.succeed(emitted, undefined)),
            Stream.take(2),
            Stream.runCollect,
            Effect.forkChild,
          );

          // Only append the second record once the tail has emitted the
          // first, so the record cannot be picked up by the initial
          // catch-up. Reaching it requires a wake-up.
          yield* Deferred.await(emitted);
          yield* append(1, [text('two')]);

          return yield* Fiber.join(fiber);
        }),
      );

      expect(texts(collected)).toEqual(['one', 'two']);
    });

    // The counterpart of the wake-up case. A change feed that stops
    // delivering while looking healthy is indistinguishable from a
    // conversation where nothing is happening, which is the shape of every
    // silent-stall bug this family has had. It must surface.
    const failing = options.layerWithFailingChanges;
    if (failing === undefined) {
      it.skip('surfaces a dead change feed to a tail (needs layerWithFailingChanges)', () => {});
    } else {
      it('surfaces a dead change feed to a tail rather than hanging', async () => {
        const outcome = await runIn(
          failing('dead-feed'),
          Effect.gen(function* () {
            const store = yield* LogStore.Service;
            yield* store.create('dead-feed', 'identity');
            return yield* Tail.from('dead-feed', LogOffset.START).pipe(
              Stream.runCollect,
              Effect.result,
            );
          }),
        );

        expect(outcome._tag).toBe('Failure');
        if (outcome._tag === 'Failure') {
          expect(outcome.failure).toMatchObject({ operation: 'changes' });
        }
      });
    }

    // A payload a backend could not have written must fail the append, not
    // be dropped or stored half-formed. `Schema.Unknown` is the hole through
    // which one arrives.
    it('rejects records that cannot be encoded for persistence', async () => {
      const outcome = await run(
        Effect.gen(function* () {
          const { append } = yield* open('unencodable');
          return yield* append(0, [
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
        }),
      );

      expect(outcome._tag).toBe('Failure');
      if (outcome._tag === 'Failure') {
        expect(outcome.failure).toMatchObject({
          operation: 'append',
          reason: 'encoding',
        });
      }
    });

    it('rejects every value JSON would alter or discard', async () => {
      const cyclic: { self?: unknown } = {};
      cyclic.self = cyclic;
      const sparse: unknown[] = [];
      sparse.length = 1;
      const symbolProperty = { okay: true } as Record<PropertyKey, unknown>;
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

      const outcomes = await run(
        Effect.gen(function* () {
          const { append } = yield* open('all-lossy-values');
          return yield* Effect.forEach(lossy, (params) =>
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
        }),
      );

      expect(outcomes).toHaveLength(lossy.length);
      for (const outcome of outcomes) {
        expect(outcome._tag).toBe('Failure');
        if (outcome._tag === 'Failure') {
          expect(outcome.failure).toMatchObject({ reason: 'encoding' });
        }
      }
    });

    it('stores an encoded clone rather than caller-owned objects', async () => {
      const params = {
        nested: { value: 'before' },
        ['__proto__']: { retained: true },
      };
      const read = await run(
        Effect.gen(function* () {
          const { store, append } = yield* open('encoded-clone');
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
          return yield* store.read('encoded-clone');
        }),
      );

      expect(read.records[0]?.record).toMatchObject({
        params: { nested: { value: 'before' } },
      });
      const record = read.records[0]?.record;
      expect(record?._tag).toBe('ToolCall');
      if (record?._tag === 'ToolCall') {
        const stored = record.params as Record<string, unknown>;
        expect(Object.hasOwn(stored, '__proto__')).toBe(true);
        expect(stored['__proto__']).toEqual({ retained: true });
      }
    });

    it('treats object insertion order as irrelevant to retry identity', async () => {
      const result = await run(
        Effect.gen(function* () {
          const { append } = yield* open('canonical-key-order');
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
          return { first, retry };
        }),
      );

      expect(result.retry).toBe(result.first);
    });

    it('rejects invalid read limits and malformed offsets', async () => {
      const outcomes = await run(
        Effect.gen(function* () {
          const { store } = yield* open('invalid-read-options');
          return yield* Effect.all([
            store
              .read('invalid-read-options', { limit: 0 })
              .pipe(Effect.result),
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
                after: 'malformed' as unknown as LogOffset.Offset,
              })
              .pipe(Effect.result),
          ]);
        }),
      );

      for (const outcome of outcomes) {
        expect(outcome._tag).toBe('Failure');
        if (outcome._tag === 'Failure') {
          expect(outcome.failure).toMatchObject({
            operation: 'read',
            reason: 'invalid',
          });
        }
      }
    });

    describe('stream lifecycle', () => {
      it('reports meta for a created stream', async () => {
        const meta = await run(
          Effect.gen(function* () {
            const { store, append } = yield* open('meta');
            yield* append(0, [text('one'), text('two')]);
            return yield* store.meta('meta');
          }),
        );

        expect(Option.isSome(meta)).toBe(true);
        expect(Option.getOrThrow(meta)).toMatchObject({
          path: 'meta',
          identity: 'identity:meta',
          records: 2,
        });
      });

      it('returns None for a stream that was never created', async () => {
        const meta = await run(
          Effect.gen(function* () {
            const store = yield* LogStore.Service;
            return yield* store.meta('absent');
          }),
        );

        expect(Option.isNone(meta)).toBe(true);
      });

      it('rejects creating a stream twice', async () => {
        const outcome = await run(
          Effect.gen(function* () {
            const store = yield* LogStore.Service;
            yield* store.create('twice', 'identity');
            return yield* store.create('twice', 'identity').pipe(Effect.result);
          }),
        );

        expect(outcome._tag).toBe('Failure');
        if (outcome._tag === 'Failure') {
          expect(outcome.failure).toMatchObject({
            operation: 'create',
            reason: 'conflict',
          });
        }
      });

      // A typo in a conversation id must not quietly start a second,
      // parallel history. That is why creation is explicit.
      it('fails to read a stream that was never created', async () => {
        const outcome = await run(
          Effect.gen(function* () {
            const store = yield* LogStore.Service;
            return yield* store.read('never').pipe(Effect.result);
          }),
        );

        expect(outcome._tag).toBe('Failure');
        if (outcome._tag === 'Failure') {
          expect(outcome.failure).toMatchObject({
            operation: 'read',
            reason: 'not_found',
          });
        }
      });

      it('fails to acquire a stream that was never created', async () => {
        const outcome = await run(
          Effect.gen(function* () {
            const store = yield* LogStore.Service;
            return yield* store.acquire('never', producer1).pipe(Effect.result);
          }),
        );

        expect(outcome._tag).toBe('Failure');
        if (outcome._tag === 'Failure') {
          expect(outcome.failure).toMatchObject({
            operation: 'acquire',
            reason: 'not_found',
          });
        }
      });
    });
  });
};

export * as LogStoreContract from './log-store-contract.js';
