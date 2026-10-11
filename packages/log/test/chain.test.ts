import * as NodeServices from '@effect/platform-node/NodeServices';
import { describe, expect, it } from '@effect/vitest';
import { Effect, Layer, Option } from 'effect';

import { LogChain } from '../src/chain.js';
import { LogStoreMemory } from '../src/layer-memory.js';
import { LogStore } from '../src/log-store.js';
import { LogOffset } from '../src/offset.js';
import type { ConversationRecord } from '../src/record.js';
import { LogVocabulary } from '../src/vocabulary.js';

const PATH = 'chain-stream';
const IDENTITY = 'identity:chain-stream';
const conversationId = LogVocabulary.ConversationId.make('conversation-1');
const producer = LogVocabulary.ProducerId.make('producer-1');

const text = (value: string): ConversationRecord.Entry => ({
  conversationId,
  timestamp: 1_700_000_000_000,
  record: { _tag: 'Text', step: 1, text: value },
});

const at = <T>(values: ReadonlyArray<T>, index: number): T => {
  const value = values[index];
  if (value === undefined) {
    throw new Error(`missing value at index ${String(index)}`);
  }
  return value;
};

const memory = LogStoreMemory.layer.pipe(Layer.provide(NodeServices.layer));
// The store plus the `Crypto` the verifier needs, provided in one step.
const env = Layer.merge(memory, NodeServices.layer);

/** Append `batches` (each a list of texts) and return what is stored. */
const written = (batches: ReadonlyArray<ReadonlyArray<string>>) =>
  Effect.gen(function* () {
    const store = yield* LogStore.Service;
    yield* store.create(PATH, IDENTITY);
    const claim = yield* store.acquire(PATH, producer);
    for (const [index, batch] of batches.entries()) {
      yield* store.append({
        path: PATH,
        producerId: claim.producerId,
        epoch: claim.epoch,
        sequence: LogVocabulary.ProducerSequence.make(index),
        records: batch.map(text),
      });
    }
    return {
      page: yield* store.readBatches(PATH),
      meta: yield* store.meta(PATH),
    };
  });

const verify = (
  batches: Iterable<LogStore.StoredBatch>,
  options?: LogChain.VerifyOptions,
) =>
  LogChain.verifyBatches({ path: PATH, identity: IDENTITY }, batches, options);

const rebuild = (
  batch: LogStore.StoredBatch,
  change: {
    readonly records?: ReadonlyArray<ConversationRecord.Envelope>;
    readonly sequence?: LogVocabulary.ProducerSequence;
    readonly digest?: string;
  },
): LogStore.StoredBatch => ({
  producerId: batch.producerId,
  epoch: batch.epoch,
  sequence: change.sequence ?? batch.sequence,
  records: change.records ?? batch.records,
  digest: change.digest ?? batch.digest,
});

const reenvelope = (
  envelope: ConversationRecord.Envelope,
  change: {
    readonly offset?: LogOffset.Offset;
    readonly record?: ConversationRecord.Record;
  },
): ConversationRecord.Envelope => ({
  offset: change.offset ?? envelope.offset,
  conversationId: envelope.conversationId,
  timestamp: envelope.timestamp,
  record: change.record ?? envelope.record,
});

const textRecord = (value: string): ConversationRecord.Record => ({
  _tag: 'Text',
  step: 1,
  text: value,
});

const rewriteText = (
  batch: LogStore.StoredBatch,
  replacement: string,
): LogStore.StoredBatch =>
  rebuild(batch, {
    records: batch.records.map((envelope, index) =>
      index === 0
        ? reenvelope(envelope, { record: textRecord(replacement) })
        : envelope,
    ),
  });

const offsetOf = (seq: number) => LogOffset.fromSeq(BigInt(seq));

const stored = written([['a', 'b'], ['c'], ['d', 'e'], ['f']]).pipe(
  Effect.map(({ page, meta }) => ({
    batches: page.batches,
    head: Option.flatMap(meta, (found) => found.chainHead),
  })),
  Effect.provide(memory),
);

describe('LogChain.verifyBatches', () => {
  it.effect('accepts an untouched log and reports the stored head', () =>
    Effect.gen(function* () {
      const { batches, head } = yield* stored;
      const result = yield* verify(batches);
      expect(result).toMatchObject({ _tag: 'Valid', batches: 4, records: 6 });
      expect(Option.isSome(head)).toBe(true);
      expect(result._tag === 'Valid' ? result.head : Option.none()).toEqual(
        head,
      );
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect('derives the same head for the same history every time', () =>
    Effect.gen(function* () {
      const first = yield* stored;
      const second = yield* stored;
      expect(first.head).toEqual(second.head);
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect('detects an edited record at the batch that holds it', () =>
    Effect.gen(function* () {
      const { batches } = yield* stored;
      const edited = batches.map((batch, index) =>
        index === 2 ? rewriteText(batch, 'tampered') : batch,
      );
      const result = yield* verify(edited);
      expect(result).toMatchObject({
        _tag: 'Broken',
        offset: offsetOf(3),
        reason: 'content_modified',
      });
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect('detects an edit to a later record inside a batch', () =>
    Effect.gen(function* () {
      const { batches } = yield* stored;
      const edited = batches.map((batch, index) =>
        index === 0
          ? rebuild(batch, {
              records: batch.records.map((envelope, position) =>
                position === 1
                  ? reenvelope(envelope, { record: textRecord('x') })
                  : envelope,
              ),
            })
          : batch,
      );
      expect(yield* verify(edited)).toMatchObject({
        _tag: 'Broken',
        offset: offsetOf(0),
        reason: 'content_modified',
      });
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect('detects an edited producer coordinate', () =>
    Effect.gen(function* () {
      const { batches } = yield* stored;
      const edited = batches.map((batch, index) =>
        index === 1
          ? rebuild(batch, {
              sequence: LogVocabulary.ProducerSequence.make(9),
            })
          : batch,
      );
      expect(yield* verify(edited)).toMatchObject({
        _tag: 'Broken',
        offset: offsetOf(2),
        reason: 'content_modified',
      });
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect('reports a deleted batch where the gap opens', () =>
    Effect.gen(function* () {
      const { batches } = yield* stored;
      expect(
        yield* verify(batches.filter((_, index) => index !== 1)),
      ).toMatchObject({
        _tag: 'Broken',
        offset: offsetOf(2),
        reason: 'offset_gap',
      });
      expect(yield* verify(batches.slice(1))).toMatchObject({
        _tag: 'Broken',
        offset: offsetOf(0),
        reason: 'offset_gap',
      });
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect('detects two batches swapped, by position or by content', () =>
    Effect.gen(function* () {
      const { batches } = yield* stored;
      const swappedPositions = [
        at(batches, 0),
        at(batches, 2),
        at(batches, 1),
        at(batches, 3),
      ];
      expect(yield* verify(swappedPositions)).toMatchObject({
        _tag: 'Broken',
        offset: offsetOf(2),
        reason: 'offset_gap',
      });

      // Swap what the batches contain but keep each one's offsets.
      const swappedContent = [
        at(batches, 0),
        rebuild(at(batches, 2), {
          records: at(batches, 2)
            .records.slice(0, 1)
            .map((envelope) => reenvelope(envelope, { offset: offsetOf(2) })),
        }),
        rebuild(at(batches, 1), {
          records: at(batches, 1).records.map((envelope) =>
            reenvelope(envelope, { offset: offsetOf(3) }),
          ),
        }),
        at(batches, 3),
      ];
      expect(yield* verify(swappedContent)).toMatchObject({
        _tag: 'Broken',
        offset: offsetOf(2),
        reason: 'content_modified',
      });
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect('detects a repeated or inserted batch', () =>
    Effect.gen(function* () {
      const { batches } = yield* stored;
      expect(
        yield* verify([
          at(batches, 0),
          at(batches, 1),
          at(batches, 1),
          at(batches, 2),
        ]),
      ).toMatchObject({
        _tag: 'Broken',
        offset: offsetOf(2),
        reason: 'offset_overlap',
      });
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect('only a pinned head exposes truncation of the newest batches', () =>
    Effect.gen(function* () {
      const { batches, head } = yield* stored;
      const truncated = batches.slice(0, 3);
      expect(yield* verify(truncated)).toMatchObject({ _tag: 'Valid' });
      expect(
        yield* verify(truncated, { expectedHead: Option.getOrThrow(head) }),
      ).toMatchObject({
        _tag: 'Broken',
        offset: offsetOf(4),
        reason: 'head_mismatch',
      });
      expect(
        yield* verify(batches, { expectedHead: Option.getOrThrow(head) }),
      ).toMatchObject({ _tag: 'Valid' });
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect('treats a batch without a digest as malformed', () =>
    Effect.gen(function* () {
      const { batches } = yield* stored;
      expect(
        yield* verify(
          batches.map((batch, index) =>
            index === 0 ? rebuild(batch, { digest: '' }) : batch,
          ),
        ),
      ).toMatchObject({
        _tag: 'Broken',
        offset: offsetOf(0),
        reason: 'malformed',
      });
    }).pipe(Effect.provide(NodeServices.layer)),
  );
});

describe('LogChain.verify', () => {
  it.effect('verifies through the store and fails on a missing stream', () =>
    Effect.gen(function* () {
      yield* written([['a'], ['b']]);
      expect(yield* LogChain.verify(PATH)).toMatchObject({
        _tag: 'Valid',
        batches: 2,
      });
      const missing = yield* LogChain.verify('nope').pipe(Effect.flip);
      expect(missing).toMatchObject({ reason: 'not_found' });
    }).pipe(Effect.provide(env)),
  );

  it.effect('flags a store whose batches no longer match its head', () => {
    const truncating = Layer.effect(
      LogStore.Service,
      Effect.map(LogStore.Service, (store) =>
        LogStore.Service.of({
          ...store,
          readBatches: (path, options) =>
            Effect.map(store.readBatches(path, options), (page) => ({
              ...page,
              batches: page.batches.slice(0, -1),
            })),
        }),
      ),
    ).pipe(Layer.provideMerge(env));
    return Effect.gen(function* () {
      yield* written([['a'], ['b'], ['c']]);
      expect(yield* LogChain.verify(PATH)).toMatchObject({
        _tag: 'Broken',
        reason: 'head_mismatch',
      });
    }).pipe(Effect.provide(truncating));
  });
});
