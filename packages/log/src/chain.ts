import { Effect, Option, Schema, type Crypto } from 'effect';

import { LogStore } from './log-store.js';
import { LogOffset } from './offset.js';
import type { ConversationRecord } from './record.js';
import { RecordBatch } from './record-batch.js';
import type { LogVocabulary } from './vocabulary.js';

// A hash chain over each stream's batches, and the verifier for it.
//
// A batch's fingerprint (see `./record-batch.ts`) is a digest of its records
// alone, kept to recognise a producer's retry. It says nothing about order,
// so deleting, reordering, or replaying batches leaves every fingerprint
// intact. The chained digest closes that: each batch's digest commits to the
// digest of the batch before it, so the newest digest -- the stream's
// `chainHead` -- commits to the entire history. Pin that one value somewhere
// the log's operator cannot rewrite and any later edit is detectable by a
// party that trusts neither the runtime nor the database.
//
// ## What is hashed
//
// Every digest is SHA-256, lowercase hex, over the UTF-8 bytes of a JSON
// array whose first element is a version tag. JSON arrays of strings and
// integers-as-strings are unambiguous, so no field can bleed into another:
//
//   genesis  ["vesper.log.chain.genesis.v1", path, identity]
//   batch    ["vesper.log.chain.batch.v1", previousDigest, firstOffset,
//             producerId, epoch, sequence, canonicalBatch]
//
// `previousDigest` is the genesis digest for a stream's first batch.
// `canonicalBatch` is `RecordBatch.prepare(...).encoded`: the canonical JSON
// text of the batch's entries, covering every record's conversation id,
// timestamp, and body. `firstOffset` plus the entry count fixes the offset of
// every record, so a record cannot move. `epoch` (as a decimal string) and
// `sequence` are the producer coordinates the store also enforces. Path and
// identity are committed by the genesis digest, so a stream cannot be copied
// to another path or relabelled and still verify.
//
// Not covered, and therefore not tamper-evident: `written_at`-style storage
// bookkeeping, the stream's current epoch / producer holder (they govern
// future appends, not history), and the retry fingerprint.
//
// ## Ordering
//
// The digest is computed inside the append's critical section (the memory
// store's mutex, the SQLite transaction, the Postgres `FOR UPDATE` row lock)
// from the head stored on the stream row, and written atomically with the
// batch. The producer fencing that already serialises appends therefore also
// makes the chain linear; there is no second ordering mechanism.
//
// ## Limits
//
// The chain proves internal consistency. An adversary who can rewrite the
// whole store can recompute every digest, and truncating the newest batches
// leaves a valid shorter chain. Both are caught only by comparing the head
// against a value kept elsewhere: pass it as `expectedHead`.

const GENESIS_TAG = 'vesper.log.chain.genesis.v1';
const BATCH_TAG = 'vesper.log.chain.batch.v1';

/** The digest every stream's first batch chains from. */
export const genesis = (
  path: string,
  identity: string,
): Effect.Effect<string, RecordBatch.EncodeError, Crypto.Crypto> =>
  RecordBatch.sha256(JSON.stringify([GENESIS_TAG, path, identity]));

export interface BatchLink {
  readonly previous: string;
  readonly firstOffset: LogOffset.Offset;
  readonly producerId: LogVocabulary.ProducerId;
  readonly epoch: LogVocabulary.Epoch;
  readonly sequence: LogVocabulary.ProducerSequence;
  /** `RecordBatch.prepare(...).encoded` of the batch. */
  readonly encoded: string;
}

/** The chained digest of one batch. */
export const link = (
  input: BatchLink,
): Effect.Effect<string, RecordBatch.EncodeError, Crypto.Crypto> =>
  RecordBatch.sha256(
    JSON.stringify([
      BATCH_TAG,
      input.previous,
      input.firstOffset,
      input.producerId,
      String(input.epoch),
      String(input.sequence),
      input.encoded,
    ]),
  );

export interface AppendLink extends Omit<BatchLink, 'previous'> {
  readonly path: string;
  readonly identity: string;
  /** The stream's stored chain head; empty when it has no batch yet. */
  readonly head: string;
}

/**
 * The digest to store for a batch about to be appended, derived from the
 * stream row an append already holds locked. Backends call this and nothing
 * else; the previous-digest rule (head, else genesis)
 * lives here once.
 */
export const forAppend = (
  input: AppendLink,
): Effect.Effect<string, LogStore.LogStoreError, Crypto.Crypto> =>
  Effect.gen(function* () {
    const previous =
      input.head !== ''
        ? input.head
        : yield* genesis(input.path, input.identity);
    return yield* link({ ...input, previous });
  }).pipe(
    Effect.mapError((error) =>
      LogStore.makeError(
        input.path,
        'append',
        'storage',
        `cannot compute chain digest: ${error.detail}`,
      ),
    ),
  );

/** One stored record plus the batch coordinates a backend keeps per row. */
export interface StoredRow {
  readonly envelope: ConversationRecord.Envelope;
  readonly producerId: LogVocabulary.ProducerId;
  readonly epoch: LogVocabulary.Epoch;
  readonly sequence: LogVocabulary.ProducerSequence;
  readonly batchIndex: number;
  /** The digest of the batch this row belongs to. */
  readonly digest: string;
}

/**
 * Group offset-ordered rows into batches. A new batch starts at batch index
 * zero or whenever the producer coordinates change.
 */
export const groupRows = (
  rows: ReadonlyArray<StoredRow>,
): ReadonlyArray<LogStore.StoredBatch> => {
  const batches: LogStore.StoredBatch[] = [];
  let current:
    | { row: StoredRow; records: ConversationRecord.Envelope[] }
    | undefined;
  const flush = () => {
    if (current !== undefined) {
      batches.push({
        producerId: current.row.producerId,
        epoch: current.row.epoch,
        sequence: current.row.sequence,
        records: current.records,
        digest: current.row.digest,
      });
    }
  };
  for (const row of rows) {
    if (
      current === undefined ||
      row.batchIndex === 0 ||
      row.producerId !== current.row.producerId ||
      row.epoch !== current.row.epoch ||
      row.sequence !== current.row.sequence
    ) {
      flush();
      current = { row, records: [] };
    }
    current.records.push(row.envelope);
  }
  flush();
  return batches;
};

/** Why verification stopped. */
export const BreakReason = Schema.Literals([
  /** A batch's records or producer coordinates do not hash to its digest. */
  'content_modified',
  /** Offsets skip ahead: a batch or record was removed. */
  'offset_gap',
  /** Offsets repeat or go backwards: a batch was inserted, duplicated or reordered. */
  'offset_overlap',
  /** The recomputed head differs from the stored or pinned head. */
  'head_mismatch',
  /** The stored shape cannot be a batch (empty, no digest, undecodable offsets). */
  'malformed',
]);
export type BreakReason = typeof BreakReason.Type;

/** The first place a stream stops verifying. */
export class Broken extends Schema.TaggedError<Broken>(
  '@sunfall/vesper-log/ChainBroken',
)('Broken', {
  /** Offset of the first record that cannot be trusted. */
  offset: LogOffset.Offset,
  reason: BreakReason,
  detail: Schema.String,
}) {}

/** A stream whose chain recomputes end to end. */
export interface Valid {
  readonly _tag: 'Valid';
  /** Newest digest, or none for an empty stream. Pin this. */
  readonly head: Option.Option<string>;
  readonly batches: number;
  readonly records: number;
}

export type Result = Valid | Broken;

export interface VerifyOptions {
  /** A head pinned elsewhere; the stream must end exactly at it. */
  readonly expectedHead?: string;
}

interface Position {
  readonly path: string;
  readonly identity: string;
}

interface State {
  readonly head: Option.Option<string>;
  /** Sequence the next record must have. */
  readonly nextSeq: bigint;
  readonly batches: number;
  readonly records: number;
}

const initial: State = {
  head: Option.none(),
  nextSeq: 0n,
  batches: 0,
  records: 0,
};

const lastVerified = (state: State): LogOffset.Offset =>
  state.nextSeq === 0n
    ? LogOffset.START
    : LogOffset.fromSeq(state.nextSeq - 1n);

const entryOf = (
  envelope: ConversationRecord.Envelope,
): ConversationRecord.Entry => ({
  conversationId: envelope.conversationId,
  timestamp: envelope.timestamp,
  record: envelope.record,
});

/** Check one batch against the running state; fails with {@link Broken}. */
const step = (
  position: Position,
  state: State,
  batch: LogStore.StoredBatch,
): Effect.Effect<State, Broken, Crypto.Crypto> =>
  Effect.gen(function* () {
    const broken = (
      offset: LogOffset.Offset,
      reason: BreakReason,
      detail: string,
    ) => new Broken({ offset, reason, detail });
    const expectedOffset = LogOffset.fromSeq(state.nextSeq);

    const first = batch.records[0];
    if (first === undefined) {
      return yield* broken(expectedOffset, 'malformed', 'batch has no records');
    }

    let seq = state.nextSeq;
    for (const envelope of batch.records) {
      const actual = yield* LogOffset.toSeq(envelope.offset).pipe(
        Effect.mapError(() =>
          broken(expectedOffset, 'malformed', 'record offset is malformed'),
        ),
      );
      if (actual > seq) {
        return yield* broken(
          LogOffset.fromSeq(seq),
          'offset_gap',
          `expected offset ${LogOffset.fromSeq(seq)}, found ${envelope.offset}: records are missing`,
        );
      }
      if (actual < seq) {
        return yield* broken(
          envelope.offset,
          'offset_overlap',
          `offset ${envelope.offset} was already accounted for: records were inserted, repeated, or reordered`,
        );
      }
      seq += 1n;
    }
    const counted = {
      batches: state.batches + 1,
      records: state.records + batch.records.length,
      nextSeq: seq,
    };

    if (batch.digest === '') {
      return yield* broken(first.offset, 'malformed', 'batch has no digest');
    }

    const wrap = (error: RecordBatch.EncodeError) =>
      broken(first.offset, 'malformed', error.detail);
    const previous = Option.isSome(state.head)
      ? state.head.value
      : yield* genesis(position.path, position.identity).pipe(
          Effect.mapError(wrap),
        );
    // Entries are re-prepared exactly as an append prepared them, so the
    // canonical bytes depend only on content, not on how a backend stored it.
    const prepared = yield* RecordBatch.prepare(
      batch.records.map(entryOf),
    ).pipe(
      Effect.mapError((error) =>
        broken(first.offset, 'content_modified', error.detail),
      ),
    );
    const recomputed = yield* link({
      previous,
      firstOffset: first.offset,
      producerId: batch.producerId,
      epoch: batch.epoch,
      sequence: batch.sequence,
      encoded: prepared.encoded,
    }).pipe(Effect.mapError(wrap));
    if (recomputed !== batch.digest) {
      return yield* broken(
        first.offset,
        'content_modified',
        `batch at ${first.offset} does not hash to its stored digest: its records or producer coordinates were altered, or it was swapped with another batch`,
      );
    }
    return { ...state, ...counted, head: Option.some(recomputed) };
  });

const finish = (state: State, options?: VerifyOptions): Result => {
  if (
    options?.expectedHead !== undefined &&
    !Option.contains(state.head, options.expectedHead)
  ) {
    return new Broken({
      offset: lastVerified(state),
      reason: 'head_mismatch',
      detail: `chain ends at ${Option.getOrElse(state.head, () => 'no batch')}, expected ${options.expectedHead}: the stream was truncated, replaced, or the pin is stale`,
    });
  }
  return {
    _tag: 'Valid',
    head: state.head,
    batches: state.batches,
    records: state.records,
  };
};

/**
 * Recompute a stream's chain from its batches as stored.
 *
 * Pure apart from hashing: nothing is read from or trusted of the store.
 * Batches must be the stream's complete history in offset order. The result
 * is the first break found, or the head to pin.
 */
export const verifyBatches = (
  position: Position,
  batches: Iterable<LogStore.StoredBatch>,
  options?: VerifyOptions,
): Effect.Effect<Result, never, Crypto.Crypto> =>
  Effect.gen(function* () {
    let state = initial;
    for (const batch of batches) {
      state = yield* step(position, state, batch);
    }
    return finish(state, options);
  }).pipe(Effect.catchTag('Broken', (broken) => Effect.succeed(broken)));

/** Page size for {@link verify}; bounded because a batch may hold 16,384 records. */
const VERIFY_BATCHES_PER_PAGE = 64;

/**
 * Verify a stream through a {@link LogStore.Service}.
 *
 * Reads the stream in order, recomputes every digest, and also checks the
 * result against the head the store reports, so a store whose stored head
 * and batches disagree is a `head_mismatch`. Provide `expectedHead` to pin
 * against a value held outside the store.
 */
export const verify = (
  path: string,
  options?: VerifyOptions,
): Effect.Effect<
  Result,
  LogStore.LogStoreError,
  LogStore.Service | Crypto.Crypto
> =>
  Effect.gen(function* () {
    const store = yield* LogStore.Service;
    const found = yield* store.meta(path);
    if (Option.isNone(found)) {
      return yield* LogStore.makeError(
        path,
        'meta',
        'not_found',
        'no stream at this path',
      );
    }
    const position = { path, identity: found.value.identity };
    let state = initial;
    let after = LogOffset.START;
    let upToDate = false;
    while (!upToDate) {
      const page = yield* store.readBatches(path, {
        after,
        limit: VERIFY_BATCHES_PER_PAGE,
      });
      for (const batch of page.batches) {
        state = yield* step(position, state, batch);
      }
      after = page.cursor;
      upToDate = page.upToDate;
    }
    const checked = finish(state, options);
    if (checked._tag === 'Broken') {
      return checked;
    }

    // The stream row's own account of its head must match what the batches
    // recompute to. Skipped when an append raced the read, since the two
    // snapshots then legitimately differ.
    const latest = yield* store.meta(path);
    if (
      Option.isSome(latest) &&
      latest.value.head === found.value.head &&
      (latest.value.head !== lastVerified(state) ||
        Option.getOrUndefined(latest.value.chainHead) !==
          Option.getOrUndefined(state.head))
    ) {
      return new Broken({
        offset: lastVerified(state),
        reason: 'head_mismatch',
        detail: `stream reports head ${latest.value.head} (${Option.getOrElse(latest.value.chainHead, () => 'empty')}) but its batches recompute to ${lastVerified(state)} (${Option.getOrElse(state.head, () => 'empty')})`,
      });
    }
    return checked;
  }).pipe(
    Effect.catchTag('Broken', (broken) => Effect.succeed<Result>(broken)),
  );

export * as LogChain from './chain.js';
