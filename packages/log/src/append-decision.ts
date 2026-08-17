import { Effect, Schema } from 'effect';

import { LogStore } from './log-store.js';
import { ConversationRecord } from './record.js';
import { RecordBatch } from './record-batch.js';
import { LogVocabulary } from './vocabulary.js';

/**
 * The backend-independent part of an append.
 *
 * The memory and Postgres adapters deliberately keep their own critical
 * sections: one mutates an in-process array and the other holds a locked row
 * inside a transaction. They must nevertheless agree on the wire checks,
 * producer fencing, retry identity, and sequence progression. This module
 * owns those decisions without knowing how an accepted batch is persisted.
 */

type Operation = LogStore.LogStoreError['operation'];
type Reason = LogStore.LogStoreError['reason'];

export type Failure = (
  path: string,
  operation: Operation,
  reason: Reason,
  detail: string,
) => LogStore.LogStoreError;

export interface ValidatedInput {
  readonly path: string;
  readonly producerId: LogVocabulary.ProducerId;
  readonly epoch: LogVocabulary.Epoch;
  readonly sequence: LogVocabulary.ProducerSequence;
  readonly records: ReadonlyArray<ConversationRecord.Entry>;
}

export interface FencingState {
  readonly epoch: LogVocabulary.Epoch;
  readonly producerId: LogVocabulary.ProducerId | undefined;
  /** The next producer sequence accepted by this stream. */
  readonly nextSequence: LogVocabulary.ProducerSequence;
  readonly lastFingerprint: string;
}

export interface AppendDecision {
  readonly kind: 'retry' | 'append';
  readonly prepared: RecordBatch.PreparedBatch;
  /** The next producer sequence after an accepted append. */
  readonly nextSequence: LogVocabulary.ProducerSequence;
}

/**
 * Cheap wire validation, performed before either adapter enters its write
 * section.
 */
export const validateInput = (
  input: LogStore.AppendInput,
  failure: Failure,
): Effect.Effect<ValidatedInput, LogStore.LogStoreError> =>
  Effect.gen(function* () {
    if (input.records.length === 0) {
      return yield* Effect.fail(
        failure(input.path, 'append', 'empty', 'append carried no records'),
      );
    }

    const sequence = yield* Schema.decodeUnknownEffect(
      LogVocabulary.ProducerSequence,
    )(input.sequence).pipe(
      Effect.mapError(() =>
        failure(
          input.path,
          'append',
          'conflict',
          `sequence ${input.sequence} is not a safe natural integer`,
        ),
      ),
    );
    const epoch = yield* Schema.decodeUnknownEffect(LogVocabulary.Epoch)(
      input.epoch,
    ).pipe(
      Effect.mapError(() =>
        failure(
          input.path,
          'append',
          'conflict',
          `epoch ${input.epoch} is not a safe natural integer`,
        ),
      ),
    );
    const producerId = yield* Schema.decodeUnknownEffect(
      LogVocabulary.ProducerId,
    )(input.producerId).pipe(
      Effect.mapError(() =>
        failure(
          input.path,
          'append',
          'conflict',
          'producer id must be non-empty',
        ),
      ),
    );

    return {
      path: input.path,
      producerId,
      epoch,
      sequence,
      records: input.records,
    } satisfies ValidatedInput;
  });

/**
 * Decide whether a validated append is fenced, a retry, a sequence error, or
 * a new batch. Preparation intentionally follows the identity checks: an
 * invalid payload from a stale producer must still report `fenced`.
 */
export const decide = (
  input: ValidatedInput,
  state: FencingState,
  failure: Failure,
): Effect.Effect<AppendDecision, LogStore.LogStoreError> =>
  Effect.gen(function* () {
    const reject = (reason: Reason, detail: string) =>
      Effect.fail(failure(input.path, 'append', reason, detail));

    if (input.epoch !== state.epoch) {
      return yield* reject(
        'fenced',
        `epoch ${input.epoch} is not the current epoch ${state.epoch}`,
      );
    }
    if (input.producerId !== state.producerId) {
      return yield* reject(
        'conflict',
        `producer ${input.producerId} does not hold epoch ${state.epoch}`,
      );
    }

    const prepared = yield* RecordBatch.prepare(input.records).pipe(
      Effect.mapError((error) =>
        failure(input.path, 'append', 'encoding', error.detail),
      ),
    );

    if (state.nextSequence > 0 && input.sequence === state.nextSequence - 1) {
      if (prepared.fingerprint !== state.lastFingerprint) {
        return yield* reject(
          'conflict',
          `sequence ${input.sequence} was reused with different content`,
        );
      }
      return {
        kind: 'retry',
        prepared,
        nextSequence: state.nextSequence,
      } satisfies AppendDecision;
    }

    if (input.sequence !== state.nextSequence) {
      return yield* reject(
        input.sequence > state.nextSequence ? 'gap' : 'conflict',
        `expected sequence ${state.nextSequence}, got ${input.sequence}`,
      );
    }

    return {
      kind: 'append',
      prepared,
      nextSequence: LogVocabulary.ProducerSequence.make(
        Number(input.sequence) + 1,
      ),
    } satisfies AppendDecision;
  });
