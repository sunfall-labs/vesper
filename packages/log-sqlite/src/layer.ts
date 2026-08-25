import { SqlClient } from 'effect/unstable/sql';
import type * as SqlError from 'effect/unstable/sql/SqlError';
import { Reactivity } from 'effect/unstable/reactivity/Reactivity';
import { Crypto, Effect, Layer, Option, Schema, Stream } from 'effect';

import { LogStoreAdapter } from '@sunfall/vesper-log/adapter';
import { LogStore } from '@sunfall/vesper-log/log-store';
import { LogOffset } from '@sunfall/vesper-log/offset';
import { RecordBatch } from '@sunfall/vesper-log/record-batch';
import type { ConversationRecord } from '@sunfall/vesper-log/record';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';

/**
 * SQLite LogStore backed by an Effect SqlClient.
 *
 * The adapter intentionally depends on the generic SqlClient rather than a
 * SQLite driver. Applications can therefore use the same native SQLite layer
 * for their other tables (including Outrider's Bun/Node adapter). SQLite has
 * no cross-process notification primitive, so `changes` uses Effect's
 * process-local Reactivity service; append invalidates the path after commit.
 */

type Row = Readonly<Record<string, unknown>>;

const REACTIVITY_KEY = 'vesper-log-sqlite';
const UnknownJson = Schema.fromJsonString(Schema.Unknown);

const asError =
  (path: string, operation: LogStore.Operation) =>
  (error: SqlError.SqlError | LogStore.LogStoreError): LogStore.LogStoreError =>
    Schema.is(LogStore.LogStoreError)(error)
      ? error
      : LogStore.makeError(path, operation, 'storage', error.message);

class RowDecodeError extends Schema.TaggedError<RowDecodeError>(
  '@sunfall/vesper-log-sqlite/RowDecodeError',
)('RowDecodeError', {
  field: Schema.String,
  value: Schema.Unknown,
  expected: Schema.String,
  message: Schema.String,
}) {}

const rowDecodeError = (
  field: string,
  value: unknown,
  expected: string,
): RowDecodeError =>
  new RowDecodeError({
    field,
    value,
    expected,
    message: `row field ${field} expected ${expected}, got ${value === null ? 'null' : typeof value}`,
  });

const asString = (
  field: string,
  value: unknown,
): Effect.Effect<string, RowDecodeError> =>
  typeof value === 'string'
    ? Effect.succeed(value)
    : Effect.fail(rowDecodeError(field, value, 'a string'));

const asNumber = (
  field: string,
  value: unknown,
): Effect.Effect<number, RowDecodeError> => {
  if (typeof value === 'number' && Number.isSafeInteger(value)) {
    return Effect.succeed(value);
  }
  if (typeof value === 'bigint') {
    const result = Number(value);
    return Number.isSafeInteger(result)
      ? Effect.succeed(result)
      : Effect.fail(rowDecodeError(field, value, 'a safe integer'));
  }
  if (typeof value === 'string' && /^[+-]?\d+$/.test(value)) {
    const result = Number(value);
    return Number.isSafeInteger(result)
      ? Effect.succeed(result)
      : Effect.fail(rowDecodeError(field, value, 'a safe integer'));
  }
  return Effect.fail(rowDecodeError(field, value, 'a safe integer'));
};

const asBigInt = (
  field: string,
  value: unknown,
): Effect.Effect<bigint, RowDecodeError> => {
  if (typeof value === 'bigint') {
    return Effect.succeed(value);
  }
  if (typeof value === 'number' && Number.isSafeInteger(value)) {
    return Effect.succeed(BigInt(value));
  }
  if (typeof value === 'string' && /^[+-]?\d+$/.test(value)) {
    return Effect.succeed(BigInt(value));
  }
  return Effect.fail(rowDecodeError(field, value, 'an integer'));
};

interface StreamRow {
  readonly identity: string;
  readonly epoch: LogVocabulary.Epoch;
  readonly producerId: LogVocabulary.ProducerId | undefined;
  readonly nextSequence: bigint;
  readonly nextProducerSequence: LogVocabulary.ProducerSequence;
  readonly lastFingerprint: string;
  readonly lastOffset: LogOffset.Offset;
}

const readStream = (row: Row): Effect.Effect<StreamRow, RowDecodeError> =>
  Effect.gen(function* () {
    const identity = yield* asString('identity', row['identity']);
    const epochNumber = yield* asNumber('epoch', row['epoch']);
    const epoch = Schema.is(Schema.Natural)(epochNumber)
      ? LogVocabulary.Epoch.make(epochNumber)
      : yield* rowDecodeError('epoch', epochNumber, 'a natural number');
    const producerIdValue =
      row['producer_id'] === null || row['producer_id'] === undefined
        ? undefined
        : yield* asString('producer_id', row['producer_id']);
    const producerId =
      producerIdValue === undefined
        ? undefined
        : producerIdValue.length > 0
          ? LogVocabulary.ProducerId.make(producerIdValue)
          : yield* rowDecodeError(
              'producer_id',
              producerIdValue,
              'a non-empty string',
            );
    const nextProducerSequenceNumber = yield* asNumber(
      'next_producer_sequence',
      row['next_producer_sequence'],
    );
    const nextProducerSequence = Schema.is(Schema.Natural)(
      nextProducerSequenceNumber,
    )
      ? LogVocabulary.ProducerSequence.make(nextProducerSequenceNumber)
      : yield* rowDecodeError(
          'next_producer_sequence',
          nextProducerSequenceNumber,
          'a natural number',
        );
    return {
      identity,
      epoch,
      producerId,
      nextProducerSequence,
      nextSequence: yield* asBigInt('next_sequence', row['next_sequence']),
      lastFingerprint: yield* asString(
        'last_fingerprint',
        row['last_fingerprint'],
      ),
      lastOffset: yield* LogOffset.decode(
        yield* asString('last_offset', row['last_offset']),
      ),
    };
  }).pipe(
    Effect.mapError((error) =>
      Schema.is(RowDecodeError)(error)
        ? error
        : rowDecodeError('row', error, `a valid stream row (${String(error)})`),
    ),
  );

const readRecord = (
  row: Row,
): Effect.Effect<
  ConversationRecord.Envelope,
  RowDecodeError | Schema.SchemaError
> =>
  Effect.gen(function* () {
    const encoded = row['record'];
    const record =
      typeof encoded === 'string'
        ? yield* Schema.decodeEffect(UnknownJson)(encoded).pipe(
            Effect.mapError((cause) => rowDecodeError('record', cause, 'JSON')),
          )
        : encoded;
    return yield* RecordBatch.decodeEnvelope({
      offset: yield* asString('record_offset', row['record_offset']),
      conversationId: yield* asString(
        'conversation_id',
        row['conversation_id'],
      ),
      timestamp: yield* asNumber('record_timestamp', row['record_timestamp']),
      record,
    });
  });

const metaOf = (path: string, row: StreamRow): LogStore.StreamMeta => ({
  path,
  identity: row.identity,
  epoch: row.epoch,
  producerId:
    row.producerId === undefined ? Option.none() : Option.some(row.producerId),
  head: row.lastOffset,
  records: Number(row.nextSequence),
});

export interface Options {
  /** Names are fixed by the published migration; this is for table prefixes. */
  readonly tablePrefix?: string;
}

/** Create the tables used by this adapter. Safe to run at every startup. */
export const migrate = (
  options?: Options,
): Effect.Effect<void, SqlError.SqlError, SqlClient.SqlClient> =>
  Effect.gen(function* () {
    const client = (yield* SqlClient.SqlClient).withoutTransforms();
    const prefix = options?.tablePrefix ?? 'vesper_log_';
    const streams = tableName(prefix, 'streams');
    const records = tableName(prefix, 'records');
    yield* client.unsafe(
      `CREATE TABLE IF NOT EXISTS ${streams} (
        path TEXT PRIMARY KEY,
        identity TEXT NOT NULL,
        epoch INTEGER NOT NULL DEFAULT 0,
        producer_id TEXT,
        next_sequence TEXT NOT NULL DEFAULT '0',
        next_producer_sequence INTEGER NOT NULL DEFAULT 0,
        last_fingerprint TEXT NOT NULL DEFAULT '',
        last_offset TEXT NOT NULL DEFAULT '-1'
      )`,
    ).unprepared;
    yield* client.unsafe(
      `CREATE TABLE IF NOT EXISTS ${records} (
        path TEXT NOT NULL,
        seq TEXT NOT NULL,
        record_offset TEXT NOT NULL,
        producer_id TEXT NOT NULL,
        producer_epoch INTEGER NOT NULL,
        producer_sequence INTEGER NOT NULL,
        batch_index INTEGER NOT NULL,
        conversation_id TEXT NOT NULL,
        record_timestamp INTEGER NOT NULL,
        record TEXT NOT NULL,
        PRIMARY KEY (path, record_offset)
      )`,
    ).unprepared;
    yield* client.unsafe(
      `CREATE UNIQUE INDEX IF NOT EXISTS ${records}_producer_batch_unique
       ON ${records} (path, producer_id, producer_epoch, producer_sequence, batch_index)`,
    ).unprepared;
    yield* client.unsafe(
      `CREATE INDEX IF NOT EXISTS ${records}_path_offset_idx
       ON ${records} (path, record_offset)`,
    ).unprepared;
  });

const tableName = (prefix: string, table: string): string => {
  if (!/^[a-z_][a-z0-9_]*$/.test(prefix)) {
    throw new Error(`Not a usable SQLite table prefix: ${prefix}`);
  }
  return `${prefix}${table}`;
};

export const make = (
  client: SqlClient.SqlClient,
  options?: Options,
): Effect.Effect<LogStore.Interface, never, Crypto.Crypto | Reactivity> =>
  Effect.gen(function* () {
    const crypto = yield* Crypto.Crypto;
    const reactivity = yield* Reactivity;
    const prefix = options?.tablePrefix ?? 'vesper_log_';
    const streams = tableName(prefix, 'streams');
    const records = tableName(prefix, 'records');
    const failure = LogStore.makeError;

    const unsafe = <A extends object>(
      sql: string,
      params: ReadonlyArray<unknown> = [],
    ): Effect.Effect<ReadonlyArray<A>, SqlError.SqlError> =>
      client.withoutTransforms().unsafe<A>(sql, params).unprepared;

    const streamColumns =
      'identity, epoch, producer_id, next_sequence, next_producer_sequence, last_fingerprint, last_offset';

    const create = Effect.fn('LogStoreSqlite.create')(function* (
      path: string,
      identity: string,
    ) {
      const rows = yield* unsafe<Row>(
        `INSERT INTO ${streams} (path, identity) VALUES (?, ?)
         ON CONFLICT(path) DO NOTHING RETURNING ${streamColumns}`,
        [path, identity],
      ).pipe(Effect.mapError(asError(path, 'create')));
      const row = rows[0];
      if (row === undefined) {
        return yield* failure(
          path,
          'create',
          'conflict',
          'a stream already exists here',
        );
      }
      const decoded = yield* readStream(row).pipe(
        Effect.mapError((error) =>
          failure(
            path,
            'create',
            'storage',
            `stored stream does not decode: ${String(error)}`,
          ),
        ),
      );
      return metaOf(path, decoded);
    });

    const acquire = Effect.fn('LogStoreSqlite.acquire')(function* (
      path: string,
      producerId: LogVocabulary.ProducerId,
      expected?: LogStore.AcquireExpected,
    ) {
      if (typeof producerId !== 'string' || producerId.length === 0) {
        return yield* failure(
          path,
          'acquire',
          'invalid',
          'producer id must be non-empty',
        );
      }
      const decodedProducerId = LogVocabulary.ProducerId.make(producerId);
      const expectedEpoch = expected?.epoch ?? null;
      const expectedHead = expected?.head ?? null;
      const rows = yield* unsafe<Row>(
        `UPDATE ${streams}
         SET epoch = epoch + 1, producer_id = ?, next_producer_sequence = 0,
             last_fingerprint = ''
         WHERE path = ? AND (? IS NULL OR epoch = ?) AND (? IS NULL OR last_offset = ?)
         RETURNING epoch`,
        [
          decodedProducerId,
          path,
          expectedEpoch,
          expectedEpoch,
          expectedHead,
          expectedHead,
        ],
      ).pipe(Effect.mapError(asError(path, 'acquire')));
      const row = rows[0];
      if (row === undefined) {
        if (expected !== undefined) {
          const existing = yield* unsafe<Row>(
            `SELECT 1 AS present FROM ${streams} WHERE path = ?`,
            [path],
          ).pipe(Effect.mapError(asError(path, 'acquire')));
          if (existing[0] !== undefined) {
            return yield* failure(
              path,
              'acquire',
              'conflict',
              `stream changed from epoch ${String(expected.epoch)} at ${String(expected.head)}`,
            );
          }
        }
        return yield* failure(
          path,
          'acquire',
          'not_found',
          'no stream at this path',
        );
      }
      const epoch = yield* asNumber('epoch', row['epoch']).pipe(
        Effect.mapError((error) =>
          failure(
            path,
            'acquire',
            'storage',
            `stored epoch does not decode: ${error.message}`,
          ),
        ),
      );
      if (!Schema.is(Schema.Natural)(epoch)) {
        return yield* failure(
          path,
          'acquire',
          'storage',
          'stored epoch does not decode',
        );
      }
      return {
        path,
        producerId: decodedProducerId,
        epoch: LogVocabulary.Epoch.make(epoch),
        nextSequence: LogVocabulary.ProducerSequence.make(0),
      } satisfies LogStore.ProducerClaim;
    });

    const append = Effect.fn('LogStoreSqlite.append')(function* (
      input: LogStore.AppendInput,
    ) {
      const validated = yield* LogStoreAdapter.validateInput(input);
      const result = yield* client
        .withTransaction(
          Effect.gen(function* () {
            const locked = yield* unsafe<Row>(
              `SELECT ${streamColumns} FROM ${streams} WHERE path = ?`,
              [input.path],
            );
            const found = locked[0];
            if (found === undefined) {
              return yield* failure(
                input.path,
                'append',
                'not_found',
                'no stream at this path',
              );
            }
            const state = yield* readStream(found).pipe(
              Effect.mapError((error) =>
                failure(
                  input.path,
                  'append',
                  'storage',
                  `stored stream does not decode: ${String(error)}`,
                ),
              ),
            );
            const decision = yield* LogStoreAdapter.decide(validated, {
              epoch: state.epoch,
              producerId: state.producerId,
              nextSequence: state.nextProducerSequence,
              lastFingerprint: state.lastFingerprint,
            }).pipe(Effect.provideService(Crypto.Crypto, crypto));
            if (decision.kind === 'retry') {
              return state.lastOffset;
            }

            const nextSequence =
              state.nextSequence + BigInt(decision.prepared.entries.length);
            const offset = LogOffset.fromSeq(nextSequence - 1n);
            // JSON1 lets a whole batch be inserted in one statement without
            // exceeding SQLite's bind-variable limit for large batches.
            const payload = yield* Schema.encodeEffect(UnknownJson)(
              decision.prepared.entries.map((entry, index) => {
                const seq = state.nextSequence + BigInt(index);
                return {
                  seq: seq.toString(),
                  offset: LogOffset.fromSeq(seq),
                  conversationId: entry.conversationId,
                  timestamp: entry.timestamp,
                  record: entry.record,
                };
              }),
            ).pipe(
              Effect.mapError((error) =>
                failure(
                  input.path,
                  'append',
                  'invalid',
                  `batch does not encode as JSON: ${error.message}`,
                ),
              ),
            );
            yield* unsafe<Row>(
              `INSERT INTO ${records}
               (path, seq, record_offset, producer_id, producer_epoch, producer_sequence,
                batch_index, conversation_id, record_timestamp, record)
               SELECT ?, json_extract(value, '$.seq'), json_extract(value, '$.offset'),
                      ?, ?, ?, CAST(key AS INTEGER), json_extract(value, '$.conversationId'),
                      json_extract(value, '$.timestamp'), json_extract(value, '$.record')
               FROM json_each(?)`,
              [
                input.path,
                validated.producerId,
                validated.epoch,
                validated.sequence,
                payload,
              ],
            );
            yield* unsafe<Row>(
              `UPDATE ${streams}
               SET next_sequence = ?, next_producer_sequence = ?,
                   last_fingerprint = ?, last_offset = ? WHERE path = ?`,
              [
                nextSequence.toString(),
                decision.nextSequence,
                decision.prepared.fingerprint,
                offset,
                input.path,
              ],
            );
            return offset;
          }),
        )
        .pipe(Effect.mapError(asError(input.path, 'append')));
      yield* reactivity.invalidate([REACTIVITY_KEY, input.path]);
      return result;
    });

    const read = Effect.fn('LogStoreSqlite.read')(function* (
      path: string,
      readOptions?: LogStore.ReadOptions,
    ) {
      const { after, limit } = yield* LogStore.normalizeReadOptions(
        readOptions,
      ).pipe(
        Effect.mapError((error) =>
          failure(path, 'read', 'invalid', error.detail),
        ),
      );
      const rows = yield* unsafe<Row>(
        `SELECT r.record_offset, r.conversation_id, r.record_timestamp, r.record
         FROM ${streams} s LEFT JOIN ${records} r
           ON r.path = s.path AND r.record_offset > ?
         WHERE s.path = ? ORDER BY r.record_offset ASC LIMIT ?`,
        [after, path, limit + 1],
      ).pipe(Effect.mapError(asError(path, 'read')));
      if (rows.length === 0) {
        return yield* failure(
          path,
          'read',
          'not_found',
          'no stream at this path',
        );
      }
      if (
        rows[0]?.['record_offset'] === null ||
        rows[0]?.['record_offset'] === undefined
      ) {
        return {
          records: [],
          cursor: after,
          upToDate: true,
        } satisfies LogStore.Page;
      }
      const upToDate = rows.length <= limit;
      const decoded = yield* Effect.forEach(
        upToDate ? rows : rows.slice(0, limit),
        readRecord,
      ).pipe(
        Effect.mapError((error) =>
          failure(
            path,
            'read',
            'storage',
            `stored record does not decode: ${String(error)}`,
          ),
        ),
      );
      return {
        records: decoded,
        cursor: decoded.at(-1)?.offset ?? after,
        upToDate,
      } satisfies LogStore.Page;
    });

    const readBackwards = Effect.fn('LogStoreSqlite.readBackwards')(function* (
      path: string,
      readOptions?: LogStore.ReadBackwardsOptions,
    ) {
      const normalized = yield* LogStore.normalizeReadBackwardsOptions(
        readOptions,
      ).pipe(
        Effect.mapError((error) =>
          failure(path, 'readBackwards', 'invalid', error.detail),
        ),
      );
      const before = Option.getOrUndefined(normalized.before);
      const rows = yield* unsafe<Row>(
        `SELECT r.record_offset, r.conversation_id, r.record_timestamp, r.record
         FROM ${streams} s LEFT JOIN ${records} r
           ON r.path = s.path AND (? IS NULL OR r.record_offset < ?)
         WHERE s.path = ? ORDER BY r.record_offset DESC LIMIT ?`,
        [before ?? null, before ?? null, path, normalized.limit + 1],
      ).pipe(Effect.mapError(asError(path, 'readBackwards')));
      if (rows.length === 0) {
        return yield* failure(
          path,
          'readBackwards',
          'not_found',
          'no stream at this path',
        );
      }
      if (
        rows[0]?.['record_offset'] === null ||
        rows[0]?.['record_offset'] === undefined
      ) {
        return {
          records: [],
          cursor: before ?? LogOffset.START,
          upToDate: true,
        } satisfies LogStore.BackwardsPage;
      }
      const upToDate = rows.length <= normalized.limit;
      const decoded = yield* Effect.forEach(
        upToDate ? rows : rows.slice(0, normalized.limit),
        readRecord,
      ).pipe(
        Effect.mapError((error) =>
          failure(
            path,
            'readBackwards',
            'storage',
            `stored record does not decode: ${String(error)}`,
          ),
        ),
      );
      return {
        records: decoded,
        cursor: decoded.at(-1)?.offset ?? before ?? LogOffset.START,
        upToDate,
      } satisfies LogStore.BackwardsPage;
    });

    const meta = Effect.fn('LogStoreSqlite.meta')(function* (path: string) {
      const rows = yield* unsafe<Row>(
        `SELECT ${streamColumns} FROM ${streams} WHERE path = ?`,
        [path],
      ).pipe(Effect.mapError(asError(path, 'meta')));
      const row = rows[0];
      if (row === undefined) {
        return Option.none<LogStore.StreamMeta>();
      }
      return Option.some(
        metaOf(
          path,
          yield* readStream(row).pipe(
            Effect.mapError((error) =>
              failure(
                path,
                'meta',
                'storage',
                `stored stream does not decode: ${String(error)}`,
              ),
            ),
          ),
        ),
      );
    });

    const changes = (
      path: string,
    ): Stream.Stream<void, LogStore.LogStoreError> =>
      client
        .reactive([REACTIVITY_KEY, path], Effect.void)
        .pipe(Stream.mapError(asError(path, 'changes')));

    return LogStore.Service.of({
      create,
      acquire,
      append,
      read,
      readBackwards,
      meta,
      changes,
    });
  });

export const layer = (
  options?: Options,
): Layer.Layer<
  LogStore.Service,
  never,
  SqlClient.SqlClient | Crypto.Crypto | Reactivity
> =>
  Layer.effect(
    LogStore.Service,
    Effect.flatMap(SqlClient.SqlClient, (client) => make(client, options)),
  );

export * as LogStoreSqlite from './layer.js';
