import { PgClient } from '@effect/sql-pg';
import { Crypto, Effect, Layer, Option, Schema, Stream } from 'effect';
import { SqlError } from 'effect/unstable/sql';

import { LogStoreAdapter } from '@sunfall/vesper-log/adapter';
import { LogStore } from '@sunfall/vesper-log/log-store';
import { LogOffset } from '@sunfall/vesper-log/offset';
import { RecordBatch } from '@sunfall/vesper-log/record-batch';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';

// Postgres log store.
//
// The durable backend. Everything the memory backend keeps in a
// `MutableHashMap` lives in two tables here, and the two things that have no
// memory analogue — batch atomicity across a crash, and wake-ups that reach a
// pod that did not write — are the reason this file exists.
//
// ## Driver and connection lifecycle
//
// A caller supplies the official `@effect/sql-pg` client directly, or provides
// it to {@link layer}. Its pool and connection lifecycle remain owned by the
// PgClient layer.
//
// Nothing here runs migrations. The authoritative DDL applications should put
// into their migration system ships at `migrations/001-initial.sql`, and the
// integration harness applies that exact published asset.
//
// ## Where the invariants actually live
//
// Two mechanisms, and it is worth being precise about which does what.
//
// `SELECT ... FOR UPDATE` on the stream row is what makes an append a
// read-check-write that no concurrent append can interleave with. Fencing
// state — epoch, holder, next producer sequence, last fingerprint, last
// offset — is columns on that row, mirroring the memory backend field for
// field so the two cannot drift.
//
// The `UNIQUE (path, producer_id, producer_epoch, producer_sequence,
// batch_index)` index on the records table is the durable half: a given
// producer's batch slot can be filled exactly once, enforced by the database
// rather than by the code above it. Note the fifth column. The natural index
// for this is four columns, which is right for a log that writes one row per
// *batch*; these offsets are per record, so one batch is many rows sharing a
// producer sequence and a four-column index would reject its own second row.
// `batch_index` is the minimal correction that keeps the property — insert
// this batch twice and the second insert raises 23505 and takes the whole
// transaction with it.
//
// ## Ordering and collation
//
// Offsets are fixed-width zero-padded decimals precisely so ordering is byte
// comparison everywhere. "Everywhere" does not include Postgres by default:
// under a glibc locale like `en_US.UTF-8`, punctuation is ignored at the
// primary comparison level, and `'-1'` — {@link LogOffset.START} — collates
// *after* `'0000…'` rather than before it, so a read from the beginning would
// return nothing. Every comparison and every ordering on `record_offset` here
// therefore pins `COLLATE "C"` explicitly, which is byte order and is exactly
// what JavaScript's `<` does. Do not remove those; the failure they prevent is
// an empty log that looks like an empty conversation.

/** A row as PostgreSQL hands it back. */
type Row = Readonly<Record<string, unknown>>;

/** The database operations the log store actually uses. */
export interface Client {
  readonly unsafe: (
    sql: string,
    params?: ReadonlyArray<unknown>,
  ) => Effect.Effect<ReadonlyArray<Row>, SqlError.SqlError>;
  readonly withTransaction: <R, E, A>(
    effect: Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E | SqlError.SqlError, R>;
  readonly listen: (
    channel: string,
  ) => Stream.Stream<string, SqlError.SqlError>;
}

export interface Options {
  /**
   * Postgres schema holding `streams` and `records`. Defaults to `ai_log`,
   * which is what `migrations/001-initial.sql` creates.
   */
  readonly schema?: string;
  /** Bounds every statement run while an append transaction holds a connection. */
  readonly transactionStatementTimeoutMs?: number;
}

const DEFAULT_SCHEMA = 'ai_log';
const IDENTIFIER = /^[a-z_][a-z0-9_]*$/;

/**
 * Wake-up channel for a path.
 *
 * `LISTEN` takes an identifier, not a value, so the path cannot be the channel
 * — paths are arbitrary strings and identifiers truncate at 63 bytes. A 64-bit
 * FNV-1a of the path is, and two paths that collide wake each other. That is
 * allowed and cheap: the interface permits spurious wake-ups, and a spurious
 * one costs one empty read. Deriving the channel from a hash rather than
 * sharing one channel for the whole store is what keeps a busy conversation
 * from waking every reader in the fleet.
 */
const FNV_OFFSET_BASIS = 0xcbf29ce484222325n;
const FNV_PRIME = 0x100000001b3n;
const SIXTY_FOUR_BITS = 0xffffffffffffffffn;

export const channelFor = (path: string): string => {
  let hash = FNV_OFFSET_BASIS;
  for (let index = 0; index < path.length; index += 1) {
    hash =
      ((hash ^ BigInt(path.charCodeAt(index))) * FNV_PRIME) & SIXTY_FOUR_BITS;
  }
  return `ai_log_${hash.toString(16).padStart(16, '0')}`;
};

/**
 * The `NOTIFY` payload is deliberately empty. The wake-up carries nothing by
 * design, and including the path would expose it to database observability
 * without adding any information to the listener.
 */
const NOTIFY_PAYLOAD = '';

class RowDecodeError extends Error {
  constructor(
    readonly field: string,
    readonly value: unknown,
    expected: string,
  ) {
    const actual = value === null ? 'null' : typeof value;
    super(`row field ${field} expected ${expected}, got ${actual}`);
  }
}

const asString = (
  field: string,
  value: unknown,
): Effect.Effect<string, RowDecodeError> =>
  typeof value === 'string'
    ? Effect.succeed(value)
    : Effect.fail(new RowDecodeError(field, value, 'a string'));

const asNumber = (
  field: string,
  value: unknown,
): Effect.Effect<number, RowDecodeError> => {
  if (typeof value === 'number') {
    return Number.isSafeInteger(value)
      ? Effect.succeed(value)
      : Effect.fail(new RowDecodeError(field, value, 'a safe integer'));
  }
  if (typeof value === 'bigint') {
    const result = Number(value);
    return Number.isSafeInteger(result)
      ? Effect.succeed(result)
      : Effect.fail(new RowDecodeError(field, value, 'a safe integer'));
  }
  if (typeof value === 'string' && /^[+-]?\d+$/.test(value)) {
    const result = Number(value);
    return Number.isSafeInteger(result)
      ? Effect.succeed(result)
      : Effect.fail(new RowDecodeError(field, value, 'a safe integer'));
  }
  return Effect.fail(new RowDecodeError(field, value, 'a safe integer'));
};

const asBigInt = (
  field: string,
  value: unknown,
): Effect.Effect<bigint, RowDecodeError> => {
  if (typeof value === 'bigint') return Effect.succeed(value);
  if (typeof value === 'number' && Number.isSafeInteger(value)) {
    return Effect.succeed(BigInt(value));
  }
  if (typeof value === 'string' && /^[+-]?\d+$/.test(value)) {
    return Effect.succeed(BigInt(value));
  }
  return Effect.fail(new RowDecodeError(field, value, 'an integer bigint'));
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

const StreamRowSchema = Schema.Struct({
  identity: Schema.String,
  epoch: LogVocabulary.Epoch,
  producerId: Schema.NullOr(LogVocabulary.ProducerId),
  nextProducerSequence: LogVocabulary.ProducerSequence,
});

const readStreamRow = (row: Row): Effect.Effect<StreamRow, unknown> =>
  Effect.gen(function* () {
    const identity = yield* asString('identity', row['identity']);
    const epoch = yield* asNumber('epoch', row['epoch']);
    const producerId =
      row['producer_id'] === null || row['producer_id'] === undefined
        ? null
        : yield* asString('producer_id', row['producer_id']);
    const nextProducerSequence = yield* asNumber(
      'next_producer_sequence',
      row['next_producer_sequence'],
    );
    const decoded = yield* Schema.decodeUnknownEffect(StreamRowSchema)({
      identity,
      epoch,
      producerId,
      nextProducerSequence,
    });
    const lastOffset = yield* LogOffset.decode(
      yield* asString('last_offset', row['last_offset']),
    );
    const nextSequence = yield* asBigInt('next_sequence', row['next_sequence']);
    const lastFingerprint = yield* asString(
      'last_fingerprint',
      row['last_fingerprint'],
    );
    return {
      ...decoded,
      producerId: decoded.producerId ?? undefined,
      nextSequence,
      lastFingerprint,
      lastOffset,
    };
  });

const readRecord = (row: Row) =>
  Effect.gen(function* () {
    return yield* RecordBatch.decodeEnvelope({
      offset: yield* asString('record_offset', row['record_offset']),
      conversationId: yield* asString(
        'conversation_id',
        row['conversation_id'],
      ),
      timestamp: yield* asNumber('record_timestamp', row['record_timestamp']),
      record: row['record'],
    });
  });

const metaOf = (path: string, row: StreamRow): LogStore.StreamMeta => ({
  path,
  identity: row.identity,
  epoch: row.epoch,
  producerId:
    row.producerId === undefined ? Option.none() : Option.some(row.producerId),
  head: row.lastOffset,
  // Record sequences start at zero and advance by one per record, so the next
  // one to assign is also how many exist. No counter to keep in step.
  records: Number(row.nextSequence),
});

export const make = (
  client: Client,
  options?: Options,
): Effect.Effect<LogStore.Interface, never, Crypto.Crypto> =>
  Effect.map(Crypto.Crypto, (crypto) => {
    const schema = options?.schema ?? DEFAULT_SCHEMA;
    if (!IDENTIFIER.test(schema)) {
      // A defect, not a failure: the schema name is wiring, not input, and it is
      // interpolated into SQL because Postgres has no parameter for an
      // identifier.
      throw new Error(`Not a usable Postgres schema name: ${schema}`);
    }
    const transactionStatementTimeoutMs =
      options?.transactionStatementTimeoutMs ?? 30_000;
    if (
      !Number.isSafeInteger(transactionStatementTimeoutMs) ||
      transactionStatementTimeoutMs < 1
    ) {
      throw new Error(
        `transactionStatementTimeoutMs must be a positive integer, got ${transactionStatementTimeoutMs}`,
      );
    }
    const streams = `${schema}.streams`;
    const records = `${schema}.records`;

    const failure = LogStore.makeError;

    /** Driver failures become `storage`; our own failures pass through. */
    const asLogStoreError =
      (path: string, operation: LogStore.Operation) =>
      (
        error: SqlError.SqlError | LogStore.LogStoreError,
      ): LogStore.LogStoreError =>
        error instanceof LogStore.LogStoreError
          ? error
          : LogStore.makeError(path, operation, 'storage', error.message);

    const create = Effect.fn('LogStore.create')(function* (
      path: string,
      identity: string,
    ) {
      const rows = yield* client
        .unsafe(
          `INSERT INTO ${streams} (path, identity)
         VALUES ($1, $2)
         ON CONFLICT (path) DO NOTHING
         RETURNING identity, epoch, producer_id, next_sequence,
                   next_producer_sequence, last_fingerprint, last_offset`,
          [path, identity],
        )
        .pipe(Effect.mapError(asLogStoreError(path, 'create')));

      const row = rows[0];
      if (row === undefined) {
        return yield* Effect.fail(
          failure(path, 'create', 'conflict', 'a stream already exists here'),
        );
      }
      const decoded = yield* readStreamRow(row).pipe(
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

    const acquire = Effect.fn('LogStore.acquire')(function* (
      path: string,
      producerId: LogVocabulary.ProducerId,
      expected?: LogStore.AcquireExpected,
    ) {
      const decodedProducerId = yield* Schema.decodeUnknownEffect(
        LogVocabulary.ProducerId,
      )(producerId).pipe(
        Effect.mapError(() =>
          failure(path, 'acquire', 'invalid', 'producer id must be non-empty'),
        ),
      );
      const rows = yield* client
        .unsafe(
          `UPDATE ${streams}
         SET epoch = epoch + 1,
             producer_id = $2,
             next_producer_sequence = 0,
             last_fingerprint = ''
         WHERE path = $1
            AND ($3::bigint IS NULL OR epoch = $3)
           AND ($4::text IS NULL OR last_offset = $4)
         RETURNING epoch`,
          [
            path,
            decodedProducerId,
            expected?.epoch ?? null,
            expected?.head ?? null,
          ],
        )
        .pipe(Effect.mapError(asLogStoreError(path, 'acquire')));

      const row = rows[0];
      if (row === undefined) {
        if (expected !== undefined) {
          const existing = yield* client
            .unsafe(`SELECT 1 FROM ${streams} WHERE path = $1`, [path])
            .pipe(Effect.mapError(asLogStoreError(path, 'acquire')));
          if (existing[0] !== undefined) {
            return yield* Effect.fail(
              failure(
                path,
                'acquire',
                'conflict',
                `stream changed from epoch ${expected.epoch} at ${expected.head}`,
              ),
            );
          }
        }
        return yield* Effect.fail(
          failure(path, 'acquire', 'not_found', 'no stream at this path'),
        );
      }

      const epochNumber = yield* asNumber('epoch', row['epoch']).pipe(
        Effect.mapError((error) =>
          failure(
            path,
            'acquire',
            'storage',
            `stored epoch does not decode: ${error.message}`,
          ),
        ),
      );

      return {
        path,
        producerId: decodedProducerId,
        epoch: yield* Schema.decodeUnknownEffect(LogVocabulary.Epoch)(
          epochNumber,
        ).pipe(
          Effect.mapError((error) =>
            failure(
              path,
              'acquire',
              'storage',
              `stored epoch does not decode: ${String(error)}`,
            ),
          ),
        ),
        nextSequence: LogVocabulary.ProducerSequence.make(0),
      } satisfies LogStore.ProducerClaim;
    });

    const append = Effect.fn('LogStore.append')(function* (
      input: LogStore.AppendInput,
    ) {
      const validated = yield* LogStoreAdapter.validateInput(input);

      return yield* client
        .withTransaction(
          Effect.gen(function* () {
            yield* client.unsafe(
              `SET LOCAL statement_timeout = ${transactionStatementTimeoutMs}`,
            );
            // The row lock. Every check below reads state that the write at the
            // bottom then advances, so the two have to be one critical section
            // per stream — which producer fencing already implies, since a
            // stream has one writer at a time by construction.
            const locked = yield* client.unsafe(
              `SELECT identity, epoch, producer_id, next_sequence,
                    next_producer_sequence, last_fingerprint, last_offset
             FROM ${streams}
             WHERE path = $1
             FOR UPDATE`,
              [input.path],
            );

            const found = locked[0];
            if (found === undefined) {
              return yield* Effect.fail(
                failure(
                  input.path,
                  'append',
                  'not_found',
                  'no stream at this path',
                ),
              );
            }
            const state = yield* readStreamRow(found).pipe(
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
            if (decision.kind === 'retry') return state.lastOffset;

            // Everything above rejects without writing. From here the batch
            // either commits whole or the transaction rolls back.
            const sequence =
              state.nextSequence + BigInt(decision.prepared.entries.length);
            const offset = LogOffset.fromSeq(sequence - 1n);

            // One JSON parameter avoids PostgreSQL's 65,535 bind-parameter
            // ceiling without chunking the logical append. The data-modifying
            // CTEs remain one statement and one transaction, so a large batch is
            // still all-or-nothing and occupies exactly one producer sequence.
            // `prepared.encoded` is also the exact material fingerprinted above:
            // no second schema encode or per-record stringify can drift from it.
            yield* client.unsafe(
              `WITH input AS (
               SELECT $2::bigint + item.ordinality - 1 AS seq,
                      (item.ordinality - 1)::integer AS batch_index,
                      item.value->>'conversationId' AS conversation_id,
                      (item.value->>'timestamp')::bigint AS record_timestamp,
                      item.value->'record' AS record
               FROM jsonb_array_elements($6::jsonb)
                    WITH ORDINALITY AS item(value, ordinality)
             ), inserted AS (
               INSERT INTO ${records}
                 (path, seq, record_offset, producer_id, producer_epoch,
                  producer_sequence, batch_index, conversation_id,
                  record_timestamp, record)
               SELECT $1,
                      seq,
                      lpad((seq / 10000000000000000)::text, 16, '0') || '_' ||
                        lpad((seq % 10000000000000000)::text, 16, '0'),
                      $3, $4, $5, batch_index, conversation_id,
                      record_timestamp, record
               FROM input
               ORDER BY seq
               RETURNING 1
             ), advanced AS (
               UPDATE ${streams}
               SET next_sequence = $7,
                   next_producer_sequence = $8,
                   last_fingerprint = $9,
                   last_offset = $10
               WHERE path = $1
               RETURNING last_offset
             )
             SELECT last_offset, pg_notify($11, $12)
             FROM advanced`,
              [
                input.path,
                state.nextSequence.toString(),
                validated.producerId,
                validated.epoch,
                validated.sequence,
                decision.prepared.encoded,
                sequence.toString(),
                decision.nextSequence,
                decision.prepared.fingerprint,
                offset,
                channelFor(input.path),
                NOTIFY_PAYLOAD,
              ],
            );

            // `pg_notify` above is inside the transaction on purpose. Postgres
            // delivers it at commit, so a reader never wakes before these rows
            // exist, and an interrupted or rolled-back append wakes nobody.

            return offset;
          }),
        )
        .pipe(Effect.mapError(asLogStoreError(input.path, 'append')));
    });

    const read = Effect.fn('LogStore.read')(function* (
      path: string,
      options?: LogStore.ReadOptions,
    ) {
      const { after, limit } = yield* LogStore.normalizeReadOptions(
        options,
      ).pipe(
        Effect.mapError((error) =>
          failure(path, 'read', 'invalid', error.detail),
        ),
      );

      // One round trip, and the `LEFT JOIN LATERAL` is what makes it one: the
      // outer row proves the stream exists (zero rows is `not_found`, which a
      // plain record query cannot distinguish from an empty log), and the
      // lateral side pages it. `limit + 1` answers `upToDate` without a second
      // count — a backend that guesses `false` here makes `Tail` spin.
      const rows = yield* client
        .unsafe(
          `SELECT r.record_offset, r.conversation_id, r.record_timestamp, r.record
         FROM ${streams} s
         LEFT JOIN LATERAL (
           SELECT record_offset, conversation_id, record_timestamp, record
           FROM ${records}
           WHERE path = s.path
             AND record_offset COLLATE "C" > $2 COLLATE "C"
           ORDER BY record_offset COLLATE "C" ASC
           LIMIT $3
         ) r ON true
         WHERE s.path = $1`,
          [path, after, limit + 1],
        )
        .pipe(Effect.mapError(asLogStoreError(path, 'read')));

      if (rows.length === 0) {
        return yield* Effect.fail(
          failure(path, 'read', 'not_found', 'no stream at this path'),
        );
      }

      // The stream exists but has nothing past `after`: the lateral side
      // produced no row and the outer join filled it with nulls.
      const first = rows[0];
      if (first?.['record_offset'] == null) {
        return {
          records: [],
          cursor: after,
          upToDate: true,
        } satisfies LogStore.Page;
      }

      const upToDate = rows.length <= limit;
      const page = upToDate ? rows : rows.slice(0, limit);

      const decoded = yield* Effect.forEach(page, readRecord).pipe(
        Effect.mapError((error) =>
          // A row this backend wrote that it can no longer decode is the
          // backend being wrong, not the caller — schema drift, a bad manual
          // edit, a driver that mangled jsonb.
          failure(
            path,
            'read',
            'storage',
            `stored record does not decode: ${String(error)}`,
          ),
        ),
      );

      const last = decoded[decoded.length - 1];
      return {
        records: decoded,
        cursor: last === undefined ? after : last.offset,
        upToDate,
      } satisfies LogStore.Page;
    });

    const readBackwards = Effect.fn('LogStore.readBackwards')(function* (
      path: string,
      options?: LogStore.ReadBackwardsOptions,
    ) {
      const normalized = yield* LogStore.normalizeReadBackwardsOptions(
        options,
      ).pipe(
        Effect.mapError((error) =>
          failure(path, 'readBackwards', 'invalid', error.detail),
        ),
      );
      const before = Option.getOrUndefined(normalized.before);
      const rows = yield* client
        .unsafe(
          `SELECT r.record_offset, r.conversation_id, r.record_timestamp, r.record
           FROM ${streams} s
           LEFT JOIN LATERAL (
             SELECT record_offset, conversation_id, record_timestamp, record
             FROM ${records}
             WHERE path = s.path
               AND ($2::text IS NULL OR record_offset COLLATE "C" < $2 COLLATE "C")
             ORDER BY record_offset COLLATE "C" DESC
             LIMIT $3
           ) r ON true
           WHERE s.path = $1`,
          [path, before ?? null, normalized.limit + 1],
        )
        .pipe(Effect.mapError(asLogStoreError(path, 'readBackwards')));
      if (rows.length === 0) {
        return yield* Effect.fail(
          failure(path, 'readBackwards', 'not_found', 'no stream at this path'),
        );
      }
      const first = rows[0];
      if (first?.['record_offset'] == null) {
        return {
          records: [],
          cursor: before ?? LogOffset.START,
          upToDate: true,
        } satisfies LogStore.BackwardsPage;
      }
      const upToDate = rows.length <= normalized.limit;
      const page = upToDate ? rows : rows.slice(0, normalized.limit);
      const decoded = yield* Effect.forEach(page, readRecord).pipe(
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

    const meta = Effect.fn('LogStore.meta')(function* (path: string) {
      const rows = yield* client
        .unsafe(
          `SELECT identity, epoch, producer_id, next_sequence,
                next_producer_sequence, last_fingerprint, last_offset
         FROM ${streams}
         WHERE path = $1`,
          [path],
        )
        .pipe(Effect.mapError(asLogStoreError(path, 'meta')));

      const row = rows[0];
      return row === undefined
        ? Option.none<LogStore.StreamMeta>()
        : Option.some(
            metaOf(
              path,
              yield* readStreamRow(row).pipe(
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

    // The corrected listener's first element is its readiness handshake: LISTEN
    // has completed before this opening wake-up can be observed.
    const changes = (
      path: string,
    ): Stream.Stream<void, LogStore.LogStoreError> =>
      client.listen(channelFor(path)).pipe(
        Stream.map(() => undefined),
        Stream.mapError(asLogStoreError(path, 'changes')),
      );

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
): Layer.Layer<LogStore.Service, never, PgClient.PgClient | Crypto.Crypto> =>
  Layer.effect(
    LogStore.Service,
    Effect.flatMap(Effect.service(PgClient.PgClient), (client) =>
      make(client, options),
    ),
  );

export * as LogStorePg from './layer.js';
